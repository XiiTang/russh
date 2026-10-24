use russh::{client, server};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};

type Forward = (String, u32, oneshot::Sender<u32>);
type Open = (russh::ChannelId, server::ChannelOpenHandle);
struct Peer {
    opens: mpsc::UnboundedSender<Open>,
    closes: mpsc::UnboundedSender<russh::ChannelId>,
    forwards: mpsc::UnboundedSender<Forward>,
    cancels: mpsc::UnboundedSender<(String, u32)>,
    auth: Option<mpsc::UnboundedSender<oneshot::Sender<Option<bool>>>>,
    listeners: HashSet<(String, u32)>,
}
impl server::Handler for Peer {
    type Error = russh::Error;
    async fn auth_none(&mut self, _: &str) -> Result<server::Auth, Self::Error> {
        let accepted = if let Some(auth) = &self.auth {
            let (tx, rx) = oneshot::channel();
            auth.send(tx).unwrap();
            rx.await
                .map_err(|_| russh::Error::Disconnect)?
                .ok_or(russh::Error::Disconnect)?
        } else {
            true
        };
        Ok(if accepted {
            server::Auth::Accept
        } else {
            server::Auth::reject()
        })
    }
    async fn channel_open_session(
        &mut self,
        channel: russh::Channel<server::Msg>,
        reply: server::ChannelOpenHandle,
        _: &mut server::Session,
    ) -> Result<(), Self::Error> {
        self.opens.send((channel.id(), reply)).unwrap();
        Ok(())
    }
    async fn channel_close(
        &mut self,
        id: russh::ChannelId,
        _: &mut server::Session,
    ) -> Result<(), Self::Error> {
        self.closes.send(id).unwrap();
        Ok(())
    }
    async fn tcpip_forward(
        &mut self,
        address: &str,
        port: &mut u32,
        _: &mut server::Session,
    ) -> Result<bool, Self::Error> {
        let (tx, rx) = oneshot::channel();
        self.forwards.send((address.into(), *port, tx)).unwrap();
        *port = rx.await.unwrap();
        assert!(self.listeners.insert((address.into(), *port)));
        Ok(true)
    }
    async fn cancel_tcpip_forward(
        &mut self,
        address: &str,
        port: u32,
        _: &mut server::Session,
    ) -> Result<bool, Self::Error> {
        assert!(
            self.listeners.remove(&(address.into(), port)),
            "unknown listener cancelled"
        );
        self.cancels.send((address.into(), port)).unwrap();
        Ok(true)
    }
}
struct Trusted(mpsc::UnboundedSender<russh::ChannelId>);
impl client::Handler for Trusted {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        _: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
    async fn channel_open_confirmation(
        &mut self,
        id: russh::ChannelId,
        _: u32,
        _: u32,
        _: &mut client::Session,
    ) -> Result<(), Self::Error> {
        self.0.send(id).unwrap();
        Ok(())
    }
}
struct Fixture {
    client: Option<client::Handle<Trusted>>,
    server: Option<server::RunningSession<Peer>>,
    opens: mpsc::UnboundedReceiver<Open>,
    closes: mpsc::UnboundedReceiver<russh::ChannelId>,
    confirmations: mpsc::UnboundedReceiver<russh::ChannelId>,
    forwards: mpsc::UnboundedReceiver<Forward>,
    cancels: mpsc::UnboundedReceiver<(String, u32)>,
    auth: mpsc::UnboundedReceiver<oneshot::Sender<Option<bool>>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(client) = &self.client {
            client.stop();
        }
    }
}
impl Fixture {
    async fn new(gated_auth: bool) -> Self {
        let (left, right) = tokio::io::duplex(65536);
        let (open_tx, opens) = mpsc::unbounded_channel();
        let (close_tx, closes) = mpsc::unbounded_channel();
        let (confirm_tx, confirmations) = mpsc::unbounded_channel();
        let (forward_tx, forwards) = mpsc::unbounded_channel();
        let (cancel_tx, cancels) = mpsc::unbounded_channel();
        let (auth_tx, auth) = mpsc::unbounded_channel();
        use russh::keys::ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey};
        let key = russh::keys::PrivateKey::from(Ed25519Keypair::from(
            Ed25519PrivateKey::from_bytes(&[73; 32]),
        ));
        let server = server::run_stream(
            Arc::new(server::Config {
                keys: vec![key],
                auth_rejection_time: Duration::ZERO,
                ..Default::default()
            }),
            right,
            Peer {
                opens: open_tx,
                closes: close_tx,
                forwards: forward_tx,
                cancels: cancel_tx,
                auth: gated_auth.then_some(auth_tx),
                listeners: HashSet::new(),
            },
        );
        let client =
            client::connect_stream(Arc::new(Default::default()), left, Trusted(confirm_tx));
        let (server, client) = tokio::join!(server, client);
        Self {
            client: Some(client.unwrap()),
            server: Some(server.unwrap()),
            opens,
            closes,
            confirmations,
            forwards,
            cancels,
            auth,
        }
    }
    async fn finish(mut self) {
        let client = self.client.take().unwrap();
        client
            .disconnect(russh::Disconnect::ByApplication, "fixture complete", "en")
            .await
            .unwrap();
        assert!(matches!(
            client.await,
            Ok(()) | Err(russh::Error::Disconnect)
        ));
        self.server.take().unwrap().await.unwrap();
    }
}
async fn next<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), rx.recv())
        .await
        .expect("native state was not reached")
        .expect("native owner ended")
}

// A test recovery owner: it records release only after the native cancellation
// succeeds. The callback merely transfers ownership to this longer-lived owner.
async fn recover_listener(
    client: &client::Handle<Trusted>,
    replies: &mut mpsc::UnboundedReceiver<Option<u32>>,
    observations: mpsc::UnboundedSender<(u32, &'static str)>,
) {
    let port = next(replies).await.expect("listener grant rejected");
    client.cancel_tcpip_forward("late", port).await.unwrap();
    observations.send((port, "resource released")).unwrap();
}

#[tokio::test]
async fn a_late_listener_grant_is_cancelled_and_records_resource_released() {
    for sent_before_drop in [false, true] {
        let mut fixture = Fixture::new(false).await;
        assert!(
            fixture
                .client
                .as_mut()
                .unwrap()
                .authenticate_none("fixture")
                .await
                .unwrap()
                .success()
        );
        let client = fixture.client.as_ref().unwrap();
        // Neither no-reply request may reserve a position in the reply FIFO.
        client.send_keepalive(false).await.unwrap();
        client.no_more_sessions(false).await.unwrap();
        let (sink, mut disposed) = mpsc::unbounded_channel();
        let mut abandoned = Box::pin(client.tcpip_forward_with_sink(
            "late",
            0,
            Some(Arc::new(move |reply| {
                sink.send(reply).unwrap();
            })),
        ));
        assert!(futures::poll!(abandoned.as_mut()).is_pending());
        let (address, port, grant) = next(&mut fixture.forwards).await;
        assert_eq!((address.as_str(), port), ("late", 0));
        let mut live = Box::pin(client.tcpip_forward("live", 0));
        assert!(futures::poll!(live.as_mut()).is_pending());
        let mut abandoned = Some(abandoned);
        if !sent_before_drop {
            drop(abandoned.take());
        }
        grant.send(41000).unwrap();
        let (address, port, grant) = next(&mut fixture.forwards).await;
        assert_eq!((address.as_str(), port), ("live", 0));
        grant.send(41001).unwrap();
        // This later native reply proves the first was sent; its waiter has
        // never been polled again. A closed waiter must still keep its slot.
        assert_eq!(live.await.unwrap(), 41001);
        drop(abandoned.take());
        drop(abandoned);
        let (record, mut observations) = mpsc::unbounded_channel();
        recover_listener(client, &mut disposed, record).await;
        assert_eq!(next(&mut fixture.cancels).await, ("late".into(), 41000));
        assert_eq!(next(&mut observations).await, (41000, "resource released"));
        client.cancel_tcpip_forward("live", 41001).await.unwrap();
        assert_eq!(next(&mut fixture.cancels).await, ("live".into(), 41001));
        client.send_ping().await.unwrap();
        assert!(disposed.try_recv().is_err());
        assert!(observations.try_recv().is_err());
        fixture.finish().await;
    }
}

#[tokio::test]
async fn an_untaken_channel_reaches_its_owner_in_both_completion_races() {
    for sent_before_drop in [false, true] {
        let mut fixture = Fixture::new(false).await;
        assert!(
            fixture
                .client
                .as_mut()
                .unwrap()
                .authenticate_none("fixture")
                .await
                .unwrap()
                .success()
        );
        let client = fixture.client.as_ref().unwrap();
        let (sink, mut disposed) = mpsc::unbounded_channel();
        let mut abandoned = Box::pin(client.channel_open_session_with_sink(Some(Arc::new(
            move |reply| {
                sink.send(reply).unwrap();
            },
        ))));
        assert!(futures::poll!(abandoned.as_mut()).is_pending());
        let (peer_id, accept) = next(&mut fixture.opens).await;
        let mut abandoned = Some(abandoned);
        if !sent_before_drop {
            drop(abandoned.take());
        }
        accept.accept().await.unwrap();
        let id = next(&mut fixture.confirmations).await;
        drop(abandoned.take());
        drop(abandoned);
        let channel = next(&mut disposed).await.unwrap();
        assert_eq!(channel.id(), id);
        client.send_ping().await.unwrap();
        assert!(
            fixture.closes.try_recv().is_err(),
            "sink did not own the channel"
        );
        channel.close().await.unwrap();
        assert_eq!(next(&mut fixture.closes).await, peer_id);
        drop(channel);
        let mut sibling = Box::pin(client.channel_open_session());
        assert!(futures::poll!(sibling.as_mut()).is_pending());
        let (peer_id, accept) = next(&mut fixture.opens).await;
        accept.accept().await.unwrap();
        let sibling = sibling.await.unwrap();
        sibling.request_shell(false).await.unwrap();
        sibling.close().await.unwrap();
        assert_eq!(next(&mut fixture.closes).await, peer_id);
        drop(sibling);
        assert!(disposed.try_recv().is_err());
        fixture.finish().await;
    }
}

#[tokio::test]
async fn a_new_auth_waits_for_the_abandoned_attempt_and_gets_its_own_reply() {
    let mut fixture = Fixture::new(true).await;
    let (sink, mut disposed) = mpsc::unbounded_channel();
    let mut abandoned = Box::pin(
        fixture
            .client
            .as_mut()
            .unwrap()
            .authenticate_none_with_sink(
                "first",
                Some(Arc::new(move |reply| {
                    sink.send(reply).unwrap();
                })),
            ),
    );
    assert!(futures::poll!(abandoned.as_mut()).is_pending());
    let first_reply = next(&mut fixture.auth).await;
    drop(abandoned);
    let mut later = Box::pin(fixture.client.as_mut().unwrap().authenticate_none("second"));
    assert!(futures::poll!(later.as_mut()).is_pending());
    assert!(
        fixture.auth.try_recv().is_err(),
        "another native attempt overtook the first"
    );
    first_reply.send(Some(false)).unwrap();
    let second_reply = tokio::select! {
        reply = next(&mut fixture.auth) => reply,
        result = &mut later => panic!("second attempt took the first reply: {result:?}"),
    };
    assert!(matches!(
        next(&mut disposed).await,
        client::AuthenticationReply::AuthFailure { .. }
    ));
    second_reply.send(Some(true)).unwrap();
    assert!(later.await.unwrap().success());
    fixture.client.as_ref().unwrap().send_ping().await.unwrap();
    assert!(disposed.try_recv().is_err());
    fixture.finish().await;
}

#[tokio::test]
async fn a_session_end_answers_an_auth_call_waiting_for_an_abandoned_attempt() {
    let mut fixture = Fixture::new(true).await;
    let mut abandoned = Box::pin(fixture.client.as_mut().unwrap().authenticate_none("first"));
    assert!(futures::poll!(abandoned.as_mut()).is_pending());
    let first_reply = next(&mut fixture.auth).await;
    drop(abandoned);
    let mut later = Box::pin(fixture.client.as_mut().unwrap().authenticate_none("second"));
    assert!(futures::poll!(later.as_mut()).is_pending());
    assert!(fixture.auth.try_recv().is_err());
    first_reply.send(None).unwrap();
    assert!(matches!(
        fixture.server.take().unwrap().await,
        Err(russh::Error::Disconnect)
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(15), later)
            .await
            .unwrap(),
        Err(russh::Error::Disconnect)
    ));
    let _ = fixture.client.take().unwrap().await;
}

#[tokio::test]
async fn an_untaken_channel_refusal_reaches_the_sink_in_both_completion_races() {
    for sent_before_drop in [false, true] {
        let mut fixture = Fixture::new(false).await;
        assert!(
            fixture
                .client
                .as_mut()
                .unwrap()
                .authenticate_none("fixture")
                .await
                .unwrap()
                .success()
        );
        let client = fixture.client.as_ref().unwrap();
        let (sink, mut disposed) = mpsc::unbounded_channel();
        let mut abandoned = Box::pin(client.channel_open_session_with_sink(Some(Arc::new(
            move |reply| {
                sink.send(reply).unwrap();
            },
        ))));
        assert!(futures::poll!(abandoned.as_mut()).is_pending());
        let (_, reply) = next(&mut fixture.opens).await;
        let mut abandoned = Some(abandoned);
        if !sent_before_drop {
            drop(abandoned.take());
        }
        reply
            .reject(russh::ChannelOpenFailure::ConnectFailed)
            .await
            .unwrap();
        client.send_ping().await.unwrap();
        drop(abandoned.take());
        drop(abandoned);
        assert!(matches!(
            next(&mut disposed).await,
            Err(russh::Error::ChannelOpenFailure(
                russh::ChannelOpenFailure::ConnectFailed
            ))
        ));
        assert!(disposed.try_recv().is_err());
        fixture.finish().await;
    }
}

#[tokio::test]
async fn a_managed_open_keeps_its_native_cancellation_and_disposes_the_late_channel() {
    let mut fixture = Fixture::new(false).await;
    assert!(
        fixture
            .client
            .as_mut()
            .unwrap()
            .authenticate_none("fixture")
            .await
            .unwrap()
            .success()
    );
    let client = fixture.client.as_ref().unwrap();
    let (sink, mut disposed) = mpsc::unbounded_channel();
    let mut abandoned = Box::pin(client.channel_open_session_managed_with_sink(Some(Arc::new(
        move |reply| {
            sink.send(reply).unwrap();
        },
    ))));
    assert!(futures::poll!(abandoned.as_mut()).is_pending());
    let (peer_id, reply) = next(&mut fixture.opens).await;
    drop(abandoned);
    reply.accept().await.unwrap();
    let channel = next(&mut disposed).await.unwrap();
    assert_eq!(next(&mut fixture.closes).await, peer_id);
    drop(channel);
    client.send_ping().await.unwrap();
    assert!(disposed.try_recv().is_err());
    fixture.finish().await;
}

#[tokio::test]
async fn a_dropped_auth_waiter_without_a_sink_cannot_give_its_reply_to_the_next_call() {
    let mut fixture = Fixture::new(true).await;
    let mut first = Box::pin(fixture.client.as_mut().unwrap().authenticate_none("first"));
    assert!(futures::poll!(first.as_mut()).is_pending());
    let first_reply = next(&mut fixture.auth).await;
    drop(first);
    let mut later = Box::pin(fixture.client.as_mut().unwrap().authenticate_none("second"));
    assert!(futures::poll!(later.as_mut()).is_pending());
    first_reply.send(Some(false)).unwrap();
    let second_reply = tokio::select! {
        reply = next(&mut fixture.auth) => reply,
        result = &mut later => panic!("second attempt took the first reply: {result:?}"),
    };
    second_reply.send(Some(true)).unwrap();
    assert!(
        later.await.unwrap().success(),
        "second attempt received the first rejection"
    );
    fixture.finish().await;
}
