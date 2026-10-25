use russh::{ChannelInboxLimits, ChannelMsg, InboxLimitExceeded, client, server};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc;

struct Peer {
    opens: mpsc::UnboundedSender<russh::ChannelId>,
    closes: mpsc::UnboundedSender<russh::ChannelId>,
    channels: Vec<russh::Channel<server::Msg>>,
}
impl server::Handler for Peer {
    type Error = russh::Error;
    async fn auth_none(&mut self, _: &str) -> Result<server::Auth, Self::Error> {
        Ok(server::Auth::Accept)
    }
    async fn channel_open_session(
        &mut self,
        channel: russh::Channel<server::Msg>,
        reply: server::ChannelOpenHandle,
        _: &mut server::Session,
    ) -> Result<(), Self::Error> {
        let id = channel.id();
        self.channels.push(channel);
        reply.accept().await?;
        self.opens.send(id).map_err(|_| russh::Error::Disconnect)?;
        Ok(())
    }
    async fn channel_close(
        &mut self,
        id: russh::ChannelId,
        _: &mut server::Session,
    ) -> Result<(), Self::Error> {
        self.closes.send(id).map_err(|_| russh::Error::Disconnect)?;
        Ok(())
    }
}
struct Trusted(Arc<AtomicUsize>);
impl client::Handler for Trusted {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        _: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
    async fn data(
        &mut self,
        _: russh::ChannelId,
        _: &[u8],
        _: &mut client::Session,
    ) -> Result<(), Self::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn exit_signal(
        &mut self,
        _: russh::ChannelId,
        _: russh::Sig,
        _: bool,
        _: &str,
        _: &str,
        _: &mut client::Session,
    ) -> Result<(), Self::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn exercise(control_bytes: bool) {
    use russh::keys::ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey};
    let key = russh::keys::PrivateKey::from(Ed25519Keypair::from(Ed25519PrivateKey::from_bytes(
        &[91; 32],
    )));
    let (left, right) = tokio::io::duplex(65536);
    let (open_tx, mut opens) = mpsc::unbounded_channel();
    let (close_tx, mut closes) = mpsc::unbounded_channel();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let limits = ChannelInboxLimits {
        max_control_bytes: 64,
        max_entries: if control_bytes { 8 } else { 2 },
    };
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
            channels: Vec::new(),
        },
    );
    let client = client::connect_stream(
        Arc::new(client::Config {
            channel_inbox_limits: limits,
            window_size: 64,
            maximum_packet_size: 128,
            ..Default::default()
        }),
        left,
        Trusted(callbacks.clone()),
    );
    let (server, client) = tokio::join!(server, client);
    let server = server.unwrap();
    let mut client = client.unwrap();
    assert!(client.authenticate_none("fixture").await.unwrap().success());
    let mut affected = client.channel_open_session().await.unwrap();
    let peer_id = opens.recv().await.unwrap();
    let mut sibling = client.channel_open_session().await.unwrap();
    let sibling_peer_id = opens.recv().await.unwrap();
    let handle = server.handle();
    if control_bytes {
        for _ in 0..2 {
            handle
                .exit_signal_request(
                    peer_id,
                    russh::Sig::TERM,
                    false,
                    "x".repeat(40),
                    "en".into(),
                )
                .await
                .unwrap();
        }
    } else {
        for _ in 0..3 {
            handle.data(peer_id, b"x".as_slice()).await.unwrap();
        }
    }
    // Do not drain the affected inbox until the peer observes its native close.
    assert_eq!(closes.recv().await, Some(peer_id));
    assert_eq!(
        callbacks.load(Ordering::SeqCst),
        if control_bytes { 1 } else { 2 },
        "the refused message reached a handler"
    );
    if control_bytes {
        assert!(
            matches!(affected.wait().await, Some(ChannelMsg::ExitSignal { error_message, .. }) if error_message == "x".repeat(40))
        );
    } else {
        for _ in 0..2 {
            assert!(
                matches!(affected.wait().await, Some(ChannelMsg::Data { data }) if data.as_ref() == b"x")
            );
        }
    }
    let cause = if control_bytes {
        InboxLimitExceeded::ControlBytes { limit: 64 }
    } else {
        InboxLimitExceeded::Entries { limit: 2 }
    };
    assert!(
        matches!(affected.wait().await, Some(ChannelMsg::InboxLimitExceeded(actual)) if actual == cause)
    );
    assert!(affected.wait().await.is_none());
    handle
        .data(sibling_peer_id, b"still live".as_slice())
        .await
        .unwrap();
    assert!(
        matches!(sibling.wait().await, Some(ChannelMsg::Data { data }) if data.as_ref() == b"still live")
    );
    // Opening another channel exercises the session, not only a queued sibling.
    let _another = client.channel_open_session().await.unwrap();
    let _ = opens.recv().await.unwrap();
    assert!(
        closes.try_recv().is_err(),
        "more than one close was observed"
    );
    client
        .disconnect(russh::Disconnect::ByApplication, "fixture complete", "")
        .await
        .unwrap();
    assert!(matches!(
        client.await,
        Ok(()) | Err(russh::Error::Disconnect)
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn large_controls_end_only_the_affected_channel_with_a_typed_cause() {
    tokio::time::timeout(Duration::from_secs(15), exercise(true))
        .await
        .unwrap();
}
#[tokio::test]
async fn tiny_data_entries_end_only_the_affected_channel_with_a_typed_cause() {
    tokio::time::timeout(Duration::from_secs(15), exercise(false))
        .await
        .unwrap();
}
