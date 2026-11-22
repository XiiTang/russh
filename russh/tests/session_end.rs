#![cfg(not(target_arch = "wasm32"))]

use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use russh::{ChannelId, ChannelMsg, Disconnect, Error, client, server};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    sync::{mpsc, oneshot},
};

const BUDGET: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Eq)]
enum End {
    Received(u32, String),
    Local,
    Io(io::ErrorKind),
    UnknownKey,
}

struct Client {
    ended: Option<oneshot::Sender<End>>,
    disconnected: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
    trusted: russh::keys::PrivateKey,
}

impl client::Handler for Client {
    type Error = Error;

    async fn check_server_key(
        &mut self,
        key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Error> {
        Ok(key.public_key().key_data() == self.trusted.public_key().key_data())
    }

    fn session_ended(&mut self, end: client::SessionEnd<'_, Error>) {
        let end = match end {
            client::SessionEnd::Received(info) => {
                End::Received(info.reason_code as u32, info.message.clone())
            }
            client::SessionEnd::Local => End::Local,
            client::SessionEnd::Error(Error::IO(error)) => End::Io(error.kind()),
            client::SessionEnd::Error(Error::UnknownKey) => End::UnknownKey,
            end => panic!("unexpected session end: {end:?}"),
        };
        self.ended
            .take()
            .expect("one end per session")
            .send(end)
            .unwrap();
    }

    async fn disconnected(&mut self, _: client::DisconnectReason<Error>) -> Result<(), Error> {
        if let Some((entered, resume)) = self.disconnected.take() {
            entered.send(()).unwrap();
            resume.await.unwrap();
        }
        Ok(())
    }
}

struct Peer(mpsc::UnboundedSender<(ChannelId, server::ChannelOpenHandle)>);

impl server::Handler for Peer {
    type Error = Error;

    async fn auth_none(&mut self, _: &str) -> Result<server::Auth, Error> {
        Ok(server::Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        channel: russh::Channel<server::Msg>,
        reply: server::ChannelOpenHandle,
        _: &mut server::Session,
    ) -> Result<(), Error> {
        self.0.send((channel.id(), reply)).unwrap();
        Ok(())
    }
}

fn key(seed: u8) -> russh::keys::PrivateKey {
    use russh::keys::ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey};
    Ed25519Keypair::from(Ed25519PrivateKey::from_bytes(&[seed; 32])).into()
}

struct Transport {
    inner: DuplexStream,
    cut: Option<oneshot::Receiver<()>>,
    fail_flush: Arc<AtomicBool>,
    hold_shutdown: Option<oneshot::Sender<()>>,
    shutdown_held: bool,
}

impl Transport {
    fn new(inner: DuplexStream) -> Self {
        Self {
            inner,
            cut: None,
            fail_flush: Arc::default(),
            hold_shutdown: None,
            shutdown_held: false,
        }
    }
}

impl AsyncRead for Transport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(cut) = self.cut.as_mut()
            && Pin::new(cut).poll(cx).is_ready()
        {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.fail_flush.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(entered) = self.hold_shutdown.take() {
            self.shutdown_held = true;
            entered.send(()).unwrap();
        }
        if self.shutdown_held {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

async fn serve(
    peer: DuplexStream,
    opens: mpsc::UnboundedSender<(ChannelId, server::ChannelOpenHandle)>,
) -> server::RunningSession<Peer> {
    let config = server::Config {
        keys: vec![key(71)],
        auth_rejection_time: Duration::ZERO,
        auth_rejection_time_initial: Some(Duration::ZERO),
        ..Default::default()
    };
    server::run_stream(Arc::new(config), peer, Peer(opens))
        .await
        .unwrap()
}

#[tokio::test]
async fn an_ended_session_releases_waiters_before_the_async_disconnect_callback_returns() {
    tokio::time::timeout(BUDGET, async {
        let (stream, peer) = tokio::io::duplex(65536);
        let (opens, mut opened) = mpsc::unbounded_channel();
        let server = tokio::spawn(serve(peer, opens));
        let (ended, observed) = oneshot::channel();
        let (entered, callback_waiting) = oneshot::channel();
        let (resume, proceed) = oneshot::channel();
        let mut client = client::connect_stream(
            Arc::default(),
            stream,
            Client {
                ended: Some(ended),
                disconnected: Some((entered, proceed)),
                trusted: key(71),
            },
        ).await.unwrap();
        let server = server.await.unwrap();
        let peer_handle = server.handle();
        assert!(client.authenticate_none("user").await.unwrap().success());

        let mut first = Box::pin(client.channel_open_session());
        let (channel_id, reply) = tokio::select! {
            received = opened.recv() => received.unwrap(),
            result = &mut first => panic!("the server has not answered this OPEN: {result:?}"),
        };
        reply.accept().await.unwrap();
        let mut channel = first.await.unwrap();

        let mut second = Box::pin(client.channel_open_session());
        let (_, held_reply) = tokio::select! {
            received = opened.recv() => received.unwrap(),
            result = &mut second => panic!("the server has not answered this OPEN: {result:?}"),
        };
        peer_handle.data(channel_id, b"accepted".to_vec()).await.unwrap();
        peer_handle.disconnect(Disconnect::ByApplication, "bye".into(), String::new()).await.unwrap();
        callback_waiting.await.unwrap();

        assert_eq!(observed.await.unwrap(), End::Received(Disconnect::ByApplication as u32, "bye".into()));
        assert!(matches!(second.await, Err(Error::Disconnect)));
        assert!(matches!(client.rekey_soon().await, Err(Error::SendError)));
        assert!(matches!(channel.wait().await, Some(ChannelMsg::Data { data }) if data.as_ref() == b"accepted"));
        assert!(channel.wait().await.is_none());
        drop(held_reply);
        resume.send(()).unwrap();
        client.await.unwrap();
        let _ = server.await;
    }).await.unwrap();
}

#[tokio::test]
async fn a_local_disconnect_is_reported_after_its_flush_succeeds() {
    tokio::time::timeout(BUDGET, async {
        for (eof, fail_flush, expected) in [
            (false, false, End::Local),
            (false, true, End::Io(io::ErrorKind::BrokenPipe)),
            (true, false, End::Io(io::ErrorKind::UnexpectedEof)),
        ] {
            let (stream, peer) = tokio::io::duplex(65536);
            let (opens, _) = mpsc::unbounded_channel();
            let server = tokio::spawn(serve(peer, opens));
            let (ended, observed) = oneshot::channel();
            let (cut, read_eof) = oneshot::channel();
            let mut stream = Transport::new(stream);
            stream.cut = Some(read_eof);
            let flush = stream.fail_flush.clone();
            let mut client = client::connect_stream(
                Arc::default(),
                stream,
                Client {
                    ended: Some(ended),
                    disconnected: None,
                    trusted: key(71),
                },
            )
            .await
            .unwrap();
            let server = server.await.unwrap();
            assert!(client.authenticate_none("user").await.unwrap().success());
            flush.store(fail_flush, Ordering::Release);
            if eof {
                cut.send(()).unwrap();
            } else {
                client
                    .disconnect(Disconnect::ByApplication, "bye", "")
                    .await
                    .unwrap();
            }
            assert_eq!(observed.await.unwrap(), expected);
            assert!(matches!(client.await, Err(Error::Disconnect)));
            let _ = server.await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn an_initial_key_refusal_answers_while_transport_shutdown_waits() {
    tokio::time::timeout(BUDGET, async {
        let (stream, peer) = tokio::io::duplex(65536);
        let (opens, _) = mpsc::unbounded_channel();
        let server = tokio::spawn(serve(peer, opens));
        let (ended, observed) = oneshot::channel();
        let (hold_shutdown, entered) = oneshot::channel();
        let mut stream = Transport::new(stream);
        stream.hold_shutdown = Some(hold_shutdown);
        let mut owner = client::connect_stream_owned(
            Arc::default(),
            stream,
            Client {
                ended: Some(ended),
                disconnected: None,
                trusted: key(72),
            },
        )
        .await
        .unwrap();

        assert!(matches!(owner.establish().await, Err(Error::UnknownKey)));
        assert_eq!(observed.await.unwrap(), End::UnknownKey);
        entered.await.unwrap();
        assert!(!owner.is_joined());
        owner.stop();
        owner.await.unwrap();
        let _ = server.await.unwrap().await;
    })
    .await
    .unwrap();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn the_server_fixture_can_present_a_changed_key_before_authentication() {
    tokio::time::timeout(BUDGET, async {
        let (stream, peer) = tokio::io::duplex(65536);
        let (opens, _) = mpsc::unbounded_channel();
        let server = tokio::spawn(serve(peer, opens));
        let (ended, observed) = oneshot::channel();
        let client = client::connect_stream(
            Arc::default(),
            stream,
            Client {
                ended: Some(ended),
                disconnected: None,
                trusted: key(71),
            },
        )
        .await
        .unwrap();
        let server = server.await.unwrap();
        let started = server
            .handle()
            .rekey_with_host_keys_for_tests(vec![key(72)])
            .await
            .unwrap();

        started.await.unwrap();
        assert_eq!(observed.await.unwrap(), End::UnknownKey);
        assert!(matches!(client.await, Err(Error::Disconnect)));
        let _ = server.await;
    })
    .await
    .unwrap();
}
