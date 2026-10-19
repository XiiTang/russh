#![cfg(not(target_arch = "wasm32"))]
//! The handler learns how a session ended even when the socket's shutdown
//! fails afterwards.
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

/// A client socket whose shutdown always fails, and whose reads fail once
/// `cut` fires.
struct FailingShutdown {
    inner: DuplexStream,
    cut: Option<tokio::sync::oneshot::Receiver<()>>,
}
impl AsyncRead for FailingShutdown {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(cut) = self.cut.as_mut()
            && Pin::new(cut).poll(cx).is_ready()
        {
            return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for FailingShutdown {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::ErrorKind::NotConnected.into()))
    }
}
use std::future::Future as _;

/// Records each end the session reports.
struct Recording(Arc<Mutex<Vec<String>>>);
impl russh::client::Handler for Recording {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        _: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
    async fn disconnected(
        &mut self,
        reason: russh::client::DisconnectReason<Self::Error>,
    ) -> Result<(), Self::Error> {
        self.0.lock().unwrap().push(match reason {
            russh::client::DisconnectReason::ReceivedDisconnect(info) => {
                format!("received {:?} {}", info.reason_code, info.message)
            }
            russh::client::DisconnectReason::Error(error) => format!("error {error}"),
        });
        Ok(())
    }
}
struct Peer;
impl russh::server::Handler for Peer {
    type Error = russh::Error;
}
fn key() -> russh::keys::PrivateKey {
    use russh::keys::ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey};
    russh::keys::PrivateKey::from(Ed25519Keypair::from(Ed25519PrivateKey::from_bytes(
        &[71; 32],
    )))
}

/// A connected client over a socket whose shutdown fails, the server's
/// session, and the ends the client's handler was told of.
async fn connect(
    cut: Option<tokio::sync::oneshot::Receiver<()>>,
) -> (
    russh::client::Handle<Recording>,
    russh::server::RunningSession<Peer>,
    Arc<Mutex<Vec<String>>>,
) {
    let (client, peer) = tokio::io::duplex(65536);
    let server = tokio::spawn(async move {
        let config = russh::server::Config {
            keys: vec![key()],
            ..Default::default()
        };
        russh::server::run_stream(Arc::new(config), peer, Peer).await
    });
    let ends = Arc::new(Mutex::new(Vec::new()));
    let handle = russh::client::connect_stream(
        Arc::new(Default::default()),
        FailingShutdown { inner: client, cut },
        Recording(ends.clone()),
    )
    .await
    .unwrap();
    (handle, server.await.unwrap().unwrap(), ends)
}

fn assert_shutdown_failure(result: Result<(), russh::Error>) {
    match result {
        Err(russh::Error::IO(error)) => assert_eq!(error.kind(), io::ErrorKind::NotConnected),
        result => panic!("the session did not return its shutdown failure: {result:?}"),
    }
}

#[tokio::test]
async fn a_peers_disconnect_reaches_the_handler_though_shutdown_fails() {
    // Only a hang budget; the session's own end orders the test.
    tokio::time::timeout(Duration::from_secs(30), async {
        let (mut handle, server, ends) = connect(None).await;
        server
            .handle()
            .disconnect(russh::Disconnect::ByApplication, "bye".into(), "".into())
            .await
            .unwrap();
        assert_shutdown_failure((&mut handle).await);
        assert_eq!(*ends.lock().unwrap(), ["received ByApplication bye"]);
        let _ = server.await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn a_failed_session_reaches_the_handler_though_shutdown_fails() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let (cut, read_fails) = tokio::sync::oneshot::channel();
        let (mut handle, server, ends) = connect(Some(read_fails)).await;
        cut.send(()).unwrap();
        assert_shutdown_failure((&mut handle).await);
        let ends = ends.lock().unwrap().clone();
        assert_eq!(ends.len(), 1, "{ends:?}");
        assert!(ends[0].starts_with("error "), "{ends:?}");
        drop(server);
    })
    .await
    .unwrap();
}
