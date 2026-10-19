#![cfg(not(target_arch = "wasm32"))]
//! An authentication the session's end interrupts ends with `Disconnect`,
//! never with a failure the server did not send.
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use russh::keys::ssh_key::{
    self, HashAlg,
    certificate::{Builder, CertType},
    private::{Ed25519Keypair, Ed25519PrivateKey},
};
use russh::keys::{PrivateKey, PrivateKeyWithHashAlg, agent::AgentIdentity};
use russh::{client, server};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

/// A client socket whose reads fail once `cut` fires: the session ends
/// under whatever waits on it.
struct Cuttable {
    inner: DuplexStream,
    cut: tokio::sync::oneshot::Receiver<()>,
}
impl AsyncRead for Cuttable {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if Pin::new(&mut self.cut).poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for Cuttable {
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
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

struct Trusting;
impl client::Handler for Trusting {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        _: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}
/// Refuses every key it is offered, before any signature.
struct Refusing;
impl server::Handler for Refusing {
    type Error = russh::Error;
    async fn auth_publickey_offered(
        &mut self,
        _: &str,
        _: &ssh_key::PublicKey,
    ) -> Result<server::Auth, Self::Error> {
        Ok(server::Auth::reject())
    }
}
fn key(seed: u8) -> PrivateKey {
    PrivateKey::from(Ed25519Keypair::from(Ed25519PrivateKey::from_bytes(
        &[seed; 32],
    )))
}
fn certificate() -> ssh_key::Certificate {
    let mut builder =
        Builder::new_with_random_nonce(&mut rand::rng(), key(81).public_key().clone(), 0, u64::MAX)
            .unwrap();
    builder.cert_type(CertType::User).unwrap();
    builder.valid_principal("user").unwrap();
    builder.sign(&key(82)).unwrap()
}

/// What a provider ends the authentication with: the library's report, or
/// its own failure.
#[derive(Debug)]
enum Ended {
    Russh(russh::Error),
    Provider,
}
impl From<russh::Error> for Ended {
    fn from(error: russh::Error) -> Self {
        Self::Russh(error)
    }
}
struct Provider;
impl russh::Signer for Provider {
    type Error = Ended;
    async fn auth_sign(
        &mut self,
        _: &AgentIdentity,
        _: Option<HashAlg>,
        _: Vec<u8>,
    ) -> Result<Vec<u8>, Self::Error> {
        Err(Ended::Provider)
    }
}
impl russh::GssapiAuthenticator for Provider {
    type Error = Ended;
    async fn gssapi_step(
        &mut self,
        _: Option<Vec<u8>>,
        _: Option<Vec<u8>>,
        _: Vec<u8>,
    ) -> Result<russh::GssapiStep, Self::Error> {
        Err(Ended::Provider)
    }
}

#[derive(Clone, Copy, Debug)]
enum Entry {
    PublicKey,
    PublicKeyWith,
    CertificateWith,
    Gssapi,
}
const ENTRIES: [Entry; 4] = [
    Entry::PublicKey,
    Entry::PublicKeyWith,
    Entry::CertificateWith,
    Entry::Gssapi,
];
async fn authenticate(
    handle: &mut client::Handle<Trusting>,
    entry: Entry,
) -> Result<client::AuthResult, Ended> {
    match entry {
        Entry::PublicKey => handle
            .authenticate_publickey("user", PrivateKeyWithHashAlg::new(Arc::new(key(81)), None))
            .await
            .map_err(Ended::Russh),
        Entry::PublicKeyWith => {
            handle
                .authenticate_publickey_with(
                    "user",
                    key(81).public_key().clone(),
                    None,
                    &mut Provider,
                )
                .await
        }
        Entry::CertificateWith => {
            handle
                .authenticate_certificate_with("user", certificate(), None, &mut Provider)
                .await
        }
        Entry::Gssapi => {
            // The DER encoding of the Kerberos V5 mechanism OID.
            let oid = vec![
                0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02,
            ];
            handle
                .authenticate_gssapi_with_mic("user", vec![oid], &mut Provider)
                .await
        }
    }
}
async fn connect() -> (client::Handle<Trusting>, tokio::sync::oneshot::Sender<()>) {
    let (client, peer) = tokio::io::duplex(65536);
    tokio::spawn(async move {
        let config = server::Config {
            keys: vec![key(80)],
            auth_rejection_time: Duration::ZERO,
            auth_rejection_time_initial: Some(Duration::ZERO),
            ..Default::default()
        };
        let session = server::run_stream(Arc::new(config), peer, Refusing).await?;
        session.await
    });
    let (cut, read_fails) = tokio::sync::oneshot::channel();
    let handle = client::connect_stream(
        Arc::new(Default::default()),
        Cuttable {
            inner: client,
            cut: read_fails,
        },
        Trusting,
    )
    .await
    .unwrap();
    (handle, cut)
}

#[tokio::test]
async fn an_authentication_the_sessions_end_interrupts_is_a_disconnect() {
    // Only a hang budget; the first poll queues the request, and the
    // single-threaded runtime runs the session only after the cut.
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut wrong = Vec::new();
        for entry in ENTRIES {
            let (mut handle, cut) = connect().await;
            let mut waiting = Box::pin(authenticate(&mut handle, entry));
            assert!(
                futures::poll!(&mut waiting).is_pending(),
                "{entry:?} answered before its request was sent"
            );
            cut.send(()).unwrap();
            match waiting.await {
                Err(Ended::Russh(russh::Error::Disconnect)) => {}
                result => wrong.push(format!("{entry:?} ended as {result:?}")),
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn a_refusal_the_server_sends_stays_a_failure() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for entry in [Entry::PublicKey, Entry::PublicKeyWith, Entry::Gssapi] {
            let (mut handle, _cut) = connect().await;
            match authenticate(&mut handle, entry).await {
                Ok(client::AuthResult::Failure {
                    remaining_methods,
                    partial_success: false,
                }) if !remaining_methods.is_empty() => {}
                result => panic!("{entry:?} ended as {result:?}"),
            }
        }
    })
    .await
    .unwrap();
}
