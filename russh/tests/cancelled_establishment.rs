#![cfg(not(target_arch = "wasm32"))]
use std::{sync::Arc, time::Duration};

struct HeldKeyCheck {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    ended: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Drop for HeldKeyCheck {
    fn drop(&mut self) {
        if let Some(ended) = self.ended.take() {
            let _ = ended.send(());
        }
    }
}
impl russh::client::Handler for HeldKeyCheck {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        _: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        self.entered.take().unwrap().send(()).unwrap();
        std::future::pending().await
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

#[tokio::test]
async fn cancelling_key_exchange_ends_the_owned_session_task() {
    // Only a hang budget; the handler's entered/ended signals establish task lifetime.
    tokio::time::timeout(Duration::from_secs(60), async {
        let (client, peer) = tokio::io::duplex(65536);
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (ended, mut released) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let config = russh::server::Config { keys: vec![key()], ..Default::default() };
            let session = russh::server::run_stream(Arc::new(config), peer, Peer).await?;
            session.await
        });
        let mut connecting = Box::pin(russh::client::connect_stream(Arc::new(Default::default()), client, HeldKeyCheck { entered: Some(entered), ended: Some(ended) }));
        tokio::select! { _ = ready => {}, result = &mut connecting => panic!("key check unexpectedly ended: {:?}", result.err()) }
        drop(connecting);
        let stopped = tokio::time::timeout(Duration::from_secs(30), &mut released).await;
        if stopped.is_err() { server.abort(); }
        let _ = server.await;
        assert!(stopped.is_ok(), "cancelled establishment left its handler/session task running");
    }).await.unwrap();
}
