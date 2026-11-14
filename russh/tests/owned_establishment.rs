#![cfg(not(target_arch = "wasm32"))]
use std::{sync::Arc, time::Duration};

struct KeyCheck {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    released: Option<tokio::sync::oneshot::Sender<()>>,
    proceed: Option<tokio::sync::oneshot::Receiver<bool>>,
}
impl Drop for KeyCheck {
    fn drop(&mut self) {
        if let Some(released) = self.released.take() {
            let _ = released.send(());
        }
    }
}
impl russh::client::Handler for KeyCheck {
    type Error = russh::Error;
    #[expect(
        clippy::panic,
        reason = "The regression forces a native key-exchange task panic"
    )]
    async fn check_server_key(
        &mut self,
        _: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let Some(entered) = self.entered.take() else {
            return Err(russh::Error::Inconsistent);
        };
        entered.send(()).map_err(|_| russh::Error::Disconnect)?;
        if self
            .proceed
            .take()
            .ok_or(russh::Error::Inconsistent)?
            .await
            .map_err(|_| russh::Error::Disconnect)?
        {
            panic!("a native key-exchange task panics");
        }
        Ok(true)
    }
}
struct Peer;
impl russh::server::Handler for Peer {
    type Error = russh::Error;
}
fn key() -> russh::keys::PrivateKey {
    use russh::keys::ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey};
    russh::keys::PrivateKey::from(Ed25519Keypair::from(Ed25519PrivateKey::from_bytes(
        &[73; 32],
    )))
}

#[tokio::test]
async fn a_pre_handle_owner_reads_cancelled_and_panicked_native_completion()
-> Result<(), Box<dyn std::error::Error>> {
    // One anti-hang budget; oneshots establish key exchange and native destruction.
    tokio::time::timeout(Duration::from_secs(60), async {
        for mode in [0, 1, 2] {
            let (client, peer) = tokio::io::duplex(65536);
            let server = tokio::spawn(async move {
                let config = russh::server::Config { keys: vec![key()], ..Default::default() };
                russh::server::run_stream(Arc::new(config), peer, Peer).await?.await
            });
            let (entered, ready) = tokio::sync::oneshot::channel();
            let (released, mut destroyed) = tokio::sync::oneshot::channel();
            let (proceed, permit) = tokio::sync::oneshot::channel();
            // This value is available before the task reaches the held key check.
            let mut owner = russh::client::connect_stream_owned(
                Arc::new(Default::default()), client,
                KeyCheck { entered: Some(entered), released: Some(released), proceed: Some(permit) },
            ).await?;
            {
                let establishment = owner.establish();
                tokio::pin!(establishment);
                tokio::select! {
                    _ = ready => {},
                    result = &mut establishment => return Err(std::io::Error::other(format!("key check unexpectedly ended: {result:?}")).into()),
                }
            }
            assert!(matches!(destroyed.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
            if mode != 0 {
                proceed.send(true).map_err(|_| std::io::Error::other("key-check waiter departed"))?;
                if mode == 1 {
                    assert!(matches!(owner.await, Err(russh::Error::Join(_))));
                } else {
                    assert!(matches!(owner.establish().await, Err(russh::Error::Join(_))));
                    assert!(owner.is_joined());
                    owner.stop();
                }
            } else {
                owner.stop();
                owner.stop();
                owner.await?;
                drop(proceed);
            }
            // Reading the native completion establishes destruction, without a later wait.
            destroyed.try_recv()?;
            let _ = server.await?;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }).await?
}

#[tokio::test]
async fn a_successful_key_exchange_transfers_the_same_owned_task_to_its_handle()
-> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(60), async {
        let (client, peer) = tokio::io::duplex(65536);
        let server = tokio::spawn(async move {
            let config = russh::server::Config {
                keys: vec![key()],
                ..Default::default()
            };
            russh::server::run_stream(Arc::new(config), peer, Peer)
                .await?
                .await
        });
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (released, mut destroyed) = tokio::sync::oneshot::channel();
        let (proceed, permit) = tokio::sync::oneshot::channel();
        let mut owner = russh::client::connect_stream_owned(
            Arc::new(Default::default()),
            client,
            KeyCheck {
                entered: Some(entered),
                released: Some(released),
                proceed: Some(permit),
            },
        )
        .await?;
        ready.await?;
        proceed
            .send(false)
            .map_err(|_| std::io::Error::other("key-check waiter departed"))?;
        owner.establish().await?;
        let handle = owner.into_handle()?;
        handle.stop();
        handle.await?;
        destroyed.try_recv()?;
        let _ = server.await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await?
}
