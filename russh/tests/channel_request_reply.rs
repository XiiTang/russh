#![cfg(not(target_arch = "wasm32"))]
//! The handler hears a channel request's reply before the channel's reader
//! can read its mirror.
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use russh::{ChannelId, ChannelMsg, client, server};
use tokio::sync::Notify;

/// Records each reply it is told of, once the test lets it.
struct Hook {
    entered: Arc<Notify>,
    resume: Arc<Notify>,
    told: Arc<AtomicUsize>,
}
impl Hook {
    async fn told(&self) {
        self.entered.notify_one();
        self.resume.notified().await;
        self.told.fetch_add(1, Ordering::SeqCst);
    }
}
impl client::Handler for Hook {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        _: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
    async fn channel_success(
        &mut self,
        _: ChannelId,
        _: &mut client::Session,
    ) -> Result<(), Self::Error> {
        self.told().await;
        Ok(())
    }
    async fn channel_failure(
        &mut self,
        _: ChannelId,
        _: &mut client::Session,
    ) -> Result<(), Self::Error> {
        self.told().await;
        Ok(())
    }
}
/// Answers `deny` with CHANNEL_FAILURE and every other command with
/// CHANNEL_SUCCESS.
struct Peer;
impl server::Handler for Peer {
    type Error = russh::Error;
    async fn auth_none(&mut self, _: &str) -> Result<server::Auth, Self::Error> {
        Ok(server::Auth::Accept)
    }
    async fn channel_open_session(
        &mut self,
        _: russh::Channel<server::Msg>,
        reply: server::ChannelOpenHandle,
        _: &mut server::Session,
    ) -> Result<(), Self::Error> {
        reply.accept().await?;
        Ok(())
    }
    async fn exec_request(
        &mut self,
        id: ChannelId,
        data: &[u8],
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        match data {
            b"deny" => session.channel_failure(id)?,
            _ => session.channel_success(id)?,
        }
        Ok(())
    }
}
fn key() -> russh::keys::PrivateKey {
    use russh::keys::ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey};
    russh::keys::PrivateKey::from(Ed25519Keypair::from(Ed25519PrivateKey::from_bytes(
        &[71; 32],
    )))
}

#[tokio::test]
async fn a_request_reply_reaches_the_handler_before_the_channel_reader() {
    // Only a hang budget; the hook's entry and the test's resume order it.
    tokio::time::timeout(Duration::from_secs(30), async {
        let (client, peer) = tokio::io::duplex(65536);
        let server = tokio::spawn(async move {
            let config = server::Config {
                keys: vec![key()],
                ..Default::default()
            };
            server::run_stream(Arc::new(config), peer, Peer)
                .await?
                .await
        });
        let entered = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let told = Arc::new(AtomicUsize::new(0));
        let mut handle = client::connect_stream(
            Arc::new(Default::default()),
            client,
            Hook {
                entered: entered.clone(),
                resume: resume.clone(),
                told: told.clone(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            handle.authenticate_none("user").await.unwrap(),
            client::AuthResult::Success
        ));
        let mut channel = handle.channel_open_session().await.unwrap();
        for (command, reply) in [("run", true), ("deny", false)] {
            channel.exec(true, command).await.unwrap();
            entered.notified().await;
            // The handler holds the reply: its mirror is not queued yet.
            assert!(
                futures::poll!(Box::pin(channel.wait())).is_pending(),
                "the reader read the reply before the handler was told of it"
            );
            resume.notify_one();
            let message = channel.wait().await;
            assert!(
                matches!(
                    (&message, reply),
                    (Some(ChannelMsg::Success), true) | (Some(ChannelMsg::Failure), false)
                ),
                "{message:?}"
            );
            assert_eq!(told.load(Ordering::SeqCst), usize::from(!reply) + 1);
        }
        // The server's session ends with the client's.
        drop((channel, handle));
        let _ = server.await.unwrap();
    })
    .await
    .unwrap();
}
