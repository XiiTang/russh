#![cfg(not(target_arch = "wasm32"))]
//! A channel request's reply reaches the channel's reader only where the
//! handler lets it through, as the default does.
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use russh::{ChannelId, ChannelMsg, client, server};

/// Hears each reply and keeps it from the reader.
struct Taking {
    heard: Arc<Mutex<Vec<bool>>>,
}
impl client::Handler for Taking {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        _: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
    async fn channel_reply(
        &mut self,
        _: ChannelId,
        success: bool,
        _: &mut client::Session,
    ) -> Result<bool, Self::Error> {
        self.heard.lock().unwrap().push(success);
        Ok(false)
    }
}
/// Keeps the default reply entry: it tells `channel_success` and
/// `channel_failure`.
struct Upstream {
    heard: Arc<Mutex<Vec<bool>>>,
}
impl client::Handler for Upstream {
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
        self.heard.lock().unwrap().push(true);
        Ok(())
    }
    async fn channel_failure(
        &mut self,
        _: ChannelId,
        _: &mut client::Session,
    ) -> Result<(), Self::Error> {
        self.heard.lock().unwrap().push(false);
        Ok(())
    }
}
/// Answers `deny` with CHANNEL_FAILURE and every other command with
/// CHANNEL_SUCCESS, then sends the command back as data.
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
        session.data(id, bytes::Bytes::copy_from_slice(data))?;
        Ok(())
    }
}
fn key() -> russh::keys::PrivateKey {
    use russh::keys::ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey};
    russh::keys::PrivateKey::from(Ed25519Keypair::from(Ed25519PrivateKey::from_bytes(
        &[71; 32],
    )))
}
/// Runs `run` and `deny` on a channel of a client with `handler`, and
/// answers what its reader read, the server joined once the client ends.
async fn read<H: client::Handler<Error = russh::Error> + 'static>(handler: H) -> Vec<ChannelMsg> {
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
    let mut handle = client::connect_stream(Arc::new(Default::default()), client, handler)
        .await
        .unwrap();
    assert!(matches!(
        handle.authenticate_none("user").await.unwrap(),
        client::AuthResult::Success
    ));
    let mut channel = handle.channel_open_session().await.unwrap();
    let mut read = Vec::new();
    for command in ["run", "deny"] {
        channel.exec(true, command).await.unwrap();
        // The command's data follows its reply.
        loop {
            let message = channel.wait().await.unwrap();
            let data = matches!(message, ChannelMsg::Data { .. });
            read.push(message);
            if data {
                break;
            }
        }
    }
    drop((channel, handle));
    let _ = server.await.unwrap();
    read
}

#[tokio::test]
async fn a_reply_the_handler_takes_never_reaches_the_reader() {
    // Only a hang budget; each command's data ends its wait.
    tokio::time::timeout(Duration::from_secs(30), async {
        let heard = Arc::new(Mutex::new(Vec::new()));
        let read = read(Taking {
            heard: heard.clone(),
        })
        .await;
        assert_eq!(*heard.lock().unwrap(), [true, false]);
        assert!(
            matches!(
                read.as_slice(),
                [ChannelMsg::Data { data: run }, ChannelMsg::Data { data: deny }]
                    if &run[..] == b"run" && &deny[..] == b"deny"
            ),
            "{read:?}"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn a_reply_the_default_handler_hears_reaches_the_reader() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let heard = Arc::new(Mutex::new(Vec::new()));
        let read = read(Upstream {
            heard: heard.clone(),
        })
        .await;
        assert_eq!(*heard.lock().unwrap(), [true, false]);
        assert!(
            matches!(
                read.as_slice(),
                [
                    ChannelMsg::Success,
                    ChannelMsg::Data { .. },
                    ChannelMsg::Failure,
                    ChannelMsg::Data { .. }
                ]
            ),
            "{read:?}"
        );
    })
    .await
    .unwrap();
}
