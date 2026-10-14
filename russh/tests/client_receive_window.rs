use russh::{ChannelMsg, server};
use std::{sync::Arc, time::Duration};
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
        id: russh::ChannelId,
        command: &[u8],
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        session.channel_success(id)?;
        if command == b"bulk" {
            let handle = session.handle();
            tokio::spawn(async move {
                for i in 0..128u8 {
                    if handle.data(id, vec![i; 16]).await.is_err() {
                        return;
                    }
                }
                let _ = handle.exit_status_request(id, 23).await;
                let _ = handle.eof(id).await;
                let _ = handle.close(id).await;
            });
        } else {
            session.data(id, b"sibling".as_slice())?;
        }
        Ok(())
    }
}
struct Trusted;
impl russh::client::Handler for Trusted {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        _: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}
#[tokio::test]
async fn unread_channel_keeps_sibling_and_keepalive_live_then_delivers_exact_tail() {
    tokio::time::timeout(Duration::from_secs(10), async {
        use russh::keys::ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey};
        let key = russh::keys::PrivateKey::from(Ed25519Keypair::from(Ed25519PrivateKey::from_bytes(&[19;32])));
        let (left, right) = tokio::io::duplex(65536);
        let server = tokio::spawn(async move {
            let config = server::Config { keys: vec![key], auth_rejection_time: Duration::ZERO, event_buffer_size: 1, ..Default::default() };
            server::run_stream(Arc::new(config), right, Peer).await.unwrap().await
        });
        let config = russh::client::Config { window_size: 64, maximum_packet_size: 32, channel_buffer_size: 1, keepalive_interval: Some(Duration::from_millis(30)), keepalive_max: 3, ..Default::default() };
        let mut client = russh::client::connect_stream(Arc::new(config), left, Trusted).await.unwrap();
        assert!(client.authenticate_none("test").await.unwrap().success());
        let mut bulk = client.channel_open_session().await.unwrap();
        bulk.exec(true, b"bulk".as_slice()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut sibling = client.channel_open_session().await.unwrap();
        sibling.exec(true, b"small".as_slice()).await.unwrap();
        assert!(matches!(sibling.wait().await, Some(ChannelMsg::Success)));
        assert!(matches!(sibling.wait().await, Some(ChannelMsg::Data {data}) if data.as_ref()==b"sibling"));
        assert!(matches!(bulk.wait().await, Some(ChannelMsg::Success)));
        let mut bytes = Vec::new();
        let mut status = None;
        let mut eof = false;
        while let Some(message) = bulk.wait().await {
            match message {
                ChannelMsg::Data {data} => bytes.extend_from_slice(&data),
                ChannelMsg::ExitStatus {exit_status} => status=Some(exit_status),
                ChannelMsg::Eof => eof=true,
                ChannelMsg::Close => break,
                ChannelMsg::WindowAdjusted {..} => {},
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(bytes, (0..128u8).flat_map(|i| vec![i;16]).collect::<Vec<_>>());
        assert_eq!(status, Some(23));
        assert!(eof);
        client.disconnect(russh::Disconnect::ByApplication, "done", "").await.unwrap();
        server.await.unwrap().unwrap();
    }).await.unwrap();
}
