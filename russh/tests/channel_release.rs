//! Letting a channel go ends it gracefully: what its handles queued leaves
//! before CLOSE, and a reader-side event ends only the inbound direction.
use russh::{ChannelMsg, server};
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn key() -> russh::keys::PrivateKey {
    use russh::keys::ssh_key::private::{Ed25519Keypair, Ed25519PrivateKey};
    russh::keys::PrivateKey::from(Ed25519Keypair::from(Ed25519PrivateKey::from_bytes(&[69; 32])))
}

#[derive(Debug)]
enum Seen {
    Eof(russh::ChannelId),
    Close(russh::ChannelId),
}

struct Peer {
    events: tokio::sync::mpsc::UnboundedSender<Seen>,
    channels: tokio::sync::mpsc::UnboundedSender<russh::Channel<server::Msg>>,
    /// Holds the server's engine inside its first data callback, so the
    /// window adjustment that data earns stays unsent until it opens.
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
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
        let _ = self.channels.send(channel);
        reply.accept().await?;
        Ok(())
    }
    async fn data(&mut self, _: russh::ChannelId, _: &[u8], _: &mut server::Session) -> Result<(), Self::Error> {
        if let Some(gate) = self.gate.take() {
            let _ = gate.await;
        }
        Ok(())
    }
    async fn channel_eof(&mut self, id: russh::ChannelId, _: &mut server::Session) -> Result<(), Self::Error> {
        let _ = self.events.send(Seen::Eof(id));
        Ok(())
    }
    async fn channel_close(&mut self, id: russh::ChannelId, _: &mut server::Session) -> Result<(), Self::Error> {
        let _ = self.events.send(Seen::Close(id));
        Ok(())
    }
}
struct Trusted;
impl russh::client::Handler for Trusted {
    type Error = russh::Error;
    async fn check_server_key(&mut self, _: &russh::keys::PublicKeyOrCertificate) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

struct Pair {
    client: russh::client::Handle<Trusted>,
    seen: tokio::sync::mpsc::UnboundedReceiver<Seen>,
    opened: tokio::sync::mpsc::UnboundedReceiver<russh::Channel<server::Msg>>,
}

async fn pair(
    server_window: u32,
    client: russh::client::Config,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
) -> Pair {
    let (left, right) = tokio::io::duplex(65536);
    let (events, seen) = tokio::sync::mpsc::unbounded_channel();
    let (channels, opened) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let config = server::Config {
            keys: vec![key()],
            window_size: server_window,
            auth_rejection_time: Duration::ZERO,
            ..Default::default()
        };
        let _ = server::run_stream(Arc::new(config), right, Peer { events, channels, gate })
            .await
            .unwrap()
            .await;
    });
    let mut client = russh::client::connect_stream(Arc::new(client), left, Trusted).await.unwrap();
    assert!(client.authenticate_none("fixture").await.unwrap().success());
    Pair { client, seen, opened }
}

async fn next(seen: &mut tokio::sync::mpsc::UnboundedReceiver<Seen>) -> Seen {
    tokio::time::timeout(Duration::from_secs(5), seen.recv()).await.unwrap().unwrap()
}

/// Everything until the peer's CLOSE, or the end of its messages.
async fn read_until_close(channel: &mut russh::Channel<server::Msg>) -> Vec<u8> {
    let mut received = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), channel.wait()).await.unwrap() {
            Some(ChannelMsg::Data { data }) => received.extend_from_slice(&data),
            Some(ChannelMsg::Close) | None => return received,
            _ => {}
        }
    }
}

// The check point: a channel let go of closes only after the messages it
// queued, even while a sibling's zero window keeps the engine from taking more.
#[tokio::test]
async fn queued_messages_leave_before_the_close_of_a_released_channel() {
    let Pair { client, mut seen, opened: _opened } = pair(0, Default::default(), None).await;
    let blocked = client.channel_open_session().await.unwrap();
    let released = client.channel_open_session().await.unwrap();
    let id = released.id();
    client.data(blocked.id(), bytes::Bytes::from_static(b"held by a zero window")).await.unwrap();
    released.eof().await.unwrap();
    drop(released);
    let mut order = Vec::new();
    loop {
        match next(&mut seen).await {
            Seen::Eof(c) if c == id => order.push("eof"),
            Seen::Close(c) if c == id => {
                order.push("close");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(order, ["eof", "close"]);
}

// Data waiting for the peer's window when a stream is let go is sent once the
// window opens, and CLOSE follows it.
#[tokio::test]
async fn a_window_blocked_stream_let_go_sends_its_pending_data_then_closes() {
    let (open, gate) = tokio::sync::oneshot::channel();
    let Pair { client, mut opened, .. } = pair(8, Default::default(), Some(gate)).await;
    let channel = client.channel_open_session().await.unwrap();
    let id = channel.id();
    let mut server_side = opened.recv().await.unwrap();
    let mut stream = channel.into_stream();
    stream.write_all(b"window!!").await.unwrap();
    client.data(id, bytes::Bytes::from_static(b" and the rest")).await.unwrap();
    drop(stream);
    open.send(()).unwrap();
    assert_eq!(read_until_close(&mut server_side).await, b"window!! and the rest");
}

// Reading the peer's EOF ends only its direction: this side still writes.
#[tokio::test]
async fn a_stream_still_writes_after_reading_the_peers_eof() {
    let Pair { client, mut opened, .. } = pair(2097152, Default::default(), None).await;
    let channel = client.channel_open_session().await.unwrap();
    let mut server_side = opened.recv().await.unwrap();
    let mut stream = channel.into_stream();
    server_side.data(&b"done"[..]).await.unwrap();
    server_side.eof().await.unwrap();
    let mut got = Vec::new();
    stream.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, b"done");
    // A round trip through the engine after the reader saw EOF.
    let _sibling = client.channel_open_session().await.unwrap();
    stream.write_all(b"after eof").await.unwrap();
    drop(stream);
    assert_eq!(read_until_close(&mut server_side).await, b"after eof");
}

// Letting only the read half go keeps the write half working, and what the
// peer sends meanwhile still earns it window.
#[tokio::test]
async fn letting_the_read_half_go_keeps_the_write_half_and_credits_the_peer() {
    let config = russh::client::Config { window_size: 64, maximum_packet_size: 32, ..Default::default() };
    let Pair { client, mut opened, .. } = pair(2097152, config, None).await;
    let channel = client.channel_open_session().await.unwrap();
    let mut server_side = opened.recv().await.unwrap();
    let (reader, writer) = channel.split();
    drop(reader);
    tokio::time::timeout(Duration::from_secs(5), server_side.data(&[7u8; 1024][..]))
        .await
        .unwrap()
        .unwrap();
    writer.data(&b"still writing"[..]).await.unwrap();
    drop(writer);
    assert_eq!(read_until_close(&mut server_side).await, b"still writing");
}

// The server's engine closes a stream let go of, after the data it queued.
#[tokio::test]
async fn a_server_stream_let_go_closes_after_its_queued_data() {
    let Pair { client, mut opened, .. } = pair(2097152, Default::default(), None).await;
    let mut channel = client.channel_open_session().await.unwrap();
    let mut server_side = opened.recv().await.unwrap().into_stream();
    server_side.write_all(b"bye").await.unwrap();
    drop(server_side);
    let mut received = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), channel.wait()).await.unwrap() {
            Some(ChannelMsg::Data { data }) => received.extend_from_slice(&data),
            Some(ChannelMsg::Close) => break,
            Some(_) => {}
            None => panic!("the channel ended without CLOSE"),
        }
    }
    assert_eq!(received, b"bye");
}
