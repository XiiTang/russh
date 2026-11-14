use std::borrow::BorrowMut;
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use tokio::io::AsyncRead;

use super::{ChannelMsg, ChannelReadHalf};

#[derive(Debug)]
pub struct ChannelRx<R> {
    channel: R,
    buffer: Option<(ChannelMsg, usize)>,

    ext: Option<u32>,
}

impl<R> ChannelRx<R> {
    pub fn new(channel: R, ext: Option<u32>) -> Self {
        Self {
            channel,
            buffer: None,
            ext,
        }
    }
}

impl<R> AsyncRead for ChannelRx<R>
where
    R: BorrowMut<ChannelReadHalf> + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let (msg, mut idx) = match self.buffer.take() {
            Some(msg) => msg,
            None => match ready!(self.channel.borrow_mut().poll_recv(cx)) {
                Some(msg) => (msg, 0),
                None => return Poll::Ready(Ok(())),
            },
        };

        match (&msg, self.ext) {
            (ChannelMsg::Data { data }, None) => {
                let readable = buf.remaining().min(data.len() - idx);

                // Clamped to maximum `buf.remaining()` and `data.len() - idx` with `.min`
                #[allow(clippy::indexing_slicing)]
                buf.put_slice(&data[idx..idx + readable]);
                idx += readable;

                if idx != data.len() {
                    self.buffer = Some((msg, idx));
                }

                Poll::Ready(Ok(()))
            }
            (ChannelMsg::ExtendedData { data, ext }, Some(target)) if *ext == target => {
                let readable = buf.remaining().min(data.len() - idx);

                // Clamped to maximum `buf.remaining()` and `data.len() - idx` with `.min`
                #[allow(clippy::indexing_slicing)]
                buf.put_slice(&data[idx..idx + readable]);
                idx += readable;

                if idx != data.len() {
                    self.buffer = Some((msg, idx));
                }

                Poll::Ready(Ok(()))
            }
            (ChannelMsg::InboxOverflow(cause), _) => {
                Poll::Ready(Err(io::Error::other(*cause)))
            }
            (ChannelMsg::Eof, _) => {
                self.channel.borrow_mut().close();

                Poll::Ready(Ok(()))
            }
            _ => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Channel, ChannelId};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn required_overflow_is_typed_after_accepted_stream_data() {
        let (sender, _messages) = tokio::sync::mpsc::channel::<(ChannelId, ChannelMsg)>(1);
        let wake = Arc::new(tokio::sync::Notify::new());
        let (mut channel, _reference) = Channel::new(ChannelId(1), sender, 1024, 1024, 1, wake.clone());
        let inbox = crate::channels::inbox::Inbox::new(wake, 128);
        channel.read_half.inbox = Some(inbox.clone());
        assert!(inbox.send(ChannelMsg::Data { data: bytes::Bytes::from_static(b"prefix") }).is_ok());
        for _ in 0..128 { assert!(inbox.send(ChannelMsg::Success).is_ok()); }
        assert!(inbox.send(ChannelMsg::Failure).is_err());
        inbox.close_sender();
        let mut reader = channel.make_reader();
        let mut data = [0; 6];
        assert!(reader.read_exact(&mut data).await.is_ok());
        assert_eq!(&data, b"prefix");
        let result = reader.read(&mut data).await;
        assert!(matches!(&result, Err(error) if error.get_ref().and_then(|cause| cause.downcast_ref::<crate::ChannelInboxOverflow>()) == Some(&crate::ChannelInboxOverflow { maximum: 128 })));
        assert!(matches!(reader.read(&mut data).await, Ok(0)));
    }
}
