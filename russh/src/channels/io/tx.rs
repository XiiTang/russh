use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use tokio::io::AsyncWrite;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{self, OwnedPermit};

use bytes::Bytes;

use super::ChannelMsg;
use crate::channels::SendCredit;
use crate::ChannelId;

type BoxedThreadsafeFuture<T> = Pin<Box<dyn Sync + Send + std::future::Future<Output = T>>>;
type OwnedPermitFuture<S> = BoxedThreadsafeFuture<Result<OwnedPermit<S>, SendError<()>>>;

/// A channel's `AsyncWrite`. A write first waits for credit, then reserves
/// room in the session queue, as tokio-util's `PollSender` reserves before
/// it sends, and builds its message from the buffer it is called with only
/// once both are there: a `Pending` write has taken no credit and queued
/// nothing, so a write retried with another buffer, a shutdown, or dropping
/// the writer loses nothing.
pub struct ChannelTx<S> {
    sender: mpsc::Sender<S>,
    id: ChannelId,
    credit: SendCredit,
    ready: Option<BoxedThreadsafeFuture<()>>,
    reserve: Option<OwnedPermitFuture<S>>,
    max_packet_size: u32,
    ext: Option<u32>,
    _writer: Option<Arc<crate::channels::handles::WriterHold>>,
}

impl<S> ChannelTx<S>
where
    S: From<(ChannelId, ChannelMsg)> + 'static + Send,
{
    pub(crate) fn new(
        sender: mpsc::Sender<S>,
        id: ChannelId,
        credit: SendCredit,
        max_packet_size: u32,
        ext: Option<u32>,
    ) -> Self {
        Self {
            sender,
            id,
            credit,
            ready: None,
            reserve: None,
            max_packet_size,
            ext,
            _writer: None,
        }
    }

    /// Keeps the channel's write direction held while this writer lives.
    pub(crate) fn holding(mut self, writer: Option<Arc<crate::channels::handles::WriterHold>>) -> Self {
        self._writer = writer;
        self
    }

    fn poll_permit(&mut self, cx: &mut Context<'_>) -> Poll<Result<OwnedPermit<S>, io::Error>> {
        let sender = &self.sender;
        let reserve = self
            .reserve
            .get_or_insert_with(|| Box::pin(sender.clone().reserve_owned()));
        let reserved = ready!(reserve.as_mut().poll(cx));
        self.reserve = None;
        Poll::Ready(
            reserved.map_err(|SendError(())| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed")),
        )
    }
}

impl<S> AsyncWrite for ChannelTx<S>
where
    S: From<(ChannelId, ChannelMsg)> + 'static + Send,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if buf.is_empty() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "cannot send empty buffer",
            )));
        }
        if self.max_packet_size == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the peer's maximum packet size is zero",
            )));
        }
        let this = &mut *self;
        loop {
            // Credit first: a writer waiting for the peer's window holds no
            // slot of the queue every channel of the session shares.
            if this.reserve.is_none() {
                let credit = this.credit.clone();
                let ready = this
                    .ready
                    .get_or_insert_with(|| Box::pin(async move { credit.ready().await }));
                ready!(ready.as_mut().poll(cx));
                this.ready = None;
            }
            let permit = ready!(this.poll_permit(cx))?;
            let (id, ext) = (this.id, this.ext);
            let admitted = this.credit.admit(buf.len(), this.max_packet_size, |len| {
                #[allow(clippy::indexing_slicing)] // `admit` passes at most `buf.len()`
                let data = Bytes::copy_from_slice(&buf[..len]);
                let msg = match ext {
                    None => ChannelMsg::Data { data },
                    Some(ext) => ChannelMsg::ExtendedData { data, ext },
                };
                permit.send((id, msg).into());
            });
            if admitted > 0 {
                return Poll::Ready(Ok(admitted));
            }
            // Another writer of the channel took the credit: wait for more.
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // EOF takes no credit; a write still waiting for it is abandoned.
        self.ready = None;
        let permit = ready!(self.poll_permit(cx))?;
        permit.send((self.id, ChannelMsg::Eof).into());
        Poll::Ready(Ok(()))
    }
}
