//! Cancellation closes are consumed by the SSH engine, never detached tasks.
use super::*;
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Debug)]
pub(crate) struct CloseGuard {
    pub flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}
impl CloseGuard {
    pub(crate) fn new(notify: Arc<Notify>) -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            notify,
        }
    }
}
impl Drop for CloseGuard {
    fn drop(&mut self) {
        self.flag.store(true, Ordering::Release);
        self.notify.notify_one();
    }
}

/// A channel whose cancellation is owned by the connection engine. Dropping it
/// or its stream cancels unsent channel data and queues CLOSE even when a peer
/// window or the application message queue is full. Sibling channels stay live.
pub struct ManagedChannel<S: From<(ChannelId, ChannelMsg)> + Send + 'static> {
    pub(crate) channel: Channel<S>,
    pub(crate) guard: CloseGuard,
}
impl<S: From<(ChannelId, ChannelMsg)> + Send + 'static> Deref for ManagedChannel<S> {
    type Target = Channel<S>;
    fn deref(&self) -> &Self::Target {
        &self.channel
    }
}
impl<S: From<(ChannelId, ChannelMsg)> + Send + 'static> DerefMut for ManagedChannel<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.channel
    }
}
impl<S: From<(ChannelId, ChannelMsg)> + Send + Sync + 'static> ManagedChannel<S> {
    pub fn into_stream(self) -> ChannelStream<S> {
        let Self { channel, guard } = self;
        channel.stream_with_close(Some(guard))
    }
}
