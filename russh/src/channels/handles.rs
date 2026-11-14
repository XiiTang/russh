//! Which of a channel's handles the program still holds. Letting a channel go
//! ends it gracefully: the engine closes it on the program's behalf only once
//! both its reader and every writer are gone, after the messages they queued.
//! A client closes every channel let go of. A server closes only one turned
//! into a stream: a server program may drop a channel and keep writing to it
//! through its session by id, as upstream allows.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::Notify;

#[derive(Debug)]
pub(crate) struct Handles {
    reader_released: AtomicBool,
    writer_released: AtomicBool,
    streamed: AtomicBool,
    wake: Arc<Notify>,
    retained: std::sync::OnceLock<Arc<dyn std::fmt::Debug + Send + Sync>>,
}

impl Handles {
    /// The engine's view of a new channel, its reader's hold and the hold its
    /// write half shares with every writer made from it.
    pub(crate) fn new(wake: Arc<Notify>) -> (Arc<Self>, ReaderHold, Arc<WriterHold>) {
        let handles = Arc::new(Self {
            reader_released: AtomicBool::new(false),
            writer_released: AtomicBool::new(false),
            streamed: AtomicBool::new(false),
            wake,
            retained: std::sync::OnceLock::new(),
        });
        (
            handles.clone(),
            ReaderHold(handles.clone()),
            Arc::new(WriterHold(handles)),
        )
    }

    /// Attach an external owner once; every native and application hold shares it.
    pub(crate) fn retain(&self, owner: Arc<dyn std::fmt::Debug + Send + Sync>) {
        assert!(self.retained.set(owner).is_ok(), "a channel owner is fixed once");
    }

    /// Whether the program let both directions go. A hold is released after
    /// everything its owner queued, so the engine sees those messages too.
    pub(crate) fn released(&self) -> bool {
        self.reader_released.load(Ordering::Acquire)
            && self.writer_released.load(Ordering::Acquire)
    }

    /// Whether the channel was turned into a stream, whose letting go closes
    /// it on a server too.
    pub(crate) fn streamed(&self) -> bool {
        self.streamed.load(Ordering::Acquire)
    }

    fn release(&self, side: &AtomicBool) {
        side.store(true, Ordering::Release);
        self.wake.notify_one();
    }
}

/// Held by a channel's read half.
#[derive(Debug)]
pub(crate) struct ReaderHold(Arc<Handles>);

impl Drop for ReaderHold {
    fn drop(&mut self) {
        self.0.release(&self.0.reader_released);
    }
}

/// Shared by a channel's write half and every writer made from it.
#[derive(Debug)]
pub(crate) struct WriterHold(Arc<Handles>);

impl WriterHold {
    pub(crate) fn retain(&self, owner: Arc<dyn std::fmt::Debug + Send + Sync>) {
        self.0.retain(owner);
    }

    pub(crate) fn mark_streamed(&self) {
        self.0.streamed.store(true, Ordering::Release);
    }
}

impl Drop for WriterHold {
    fn drop(&mut self) {
        self.0.release(&self.0.writer_released);
    }
}
