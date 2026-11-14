//! Client receive queues are bounded by the advertised SSH byte window.
//! The connection reader must never await an application channel receiver.
//! A reader that stops reading ends only the inbound direction: data that
//! reaches nobody still counts as consumed, so the peer's window keeps moving.
use crate::ChannelMsg;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
};
use tokio::sync::Notify;

#[derive(Debug)]
struct State {
    queue: VecDeque<ChannelMsg>,
    controls: usize,
    consumed: u32,
    closed: bool,
    reader_closed: bool,
    overflowed: bool,
    overflow_reported: bool,
    waker: Option<Waker>,
}
#[derive(Debug)]
pub(crate) struct Inbox {
    state: Mutex<State>,
    wake_session: Arc<Notify>,
    control_limit: usize,
}
impl Inbox {
    #[track_caller]
    fn state(&self) -> MutexGuard<'_, State> {
        let state = self.state.lock();
        if let Err(error) = &state {
            assert!(
                state.is_ok(),
                "called `Result::unwrap()` on an `Err` value: {error:?}"
            );
        }
        state.unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn new(wake_session: Arc<Notify>, control_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                controls: 0,
                consumed: 0,
                closed: false,
                reader_closed: false,
                overflowed: false,
                overflow_reported: false,
                waker: None,
            }),
            wake_session,
            control_limit: control_limit.max(128),
        })
    }
    pub(crate) fn send(
        &self,
        message: ChannelMsg,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<ChannelMsg>> {
        let mut state = self.state();
        // What the inbox refuses reaches nobody and still counts as consumed:
        // an overflowed channel stays registered until the engine closes it.
        if state.reader_closed || state.overflowed || state.closed {
            state.consumed += bytes(&message);
            return Err(tokio::sync::mpsc::error::SendError(message));
        }
        if bytes(&message) == 0 {
            // Empty data carries no useful stream content and consumes no SSH credit.
            if matches!(
                message,
                ChannelMsg::Data { .. } | ChannelMsg::ExtendedData { .. }
            ) {
                return Ok(());
            }
            if state.controls == self.control_limit {
                // Upstream drops a full-buffer informational notification.
                // Credit was already granted and its writer already woken.
                if matches!(message, ChannelMsg::WindowAdjusted { .. }) {
                    return Err(tokio::sync::mpsc::error::SendError(message));
                }
                state.overflowed = true;
                self.wake_session.notify_one();
                if let Some(waker) = state.waker.take() {
                    waker.wake();
                }
                return Err(tokio::sync::mpsc::error::SendError(message));
            }
            state.controls += 1;
        }
        state.queue.push_back(message);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
        Ok(())
    }
    pub(crate) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<ChannelMsg>> {
        let mut state = self.state();
        if let Some(message) = state.queue.pop_front() {
            let size = bytes(&message);
            if size == 0 {
                state.controls -= 1;
            } else {
                state.consumed += size;
                self.wake_session.notify_one();
            }
            return Poll::Ready(Some(message));
        }
        if state.overflowed && !state.overflow_reported {
            state.overflow_reported = true;
            return Poll::Ready(Some(ChannelMsg::InboxOverflow(crate::ChannelInboxOverflow {
                maximum: self.control_limit,
            })));
        }
        if state.closed || state.reader_closed || state.overflowed {
            return Poll::Ready(None);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
    pub(crate) fn take_consumed(&self) -> u32 {
        std::mem::take(&mut self.state().consumed)
    }
    /// Whether the peer queued more controls than the reader takes; the
    /// engine then closes the channel.
    pub(crate) fn overflowed(&self) -> bool {
        self.state().overflowed
    }
    /// Ends inbound delivery: what is queued and what arrives later counts as
    /// consumed.
    pub(crate) fn close_reader(&self) {
        let mut state = self.state();
        state.reader_closed = true;
        let queued: u32 = state.queue.drain(..).map(|message| bytes(&message)).sum();
        state.consumed += queued;
        state.controls = 0;
        self.wake_session.notify_one();
    }
    pub(crate) fn close_sender(&self) {
        let mut state = self.state();
        state.closed = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}
fn bytes(message: &ChannelMsg) -> u32 {
    match message {
        ChannelMsg::Data { data } | ChannelMsg::ExtendedData { data, .. } => data.len() as u32,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
    use std::task::Wake;

    #[test]
    fn required_control_overflow_reports_the_typed_tail_once() {
        let inbox = Inbox::new(Arc::new(Notify::new()), 1);
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..128 { assert!(inbox.send(ChannelMsg::Success).is_ok()); }
        // Upstream's optional notification refusal keeps all credit/callback
        // behavior and does not end the native channel.
        assert!(inbox.send(ChannelMsg::WindowAdjusted { new_size: 42 }).is_err());
        assert!(!inbox.overflowed());
        assert!(inbox.send(ChannelMsg::Failure).is_err());
        inbox.close_sender();
        assert!(inbox.send(ChannelMsg::Close).is_err());
        for _ in 0..128 { assert!(matches!(inbox.poll_recv(&mut cx), Poll::Ready(Some(ChannelMsg::Success)))); }
        assert!(matches!(inbox.poll_recv(&mut cx), Poll::Ready(Some(ChannelMsg::InboxOverflow(crate::ChannelInboxOverflow { maximum: 128 })))));
        assert!(matches!(inbox.poll_recv(&mut cx), Poll::Ready(None)));
        assert!(matches!(inbox.poll_recv(&mut cx), Poll::Ready(None)));
    }

    struct PanickingWake;

    impl Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            // Inject a caller's panic while send holds the inbox lock.
            resume_unwind(Box::new("reader waker panicked"));
        }
    }

    #[test]
    fn every_inbox_operation_preserves_the_poisoned_lock_panic() {
        // The raw-packet fixture temporarily captures every process-wide panic.
        // Keep these deliberate panics out of its observation window.
        let _panic_hook_guard = crate::tests::raw_no_crypto::PANIC_HOOK_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let inbox = Inbox::new(Arc::new(Notify::new()), 128);
        let waker = Waker::from(Arc::new(PanickingWake));
        let mut cx = Context::from_waker(&waker);
        assert!(inbox.poll_recv(&mut cx).is_pending());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ = inbox.send(ChannelMsg::Data {
                    data: bytes::Bytes::from_static(b"queued"),
                });
            }))
            .is_err()
        );
        assert!(inbox.state.is_poisoned());

        let operations: [fn(&Inbox); 6] = [
            |inbox| {
                let _ = inbox.send(ChannelMsg::Eof);
            },
            |inbox| {
                let mut cx = Context::from_waker(Waker::noop());
                let _ = inbox.poll_recv(&mut cx);
            },
            |inbox| {
                let _ = inbox.take_consumed();
            },
            |inbox| {
                let _ = inbox.overflowed();
            },
            Inbox::close_reader,
            Inbox::close_sender,
        ];
        for operation in operations {
            let result = catch_unwind(AssertUnwindSafe(|| operation(&inbox)));
            assert!(result.is_err(), "an operation recovered a poisoned inbox");
            if let Err(payload) = result {
                assert_eq!(
                    payload.downcast_ref::<String>().map(String::as_str),
                    Some("called `Result::unwrap()` on an `Err` value: PoisonError { .. }")
                );
            }
        }

        // No operation may observe or mutate the potentially damaged state.
        let state = inbox
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(state.queue.len(), 1);
        assert_eq!(state.controls, 0);
        assert_eq!(state.consumed, 0);
        assert!(!state.closed);
        assert!(!state.reader_closed);
        assert!(!state.overflowed);
    }
}
