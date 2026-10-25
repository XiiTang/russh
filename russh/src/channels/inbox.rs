//! Nonblocking client receive queues: the SSH window bounds DATA bytes;
//! explicit storage and entry limits also bound controls and tiny DATA entries.
use crate::{ChannelInboxLimits, ChannelMsg, ChannelOpenFailure, InboxLimitExceeded, Sig};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
};
use tokio::sync::Notify;

#[derive(Debug)]
struct Queued {
    message: ChannelMsg,
    control_bytes: usize,
}

#[derive(Debug)]
enum Terminal {
    Close,
    Limit(InboxLimitExceeded),
}

#[derive(Debug)]
struct State {
    queue: VecDeque<Queued>,
    control_bytes: usize,
    consumed: u32,
    closed: bool,
    reader_closed: bool,
    // Retained after delivering the one terminal message, also for AsyncRead.
    failure: Option<InboxLimitExceeded>,
    terminal: Option<Terminal>,
    waker: Option<Waker>,
}
#[derive(Debug)]
pub(crate) struct Inbox {
    state: Mutex<State>,
    wake_session: Arc<Notify>,
    limits: ChannelInboxLimits,
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

    // Keep the existing fixture constructor, including its historical minimum.
    #[cfg(test)]
    pub(crate) fn new(wake_session: Arc<Notify>, control_limit: usize) -> Arc<Self> {
        Self::with_limits(
            wake_session,
            ChannelInboxLimits {
                max_entries: control_limit.max(128),
                ..Default::default()
            },
        )
    }

    pub(crate) fn with_limits(wake_session: Arc<Notify>, limits: ChannelInboxLimits) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                control_bytes: 0,
                consumed: 0,
                closed: false,
                reader_closed: false,
                failure: None,
                terminal: None,
                waker: None,
            }),
            wake_session,
            limits,
        })
    }
    pub(crate) fn send(
        &self,
        message: ChannelMsg,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<ChannelMsg>> {
        let mut state = self.state();
        if state.reader_closed || state.closed {
            state.consumed += bytes(&message);
            return Err(tokio::sync::mpsc::error::SendError(message));
        }
        if matches!(&message, ChannelMsg::Data { data } | ChannelMsg::ExtendedData { data, .. } if data.is_empty())
        {
            return Ok(());
        }
        // Reserve the end independently of queue allocation. Peer CLOSE also
        // stays last and cannot itself be refused by a full ordinary queue.
        let terminal = match &message {
            ChannelMsg::Close => Some(Terminal::Close),
            ChannelMsg::InboxLimitExceeded(cause) => {
                state.failure = Some(*cause);
                Some(Terminal::Limit(*cause))
            }
            _ => None,
        };
        if let Some(terminal) = terminal {
            state.closed = true;
            state.terminal = Some(terminal);
            self.wake_session.notify_one();
        } else {
            let admission = if state.queue.len() >= self.limits.max_entries {
                Err(InboxLimitExceeded::Entries {
                    limit: self.limits.max_entries,
                })
            } else {
                control_bytes(&message)
                    .and_then(|charge| {
                        state
                            .control_bytes
                            .checked_add(charge)
                            .map(|total| (charge, total))
                    })
                    .filter(|(_, total)| *total <= self.limits.max_control_bytes)
                    .ok_or(InboxLimitExceeded::ControlBytes {
                        limit: self.limits.max_control_bytes,
                    })
            };
            match admission {
                Ok((charge, total)) => {
                    state.queue.push_back(Queued {
                        message,
                        control_bytes: charge,
                    });
                    state.control_bytes = total;
                }
                Err(cause) => {
                    state.failure = Some(cause);
                    state.terminal = Some(Terminal::Limit(cause));
                    state.closed = true;
                    state.consumed += bytes(&message);
                    self.wake_session.notify_one();
                    if let Some(waker) = state.waker.take() {
                        waker.wake();
                    }
                    return Err(tokio::sync::mpsc::error::SendError(message));
                }
            }
        }
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
        Ok(())
    }
    pub(crate) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<ChannelMsg>> {
        let mut state = self.state();
        if let Some(queued) = state.queue.pop_front() {
            state.control_bytes -= queued.control_bytes;
            let size = bytes(&queued.message);
            if size > 0 {
                state.consumed += size;
                self.wake_session.notify_one();
            }
            return Poll::Ready(Some(queued.message));
        }
        if let Some(terminal) = state.terminal.take() {
            return Poll::Ready(Some(match terminal {
                Terminal::Close => ChannelMsg::Close,
                Terminal::Limit(cause) => ChannelMsg::InboxLimitExceeded(cause),
            }));
        }
        if state.closed || state.reader_closed {
            return Poll::Ready(None);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
    pub(crate) fn take_consumed(&self) -> u32 {
        std::mem::take(&mut self.state().consumed)
    }
    pub(crate) fn overflowed(&self) -> bool {
        self.failure().is_some()
    }
    pub(crate) fn failure(&self) -> Option<InboxLimitExceeded> {
        self.state().failure
    }
    /// Discarded DATA still counts as consumed, as before; closing a reader
    /// never reopens a failed queue or erases its terminal cause.
    pub(crate) fn close_reader(&self) {
        let mut state = self.state();
        state.reader_closed = true;
        let queued: u32 = state
            .queue
            .drain(..)
            .map(|queued| bytes(&queued.message))
            .sum();
        state.consumed += queued;
        state.control_bytes = 0;
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
fn signal_bytes(signal: &Sig) -> usize {
    match signal {
        Sig::Custom(name) => name.capacity(),
        Sig::ABRT
        | Sig::ALRM
        | Sig::FPE
        | Sig::HUP
        | Sig::ILL
        | Sig::INT
        | Sig::KILL
        | Sig::PIPE
        | Sig::QUIT
        | Sig::SEGV
        | Sig::TERM
        | Sig::USR1 => 0,
    }
}
// Exhaustive matches keep new control payloads from silently bypassing charging.
// DATA payload bytes have the independent SSH receive-window bound.
fn control_bytes(message: &ChannelMsg) -> Option<usize> {
    Some(match message {
        ChannelMsg::RequestPty {
            term,
            terminal_modes,
            ..
        } => term.capacity().checked_add(
            terminal_modes
                .capacity()
                .checked_mul(std::mem::size_of::<(crate::Pty, u32)>())?,
        )?,
        ChannelMsg::Exec { command, .. } => command.capacity(),
        ChannelMsg::Signal { signal } => signal_bytes(signal),
        ChannelMsg::RequestSubsystem { name, .. } => name.capacity(),
        ChannelMsg::RequestX11 {
            x11_authentication_protocol,
            x11_authentication_cookie,
            ..
        } => x11_authentication_protocol
            .capacity()
            .checked_add(x11_authentication_cookie.capacity())?,
        ChannelMsg::SetEnv {
            variable_name,
            variable_value,
            ..
        } => variable_name
            .capacity()
            .checked_add(variable_value.capacity())?,
        ChannelMsg::ExitSignal {
            signal_name,
            error_message,
            lang_tag,
            ..
        } => signal_bytes(signal_name)
            .checked_add(error_message.capacity())?
            .checked_add(lang_tag.capacity())?,
        ChannelMsg::OpenFailure(reason) => match reason {
            ChannelOpenFailure::Other { reason, .. } => reason.capacity(),
            ChannelOpenFailure::AdministrativelyProhibited
            | ChannelOpenFailure::ConnectFailed
            | ChannelOpenFailure::UnknownChannelType
            | ChannelOpenFailure::ResourceShortage => 0,
        },
        ChannelMsg::HandleData { data, .. } => data.len(),
        ChannelMsg::Open { .. }
        | ChannelMsg::Data { .. }
        | ChannelMsg::ExtendedData { .. }
        | ChannelMsg::Eof
        | ChannelMsg::Close
        | ChannelMsg::InboxLimitExceeded(_)
        | ChannelMsg::RequestShell { .. }
        | ChannelMsg::WindowChange { .. }
        | ChannelMsg::AgentForward { .. }
        | ChannelMsg::XonXoff { .. }
        | ChannelMsg::ExitStatus { .. }
        | ChannelMsg::WindowAdjusted { .. }
        | ChannelMsg::Success
        | ChannelMsg::Failure => 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
    use std::task::Wake;

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

        let operations: [fn(&Inbox); 7] = [
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
            |inbox| {
                let _ = inbox.failure();
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
        assert_eq!(state.control_bytes, 0);
        assert_eq!(state.consumed, 0);
        assert!(!state.closed);
        assert!(!state.reader_closed);
        assert!(state.failure.is_none());
    }
    #[test]
    fn w17c_large_queued_controls_hit_the_byte_limit_before_growth() {
        let inbox = Inbox::new(Arc::new(Notify::new()), 128);
        let control = || ChannelMsg::ExitSignal {
            signal_name: crate::Sig::TERM,
            core_dumped: false,
            error_message: "x".repeat(600 * 1024),
            lang_tag: String::new(),
        };
        assert!(inbox.send(control()).is_ok());
        assert!(
            inbox.send(control()).is_err(),
            "two controls retained more than 1 MiB"
        );
    }

    #[test]
    fn w17c_tiny_data_hits_the_entry_limit_before_growth() {
        let inbox = Inbox::new(Arc::new(Notify::new()), 128);
        for _ in 0..128 {
            assert!(
                inbox
                    .send(ChannelMsg::Data {
                        data: bytes::Bytes::from_static(b"x")
                    })
                    .is_ok()
            );
        }
        assert!(
            inbox
                .send(ChannelMsg::Data {
                    data: bytes::Bytes::from_static(b"x")
                })
                .is_err(),
            "129 tiny DATA entries bypassed the structural limit"
        );
    }

    fn poll(inbox: &Inbox) -> Poll<Option<ChannelMsg>> {
        inbox.poll_recv(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn w17c_terminal_follows_the_prefix_once_even_after_native_sender_drop() {
        let inbox = Inbox::with_limits(
            Arc::new(Notify::new()),
            ChannelInboxLimits {
                max_entries: 2,
                max_control_bytes: 0,
            },
        );
        assert!(
            inbox
                .send(ChannelMsg::Data {
                    data: bytes::Bytes::from_static(b"a")
                })
                .is_ok()
        );
        assert!(
            inbox
                .send(ChannelMsg::ExtendedData {
                    data: bytes::Bytes::from_static(b"b"),
                    ext: 1
                })
                .is_ok()
        );
        assert!(inbox.send(ChannelMsg::Eof).is_err());
        assert!(inbox.send(ChannelMsg::Close).is_err());
        assert!(
            inbox
                .send(ChannelMsg::Data {
                    data: bytes::Bytes::from_static(b"discarded")
                })
                .is_err()
        );
        inbox.close_sender();
        assert!(
            matches!(poll(&inbox), Poll::Ready(Some(ChannelMsg::Data { data })) if data.as_ref() == b"a")
        );
        assert!(
            matches!(poll(&inbox), Poll::Ready(Some(ChannelMsg::ExtendedData { data, ext: 1 })) if data.as_ref() == b"b")
        );
        assert!(matches!(
            poll(&inbox),
            Poll::Ready(Some(ChannelMsg::InboxLimitExceeded(
                InboxLimitExceeded::Entries { limit: 2 }
            )))
        ));
        assert!(matches!(poll(&inbox), Poll::Ready(None)));
        assert_eq!(inbox.take_consumed(), 11);
    }

    #[test]
    fn w17c_peer_close_uses_its_reserved_last_slot_without_exhaustion() {
        let inbox = Inbox::with_limits(
            Arc::new(Notify::new()),
            ChannelInboxLimits {
                max_entries: 1,
                max_control_bytes: 0,
            },
        );
        assert!(inbox.send(ChannelMsg::Eof).is_ok());
        assert!(inbox.send(ChannelMsg::Close).is_ok());
        assert!(inbox.send(ChannelMsg::Success).is_err());
        assert!(matches!(poll(&inbox), Poll::Ready(Some(ChannelMsg::Eof))));
        assert!(matches!(poll(&inbox), Poll::Ready(Some(ChannelMsg::Close))));
        assert!(matches!(poll(&inbox), Poll::Ready(None)));
        assert_eq!(inbox.failure(), None);
    }

    #[test]
    fn w17c_control_capacity_is_charged_and_refunded_on_dequeue() {
        let inbox = Inbox::with_limits(
            Arc::new(Notify::new()),
            ChannelInboxLimits {
                max_entries: 10,
                max_control_bytes: 8,
            },
        );
        // An empty String can still retain storage: length-only accounting
        // would let these controls pass an arbitrarily small byte limit.
        let control = || ChannelMsg::ExitSignal {
            signal_name: Sig::TERM,
            core_dumped: false,
            error_message: String::with_capacity(8),
            lang_tag: String::new(),
        };
        assert!(inbox.send(control()).is_ok());
        assert!(matches!(
            poll(&inbox),
            Poll::Ready(Some(ChannelMsg::ExitSignal { .. }))
        ));
        assert!(inbox.send(control()).is_ok());
        assert!(inbox.send(control()).is_err());
        assert_eq!(inbox.state().control_bytes, 8);
        assert_eq!(inbox.state().queue.len(), 1);
        assert!(matches!(
            poll(&inbox),
            Poll::Ready(Some(ChannelMsg::ExitSignal { .. }))
        ));
        assert_eq!(inbox.state().control_bytes, 0);
        assert!(matches!(
            poll(&inbox),
            Poll::Ready(Some(ChannelMsg::InboxLimitExceeded(
                InboxLimitExceeded::ControlBytes { limit: 8 }
            )))
        ));
    }

    #[test]
    fn w17c_zero_limits_are_exact_and_empty_data_uses_no_entry() {
        let inbox = Inbox::with_limits(
            Arc::new(Notify::new()),
            ChannelInboxLimits {
                max_entries: 0,
                max_control_bytes: 0,
            },
        );
        assert!(
            inbox
                .send(ChannelMsg::Data {
                    data: bytes::Bytes::new()
                })
                .is_ok()
        );
        assert!(
            inbox
                .send(ChannelMsg::ExtendedData {
                    data: bytes::Bytes::new(),
                    ext: 1
                })
                .is_ok()
        );
        assert!(poll(&inbox).is_pending());
        assert!(inbox.send(ChannelMsg::Success).is_err());
        assert!(matches!(
            poll(&inbox),
            Poll::Ready(Some(ChannelMsg::InboxLimitExceeded(
                InboxLimitExceeded::Entries { limit: 0 }
            )))
        ));
        assert!(matches!(poll(&inbox), Poll::Ready(None)));
    }

    #[tokio::test]
    async fn w17c_stream_readers_keep_the_typed_end_including_a_later_adapter() {
        use tokio::io::AsyncReadExt;
        let inbox = Inbox::with_limits(
            Arc::new(Notify::new()),
            ChannelInboxLimits {
                max_entries: 1,
                max_control_bytes: 0,
            },
        );
        assert!(
            inbox
                .send(ChannelMsg::Data {
                    data: bytes::Bytes::from_static(b"x")
                })
                .is_ok()
        );
        assert!(inbox.send(ChannelMsg::Success).is_err());
        inbox.close_sender();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(sender);
        let mut reader = crate::ChannelReadHalf {
            receiver,
            inbox: Some(inbox),
            _hold: None,
        };
        let mut buffer = [0];
        assert!(matches!(
            reader.make_reader().read(&mut buffer).await,
            Ok(1)
        ));
        assert_eq!(buffer, [b'x']);
        for _ in 0..2 {
            let result = reader.make_reader().read(&mut buffer).await;
            assert!(result.is_err());
            if let Err(error) = result {
                assert_eq!(
                    error
                        .get_ref()
                        .and_then(|cause| cause.downcast_ref::<InboxLimitExceeded>()),
                    Some(&InboxLimitExceeded::Entries { limit: 1 })
                );
            }
        }
    }
}
