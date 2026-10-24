//! Native reply ownership, including replies committed before a waiter leaves.
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};

/// An abandoned native reply's owner. Called synchronously, once per untaken
/// reply, on the session task or on the task dropping the waiter. It must not
/// block or panic; enqueue asynchronous cleanup in an owner that outlives the
/// waiter. The callback receives ownership of the reply, including any channel.
///
/// Authentication calls wait for an earlier call's native reply (or session end)
/// before submitting another attempt. Dropping the waiting future adds no timer
/// and does not stop the session. A continuation still needs its normal response.
pub type CompletionSink<T> = Arc<dyn Fn(T) + Send + Sync + 'static>;

// The envelope owns the disposition until take(). In particular, an mpsc send
// admitted before close() can still be in flight when the receiver drains; the
// channel dropping that message must dispose of it too.
struct OwnedReply<T> {
    value: Option<T>,
    sink: Option<CompletionSink<T>>,
}
impl<T> OwnedReply<T> {
    fn new(value: T, sink: Option<CompletionSink<T>>) -> Self {
        Self {
            value: Some(value),
            sink,
        }
    }
    fn take(mut self) -> Option<T> {
        self.value.take()
    }
}
impl<T> Drop for OwnedReply<T> {
    fn drop(&mut self) {
        if let (Some(value), Some(sink)) = (self.value.take(), self.sink.take()) {
            sink(value);
        }
    }
}

enum CompletionTx<T> {
    Plain(oneshot::Sender<T>),
    Guarded(oneshot::Sender<OwnedReply<T>>),
}
/// The sending half of a native completion. Constructed by the client APIs.
#[doc(hidden)]
pub struct CompletionSender<T> {
    sender: CompletionTx<T>,
    sink: Option<CompletionSink<T>>,
}
impl<T> std::fmt::Debug for CompletionSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletionSender").finish_non_exhaustive()
    }
}
impl<T> From<oneshot::Sender<T>> for CompletionSender<T> {
    fn from(sender: oneshot::Sender<T>) -> Self {
        Self {
            sender: CompletionTx::Plain(sender),
            sink: None,
        }
    }
}
impl<T> CompletionSender<T> {
    pub(crate) fn send(self, value: T) -> Result<(), ()> {
        match self.sender {
            CompletionTx::Plain(sender) => sender.send(value).map_err(|_| ()),
            CompletionTx::Guarded(sender) => {
                sender.send(OwnedReply::new(value, self.sink)).map_err(drop)
            }
        }
    }
}
pub(crate) struct CompletionReceiver<T> {
    receiver: oneshot::Receiver<OwnedReply<T>>,
}
pub(crate) fn channel<T>(
    sink: Option<CompletionSink<T>>,
) -> (CompletionSender<T>, CompletionReceiver<T>) {
    let (sender, receiver) = oneshot::channel();
    (
        CompletionSender {
            sender: CompletionTx::Guarded(sender),
            sink,
        },
        CompletionReceiver { receiver },
    )
}
impl<T> CompletionReceiver<T> {
    pub(crate) async fn recv(&mut self) -> Result<T, crate::Error> {
        (&mut self.receiver)
            .await
            .map_err(|_| crate::Error::Disconnect)?
            .take()
            .ok_or(crate::Error::Inconsistent)
    }
}
impl<T> Drop for CompletionReceiver<T> {
    fn drop(&mut self) {
        self.receiver.close();
        // Closing first makes either this take or the native sender own the reply.
        drop(self.receiver.try_recv());
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthState {
    Pending,
    Complete,
    Ended,
}
struct AuthAttempt<T> {
    state: watch::Sender<AuthState>,
    finished: fn(&T) -> bool,
    admitted: bool,
}
enum AuthTx<T> {
    Plain(mpsc::UnboundedSender<T>),
    Guarded(mpsc::UnboundedSender<OwnedReply<T>>),
}
/// The sending half of a native authentication reply stream.
#[doc(hidden)]
pub struct AuthReplySender<T> {
    sender: AuthTx<T>,
    sink: Option<CompletionSink<T>>,
    attempt: Option<AuthAttempt<T>>,
}
impl<T> std::fmt::Debug for AuthReplySender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthReplySender").finish_non_exhaustive()
    }
}
impl<T> From<mpsc::UnboundedSender<T>> for AuthReplySender<T> {
    fn from(sender: mpsc::UnboundedSender<T>) -> Self {
        Self {
            sender: AuthTx::Plain(sender),
            sink: None,
            attempt: None,
        }
    }
}
impl<T> AuthReplySender<T> {
    pub(crate) fn track(
        mut self,
        state: watch::Sender<AuthState>,
        finished: fn(&T) -> bool,
    ) -> Self {
        self.attempt = Some(AuthAttempt {
            state,
            finished,
            admitted: false,
        });
        self
    }
    pub(crate) fn admit(&mut self) {
        if let Some(attempt) = &mut self.attempt {
            attempt.admitted = true;
        }
    }
    pub(crate) fn send(&mut self, value: T) -> Result<(), ()> {
        let finished = self
            .attempt
            .as_ref()
            .is_some_and(|attempt| (attempt.finished)(&value));
        let sink = if finished {
            self.sink.take()
        } else {
            self.sink.clone()
        };
        let result = match &self.sender {
            AuthTx::Plain(sender) => sender.send(value).map_err(|_| ()),
            AuthTx::Guarded(sender) => {
                // An abandoned waiter is not a session failure, with or without
                // a configured sink. A failed send drops the owned envelope.
                drop(sender.send(OwnedReply::new(value, sink)));
                Ok(())
            }
        };
        if finished && let Some(attempt) = self.attempt.take() {
            attempt.state.send_replace(AuthState::Complete);
        }
        result
    }
}
impl<T> Drop for AuthReplySender<T> {
    fn drop(&mut self) {
        if let Some(attempt) = self.attempt.take() {
            // Cancellation before queue admission has no native attempt to wait
            // for. Once admitted, losing its sender means the session ended.
            attempt.state.send_replace(if attempt.admitted {
                AuthState::Ended
            } else {
                AuthState::Complete
            });
        }
    }
}
pub(crate) struct AuthReplyReceiver<T> {
    receiver: mpsc::UnboundedReceiver<OwnedReply<T>>,
}
pub(crate) fn auth_channel<T>(
    sink: Option<CompletionSink<T>>,
) -> (AuthReplySender<T>, AuthReplyReceiver<T>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (
        AuthReplySender {
            sender: AuthTx::Guarded(sender),
            sink,
            attempt: None,
        },
        AuthReplyReceiver { receiver },
    )
}
impl<T> AuthReplyReceiver<T> {
    pub(crate) async fn recv(&mut self) -> Option<T> {
        self.receiver.recv().await.and_then(OwnedReply::take)
    }
}
impl<T> Drop for AuthReplyReceiver<T> {
    fn drop(&mut self) {
        self.receiver.close();
        while let Ok(reply) = self.receiver.try_recv() {
            drop(reply);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)] // Test assertions and the taken-reply trap must fail on unexpected native values.
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[tokio::test]
    async fn both_completion_races_dispose_once_and_a_taken_reply_never_does() {
        for sent_first in [false, true] {
            let count = Arc::new(AtomicUsize::new(0));
            let seen = count.clone();
            let (sender, receiver) = channel(Some(Arc::new(move |v| {
                assert_eq!(v, 7);
                seen.fetch_add(1, Ordering::SeqCst);
            })));
            if sent_first {
                sender.send(7).unwrap();
                drop(receiver);
            } else {
                drop(receiver);
                assert!(sender.send(7).is_err());
            }
            assert_eq!(count.load(Ordering::SeqCst), 1);
        }
        let (sender, mut receiver) = channel(Some(Arc::new(|_: u8| panic!("reply taken"))));
        sender.send(7).unwrap();
        assert_eq!(receiver.recv().await.unwrap(), 7);
        drop(receiver);
    }
    #[tokio::test]
    async fn auth_stream_closes_then_takes_before_late_sends_use_its_sink() {
        let count = Arc::new(AtomicUsize::new(0));
        let seen = count.clone();
        let (mut sender, mut receiver) = auth_channel(Some(Arc::new(move |v| {
            seen.fetch_add(v, Ordering::SeqCst);
        })));
        sender.send(1usize).unwrap();
        assert_eq!(receiver.recv().await, Some(1));
        sender.send(2).unwrap();
        drop(receiver);
        sender.send(4).unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 6);
    }
}

#[cfg(test)]
mod attempt_tests {
    #![allow(clippy::unwrap_used)] // Fixture channels and native-state assertions must fail on unexpected closure.
    use super::*;

    #[tokio::test]
    async fn only_a_native_end_ends_the_wait_and_a_final_reply_releases_its_sink() {
        for admitted in [false, true] {
            let (state, mut pending) = watch::channel(AuthState::Pending);
            let (mut sender, _receiver) = auth_channel::<u8>(None);
            sender = sender.track(state, |_| true);
            if admitted {
                sender.admit();
            }
            drop(sender);
            pending.changed().await.unwrap();
            assert!(
                *pending.borrow()
                    == if admitted {
                        AuthState::Ended
                    } else {
                        AuthState::Complete
                    }
            );
        }
        let sink: CompletionSink<u8> = Arc::new(|_| {});
        let (state, pending) = watch::channel(AuthState::Pending);
        let (sender, mut receiver) = auth_channel(Some(sink.clone()));
        let mut sender = sender.track(state, |v| *v == 2);
        sender.admit();
        sender.send(1).unwrap();
        assert_eq!(receiver.recv().await, Some(1));
        assert!(*pending.borrow() == AuthState::Pending);
        sender.send(2).unwrap();
        assert!(*pending.borrow() == AuthState::Complete);
        assert_eq!(receiver.recv().await, Some(2));
        assert_eq!(
            Arc::strong_count(&sink),
            1,
            "completed native sender retained its sink"
        );
        drop(sender);
        assert!(*pending.borrow() == AuthState::Complete);
    }
}
