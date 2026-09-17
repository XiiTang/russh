//! Client receive queues are bounded by the advertised SSH byte window.
//! The connection reader must never await an application channel receiver.
use crate::ChannelMsg;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
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
    waker: Option<Waker>,
}
#[derive(Debug)]
pub(crate) struct Inbox {
    state: Mutex<State>,
    wake_session: Arc<Notify>,
    control_limit: usize,
}
impl Inbox {
    pub(crate) fn new(wake_session: Arc<Notify>, control_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                controls: 0,
                consumed: 0,
                closed: false,
                reader_closed: false,
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
        let mut state = self.state.lock().unwrap();
        if state.reader_closed || state.closed {
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
                state.reader_closed = true;
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
        let mut state = self.state.lock().unwrap();
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
        if state.closed || state.reader_closed {
            return Poll::Ready(None);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
    pub(crate) fn take_consumed(&self) -> u32 {
        std::mem::take(&mut self.state.lock().unwrap().consumed)
    }
    pub(crate) fn abandoned(&self) -> bool {
        self.state.lock().unwrap().reader_closed
    }
    pub(crate) fn close_reader(&self) {
        self.state.lock().unwrap().reader_closed = true;
        self.wake_session.notify_one();
    }
    pub(crate) fn close_sender(&self) {
        let mut state = self.state.lock().unwrap();
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
