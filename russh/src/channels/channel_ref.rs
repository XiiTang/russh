use tokio::sync::mpsc::{
    Sender,
    error::{SendError, TrySendError},
};

use super::SendCredit;
use crate::ChannelMsg;

/// A handle to the [`super::Channel`]'s receive queue and send-window credit.
///
/// Enqueue through [`Self::send`] or [`Self::try_send`]. The underlying Sender
/// is private, so an inbox-backed channel cannot bypass inbox admission with
/// a raw send, cloned Sender, or reserved permit.
#[derive(Debug)]
pub struct ChannelRef {
    pub(crate) client_open: Option<crate::client::PendingChannel>,
    pub(crate) inbox: Option<std::sync::Arc<super::inbox::Inbox>>,
    sender: Sender<ChannelMsg>,
    pub(super) window_size: SendCredit,
    pub(crate) managed_close: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    pub(crate) handles: Option<std::sync::Arc<super::handles::Handles>>,
}

impl Drop for ChannelRef {
    fn drop(&mut self) {
        if let Some(inbox) = &self.inbox {
            inbox.close_sender();
        }
    }
}
impl ChannelRef {
    /// Send to the channel's reader, applying client inbox admission before push.
    /// Channels without an inbox keep their bounded mpsc backpressure.
    pub async fn send(&self, message: ChannelMsg) -> Result<(), SendError<ChannelMsg>> {
        match &self.inbox {
            Some(inbox) => inbox.send(message),
            None => self.sender.send(message).await,
        }
    }

    /// Send without waiting, through the same client inbox admission as `send`.
    /// An exhausted inbox is closed; its reader receives the typed terminal
    /// cause. Channels without an inbox retain mpsc's Full/Closed errors.
    pub fn try_send(&self, message: ChannelMsg) -> Result<(), TrySendError<ChannelMsg>> {
        match &self.inbox {
            Some(inbox) => inbox
                .send(message)
                .map_err(|error| TrySendError::Closed(error.0)),
            None => self.sender.try_send(message),
        }
    }

    pub fn new(sender: Sender<ChannelMsg>) -> Self {
        Self::with_window_size(sender, SendCredit::new(0))
    }

    pub(super) fn with_window_size(sender: Sender<ChannelMsg>, window_size: SendCredit) -> Self {
        Self {
            client_open: None,
            sender,
            window_size,
            managed_close: None,
            inbox: None,
            handles: None,
        }
    }

    pub(crate) fn window_size(&self) -> &SendCredit {
        &self.window_size
    }
}
