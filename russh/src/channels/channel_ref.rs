use tokio::sync::mpsc::Sender;

use super::SendCredit;
use crate::ChannelMsg;

/// A handle to the [`super::Channel`]'s to be able to transmit messages
/// to it and update it's `window_size`.
#[derive(Debug)]
pub struct ChannelRef {
    pub(crate) inbox: Option<std::sync::Arc<super::inbox::Inbox>>,
    pub(super) sender: Sender<ChannelMsg>,
    pub(super) window_size: SendCredit,
    pub(crate) managed_close: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    pub(crate) handles: Option<std::sync::Arc<super::handles::Handles>>,
}

impl Drop for ChannelRef {
    fn drop(&mut self) { if let Some(inbox) = &self.inbox { inbox.close_sender(); } }
}
impl ChannelRef {
    pub(crate) async fn send(&self, message: ChannelMsg) -> Result<(), tokio::sync::mpsc::error::SendError<ChannelMsg>> {
        match &self.inbox { Some(inbox) => inbox.send(message), None => self.sender.send(message).await }
    }
    pub(crate) fn try_send(&self, message: ChannelMsg) -> Result<(), tokio::sync::mpsc::error::TrySendError<ChannelMsg>> {
        match &self.inbox {
            Some(inbox) => inbox.send(message).map_err(|error| tokio::sync::mpsc::error::TrySendError::Closed(error.0)),
            None => self.sender.try_send(message),
        }
    }
    pub fn new(sender: Sender<ChannelMsg>) -> Self {
        Self {
            sender,
            window_size: SendCredit::new(0),
            managed_close: None,
            inbox: None,
            handles: None,
        }
    }

    pub(crate) fn window_size(&self) -> &SendCredit {
        &self.window_size
    }
}

impl std::ops::Deref for ChannelRef {
    type Target = Sender<ChannelMsg>;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}
