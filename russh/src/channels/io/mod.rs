mod rx;
use std::borrow::{Borrow, BorrowMut};

pub use rx::ChannelRx;

mod tx;
pub use tx::ChannelTx;

use crate::{Channel, ChannelId, ChannelMsg, ChannelReadHalf};

/// The channel a [`super::ChannelStream`] owns: its reader reads through it,
/// and it carries a managed channel's [`super::managed::CloseGuard`]. Dropping
/// it does nothing itself: a managed channel is cancelled by its guard, and any
/// other channel is closed gracefully by its engine once both halves are gone.
#[derive(Debug)]
pub struct ChannelCloseOnDrop<S: From<(ChannelId, ChannelMsg)> + Send + 'static>(
    pub Channel<S>,
    // Held for its drop, which cancels a managed channel.
    #[allow(dead_code)] pub(crate) Option<super::managed::CloseGuard>,
);

impl<S: From<(ChannelId, ChannelMsg)> + Send + 'static> Borrow<ChannelReadHalf>
    for ChannelCloseOnDrop<S>
{
    fn borrow(&self) -> &ChannelReadHalf {
        &self.0.read_half
    }
}

impl<S: From<(ChannelId, ChannelMsg)> + Send + 'static> BorrowMut<ChannelReadHalf>
    for ChannelCloseOnDrop<S>
{
    fn borrow_mut(&mut self) -> &mut ChannelReadHalf {
        &mut self.0.read_half
    }
}
