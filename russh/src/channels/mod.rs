pub(crate) mod handles;
pub(crate) mod inbox;
use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Notify;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{ChannelId, ChannelOpenFailure, Error, Pty, Sig};

pub mod io;

mod channel_ref;
pub use channel_ref::ChannelRef;

mod channel_stream;
pub use channel_stream::ChannelStream;
pub(crate) mod managed;
pub use managed::ManagedChannel;

/// Limits for one client's queued inbound channel messages.
/// DATA bytes remain governed by the SSH window and packet limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelInboxLimits {
    /// Retained heap storage of queued controls, including String/Vec capacity.
    pub max_control_bytes: usize,
    /// Queued ordinary messages, including each nonempty DATA/EXTENDED_DATA.
    /// The final Close or limit-exceeded message has a separate fixed slot.
    pub max_entries: usize,
}

impl Default for ChannelInboxLimits {
    fn default() -> Self {
        Self {
            max_control_bytes: 1024 * 1024,
            max_entries: 128,
        }
    }
}

/// Why one channel's inbound queue ended. The session and siblings stay live.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InboxLimitExceeded {
    /// Queued controls would exceed their retained-storage limit.
    #[error("channel inbox control storage exceeded {limit} bytes")]
    ControlBytes { limit: usize },
    /// Another ordinary message would exceed the queue's entry limit.
    #[error("channel inbox exceeded {limit} entries")]
    Entries { limit: usize },
}

#[derive(Debug)]
#[non_exhaustive]
/// Possible messages that [Channel::wait] can receive.
pub enum ChannelMsg {
    Open {
        id: ChannelId,
        max_packet_size: u32,
        window_size: u32,
    },
    Data {
        data: Bytes,
    },
    ExtendedData {
        data: Bytes,
        ext: u32,
    },
    /// Data a session handle sends outside the channel's writers, which the
    /// engine charges to their credit when it takes it.
    #[doc(hidden)]
    HandleData {
        data: Bytes,
        ext: Option<u32>,
    },
    Eof,
    Close,
    /// Client receive-only terminal message, after all accepted messages.
    /// Nothing follows it, as with Close. The engine cancels the native channel
    /// immediately; observing this message does not drive that cancellation.
    InboxLimitExceeded(InboxLimitExceeded),
    /// (client only)
    RequestPty {
        want_reply: bool,
        term: String,
        col_width: u32,
        row_height: u32,
        pix_width: u32,
        pix_height: u32,
        terminal_modes: Vec<(Pty, u32)>,
    },
    /// (client only)
    RequestShell {
        want_reply: bool,
    },
    /// (client only)
    Exec {
        want_reply: bool,
        command: Vec<u8>,
    },
    /// (client only)
    Signal {
        signal: Sig,
    },
    /// (client only)
    RequestSubsystem {
        want_reply: bool,
        name: String,
    },
    /// (client only)
    RequestX11 {
        want_reply: bool,
        single_connection: bool,
        x11_authentication_protocol: String,
        x11_authentication_cookie: String,
        x11_screen_number: u32,
    },
    /// (client only)
    SetEnv {
        want_reply: bool,
        variable_name: String,
        variable_value: String,
    },
    /// (client only)
    WindowChange {
        col_width: u32,
        row_height: u32,
        pix_width: u32,
        pix_height: u32,
    },
    /// (client only)
    AgentForward {
        want_reply: bool,
    },

    /// (server only)
    XonXoff {
        client_can_do: bool,
    },
    /// (server only)
    ExitStatus {
        exit_status: u32,
    },
    /// (server only)
    ExitSignal {
        signal_name: Sig,
        core_dumped: bool,
        error_message: String,
        lang_tag: String,
    },
    /// (server only)
    WindowAdjusted {
        new_size: u32,
    },
    /// (server only)
    Success,
    /// (server only)
    Failure,
    OpenFailure(ChannelOpenFailure),
}

/// The credit a channel's writers may still hand to the session: the peer's
/// window, less what was admitted to the session queue and not yet sent.
///
/// It moves only where its owner changes: [`SendCredit::admit`] debits a
/// writer's chunk in the same synchronous step that its queue permit sends
/// it, [`SendCredit::charge`] debits data handed to the engine outside the
/// writers, and [`SendCredit::grant`] credits what the peer's window grows by. A
/// write cut short before its permit sends keeps nothing, and the engine,
/// which sends no byte beyond the window it keeps itself, never overwrites
/// it: credit it already debited for queued data is never counted twice.
#[derive(Clone, Debug)]
pub(crate) struct SendCredit {
    // Below zero when the engine took data outside the writers beyond it.
    value: Arc<std::sync::Mutex<i64>>,
    notifier: Arc<Notify>,
}

impl SendCredit {
    pub(crate) fn new(initial: u32) -> Self {
        Self {
            value: Arc::new(std::sync::Mutex::new(i64::from(initial))),
            notifier: Arc::new(Notify::new()),
        }
    }

    fn value(&self) -> std::sync::MutexGuard<'_, i64> {
        self.value
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Credits what the peer's window grew by.
    pub(crate) fn grant(&self, amount: u32) {
        {
            let mut value = self.value();
            *value = value
                .saturating_add(i64::from(amount))
                .min(i64::from(u32::MAX));
        }
        self.notifier.notify_waiters();
    }

    /// Debits data handed to the engine without a writer's admission. On a
    /// channel already closing the engine discards it; that channel sends
    /// nothing more, so its credit is never read again.
    pub(crate) fn charge(&self, bytes: usize) {
        let mut value = self.value();
        *value = value.saturating_sub(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    /// Waits until there is credit to admit, taking none.
    pub(crate) async fn ready(&self) {
        loop {
            let notified = self.notifier.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if *self.value() > 0 {
                return;
            }
            notified.await;
        }
    }

    /// Admits up to `len` bytes, at most `max_packet_size` of them: `send`
    /// hands the admitted length to the session queue through a permit it
    /// already holds, under the same lock that debits it. Returns 0, without
    /// calling `send`, when another writer took the credit first.
    pub(crate) fn admit(
        &self,
        len: usize,
        max_packet_size: u32,
        send: impl FnOnce(usize),
    ) -> usize {
        let mut value = self.value();
        let available = usize::try_from((*value).max(0)).unwrap_or(usize::MAX);
        let admitted = available.min(max_packet_size as usize).min(len);
        if admitted > 0 {
            *value -= admitted as i64;
            send(admitted);
        }
        admitted
    }

    fn writable(&self, max_packet_size: u32) -> usize {
        let available = usize::try_from((*self.value()).max(0)).unwrap_or(usize::MAX);
        available.min(max_packet_size as usize)
    }
}

/// A handle to the reading part of a session channel.
///
/// Allows you to read from a channel without borrowing the session
pub struct ChannelReadHalf {
    pub(crate) receiver: Receiver<ChannelMsg>,
    pub(crate) inbox: Option<Arc<inbox::Inbox>>,
    pub(crate) _hold: Option<handles::ReaderHold>,
}

impl std::fmt::Debug for ChannelReadHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelReadHalf").finish()
    }
}

impl Drop for ChannelReadHalf {
    fn drop(&mut self) {
        if let Some(inbox) = &self.inbox {
            inbox.close_reader();
        }
    }
}

impl ChannelReadHalf {
    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<ChannelMsg>> {
        match &self.inbox {
            Some(inbox) => inbox.poll_recv(cx),
            None => self.receiver.poll_recv(cx),
        }
    }
    pub(crate) fn close(&mut self) {
        if let Some(inbox) = &self.inbox {
            inbox.close_reader();
        }
        self.receiver.close();
    }
    /// Awaits an incoming [`ChannelMsg`], returning [`None`] after the channel ends.
    /// An exhausted client inbox drains its accepted prefix, then yields one
    /// [`ChannelMsg::InboxLimitExceeded`] before `None`, just as peer Close is
    /// delivered before `None`. Native cancellation does not wait for a read.
    pub async fn wait(&mut self) -> Option<ChannelMsg> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Make a reader for the [`Channel`] to receive [`ChannelMsg::Data`]
    /// through the `AsyncRead` trait. An exhausted client inbox returns an
    /// `io::Error` containing [`InboxLimitExceeded`], including on later reads.
    pub fn make_reader(&mut self) -> impl AsyncRead + '_ {
        self.make_reader_ext(None)
    }

    /// Make a reader for the [`Channel`] to receive [`ChannelMsg::Data`] or [`ChannelMsg::ExtendedData`]
    /// depending on the `ext` parameter, through the `AsyncRead` trait.
    pub fn make_reader_ext(&mut self, ext: Option<u32>) -> impl AsyncRead + '_ {
        io::ChannelRx::new(self, ext)
    }
}

/// A handle to the writing part of a session channel.
///
/// Allows you to write to a channel without borrowing the session
pub struct ChannelWriteHalf<Send: From<(ChannelId, ChannelMsg)>> {
    pub(crate) id: ChannelId,
    pub(crate) sender: Sender<Send>,
    pub(crate) max_packet_size: u32,
    pub(crate) window_size: SendCredit,
    pub(crate) hold: Option<Arc<handles::WriterHold>>,
}

impl<S: From<(ChannelId, ChannelMsg)>> std::fmt::Debug for ChannelWriteHalf<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelWriteHalf")
            .field("id", &self.id)
            .finish()
    }
}

impl<S: From<(ChannelId, ChannelMsg)> + Send + Sync + 'static> ChannelWriteHalf<S> {
    /// Returns the min between the maximum packet size and the
    /// remaining window size in the channel.
    pub async fn writable_packet_size(&self) -> usize {
        self.window_size.writable(self.max_packet_size)
    }

    pub fn id(&self) -> ChannelId {
        self.id
    }

    /// Request a pseudo-terminal with the given characteristics.
    #[allow(clippy::too_many_arguments)] // length checked
    pub async fn request_pty(
        &self,
        want_reply: bool,
        term: &str,
        col_width: u32,
        row_height: u32,
        pix_width: u32,
        pix_height: u32,
        terminal_modes: &[(Pty, u32)],
    ) -> Result<(), Error> {
        self.send_msg(ChannelMsg::RequestPty {
            want_reply,
            term: term.to_string(),
            col_width,
            row_height,
            pix_width,
            pix_height,
            terminal_modes: terminal_modes.to_vec(),
        })
        .await
    }

    /// Request a remote shell.
    pub async fn request_shell(&self, want_reply: bool) -> Result<(), Error> {
        self.send_msg(ChannelMsg::RequestShell { want_reply }).await
    }

    /// Execute a remote program (will be passed to a shell). This can
    /// be used to implement scp (by calling a remote scp and
    /// tunneling to its standard input).
    pub async fn exec<A: Into<Vec<u8>>>(&self, want_reply: bool, command: A) -> Result<(), Error> {
        self.send_msg(ChannelMsg::Exec {
            want_reply,
            command: command.into(),
        })
        .await
    }

    /// Signal a remote process.
    pub async fn signal(&self, signal: Sig) -> Result<(), Error> {
        self.send_msg(ChannelMsg::Signal { signal }).await
    }

    /// Request the start of a subsystem with the given name.
    pub async fn request_subsystem<A: Into<String>>(
        &self,
        want_reply: bool,
        name: A,
    ) -> Result<(), Error> {
        self.send_msg(ChannelMsg::RequestSubsystem {
            want_reply,
            name: name.into(),
        })
        .await
    }

    /// Request X11 forwarding through an already opened X11
    /// channel. See
    /// [RFC4254](https://tools.ietf.org/html/rfc4254#section-6.3.1)
    /// for security issues related to cookies.
    pub async fn request_x11<A: Into<String>, B: Into<String>>(
        &self,
        want_reply: bool,
        single_connection: bool,
        x11_authentication_protocol: A,
        x11_authentication_cookie: B,
        x11_screen_number: u32,
    ) -> Result<(), Error> {
        self.send_msg(ChannelMsg::RequestX11 {
            want_reply,
            single_connection,
            x11_authentication_protocol: x11_authentication_protocol.into(),
            x11_authentication_cookie: x11_authentication_cookie.into(),
            x11_screen_number,
        })
        .await
    }

    /// Set a remote environment variable.
    pub async fn set_env<A: Into<String>, B: Into<String>>(
        &self,
        want_reply: bool,
        variable_name: A,
        variable_value: B,
    ) -> Result<(), Error> {
        self.send_msg(ChannelMsg::SetEnv {
            want_reply,
            variable_name: variable_name.into(),
            variable_value: variable_value.into(),
        })
        .await
    }

    /// Inform the server that our window size has changed.
    pub async fn window_change(
        &self,
        col_width: u32,
        row_height: u32,
        pix_width: u32,
        pix_height: u32,
    ) -> Result<(), Error> {
        self.send_msg(ChannelMsg::WindowChange {
            col_width,
            row_height,
            pix_width,
            pix_height,
        })
        .await
    }

    /// Inform the server that we will accept agent forwarding channels
    pub async fn agent_forward(&self, want_reply: bool) -> Result<(), Error> {
        self.send_msg(ChannelMsg::AgentForward { want_reply }).await
    }

    /// Send data to a channel.
    pub async fn data<R: tokio::io::AsyncRead + Unpin>(&self, data: R) -> Result<(), Error> {
        self.send_data(None, data).await
    }

    /// Send owned bytes to a channel without copying them into the `AsyncWrite` path.
    pub async fn data_bytes(&self, data: impl Into<Bytes>) -> Result<(), Error> {
        self.send_bytes(None, data.into()).await
    }

    /// Send data to a channel. The number of bytes added to the
    /// "sending pipeline" (to be processed by the event loop) is
    /// returned.
    pub async fn extended_data<R: tokio::io::AsyncRead + Unpin>(
        &self,
        ext: u32,
        data: R,
    ) -> Result<(), Error> {
        self.send_data(Some(ext), data).await
    }

    /// Send owned extended data to a channel without copying it into the `AsyncWrite` path.
    pub async fn extended_data_bytes(&self, ext: u32, data: impl Into<Bytes>) -> Result<(), Error> {
        self.send_bytes(Some(ext), data.into()).await
    }

    async fn send_data<R: tokio::io::AsyncRead + Unpin>(
        &self,
        ext: Option<u32>,
        mut data: R,
    ) -> Result<(), Error> {
        let mut tx = self.make_writer_ext(ext);

        tokio::io::copy(&mut data, &mut tx).await?;

        Ok(())
    }

    /// Sends `data` chunk by chunk. Each chunk waits for credit, then for room
    /// in the session queue, holding neither while it waits for the other,
    /// and takes its credit only in the step its permit sends it.
    async fn send_bytes(&self, ext: Option<u32>, data: Bytes) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }
        if self.max_packet_size == 0 {
            return Err(Error::Inconsistent);
        }

        let mut offset = 0;
        while offset < data.len() {
            self.window_size.ready().await;
            let permit = self.sender.reserve().await.map_err(|_| Error::SendError)?;
            let start = offset;
            offset += self
                .window_size
                .admit(data.len() - start, self.max_packet_size, |len| {
                    let chunk = data.slice(start..start + len);
                    let msg = match ext {
                        None => ChannelMsg::Data { data: chunk },
                        Some(ext) => ChannelMsg::ExtendedData { data: chunk, ext },
                    };
                    permit.send((self.id, msg).into());
                });
        }

        Ok(())
    }

    pub async fn eof(&self) -> Result<(), Error> {
        self.send_msg(ChannelMsg::Eof).await
    }

    pub async fn exit_status(&self, exit_status: u32) -> Result<(), Error> {
        self.send_msg(ChannelMsg::ExitStatus { exit_status }).await
    }

    /// Request that the channel be closed.
    pub async fn close(&self) -> Result<(), Error> {
        self.send_msg(ChannelMsg::Close).await
    }

    async fn send_msg(&self, msg: ChannelMsg) -> Result<(), Error> {
        self.sender
            .send((self.id, msg).into())
            .await
            .map_err(|_| Error::SendError)
    }

    /// Make a writer for the [`Channel`] to send [`ChannelMsg::Data`]
    /// through the `AsyncWrite` trait.
    pub fn make_writer(&self) -> impl AsyncWrite + 'static {
        self.make_writer_ext(None)
    }

    /// Make a writer for the [`Channel`] to send [`ChannelMsg::Data`] or [`ChannelMsg::ExtendedData`]
    /// depending on the `ext` parameter, through the `AsyncWrite` trait.
    pub fn make_writer_ext(&self, ext: Option<u32>) -> impl AsyncWrite + 'static {
        io::ChannelTx::new(
            self.sender.clone(),
            self.id,
            self.window_size.clone(),
            self.max_packet_size,
            ext,
        )
        .holding(self.hold.clone())
    }
}

/// A handle to a session channel.
///
/// Allows you to read and write from a channel without borrowing the session
pub struct Channel<Send: From<(ChannelId, ChannelMsg)>> {
    pub(crate) read_half: ChannelReadHalf,
    pub(crate) write_half: ChannelWriteHalf<Send>,
}

impl<T: From<(ChannelId, ChannelMsg)>> std::fmt::Debug for Channel<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field("id", &self.write_half.id)
            .finish()
    }
}

impl<S: From<(ChannelId, ChannelMsg)> + Send + Sync + 'static> Channel<S> {
    pub(crate) fn new(
        id: ChannelId,
        sender: Sender<S>,
        max_packet_size: u32,
        window_size: u32,
        channel_buffer_size: usize,
        wake: Arc<Notify>,
    ) -> (Self, ChannelRef) {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_buffer_size);
        let window_size = SendCredit::new(window_size);
        let (handles, reader, writer) = handles::Handles::new(wake);
        let read_half = ChannelReadHalf {
            receiver: rx,
            inbox: None,
            _hold: Some(reader),
        };
        let write_half = ChannelWriteHalf {
            id,
            sender,
            max_packet_size,
            window_size: window_size.clone(),
            hold: Some(writer),
        };

        let mut channel_ref = ChannelRef::with_window_size(tx, window_size);
        channel_ref.handles = Some(handles);
        (
            Self {
                write_half,
                read_half,
            },
            channel_ref,
        )
    }

    /// Returns the min between the maximum packet size and the
    /// remaining window size in the channel.
    pub async fn writable_packet_size(&self) -> usize {
        self.write_half.writable_packet_size().await
    }

    pub fn id(&self) -> ChannelId {
        self.write_half.id()
    }

    /// Split this [`Channel`] into a [`ChannelReadHalf`] and a [`ChannelWriteHalf`], which can be
    /// used to read and write concurrently.
    pub fn split(self) -> (ChannelReadHalf, ChannelWriteHalf<S>) {
        (self.read_half, self.write_half)
    }

    /// Request a pseudo-terminal with the given characteristics.
    #[allow(clippy::too_many_arguments)] // length checked
    pub async fn request_pty(
        &self,
        want_reply: bool,
        term: &str,
        col_width: u32,
        row_height: u32,
        pix_width: u32,
        pix_height: u32,
        terminal_modes: &[(Pty, u32)],
    ) -> Result<(), Error> {
        self.write_half
            .request_pty(
                want_reply,
                term,
                col_width,
                row_height,
                pix_width,
                pix_height,
                terminal_modes,
            )
            .await
    }

    /// Request a remote shell.
    pub async fn request_shell(&self, want_reply: bool) -> Result<(), Error> {
        self.write_half.request_shell(want_reply).await
    }

    /// Execute a remote program (will be passed to a shell). This can
    /// be used to implement scp (by calling a remote scp and
    /// tunneling to its standard input).
    pub async fn exec<A: Into<Vec<u8>>>(&self, want_reply: bool, command: A) -> Result<(), Error> {
        self.write_half.exec(want_reply, command).await
    }

    /// Signal a remote process.
    pub async fn signal(&self, signal: Sig) -> Result<(), Error> {
        self.write_half.signal(signal).await
    }

    /// Request the start of a subsystem with the given name.
    pub async fn request_subsystem<A: Into<String>>(
        &self,
        want_reply: bool,
        name: A,
    ) -> Result<(), Error> {
        self.write_half.request_subsystem(want_reply, name).await
    }

    /// Request X11 forwarding through an already opened X11
    /// channel. See
    /// [RFC4254](https://tools.ietf.org/html/rfc4254#section-6.3.1)
    /// for security issues related to cookies.
    pub async fn request_x11<A: Into<String>, B: Into<String>>(
        &self,
        want_reply: bool,
        single_connection: bool,
        x11_authentication_protocol: A,
        x11_authentication_cookie: B,
        x11_screen_number: u32,
    ) -> Result<(), Error> {
        self.write_half
            .request_x11(
                want_reply,
                single_connection,
                x11_authentication_protocol,
                x11_authentication_cookie,
                x11_screen_number,
            )
            .await
    }

    /// Set a remote environment variable.
    pub async fn set_env<A: Into<String>, B: Into<String>>(
        &self,
        want_reply: bool,
        variable_name: A,
        variable_value: B,
    ) -> Result<(), Error> {
        self.write_half
            .set_env(want_reply, variable_name, variable_value)
            .await
    }

    /// Inform the server that our window size has changed.
    pub async fn window_change(
        &self,
        col_width: u32,
        row_height: u32,
        pix_width: u32,
        pix_height: u32,
    ) -> Result<(), Error> {
        self.write_half
            .window_change(col_width, row_height, pix_width, pix_height)
            .await
    }

    /// Inform the server that we will accept agent forwarding channels
    pub async fn agent_forward(&self, want_reply: bool) -> Result<(), Error> {
        self.write_half.agent_forward(want_reply).await
    }

    /// Send data to a channel.
    pub async fn data<R: tokio::io::AsyncRead + Unpin>(&self, data: R) -> Result<(), Error> {
        self.write_half.data(data).await
    }

    /// Send owned bytes to a channel without copying them into the `AsyncWrite` path.
    pub async fn data_bytes(&self, data: impl Into<Bytes>) -> Result<(), Error> {
        self.write_half.data_bytes(data).await
    }

    /// Send data to a channel. The number of bytes added to the
    /// "sending pipeline" (to be processed by the event loop) is
    /// returned.
    pub async fn extended_data<R: tokio::io::AsyncRead + Unpin>(
        &self,
        ext: u32,
        data: R,
    ) -> Result<(), Error> {
        self.write_half.extended_data(ext, data).await
    }

    /// Send owned extended data to a channel without copying it into the `AsyncWrite` path.
    pub async fn extended_data_bytes(&self, ext: u32, data: impl Into<Bytes>) -> Result<(), Error> {
        self.write_half.extended_data_bytes(ext, data).await
    }

    pub async fn eof(&self) -> Result<(), Error> {
        self.write_half.eof().await
    }

    pub async fn exit_status(&self, exit_status: u32) -> Result<(), Error> {
        self.write_half.exit_status(exit_status).await
    }

    /// Request that the channel be closed.
    pub async fn close(&self) -> Result<(), Error> {
        self.write_half.close().await
    }

    /// Awaits an incoming [`ChannelMsg`], returning [`None`] after the channel ends.
    /// An exhausted client inbox drains its accepted prefix, then yields one
    /// [`ChannelMsg::InboxLimitExceeded`] before `None`, just as peer Close is
    /// delivered before `None`. Native cancellation does not wait for a read.
    pub async fn wait(&mut self) -> Option<ChannelMsg> {
        self.read_half.wait().await
    }

    /// Consume the [`Channel`] to produce a bidirectionnal stream,
    /// sending and receiving [`ChannelMsg::Data`] as `AsyncRead` + `AsyncWrite`.
    pub fn into_stream(self) -> ChannelStream<S> {
        self.stream_with_close(None)
    }
    fn stream_with_close(self, close: Option<managed::CloseGuard>) -> ChannelStream<S> {
        if let Some(hold) = &self.write_half.hold {
            hold.mark_streamed();
        }
        ChannelStream::new(
            io::ChannelTx::new(
                self.write_half.sender.clone(),
                self.write_half.id,
                self.write_half.window_size.clone(),
                self.write_half.max_packet_size,
                None,
            )
            .holding(self.write_half.hold.clone()),
            io::ChannelRx::new(io::ChannelCloseOnDrop(self, close), None),
        )
    }

    /// Make a reader for the [`Channel`] to receive [`ChannelMsg::Data`]
    /// through the `AsyncRead` trait. An exhausted client inbox returns an
    /// `io::Error` containing [`InboxLimitExceeded`], including on later reads.
    pub fn make_reader(&mut self) -> impl AsyncRead + '_ {
        self.read_half.make_reader()
    }

    /// Make a reader for the [`Channel`] to receive [`ChannelMsg::Data`] or [`ChannelMsg::ExtendedData`]
    /// depending on the `ext` parameter, through the `AsyncRead` trait.
    pub fn make_reader_ext(&mut self, ext: Option<u32>) -> impl AsyncRead + '_ {
        self.read_half.make_reader_ext(ext)
    }

    /// Make a writer for the [`Channel`] to send [`ChannelMsg::Data`]
    /// through the `AsyncWrite` trait.
    pub fn make_writer(&self) -> impl AsyncWrite + 'static {
        self.write_half.make_writer()
    }

    /// Make a writer for the [`Channel`] to send [`ChannelMsg::Data`] or [`ChannelMsg::ExtendedData`]
    /// depending on the `ext` parameter, through the `AsyncWrite` trait.
    pub fn make_writer_ext(&self, ext: Option<u32>) -> impl AsyncWrite + 'static {
        self.write_half.make_writer_ext(ext)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)] // Test assertions on fixture replies and bytes must fail on unexpected values.
    use tokio::sync::mpsc;

    use super::*;

    type WriteFixture = (
        ChannelWriteHalf<(ChannelId, ChannelMsg)>,
        mpsc::Receiver<(ChannelId, ChannelMsg)>,
    );

    fn test_write_half(window_size: SendCredit, max_packet_size: u32) -> WriteFixture {
        let (sender, receiver) = mpsc::channel(8);
        (
            ChannelWriteHalf {
                id: ChannelId(7),
                sender,
                max_packet_size,
                window_size,
                hold: None,
            },
            receiver,
        )
    }

    #[tokio::test]
    async fn data_bytes_sends_one_owned_message_when_window_permits() {
        let payload = Bytes::from_static(b"owned data");
        let (write_half, mut receiver) = test_write_half(SendCredit::new(1024), 1024);

        write_half.data_bytes(payload.clone()).await.unwrap();

        match receiver.recv().await.unwrap() {
            (ChannelId(7), ChannelMsg::Data { data }) => {
                assert_eq!(data, payload);
                assert_eq!(data.as_ptr(), payload.as_ptr());
            }
            msg => panic!("unexpected message: {msg:?}"),
        }
    }

    #[tokio::test]
    async fn data_bytes_splits_by_max_packet_size_without_copying() {
        let payload = Bytes::from_static(b"abcdefghij");
        let (write_half, mut receiver) = test_write_half(SendCredit::new(1024), 4);

        write_half.data_bytes(payload.clone()).await.unwrap();

        for (range, expected) in [
            (0..4, &b"abcd"[..]),
            (4..8, &b"efgh"[..]),
            (8..10, &b"ij"[..]),
        ] {
            match receiver.recv().await.unwrap() {
                (ChannelId(7), ChannelMsg::Data { data }) => {
                    assert_eq!(data.as_ref(), expected);
                    assert_eq!(data.as_ptr(), payload.slice(range).as_ptr());
                }
                msg => panic!("unexpected message: {msg:?}"),
            }
        }
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn extended_data_bytes_preserves_extension_code() {
        let payload = Bytes::from_static(b"stderr");
        let (write_half, mut receiver) = test_write_half(SendCredit::new(1024), 1024);

        write_half
            .extended_data_bytes(1, payload.clone())
            .await
            .unwrap();

        match receiver.recv().await.unwrap() {
            (ChannelId(7), ChannelMsg::ExtendedData { data, ext }) => {
                assert_eq!(ext, 1);
                assert_eq!(data, payload);
                assert_eq!(data.as_ptr(), payload.as_ptr());
            }
            msg => panic!("unexpected message: {msg:?}"),
        }
    }

    #[tokio::test]
    async fn data_bytes_empty_payload_sends_nothing() {
        let (write_half, mut receiver) = test_write_half(SendCredit::new(1024), 1024);

        write_half.data_bytes(Bytes::new()).await.unwrap();

        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn data_bytes_waits_for_window_update() {
        let window_size = SendCredit::new(0);
        let (write_half, mut receiver) = test_write_half(window_size.clone(), 1024);
        let send = tokio::spawn(async move {
            write_half
                .data_bytes(Bytes::from_static(b"after-window"))
                .await
                .unwrap();
        });

        tokio::task::yield_now().await;
        assert!(!send.is_finished());

        window_size.grant(1024);
        send.await.unwrap();

        match receiver.recv().await.unwrap() {
            (ChannelId(7), ChannelMsg::Data { data }) => {
                assert_eq!(data.as_ref(), b"after-window");
            }
            msg => panic!("unexpected message: {msg:?}"),
        }
    }

    #[tokio::test]
    async fn data_bytes_rejects_zero_max_packet_size() {
        let (write_half, mut receiver) = test_write_half(SendCredit::new(1024), 0);

        let result = write_half.data_bytes(Bytes::from_static(b"owned")).await;

        assert!(matches!(result, Err(Error::Inconsistent)));
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn channel_data_bytes_forwards_to_write_half() {
        let (sender, mut receiver) = mpsc::channel(8);
        let (channel, _reference) = Channel::<(ChannelId, ChannelMsg)>::new(
            ChannelId(9),
            sender,
            1024,
            1024,
            8,
            Default::default(),
        );

        channel
            .data_bytes(Bytes::from_static(b"channel"))
            .await
            .unwrap();

        match receiver.recv().await.unwrap() {
            (ChannelId(9), ChannelMsg::Data { data }) => {
                assert_eq!(data.as_ref(), b"channel");
            }
            msg => panic!("unexpected message: {msg:?}"),
        }
    }

    /// A write half whose session queue holds one message, already taken by
    /// `blocker`, so the next send waits for room.
    fn blocked_write_half(credit: u32) -> WriteFixture {
        let (sender, receiver) = mpsc::channel(1);
        sender.try_send((ChannelId(1), ChannelMsg::Eof)).unwrap();
        let write_half = ChannelWriteHalf {
            id: ChannelId(7),
            sender,
            max_packet_size: 1024,
            window_size: SendCredit::new(credit),
            hold: None,
        };
        (write_half, receiver)
    }

    #[tokio::test]
    async fn a_write_cut_short_before_the_queue_takes_it_keeps_its_credit() {
        let (write_half, mut receiver) = blocked_write_half(64);

        let mut cut = Box::pin(write_half.data_bytes(Bytes::from(vec![1; 32])));
        assert!(futures::poll!(cut.as_mut()).is_pending());
        drop(cut);
        assert_eq!(write_half.writable_packet_size().await, 64);

        // A follow-up write can use the whole window.
        receiver.recv().await.unwrap();
        write_half
            .data_bytes(Bytes::from(vec![2; 64]))
            .await
            .unwrap();
        match receiver.recv().await.unwrap() {
            (ChannelId(7), ChannelMsg::Data { data }) => assert_eq!(data.len(), 64),
            msg => panic!("unexpected message: {msg:?}"),
        }
        assert_eq!(write_half.writable_packet_size().await, 0);
    }

    #[tokio::test]
    async fn a_pending_async_write_takes_nothing_until_the_queue_takes_its_buffer() {
        use tokio::io::AsyncWriteExt;
        let (write_half, mut receiver) = blocked_write_half(64);
        let mut writer = Box::pin(write_half.make_writer());

        assert!(futures::poll!(Box::pin(writer.write(b"first"))).is_pending());
        assert_eq!(write_half.writable_packet_size().await, 64);
        // Retried with another buffer, the write sends that buffer.
        receiver.recv().await.unwrap();
        assert_eq!(writer.write(b"second").await.unwrap(), 6);
        match receiver.recv().await.unwrap() {
            (ChannelId(7), ChannelMsg::Data { data }) => assert_eq!(data.as_ref(), b"second"),
            msg => panic!("unexpected message: {msg:?}"),
        }
        assert_eq!(write_half.writable_packet_size().await, 58);

        // A shutdown after a pending write sends only EOF.
        write_half
            .sender
            .try_send((ChannelId(1), ChannelMsg::Eof))
            .unwrap();
        assert!(futures::poll!(Box::pin(writer.write(b"third"))).is_pending());
        receiver.recv().await.unwrap();
        writer.shutdown().await.unwrap();
        assert!(matches!(
            receiver.recv().await.unwrap(),
            (ChannelId(7), ChannelMsg::Eof)
        ));
        assert_eq!(write_half.writable_packet_size().await, 58);
    }
}
