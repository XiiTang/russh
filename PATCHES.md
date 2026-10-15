# Boundless patches

Based on upstream `v0.63.3` (`f33baf439be8c59c49cb6cb2ae5976c579495bdf`).
`main` mirrors upstream; every commit Boundless pins is on `boundless`.
Retain upstream history and licenses; keep application behavior outside this
fork. Keep a patch only while upstream lacks the behavior, and run its
regression and Boundless's SSH tests before moving the pin.

## Explicit password-change authentication (`1ee788d`)

RFC 4252 section 8 lets a server answer password authentication with
`SSH_MSG_USERAUTH_PASSWD_CHANGEREQ`. Upstream treated that message as a
keyboard-interactive or GSSAPI reply and offered no way to send a new
password. `Method::Password` carries an optional new password (zeroized with
the old one), and `PasswordAuthResponse::ChangeRequired` returns the server's
prompt and language to the caller, which decides whether to change it.
Boundless covers it with its AsyncSSH peer case
`independent_asyncssh_password_change_keeps_frozen_access`.

## Cancellation-owned session channels (`90505bb`)

A dropped upstream channel closed itself from a detached task, which could
wait behind a full peer window or application queue and outlive its owner.
`channel_open_session_managed` returns a `ManagedChannel` whose drop the
connection engine consumes: unsent data is cancelled and CLOSE queued even
while the window or queue is full, an OPEN abandoned before confirmation is
closed on confirmation, and sibling channels stay live. Run
`cargo test -p russh --test managed_channels`
(`cancelled_open_and_window_blocked_stream_close_in_engine_with_live_sibling`).

## Client receive windows driven by consumption (`585dd71`)

The connection reader awaited an application channel receiver, so one unread
channel stalled every sibling and keepalives. Each channel now has a bounded
inbox the reader never waits on; the advertised SSH window is replenished only
as the program consumes data, which bounds what is queued. Run
`cargo test -p russh --test client_receive_window`
(`unread_channel_keeps_sibling_and_keepalive_live_then_delivers_exact_tail`).
A reader that stops reading ends only the inbound direction; see "Letting a
channel go ends it gracefully".

## Channel close state and terminal boundaries (`2d5830f`)

A channel sent CHANNEL_CLOSE more than once, kept writing after either side
closed, and dropped its state before the peer's CLOSE arrived. CLOSE is sent
once, deferred behind pending data only until the peer closes, queued writes
are discarded once the peer closed its receive direction, and the state is
reclaimed only after both CLOSE messages. Run the `session.rs` unit tests
`active_close_reclaims_state_only_after_peer_close`,
`flush_pending_replays_deferred_close_then_waits_for_peer` and
`flush_all_pending_replays_deferred_close_then_waits_for_peer`.

## Channel-open replies return their send result

`ChannelOpenHandle::accept` and `reject` discarded the result of handing the
reply to the session loop, so a caller could not tell a reply that went out
from one that reached nothing after the session ended. Both return
`Error::SendError` in that case. Regression:
`cargo test -p russh --test managed_channels a_reply_after_its_session_ended_returns_the_send_failure`;
on the previous commit the old signature returns `()` and the regression does
not compile.

## An undecodable tcpip-forward success ends the session

A REQUEST_SUCCESS to tcpip-forward whose port did not decode, or that carried
bytes after it, reached `tcpip_forward` as `None`, the value of the server's
REQUEST_FAILURE, so the caller reported a refusal the server never sent. It is
now a protocol error that ends the session, and `tcpip_forward` returns
`Disconnect`. Regression:
`cargo test -p russh --lib an_undecodable_forward_success_ends_the_session`,
which fails on the previous commit with `RequestDenied`.

## Own the client session until key exchange hands it to Handle (#63)

A private PendingSession owns the session join and an AbortHandle through every
await before Handle construction. Dropping establishment aborts its existing
session future; successful construction transfers its join and disarms the
guard. The futures Abortable runs inside that same session task, so an
owner-requested stop returns Ok(()) without a detached monitor or error report.
Natural session errors and panics retain the base fork's existing result path.
No public establishment API changes. The futures Abortable source was inspected:
abort wakes the existing task, whose wrapper drops the stopped session future.

This adapts 991c731 onto d2a3313 without the managed direct TCP/IP patch 0f22b8a
or the independent runtime panic-payload patch 04d3d6b. Only PATCHES.md context
needed conflict resolution; the client-module change and regression are retained.

Regression: `cargo test --locked -p russh --test cancelled_establishment`.
A handler-entered signal establishes suspended key exchange; dropping
establishment must release the handler/session before peer teardown, and both
owned peer and session must finish. The timeout is only a hang budget.
On macOS arm64 the unchanged regression fails against d2a3313's client module
(exit 101, handler/session still running) and passes with this adaptation
(1 test, exit 0). The ignored lockfile is copied from the original fork clone.

## Let a Handle owner stop and read its client session (#63)

A completed Handle retains a clone of PendingSession's AbortHandle and exposes
`Handle::stop(&self)`. Establishment and Handle owners therefore cancel the same
Abortable session future; there is no second task, command-queue stop message,
or socket-only cancellation path. This also wakes a session parked waiting for
keyboard-interactive answers, a public-key signature, or a certificate signature.
The Handle keeps its join, so awaiting it still reads the session's end. The
existing wrapper reports an owner-requested stop as Ok(()); already completed
results and the base fork's natural error/panic path are retained. The stop
request returns immediately and repeated requests are harmless.

This is cancellation: the peer sees the TCP connection close, no SSH DISCONNECT
is sent, and channel data already queued is not guaranteed to be sent. It is
distinct from the graceful end of an ordinary channel when that channel is let
go of. Await the Handle after requesting stop to observe completion.

Before implementing this, Tokio 1.53.3's source was read: JoinHandle::abort calls
remote_abort, whose cancellation transition leaves join interest intact; task
completion retains the output and wakes the join reader. Its Future still reads
that output. The futures-util 0.3.34 AbortHandle/Abortable source supplies the same
separation here, preserving the pre-Handle patch's existing Ok(()) stop result.

Regression: `cargo test --locked -p russh --test stopped_session`.
Three real TCP fixtures observe a keyboard-interactive prompt or a signing
request whose signer refuses, leaving the native session waiting without an
answer. Each calls stop twice, awaits the retained Handle successfully, checks
it is closed, stops it again, and reads the peer's end before teardown. The peer
receives no authentication answer. All three pass with stop and fail at their
hang budgets when only stop's abort call is replaced by a no-op (exit 101).
On macOS arm64 the stop/establishment regressions and 25 existing client,
channel, certificate-authentication, natural-error, and fork regressions pass
(29 tests total). Both CI commands, `cargo clippy -- -D warnings` and
`cargo clippy --all-features -- -D warnings`, exit 101 with the same 44 errors
on the base client module and on this patch: diagnostic messages, paths and
source excerpts match after ignoring shifted line numbers. No lint is suppressed
and no unrelated Clippy repair is included.

## Letting a channel go ends it gracefully

The client engine treated a closed reader, whether dropped or after reading the
peer's EOF, as a cancellation of the whole channel: it discarded the channel's
pending data and sent CLOSE before the messages its handles had already queued,
so data accepted by `write_all` was lost. Upstream's detached close task, which
queued CLOSE behind those messages, was an unowned task.

Letting a channel go now ends it the way OpenSSH and TCP do: what was accepted
leaves first. A reader-side event ends only the inbound direction; data that
reaches no reader still counts as consumed, so the peer's window keeps moving
(OpenSSH `channels.c` `channel_input_data`). The engine closes a channel on the
program's behalf only once its read half and every writer are gone: it handles
the messages already queued, then queues CLOSE behind the pending data. Only a
managed channel's cancellation discards pending data. A control overflow closes
the channel the same graceful way. The two ends differ in which channels this
covers:

- A client closes every channel let go of. A dropped client channel already
  closed itself on this branch, and Boundless relies on it.
- A server closes only a channel turned into a stream (`into_stream`). Upstream
  never closed a dropped server `Channel`: a server handler may drop it and keep
  writing by id through its session, and upstream's own tests do so.

Run `cargo test -p russh --test channel_release`.
`queued_messages_leave_before_the_close_of_a_released_channel`,
`a_window_blocked_stream_let_go_sends_its_pending_data_then_closes`,
`a_stream_still_writes_after_reading_the_peers_eof` and
`letting_the_read_half_go_keeps_the_write_half_and_credits_the_peer` fail on
the previous commit; `a_server_stream_let_go_closes_after_its_queued_data`
keeps a server stream's close without the detached task. The `--lib` tests
`a_channel_closing_behind_pending_data_still_credits_what_arrives`,
`a_managed_channel_let_go_is_cancelled_though_its_holds_drop_first`,
`a_rekey_the_queue_starts_holds_the_close_behind_what_is_still_queued` and
`a_stream_let_go_before_its_open_is_accepted_closes_once_registered` cover the
close's edges: credit while CLOSE waits, a managed channel's guard dropping
after its holds, a rekey started while handling the queue, and a server stream
registered after its wakeup. Upstream's
`tests::channels::test_channel_streams` no longer fails intermittently.
