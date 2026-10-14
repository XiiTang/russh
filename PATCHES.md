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
