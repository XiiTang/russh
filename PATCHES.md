# Boundless patches

Based on upstream `v0.64.1` (`e5d80a07dc554480b6ea3b2e14adfc190407b627`).
`main` mirrors upstream; every commit Boundless pins is on `boundless`.
Retain upstream history and licenses; keep application behavior outside this
fork. Keep a patch only while upstream lacks the behavior, and run its
regression and Boundless's SSH tests before moving the pin.

## 0.64.1 rebuild (#277)

The upstream source at the revision above was compared with every old patch.
No required behavior is supplied by 0.64.1, so none of these patches is dropped.
The original commit ids below identify the patches, rather than their replayed
ids. The regression sections below retain their original before/after evidence;
the complete russh suite is rerun on this release. Documentation-only commits
and test-peer ownership corrections carry no additional production mechanism.

| Original patch | Decision and source evidence in upstream 0.64.1 | Regression retained |
| --- | --- | --- |
| `1ee788d` password change | Keep: `client/mod.rs:345-360` sends only the old password; `client/encrypted.rs:163-195` has no password-change reply. | password wire encoding and explicit change prompt; Boundless independent password-change peer |
| `90505bb` managed channels | Keep: `channels/io/mod.rs:30-43` spawns an unowned close task. | `managed_channels` |
| `585dd71` receive credit | Adapt to upstream's let-chain formatting: `client/encrypted.rs:578-620` still replenishes on arrival and awaits application delivery. | `client_receive_window` |
| `2d5830f` close state | Keep: `session.rs:287-301` has no sent/received CLOSE state. | session close-state unit tests |
| `a019216` open reply result | Keep: `lib_inner.rs:596-607` discards the native send result. New upstream peer tests must handle the returned Result. | `managed_channels::a_reply_after_its_session_ended_returns_the_send_failure` |
| `589b786` malformed forward reply | Keep: `client/encrypted.rs:958-984` converts a malformed success to the same None as rejection. | `raw_no_crypto::an_undecodable_forward_success_ends_the_session` |
| `d2a3313` patch record | Keep as maintenance history; this inventory supersedes its release baseline. | no runtime change |
| `2e24684` establishment owner | Keep: `client/mod.rs:1219` spawns before establishment completes without an abort owner. | `cancelled_establishment` |
| `ac22f3b` Handle stop | Keep: `client/mod.rs:309-325` retains only the join, without a stop handle. | `stopped_session` |
| `401162f` graceful release | Adapt the event-loop insertion beside the new upstream rekey timer; preserve that timer. Upstream `channels/io/mod.rs:30-43` still uses the detached task. | `channel_release` |
| `697406c` release edges | Adapt test insertion beside upstream's new rekey tests; retain both sets. Same upstream release path as above. | release edge unit tests |
| `eba0af1` queue fence | Keep: the upstream detached close queues behind accepted data; the fork must preserve this ordering without a detached task. | fixed-fence and blocked-sibling unit tests |
| `3163ece` post-CLOSE exclusion | Adapt to upstream's let chains in client/server session writers; `session.rs:278-301` otherwise allows EOF after CLOSE. | `nothing_follows_this_sides_close`, `no_request_follows_this_sides_close` |
| `14ea6dd` send credit | Adapt next to the new hostkeys-prove method, keeping it intact. `channels/mod.rs:362-391` still debits before queue admission; `client/encrypted.rs:727-759` resets rather than adds credit. | send-credit unit tests |
| `3646703` disconnect observation | Keep: `client/mod.rs:1313-1333` returns a shutdown error before calling the handler. | `disconnect_reason` |
| `a8e430b` interrupted auth | Adapt formatting; `client/mod.rs:435-461` still synthesizes refusal on a closed reply stream. | `auth_session_end` |
| `72492b9` request-reply ordering | Keep as history under the later `7596d8a` replacement: `client/encrypted.rs:794-816` still queues before calling the handler. | `channel_reply` |
| `123b6be` owned regression peers | Keep fixture ownership corrections when replaying the tests. | regressions join their own peers |
| `7596d8a` request-reply consumption | Keep: those upstream arms still always deliver replies to the reader. | `channel_reply` |

The channel-reply hook follows upstream's let-chain style. The six receive-inbox
lock unwraps already present on the old boundless branch still fail upstream's
strict Clippy policy. Changing their poisoned-lock behavior is a separate repair;
this rebuild neither suppresses the lint nor changes that behavior. Upstream's
new open-reply fixture handles the fork's existing Result-returning accept method.
Stable rustfmt formats the replayed integration tests; it does not recursively
format the library's include file.

The upgrade retains upstream's agent lifetime-constraint admission fix and the
server signature-algorithm check, rather than copying either into a Boundless
patch. Their advisory exceptions are removed with Boundless's pin update.

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
program's behalf only once its read half and every writer are gone. When it
first sees the release it fixes a fence on each application queue: the
messages taken so far plus those queued then, which include everything the
released handles queued. Messages are still taken only through the ordinary
window gate, which leaves application output in the bounded queues while a
channel is window-blocked; once the count reaches the fence and no key exchange
runs, `Encrypted::close` queues CLOSE behind the channel's pending data. A
sibling the peer's window holds back therefore holds the close back too, as
upstream's ordered Close did, and the queues stay the memory bound. Only a
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
`an_overflowed_channel_waiting_at_its_fence_still_credits_what_arrives`,
`a_managed_channel_let_go_is_cancelled_though_its_holds_drop_first`,
`releases_never_take_messages_past_a_blocked_siblings_window`,
`a_release_during_kex_closes_after_it_behind_its_fixed_fence` and
`a_stream_let_go_before_its_open_is_accepted_closes_once_registered` cover the
close's edges: credit while CLOSE or the fence waits, a managed channel's guard dropping
after its holds, repeated releases beside siblings held on both client queues
(their pending data does not grow), a key exchange pausing the count, and a
server stream registered after its wakeup. Upstream's
`tests::channels::test_channel_streams` no longer fails intermittently.

## Nothing follows this side's CHANNEL_CLOSE

RFC 4254 section 5.3 allows no message on a channel after this side sent
CHANNEL_CLOSE. Data was already refused, but EOF, window adjustments and every
channel request (`exec`, `request_pty`, `request_shell`, `signal`, `set_env`,
`request_subsystem`, `request_x11`, `window_change`, `agent_forward`,
`exit_status_request`, `exit_signal_request`, `xon_xoff_request`,
`channel_success`, `channel_failure`) only checked that the channel was still
in the table, which it stays until the peer's CLOSE arrives. Every writer of a
channel message now passes `session::sendable`, which refuses a channel whose
CLOSE was sent; so does the replay of pending data, which CLOSE already waits
for. `Encrypted::close` writes the CLOSE itself. Regressions:
`cargo test -p russh --lib nothing_follows_this_sides_close` and
`no_request_follows_this_sides_close`, which write `[97, 96, 93, 94]` (EOF,
window adjustment, replayed data) and `[97, 98, 99]` (exit-status, SUCCESS) on
the previous commit instead of `[97]`.

## Writers take send credit only for what the session queue took (#72)

A channel's writers keep their own copy of the peer's window so they wait at
zero without asking the engine; the engine keeps the window it sends against.
Upstream debited the writers' copy before waiting for room in the session
queue (`reserve_writable_chunk`, `ChannelTx::poll_writable`): a write cut short
there kept no message and lost its credit, and a `ChannelTx` retried after
`Pending` sent the first buffer's bytes, or replaced them with EOF on
shutdown. On WINDOW_ADJUST the engine set the writers' copy to its own window,
counting again the data writers had queued that it had not taken yet; it parks
the excess in `pending_data`, and the outbound gate then stops every channel of
the session until the peer adjusts that one.

`SendCredit` replaces `WindowSizeRef`. Its value is private and moves only
through `admit`, which debits a writer's chunk in the same synchronous step its
queue permit sends it, `charge`, which debits data the session handles send
outside the writers (`Handle::data`, server `Handle::extended_data`, client
and server `Session::data` and `Session::extended_data`, possibly into debt),
and `grant`, which adds what a WINDOW_ADJUST adds and what confirming an open
changes the window by (a repeated confirmation replaces the window, so the
credit moves by the difference, never by the whole window again). A write
waits for credit, then for queue room, holding neither while it waits for the
other, as tokio-util's
`PollSender` reserves before it sends; qpid-proton likewise moves its
application credit only by what a peer flow adds. Handle data reaches the
engine as the hidden `ChannelMsg::HandleData`, so the engine charges it and
not a writer's data. `ChannelTx::new` is crate-private.

Run `cargo test -p russh --lib`:
`a_write_cut_short_before_the_queue_takes_it_keeps_its_credit` (credit 32
instead of 64 on the previous commit),
`a_pending_async_write_takes_nothing_until_the_queue_takes_its_buffer` (59
instead of 64) and `a_window_adjust_never_lets_writers_queue_past_the_window`
(the write past the window completes) fail on the previous commit;
`a_repeated_open_confirmation_never_lets_writers_queue_past_the_window` keeps a
duplicate CHANNEL_OPEN_CONFIRMATION from granting the window twice.

## The handler hears how a session ended before its shutdown result (#22)

`Session::run` shut the socket down with `?` before calling
`Handler::disconnected`, so a shutdown that failed returned its own error and
the handler never heard of the peer's DISCONNECT or the error that ended the
session. Boundless publishes the connection's end from that callback, and an
operation that meets the session's end reads it there. The shutdown's result
is now kept and returned only after the handler was told; during key exchange,
where no handler is told, it still comes first. Regression:
`cargo test -p russh --test disconnect_reason`, whose
`a_peers_disconnect_reaches_the_handler_though_shutdown_fails` and
`a_failed_session_reaches_the_handler_though_shutdown_fails` record no
`disconnected` call on the previous commit.

## An authentication the session's end interrupts is a Disconnect (#67)

`wait_recv_reply` and the waits of `authenticate_publickey_with`,
`authenticate_certificate_with` and `authenticate_gssapi_with_mic` answered a
closed reply channel with `AuthResult::Failure` and an empty method set, a
refusal the server never sent; a caller then reported its credentials
rejected when the session had ended. Every one of them now takes its reply
through `recv_auth_reply`, which answers the closed channel with
`Error::Disconnect`, as the password path already did. The `Signer` and
`GssapiAuthenticator` error bound changes from `From<SendError>` to
`From<Error>` so these methods can return it; a request or step the command
queue did not take is still `Error::SendError`, so a caller can tell a
request never sent from one whose session ended while it waited.
`AgentAuthError` gains `Session` for the latter and keeps `Send` for the
former. Regression: `cargo test -p russh --test auth_session_end`, whose
`an_authentication_the_sessions_end_interrupts_is_a_disconnect` cuts the
socket under each of the four waits and gets `Ok(Failure { [] })` from all
four on the previous commit; `a_refusal_the_server_sends_stays_a_failure`
keeps a real USERAUTH_FAILURE a failure.

## The handler takes a channel request's reply, which then skips the reader (#75)

The client engine queued CHANNEL_SUCCESS and CHANNEL_FAILURE on the
channel's inbox, where the channel's reader meets them in order with the
channel's data, and told the handler only through `channel_success` and
`channel_failure`. Boundless answers each request as its reply arrives,
because a reader blocked behind unread data never reaches the reply; a
reply it has answered must then not reach the reader at all, or the
replies the program asks for fill the inbox's control queue while the
program does not read and the engine closes the channel. A new
`Handler::channel_reply(channel, success, session)` takes both replies in
one arm and answers whether the reply still reaches the reader; its
default calls `channel_success` or `channel_failure` and lets it through,
as upstream does. (This replaces 72492b9's reason, which called the hooks
before queueing the reply for a reader that matched it to the hook's
record.) Regression: `cargo test -p russh --test channel_reply`: a reply
the handler takes never reaches the reader, and the default handler hears
each reply, which reaches the reader as before.
