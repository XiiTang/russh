# Boundless patches

Based on upstream `v0.65.0` (`183a22e8f8f490fa09b6eb7bd044ef99d8c4414f`).
`main` mirrors upstream; every commit Boundless pins is on `boundless`.
Retain upstream history and licenses; keep application behavior outside this
fork. Keep a patch only while upstream lacks the behavior, and run its
regression and Boundless's SSH tests before moving the pin.

## 0.65.0 rebuild (#416)

Every required patch was replayed on the release, rather than merging upstream
into the patched tree. The table is the current disposition of every patch;
older sections retain their original reproduction and historical validation.
Documentation commits, peer-join corrections and replaced intermediate hook
implementations add no separate production mechanism.

| Patch / observed failure it prevents | Upstream 0.65 source and disposition | Retained regression | Rebuilt commit(s) |
| --- | --- | --- | --- |
| Explicit password change | Keep `client::Handle`/`Session::write_auth_request_if_needed`: upstream still offers only the old-password exchange and does not publish a replacement prompt. | `client::encrypted::tests::password_change_encodes_only_an_explicit_replacement`; password prompt library tests | d606b90 |
| Cancellation-owned channels | Keep engine-owned `channels::managed`; upstream `channels/io/mod.rs::ChannelStream::drop` still spawns an unowned close. | `managed_channels` | 9c628a3 |
| Receive credit / unread sibling stalls | Keep `channels::inbox` and consumption-driven replenishment; upstream `client/encrypted.rs` still replenishes DATA credit on arrival and awaits application delivery. | `client_receive_window` | 96dc9ec |
| Channel close state | Keep `session::sendable` and sent/received CLOSE state; upstream `Encrypted::eof` and request writers still accept a channel until removal. | session close-state library tests | 0b4c7f5 |
| Channel-open reply result | Keep the existing accept/reject send result in `ChannelOpenHandleInner`; upstream discards it. Adapt the new upstream window-adjust fixture's accept call. | `managed_channels::a_reply_after_its_session_ended_returns_the_send_failure` | 0b1b252 |
| Malformed forward success | Keep the native `client/encrypted.rs` reply decoder's error; upstream still turns malformed success into rejection. | `raw_no_crypto::an_undecodable_forward_success_ends_the_session` | 4bcbb7d |
| Establishment cancellation | Keep stop/join custody through `PendingSession`; upstream `connect_stream` still spawns before awaiting key exchange without a retained abort owner. | `cancelled_establishment` | 285e5c2 |
| Handle stop | Keep the existing AbortHandle beside the join; upstream Handle cannot request cancellation while retaining its result. | `stopped_session` (signing held at an explicit barrier; 0.65 abandons refused signatures) | fb68e32 |
| Graceful channel release and release edges | Keep engine queue fences and ownership through pending data; upstream still queues close from a detached stream-drop task. Preserve its rule that a bare server Channel need not close when dropped. | `channel_release`; release-edge library tests | 2ebba3c, 52aa39a |
| Fixed queue fence / blocked sibling | Keep the existing event-loop window gate and fixed accepted-message fence; do not drain more application data around that gate. Upstream ordered close remains ordered behind queued data. | blocked-sibling/fence/KEX library tests | 43b5059 |
| No message after CLOSE | Keep `session::sendable` across EOF, requests, adjustment and pending replay; upstream has no sent-CLOSE refusal there. | `nothing_follows_this_sides_close`; `no_request_follows_this_sides_close` | b9c5b0d |
| Writer credit at queue admission | Keep `SendCredit::admit`, `charge`, `grant`. Upstream #790 now adds WINDOW_ADJUST credit, so retain that corrected behavior through `grant`; it still debits writer credit before queue admission and has no difference-based repeated-confirmation accounting. No parallel upstream `grow` remains. | cancelled/pending write, window-adjust and repeated-confirmation library tests; upstream `test_window_adjust` | 11dff59 |
| Disconnect observation | Keep handler publication before returning socket shutdown failure; upstream `Session::run` still uses `shutdown().await?` before the callback. Preserve upstream shutdown result and pre-handshake handling. | `disconnect_reason` | 3e80707 |
| Interrupted authentication | Keep `recv_auth_reply` returning Disconnect instead of a fabricated refusal; upstream still returns empty-method Failure on closed reply streams. Preserve 0.65's new SignFailed/abandon_sign_request behavior in both signing paths. Adapt the new upstream signer fixture to the fork's existing From<Error> contract. | `auth_session_end`; upstream `auth_signer_failure` | 6be5385 |
| Channel reply consumption | Keep final `Handler::channel_reply` replacement for the old ordering-only hook; upstream always queues replies for the reader, even after the handler answers them. | `channel_reply` | c43f884 |
| Owned pre-Handle establishment | Adapt `PendingSession::establish` to carry 0.65's optional AgentSessionBinding into the same Handle after key exchange. Preserve cancellation, joined-result and error paths; upstream's normal connect path still supplies the same binding. | `owned_establishment`; upstream `test_agent_session_bind` | 419548f, 9309c9c |
| Strict Rust 1.97 / poisoned inbox | Keep existing private Inbox::state assertion and test/example lint repairs. Upstream has no fork inbox, so supplies no equivalent poison invariant or strict regression coverage. New server-fixture signatures use 0.65 PublicKeyOffer/VerifiedPublicKey, preserving their assertions. | inbox poison library test; `russh-config::tests::expand_home`; strict Clippy | 422bdb6, f1c9710, 9309c9c, 34a6027 |
| Window notifications and adjustment hook | Keep actual Inbox routing and the upstream hook at consumed-credit advertisement; upstream's try_send still targets its own mpsc and replenishes on arrival. | `client_receive_window::channel_window_notifications_and_the_receive_adjustment_hook_remain_reachable` | 6d4a4c8 |
| Optional notification refusal | Keep refusal of excess informational WindowAdjusted nonfatal, as upstream try_send does, without changing credit or callbacks. | `client_receive_window::unread_upload_credit_notifications_do_not_close_the_channel` | d2602bc |
| Required-control overflow tail | Keep one typed terminal tail through Inbox::poll_recv and ChannelRx; upstream has no fork control queue or overflow fact. | inbox admission and ChannelRx typed-error library tests | 8e2d425 |
| External ownership through retirement | Keep existing channel holds through the native entry, release fence and pending output; upstream has no external admitted owner. | `client::tests::a_released_stream_keeps_its_owner_through_the_fence_and_native_pending_data` | 90749b1 |
| Correlated refusals and auth facts | Keep complete CHANNEL_OPEN_FAILURE fields in the original inbox and Error, auth_failure before method filtering, and the alias of the existing packet bound; upstream still loses those received fields. | three `w1d3_` library tests | 20a807f |


The release supplies its security fixes unchanged at their native checkpoints:
`client::Session::handle_ext_info` refuses a third EXT_INFO (b33e93b);
`OpenChannelMessage::parse` and `ChannelOpenConfirmation::parse` refuse zero
maximum packet size (39cb9db); `Connection::add_key` deduplicates confirmation
constraints (23aa5d2); `Connection::try_sign` calls Agent::confirm (62db6e7);
and PageantStream checks its shared-memory frame capacity before waiting for a
body (69e4b55, pageant 0.2.5). No Boundless-side duplicate check is added.
The previous fork already refused zero-sized writer chunks, but the new parser
check rejects the invalid declaration before channel state is installed.

Upstream's new pre-auth keepalive rearming, wasm rekey handling, RSA agent
flags and agent session binding remain intact. The certificate nesting limit
is retained with the structural correction documented below. No cancellation,
timeout, retry, close or error behavior from 0.65 is dropped.
The old pinned history is retained through ours merges without moving either
published/default branch. Local boundless was behind the consuming pin, so the
consuming pin is also retained explicitly; the landing owner publishes later.

Validation on macOS arm64 / Rust 1.97.0, CARGO_INCREMENTAL unset:

- `cargo +1.97.0 test -p russh --lib --tests --no-default-features --features ring,rsa,flate2`:
  exit 0, initially 293 tests and then 294 with the certificate regression,
  covering every named russh regression above, the new
  EXT_INFO/zero-packet/agent-constraint tests and the upstream signer/binding/
  window-adjust tests. The fixtures use registry RSA 0.10.0-rc.18, as upstream
  expects. The first consumer-RSA run exited 101: seven RSA signing/agent
  fixtures require private-key operations that Boundless's pinned RSA deliberately
  refuses. No product refusal or test assertion was weakened. Boundless's own
  SSH/SFTP tests exercise its pinned RSA and aws-lc local signer separately.
- `cargo +1.97.0 test -p russh-config --lib --locked -- tests::expand_home --exact`:
  exit 0, one test.
- `cargo +1.97.0 clippy -p russh --lib --no-default-features --features ring,rsa,flate2 --locked -- -D warnings`:
  exit 0. The first additional all-targets command exited 101 at the new
  upstream `test_keepalive_before_auth` fixture's field_reassign_with_default
  warning. Its initializer and the upstream signer fixture's initializer now
  set the same fields in their Config literal; no behavior or assertion changes.
  `cargo +1.97.0 clippy --workspace --all-targets --locked -- -D warnings`
  and the same command with `--no-default-features --features ring,rsa,flate2`
  now both exit 0. The new certificate unit setup propagates its errors through
  Result rather than adding unwraps under the library's strict lint rules.
- `cargo +1.97.0 fmt --all --check`: exit 0.

No Windows Pageant runtime validation is claimed on this host.

## Certificate nesting counts structure, not opaque key IDs (#416)

Upstream 0.65 commit `730d663` adds `keys::key::check_cert_nesting` before raw
network key/certificate decoding. It counts every `-cert-v01@` substring in the
blob and refuses more than eight, to stop ssh-key's unbounded recursive
`Certificate::signature_key` decoding. A valid signed shallow certificate whose
key_id contains eight such strings consequently fails SSH host validation:
one real algorithm plus eight opaque strings exceeds that heuristic limit.
The unit regression first decoded and verified the signature with ssh-key,
then failed at the unchanged guard on the rebuilt branch (exit 101).

Keep the same private check point, callers, limit and depth error. Walk only
actual signature_key fields iteratively, using `Algorithm::new_certificate`
(including custom names), nonrecursive `KeyData::decode_as` for the subject
key, and ssh-encoding's length-prefixed reads for opaque fields. This follows
ssh-key 0.7.0-rc.11's `Certificate::decode_as` layout without recursing. Normal
decoding and signature validation still follow the guard; none of the guard's
network entry points can bypass it. Retire this patch when upstream counts
structural depth or ssh-key bounds its own recursive decode.

`keys::key::test::certificate_key_id_markers_do_not_count_as_nesting` now
passes for the signed shallow certificate and public certificate decode.
`deeply_nested_certificate_is_rejected` accepts eight actual levels, refuses
nine, and retains the original 2,848-level attack through both certificate
and public-key entry points. Both focused unit tests exit 0 on macOS arm64 /
Rust 1.97.0. The existing `test_server_cert` fixtures also carry eight marker
strings in their key_id so the real host-certificate handshake exercises this
compatibility rule, including its RSA and authority/validity controls.
The complete russh library/integration suite exits 0 (294 tests), including
all five host-certificate tests. Workspace all-targets strict Clippy exits 0
in default and Boundless feature modes; cargo fmt and direct key.rs rustfmt
checks exit 0. No test assertion or production depth refusal is weakened.

## Correlated channel refusals and complete authentication facts (W4b/W5-D3)

Upstream v0.64.1 decodes CHANNEL_OPEN_FAILURE description/language in both
client and server roles, but the existing per-channel inbox and returned Error
carry only the outgoing reason enum. Concurrent opens lose their own fields.
Carry raw reason_code, description and language in RemoteChannelOpenFailureInfo
through that same inbox and Error. Keep outgoing rejection APIs, existing
handler callback, channel custody and retirement behavior. No last-error slot
or completion framework is added.

Upstream converts USERAUTH_FAILURE's NameList to MethodSet before publication,
filtering unknown names. A default no-op Handler::auth_failure receives complete
decoded names/partial-success before filtering and the existing reply. Keep
selection, receiver lifetime and interruption behavior. The existing provider
callback already exposes GSSAPI_ERROR; it needs no native patch.

Export MAXIMUM_INCOMING_PACKET_LEN as an alias of the existing enforced
transport/decompressed bound for prepaid required reply data. No new packet
limit, formula or outgoing restriction is introduced.

The three w1d3_ library regressions drive client decode through its original
inbox/confirmation reader (concurrent opens, reverse replies, different/empty
text/languages and an unknown raw code), the server decoder's original inbox,
and complete USERAUTH_FAILURE hook facts before the typed reply is read.
They and strict library/test Clippy pass with ring/rsa/flate2 and no default
features. Interrupted-authentication and Boundless integration regressions
remain required before publication. The optional partial formatter could not
start on this host because the installed nightly lacks librustc_driver;
this is not recorded as a formatting pass.

## Strict Rust 1.97 Clippy and receive-inbox lock invariants (#340)

The six receive-inbox locks now use the private `Inbox::state` accessor.
It asserts against poisoning explicitly and preserves the former `Result::unwrap`
panic, its String payload, and normal unwind. Recovering the guard would
expose potentially inconsistent queue state and change callers' observations.
Healthy queue behavior, public APIs, and SSH flow control are unchanged.
The review follow-up uses an assertion on the original lock result and then
`unwrap_or_else(PoisonError::into_inner)` on the success path. The assertion
unwinds before a poisoned guard can be recovered. `#[track_caller]` retains
the caller location, and no production lint exception is needed. The initial
repair used an avoidable `clippy::panic` exception; the follow-up removes it.

Upstream main at `e5d80a07dc554480b6ea3b2e14adfc190407b627` has no fork inbox
and uses an async mutex for channel windows. The fork's `SendCredit::value`
supplies its private accessor idiom; its poison recovery is deliberately not
copied. Rust 1.97's `Result::unwrap` supplies the retained panic behavior.

The exact workspace-root strict commands initially exit 101 on pristine
`daac975`: a `russh-config` test unwrap stops the build before the inbox.
Running them from the `russh` crate also exits 101, reproducing the six inbox
sites, map-key iteration, and 761 library-test diagnostics. Removing those
initial blockers exposes further integration/example warnings; all reproduced
findings are covered by this repair. `russh-config` follows its existing
`expect("to_str")` test idiom. Production agent key traversal uses `keys()`
with identical order. True test/example warnings use explicit initializers,
borrow cleanup, a tuple alias, `repeat_n`, Option/Result matches, and map-value
iteration. KEX test mutex guards end before disconnect awaits. Explicitly
dropping already-running server handles retains the former `let _` behavior;
it does not newly await session termination. The encrypted-client test module
moves after production items without changing those items.

Restriction warnings on deliberate fixture assertions are false positives:
a failed assertion must fail the test, including an unexpected variant or
invalid fixed packet offset. Exceptions name only reproduced restriction
lints, inside `cfg(test)` modules or individual test functions. Production
lint policy is unchanged. The table lists all newly excepted assertion scopes;
each has its reason beside it. Paths below are relative to the workspace root.

| Test-only scope | Restriction lints excepted (`clippy::`) |
| --- | --- |
| `cryptovec/src/cryptovec.rs` (cfg(test) module at original line 485) | `unwrap_used`, `indexing_slicing` |
| `russh/src/channels/mod.rs` (cfg(test) module at original line 778) | `unwrap_used`, `panic` |
| `russh/src/client/encrypted.rs` (cfg(test) module at original line 1116) | `unwrap_used`, `indexing_slicing` |
| `russh/src/client/mod.rs` (cfg(test) module at original line 2363) | `unwrap_used`, `expect_used`, `indexing_slicing` |
| `russh/src/client/test.rs` (cfg(test) module at original line 2) | `unwrap_used`, `panic` |
| `russh/src/compression.rs` (cfg(test) module at original line 172) | `unwrap_used`, `indexing_slicing` |
| `russh/src/helpers.rs` (cfg(test) module at original line 140) | `unwrap_used` |
| `russh/src/kex/curve25519.rs` (cfg(test) module at original line 189) | `unwrap_used` |
| `russh/src/kex/ecdh_nistp.rs` (cfg(test) module at original line 211) | `unwrap_used` |
| `russh/src/kex/hybrid_mlkem.rs` (cfg(test) module at original line 250) | `unwrap_used`, `indexing_slicing` |
| `russh/src/kex/mod.rs` (cfg(test) module at original line 505) | `unwrap_used` |
| `russh/src/keys/agent/client.rs` (cfg(test) module at original line 594) | `unwrap_used`, `expect_used` |
| `russh/src/keys/agent/mod.rs` (cfg(test) module at original line 84) | `unwrap_used` |
| `russh/src/keys/agent/server.rs` (cfg(test) module at original line 374) | `unwrap_used`, `expect_used` |
| `russh/src/keys/format/tests.rs` (cfg(test) format::tests file) | `unwrap_used` |
| `russh/src/keys/known_hosts.rs` (cfg(test) module at original line 177) | `unwrap_used` |
| `russh/src/keys/mod.rs` (cfg(test) module at original line 269) | `unwrap_used`, `expect_used`, `indexing_slicing`, `panic` |
| `russh/src/negotiation.rs` (cfg(test) module at original line 638) | `unwrap_used` |
| `russh/src/parsing.rs` (cfg(test) module at original line 210) | `expect_used` |
| `russh/src/server/encrypted.rs` (cfg(test) module at original line 167) | `unwrap_used`, `expect_used` |
| `russh/src/server/session.rs` (cfg(test) module at original line 1681) | `unwrap_used`, `indexing_slicing` |
| `russh/src/session.rs` (cfg(test) module at original line 970) | `unwrap_used`, `indexing_slicing` |
| `russh/src/ssh_read.rs` (cfg(test) module at original line 217) | `unwrap_used` |
| `russh/src/sshbuffer.rs` (packet_bytes_compressed_matches_packet_output; test_packet_retains_plaintext_for_compressed_packets; test_write_packet_compressed_matches_clear_cipher_output; test_write_packet_restores_output_buffer_on_error; automatic_rekey_read_counter_tracks_payload_bytes; installing_a_cipher_starts_a_new_write_key_epoch; test_write_packet_matches_clear_cipher_write_output; packet_bytes_matches_packet_output; packet_bytes_returns_retained_bytes; test_packet_returns_retained_bytes; test_write_packet_leaves_reusable_buffer_for_cold_path_packets) | `unwrap_used` |
| `russh/src/tests.rs` (cfg(test) tests file) | `indexing_slicing` |

The inbox poison regression needs no test-only lint exception.
In `russh/src/tests.rs`, `async_yields_async` is scoped to `test_session`,
`test_server_channels`, `test_channel_streams`, `test_channel_objects`,
`test_server_receives_close_on_client_close`, `test_channel_window_size`,
`send_pty_req`, and `test_future_certificate_auth_full_flow`: their returned
Future-valued session handles transfer live ownership to the fixture; awaiting
them to termination would block the fixture. `capture_panics` alone permits
`await_holding_lock`, because its global panic-hook observation must cover
native completion. Its sole async caller owns its runtime; the competing
synchronous poison test runs on another test thread. Both tests share the same
hook lock so intentional unwinds cannot taint the raw-packet panic assertion.
These exceptions are false positives and preserve existing fixture lifecycles.

Validation on macOS arm64, Rust 1.97.0, through `build-slot`, with
`CARGO_INCREMENTAL=0`:

- `cargo clippy --all-targets -- -D warnings`: before 101; after 0.
- `cargo clippy --all-targets --all-features -- -D warnings`: before 101;
  after 0. Nine cryptovec assertion warnings were separately reproduced
  before adding its test-only restriction exceptions.
- `cargo test -p russh --lib --locked --
  channels::inbox::tests::every_inbox_operation_preserves_the_poisoned_lock_panic
  --exact`: before 0 (same test on unchanged production); after 0, one test.
  The test poisons the actual inbox through a reader waker during `send`, then
  verifies all six methods' panic payloads and retained queue/credit/close state.
- `cargo test -p russh --lib --locked --
  every_inbox_operation_preserves_the_poisoned_lock_panic
  malformed_pty_req_truncated_modes_rejected_by_server --test-threads=2`:
  before 0 and after 0, two tests each, including panic-hook isolation.
- `cargo test -p russh-config --lib --locked -- tests::expand_home --exact`: 0.
- `cargo fmt --all --check`: 0 after repair. The include-file inbox is also
  formatted explicitly with Rust 1.97 `rustfmt --edition 2024`.

The nine selected integration targets (`auth_partial_success`, `auth_state_reset`,
`managed_channels`, `test_backpressure`, `test_kex_shared_secret`,
`test_max_channel_packet_size`, `test_mlkem_kex`, `test_rekey_strict_kex`,
`test_server_cert`) also exit 0 after their fixture repairs.

The strict command's 101-to-0 transition is the lint regression; the poison
regression's 0-to-0 result demonstrates preservation of the existing failure
behavior.

Review follow-up on `b0298e3` (macOS arm64, Rust 1.97.0): replace the avoidable
production `clippy::panic` exception with the assertion above, without amending
the original repair. Both strict modes and the two preservation tests passed
before and after (exit 0). `cargo +1.97.0 fmt --all --check` and the explicit
inbox rustfmt check also exit 0. Complete workspace verification, through
`build-slot` with `CARGO_INCREMENTAL=0`, now includes:

- `cargo +1.97.0 test --workspace --locked`: exit 0, 311 passed, 8 ignored,
  including doctests.
- `cargo +1.97.0 test --workspace --locked --all-features`: exit 0, 312 passed,
  8 ignored, including doctests.

Those complete suites had not run before review because a lane held the markers;
the earlier selected-target runs were not complete-suite evidence.

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

Validation of the rebuilt native code at `3dfd52f` on macOS arm64:
`cargo test -p russh` and `cargo test --locked -p russh --all-features`
exit 0; `cargo fmt --all --check` exits 0. Both strict Clippy feature variants
exit 101 at the six pre-existing `channels/inbox.rs` lock unwraps described above.
The additional fork regression
`client::encrypted::tests::password_change_encodes_only_an_explicit_replacement`
exits 101 when the replacement-present wire flag is disabled, and 0 with the
carried password patch restored. Its upstream baseline has neither the replacement
field nor the password-change reply API. The existing per-patch regression
sections below name their checkpoints and original before/after evidence.

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

## Own a client session before its key exchange completes (#292)

`connect_stream` created a native session task and suspended for key exchange
while its stop and join were held only by private `PendingSession`. Dropping
establishment requested abort and discarded the join receiver. A caller could
not join the native work before releasing its own opening lifetime.

`connect_stream_owned` returns `PendingSession` immediately after task creation,
before the next suspension. Its `establish` wait borrows the owner, `stop`
requests the same native cancellation, and awaiting the owner reads that task's
completion and result, including a panic. Successful key exchange transfers the
same Handle through `into_handle`. `is_joined` identifies a native result already
read by `establish`, so a cleanup owner does not poll that join twice. The ordinary `connect_stream` delegates to
these steps and retains its existing result and cancellation behavior. No new
task or completion observer is introduced.

Before implementing this, the existing PendingSession, russh-util's oneshot
join, Handle stop/Future and futures Abortable ownership paths were inspected.
The new `owned_establishment` tests hold key exchange at a handler gate, leave
the borrowed establishment wait, and read cancelled or panicked native
completion before checking destruction. A successful exchange transfers and
joins the same task through Handle. On the unmodified pinned baseline the test
command exits 101 because the required owned API does not exist; this is an API
capability regression, not a passing runtime reproduction on the old API.

On macOS arm64, the owned-establishment, cancelled-establishment and
stopped-session tests pass (6 tests). The complete default russh library and
integration tests pass (269 tests). Strict library/test Clippy remains blocked on the
unchanged baseline's six library lock unwraps and 760 library-test diagnostics;
the same command fails on both revisions. No lint policy or unrelated code is
changed. Detailed commands and results belong to the consuming change's report.

## Restore upstream window notifications and adjustment hook (F9)

Start from upstream e5d80a07 client/encrypted.rs WindowAdjusted try_send and
client/mod.rs Handler::adjust_window / target_window_size behavior. The inbox
patch changed the client's reader but left try_send routed to its unused mpsc
receiver. ChannelRef now routes that same nonblocking call through the existing
Inbox::send checkpoint, retaining upstream's mpsc try_send for legacy channels.
The existing bounded-control overflow policy and send-credit wakeup remain.

Restore upstream's target-window callback value/positive-return retention at
actual consumed-credit replenishment. The necessary difference is its timing:
the recorded receive-backpressure patch must return credit only after the
application consumes bytes, never merely when a packet arrives. Therefore the
hook runs when that consumed credit is advertised, using the same upstream
callback block, rather than restoring arrival-based replenishment. The checked
receive-window debit / invalid-peer overrun rejection is unchanged.
`channel_window_notifications_and_the_receive_adjustment_hook_remain_reachable`
uses a real client/server session and observes both the ChannelMsg and callback;
the existing unread-channel/sibling and window-overrun tests retain backpressure.

On macOS arm64 with consumer Rust 1.97.0, workspace tests, strict all-target
workspace Clippy, and workspace formatting pass, using the consumer-pinned RSA.

## Preserve non-fatal informational credit refusal (review A R1)

Starting from upstream e5d80a07 `client/encrypted.rs:748-755`, keep the
informational `try_send` refusal when the receiver is full: delivery may be
omitted, while the send-credit grant, writer wake and handler callback still
happen. The necessary fork difference is routing to the actual bounded Inbox,
whose `send` is the single admission checkpoint. At its control limit an
informational WindowAdjusted is now refused without marking that inbox
overflowed; other control overflow and checked receive-window debit stay.
There is no recorded need for information loss to close a healthy channel.

`unread_upload_credit_notifications_do_not_close_the_channel` uses a real SSH
session with an unread reader, synchronizes each of 129 uploads with the
client credit callback, then reads the retained notifications, executes a
command, observes its output and uploads again. Against bdd1e3b4 it fails
because that command's reader ends; with this correction it stays usable.

On macOS arm64 with consumer Rust 1.97.0 and the pinned RSA, the complete
workspace tests, strict all-target Clippy and fmt pass (exit 0). The real
unread-upload regression passes alongside the notification/hook and unread
receive-window regressions.


## W17c2: type the existing required-control inbox overflow

Base d74984cdc61cd8cd82c3010754c490dafb9d14a0 already preserves upstream
 e5d80a07's optional WindowAdjusted notification refusal: native credit and
writer wake happen before try_send, and the callback still runs. Keep that
behavior, the existing control count/minimum, SSH DATA window, consumed-credit
accounting, close release fence and KEX scheduling. No new limit, byte counter,
raw-sender restriction, cancellation or cap-driven early close is added.

The observed failure is `Inbox::poll_recv` turning a latched required-control
overflow into EOF, and ChannelRx ignoring a terminal message through its
wildcard. `Inbox::send` remains the one count-admission checkpoint; the private
latched state produces a single `ChannelMsg::InboxOverflow` tail outside the
queue, after accepted messages and before EOF. The public immutable
`ChannelInboxOverflow { maximum }` is Error/Display. ChannelRx explicitly carries
it in io::Error; ChannelStream delegates to that reader. Later CLOSE/sender Drop
cannot replace this latch. Upstream's channel notification queue does not supply
this typed overflow; ordinary upstream EOF and the pin's close/fence ownership
remain unchanged. Only the already required failure is made observable.

Focused consumer-pinned Rust 1.97.0 / macOS Darwin 27.2.0 arm64 regressions,
through build-slot, are being completed on this branch. The new private Inbox
checkpoint test asserts accepted controls, nonfatal excess WindowAdjusted,
typed maximum once and EOF. The ChannelRx effect test reads accepted stream
bytes then the actual typed io::Error. Existing client_receive_window tests
exercise real notification/callback, unread upload and sibling/keepalive credit.
No test asserts the removed fatal informational-notification policy.


Validation of source tip e1336ac9f42debb5f5f557aadbd1a3dcfff4165a on macOS
Darwin 27.2.0 arm64 / Rust 1.97.0, all through build-slot. The consumer mode is
`--no-default-features --features ring,rsa,flate2`; Cargo's local patch pins RSA
 d8a034bbb784b8922efa4b37b71bf592b8103761. Put `--config` after the external
Clippy subcommand so it is forwarded to its Cargo check (a config before Clippy
alone did not retain that patch).

| cargo arguments, with that local RSA config | Exit / result |
| --- | --- |
| test -p russh --lib channels::inbox::tests:: | 0; 2 tests, default features |
| test -p russh --lib channels::io::rx::tests:: --locked | 0; 1 test, default features |
| test -p russh --test client_receive_window --locked | 0; 3 tests, default features |
| test -p russh --no-default-features --features ring,rsa,flate2 --lib channels::inbox::tests:: --locked | 0; 2 tests |
| test -p russh --no-default-features --features ring,rsa,flate2 --lib channels::io::rx::tests:: --locked | 0; 1 test |
| test -p russh --no-default-features --features ring,rsa,flate2 --test client_receive_window --locked | 0; 3 tests |
| test -p russh --no-default-features --features ring,rsa,flate2 --lib an_overflowed_channel_waiting_at_its_fence_still_credits_what_arrives --locked | 0; 1 test |
| clippy -p russh --lib --tests -- -D warnings | 0; pinned RSA, default features |
| clippy -p russh --no-default-features --features ring,rsa,flate2 --lib --tests --locked -- -D warnings | 0; pinned RSA, consumer features |
| fmt --all --check | Not started: disk-preflight runner exit 70 at 29 GiB free |

An earlier locked Clippy attempt exited 101 because its ignored local lock
needed resolution; the first nonlocked/default rerun resolved registry RSA.
The corrected invocation after the subcommand resolved the consumer-pinned Git
RSA and passed strict Clippy; the locked consumer-feature result above is the
relevant integration check. No manifest patch/configuration or Cargo.lock is
committed. Formatting and complete Boundless integration remain pending under
the consuming task's explicit 30 GiB stop rule. No push or boundless fast-forward.

Pinned-toolchain formatting recheck (2026-10-29, macOS arm64):
`RUSTUP_TOOLCHAIN=1.97.0 build-slot cargo fmt --all --check` exited 0
on source tip `e1336ac9f42debb5f5f557aadbd1a3dcfff4165a`. No source changed for this recheck.


## External channel ownership through native retirement (W17a)

Upstream `e5d80a07` registers OPENs in its native channel tables and keeps native
channel state independently of the application OPEN waiter. The existing Boundless
fork adds the queue-fence and managed-release policy around pending data and CLOSE.
An application waiter/stream is not that entry: dropping it cannot release an external admitted
block while those native owners remain. Optional external ownership now attaches
once to the existing channel holds before an outgoing OPEN is queued, or before
an incoming channel is accepted. The same holds live in the encrypted channel
parameters, after their pending data, as well as in the engine reference and
application halves. No new cap, cleanup reservation, scheduling or cancellation
policy is introduced. Existing callers without an external owner keep their API.

Source precedent: OpenSSH channels.c::channel_garbage_collect (2711) releases
only after nchan.c::chan_is_dead (333); channel_input_open_failure (3767) marks
an opening channel dead. Envoy source/common/conn_pool/conn_pool_base.cc (934-947)
counts the ActiveClient until releaseResourcesBase, independent of its waiters.

The native regression
client::tests::a_released_stream_keeps_its_owner_through_the_fence_and_native_pending_data
uses an actual ChannelStream release, a queued application message and native
pending data. The external owner remains through both tables and is dropped
only at native retirement. Boundless's held-OPEN bounded-account fixture covers
abandonment before confirmation through the real adapter and native opening.
