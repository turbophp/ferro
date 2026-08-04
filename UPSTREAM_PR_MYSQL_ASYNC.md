# Upstream `mysql_async` PR (draft) — negotiate `CLIENT_SESSION_TRACK` (surface OK-packet session trackers)

**Status: DRAFT — not yet submitted (pending maintainer sign-off / human authorization).**
**Upstream repo:** https://github.com/blackbeam/mysql_async
**PR URL:** _(none yet — filled in once a human authorizes opening it)_

This file is the standing ledger for the fork Ferro currently vendors at `vendor/mysql-async`
(a patched copy of `mysql_async` 0.37.0, wired in via the root `Cargo.toml`'s `[patch.crates-io]`).
It mirrors `/UPSTREAM_PR.md` (the `tokio-postgres` RFQ-status fork from M1-S1) so the pointer written
into the vendored fork's own doc-comment (`vendor/mysql-async/src/opts/mod.rs`,
`get_capabilities()`), the root `Cargo.toml` `[patch]` comment, and `ferro-backend-mysql`'s crate
docs resolves to real content instead of a dangling reference.

**No PR has been opened, no branch has been pushed, and no network/publish command has been run
against `github.com/blackbeam/mysql_async` or anywhere else.** Opening a PR against a third-party
repository is an outward-facing action reserved for explicit human authorization — this document is
the prepared draft, not the act of filing it.

---

## Why this is upstream-worthy

`mysql_common` (the shared protocol crate `mysql_async` already depends on) *fully parses and
publicly exposes* MySQL's OK-packet **session-state trackers** — `OkPacket::session_state_info() ->
Vec<SessionStateInfo>`, `SessionStateInfo::{data_type(), decode()}` yielding
`SessionStateChange::{SystemVariables, Schema, TransactionState, TransactionCharacteristics, Gtids,
IsTracked, Unsupported}`, and `OkPacket::status_flags()`. `mysql_async` even re-exports all of these
types from its crate root.

But per the MySQL protocol, the server only **appends** session-state-info to an OK packet when the
client advertised the `CLIENT_SESSION_TRACK` (`0x0080_0000`) capability at handshake. And
`mysql_async` never advertises it: `Opts::get_capabilities()` (`src/opts/mod.rs`) OR-s a fixed
capability set that omits `CLIENT_SESSION_TRACK`, the method is `pub(crate)`, there is no
`additional_capabilities` field on `Opts`, and no `OptsBuilder` method to add custom flags.

The net effect: on a stock `mysql_async` connection `OkPacket::session_state_info()` is **always
empty** — the entire re-exported tracker API is dead on arrival. Any consumer that wants to observe
protocol-reported session mutations (a system variable changed, the current schema changed, a
transaction's state, GTIDs) — a connection pool deciding whether a pooled connection is safe to
reuse, a proxy tracking session state, an observability layer — currently has no way to switch the
feature on without re-implementing capability negotiation.

This is the direct MySQL analog of what Ferro's `tokio-postgres` fork does for the `ReadyForQuery`
transaction-status byte (see `/UPSTREAM_PR.md`): surface a server-reported signal the crate already
knows how to parse but never enables.

## Proposed PR title

> Negotiate `CLIENT_SESSION_TRACK` so `OkPacket::session_state_info()` is populated

## Proposed PR body

> ### What
>
> Advertise the `CLIENT_SESSION_TRACK` capability during handshake so the server sends OK-packet
> session-state-info, making the already-public `OkPacket::session_state_info()` /
> `SessionStateChange` API actually return data.
>
> Minimal form: OR `CapabilityFlags::CLIENT_SESSION_TRACK` into the fixed set in
> `Opts::get_capabilities()`. Optional richer form (if preferred): add an
> `OptsBuilder::additional_capabilities(CapabilityFlags)` / `Opts` field so callers can opt in per
> connection instead of it being always-on.
>
> ### Why
>
> `mysql_common` parses and `mysql_async` re-exports the full session-tracker surface
> (`session_state_info()`, `SessionStateChange::{SystemVariables, Schema, TransactionState, …}`), but
> the server only emits those trackers when the client negotiated `CLIENT_SESSION_TRACK`. Because
> `get_capabilities()` never sets that bit — and there is no opts hook to add it — the accessor is
> always empty in practice. Connection poolers, proxies, and observability tools that want to react to
> server-reported session state (system-variable changes, schema switches, transaction state) can't,
> without vendoring the crate. Negotiating the bit closes that gap using code already present.
>
> ### How
>
> One line in `src/opts/mod.rs`, `Opts::get_capabilities()`: add
> `| CapabilityFlags::CLIENT_SESSION_TRACK` to the initial `out` set. No new wire parsing (the OK-packet
> tracker decode already exists in `mysql_common`), no new dependency, no new async surface.
>
> Note on `CLIENT_DEPRECATE_EOF` interaction: `mysql_async` already negotiates `CLIENT_DEPRECATE_EOF`,
> so result sets terminate in an OK packet whose `status_flags()` / `session_state_info()` are already
> surfaced through `Conn::last_ok_packet()`. Enabling `CLIENT_SESSION_TRACK` simply makes the
> session-state-info portion of those OK packets non-empty.
>
> ### Compatibility
>
> `CLIENT_SESSION_TRACK` is honored by MySQL ≥ 5.7 and MariaDB ≥ 10.2; older servers that do not
> advertise it back in their handshake capabilities simply won't send trackers (the intersection of
> client+server capabilities governs the wire), so advertising it is backward-safe. The added trackers
> are appended to OK packets that `mysql_async` already reads; existing callers that ignore
> `session_state_info()` are unaffected.
>
> ### Testing
>
> The Ferro fork carries a live behavioral test against a real MySQL 8 (`session_track_state_change=ON`,
> `session_track_system_variables='*'`, `session_track_transaction_info=STATE`): a
> `SET SESSION sort_buffer_size = 262144` produces a non-empty `session_state_info()` decoding a
> `SessionStateChange::SystemVariables` naming `sort_buffer_size=262144`, and `START TRANSACTION` / a
> read / `COMMIT` toggles `status_flags() & SERVER_STATUS_IN_TRANS`. A control run with the capability
> bit removed shows the same statements yield an EMPTY `session_state_info()` — confirming the bit is
> load-bearing. Happy to port a version of this into `mysql_async`'s own integration suite as part of
> the PR.

---

## Status and next step

**DRAFT.** This PR has not been opened. Filing it against `github.com/blackbeam/mysql_async` is a
human decision (maintainer sign-off / explicit authorization to take an outward-facing action on a
third-party repository) — not something an agent working inside this repo unilaterally does. When a
human authorizes it:

1. Open the PR from a clean branch off `mysql_async` 0.37.0 (or current upstream `master`), applying
   the one-line capability change.
2. Fill in the **PR URL** at the top of this file.
3. Flip **Status** to `Status: OPEN — awaiting upstream review` (then `MERGED` / `CLOSED` /
   `REJECTED` as it resolves).

## Standing note — drop the fork when this lands

**If this (or an equivalent, e.g. an `additional_capabilities` opts hook) lands upstream, DROP the
vendored fork and the `[patch.crates-io]` entry:**

- Delete `vendor/mysql-async/` and the `mysql_async` line + its explanatory comment in the root
  `Cargo.toml` `[patch.crates-io]` block.
- Remove `vendor/mysql-async` from the `exclude` list (root `Cargo.toml`).
- Bump `ferro-backend-mysql`'s `mysql_async` dependency to the released version that negotiates
  `CLIENT_SESSION_TRACK` (or set the new opts hook, if that is the form that landed).
- Re-run the M1-S6 live tracker spike (`ferro-backend-mysql`'s `tracker_spike_it.rs`) unchanged
  against the released crate to confirm `session_state_info()` is still non-empty.

Until then, the fork stays: it is the only way Ferro's MySQL pin engine (M1-S6 task 2+) can read the
OK-packet session trackers that are its authoritative signal for protocol-invisible session
mutations — the MySQL counterpart of the `tokio-postgres` RFQ-byte authority from M1-S1.
