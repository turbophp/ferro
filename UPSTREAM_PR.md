# Upstream `tokio-postgres` PR (draft) — expose `ReadyForQuery` + `ParameterStatus` on `Client`

**Status: DRAFT — not yet submitted (pending maintainer sign-off / human authorization).**
**Upstream repo:** https://github.com/sfackler/rust-postgres
**PR URL:** _(none yet — filled in once a human authorizes opening it)_

This file is the standing ledger for the fork Ferro currently vendors at `vendor/tokio-postgres`
(a patched copy of `tokio-postgres` 0.7.18, wired in via the root `Cargo.toml`'s
`[patch.crates-io]`). It exists so the `/UPSTREAM_PR.md` pointer already written into `deny.toml`'s
rationale comment and the fork's own doc-comments (`vendor/tokio-postgres/src/{codec,client,
connect_raw,connection}.rs`, all added in M1-S1 Task 1/2) resolves to real content instead of a
dangling reference.

**No PR has been opened, no branch has been pushed, and no network/publish command has been run
against `github.com/sfackler/rust-postgres` or anywhere else.** Opening a PR against a third-party
repository is an outward-facing action reserved for explicit human authorization — this document is
the prepared draft, not the act of filing it.

---

## Why this is upstream-worthy

`tokio-postgres` already parses the two pieces of server-reported state a connection-pooling client
needs and then throws them away:

- Every `ReadyForQuery` message carries a 1-byte transaction-status indicator (`I`dle, `T`ransaction
  block, `E`rror'd transaction block) — the *authoritative*, server-confirmed signal for "is this
  connection safe to hand to someone else right now, and does it need a `ROLLBACK` first." Any
  connection pool that wants correct, protocol-driven pinning (rather than inferring transaction
  state by tracking which SQL keywords it thinks it sent) needs this. `tokio-postgres`'s `codec.rs`
  already decodes the `ReadyForQuery` frame; it just doesn't keep the byte anywhere reachable from
  `Client`.
- `GUC_REPORT` `ParameterStatus` messages (`server_version`, `TimeZone`, `client_encoding`, ...) are
  parsed by the spawned `Connection` driver and stored in a private `HashMap`, but there is no way
  for a `Client` handle to read the latest value without a round trip.

Both are small, purely additive, read-only accessors over data the crate is already decoding. They
add no new wire behavior, no new dependencies, and no new async surface — just two synchronous
getters on `Client`/`InnerClient`. This is the kind of feature generically useful to any consumer
building a pool or proxy on top of `tokio-postgres` (Ferro is one; PgBouncer-alikes and other
Rust-native poolers would want the same thing), not something Ferro-specific.

## Proposed PR title

> Expose `ReadyForQuery` transaction status and `ParameterStatus` values on `Client`

## Proposed PR body

> ### What
>
> Adds two small, synchronous, read-only accessors to `Client`:
>
> - `Client::transaction_status(&self) -> u8` — the raw `ReadyForQuery` status byte last reported by
>   the server (`b'I'` / `b'T'` / `b'E'`), updated on every `ReadyForQuery` the connection processes.
> - `Client::parameter(&self, name: &str) -> Option<String>` — the latest value of a `GUC_REPORT`
>   `ParameterStatus` for `name`, if the server has reported one.
>
> ### Why
>
> `tokio-postgres` already decodes both pieces of state (`ReadyForQuery`'s status byte in
> `codec::decode`; `ParameterStatus` in `Connection::poll_message`) and discards them once consumed.
> A connection pool sitting on top of `tokio-postgres` — anything that needs to know "is this
> connection idle / mid-transaction / in a failed transaction block" to decide whether it's safe to
> reuse, or wants to read `server_version`/`TimeZone` without a round trip — currently has no way to
> observe either without re-implementing wire parsing itself or tracking state heuristically
> (inferring transaction status from which statements the caller *thinks* it sent, which is wrong
> the moment a multi-statement batch or a server-side error is involved). Surfacing what the crate
> already parses closes that gap with no behavior change for existing callers.
>
> ### How
>
> - `PostgresCodec` (`codec.rs`) gains an `Arc<AtomicU8>` (`tx_status`) written with `Ordering::
>   Relaxed` on every decoded `ReadyForQuery` frame (the status byte is the last byte of that frame,
>   already fully buffered by the existing length check).
> - `connect_raw` (`connect_raw.rs`) constructs that atomic once and shares one clone with the
>   `PostgresCodec` (written by the spawned `Connection`'s decoder) and one with `InnerClient`
>   (read by the new accessor) — same construction site as the parameter map below.
> - `InnerClient` (`client.rs`) holds the shared atomic and exposes `Client::transaction_status()`
>   as a `Relaxed` load — synchronous, no round trip.
> - For `parameter()`: `connect_raw` also seeds an `Arc<Mutex<HashMap<String, String>>>` from the
>   startup `ParameterStatus` values already read in `read_info`, shares one clone with the spawned
>   `Connection` (`connection.rs`, dual-written alongside the existing private `parameters` field on
>   every `ParameterStatus` message so `Connection`'s own accessor is untouched) and one with
>   `InnerClient`, which exposes `Client::parameter(name)` as a `Mutex` lock + `get`.
>
> ### Diff surface
>
> Six touch points across four files, none behavior-changing for existing callers (purely additive
> fields + accessors):
>
> - `src/codec.rs` — `PostgresCodec` gains the `tx_status: Arc<AtomicU8>` field; `decode` writes it
>   on `ReadyForQuery`.
> - `src/connect_raw.rs` — constructs the shared atomic and the shared parameter map once, at the
>   single `PostgresCodec`/`InnerClient`/`Connection` construction site in `connect_raw`, and passes
>   one clone of each to the codec/connection and one to the client.
> - `src/client.rs` — `InnerClient` gains the `tx_status: Arc<AtomicU8>` and
>   `parameters: Arc<Mutex<HashMap<String, String>>>` fields; `Client` gains the two accessor
>   methods (`transaction_status()`, `parameter()`).
> - `src/connection.rs` — `Connection` gains the shared `shared_parameters` field, written alongside
>   the existing local `parameters` map on every `ParameterStatus` message.
>
> No public API is removed or changed; no new dependencies; no new feature flag. `cfg`-gateable
> behind nothing — this is always-on, matching how `ReadyForQuery`/`ParameterStatus` are always
> present on the wire.
>
> ### Testing
>
> The Ferro fork carries live tests against a real Postgres asserting: `transaction_status()` tracks
> idle → in-transaction → failed-transaction → idle across `BEGIN`/an erroring statement/`ROLLBACK`;
> `parameter("server_version")` is populated immediately after connect; an unreported parameter name
> reads back `None`; `SET TimeZone = 'UTC'` updates `parameter("TimeZone")` observably. Happy to
> port these into `tokio-postgres`'s own test suite as part of the PR if that's preferred over
> relying on the description above.

---

## Status and next step

**DRAFT.** This PR has not been opened. Filing it against `github.com/sfackler/rust-postgres` is a
human decision (maintainer sign-off / explicit authorization to take an outward-facing action on a
third-party repository) — not something an agent working inside this repo unilaterally does. When a
human authorizes it:

1. Open the PR from a clean branch off `tokio-postgres` 0.7.18 (or current upstream `master`,
   rebasing the fork's two commits — RFQ status, then `ParameterStatus` — accordingly).
2. Fill in the **PR URL** at the top of this file.
3. Flip **Status** to `Status: OPEN — awaiting upstream review` (then `MERGED` / `CLOSED` /
   `REJECTED` as it resolves).

## Standing note — drop the fork when this lands

**If this (or an equivalent) lands upstream, DROP the vendored fork and the `[patch.crates-io]`
entry:**

- Delete `vendor/tokio-postgres/` and the `[patch.crates-io]` block + its explanatory comment in the
  root `Cargo.toml` (currently lines ~28–33).
- Remove the `exclude = ["vendor/tokio-postgres"]` workspace entry (root `Cargo.toml`).
- Bump `ferro-backend-pg`'s `tokio-postgres` dependency to the released version that carries
  `transaction_status()`/`parameter()`.
- Remove this file's cross-references from `deny.toml`'s rationale comment and from the fork's own
  doc-comments (moot once the fork itself is deleted).
- Re-run the M1-S1 live suites (`ferro-backend-pg`'s `pg_rfq_status_it.rs`, `pg_tx_status_it.rs`,
  `pg_parameter_status_it.rs`, `pg_pool_it.rs`, and `ferrod`'s `tx_it.rs`) unchanged against the
  released crate to confirm the accessors behave identically.

Until then, the fork stays: it is the only way `ferro-pool`'s RFQ-driven pin authority
(`PoolBackend::tx_status`, wired in `ferro-backend-pg/src/conn.rs`, consumed by
`ferro-pool/src/pool.rs`'s `Checkout::apply_tx_status`) can read the real transaction-status byte
off the wire, replacing the M0 §22.1 **M-2** open item's TX-lifecycle stub (SPEC `ferro-spec-v0.2.md`
§22.1; M1 decision **M1-D2**, `docs/superpowers/specs/2026-07-28-ferro-m1-execution-design.md`).
