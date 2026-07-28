# Ferro M1 · Slice S1 — Fork tokio-postgres + expose RFQ → the real PG pin engine Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.
> **v2** — adversarial plan-verification (`wf_4d926b8e-743`, FIX_FIRST) folded: fork mechanics confirmed exact; 3 blockers + 2 majors + 3 minors fixed (see Self-Review). Fork edits unchanged; the fixes are in `.gitignore`, the FakeBackend seam, and `apply_tx_status`.

**Goal:** Replace the M0 TX-lifecycle pin **stub** with **protocol-signal pinning** whose authority is PostgreSQL's `ReadyForQuery` status byte (I/T/E), surfaced by a minimal `tokio-postgres` fork; the `ferro-pool` pin state machine reads it after every statement (pin on `T`/`E`, unpin on `I`), with the S5/S6 `is_bare_tx_control` guard kept as defense-in-depth.

**Architecture:** The forked `tokio-postgres 0.7.18` gives its `PostgresCodec` an `Arc<AtomicU8>` that it writes the RFQ status into on each end-of-cycle `ReadyForQuery`; the `Client` exposes it via a synchronous `transaction_status()` (no round trip). `ferro-backend-pg`'s `PgConn` forwards it as a new `PoolBackend::tx_status(conn)`. After each `Checkout::{query,exec,tx_control,begin_tx_with,commit_tx,rollback_tx}` await returns (all of which fully drain their response before returning), the pool reads `tx_status` and updates `PinState`/`tx_open`/`tainted` from the real I/T/E — the authoritative pin. The S6 TX actor + `PinCause::Tx` are unchanged; the M0 manual pin sets and the `is_bare_tx_control` guard become defense-in-depth.

**Tech Stack:** Rust (edition 2024, tokio); a vendored/patched `tokio-postgres 0.7.18` via `[patch.crates-io]`; `postgres-protocol 0.6.12`; live PG via `testkit`.

## Global Constraints (verbatim from the spec / mechanism map — every task assumes these)

- **§7.1 — protocol signals are the authority; the lexer is assist.** PG `ReadyForQuery` (I/T/E) is authoritative for tx-pin state; `is_bare_tx_control` (S5/S6) stays as an up-front guard (defense-in-depth), never the authority. `ParameterStatus` (`GUC_REPORT`) is an **assist-only** signal (`search_path` is NOT a GUC_REPORT param) — plumb it now, consume it in S2.
- **Charter rule 2 — `/proto` is the single source of truth.** S1 adds NO wire/proto change (RFQ is a PG-backend internal signal, not a Ferro wire field). Do not touch `/proto`.
- **Charter DoD — pin-cause assertion.** Every pin path asserts `last_pin_cause()`. In S1 the only cause is `PinCause::Tx` (the assist-lexer causes land in S2); an RFQ-detected `T`/`E` on a connection is a **transaction** pin → `PinCause::Tx`.
- **One-in-flight-per-`Checkout` invariant (load-bearing for the atomic).** The shared `Arc<AtomicU8>` holds the *latest RFQ decoded off the socket*; it is per-statement-correct ONLY because the pool holds `&mut Client` exclusively per `Checkout` and each `Checkout::{query,exec,…}` fully awaits+drains before returning — never >1 in-flight statement per connection. State this precondition in code comments; if Ferro ever pipelines, this design must change (to per-stream status).
- **The lazy-stream read point.** `query_raw().await` resolves after `BindComplete`; the terminating RFQ is only decoded when the `RowStream` is fully drained. `ferro-backend-pg::query::run` ALREADY drains the stream fully before returning (`query.rs:~100-110`), so reading `tx_status` in the pool AFTER `backend.query(...).await` returns is post-drain and valid. `simple_query`/`execute`/`batch_execute` drain synchronously inside their await. Do NOT read `tx_status` mid-stream.
- **The post-drain guarantee holds ONLY on the success (`Ok`) arm** (verification MAJOR). On an error, `postgres-protocol`'s `Responses` stream returns `Err` at the `ErrorResponse` WITHOUT consuming the trailing `ReadyForQuery` (a fragmented `ErrorResponse`-then-`RFQ` leaves the atomic holding a **stale** byte). Therefore every instrumented method reads `tx_status` on BOTH arms but does NOT trust the atomic on `Err`: structure as `let r = self.pool.backend.query(...).await; let st = self.pool.backend.tx_status(self.conn_ref()); self.apply_tx_status(st); if r.is_err() { self.tainted = true; } r` — never `?` the backend call before the read, and force `tainted=true` on ANY `Err` while `tx_open` (independent of reading `E`). This is what keeps a statement that opens-a-tx-then-errors (reachable via `Checkout::exec` `batch_execute`, e.g. a leading-keyword-passes-guard `UPDATE ...; BEGIN; SELECT 1/0`) from returning an open/aborted tx that looks clean → a cross-tenant stuck pin. State this success-only scope in a code comment at each read site.
- **The vendored fork MUST be committable (verification BLOCKER).** Repo `.gitignore:11` is `**/vendor`, which matches `vendor/tokio-postgres/**` — confirmed: `git check-ignore -v vendor/tokio-postgres/src/codec.rs` → `.gitignore:11:**/vendor`. Left as-is, `git add vendor/tokio-postgres` silently skips the whole fork, CI's clean checkout has no vendor tree, and `[patch.crates-io] tokio-postgres = { path = "vendor/tokio-postgres" }` fails to resolve → the rust, integration, AND deny jobs all fail before compiling (invisible locally, where the patch resolves against the dev's own `vendor/`). Task 1 MUST narrow the ignore to `php/**/vendor` (the ignore was for PHP Composer trees) and verify `git check-ignore vendor/tokio-postgres/src/codec.rs` returns nothing + `git status` shows the tree staged.
- **Fresh connection = `I`.** Initialize the atomic to `b'I'` (the startup RFQ is always `I`).
- **`E` = failed tx block.** Treat `E` as `tx_open && tainted` (must be `ROLLBACK`'d before reuse) — dovetails with the existing checkout-time defensive-ROLLBACK/reset recycle (`pool.rs:~137-157`).
- **Charter gates** (`cargo fmt --check`, `clippy --workspace --all-targets -D warnings`, `cargo test --workspace`, `cargo-deny`) green with the fork in the workspace; live PG tests skip without `FERRO_TEST_PG_URL`.

## File Structure

```
.gitignore                              BLOCKER FIX: `**/vendor` → `php/**/vendor` so the Rust fork is committable
Cargo.toml                              + [patch.crates-io] tokio-postgres = { path = "vendor/tokio-postgres" }
vendor/tokio-postgres/                  the fork (vendored copy of 0.7.18 + the RFQ/ParameterStatus patch)
  src/codec.rs                          PostgresCodec gains `tx_status: Arc<AtomicU8>`; writes RFQ status byte in decode
  src/connect_raw.rs                    create the Arc(AtomicU8::new(b'I')), thread it to codec + Client
  src/client.rs                         InnerClient gains tx_status; + `Client::transaction_status() -> u8`; + `Client::parameter(name)`
  src/connection.rs                     publish ParameterStatus into a shared param map (assist)
deny.toml                               RATIONALE COMMENT ONLY (see Task 1) — no functional change; CI runs `check bans`, not `check sources`
engine/crates/ferro-pool/src/
  backend.rs                            + `TxStatus` enum + `fn tx_status(&self, conn) -> TxStatus` on PoolBackend
  fake.rs                               FakeBackend::tx_status (settable) for deterministic pin tests
  pin.rs                                + RFQ→pin mapping helper; PinCause::Tx for an RFQ-detected tx
  pool.rs                               Checkout::{query,exec,tx_control,begin_tx_with,commit_tx,rollback_tx}: post-await tx_status read → pin authority
engine/crates/ferro-backend-pg/src/
  conn.rs                               PgBackend::tx_status → conn.client.transaction_status() mapped to TxStatus
  (query.rs unchanged — it already drains fully before returning)
UPSTREAM_PR.md (or a ledger note)       the filed tokio-postgres upstream PR link + the "drop the fork if merged" note
```

---

### Task 1: Fork tokio-postgres 0.7.18 — expose RFQ via `Client::transaction_status()`

**Files:** Modify `.gitignore` (BLOCKER FIX — do this FIRST); Create `vendor/tokio-postgres/` (vendored 0.7.18); Modify `vendor/tokio-postgres/src/{codec.rs,connect_raw.rs,client.rs}`; Modify root `Cargo.toml` (`[patch.crates-io]`); Modify `deny.toml` (comment only).

**Interfaces produced:** `tokio_postgres::Client::transaction_status(&self) -> u8` (returns `b'I'`/`b'T'`/`b'E'`).

- **BLOCKER FIX FIRST — make the fork committable.** Edit `.gitignore:11` from `**/vendor` to `php/**/vendor` (the ignore existed for PHP Composer `vendor/` trees under `php/`; the Rust fork lives at repo-root `vendor/`). **Verify** immediately: `git check-ignore vendor/tokio-postgres/src/codec.rs` must print NOTHING (exit 1), and after `git add vendor/tokio-postgres` a `git status` must show the tree staged. Without this the entire fork is silently un-committed and every clean CI checkout fails to resolve the `[patch.crates-io]` path (see Global Constraints). Confirm the two existing PHP vendor ignores (`php/client/vendor` etc.) still match under the narrowed pattern.
- Vendor the crate: `cp -r ~/.cargo/registry/src/*/tokio-postgres-0.7.18 vendor/tokio-postgres` (confirm the version from `Cargo.lock`); add `[patch.crates-io] tokio-postgres = { path = "vendor/tokio-postgres" }` to the root `Cargo.toml`; `cargo build -p ferro-backend-pg` builds against the vendored copy (zero code change yet — prove the patch resolves).
- Patch `src/codec.rs`: change `pub struct PostgresCodec;` → `pub struct PostgresCodec { pub tx_status: std::sync::Arc<std::sync::atomic::AtomicU8> }`. In `decode`, at the `READY_FOR_QUERY_TAG` branch (where it currently sets `request_complete = true` after `idx += len`), also store the status byte — the RFQ body is `src[idx-len..idx]` and its 1-byte status is the last byte: `self.tx_status.store(src[idx - 1], std::sync::atomic::Ordering::Relaxed);` (bounds already guaranteed by the codec's prior `src[idx..].len() < len` check). Fix the other `PostgresCodec` construction sites the vendored crate has (there may be a `Framed::new(_, PostgresCodec)` elsewhere — grep and thread the field, or default it).
- Patch `src/connect_raw.rs`: before building the `Framed`, `let tx_status = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(b'I'));`; construct the codec as `PostgresCodec { tx_status: tx_status.clone() }`; pass `tx_status.clone()` into `Client::new(...)`.
- Patch `src/client.rs`: add `tx_status: Arc<AtomicU8>` to `InnerClient` + `Client::new`; add `pub fn transaction_status(&self) -> u8 { self.inner.tx_status.load(std::sync::atomic::Ordering::Relaxed) }`. NOTE (verification MINOR): `client.rs` imports only `std::sync::Arc` (line ~35), NOT `AtomicU8` — add `use std::sync::atomic::AtomicU8;` (or fully-qualify the field type) or the literal edit fails to compile.
- `deny.toml`: **comment only — no functional change** (verification MINOR). CI's deny job runs `cargo-deny check bans` (ci.yml:55), and a path `[patch]` is not a bans violation; there is no `[sources]` section, so `check sources` never runs — a functional source-allow edit is a no-op for the gate and risks an implementer adding a malformed `[sources]` block that breaks `bans` parsing. Add ONLY a one-line rationale comment pointing at `UPSTREAM_PR.md` (M1-D2: RFQ-access fork; drop when upstream merges).
- **TDD:** a live PG integration test in `ferro-backend-pg` (skip without `FERRO_TEST_PG_URL`): open a raw `PgBackend` conn, assert `transaction_status()==b'I'` after `SELECT 1`; run `BEGIN` → `b'T'`; run a failing statement (`SELECT 1/0`) → `b'E'`; `ROLLBACK` → `b'I'`; `COMMIT` after a fresh `BEGIN; INSERT` → `b'I'`. (This proves the fork end-to-end; it is the S1 foundation.) Plus a unit test on the codec if practical (feed a raw RFQ frame → the atomic updates).
- **Gate:** `cargo build --workspace` (the fork compiles into the workspace); `cargo test -p ferro-backend-pg` (live RFQ test passes); `cargo-deny check bans` (fork allow-listed); fmt/clippy clean on the fork + workspace.
- **Commit** `feat(m1-s1): fork tokio-postgres 0.7.18 — Client::transaction_status() from ReadyForQuery (I/T/E)`.

---

### Task 2: Fork — expose `GUC_REPORT` `ParameterStatus` via `Client::parameter(name)` (assist plumbing)

**Files:** Modify `vendor/tokio-postgres/src/{connection.rs,client.rs,connect_raw.rs}`.

**Interfaces produced:** `tokio_postgres::Client::parameter(&self, name: &str) -> Option<String>` (latest `ParameterStatus` value for `name`).

- The `Connection` already handles `ParameterStatus` at its `BackendMessage::Async(Message::ParameterStatus(body))` arm (feeding a local `parameters` map). Add a shared `Arc<Mutex<HashMap<String,String>>>` (or `arc_swap` for lock-free reads) created in `connect_raw` (seed it from the startup `parameters`); one clone stays in `Connection` (written in that arm — `map.insert(body.name()?.to_owned(), body.value()?.to_owned())`), one clone goes to `InnerClient`. Add `Client::parameter`.
- **TDD:** live PG — `SET client_min_messages = warning` then assert nothing (not GUC_REPORT), but `SET application_name = 'ferro'`… note most SETs are not GUC_REPORT; use a known GUC_REPORT param: assert `client.parameter("server_version").is_some()` (server_version is reported at startup) after connect; and after `SET TimeZone='UTC'`, `client.parameter("TimeZone") == Some("UTC")` (TimeZone IS GUC_REPORT). Keep this test small — ParameterStatus is assist-only; the CONSUMER is S2.
- **Gate:** `cargo test -p ferro-backend-pg` (the parameter test passes); fmt/clippy/build clean.
- **Commit** `feat(m1-s1): fork tokio-postgres — Client::parameter() from GUC_REPORT ParameterStatus (assist plumbing)`.

---

### Task 3: `ferro-pool` — `TxStatus` enum + `PoolBackend::tx_status` + backend impls

**Files:** Modify `engine/crates/ferro-pool/src/backend.rs`, `engine/crates/ferro-pool/src/fake.rs`, `engine/crates/ferro-backend-pg/src/conn.rs`.

**Interfaces produced:** `enum TxStatus { Idle, InTx, Failed }`; `PoolBackend::tx_status(&self, conn: &Self::Conn) -> TxStatus` (synchronous, mirrors `is_closed`).

- `backend.rs`: define `pub enum TxStatus { Idle, InTx, Failed }` (with a `from_pg_byte(u8) -> TxStatus` helper: `b'T'→InTx, b'E'→Failed, _→Idle`); add `fn tx_status(&self, conn: &Self::Conn) -> TxStatus;` to the `PoolBackend` trait (a synchronous fn, next to `is_closed`).
- `ferro-backend-pg/src/conn.rs`: `impl` it — `TxStatus::from_pg_byte(conn.client.transaction_status())`.
- `fake.rs`: **the fake must MODEL RFQ, not default to `Idle` (verification BLOCKER).** A fake that returns `Idle` unconditionally makes the pool's post-await `apply_tx_status(Idle)` **clobber** the manual pin that `begin_tx_with` just set — the real `S4`/`S6` pool + tx tests go RED (the Task-4 gate's own "still green" claim would be false), and live PG *hides* it (real `BEGIN`→`T`→`apply_tx_status(InTx)` keeps the pin). Store the status **per-`FakeConn`** (a `tx_status: TxStatus` field on `FakeConn`, defaulting `Idle` on checkout — matching real per-`Client` semantics, NOT a shared `FakeBackend` field), and have `simple_query`/`query`/`exec` (which already `push` to `conn.recorded`) UPDATE it by inferring from the SQL's leading keyword, reusing the existing `pin::is_bare_tx_control`-style scan: leading `BEGIN`/`START TRANSACTION` → `InTx`; leading `COMMIT`/`ROLLBACK`/`END`/`ABORT`/`RELEASE` → `Idle`. Plus a `set_tx_status(TxStatus)` test hook on `FakeConn` (or an injectable override) to drive the `Failed` (`E`) case a keyword can't express. `PoolBackend::tx_status(&self, conn)` returns `conn.tx_status`. This makes `apply_tx_status(InTx)` after a `BEGIN` PRESERVE the pin, so the existing S4/S6 tests pass UNCHANGED.
- **TDD:** unit — a `FakeConn` after a recorded `BEGIN` reports `tx_status()==InTx`; after `COMMIT`/`ROLLBACK` → `Idle`; `set_tx_status(Failed)` then `tx_status()==Failed`; a fresh checked-out `FakeConn` defaults to `Idle`; `from_pg_byte` maps `b'I'/b'T'/b'E'` correctly and any other byte → `Idle`.
- **Gate:** `cargo test -p ferro-pool -p ferro-backend-pg` (incl. that every existing `PoolBackend` impl — Pg + Fake — now implements `tx_status`, so the workspace compiles); fmt/clippy/build clean.
- **Commit** `feat(m1-s1): PoolBackend::tx_status + TxStatus (RFQ status surfaced to the pool)`.

---

### Task 4: `ferro-pool` — RFQ-driven pin authority in `Checkout` (the pin engine)

**Files:** Modify `engine/crates/ferro-pool/src/pin.rs`, `engine/crates/ferro-pool/src/pool.rs`; Tests `ferro-pool/tests/{pin_stub.rs,tx_api.rs,query_guard.rs,pool_semantics.rs}` (the `pool.rs` change is additive but touches these existing suites — see gate) + `ferro-backend-pg/tests/pg_pool_it.rs` + `ferrod/tests/tx_it.rs` (the S6 actor path, verified at Task 4 not Task 5).

**Interfaces consumed:** `PoolBackend::tx_status` (Task 3), `TxStatus` (Task 3).
**Interfaces produced:** a private `Checkout::apply_tx_status(&mut self, st: TxStatus)` that updates `pin`/`tx_open`/`tainted`/`last_pin_cause`.

- `pin.rs`: add a mapping helper. **Do NOT fabricate a `PinnedTx(sentinel)` — `PinState` is ONLY `Unpinned | PinnedTx(TxId)` (verified pin.rs:21-24); a "PinnedTx without a TxId" variant does not exist and is unbuildable as written** (verification BLOCKER). The rule, expressed as what `apply_tx_status` does to the reuse-safety bits vs the identity bits, separately:
  - **Reuse-safety bits — set UNCONDITIONALLY from I/T/E** (these are what protect the next tenant): `Idle → tx_open=false` (leave `tainted` as-is — a clean `I` doesn't clear a prior taint; the checkout-time recycle at pool.rs:137-149 does); `InTx → tx_open=true`; `Failed → tx_open=true, tainted=true`.
  - **Identity/cause bits — set only WITHOUT clobbering a real `TxId`:** set `last_pin_cause=Some(PinCause::Tx)` when `InTx`/`Failed`. For `pin`: if already `PinnedTx(real_id)` (the pool opened this tx via `begin_tx_with`), LEAVE it — never overwrite a real `TxId`. For an RFQ-only-detected `T`/`E` the pool did NOT open (a leaked/guard-bypassed tx), LEAVE `pin=Unpinned` and rely on `tx_open`/`tainted` to force the checkout-time ROLLBACK/reset — do NOT invent a `TxId`. (Reuse-danger is fully carried by `tx_open`/`tainted`; the `TxId` is only an identity for the S6 actor, which always goes through `begin_tx_with`.) Document this rule inline.
- `pool.rs`: in `Checkout::query`, `Checkout::exec`, `Checkout::tx_control`, `Checkout::begin_tx_with`, `Checkout::commit_tx`, `Checkout::rollback_tx` — read `tx_status` on BOTH the `Ok` and `Err` arms (the post-drain guarantee is success-only; see Global Constraints). Structure EACH as: `let r = self.pool.backend.{query,simple_query}(...).await; let st = self.pool.backend.tx_status(self.conn_ref()); self.apply_tx_status(st); if r.is_err() && self.tx_open { self.tainted = true; } r` — **never `?` the backend call before the read**, and force `tainted=true` on any `Err` while a tx is open (the atomic may hold a stale byte on a fragmented `ErrorResponse`-then-`RFQ`). This makes RFQ the authority on success and fails safe on error. Keep the existing manual sets in `begin_tx_with`/`commit_tx`/`rollback_tx` as **defense-in-depth** (they set the `TxId`/cause; RFQ then confirms `tx_open`) — do not delete them. Keep the `is_bare_tx_control` guard. Add a one-line comment at each read site stating the success-only scope.
- **TDD (unit, FakeBackend — deterministic):** drive the per-`FakeConn` status (via the inferred keyword or `set_tx_status`) around a `Checkout::exec`/`begin_tx_with`: after a `begin_tx_with(TxId, "BEGIN")` the conn is `InTx` → `tx_open`, `pin==PinnedTx(that_id)` (the real TxId is PRESERVED, not clobbered by `apply_tx_status(InTx)`), `last_pin_cause==Some(Tx)`; a subsequent `Idle` (commit) → `!tx_open`; `set_tx_status(Failed)` then a `Checkout::exec` → `tx_open && tainted`, `last_pin_cause==Some(Tx)`, and `pin` stays whatever it was (Unpinned for an RFQ-only `E`, or the real `PinnedTx(id)` inside a tx — never a fabricated TxId); a sequence `InTx`(begin) → `InTx`(stmt) → `Idle`(commit) leaves the conn unpinned + reusable. **Err-arm safety (verification MAJOR):** make the fake's `query`/`exec` return `Err` while its `tx_status` is `InTx` (or arrange a stale atomic) and assert `apply_tx_status` + the `is_err()&&tx_open` guard still leave `tainted==true` (a failed statement inside a tx must never look clean). **Failed-then-rolled-back (verification MINOR, documented):** `Failed`(stmt) → explicit `rollback_tx` → `Idle` clears `tx_open` but `tainted` stays true, so the next checkout eats one DISCARD-ALL reset — assert this is the behavior (it is safe/conservative, not a bug). Assert the **pin-cause DoD** in each.
- **TDD (live PG, skip without `FERRO_TEST_PG_URL`) — the S1 acceptance:** via `Checkout` against real PG: (a) `begin_tx_with("BEGIN")` then two statements then `commit_tx` — all on the SAME `pg_backend_pid` (pinned), and after commit the RFQ is `I` → `!tx_open`; (b) a **failed** statement mid-tx (`SELECT 1/0`) leaves `tx_status==Failed` → the pin is HELD (`tx_open && tainted`) until `rollback_tx`, after which RFQ is `I`; (c) an **autocommit** `Checkout::query("SELECT 1")` NEVER pins (`tx_status==Idle`, `!tx_open`); (d) the pin-cause is `PinCause::Tx` for the tx case. These are the exec-design S1 gates.
- **Gate:** `cargo test -p ferro-pool` (unit) + live `cargo test -p ferro-backend-pg` (the pg_pool_it acceptance) + `cargo test -p ferrod tx` (the S6 actor path). The existing `ferro-pool` suites (`pin_stub.rs`, `tx_api.rs`, `query_guard.rs`, `pool_semantics.rs`) and `ferrod/tests/tx_it.rs` must stay green UNCHANGED — this holds ONLY because the fake infers `InTx` from a recorded `BEGIN` (Task 3 fix), so `apply_tx_status(InTx)` PRESERVES the pin rather than clobbering it. If any of these needs edits to pass, STOP: the fix belongs in the fake's status-inference, NOT in weakening `apply_tx_status`. fmt/clippy/build clean.
- **Commit** `feat(m1-s1): RFQ-driven pin authority in Checkout (pin on T/E, unpin on I; guard + manual sets kept as defense-in-depth)`.

---

### Task 5: Reconcile with the S6 TX actor + file the upstream PR + whole-branch pass

**Files:** Modify `engine/crates/ferrod/src/tx/actor.rs` (comments/asserts only if needed); Create `UPSTREAM_PR.md` (or a ledger note); Tests: extend `ferrod` tx live tests.

- Confirm the S6 TX actor path (`co.query` Exec, `commit_tx`, `rollback_tx`, `tx_control`) now gets authoritative `tx_open`/`tainted` from the RFQ read (Task 4) — a statement that flips the connection to `E` (failed tx) inside the actor is detected authoritatively; the actor's teardown `set_tainted(true)` becomes belt-and-braces. Add a live test: a tx-scoped `EXEC` that errors (e.g. a constraint violation) leaves the tx in `E` (RFQ), and the actor's ROLLBACK/teardown returns a CLEAN conn (a subsequent checkout gets `I`).
- File the **upstream `tokio-postgres` PR** exposing `transaction_status()` (a small, general feature); record the PR link + the "drop the vendored fork if it merges" note in `UPSTREAM_PR.md` and reference it in `deny.toml`'s rationale.
- **Gate:** `cargo test --workspace` (offline) + the live suite (PG); charter gates green; `/proto` untouched (regen zero-diff trivially holds).
- **Commit** `feat(m1-s1): S6 actor uses RFQ tx-status authority + file tokio-postgres upstream PR + close the M-2 open item`.

---

## Self-Review (author against the spec + mechanism map)

- **Spec coverage (exec-design S1 gate):** the fork surfaces RFQ (T1) + ParameterStatus (T2); `PoolBackend::tx_status` (T3); RFQ-driven pin authority replacing the stub (T4) with the live gates (BEGIN..COMMIT pins one pid + unpins on I; a failed E holds the pin until ROLLBACK; autocommit never pins; pin-cause asserted); the S6 actor reconciliation + the upstream PR (T5). §7.1 "protocol signals first" realized; §22.1 M-2 open item closed.
- **The lazy-stream trap is handled:** the pool reads `tx_status` AFTER `backend.query` returns, and `query::run` drains the RowStream before returning — so the read is post-drain. Stated in Global Constraints; the live `SELECT`-with-rows test (T4c) proves it (a buffered read that drains).
- **Defense-in-depth preserved:** `is_bare_tx_control` guard + the manual `begin_tx_with`/`commit_tx`/`rollback_tx` pin sets stay; RFQ is additive authority, not a replacement of the guard.
- **Plan-verification DONE → this is v2 (FIX_FIRST, folded).** The adversarial pass (workflow `wf_4d926b8e-743`, 4 probes / 10 findings) confirmed the fork mechanics are EXACTLY correct against the real 0.7.18 source (single `PostgresCodec` construction site at `connect_raw.rs:102`; `src[idx-1]` is the RFQ status byte with the bounds guard already present; one shared `Arc<AtomicU8>` covers handshake + live traffic; the patch-path crate is not bound by the workspace `forbid(unsafe)`/clippy-deny) and the post-drain read is valid on every SUCCESS path. Three blockers were folded above: (1) the `.gitignore:11 **/vendor` swallow → narrowed to `php/**/vendor` + a commit-verify step (Task 1); (2) the `FakeBackend` `Idle`-default clobbering the pin → the fake now INFERS `InTx`/`Idle` from recorded SQL per-`FakeConn` (Task 3), keeping the S4/S6 suites green unchanged; (3) `apply_tx_status` referencing a nonexistent `PinnedTx`-without-`TxId` variant + reading only on `Ok` → rewritten to set reuse-bits unconditionally on I/T/E, never clobber a real `TxId`, never fabricate a sentinel, and read+`tainted`-on-`Err` (Task 4). Two majors folded: the `ferrod tx_it`/`pin_stub`/`tx_api` suites are named in Task 4's file list + gate; the success-only scope of the post-drain guarantee is stated in Global Constraints + at each read site. Minors folded: the `AtomicU8` import (Task 1), the deny.toml comment-only downgrade (Task 1), the documented failed-then-rollback extra reset (Task 4).

## Execution Handoff

Subagent-driven (the established M0 discipline): fresh implementer per task (TDD, gates), review after each (probe the RFQ-authority correctness + the pin-cause DoD + the defense-in-depth invariants), whole-branch review before S2. Live tests against the S2 Docker PG.
