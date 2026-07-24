# Ferro M0 · Slice S4 — Hand-rolled PG Pool + Stubbed Pin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A hand-rolled connection pool (`ferro-pool`) on a backend-agnostic `PoolBackend` trait — checkout/release with bounded `max_size`, `queue_us` timing, `checkout_timeout`, `max_lifetime` recycling, a background liveness reaper, dead-connection eviction, and a **stubbed pin state machine** (pins on the TX lifecycle, since stock `tokio-postgres` exposes no ReadyForQuery byte — decision M-2). A `ferro-backend-pg` crate implements the trait on `tokio-postgres`. A **fake in-memory backend** makes pool semantics fast to test; **live tests run against the S2 Dockerized Postgres** (`FERRO_TEST_PG_URL`, skip when unset). No SQL EXEC service yet (that is S5) — S4 delivers the pool + a minimal "acquire a connection, run `SELECT 1`, release" path.

**Architecture:** `ferro-pool` owns the pool mechanics + pin state machine over a `PoolBackend` trait (so MySQL/SQLite plug in later and the fake backend tests the mechanics without a DB). `ferro-backend-pg::PgConn` implements `PoolBackend` on `tokio-postgres` (NoTls). Checkout is semaphore-bounded; `queue_us` (pool wait) is measured first-class. The pin stub records `PinCause::Tx` on BEGIN, driven by a per-conn tx-open flag, with a defensive `ROLLBACK` on release; **bare tx-control SQL sent outside the TX service is rejected as `Unsupported`** so the stub cannot be bypassed. `cargo-deny` (S2) already forbids `deadpool`/`bb8` (decision D9).

**Tech Stack:** Rust 1.95, `tokio` (sync, time, rt), `tokio-postgres` (NoTls, no TLS in M0), `thiserror`, `tracing`, `ferro-proto` (error taxonomy tags). Dev: the fake backend; `FERRO_TEST_PG_URL` → the S2 Docker PG (`postgres://ferro:ferro@localhost:55432/ferro`).

## Global Constraints

- **Hand-rolled pool (D9):** no `deadpool`/`bb8` (cargo-deny enforces). The pin state machine + checkout mechanics ARE the product.
- **Stubbed pin (M-2):** M0 pins on the TX-service lifecycle, NOT ReadyForQuery (stock `tokio-postgres` exposes no I/T/E byte). `PinState { Unpinned, PinnedTx(tx_id) }`, `PinCause` with only `Tx` emitted in S4; per-conn tx-open flag; defensive `ROLLBACK` on release of any connection that served a transaction; a pinned connection is NEVER handed to a second checkout. The RFQ-byte dependency is a §21 open item for the M1 real pin engine.
- **Pin-cause assertion (charter DoD):** pin-engine work asserts the pin cause label — S4 tests assert `PinCause::Tx` on BEGIN and `Unpinned` after COMMIT/ROLLBACK.
- **`queue_us` is first-class** (SPEC §6/§16): checkout measures pool-wait time separately (the KPI that says "grow max_size").
- **Errors map to the taxonomy v0** (SPEC §9.2): `PoolTimeout`/`ConnectionLost` → `Retryable`; use `ferro_proto::consts::errc` codes where a wire error is produced (the pool's own errors are a Rust enum that maps to the taxonomy; no hand-written protocol numbers).
- **No transparent retry of user statements** (charter rule 3): the pool evicts a dead connection but NEVER re-runs the user's statement.
- **Integration tests skip without `FERRO_TEST_PG_URL`** (S2 convention) — offline `cargo test --workspace` stays green via the fake backend.
- **Charter gates:** `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace`.

## File Structure

```
/engine/crates/ferro-pool/
  Cargo.toml
  src/lib.rs
  src/backend.rs      PoolBackend trait (async connect/ping/is_closed/reset/exec-simple) + Conn assoc type
  src/config.rs       PoolConfig { max_size, checkout_timeout, max_lifetime, reap_interval, pin_functions? }
  src/pin.rs          PinState, PinCause, per-conn pin tracking + defensive rollback hook
  src/pool.rs         the Pool: semaphore, idle stack, checkout (queue_us)/release, max_lifetime, reaper
  src/health.rs       liveness reaper + checkout-time is_closed()/age checks, connect backoff
  src/error.rs        PoolError (-> taxonomy v0) via thiserror
  src/fake.rs         FakeBackend (in-memory) for fast, deterministic pool-semantics tests
  tests/pool_semantics.rs   tests/pin_stub.rs
/engine/crates/ferro-backend-pg/
  Cargo.toml
  src/lib.rs
  src/connect.rs      PgConfig from a DSN/url; connect() -> PgConn (tokio-postgres NoTls, spawn the connection driver)
  src/conn.rs         PgConn implementing ferro_pool::backend::PoolBackend (ping SELECT 1, is_closed, reset=DISCARD ALL, exec-simple)
  src/tx.rs           BEGIN/COMMIT/ROLLBACK helpers + the tx-open flag that drives the pin stub
  src/hygiene.rs      release-time defensive ROLLBACK (M0 minimal hygiene; full conditional hygiene is M1)
  tests/pg_pool_it.rs   (skips without FERRO_TEST_PG_URL)
```

Both crates join the workspace via the `engine/crates/*` glob.

---

### Task 1: `ferro-pool` crate + `PoolBackend` trait + fake backend

**Files:** Create `ferro-pool/{Cargo.toml, src/lib.rs, src/backend.rs, src/error.rs, src/fake.rs}`, `tests/backend_smoke.rs`.

**Interfaces:**
- `PoolBackend` (async trait via `async fn` in trait or `#[async_trait]` — pick per toolchain; edition 2024 supports async-fn-in-trait): `type Conn: Send`; `async fn connect(&self) -> Result<Self::Conn, PoolError>`; `async fn ping(conn: &mut Self::Conn) -> Result<(), PoolError>`; `fn is_closed(conn: &Self::Conn) -> bool`; `async fn reset(conn: &mut Self::Conn) -> Result<(), PoolError>` (hygiene); plus a minimal `async fn simple_query(conn: &mut Self::Conn, sql: &str) -> Result<u64, PoolError>` so tests can drive `SELECT 1`/BEGIN/COMMIT without the full SQL service.
- `FakeBackend`: an in-memory backend whose `Conn` is a struct with an `id`, a `closed` flag, an `age`, and a scriptable `fail_next` — so tests deterministically exercise dead-conn eviction, ping failure, reset, and `simple_query` recording (records the SQL it "ran" so pin tests can assert BEGIN/COMMIT were seen).
- `PoolError` (thiserror): `Timeout`, `ConnectionLost`, `Backend(String)`, `Closed` → each has a `taxonomy_branch()` (Retryable/NonRetryable) mapping.

- [ ] **TDD:** write `tests/backend_smoke.rs` first (RED): a `FakeBackend` connects, `ping` succeeds, `is_closed` reflects the flag, `simple_query("select 1")` records the SQL and returns; a `fail_next`-armed conn makes `ping` return `ConnectionLost`. Implement the trait + fake to green.
- [ ] **Gates + commit** `feat(s4): ferro-pool crate + PoolBackend trait + fake in-memory backend`.

---

### Task 2: the Pool — checkout/release, max_size, queue_us, checkout_timeout

**Files:** Create `src/config.rs`, `src/pool.rs`; extend `lib.rs`; `tests/pool_semantics.rs`.

**Interfaces:**
- `PoolConfig { max_size: usize, checkout_timeout: Duration, max_lifetime: Duration, reap_interval: Duration }` with sane defaults.
- `Pool<B: PoolBackend>`: `Pool::new(backend, config)`; `async fn checkout(&self) -> Result<Checkout<B>, PoolError>` returns a RAII guard that `Deref`s to `&mut B::Conn` and on `Drop` returns the conn to the idle set (or discards if tainted/closed). Checkout: acquire a `tokio::sync::Semaphore` permit bounded by `max_size` (measuring `queue_us` = time spent awaiting the permit + a healthy idle conn), pop an idle conn (or `connect()` a new one up to max_size), run the checkout-time health check (cheap `is_closed()` + age vs `max_lifetime`), and pipeline hygiene later (S4 keeps hygiene minimal). `checkout()` respects `checkout_timeout` → `PoolError::Timeout` (Retryable{PoolTimeout}).
- `Checkout` exposes `stats() -> CheckoutStats { queue_us: u64 }`.

- [ ] **TDD (fake backend, fast, deterministic — use `tokio::time` with `start_paused` where helpful):**
  - `checkout_release_reuse`: checkout, release, checkout again → same conn id (reuse), pool never exceeds `max_size` live conns.
  - `max_size_blocks_then_times_out`: with `max_size=1`, hold one checkout, a second `checkout()` blocks; with a short `checkout_timeout` it returns `Err(Timeout)` (a Retryable). `queue_us` on the timed-out/queued path is > 0.
  - `evicts_dead_connection`: mark an idle conn `closed`; next checkout detects it (`is_closed`) and evicts + connects a fresh one (different id), never handing out the dead one.
- [ ] **Gates + commit** `feat(s4): hand-rolled pool checkout/release (max_size, queue_us, checkout_timeout)`.

---

### Task 3: max_lifetime recycling + liveness reaper + connect backoff

**Files:** Create `src/health.rs`; extend `pool.rs`; extend `tests/pool_semantics.rs`.

**Interfaces:** background reaper task (per pool) that periodically pings idle conns and closes ones past `max_lifetime` or failing ping; checkout-time age check recycles a too-old conn; connect failures use jittered exponential backoff (base 10ms, cap 1s) before surfacing `ConnectionLost`. Reaper is cancel-safe (stops on pool drop).

- [ ] **TDD (fake backend + `tokio::time::pause`/`advance`):**
  - `max_lifetime_recycles`: a conn older than `max_lifetime` is not reused — checkout returns a fresh conn (assert a new id); with paused time, `advance` past `max_lifetime` then checkout.
  - `reaper_closes_stale_idle`: advance time; the reaper closes an idle conn past lifetime (idle count drops / a subsequent checkout reconnects).
  - `connect_backoff_then_error`: a backend whose `connect` fails N times then... surfaces `ConnectionLost` after the deadline (assert backoff was applied — e.g. count attempts, or that it took ≥ the base backoff under paused time).
- [ ] **Gates + commit** `feat(s4): max_lifetime recycling + liveness reaper + connect backoff`.

---

### Task 4: stubbed pin state machine (TX-lifecycle, defensive rollback, pin-cause)

**Files:** Create `src/pin.rs`; extend `pool.rs`, `backend.rs`; `tests/pin_stub.rs`.

**Interfaces:**
- `PinState { Unpinned, PinnedTx(TxId) }`, `PinCause { Tx /* only Tx in S4 */ }`, `TxId(u64)`.
- Per-conn pin tracking: the pool marks a checked-out conn `PinnedTx(tx_id)` when a BEGIN is observed through the pin hook, records `PinCause::Tx`, and keeps it pinned (not returned to the shared idle set) until COMMIT/ROLLBACK returns it to `Unpinned`. A pinned conn is NEVER handed to a second checkout.
- The pin hook is driven by the TX lifecycle (S5/S6 call it on BEGIN/COMMIT/ROLLBACK); for S4, expose `Checkout::begin_tx(tx_id)` / `commit_tx()` / `rollback_tx()` that update the pin state + set/clear the per-conn tx-open flag, and drive `simple_query("BEGIN"/"COMMIT"/"ROLLBACK")` through the backend.
- **Defensive release:** on `Checkout` Drop, if the conn's tx-open flag is set (a tx was left open), issue a `ROLLBACK` (via the backend) before returning it to idle — belt-and-suspenders (M0 minimal hygiene).
- **Guard:** a `simple_query` whose text is bare tx-control (`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`…) sent OUTSIDE the pin hook is rejected as `PoolError::Unsupported` so the stub cannot be bypassed (the real path is the TX service).

- [ ] **TDD (fake backend records SQL + exposes pin state):**
  - `pin_stub_tx_cause`: `checkout` → `begin_tx(TxId(1))` → assert pin state `PinnedTx(1)` and `PinCause::Tx` recorded; `commit_tx()` → `Unpinned`; the fake recorded `["BEGIN","COMMIT"]`.
  - `pinned_conn_not_reused`: hold conn A pinned (begin_tx, don't commit); a concurrent checkout does NOT get A (gets a different conn or blocks); release A (rollback) → A returns to idle.
  - `defensive_rollback_on_drop`: begin_tx then drop the Checkout without commit → the fake records a trailing `ROLLBACK` (the defensive release fired) and the conn is `Unpinned` on return.
  - `bare_tx_control_via_simple_query_rejected`: `simple_query("BEGIN")` (not via begin_tx) → `Err(Unsupported)`.
- [ ] **Gates + commit** `feat(s4): stubbed pin state machine (TX-lifecycle, pin-cause, defensive rollback, bypass guard)`.

---

### Task 5: `ferro-backend-pg` — PgConn on tokio-postgres

**Files:** Create `ferro-backend-pg/{Cargo.toml, src/lib.rs, src/connect.rs, src/conn.rs, src/tx.rs, src/hygiene.rs}`, `tests/pg_pool_it.rs`.

**Interfaces:** `PgBackend { config }` implementing `ferro_pool::backend::PoolBackend` with `type Conn = PgConn`. `connect()` parses `FERRO_TEST_PG_URL`/a DSN, `tokio_postgres::connect(.., NoTls)`, spawns the connection driver task, returns `PgConn { client, closed_flag (set when the driver task ends), created_at }`. `ping` = `SELECT 1`; `is_closed` = the driver-ended flag or `client.is_closed()`; `reset` = `DISCARD ALL` (destructive; M0 minimal — full conditional hygiene is M1); `simple_query` via `client.batch_execute`/`simple_query`. `tx.rs` runs BEGIN/COMMIT/ROLLBACK and toggles the tx-open flag. Errors map to `PoolError` (`ConnectionLost` on driver end / a killed backend).

- [ ] **TDD — `tests/pg_pool_it.rs` (skip when `FERRO_TEST_PG_URL` unset):**
  - `pg_checkout_select1_release`: a `Pool<PgBackend>` checks out, `ping` (`SELECT 1`) succeeds, releases, reuses.
  - `pg_tx_pins_single_backend_pid`: `begin_tx` then two queries on the pinned conn both report the SAME `pg_backend_pid()` (the tx stayed on one connection); after `commit_tx`, `Unpinned`.
  - `pg_release_hygiene_leaves_conn_clean`: set a session var / temp state in a tx, rollback+release; a subsequent checkout of that conn sees clean state (DISCARD ALL / defensive rollback worked).
  - `pg_killed_backend_evicted_no_retry`: `SELECT pg_backend_pid()`, then `SELECT pg_terminate_backend(<that pid>)` from a second conn (or `pg_terminate_backend(pg_backend_pid())` variant); the pool detects the dead conn on next use/checkout and evicts it, surfacing `ConnectionLost` (Retryable) — and does NOT transparently re-run the user statement (charter rule 3).
  - `pg_max_lifetime_recycles_live`: with a short `max_lifetime`, a checkout after the lifetime yields a conn with a DIFFERENT `pg_backend_pid()`.
  - Also assert `cargo-deny check bans` still passes (no deadpool/bb8 pulled in transitively).
- [ ] **Validate live:** `docker compose -f testkit/docker-compose.yml up -d`, `FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro cargo test -p ferro-backend-pg`, then `down -v`. (Or `./ci/local-gate.sh --with-pg`.)
- [ ] **Gates + commit** `feat(s4): ferro-backend-pg (tokio-postgres) + live pool integration tests`.

---

## Self-Review (to complete when authored against the real toolchain)

- **Spec coverage (design S4 gate):** pool checkout/release + max_size + timeout → T2; max_lifetime + reaper + backoff → T3; pin_stub PinCause::Tx + pinned-not-reused + defensive rollback → T4; fake backend fast tests → T1-T4; live PG (SELECT 1, tx pins one pid, killed-backend evicted no-retry, max_lifetime live) → T5; cargo-deny no deadpool/bb8 → T5 (+ S2).
- **Deferred (noted):** full pin engine (protocol trackers/RFQ, assist lexer) + conditional pipelined hygiene → M1; the real SQL EXEC service that USES this pool → S5; replica routing → M4.
- **Decisions to confirm at authoring:** async-fn-in-trait vs `#[async_trait]` for `PoolBackend`; whether the fake backend uses `tokio::time` pausing for lifetime/reaper determinism; the exact `tokio-postgres` driver-task + closed-flag wiring; the taxonomy mapping of pg errors (v0 minimal — full SQLSTATE table is S5).
- **Verify the plan before executing** (S1/S3 pattern): an adversarial plan-verification pass (compile assumptions on async-trait + tokio-postgres, the checkout/permit/RAII-drop soundness under concurrency, the reaper cancel-safety, the pin-not-reused race) before Task 1 dispatch.

## Execution Handoff

Subagent-driven: fresh implementer per task (TDD, gates), review after each (probe pool concurrency + the pinned-not-reused invariant + no-transparent-retry), whole-branch review before S5. Live PG tests run against the S2 Docker backend.
