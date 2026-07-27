# Ferro M0 · Slice S5E — Wire-level e2e scenario suite + runnable demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

> User-requested (2026-07-24): a wire-level end-to-end layer on the now-reviewed S5 EXEC path (whole-branch review → SHIP). Two deliverables: (1) a **scenario suite** exercising realistic multi-request flows, and (2) a **runnable demo** you can watch. Becomes the growing regression harness later slices plug into (S6 adds TX flows, S7 the PHP-client leg, later the §20.3 chaos matrix).

**Goal:** Prove the full client→ferrod→pool→Docker-PG→client path holds under realistic *multi-request* conditions (not just the single `SELECT 1`), and give a one-command "watch it work" artifact that prints live rows + `queue_us`/`exec_us`.

**Architecture / Decisions:**
- **D-S5E-1 — Scenario suite is in-process, reusing the S5 harness.** New `engine/crates/ferrod/tests/sql_e2e_scenarios.rs` reuses `mod common` (`TestServer::spawn_with_handler`, `TestClient`) and the real `sql::make_handler` + a real `Pool<PgBackend>` — a genuine client→UDS→ferrod→pool→PG round trip, just with the daemon in the test process (same as `sql_exec_it.rs`). PG-touching scenarios SKIP cleanly without `FERRO_TEST_PG_URL` (charter-green offline); the epoch-reconnect scenario needs no PG.
- **D-S5E-2 — Demo is a self-contained `ferro-e2e` bin.** New `engine/crates/ferro-e2e` (bin) spins up a real in-process ferrod session server on a real UDS socket pointed at Docker PG, connects a real client over that socket using the `ferro-proto` codec, runs a scripted sequence, and prints each result + stats + a summary. **One command:** `FERRO_TEST_PG_URL=… cargo run -p ferro-e2e`. A `testkit/e2e-demo.sh` wraps compose-up → run → compose-down. (Rationale: exercises the entire wire path in one runnable command without the extra moving parts of reaching the sidecar container; the container variant is a later add if wanted. This is a dev/demo tool, not shipped runtime.)
- **No new wire/proto surface.** S5E only consumes the S5 contract; it adds ZERO `/proto` changes, ZERO new message types, ZERO new `pub` symbols, and ZERO config work (verified `wf_eff75c17`). The demo spins up ferrod in-process entirely from the existing PUBLIC API: `ferrod::serve::serve(listener, Config, BootEpoch, Drain, HandlerFn)`, `ferrod::config::{Config, PoolSpec}`, `ferrod::epoch::BootEpoch`, `ferrod::shutdown::Drain::new()`, `ferrod::pools::PoolRegistry::build(&Config)`, `ferrod::services::sql::make_handler`, `ferrod::session::HandlerFn`, `ferrod::session::codec::{FrameCodec, InFrame, OutFrame}`.
- **Slow query (verified against live PG):** use `SELECT 1 FROM pg_sleep(0.2)` (a single `int4`→`I64` row, ~202 ms) or `SELECT count(*) FROM (SELECT pg_sleep(0.2)) _` (`int8`→`I64`). Do NOT use `SELECT pg_sleep(0.2), 1` — its leading `void` column (OID 2278) is rejected by `rowmap::oid_to_tag` in `query.rs` step 3 BEFORE execution → `Outcome::Error{Unsupported}`, which would break the PING/CANCEL scenarios. No engine change to accommodate the demo.
- **Pool sizing is a hardcoded 16** (`ferrod/src/pools.rs` `DEFAULT_POOL_MAX_SIZE`, not settable from `Config`, not ferro-pool's default 8) and `max_inflight` defaults to 1024 — both comfortably ≥ any scenario's N (≤8), so concurrency is admitted with zero config. Do NOT add a Config knob for pool sizing (out of S5E scope; charter rule 5).
- **Charter invariants still asserted:** every scenario asserts exactly-one-END per request id and session-survives (PING→PONG) where applicable.

## File Structure
```
engine/crates/ferrod/tests/common/mod.rs           LIFT the shared EXEC helpers here (see Task 1)
engine/crates/ferrod/tests/sql_e2e_scenarios.rs   the scenario suite (reuses mod common)
engine/crates/ferrod/tests/sql_exec_it.rs          update to consume the lifted helpers
engine/crates/ferro-e2e/                            new demo crate (bin) — auto-resolved by members=["engine/crates/*"]
  Cargo.toml         deps: ferrod, ferro-proto, tokio{net,io-util,rt-multi-thread,macros}, tokio-util{codec}, futures
  src/main.rs        spin up in-process ferrod (serve) → connect → HELLO → scripted EXEC sequence → print rows + stats
  src/client.rs      a minimal standalone client over UnixStream, framed via ferrod::session::codec (Framed<_, FrameCodec>)
testkit/e2e-demo.sh  compose up pg → cargo run -p ferro-e2e → compose down (trap)
```
(No root `Cargo.toml` edit: `members = ["engine/crates/*"]` auto-resolves the new crate; an explicit entry would risk a duplicate-member error.)

---

### Task 1: the scenario suite (`ferrod/tests/sql_e2e_scenarios.rs`)

- [ ] **First, LIFT the shared helpers into `tests/common/mod.rs`** (they are currently private fns inside the `sql_exec_it.rs` test binary, and each `tests/*.rs` is a SEPARATE crate, so a sibling file cannot import them). Move `pg_url()` (skip idiom), `exec_server()`, `req()`, `exec()`/`exec_ok()`/`exec_err()`, and `assert_session_alive()` into `common/mod.rs` (which already has `#![allow(dead_code)]` and the needed imports), then update `sql_exec_it.rs` to consume them via `mod common;`. Both binaries now share one copy. THEN write the scenarios below in `sql_e2e_scenarios.rs` reusing them. Skip without `FERRO_TEST_PG_URL` unless noted.

- [ ] **`concurrent_multiplexed_execs`** — on ONE session, fire N=8 EXEC requests with distinct `request_id`s WITHOUT awaiting each terminal, then collect all N terminals. Assert: every id gets exactly one END, every response is `Outcome::Ok` with the expected row, and ids may return in any order (multiplexing). Proves the session multiplexes concurrent in-flight EXECs (each handler owns its own pooled conn). Use `SELECT <id>` so each response is self-identifying. (N=8 < the hardcoded pool `max_size`=16 and ≪ `max_inflight`=1024, so all 8 checkouts are admitted concurrently — this proves multiplexing, not queuing. Optionally assert several responses have a small `queue_us` = no semaphore wait.)
- [ ] **`error_then_recover`** — send a syntax-error EXEC (→ `Outcome::Error{Syntax}`, one END), then a valid `SELECT 1` on the SAME session (→ `Ok`), then PING→PONG. Proves a per-request error is statement-level, never session-level.
- [ ] **`ping_during_in_flight_exec`** — start a slow EXEC (a query that takes ~200ms — see slow-query note), and BEFORE its terminal arrives, send a PING on the same session; assert the PONG comes back promptly (reader not blocked by the in-flight handler), THEN the EXEC terminal arrives (one END). Proves the reader stays responsive while a handler runs.
- [ ] **`cancel_in_flight_exec`** — the TRUE M0 behavior (verified): the EXEC handler binds its cancel token as `_cancel` (`sql.rs:65`) and NEVER reads it, so a CANCEL neither aborts the in-flight query nor produces `Outcome::Cancelled` — the session CANCEL path only calls `registry.cancel(id)` on a token nobody consumes; `co.query` runs to completion. So: start a slow EXEC (interleaved — `send_request(rid,…)` → `client.cancel(rid)` → `recv`, NOT the atomic `exec()` helper), then assert the single terminal for `rid` has `END` set, `service=SQL`/`method=EXEC`, decodes to `Outcome::Ok(ExecOk rows == expected)`, and **explicitly panic on `Outcome::Cancelled`** ("M0 CANCEL does not abort EXEC"). Then `assert_session_alive` (PING→PONG) proves both session survival AND exactly-one-END (no stray second frame arrives before the PONG). Add a note: CANCEL is a documented no-op on an in-flight EXEC in M0 (handler is not yet cancel-aware; a cancel-aware handler is post-M0).
- [ ] **`reconnect_across_boot_epoch_change`** (needs NO PG) — spawn server A (`BootEpoch(1)`), client HELLO → capture `boot_epoch` = 1; drop A; spawn server B (`BootEpoch(2)`); a fresh client HELLO → `boot_epoch` = 2; assert `2 != 1` (a restarted daemon issues a fresh epoch → the client's resilience loop, S7, voids engine-side state on the change). Optionally run a `SELECT 1` on B if PG is up. Proves the wire-level epoch-change signal §19.1 is built on.
- **Slow-query note:** `pg_sleep` returns `void` (out-of-M0 type). To get a slow query that returns an M0-typed row, use `SELECT 1 FROM pg_sleep(0.2)` or `SELECT count(*) FROM (SELECT pg_sleep(0.2)) _` — pick one that returns an `int`/`bigint`. Verify against live PG in the test; do NOT change the engine to accommodate it.
- **Gate + commit** `test(s5e): wire-level e2e scenario suite (concurrent EXECs, error-recover, PING/CANCEL mid-EXEC, epoch reconnect)`.

---

### Task 2: the runnable demo (`ferro-e2e` crate + `testkit/e2e-demo.sh`)

- [ ] `engine/crates/ferro-e2e/Cargo.toml` — a bin crate; deps: `ferrod`, `ferro-proto`, `tokio` (features `["net","io-util","rt-multi-thread","macros"]`), `tokio-util` (`["codec"]`), `futures`. **Drop `ferro-pool`/`ferro-backend-pg`** — the demo only touches `ferrod::{serve,config,pools,services::sql,epoch,session}` + `ferro-proto`, never names `Pool`/`PgBackend`. No workspace-`members` edit (auto-resolved). Crate doc notes it is a dev/demo tool, not shipped runtime. `PoolRegistry::build` + `serve` must run INSIDE the tokio runtime (`Pool::new` spawns a reaper).
- [ ] `src/client.rs` — a MINIMAL standalone client (no test deps): open a `UnixStream`, frame it with **`ferrod::session::codec::{FrameCodec, InFrame, OutFrame}` via `tokio_util::codec::Framed`** (there is NO framing codec in `ferro-proto` — it exposes only `header::{Header, HEADER_LEN}` + per-message encode/decode; the length-framing lives in `ferrod::session::codec`, exactly as `common/mod.rs` uses it). Provide `hello()` (send HELLO, read HELLO_ACK, return `boot_epoch`+`pools`), `exec(sql, params, fetch, readonly) -> Outcome`, and decode helpers. ~120 lines; the reusable "how a real client speaks the wire" reference (a Rust cousin of what S7 does in PHP).
- [ ] `src/main.rs` — read `FERRO_TEST_PG_URL` (print a friendly "set FERRO_TEST_PG_URL / docker compose up" and exit 0 if unset — never panic); build `PoolRegistry` + `sql::make_handler`; `serve` on a temp UDS socket in-process; connect the `client.rs` client; run a **scripted, narrated sequence** printing each step:
  1. HELLO — print `boot_epoch` + advertised pools.
  2. `SELECT 1` — print the row + `queue_us`/`exec_us`.
  3. `CREATE TABLE IF NOT EXISTS ferro_e2e_demo(id bigint, note text)` (fetch:none) — print `affected`.
  4. `INSERT … VALUES (?,?),(?,?)` params (fetch:none) — print `affected`.
  5. `SELECT id, note FROM ferro_e2e_demo ORDER BY id` — print each row + stats.
  6. a deliberate syntax error — print the classified `Outcome::Error{code,branch,sqlstate}` (show the taxonomy works).
  7. concurrent: fire 4 `SELECT n` without awaiting, collect, print the (possibly reordered) results — show multiplexing.
  8. print a final one-line summary (counts + total wall time).
- [ ] `testkit/e2e-demo.sh` — `docker compose -f testkit/docker-compose.yml up -d`, `wait-for-pg.sh`, `FERRO_TEST_PG_URL=… cargo run -p ferro-e2e`, `docker compose … down` (no `-v`) in a trap. Chmod +x. A short `testkit/README` line documents it.
- **Gate + commit** `feat(s5e): runnable ferro-e2e demo (in-process ferrod + live PG, narrated EXEC sequence) + testkit script`.

---

## Self-Review / Gates
- Charter gates: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` (offline: scenarios skip), then the live run with `FERRO_TEST_PG_URL` set (scenarios + a manual `e2e-demo.sh` run captured in the report). PHP untouched.
- The scenario suite must assert exactly-one-END per request id in EVERY scenario (reuse the `sql_exec_it` one-END + `assert_session_alive` helpers).
- Verify the plan before executing (S1/S3/S4/S5 pattern): a focused adversarial pass on the slow-query type choice, the CANCEL/END terminal guarantee (assert what S3 actually produces, not an assumption), the concurrent-multiplex ordering assertions (no reliance on arrival order), and the demo's unset-env / teardown behavior (never panic, always compose-down).

## Execution Handoff
Subagent-driven where used: fresh implementer per task (TDD/gates), review after, whole-branch review before declaring S5E done. Live tests + the demo run against the S2 Docker PG.
