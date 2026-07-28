# Ferro M1 — Execution Design

**Date:** 2026-07-28
**Status:** Design for review → implementation plans
**Scope:** Milestone **M1 only** (SPEC §17 M1 bullet). Bounded by M1; later-milestone features are explicitly out and marked where encountered.
**Authority:** `ferro-spec-v0.2.md` is the contract; `CLAUDE.md` is the working agreement. This document is the *execution plan* for M1 — how the spec's M1 requirements become tested, buildable slices. Deviations are amended into spec §22 in the same change.
**Predecessor:** M0 is complete and merged (S1–S8 + S5E; PR #1). The engine vertical (wire, session, hand-rolled PG pool, SQL EXEC + transactions, PHP sync client) works end-to-end against real Postgres; the D12 measurement is recorded (provisional/WSL2). This design builds on that.

---

## 1. Goal & exit gate

Deliver Ferro's **correctness layer + its first drop-in tier**: the real protocol-first pin engine, the complete §9.2 error taxonomy / write-fate matrix, a **MySQL backend**, and the **Doctrine DBAL-4 driver** — so existing Doctrine/Symfony applications run on Ferro **by configuration only**, across **PostgreSQL and MySQL**, with the full §19 write-fate guarantees intact.

**M1 exit gate (the analog of M0's "D12 recorded"):**
1. The **Doctrine DBAL 4 upstream test suite is green** in CI against Dockerized Postgres **and** MySQL (with a committed, documented allow-list of genuine incompatibilities per §14 — those are documented breaks, not failures).
2. The pin-engine **MySQL tracker-verification test** passes (or its documented conservative fallback is proven active — §7.1).
3. The **§9.2 fate matrix** is exercised by an extended chaos suite (§20.3): cancel / timeout / link-loss each resolve to their specified branch, and an `Indeterminate` write is never transparently retried.

M1 does not close until all three hold in CI.

## 2. Infrastructure topology (delta from M0)

- **MySQL/MariaDB added to `testkit/docker-compose.yml`** (digest-pinned, `healthcheck`, seed `init.sql`), alongside the existing Postgres 17 — the second real upstream the pin engine + MySQL backend + DBAL suite test against. `FERRO_TEST_MYSQL_URL` mirrors `FERRO_TEST_PG_URL` (unset → those tests skip, keeping `cargo test --workspace` green offline).
- **A forked `tokio-postgres`** (M1-D2) enters the workspace as a patched path/git dependency of `ferro-backend-pg` — the minimal surface exposing `ReadyForQuery` (I/T/E) + `GUC_REPORT` `ParameterStatus` to the pool. An upstream PR runs in parallel; the fork is dropped if it merges.
- **A PHP Doctrine test harness** (`php/doctrine-dbal/`): the DBAL 4 upstream suite run against a Ferro-backed connection, using the S7 client + a launched `ferrod` (reusing the S7/S8 process harness), against Docker PG + MySQL.
- **Bench:** the M0 `ferro-bench` gains a **bare-metal/host-network reference run** target (human sign-off) — no CI threshold (§20.3).

## 3. Resolved decisions

### 3.1 Maintainer-decided (this milestone)

| # | Decision | Choice | Consequence |
|---|----------|--------|-------------|
| **M1-D1** | M1 sequencing (5 subsystems: pin engine, MySQL, taxonomy, Doctrine, accelerator) | **Correctness core first** | Real pin engine + full taxonomy land before MySQL and the Doctrine tier, which both ride on them (§7, §9.2). The spec's stated M1 opener; unblocks the rest. |
| **M1-D2** | How the PG pin engine reads `ReadyForQuery` I/T/E (stock `tokio-postgres` exposes no I/T/E byte — the M0 §22.1 M-2 open item) | **Fork `tokio-postgres`, expose RFQ (+ `GUC_REPORT` `ParameterStatus`)** | A thin fork surfaces the status byte to the pool; auth/TLS/type-OIDs/pipelining reused. Upstream PR in parallel; revisit the fork if merged. Accepts a fork-maintenance/upstream-drift cost (R1). |
| **M1-D3** | `ext-php-rs` accelerator timing (D12 gate missed, but on WSL2/Docker — provisional) | **Bare-metal re-run EARLY to settle the decision; build the accelerator LAST and ONLY if the miss holds** | Don't build a whole `ext-php-rs` subsystem to fix WSL2 noise. The re-run is a human sign-off; the build (S9) is conditional on it. |
| **M1-D4** | Doctrine "DBAL 4 suite green" bar | **Full upstream DBAL 4 suite in CI + a committed documented incompat allow-list** (§14) | `getNativeConnection()`-expects-PDO, `pg_dump`/`mysqldump` credential passthrough (D8), COPY hacks are DOCUMENTED breaks, not red tests. |
| **M1-D5** | MySQL backend scope | **A real backend at `PoolBackend` parity with PG** (not a stub) | The tracker-verification test + cross-backend taxonomy + the DBAL MySQL platform all need a working MySQL backend (§7.1, §8). |

### 3.2 Carried-forward settled spec decisions (do not re-litigate)

- **D2** — DBAL `^4.0` first (M1 acceptance); the `^3.8` bridge is M2. Cuts a third of the SPI shims out of M1's critical path.
- **D9** — the pool stays hand-rolled (the pin state machine + conditional hygiene + checkout-pipelining *are* the product). M1 completes exactly these on the M0 pool.
- **D1** — MSSQL stays session-mode / deferred to M4; **not in M1**.
- **D12** — codec strategy is empirical; the accelerator is pulled in iff the p99 gate fails — realized as M1-D3.
- **D7** — "Ferro" naming / crates.io / Packagist / trademark check is a **maintainer human task before M1 ships** (carried).

## 4. Deviations to record in SPEC §22 (each in the change that introduces it)

1. **§7.1 / backend crate** — `ferro-backend-pg` depends on a forked `tokio-postgres` exposing `ReadyForQuery`/`ParameterStatus` (M1-D2), resolving the M0 §22.1 M-2 open item. Upstream PR referenced.
2. **§5.2 / §6 EXEC framing** — the windowed streaming DATA-channel producer (deferred in M0 by D-S5-1) is **implemented in M1-S5**, unblocking the Doctrine `iterate*()`-never-buffers contract (§14). §22.1's D-S5-1 note is updated to "landed in M1."
3. **§16.1 D12** — the provisional WSL2 result is supplemented by a **bare-metal/host-network reference run**; the `ext-php-rs` accelerator decision (M1-D3) is recorded against it.
4. Any pin-engine behavior that implementation forces to differ from §7.1–7.2 (e.g. a MySQL tracker gap → conservative fallback) is amended in §7 + noted in §22 in the same change.

## 5. The 9 slices

Each slice lands **green with practical tests before the next begins**. Dependencies noted. The correctness core (S1–S4) is the critical path; S5 (streaming) and S6 (MySQL) can parallelize after S4; S7–S8 (Doctrine) depend on S5+S6; S9 is conditional.

### S1 — Fork tokio-postgres + expose RFQ → the real PG pin engine  *(depends: M0)*
Replace the M0 TX-lifecycle pin **stub** with protocol-signal pinning driven by the authoritative `ReadyForQuery` status byte.
- **Build:** the minimal `tokio-postgres` fork surfacing `ReadyForQuery` (I/T/E) per query and `GUC_REPORT` `ParameterStatus` to `ferro-backend-pg` (a per-connection status hook/channel); the `ferro-pool` pin state machine consumes it — pin on `T`/`E` (in-tx / failed-tx), unpin on `I` — replacing `is_bare_tx_control`/`tx_open` as the authority (the S5/S6 guard stays as defense-in-depth). Keep the S6 TX actor + `PinCause::Tx`; add `PinCause` values the RFQ/lexer now distinguish. Open the upstream PR.
- **Gate:** unit (RFQ→pin-state transitions on a fake backend); live PG — a `BEGIN`…`COMMIT` pins one `pg_backend_pid` and unpins on `I`; a failed statement inside a tx (`E`) keeps the pin until `ROLLBACK`; an autocommit statement never pins; **the pin cause is asserted (charter DoD)**. `cargo test --workspace` + charter gates green; the fork builds in CI.
- **Key artifacts:** the fork (path/git dep + a `deny.toml`/`Cargo.toml` note), `ferro-backend-pg` status hook, `ferro-pool/src/pin.rs` (RFQ-driven), pin-transition tests, upstream PR link recorded.

### S2 — Assist lexer + `pin_functions` escape hatch  *(depends: S1)*
The keyword classifier for text-visible-but-protocol-invisible statements (§7.1).
- **Build:** `ferro-classify` (the assist lexer crate named in §20.1) — a keyword classifier (NOT a parser) that pins on: `LISTEN`/`UNLISTEN`; non-`_xact` advisory-lock functions; raw client `PREPARE`/`EXECUTE`/`DEALLOCATE`; temp-table DDL; non-local `SET` (PG: not `SET LOCAL`); SQLite `ATTACH`/state `PRAGMA`. `pin_on_unknown = true` default. Per-pool `pin_functions = [...]` escape hatch (statements referencing a listed function pin). Wire it into the pin engine as the *assist* signal (protocol RFQ remains authority).
- **Gate:** unit — a classification corpus (each pin-triggering form + a `pin_functions` case + `pin_on_unknown`), literal/comment/dollar-quote aware (reuse the S5 placeholder-scanner hazard discipline); live — a `LISTEN` / an advisory lock / a temp table each pins the conn for the session. **Pin-cause assertion** extended (`PinCause::{Listen, AdvisoryLock, Prepare, Temp, Set, PinFunction, Unknown}`).
- **Key artifacts:** `engine/crates/ferro-classify/`, the classification corpus, pin-cause labels (also feed §13 metrics).

### S3 — Conditional + pipelined hygiene at checkout  *(depends: S1, S2)*
Replace the M0 minimal release-time `ROLLBACK` with the §7.2 conditional, checkout-pipelined hygiene.
- **Build:** hygiene runs at **checkout**, pipelined ahead of the first user statement. A **pinned/tainted** conn → full reset (PG `DISCARD ALL`). A **never-pinned PG** conn → the targeted profile `RESET ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES;` (preserves namespaced prepares). A **known-clean** conn → skip hygiene entirely. Drive the tainted/clean decision from the S1/S2 pin state, not a flag.
- **Gate:** live PG — a conn that held an advisory lock / a temp table / a `LISTEN` is CLEAN for the next tenant (no leak — the exact v0.1 bugs §7.2 calls out); a never-pinned conn keeps its engine prepares across checkout; hygiene is measurably pipelined (issued before the first user statement). Extend the S4-pool hygiene tests.
- **Key artifacts:** `ferro-pool` checkout-hygiene path, per-backend reset profiles, leak-prevention tests.

### S4 — Full error taxonomy + write-fate matrix  *(depends: S1)*
Complete the §9.2 taxonomy and the §19 fate matrix the M0 client already keys on.
- **Build:** fill the taxonomy branches beyond M0's set; implement the **fate rules for cancel / statement-timeout / link-loss** (§9.2, §19.3) uniformly — a write's fate under each is classified to the correct branch (`Retryable` / `Indeterminate` / `NonRetryable`) and carried on the wire (the M0 3-branch machinery already exists). Extend the chaos harness (§20.3): kill/timeout/cancel mid-write and assert the reported fate + **no transparent retry**.
- **Gate:** unit — the fate matrix table (op × failure-mode → branch); live/chaos — `pg_terminate_backend` mid-write → `Indeterminate`; a statement-timeout → the specified branch; a CANCEL race → exactly-one-END + correct fate; a serialization/deadlock → `Retryable`. The M0 §19.3 guarantee holds across every new path.
- **Key artifacts:** `/proto/errors.toml` completions (+ regen + both codecs, charter rule 2), the fate-matrix module, the extended chaos suite.

### S5 — Streaming DATA-channel producer  *(depends: S4; the M0 D-S5-1 deferral)*
Implement the windowed streaming producer M0 deferred (charter rule 5 — now the Doctrine tier demands it).
- **Build:** the per-request credit wakeup (async `Notify` on replenish), per-session cap accounting + release, cross-channel terminal ordering (terminal after the last DATA frame, FIFO), `HEAD(cols)+DATA(rows)` framing, and `fetch:stream`. This is the subsystem the M0 verification (`wf_70051761`) mapped precisely — reuse those findings. Autocommit + tx-scoped EXEC both gain the streamed path; the buffered path stays for `fetch:rows`.
- **Gate:** the M0 streaming-verification concerns as tests — one-END under streamed EXEC (terminal never overtakes DATA), credit backpressure pauses+resumes on `WINDOW_UPDATE` without a lost-wakeup hang, the session cap releases (not monotonic), `exec_us` excludes backpressure. Live: a large multi-frame result streams under a small window. Golden vectors for HEAD/DATA.
- **Key artifacts:** `ferrod` streaming producer (the S5-plan-v1 subsystem), credit-wakeup primitive, HEAD/DATA vectors, PHP client streamed-read path.

### S6 — MySQL backend + session trackers + tracker-verification test  *(depends: S1–S3; parallel to S5)*
A real MySQL/MariaDB backend at `PoolBackend` parity (M1-D5).
- **Build:** `ferro-backend-mysql` (a MySQL protocol client — evaluate `mysql_async` vs a minimal wire like the PG decision, recorded as an internal decision): connection setup enabling `session_track_system_variables='*'`, `session_track_state_change=ON`, `session_track_transaction_info=CHARACTERISTICS`, `CLIENT_SESSION_TRACK` negotiated; pin decisions from **OK-packet tracker payloads**; `PoolBackend` parity with PG (connect/ping/reset via `COM_RESET_CONNECTION`/query/simple_query/cancel); OID/type mapping into the canonical `TypedValue` set. A known-clean (tracker-verified) conn skips hygiene (§7.2).
- **Gate:** **the §7.1 tracker-verification test — assert a tracker fires for `SET SESSION` executed *inside a stored procedure*; if it does NOT, the documented conservative fallback is proven active** (§7.1 + a §22 note). Live MySQL — a tx pins one connection id; a session-mutating statement pins via the tracker; a clean conn skips reset; the M0 SQL/TX live suites pass against MySQL. `FERRO_TEST_MYSQL_URL` gates.
- **Key artifacts:** `engine/crates/ferro-backend-mysql/`, the tracker config + OK-packet parser, the tracker-verification test, testkit MySQL service.

### S7 — Doctrine DBAL-4 driver  *(depends: S5, S6)*
The first drop-in tier — DBAL `^4.0` (D2).
- **Build:** `ferro/doctrine-dbal-driver` (PHP, on the S7 client): `Ferro\DBAL\Driver` (`connect()` → a pool-bound `Connection`; `getDatabasePlatform()` from `HELLO_ACK` pool metadata + server version; `getExceptionConverter()` → the §9.2 tree → DBAL exceptions uniformly across PG/MySQL, plus `Ferro\DBAL\IndeterminateWriteException`); `Connection` (`prepare/query/exec/lastInsertId/beginTransaction/commit/rollBack` → TX frames; savepoints via DBAL's path; `quote()` client-side per platform, D5; `getServerVersion()`; `getNativeConnection()` → the `Ferro\Client\Session`, a documented break); `Statement`/`Result` (`bindValue` `ParameterType`→canonical; `fetch*` from row frames; `rowCount` from `affected`; **`iterate*()` uses the S5 streamed path, never buffers**).
- **Gate:** unit (platform selection, exception conversion table, param mapping, streamed iterate) + live (a representative DBAL usage against PG + MySQL: prepared params, transactions/savepoints, `lastInsertId` per backend, streamed iteration). PHPStan L9; runtime dependency-free beyond `doctrine/dbal ^4`.
- **Key artifacts:** `php/doctrine-dbal/`, the exception-converter map, the streamed `Result`.

### S8 — DBAL 4 upstream suite green (PG + MySQL) + incompat doc  *(depends: S7)*  — **the exit-gate slice**
- **Build:** wire the upstream Doctrine DBAL 4 test suite to run against a Ferro-backed connection (the harness launches `ferrod` + PG + MySQL, points DBAL at the Ferro driver). Triage failures into: real bugs (fix) vs genuine incompatibilities (a **committed allow-list** with per-case rationale, §14/D4/D8). Write the first-class **incompat doc page** (§14 — `getNativeConnection`-expects-PDO, dump-credential passthrough, COPY, persistent-connection advice).
- **Gate:** the DBAL 4 suite is GREEN modulo the documented allow-list, in CI, against **both** PG and MySQL. `ci/check-m1-suite.sh` (new, mirroring `check-d12-recorded.sh`) gates the milestone. **This is the M1 exit condition** (with S6's tracker test + S4's fate suite).
- **Key artifacts:** the DBAL-suite CI job, the allow-list + rationale, `docs/incompat.md`.

### S9 — (conditional) bare-metal D12 re-run → `ext-php-rs` accelerator  *(depends: S5; gated by M1-D3)*
- **Build:** run the M0 `ferro-bench` on a **bare-metal / host-network** reference environment (the human sign-off §16 requires) and record it (`reference:true`). **If** the boundary p99 still misses the §16 target (p50<60µs / p99<200µs), build the `ext-php-rs` accelerator (frame codec + hydration, **same wire contract** — a transparent codec swap behind `PackerFactory`, charter rule 7 unchanged: it stays optional/runtime-detected). **If the re-run passes**, record that the accelerator is NOT needed for M1 and close the D12 item.
- **Gate:** a committed reference-env bench result; if built, the accelerator passes the S1 golden vectors byte-for-byte (same contract) + a bench showing the p99 improvement. NOT an M0/M1 CI blocker — a human sign-off on recorded numbers.
- **Key artifacts:** `bench/results/*-reference.json`, (conditional) the `ext-php-rs` crate + PHP glue behind the existing `PackerFactory` autodetect.

## 6. Test strategy (charter DoD)

- **Unit + integration** against `/testkit` PG **and** MySQL; every pin-engine slice adds a **pin-cause assertion**; every §19/taxonomy slice extends the **chaos harness**; protocol work (streaming, taxonomy) adds/updates **golden vectors** + both codecs.
- **Live** tests skip (not fail) when `FERRO_TEST_PG_URL` / `FERRO_TEST_MYSQL_URL` are unset → `cargo test --workspace` green offline; CI provisions both DBs + the DBAL suite.
- **Gates green** every slice: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, PHPUnit + PHPStan L9 (client + the DBAL driver), `/proto` regen zero-diff, `cargo-deny` (the fork is allow-listed with a rationale).

## 7. Top risks

| # | Risk | Mitigation |
|---|------|------------|
| R1 | The `tokio-postgres` fork is invasive / drifts from upstream | Keep the patch MINIMAL (a status hook only); land it in S1 behind a clear seam; open the upstream PR early; `cargo-deny` allow-list + a documented rationale; drop the fork if the PR merges. |
| R2 | MySQL session trackers do NOT fire inside stored programs (§7.1's own caveat) | The S6 verification test is a HARD gate: on failure, the documented conservative fallback (pin on any tracker-ambiguous statement) is proven active + noted in §22 — correctness over the cache target. |
| R3 | The DBAL 4 upstream suite has cases Ferro deliberately won't support (native-PDO, dump creds, COPY) | The M1-D4 allow-list makes these DOCUMENTED breaks with rationale, not red tests; the incompat doc page is a first-class deliverable (S8). |
| R4 | The streaming producer (S5) is genuinely hairy (the M0 verification found 2 blockers + 2 majors in its v1 plan) | Reuse the recorded M0 `wf_70051761` findings verbatim as the S5 plan's fix list; port the buffered-vs-streamed decision cleanly (buffered stays for `fetch:rows`). |
| R5 | `ext-php-rs` is a large optional subsystem built to chase a WSL2-inflated number | M1-D3: the bare-metal re-run settles the decision FIRST; the build (S9) is conditional and last; the accelerator is a transparent codec swap (no wire/contract change). |
| R6 | Two backends (PG + MySQL) double the taxonomy/pin/hygiene surface | The `PoolBackend` trait + the canonical `TypedValue`/taxonomy already abstract the backend; per-backend differences are isolated in the backend crates + the fate matrix table. |

## 8. Definition of done for M1

- The DBAL 4 upstream suite is green (PG + MySQL) modulo a committed, documented allow-list; `ci/check-m1-suite.sh` exits 0.
- The MySQL tracker-verification test passes, or its conservative fallback is proven + documented.
- The §9.2 fate matrix (cancel/timeout/link-loss) is exercised by the chaos suite; no `Indeterminate`/write is ever transparently retried.
- The real pin engine (RFQ + assist lexer + conditional hygiene) replaces the M0 stub, with pin-cause assertions across the trigger set and no cross-tenant state leak.
- The bare-metal D12 reference run is recorded; the `ext-php-rs` accelerator is built iff that run confirms the miss (else the D12 item is closed as "no accelerator needed for M1").
- All charter gates green across both backends; `/proto` regen zero-diff; the fork's upstream PR is filed and referenced.
