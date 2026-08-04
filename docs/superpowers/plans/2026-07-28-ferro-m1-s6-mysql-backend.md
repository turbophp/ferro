# Ferro M1 · Slice S6 — MySQL/MariaDB backend + session-tracker pin signal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.
>
> **v2** — folds the adversarial plan-verification (3 probes, FIX_FIRST). The v1 plan mis-specified the pin architecture; v2 corrects it. Two user decisions are baked in: (1) **MySQL streaming (`query_stream`) is DEFERRED to S7** (the exec-design fixed S6's parity bar at the buffered methods, before streaming existed; `mysql_async`'s row stream borrows the conn, so a channel-bridge is real work best built against S7's DBAL `iterate*()`). (2) The MySQL backend keeps the full tracker authority (the S1-fork analog), per SPEC §8. See the **Verification fold** table at the end.

**Goal:** Add a real `ferro-backend-mysql` at **buffered `PoolBackend` parity with `ferro-backend-pg`** (M1-D5), whose pin authority is **split**: tx-open from the OK-packet `SERVER_STATUS_IN_TRANS` status flag (fork-free, always present) + a session-**mutation** taint from the OK-packet session **trackers** (the "sees inside stored programs" property that beats PG — needs a minimal `mysql_async` fork) — wired into the existing pin engine (additively), the S4 fate matrix (verbatim), and the assist lexer (a MySQL dialect) — so Ferro runs the same pin/hygiene/fate guarantees against MySQL **and MariaDB** as against Postgres, verified live.

**Architecture:** The `PoolBackend` trait is the backend seam; S6 fills the MySQL implementation behind it. The pin authority is **two OK-packet signals, not one**: (a) `status_flags() & SERVER_STATUS_IN_TRANS` → `TxStatus{Idle,InTx}` (fork-free, MySQL+MariaDB uniform; MySQL has NO aborted-tx state so `TxStatus::Failed` is NEVER produced here — it is reachable only via `error_map`/the S4 fate path); (b) the session-state trackers (`session_track_state_change`/`system_variables`) → a per-lease **session-mutation taint** (requires `CLIENT_SESSION_TRACK`, which stock `mysql_async` does not negotiate → a minimal fork). Feeding (b) into the pin engine requires a NEW additive `PoolBackend` post-statement signal + a `Checkout::apply_session_tracker` step + `PinCause::SessionTracker` (v1 wrongly claimed "no `ferro-pool` change"). The assist lexer is ASSIST; `error_map` feeds the UNCHANGED `fate.rs`. `query_stream` is DEFERRED (S7).

**Tech Stack:** Rust (edition 2024, tokio); a **forked `mysql_async`** (vendored `/vendor/mysql-async`, root `[patch.crates-io]`, upstream PR drafted-not-filed — the S1 `tokio-postgres` analog) + `mysql_common`; `ferro-pool` (`PoolBackend`/`Cancel`/`TxStatus`/`Dialect`/`ResetProfile` + the new session-tracker hook); `ferro-classify` (`classify_one_mysql`); the S4 `fate.rs` + `error_map` pattern; `ferro-proto` canonical `Value`; `testkit` Docker MySQL 8 + MariaDB 11.

## Global Constraints (verbatim from SPEC §8 / §7.1–7.2 / §9.1 + the exec-design S6 + the product-vision scope + the v-verification fold — every task assumes these)

- **The pin authority is SPLIT across two OK-packet signals (verification finding P1-3):**
  - **tx-open (`InTx`/`Idle`) = `OkPacket::status_flags() & SERVER_STATUS_IN_TRANS` (0x0001).** Always present on both MySQL and MariaDB, independent of any tracker, exposed by `mysql_async` WITHOUT the fork. This is the robust base — basic transaction pinning works even on the unforked driver and is immune to MariaDB tracker-fidelity gaps.
  - **session-mutation taint = the OK-packet session trackers** (`SessionStateChange::{SystemVariable, StateChange}`). This is the ONLY thing that requires `CLIENT_SESSION_TRACK` (the fork), and it is the value: it reports mutations performed *inside stored programs* — the §7.4 blind spot PG cannot see.
- **`TxStatus::Failed` is NEVER produced by the MySQL tx signal (verification P1-2).** MySQL/MariaDB have NO aborted-open-tx state (no PG `E`): a statement error inside a tx leaves it OPEN+usable (`InTx`); a deadlock (1213) auto-rolls-back the WHOLE tx (→ `Idle`); a lock-wait-timeout (1205, default `innodb_rollback_on_timeout=OFF`) rolls back only the statement (→ stays `InTx`). So map `SERVER_STATUS_IN_TRANS` set → `InTx`, unset → `Idle`, and NEVER synthesize `Failed` from tx_status. **"Idle after an error" MUST NOT be treated as a clean commit** — the taint/fate decision keys off the ERROR (`error_map` → the S4 fate path → rollback/tombstone), not off `tx_status` returning `Idle`.
- **`mysql_async` requires a minimal FORK for the trackers (verification P1-1; the S1 analog).** Stock `mysql_async` never sets `CapabilityFlags::CLIENT_SESSION_TRACK` in `get_capabilities()` (which is `pub(crate)`, no opts hook), so the server withholds session-state-info and `OkPacket::session_state_info()` is ALWAYS empty. `mysql_common` already PARSES the trackers (`SessionStateInfo`/`SessionStateChange`), so the fork is SMALL — OR the capability bit into `get_capabilities()` (+ optionally an `OptsBuilder` hook). Vendor under `/vendor/mysql-async`, `[patch.crates-io]` in the root `Cargo.toml`, draft (not file) the upstream PR — dropped if/when merged. The tx-open half needs NO fork.
- **Do NOT over-pin (verification P1-5) — the clean-skip optimization is load-bearing (§7.2/§16).** (a) **Baseline** the session-mutation flag AFTER the connect-time tracker-enabling SETs (`SET SESSION session_track_* …`, `SET NAMES`) so they do not count toward per-lease taint — measure mutation PER-LEASE, not cumulatively. (b) `session_track_system_variables` MUST be a curated list (NOT `'*'`) OR a documented benign-var allowlist (autocommit toggled by Ferro's own tx management, charset, `time_zone`, `sql_mode` set at DBAL/Eloquent bootstrap must NOT taint) — else transaction-mode leasing taints nearly every connection and the `COM_RESET_CONNECTION`-skip collapses. (c) `last_insert_id` is NOT a session tracker → does not taint (a non-issue).
- **Feeding the session-mutation taint requires an ADDITIVE `ferro-pool` change (verification P3-2).** The ONLY backend→pin hook `Checkout` reads today is `tx_status(conn) -> TxStatus` — there is NO channel for "a session mutation happened." Add: a new `PoolBackend` post-statement signal (e.g. `fn take_session_mutated(&self, conn: &Self::Conn) -> bool`, returning + clearing a per-conn flag the backend sets from the trackers), a `Checkout::apply_session_tracker` step run alongside `apply_tx_status`/`apply_classify`, and a distinct `PinCause::SessionTracker`. PG's impl returns `false` (no-op) — additive, no PG behavior change. **The v1 "changes NOTHING in ferro-pool" claim is DROPPED.**
- **The assist lexer is ASSIST, not authority (SPEC §7.1).** The tracker (authority) sets the taint; `classify_one_mysql` pins only text-visible-but-tracker-ambiguous statements as defense-in-depth. `pin_on_unknown = true` default. **The DIRECT top-level `SET SESSION` is already caught by the existing `classify_one_mysql` `Set` trigger** (SESSION is not excluded like LOCAL) — only the `CALL`-wrapped in-proc case is the gap (see the hard gate).
- **`PoolBackend` BUFFERED parity with PG (M1-D5); `query_stream` DEFERRED to S7 (user decision).** Implement `connect`/`ping`/`is_closed`/`dialect`/`tx_status`/`simple_query`/`query`/`reset`/`clean_reset_profile`/`cancel_handle` + the new session-tracker signal. `query_stream` returns a documented `Unsupported` on a MySQL pool (the `fetch:stream` path rejects with "MySQL streaming lands in S7") — `mysql_async`'s row stream borrows `&mut Conn`, so an owned `Self::RowStream` needs a per-connection driver-task + bounded-channel bridge, built in S7 against the DBAL `iterate*()`. `simple_query` (the UNGUARDED path `begin/commit/rollback/tx_control` + the recycle `ROLLBACK` all use — 7 call sites) is REQUIRED (verification P2/P3) and assigned to Task 3.
- **`cancel_handle` = `KILL QUERY` over a SIDE connection (SPEC §8).** Capture `Conn::id()` (synchronous, borrow-free) into an owned `Send+'static` handle BEFORE any query borrow; `Cancel::cancel(self)` opens a side connection and runs `KILL QUERY <id>` best-effort. `lastInsertId` from `Conn::last_insert_id()` / the OK packet (for S7). Note: `mysql_async` populates `last_ok_packet()` only AFTER a result set is fully consumed → the pin/tracker read is POST-DRAIN (matches the S5 `finish()` invariant).
- **Errors map into the ONE §9.2 fate tree, `fate.rs` UNCHANGED (verification P2/P3).** `error_map` keys on **errno** (not SQLSTATE class): `{1213 deadlock, 1205 lock-wait-timeout}` → `Sql{ branch: RETRYABLE }` (1213 is SQLSTATE `40001` but 1205 is `HY000` — a class-40 heuristic would silently drop 1205); `{1317 ER_QUERY_INTERRUPTED (KILL), 3024 MySQL MAX_EXECUTION_TIME, 1969 MariaDB statement-timeout}` → `Sql{ code: errc::CANCELLED }` so the EXISTING `is_57014` (which already fires on `*code == errc::CANCELLED` — the designed non-PG extension point) yields the §19.3 cell (autocommit write→Indeterminate, in-tx→Retryable, read→Cancelled). Preserve raw errno/SQLSTATE. **No `fate.rs`/`is_57014` edit; no "is_57014-analog".** Bind faults → known-fate `Unsupported`, never the fate-unknown `ConnectionLost` (the §19.3 no-false-Indeterminate safety).
- **Bind pre-validation = ARITY only (verification P2).** MySQL `COM_STMT_PREPARE` returns the param COUNT but NOT useful server-inferred per-param types → the PG "each param's type accepts the column type" pre-check has NO MySQL analog. Keep the arity check (mismatch → known-fate `Unsupported`); route any `mysql_async` client-side bind fault → `Unsupported` (that, not a type pre-check, preserves §19.3 for MySQL).
- **Type mapping is POLICY (§9.1); keep the parity fixtures inside the scoped scalars (verification P2).** Map the canonical scalars; resolve ambiguities LOUDLY (an out-of-scope type → `Unsupported`, never a miscast). The pin-identity probe MUST avoid `BIGINT UNSIGNED` (`CONNECTION_ID()` returns unsigned — read `conn.id()` directly instead; use SIGNED `BIGINT` for seed/`AUTO_INCREMENT` fixtures) so the acceptance tests do not force the deferred unsigned-64 policy.
- **The `scan.rs` dialect gap (verification P3).** The shared scanner is dialect-blind and has NO backtick handling; MySQL `"..."` is a STRING LITERAL (the OPPOSITE of PG's identifier), and backticks are MySQL's identifier quote. The safe direction is OVER-pin (a quoted trigger-word treated as code just over-pins, never leaks). Task 6 either dialect-parameterizes the scanner (backtick = identifier/visible, `"..."` = hidden string for MySQL, PG call sites updated) OR explicitly ACCEPTS over-pinning on MySQL `"..."`/backtick content and drops the fragile `` `set` ``-is-CODE assertion. Do NOT claim "mirror pg's quoted-ident handling."
- **MariaDB is a first-class verified target with NAMED checks (user requirement; verification P3).** MariaDB supports `session_track_transaction_info` (≥10.3; the 11.x target is fine) — divergence is fidelity, not existence. Pre-decide + test on the pinned MariaDB 11.x digest: (a) `SERVER_STATUS_IN_TRANS` transitions (fork-free — should be robust); (b) whether a `SET SESSION` inside a stored procedure surfaces on `session_track_state_change`/`system_variables` (the R2 unknown; if NOT, the conservative fallback below must be PROVEN to close the leak on MariaDB).
- **The tracker-verification test is the S6 HARD GATE (R2, M1-D5), and its fallback must be a REAL pin (verification P2/P3).** Assert a tracker fires for `SET SESSION` inside a stored procedure (`CALL p_set_session()`) → the conn is TAINTED (via the new session-tracker wiring) → the next checkout `COM_RESET_CONNECTION`s it → **read the session var back on the recycled conn and assert it is default** (leak CLOSED). If the tracker does NOT fire (MariaDB risk): the fallback MUST actually pin — `classify_one_mysql` **hard-pins `CALL`/`DO`** (they are currently in the shared safe-list, so `pin_on_unknown` does NOT fire on them — this is the v1 false-safety bug) AND/OR `clean_reset_profile()` returns `Some(Full)` (not `None`) until the tracker is proven — and the test asserts the SAME read-back leak-closed. Documenting a hole is a FAIL; only a proven-closed leak (either branch) is a PASS. Amend SPEC §7.1/§8 + a §22 note on the branch taken.
- **Scope bound (product-vision "bound ruthlessly"):** buffered backend + the split pin signal + the dialect + fate + the hard gate — NOT `query_stream` (S7), NOT the shared statement cache, NOT MSSQL (M4), NOT the DBAL driver (S7). Charter gates green.

## File Structure

```
vendor/mysql-async/ (new)             the minimal mysql_async fork adding CLIENT_SESSION_TRACK (+ optional opts hook); root Cargo.toml [patch.crates-io]
UPSTREAM_PR_MYSQL_ASYNC.md (new)       the drafted (not filed) upstream PR text (mirrors /UPSTREAM_PR.md for tokio-postgres)
testkit/docker-compose.yml            + mysql (8.x, digest-pinned) + mariadb (11.x, digest-pinned): healthcheck, tracker config, init.sql
testkit/mysql-init.sql (new)          curated session_track_system_variables list + state_change=ON + transaction_info=STATE; seed table (SIGNED bigint); a stored procedure p_set_session() doing SET SESSION (the §7.1 gate fixture)
engine/crates/ferro-pool/src/backend.rs   + PoolBackend::take_session_mutated (default false) — the additive session-mutation hook
engine/crates/ferro-pool/src/pool.rs      + Checkout::apply_session_tracker (taints on take_session_mutated) alongside apply_tx_status/apply_classify
engine/crates/ferro-pool/src/pin.rs       + PinCause::SessionTracker
engine/crates/ferro-backend-pg/src/conn.rs  + take_session_mutated -> false (no-op; PG unaffected)
engine/crates/ferro-backend-mysql/    NEW crate (buffered parity; NO query_stream)
  Cargo.toml                          the forked mysql_async + mysql_common + ferro-pool + ferro-proto + tokio + async-trait + tracing
  src/lib.rs                          MysqlBackend + PoolBackend impl (query_stream -> Unsupported)
  src/conn.rs                         connect (fork caps + tracker SETs + baseline) + ping + is_closed + reset (COM_RESET_CONNECTION) + cancel_handle (KILL QUERY via Conn::id) + tx_status (status_flags) + take_session_mutated + simple_query
  src/tracker.rs                      OK-packet parse: status_flags -> TxStatus (InTx/Idle, never Failed); SessionStateChange::{SystemVariable,StateChange} -> session_mutated (baselined, curated)
  src/query.rs                        query (buffered) + lastInsertId; NO query_stream (S7)
  src/rowmap.rs                       MySQL value -> canonical Value (scoped scalars; loud Unsupported)
  src/bind.rs                         arity check + route bind faults to Unsupported
  src/error_map.rs                    errno-keyed -> PoolError (1213/1205 Retryable; 1317/3024/1969 CANCELLED; §22 divergence)
engine/crates/ferro-classify/src/rules.rs   classify_one_mysql live + MySQL triggers + the CALL/DO conservative-fallback pin
engine/crates/ferro-classify/src/scan.rs     dialect-aware backtick/`"..."` handling OR documented over-pin
engine/crates/ferrod/src/{config.rs,services/sql.rs}  kind="mysql" pool wiring; fetch:stream on MySQL -> Unsupported("S7")
engine/crates/ferrod/tests/mysql_it.rs (new)  the hard gate + live parity + MariaDB checks + pin-cause + MySQL chaos-fate
ferro-spec-v0.2.md §8/§7.1/§22        amend for the split authority, the fork, no-Failed-from-tracker, the fallback branch, the MySQL/MariaDB timeout-errno divergence, query_stream-deferred
```

---

### Task 1: fork `mysql_async` for `CLIENT_SESSION_TRACK` (the S1 analog) + a BEHAVIORAL tracker spike

**Files:** Create `vendor/mysql-async/` (the fork), root `Cargo.toml` `[patch.crates-io]`, `UPSTREAM_PR_MYSQL_ASYNC.md`; a spike test.

**Interfaces produced:** a `mysql_async` build that negotiates `CLIENT_SESSION_TRACK`, so `OkPacket::session_state_info()` is NON-EMPTY after a session mutation; confirmed accessors (`Conn::{id,last_insert_id,last_ok_packet}`, `OkPacket::{status_flags,session_state_info}`, `SessionStateChange`).

- Confirm the driver reality (the verification established it, re-verify): `mysql_common` exposes `OkPacket::session_state_info() -> Vec<SessionStateInfo>` + `SessionStateChange::{SystemVariable,StateChange,TransactionState,…}` + `OkPacket::status_flags()`; `mysql_async` re-exports `OkPacket` + `Conn::{id,last_insert_id,last_ok_packet}`. The GAP is `mysql_async::Opts::get_capabilities()` not setting `CLIENT_SESSION_TRACK` (no opts hook). **FORK it** (vendored `/vendor/mysql-async`, `[patch.crates-io]`): OR the `CLIENT_SESSION_TRACK` bit into `get_capabilities()` (and, if clean, add an `OptsBuilder::additional_capabilities`). Draft `UPSTREAM_PR_MYSQL_ASYNC.md` (do NOT file — human authorization, mirroring `/UPSTREAM_PR.md`).
- **BEHAVIORAL spike (not accessor-presence — verification P1-minor):** a live `#[tokio::test]` (skip without `FERRO_TEST_MYSQL_URL`) that, over the FORKED build, runs `SET SESSION sort_buffer_size=…` and asserts `last_ok_packet().session_state_info()` decodes a NON-EMPTY `SessionStateChange::{SystemVariable,StateChange}`, AND `START TRANSACTION`/`COMMIT` toggles `status_flags() & SERVER_STATUS_IN_TRANS`. Accessor presence alone is NOT evidence — the capability must be negotiated + firing live.
- **Gate:** the fork builds (`cargo build -p ferro-backend-mysql` with the `[patch]`); the behavioral spike passes live (non-empty tracker + status-flag transition); `cargo build --workspace`; the decision + evidence recorded.
- **Commit** `feat(m1-s6): fork mysql_async for CLIENT_SESSION_TRACK (S1 analog) + behavioral tracker spike (non-empty session-state-info live)`.

---

### Task 2: testkit MySQL 8 + MariaDB 11 (trackers, stored proc) + `ferro-pool` session-tracker hook + crate skeleton

**Files:** Modify `testkit/docker-compose.yml`, root `Cargo.toml`; Create `testkit/mysql-init.sql`, the `ferro-backend-mysql` skeleton; Modify `engine/crates/ferro-pool/src/{backend.rs,pool.rs,pin.rs}` + `engine/crates/ferro-backend-pg/src/conn.rs`.

**Interfaces produced:** `PoolBackend::take_session_mutated(&self, &Self::Conn) -> bool` (default-ish; PG returns `false`); `Checkout::apply_session_tracker`; `PinCause::SessionTracker`; `MysqlBackend` compiling at buffered `PoolBackend` parity (`query_stream -> Err(Unsupported)`); `mysql_url()`/`mariadb_url()` helpers; healthy MySQL 8 + MariaDB 11 with trackers + `p_set_session()`.

- **The additive `ferro-pool` change (verification P3-2):** add `take_session_mutated` to `PoolBackend` (returns + clears a per-conn mutation flag); add `Checkout::apply_session_tracker(&mut self)` that calls it and, if `true`, taints with `PinCause::SessionTracker` (run it right after `apply_tx_status`, before `apply_classify`, on the same 6 instrumented methods); add `PinCause::SessionTracker`. `ferro-backend-pg` implements `take_session_mutated -> false` (PG unaffected — additive). Update the `FakeBackend`.
- `testkit`: MySQL 8.x + MariaDB 11.x (digest-pinned, healthcheck, distinct ports), mounting `mysql-init.sql` which sets a CURATED `session_track_system_variables` (not `'*'`) + `session_track_state_change=ON` + `session_track_transaction_info=STATE`, seeds a SIGNED-`bigint` table, and defines `p_set_session()` (a proc body running `SET SESSION`). `mysql_url()`/`mariadb_url()` skip-helpers.
- `ferro-backend-mysql` skeleton: every buffered `PoolBackend` method compiling; `query_stream` returns `PoolError::Unsupported("MySQL streaming lands in M1-S7")`; `dialect() -> Dialect::MySql`.
- **TDD:** both DBs healthy; the helpers skip without env; `cargo build --workspace`; the new PG `take_session_mutated -> false` + `FakeBackend` compile; existing `ferro-pool`/PG tests green (additive).
- **Gate:** `cargo build --workspace`; `docker compose up -d mysql mariadb` healthy; `cargo test -p ferro-pool -p ferro-backend-pg` green; fmt/clippy; offline `cargo test --workspace` green.
- **Commit** `feat(m1-s6): ferro-pool session-tracker hook (take_session_mutated + apply_session_tracker + PinCause::SessionTracker, PG no-op) + testkit MySQL8/MariaDB11 + backend skeleton`.

---

### Task 3: connect + the SPLIT pin signal + `conn.rs`/`tracker.rs` (`tx_status` from status flag, taint from trackers, `simple_query`, `reset`, `cancel`)

**Files:** `engine/crates/ferro-backend-mysql/src/{conn.rs,tracker.rs,lib.rs}`.

**Interfaces produced:** `connect` (fork caps + tracker SETs + baseline); `tx_status(conn) -> TxStatus` (from `status_flags`); `take_session_mutated` (from the trackers, baselined+curated); `simple_query`; `reset` (both `ResetProfile` arms); `cancel_handle` (`KILL QUERY`); `ping`; `is_closed`; `clean_reset_profile`.

- `connect`: forked `mysql_async` (CLIENT_SESSION_TRACK negotiated), set the curated `session_track_*` on connect, then **BASELINE** — record/clear the mutation state AFTER setup so the connect SETs do not taint (per-lease measurement). Capture `Conn::id()`.
- `tracker.rs`: `tx_status` reads `last_ok_packet().status_flags() & SERVER_STATUS_IN_TRANS` → set ⇒ `InTx`, unset ⇒ `Idle` — **never `Failed`** (verification P1-2). `take_session_mutated` reads the decoded `SessionStateChange::{SystemVariable, StateChange}` since the last baseline and returns `true` iff a NON-baseline, non-allowlisted mutation was reported; then clears the per-lease flag. Match on `TransactionState` (NOT `TransactionCharacteristics`) if the tracker byte is ever consulted (verification P1-4) — but tx-open comes from the status flag, so the STATE byte is only a cross-check. Post-drain read only (`last_ok_packet` is populated after result consumption).
- `simple_query` (verification P2/P3, REQUIRED): a single-round-trip `COM_QUERY` returning `affected`/`0`, failures through `error_map` (session-fatal-first, exactly like PG's `simple_query`, so a serialization/deadlock caught AT COMMIT survives as `Sql{Retryable}`). `reset`: `COM_RESET_CONNECTION` for `Full`; define the `Targeted` arm (map to `COM_RESET_CONNECTION` or an explicit `unreachable!` tied to `clean_reset_profile`). `cancel_handle`: owned `(id, connect-opts)` handle (borrow-free, `Send+'static`); `cancel(self)` opens a side conn (same user) → `KILL QUERY <id>` best-effort. `ping`/`is_closed`: use `mysql_async`'s connection-state signal (there is NO spawned-driver `AtomicBool` to mirror PG — specify the real signal). `clean_reset_profile`: `None` (skip) ONLY once the tracker is proven to fire in stored programs (Task 7); until then / on the fallback branch, `Some(Full)`.
- **TDD (live, both DBs, skip without env):** `START TRANSACTION` → `tx_status==InTx`; `COMMIT` → `Idle`; a statement error inside a tx → still `InTx` (NOT `Failed` — MySQL keeps it open); a `SET SESSION` → `take_session_mutated==true`; the connect SETs alone → `take_session_mutated==false` (baseline works); a benign allowlisted var (autocommit toggle) → does NOT taint; `COM_RESET_CONNECTION` clears a `SET SESSION` (read-back default); `KILL QUERY` cancels a `SELECT SLEEP(3)`.
- **Gate:** `cargo test -p ferro-backend-mysql` (live, both) + offline skip; `cargo build --workspace`; fmt/clippy; PG untouched.
- **Commit** `feat(m1-s6): MySQL split pin signal — tx_status from SERVER_STATUS_IN_TRANS (never Failed) + session-mutation taint from baselined trackers + simple_query + reset + KILL QUERY cancel`.

---

### Task 4: the buffered data path — `query` + `rowmap` + `bind` + `error_map`

**Files:** `engine/crates/ferro-backend-mysql/src/{query.rs,rowmap.rs,bind.rs,error_map.rs}`.

**Interfaces produced:** `query(conn, sql, params) -> QueryResult{cols, rows, affected}` (buffered, incl. `last_insert_id`); `rowmap::extract_value`; `bind` (arity + fault-routing); `error_map::map`.

- `query.rs`: prepare (`COM_STMT_PREPARE`) + execute; `cols: Vec<ColMeta>` from the statement columns (MySQL column-type → canonical tag); `bind` ARITY check before send (mismatch → known-fate `Unsupported`); collect rows (buffered); `affected` from the OK packet; `last_insert_id` from `Conn::last_insert_id()`.
- `rowmap.rs`: MySQL value → canonical `Value` (scoped scalars: `I64`←signed `INT`/`BIGINT`, `F64`←`DOUBLE`, `Bool`←`TINYINT(1)`, `Text`←`VARCHAR`/`TEXT`, `Bytes`←`BLOB`); out-of-scope (unsigned-64, `DECIMAL`, `DATETIME`) → LOUD `Unsupported`. Keep the acceptance fixtures inside these scalars.
- `bind.rs`: arity only (no type-accepts — MySQL exposes no inferred types); route any client-side bind fault → `Unsupported` (never `ConnectionLost`) — the §19.3 no-false-Indeterminate safety.
- `error_map.rs`: errno-keyed → `PoolError` (verification P2): `{1213,1205}` → `Sql{ branch: RETRYABLE }` (mirror PG's `error_map` `classify_sqlstate` setting `branch` — `classify_fate` passes `code`+`branch` VERBATIM, it does NOT derive Retryable from the SQLSTATE); `{1317, 3024 (MySQL), 1969 (MariaDB)}` → `Sql{ code: errc::CANCELLED }` (the EXISTING `is_57014` catches it — no `fate.rs` edit); a transport failure → `ConnectionLost`. Preserve raw errno/SQLSTATE. §22 note on the MySQL(3024)/MariaDB(1969) timeout-errno divergence.
- **TDD (live both + unit):** each scoped scalar round-trips; a deadlock (two txs) → `error_map` → `classify_fate` → `Retryable`; a bind-arity mismatch → `Unsupported` (never `ConnectionLost`); `last_insert_id` after `INSERT` on a SIGNED-`bigint` `AUTO_INCREMENT`; unit — the errno→PoolError table (incl. 3024/1969), the type-map `Unsupported`.
- **Gate:** `cargo test -p ferro-backend-mysql` (live + unit) + offline skip; `cargo build --workspace`; fmt/clippy.
- **Commit** `feat(m1-s6): MySQL buffered data path — query + rowmap (scoped scalars) + bind arity/fault-routing + errno-keyed error_map into the S4 fate tree (1213/1205 Retryable; 1317/3024/1969 CANCELLED)`.

---

### Task 5: config wiring + `fetch:stream`-on-MySQL rejection

**Files:** `engine/crates/ferrod/src/{config.rs,services/sql.rs}`.

**Interfaces produced:** a `kind = "mysql"` pool spec (builds `MysqlBackend`); `fetch:stream` on a MySQL pool → a clean `Unsupported`.

- A `ferrod` pool declarable `kind = "mysql"` (credential/DSN path mirroring PG; `Config::validate` (S5-T1b) still holds). If backend selection is an enum, add the MySQL arm.
- `handle_exec`: a `fetch:stream` request routed to a MySQL pool → `end_error(unsupported("fetch=stream is not yet supported on MySQL (lands in M1-S7 with the streaming bridge)"))` — the buffered `fetch:rows`/`none` work; only streaming is rejected. A §22 note records the deferral.
- **TDD:** a `kind="mysql"` pool round-trips a buffered `SELECT` end-to-end through `ferrod` (live); a `fetch:stream` to a MySQL pool → the documented `Unsupported`; the PG streaming path unaffected.
- **Gate:** `cargo test -p ferrod` (+ live) + offline skip; `cargo build --workspace`; fmt/clippy.
- **Commit** `feat(m1-s6): kind="mysql" pool wiring + fetch:stream-on-MySQL -> Unsupported (streaming deferred to S7, §22)`.

---

### Task 6: the assist-lexer MySQL dialect — `classify_one_mysql` live + the CALL/DO conservative-fallback pin + `scan.rs` dialect

**Files:** Modify `engine/crates/ferro-classify/src/rules.rs`, `src/scan.rs`.

**Interfaces produced:** `classify_one_mysql` live with MySQL triggers + a mode where `CALL`/`DO` pin (the fallback); `scan.rs` handling MySQL quoting (or a documented over-pin).

- `classify_one_mysql`: layer MySQL triggers on the shared base list (ASSIST — the tracker is authority): `SET SESSION`/`SET @@session`/`SET GLOBAL` (already caught — SESSION not excluded); `GET_LOCK`/`RELEASE_LOCK`/`RELEASE_ALL_LOCKS`; `CREATE TEMPORARY TABLE`/temp DDL (reuse `create_is_temp`); raw `PREPARE`/`EXECUTE`/`DEALLOCATE`; `LOCK TABLES`; unknown → pin. **The conservative-fallback pin (verification P2/P3 — the false-safety fix):** `CALL` and `DO` are currently in the shared `SAFE_LEADING_KEYWORDS`, so they never pin — but a `CALL p_proc()` can mutate session state INSIDE the proc where the lexer can't see. For the tracker-unverified fallback, `classify_one_mysql` MUST pin `CALL`/`DO` (treat them as tracker-ambiguous → pin) — either always (safe, a modest over-pin) or gated on a "tracker-unverified" flag. Decide + state it: given a MySQL clean conn skips hygiene, the safe default is **`classify_one_mysql` pins `CALL`/`DO`** so a stored-proc mutation is caught even if the tracker misses it (belt-and-braces with the tracker authority).
- `scan.rs` (verification P3): MySQL `"..."` is a STRING (hide it), backticks are identifiers (visible). Either dialect-parameterize `scan()`/`leading_keyword`/`split_top_level_statements` (backtick=ident, `"..."`=string for MySQL; update the PG call sites) OR explicitly accept over-pinning on MySQL `"..."`/backtick content (a quoted keyword treated as code just over-pins — safe) and DROP any `` `set` ``-is-CODE None-assertion. Pick the simpler; document the choice. Do NOT claim "mirror pg."
- Wire `Dialect::MySql` live (the MySQL `Checkout::apply_classify` resolves to `classify_one_mysql`).
- **TDD (unit):** `classify_one_mysql` pins `SET SESSION x=1`, `GET_LOCK('a',1)`, `CREATE TEMPORARY TABLE t(...)`, `PREPARE s FROM ...`, `CALL p_set_session()` (the fallback fix), and an unknown; does NOT pin a plain `SELECT`/`INSERT`. The `scan.rs` choice has a test (either MySQL `"str"` is hidden, or the documented over-pin). The PG classifier untouched + green.
- **Gate:** `cargo test -p ferro-classify`; `cargo build --workspace`; fmt/clippy; PG classifier green.
- **Commit** `feat(m1-s6): classify_one_mysql live — MySQL triggers + CALL/DO conservative-fallback pin (closes the in-stored-proc leak) + scan.rs MySQL quoting`.

---

### Task 7: the tracker-verification HARD gate (real, read-back) + live MySQL/MariaDB pin/hygiene parity + pin-cause

**Files:** Create `engine/crates/ferrod/tests/mysql_it.rs` (live, skip without env); amend SPEC §7.1/§8/§22.

**Interfaces consumed:** the full MySQL backend + the session-tracker wiring + `Dialect::MySql` + the CALL/DO fallback.

- **THE HARD GATE (R2, real — verification P2/P3):** call `p_set_session()` (the `SET SESSION`-in-a-proc fixture). **Fires branch:** assert the session-mutation tracker fired → the conn is TAINTED (`PinCause::SessionTracker`) → the next checkout ran `COM_RESET_CONNECTION` → **read the session var back on the recycled conn and assert it is default** (leak CLOSED). **Does-NOT-fire branch (MariaDB risk):** assert the conservative fallback tainted it anyway (`classify_one_mysql` pinned the `CALL`, or `clean_reset_profile()->Some(Full)`) → the SAME read-back leak-closed assertion. Run on BOTH DBs; the branch taken is recorded in a §22 note. **A leak that survives (var not default on the next tenant) is a FAIL** regardless of any note.
- **Live parity (both DBs, pin-cause asserted — charter DoD):** a `BEGIN..COMMIT` pins exactly one MySQL connection id + unpins on commit (`PinCause::Tx`); a `SET SESSION` mid-lease pins via the tracker (`PinCause::SessionTracker`); an autocommit `SELECT` never pins; a tracker-clean conn SKIPS `COM_RESET_CONNECTION` while a tainted one runs it (read-back); the M0 SQL/TX live scenarios pass. Use `conn.id()` for identity (NOT `CONNECTION_ID()` → unsigned-64).
- **MariaDB divergence doc:** the named checks (status-flag transitions robust; whether in-proc `SET SESSION` surfaces on MariaDB) → a §22 line + the fallback branch proven if the tracker gaps.
- **Gate:** `FERRO_TEST_MYSQL_URL=… FERRO_TEST_MARIADB_URL=… cargo test -p ferrod --test mysql_it` (all pass, both DBs; the hard gate leak-closed in whichever branch); offline skip; `cargo build --workspace`; fmt/clippy; SPEC §7.1/§8 amended + §22.
- **Commit** `feat(m1-s6): tracker-verification HARD gate (SET SESSION-in-proc, read-back leak-closed) + live MySQL/MariaDB pin/hygiene parity + pin-cause (+ §22 branch/divergence notes)`.

---

### Task 8: MySQL §19.3 chaos-fate parity + tx-actor rollback re-verification + §9.1 type policy

**Files:** Extend `engine/crates/ferrod/tests/chaos_fate_it.rs` (or `mysql_chaos_it.rs`); amend SPEC §9.1/§22.

**Interfaces consumed:** the full MySQL backend + `fate.rs` + the tx actor.

- **§19.3 chaos on MySQL (charter §20.3):** kill a MySQL connection / cancel+timeout a statement; assert every in-flight request lands in the specified fate branch AND a write is applied at-most-once — the SAME `fate.rs`, now against real MySQL. Cover: cancelled/timed-out autocommit write → `Indeterminate`; in-tx cancel → rollback + `Retryable`; deadlock (1213) / lock-wait-timeout (1205) → `Retryable`; connection kill mid-write → `Indeterminate`.
- **tx-actor rollback/tombstone re-verification (verification P2 caveat):** 1213 auto-rolls-back the WHOLE tx (tracker → `Idle`); 1205 (default) rolls back only the STATEMENT (stays `InTx`). Assert the tx-actor's cancel→drain→ROLLBACK→tombstone still yields the correct ONE terminal for BOTH — esp. that a 1205 leaving the tx `InTx` doesn't confuse the teardown (the actor issues an explicit ROLLBACK regardless, so verify it's idempotent/correct against MySQL's post-error tx state).
- **§9.1 type policy:** record any pinned policy (unsigned-64, `DECIMAL`) in SPEC §9.1 + §22 so S8's DBAL suite inherits a documented policy.
- **Gate:** `cargo test -p ferrod` (+ live chaos, both DBs) + `cargo test --workspace` offline green; `cargo build --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; fmt; PHP unaffected.
- **Commit** `feat(m1-s6): MySQL §19.3 chaos-fate parity (at-most-once, all branches) + tx-actor 1213/1205 rollback re-verification + §9.1 type policy`.

---

## Verification fold (3 probes — FIX_FIRST)

| Sev | v1 defect | v2 fix (task) |
|-----|-----------|----------------|
| blocker | `mysql_async` as-is assumed to expose trackers — it never negotiates `CLIENT_SESSION_TRACK` → always empty | FORK it (add the capability bit); behavioral spike (T1) |
| blocker | pin authority routed only through the tracker (fork-dependent, MariaDB-fragile) | SPLIT: tx-open from `SERVER_STATUS_IN_TRANS` status flag (fork-free); taint from trackers (T3) |
| blocker | session-mutation taint had NO wiring into `Checkout` ("changes nothing in ferro-pool" was false) | new `take_session_mutated` + `apply_session_tracker` + `PinCause::SessionTracker` (T2) |
| blocker | the hard-gate fallback was a false safety (`CALL` is safe-listed → no pin → leak certified PASS) | `classify_one_mysql` pins `CALL`/`DO`; `clean_reset_profile->Some` until proven; gate asserts read-back leak-closed (T6/T7) |
| blocker | `query_stream` "mirror pg" impossible (mysql_async stream borrows Conn) | DEFERRED to S7 (user decision); `fetch:stream`-on-MySQL → `Unsupported` (T5) |
| blocker | `simple_query` unassigned (7 pin-hook call sites would panic) | assigned to T3 |
| major | `TxStatus::Failed` mapped from a MySQL signal that never exists | never synthesize `Failed`; fate keys off the ERROR not `Idle` (T3) |
| major | bind "type-accepts" pre-check has no MySQL analog | arity-only + route faults to `Unsupported` (T4) |
| major | `error_map` under-enumerated + MySQL/MariaDB timeout divergence | errno-keyed 1213/1205 + 1317/3024/1969; §22 (T4) |
| major | over-pinning (`'*'` + connect SETs taint every conn) | baseline after connect + curated var list (T3) |
| major | `scan.rs` "mirror pg quoted-idents" — MySQL `"..."` is the OPPOSITE (a string) | dialect-aware or documented over-pin (T6) |
| major | MariaDB "document divergence" — not pre-empted | named checks + proven fallback (T7) |
| minor | "is_57014-analog"/"classify_fate routes Retryable" wording invites a `fate.rs` edit | reworded: `error_map` sets `code=CANCELLED`/`branch=RETRYABLE`, `fate.rs` UNCHANGED (T4, global) |
| minor | `is_closed` mirrors a PG driver-task AtomicBool that mysql_async lacks; `reset` `Targeted` arm; CHARACTERISTICS vs STATE; Task-1 accessor-vs-behavioral | specified (T3, T1) |

## Self-Review

- **Spec coverage:** fork (T1); testkit + ferro-pool hook + skeleton (T2); split pin signal + conn methods + simple_query (T3); buffered data path + error_map (T4); config + stream-rejection (T5); the lexer dialect + CALL/DO fallback + scan.rs (T6); the REAL hard gate + live parity + MariaDB (T7); chaos-fate + tx-actor re-verification + type policy (T8). Every exec-design S6 / M1-D5 / R2 / §8 requirement maps to a task; `query_stream` is the one documented deferral (S7, user-chosen).
- **The pin architecture is now correct:** two OK-packet signals (status flag = tx-open authority, fork-free; trackers = session-mutation taint, forked), the additive `ferro-pool` wiring, no `Failed`-from-tracker, the over-pin baseline. The hard gate is REAL (read-back leak-closed, both branches). `fate.rs` is genuinely unchanged (errno-keyed `error_map` → the existing `is_57014`/`Sql` passthrough).
- **Scope bound:** buffered parity only; no streaming (S7), no statement cache, no MSSQL (M4), no DBAL driver (S7).

## Execution Handoff

Subagent-driven: fresh implementer per task. **T1 (the fork) is the gate — its behavioral spike must show live non-empty trackers before T3+.** T3 (split pin signal), T4 (data path + error_map), T7 (the real hard gate) are correctness-critical — review on a capable model, probing the status-flag/tracker split, the no-`Failed` mapping, the errno→fate mapping, and the read-back leak-closed gate. T2's `ferro-pool` change is small but touches shared code (PG no-op) — review for additivity. Whole-branch review before S7. Live tests against testkit MySQL 8 + MariaDB 11.
