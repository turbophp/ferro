# Ferro M1-S9a — Core Hardening Implementation Plan

> **PLAN STATUS: v1 + a PARTIAL adversarial pass (2026-08-11).** The verification agent died on a
> session limit before finishing. What it DID establish is applied inline below, marked `PLAN-VERIFY`:
> **F1** (Task 1's `params!` does not compile as written), **F2** (Task 1's comment-marker is stripped
> by MariaDB, and its missing `COMMAND` filter becomes a TIME BOMB once Task 9 lands), **F3** (Task 4's
> reaper test could not pass against a correct implementation), and **S2 REFUTED** (leading `COMMIT`
> is correctly not a hazard — it is structurally unreachable through EXEC).
> It also CONFIRMED the slice's central ordering claim by execution: Task 1's guard is genuinely
> falsifiable — both tests green at HEAD, both RED under the `sql.rs:332` mutation with the exact
> predicted shape.
>
> **THREE SUSPICIONS WERE NEVER MEASURED** and are the implementer's job to settle at task time, not
> to assume away — they are recorded in full in
> `.superpowers/sdd/2026-08-11-ferro-m1-s9a-core-hardening/plan-verify.md`:
> **S3** Task 5 uses `FrameCodec::default()`, which may not exist until Task 11 (cross-task ordering);
> **S4** Task 12's drain timing is razor-thin against a 1s tick and is suspected flaky;
> **S5** Task 11's stall detector measures time-since-frame-STARTED, not time-since-last-PROGRESS, so
> a slow-but-progressing client is killed while the docblock promises otherwise.
> If any of them proves out, STOP and report it rather than working around it.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** close the M0-core review's findings (`docs/followups/2026-08-11-m0-core-review-findings.md`) before the M1 exit gate — foremost the CONFIRMED silent at-least-once on MySQL (an implicit-commit statement inside an explicit transaction commits everything before it, and `fate.rs` then licenses replay of writes that already persisted), plus the false `Indeterminate` on a pre-dispatch connection death, the three unbounded awaits that let one wedged backend take a pool down permanently, the one-local-client daemon-exhaustion class, the SIGTERM drain that is wired to nothing, and the two §12/abort hygiene minors.

**Architecture:** Three waves, ordered by file ownership so concurrent implementers never share a file. **Wave A (Tasks 1–6, parallel)** lands the guard that makes the safety property observable BEFORE the property is changed (Task 1), the new lexical assist (Task 2), and the four self-contained bounds/hygiene fixes that each own their file (pool checkout, reaper, codec reserve, `loggable_scheme`). **Wave B (Tasks 7–10, strictly serial — they share `fate.rs`/`sql.rs`/`actor.rs`/`error.rs`)** is the fate-correctness chain: the `tx_writes_persisted` context + the RETRYABLE-suppression rule, the actor latch + pre-dispatch hazard that feed it, the `ConnectionLost{dispatched}` refinement, and the bounded post-cancel drain. **Wave C (Tasks 11–12, serial; 11 may overlap Wave B)** is the daemon availability pair: the three knobs (`max_connections`, `frame_read_timeout`, `idle_timeout`) and the real SIGTERM drain. **Task 13** is the single-author SPEC-DELTA batch — no earlier task edits the spec (the S8a (u)/(v) contradiction came from parallel spec authorship).

**Tech Stack:** Rust (edition 2024, tokio multi-thread) across `ferro-pool`, `ferro-classify`, `ferro-backend-pg`, `ferro-backend-mysql`, `ferrod`; live acceptance against the Dockerized PG 17 / MySQL 8.4 / MariaDB 11.8 testkit; no PHP change, no `/proto` change, no new dependency.

---

## Global Constraints

Every task's requirements implicitly include this section. Each hazard below was verified against the code at HEAD `d712baf` (branch `m1-build`) or measured live by the M0-core review (journals: `.superpowers/sdd/2026-08-11-m0-core-review/`).

### Contract rules (non-negotiable, copied from `CLAUDE.md`)

- **Charter rule 1 — decisions in SPEC §21 are binding.** Do not re-litigate them in code, comments, or refactors. If one proves impossible, stop and raise it — don't route around it.
- **Charter rule 2 — `/proto` is the single source of truth** for method ids, flags, error codes, and type tags. **This slice makes NO `/proto` change.** Every wire answer this plan mints reuses an existing `errc` with its registry-pinned branch (`POOL_TIMEOUT`/Retryable for the connection cap and the drain refusal; `WRITE_UNCONFIRMED`/Indeterminate for the persisted-transaction cell). Two candidates are explicitly **deferred and recorded by Task 13**, not hand-rolled: a dedicated `TX_PARTIALLY_COMMITTED` code and an `ERR_OVERLOADED`/`ERR_SHUTTING_DOWN` pair. If any task finds itself wanting a new wire constant, **STOP and raise it** — that is a registry + golden-vectors + BOTH-codecs change set.
- **Charter rule 3 — the engine never transparently retries user statements.** Nothing in this slice adds one: no reconnect-and-replay, no retry on a failed checkout, no "helpful" re-dispatch after a bounded drain expires. This slice exists to make the *reported* fates honest; the engine's own behavior stays classify-and-report.
- **Charter rule 4 — every in-flight request terminates in exactly ONE frame carrying END.** Finding 4(c) currently breaks this (a wedged post-cancel drain never declares a terminal); Task 10 closes it, and nothing else in this plan may open a new path that exits without declaring exactly one terminal. Every drain/refusal/deadline added here must be shown to declare its terminal (or provably run before a request exists, e.g. the accept-time connection cap).
- **Charter rule 5 — correctness over throughput.** Every bound added here trades a wedged-forever hang for a loud, classified error. Where a bound can misfire on a healthy-but-slow backend (the checkout deadline evicting a conn whose cleanup ran out of caller budget, Task 3), the misfire direction is a reconnect — never a dirty conn handed out, never a dropped terminal.
- **Charter rule 6 — no read/write inference from SQL text.** Task 2's `implicit_commit_hazard` is NOT read/write inference: it is the same species as the S2 assist lexer — a dialect-scoped lexical assist for a protocol-invisible hazard (a statement lost mid-flight emits no OK packet), and it can only ever make a classification MORE conservative (Retryable → Indeterminate), never less, never a routing/rewrite decision. The RFQ/OK-packet signal stays the authority everywhere it exists.
- **SPEC §19.3 — the directional rule.** Every refinement in this plan must err in the safe direction: a missed `dispatched:false` mark yields a false `Indeterminate` (cry-wolf); a missed implicit-commit hazard would yield a false `Retryable` (at-least-once) — which is why the hazard's unknown-keyword default is HAZARD and why `error_map`'s default for the new field is `dispatched: true`.
- **No cross-tenant connection-state leak.** Task 10's wedged-drain expiry drops a future mid-statement; the conn MUST be tainted before it re-enters the pool (the bounded recycle then resets or evicts it). Same for Task 3's evict-on-cleanup-expiry: never hand out a conn whose cleanup did not complete.

### THE dominant defect class in this project — tests that cannot fail

17+ across slices; the M0 review landed it on the defining safety property itself: `in_tx: true` at `sql.rs:332` is **unobservable by the entire suite** (mutation run 2026-08-11: flipped to `false`, 8 suites + 142 lib tests ALL GREEN). Binding rules for this plan:

1. **Every guard added must be proven by MUTATION**: apply the named production mutation, run the guard, record RED, restore. Each task names its mutation(s) explicitly, and each was sanity-checked against the real code path while writing this plan (three of one S8b task's four named mutations were no-ops — do not skip the RED run).
2. **A live chaos test must prove its event was in-flight**, never sleep-and-hope: PG uses a `pg_stat_activity` poll with a per-test marker (the `chaos_fate_it.rs` pattern); MySQL uses `information_schema.processlist` with an `INFO LIKE` marker (the `mysql_chaos_it.rs` pattern). A kill that lands before dispatch proves nothing and can pass for the wrong reason.
3. **Prefer compile-forced, then derived, then behavioral.** `PoolError::ConnectionLost { dispatched }` (Task 9) and the `ExecReply`/`TxLookupErr` field additions (Task 8) are struct-variant changes precisely so every site must CHOOSE at compile time rather than inherit a default.
4. **Assert from a vantage point where the property is observable.** The at-least-once proof reads the database back over a FRESH connection (never the session under chaos); the permit-leak proofs assert a LATER checkout succeeds, not that "no error was logged".
5. **`ci/assert-no-skips.sh` is live** on the Rust log: every new live test MUST print `skip: <reason>` (word `skip` followed by a colon) on its unconfigured path, and no test NAME may contain the bare word `skip` followed by `:`/space/end.

### Definition of done (charter DoD, EVERY task)

- `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace` — green **offline** (live tests skip, never fail, when `FERRO_TEST_*_URL` are unset).
- The live tiers, run BY HAND against the shared containers (NEVER `ci/local-gate.sh --live` — its EXIT trap runs `docker compose down -v`):
  `FERRO_TEST_PG_URL=... FERRO_TEST_MYSQL_URL=... FERRO_TEST_MARIADB_URL=... cargo test -p ferrod --test <suite> -- --nocapture` for every suite the task touches, plus the FULL live `ferrod` suite at each wave boundary.
- **Every guard added is mutation-proven** (rule 1 above), with the RED output recorded in the commit message or task journal.
- **No `/proto` change.** If one seems necessary, stop and raise it.
- **NO SPEC EDITS in Tasks 1–12.** Each task records the delta it forces in `docs/followups/2026-08-11-s9a-spec-deltas.md` (append-only, one `### Task N` block each); Task 13's single author applies them all to `ferro-spec-v0.2.md` + `CLAUDE.md` at the end. This is a hard rule: parallel spec authorship produced the S8a §22.2 (u)/(v) contradiction.
- **Regression proof against the sound list.** The review measured these SOUND: the wire codec + session state machine (exactly-one-END, fail-closed handshake, decode safety), the pin authority + Err-arm fail-safes, PG hygiene's reset profiles. Every task below names which of these its files touch and which existing suite re-run proves no regression — "didn't touch it" is only acceptable when the task genuinely owns none of those files.

### Live test environment

The containers are ALREADY UP and are SHARED. Do not tear them down.

```
PG      postgres://ferro:ferro@127.0.0.1:55432/ferro
MySQL   mysql://ferro:ferro@127.0.0.1:33060/ferro
MariaDB mysql://ferro:ferro@127.0.0.1:33061/ferro
```

Standard live invocation shape (mirrors every existing `ferrod` live suite):

```bash
cd /home/abdullak/projects/ferro && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_TEST_MARIADB_URL="mysql://ferro:ferro@127.0.0.1:33061/ferro" \
cargo test -p ferrod --test in_tx_fate_it -- --nocapture
```

---

## Verified hazards — a naive implementation is WRONG

**The blocker (finding 1) and its guard (finding 2)**

1. **`OpContext.in_tx` is a CALL-SITE CONSTANT** (`ferrod/src/services/sql.rs:329-333` — `in_tx: true` hardcoded for every tx-scoped EXEC Err; `sql.rs:755-759` — same for `run_tx_streamed`'s ctx), while the LIVE state it claims to describe is read and then dropped: `Checkout::apply_tx_status` (`ferro-pool/src/pool.rs:821-832`) writes `tx_open` from `SERVER_STATUS_IN_TRANS`/the RFQ byte after EVERY statement, and `Checkout::tx_open()` is already public (`pool.rs:330`). Neither the tx actor nor any fate call site consults it.
2. **MySQL implicit commit fires BEFORE the statement executes** (MySQL manual, "Statements That Cause an Implicit Commit": DDL, `LOCK TABLES`, `SET autocommit=1`, administration/replication statements — they "commit the current transaction before executing"). Measured live (m0-fate journal): `START TRANSACTION; INSERT 1; CREATE TABLE; INSERT 2;` + abrupt disconnect, NO COMMIT → **both rows persisted**. Two consequences: (a) a loss AFTER the DDL is a loss on a de-facto autocommit connection inside a partially-COMMITTED "transaction"; (b) a loss OF the DDL itself is ALREADY past the commit point — so the protocol latch alone cannot close the hole; only a PRE-dispatch lexical signal exists for (b).
3. **The replay license is branch-shaped, not errc-shaped.** Three distinct terminals license replay of an in-tx failure today: `TxDeadline{Retryable}` (`sql.rs:338-341`, `actor.rs:383/402`), `ConnectionLost{Retryable}` (`fate.rs:147-165`, the `in_tx` arm), and a **retryable `Sql` passthrough** (a 1213 deadlock on the post-DDL statement rides `fate.rs:116-138` verbatim with `branch::RETRYABLE`). Fixing only the first two leaves the third licensing replay. The rule must be: once tx-writes-persisted, `branch::RETRYABLE` is unmintable for that call site.
4. **`ExecReply::Deadline` bypasses `classify_fate` entirely** (`sql.rs:338` builds `tx_deadline(...)` directly), and the **tombstone** answers later ops on a dead `tx_id` with `TxDeadline{Retryable}` via `TxLookupErr::Tombstoned` (`tx/mod.rs:346`, unit variant) — both are replay licenses that must carry the persisted flag, or the fix has holes the width of a client retry loop.
5. **`in_tx: true` is UNOBSERVABLE by the whole suite** (mutation-confirmed 2026-08-11: `sql.rs:332` flipped to `false`; chaos_fate_it, mysql_chaos_it, sql_exec_it, tx_it, mysql_it, stream_it, sql_e2e_scenarios, ferrod --lib ALL GREEN). Root cause: no test kills the backend LINK during an in-tx statement over the wire path — every in-tx chaos test is timeout/cancel-shaped and exits via `ExecStep::Deadline`/the 57014 override, never via `classify_fate(ConnectionLost, in_tx:true)`. The guard must create exactly that missing event.
6. **`SELECT pg_terminate_backend(pg_backend_pid())` kills the session it runs on** and surfaces as a FATAL-severity error → `is_session_fatal` → `PoolError::ConnectionLost` (`ferro-backend-pg/src/error_map.rs:27-29`) — a deterministic, side-connection-free in-tx link kill for the PG half of the guard. The MySQL half needs a side-connection `KILL <id>` while the statement is PROVABLY in-flight (`information_schema.processlist` poll — a kill landing pre-dispatch is silently ineffective, the false-green race T2's review documented).
7. **`ferro-classify` has the tokenizer this needs already**: `scan::split_top_level_statements` (total, panic-safe), `scan::leading_keyword`, `scan::next_token_after_keyword`, `scan::contains_identifier_ci` (all `pub(crate)`, `scan.rs:296-412`), and `rules::create_is_temp` (`rules.rs:367`) already distinguishes `CREATE TEMPORARY` (which does NOT implicitly commit) from `CREATE` (which does). Do not write a second tokenizer.
8. **`CREATE TEMPORARY TABLE` and `DROP TEMPORARY TABLE` do NOT implicitly commit** (MySQL manual, same page); plain `SET` does not either, but `SET autocommit = 1` and `SET PASSWORD` DO. A hazard list that marks all of `CREATE`/`SET` would cry wolf on the S2-tainted-but-tx-safe shapes; one that misses `ANALYZE`/`FLUSH`/`LOCK TABLES` licenses replay. Hazard 2's directional rule decides every edge: unknown → HAZARD.

**The false Indeterminate (finding 3)**

9. **`PoolError::ConnectionLost` is a unit variant carrying no dispatch-phase information** (`ferro-pool/src/error.rs:12-13`); `ferro-backend-pg/src/query.rs` is prepare-THEN-dispatch in both entries (`run`: `.prepare()` at `:69-71`, `query_raw` at `:106-108`; `run_stream`: `.prepare()` at `:178-180`, `query_raw` at `:214-216`), and a PREPARE-phase loss means Parse/Describe only — the Execute was NEVER sent, a provable did-not-apply. Reproduced live on PG 17 (m0-fate journal): checkout, `pg_terminate_backend(pid)` from a side conn, `co.query("INSERT ...")` → `ConnectionLost`, `count(*) = 0` — and the service classifies it `WriteUnconfirmed{Indeterminate}` because `sent` is pre-built `true` (`sql.rs:552-556` buffered, `sql.rs:660-668` + `:702` stream). §19.3 reads `Retryable`. This is ALSO the mechanism behind M1-S8b's unreproduced pre-HEAD-Indeterminate sighting — the stream OPEN's error path, not a checkout failure (which passes `sent:false` and structurally cannot produce it).
10. **Production `PoolError::ConnectionLost` construction sites, enumerated** (grep at HEAD, tests excluded): `ferro-pool/src/pool.rs:134` (connect failure), `ferro-pool/src/fake.rs:542,563`; `ferro-backend-pg/src/conn.rs:114,162,235`, `ferro-backend-pg/src/error_map.rs:28`; `ferro-backend-mysql/src/conn.rs:195,233,256,317`, `ferro-backend-mysql/src/error_map.rs:66,84`; pattern-match sites: `ferro-pool/src/error.rs:79,105`, `ferro-backend-mysql/src/bind.rs:405,742` (test), `ferrod/src/services/fate.rs:147` + its unit tables, `ferrod/src/services/sql.rs:1178` (`send_err_to_pool_error`'s `LinkLost` arm). A struct-variant change is a COMPILE-FORCED choice at every one of these — that is the point.
11. **The safe default is `dispatched: true`.** A site that cannot attribute the phase must err toward Indeterminate (cry-wolf), never toward Retryable-for-a-possibly-applied write. Only PROVABLY-pre-dispatch sites take `false`: the two `prepare()` map sites in pg `query.rs`, the backends' `connect()` paths, and `pool.rs:134`.

**The wedged-backend class (finding 4)**

12. **`checkout_timeout` wraps ONLY the semaphore acquire** (`ferro-pool/src/pool.rs:104`), while `backend.connect()` at `:123` is a bare `.await` — and the module doc (`pool.rs:2-7`) plus the method doc (`:97-99`) claim the timeout covers "a usable connection in hand". CONFIRMED by execution (worktree): with a connect that parks, `checkout()` is still pending at 1000ms under `checkout_timeout=50ms`; with `max_size=2`, two wedged dials hold both permits FOREVER while a third waiter gets `Err(Timeout)` — zero usable capacity permanently, even after the backend recovers. The in-tree followup `docs/followups/2026-08-10-unbounded-backend-dial.md` documents ~127s OS TCP behavior for the black-hole case.
13. **The reaper's ping is unbounded while holding an owned permit** (`ferro-pool/src/health.rs:123-125`). CONFIRMED by execution: `block_pings()` + `max_size=1` → the reaper parks forever, the sole permit never returns, two successive checkouts 60ms apart both `Err(Timeout)`, and no idle conn is EVER evicted again for the pool's lifetime.
14. **The post-cancel drain is unbounded AND terminal-less on both EXEC paths**: `sql.rs:510-517` (`run_autocommit_exec`'s timeout/cancel arms: `cancel_handle.cancel().await; (&mut query_fut).await` — neither bounded) and `actor.rs:400-401,406-407` (`ExecStep::Deadline`/`Abort`, same shape). Against a wedged backend the handler parks forever holding its checkout permit and NEVER declares a terminal — charter rule 4 broken, not merely a hang. The team's own fix for this exact shape exists at `ferrod/src/pools.rs:108` (`VERSION_DRAIN_BUDGET = 5s`, expiry force-taints); the two production paths never got it.
15. **A drain-budget expiry drops a pinned query future mid-statement.** The `Checkout::query` internal Err-arm fail-safe never ran (the future was dropped, not completed), so nothing tainted the conn — the caller MUST `co.set_tainted(true)` before the conn re-enters the pool, or the next tenant inherits a mid-protocol connection (the S8a Task-12 lesson: under the inlined-future mutation the next tenant demonstrably received a conn still running the previous tenant's statement). The borrow works: NLL ends `query_fut`'s borrow of `co` at its last use (the timeout expression), so `co.set_tainted(true)` after it compiles.
16. **`FakeBackend` has `block_pings`/`block_query`/`block_simple_query`/`arm_fail_connect` but NO `block_connect` and NO cancel-immune query gate** (`ferro-pool/src/fake.rs` surface, verified). The review's worktree tests used a `block_connect()` gate that was never committed; Task 3 adds it (mirroring `block_pings`, `fake.rs:440-455`) and Task 10 adds `wedge_queries()` (a park the `FakeCancelHandle` does NOT release — `block_query`'s existing gate IS released by cancel, so it cannot model a wedged backend).

**Availability (finding 5)**

17. **The codec reserves up to `MAX_FRAME_PAYLOAD` (16 MiB) the instant a header arrives** (`ferrod/src/session/codec.rs:70`: `src.reserve(need - src.len())`), held until the frame completes; the reader loop awaits the next frame with NO bound (`session/mod.rs:410`); `serve` never caps concurrent connections (`serve.rs:67-134`). CONFIRMED by execution: post-handshake partial 16 MiB header + 1 body byte → no reply, no close, still writable after 800ms+400ms. `handshake_timeout` guards only the FIRST frame. Peercred bounds this to local allow-listed uids — it is an availability gap against a buggy-but-alive worker, not a remote vector.
18. **An engine idle-timeout default that is nonzero severs every quiet PHP-FPM worker.** The sync PHP client cannot ping while blocked between requests (there is no background thread), and workers legitimately idle for minutes. `idle_timeout` must default DISABLED; the teeth against the measured attack are the partial-frame deadline (a legit local frame completes in microseconds) + the reserve cap + `max_connections`.
19. **The split `Framed` reader cannot reach its codec after `.split()`** (`session/mod.rs:265-266`), so mid-frame progress must travel a shared handle CREATED with the codec: `FrameCodec` is a unit struct today (`codec.rs:57`) constructed in two places (`session/mod.rs:265`, `serve.rs:142`) — giving it a `Default`-preserving optional progress handle keeps both call sites and the golden-vector tests compiling.
20. **`JoinSet::len()` is the live-session count** already maintained by `serve`'s reap arm (`serve.rs:82-86`); the connection cap needs no new bookkeeping, only a check before spawn — and the reject must be a FRAME, not a silent drop (SPEC G-4 precedent: `deny_connection`, `serve.rs:141-146`).

**SIGTERM (finding 6)**

21. **`Drain` has exactly two non-test consumers, both in `serve`'s accept loop** (grep, confirmed in the resource journal). At `drain_deadline` the `JoinSet` is `abort_all()`ed (`serve.rs:151-161`), so the sessions' own cleanup (`registry.cancel_all()` → `tx_registry.abort_session()` → `drain_supervisors()` → writer drain, `session/mod.rs:528-533`) NEVER RUNS on a restart: no terminals, engine-side tx rollback skipped, the writer droppable mid-frame. §18's systemd story assumes this works.
22. **"Refuse new checkouts, let pins finish" is finer than refusing all requests**: a pinned tx already holds its conn — its statements, savepoints, COMMIT/ROLLBACK must keep working through the drain window; only the two checkout-ACQUIRING entries (autocommit EXEC, BEGIN) may be refused. The refusal decision therefore lives in `services/sql.rs` (where `tx_id` is parsed), not the session reader.
23. **The refusal reuses `POOL_TIMEOUT{Retryable}`** — an existing registry code whose pinned branch says exactly the right thing ("resource momentarily unavailable; retry per your policy"); the client's resilience loop then reconnects against the socket-activated successor. A new `ERR_SHUTTING_DOWN` code is a /proto change — deferred, recorded by Task 13.

**Hygiene (finding 7)**

24. **`loggable_scheme` leaks credentials for `user:pass://...`** (`ferrod/src/config.rs:95-98`): the substring before the FIRST `://` is returned as "the scheme", but a malformed DSN can put credentials there. CONFIRMED: `loggable_scheme("adminuser:s3cretPW://tcp/host") == "adminuser:s3cretPW"`, WARN-logged by `infer_pool_kind` (`config.rs:116-119`). Second occurrence of the S6 leak class in the same path; the existing test (`config.rs:559-578`) covers only no-`://` strings and even ASSERTS the pass-through of `redis` — the fix must flip that expectation, not delete it.
25. **`Checkout::drop` does `idle.lock().unwrap()`** (`pool.rs:973`; same idiom at `pool.rs:114,210` and `health.rs:99,112,132`): a poisoned mutex makes the NEXT drop panic — and a panic during unwind is `std::process::abort()`, taking every worker's connections down. The poison-recovering idiom already exists in-tree (`ferrod/src/pools.rs`, `PoolEntry::lock`): `.lock().unwrap_or_else(std::sync::PoisonError::into_inner)`. The lock only ever guards trivial pop/push, so recovery (not eviction) is correct.

**Cross-cutting compile facts**

26. **`ferro-pool` is `unsafe_code = "forbid"`, edition 2024** — no `std::env` mutation in tests; every gate is the `Arc<tokio::sync::Notify>`-style pattern `fake.rs` already uses.
27. **`Checkout::tx_open()` and `Checkout::set_tainted()` are already public** (`pool.rs:330`, `:319`); `Checkout` does NOT expose the backend `dialect()` — Task 8 adds the one-line accessor (`self.pool.backend.dialect()` is the same expression `apply_classify` already uses at `pool.rs:906`).
28. **`ExecReply` lives in `ferrod/src/tx/mod.rs:151`** (`Completed { result, exec_us }` | `Deadline`); `TxEntry::Tombstoned { owner }` at `tx/mod.rs:232`, `TxRegistry::tombstone(tx_id)` at `:319`, `TxLookupErr::Tombstoned` (unit) at `:223` with match sites at `tx/mod.rs:346` + `actor.rs:919,986,1058` (tests) + the `sql.rs` lookup mapping. Adding fields to these is a bounded, enumerated cascade — Task 8 lists every site.
29. **Do not use `tokio::time::timeout` where a shared deadline is meant**: `tokio::time::timeout_at(deadline, fut)` exists and is what makes "ONE `checkout_timeout` budget for acquire+cleanup+connect" expressible (Task 3). All `ferro-pool` time is `tokio::time::Instant` (already imported, `pool.rs:12`).
30. **`common::exec_server(url: String) -> TestServer`** (`ferrod/tests/common/mod.rs:574`) infers the pool kind from the DSN scheme; `req(sql)` defaults `readonly: true, fetch: 0, tx_id: None` (`:675-686`); `exec_ok`/`exec_err` unwrap the terminal (`:707-719`); `pg_url()`/`mysql_url()` read the env (`:533,546`). `chaos_fate_it.rs:328` has the `begin(client, rid, pool, isolation, readonly) -> u64` helper shape this plan's new test file copies (each `tests/*.rs` is its own crate — helpers cannot be imported across test binaries, only from `common/`).

---

## File Structure

Every file this plan touches, with its one responsibility in this slice. **A file appears under exactly one task per wave** — that is what makes Wave A parallel-safe.

**`engine/crates/ferro-classify`** (Task 2)
- `src/lib.rs` — gains `pub fn implicit_commit_hazard(sql, dialect) -> bool` + its unit corpus. The ONLY public-surface change to this crate.
- `src/rules.rs` — `create_is_temp` stays where it is; the new `mysql_implicit_commit` statement rule lives beside `classify_one_mysql`.

**`engine/crates/ferro-pool`**
- `src/pool.rs` (Task 3; Task 8 adds one accessor after Wave A) — the checkout deadline (acquire+cleanup+connect under ONE `checkout_timeout` budget), the poison-recovering idle locks, `Checkout::dialect()` (Task 8).
- `src/health.rs` (Task 4) — the bounded reaper ping + its poison-recovering locks.
- `src/fake.rs` (Task 3: `block_connect`/`release_connect`/`connects_waiting`; Task 10: `wedge_queries`; Task 9: the `ConnectionLost` field at `:542,563`) — sequential owners, never concurrent.
- `src/error.rs` (Task 9) — `ConnectionLost { dispatched: bool }` + `PoolError::undispatched()`.
- `tests/checkout_bound.rs` (Task 3, Create) — the wedged-dial + capacity-returns + poison-abort proofs.
- `tests/reaper_bound.rs` (Task 4, Create) — the reaper-survives-a-wedged-ping proof.

**`engine/crates/ferro-backend-pg`** (Task 9)
- `src/query.rs` — the two prepare-phase `.undispatched()` marks (`:69-71`, `:178-180`).
- `src/conn.rs` — `connect()`-path `ConnectionLost` sites take `dispatched: false`.
- `src/error_map.rs` — `map()` returns `dispatched: true` (the conservative default).
- `tests/pre_dispatch_fate_it.rs` (Create) — the live PG proof that a pre-dispatch death is `dispatched:false` with the write unapplied.

**`engine/crates/ferro-backend-mysql`** (Task 9)
- `src/conn.rs`, `src/error_map.rs`, `src/bind.rs` — the `ConnectionLost` field cascade; `connect()` sites `false`, everything else `true` unless the prep-phase error is separable (verify at implementation; `true` is the safe fallback).

**`engine/crates/ferrod`**
- `src/services/fate.rs` (Task 7; Task 9 consumes) — `OpContext.tx_writes_persisted`, the RETRYABLE-suppression post-filter, `persisted_tx_payload()`, the `dispatched` consumption (Task 9), the extended unit matrix.
- `src/services/sql.rs` (Task 7: field at every `OpContext` literal; Task 8: the reply plumbing + tombstone mapping; Task 10: `CANCEL_DRAIN_BUDGET` + the bounded autocommit drain; Task 12: the drain refusal in `handle_exec`/`handle_begin` + `make_handler(drain)`) — the busiest file; strictly serial owners.
- `src/tx/actor.rs` (Task 8: the latch + hazard + `ExecReply`/teardown plumbing; Task 10: the bounded Deadline/Abort drains) — serial.
- `src/tx/mod.rs` (Task 8) — `ExecReply`, `TxEntry::Tombstoned`, `TxRegistry::tombstone`, `TxLookupErr::Tombstoned` gain the persisted flag.
- `src/session/codec.rs` (Task 5: the 64 KiB reserve cap; Task 11: the `ReadProgress` counters) — serial.
- `src/session/mod.rs` (Task 11: the tick arm — frame-stall + idle enforcement; Task 12: the drain arm + wind-down) — serial.
- `src/serve.rs` (Task 11: the `max_connections` gate; Task 12: `Drain` into sessions + the grace backstop) — serial.
- `src/config.rs` (Task 6: `loggable_scheme` allow-list; Task 11: the three knobs) — serial.
- `src/session/error.rs` (Task 11) — one constructor for the overloaded-reject frame.
- `src/main.rs` (Task 12) — threads `drain` into `make_handler`.
- `tests/in_tx_fate_it.rs` (Task 1, Create; Task 8 appends) — the finding-2 guard + the finding-1 live acceptance.
- `tests/availability_it.rs` (Task 11, Create) — connection cap + partial-frame stall, offline (no DB needed).
- `tests/drain_it.rs` (Task 12, Create) — the live graceful-drain acceptance.
- `tests/shutdown.rs` (Task 12, Modify) — the accept-refusal tests keep passing with the new signature.

**Docs**
- `docs/followups/2026-08-11-s9a-spec-deltas.md` (every task appends; Task 13 consumes) — the append-only delta ledger.
- `ferro-spec-v0.2.md`, `CLAUDE.md` (Task 13 ONLY).

**Explicitly NOT modified**
- `/proto` — nothing here changes the wire registry (hazards 23, and the persisted cell reuses `WRITE_UNCONFIRMED`).
- `php/*` — no client change; the client already handles every code this plan emits.
- `vendor/*` — no fork change.
- `ferro-pool/src/pin.rs`, `ferro-classify`'s existing `classify` rules, `session/handshake.rs`, `session/supervisor.rs`, `session/responder.rs`, `session/registry.rs` (except the one `in_flight()` accessor if absent — Task 11 checks first) — the measured-sound core stays untouched.

---

## Sequencing — who may run when, and why

| Wave | Tasks | Mode | Reason |
|---|---|---|---|
| A | 1, 2, 3, 4, 5, 6 | **PARALLEL** (up to 6 implementers) | Disjoint files: T1 owns only `ferrod/tests/in_tx_fate_it.rs`; T2 owns `ferro-classify`; T3 owns `pool.rs`+`fake.rs`+its test file; T4 owns `health.rs`+its test file; T5 owns `session/codec.rs`; T6 owns `ferrod/src/config.rs`. |
| B | 7 → 8 → 9 → 10 | **SERIAL, in this order** | All four share `fate.rs`/`sql.rs`/`actor.rs`/`error.rs`/`fake.rs`. Order is load-bearing: 7 defines the `OpContext` field 8 threads; 8 defines the reply/tombstone shapes; 9's `ConnectionLost` cascade rewrites arms 7/8 just stabilized; 10 edits the same select arms 8 touched. T7 must not start until T1 and T2 are merged (T7's behavior change happens under T1's armed guard; T8 consumes T2's function). |
| C | 11 → 12 | **SERIAL**; T11 may start as soon as T5 and T6 are merged (it shares no file with Wave B); T12 requires T10 AND T11 merged | T11/T12 share `serve.rs`+`session/mod.rs`+`config.rs`; T12 additionally edits `services/sql.rs` (Wave B's file). |
| — | 13 | **LAST, single author** | The spec-delta batch; consumes every task's ledger entry. |

---

## Findings coverage map

| Review finding | Task(s) | Acceptance |
|---|---|---|
| (2) `in_tx` unobservable — guard first | 1 | live in-tx link-kill on PG + MySQL; mutation `sql.rs:332` → RED |
| (1) implicit-commit at-least-once — lexical assist | 2 | `implicit_commit_hazard` corpus incl. TEMPORARY/`SET autocommit` edges |
| (1) — the fate cell | 7 | RETRYABLE unmintable when `in_tx && tx_writes_persisted`; whole-matrix totality test |
| (1) — the live state + plumbing | 8 | live: BEGIN→INSERT→DDL→kill ⇒ `WRITE_UNCONFIRMED{Indeterminate}` + row 1 persisted; control without DDL stays `Retryable` |
| (3) false `Indeterminate` pre-dispatch | 9 | live PG: pre-dispatch death ⇒ `CONNECTION_LOST{Retryable}`, write unapplied; closes the §19.3 OPEN sighting |
| (4a) unbounded connect at checkout | 3 | checkout returns ≤ `checkout_timeout`; capacity returns after wedge |
| (4b) unbounded reaper ping | 4 | reaper survives a wedged ping; permit returns; eviction continues |
| (4c) unbounded post-cancel drain, no terminal | 10 | drain bounded by 5s budget; terminal always declared; conn tainted |
| (5) codec 16 MiB reserve / no caps | 5, 11 | reserve ≤ 64 KiB/step; `max_connections`; `frame_read_timeout`; `idle_timeout` knob (default off) |
| (6) SIGTERM drain wired to nothing | 12 | drain reaches sessions/pools/tx actors; new-work refusal; clean wind-down inside deadline |
| (7) `loggable_scheme` + `unwrap()` abort | 6, 3 | allow-list scheme logging; poison-recovering idle locks |
| Spec-truth (charter DoD) | 13 | §19.3/§7/§5.2/§18/§12/§22.2 + CLAUDE.md updated by ONE author |

---
## Task 1: The finding-2 guard — make `in_tx: true` observable BEFORE anything changes it

> **PLAN-VERIFY F2 (MAJOR) — two one-line divergences from the in-tree pattern this task claims to
> copy, both already proven in `mysql_chaos_it.rs`. Apply BOTH before writing the test.**
>
> **(a) The marker must be a STRING LITERAL, not a `/* comment */`.** `mysql_chaos_it.rs:46` records
> — verified live during S6 — that **MariaDB strips comments from `processlist.INFO`**. A comment
> marker works on MySQL 8.4 only, and silently forecloses ever pointing this guard at MariaDB, which
> matters because the blocker being guarded is a MySQL **and MariaDB** family bug. Use the in-tree
> predicate form: `... WHERE '{marker}' <> ''`.
>
> **(b) The processlist poll must filter `COMMAND IN ('Execute','Query')`.** `mysql_chaos_it.rs:188-196`
> records that without it the marker matches the PREPARING-but-not-yet-executing phase (the C14 flake,
> hit live). **This one is a TIME BOMB, not a flake:** at HEAD both phases classify identically so
> this task passes either way — but after **Task 9** introduces `dispatched`, a prep-phase kill
> becomes `dispatched: false` → `CONNECTION_LOST{Retryable}`, which is EXACTLY the value this test
> asserts. The `sql.rs:332` mutation would then survive GREEN and the guard on the defining safety
> property would be dead, with nothing in this task or Task 9 looking wrong in isolation.


The in-transaction half of §19.3 — the branch that decides rollback-and-tombstone versus `Indeterminate` — is effectively untested: mutating `sql.rs:332` `in_tx: true → false` left EIGHT suites green (hazard 5). This task creates the missing event — a backend LINK death during an in-tx statement, over the wire path — on both engine families, and proves by mutation that the new tests are the first ones that can catch it. **No production code changes in this task.** Tasks 7–8 then change the in-tx cell UNDER this armed guard.

**Files:**
- Create: `engine/crates/ferrod/tests/in_tx_fate_it.rs`
- Test: same file (this task is all test)

**Interfaces:**
- Consumes: `common::{TestClient, exec_err, exec_ok, exec_server, mysql_url, pg_url, req}` (`ferrod/tests/common/mod.rs:574,675,690,707,715,533,546`); `ferro_proto::messages::tx::{BeginRequest, BeginResponse}`; `ferro_proto::consts::{branch, errc, flags, method_sql, method_tx, service}`; `mysql_async` (already a `ferrod` dev-dependency via `mysql_chaos_it.rs`).
- Produces: the file itself — Task 8 APPENDS its implicit-commit acceptance tests to it (helpers below are reused there: `begin`, `tx_req`, `write_req`, `unique_key`, `raw_mysql`, `wait_for_active_conn`).

- [ ] **Step 1: Write the test file**

Create `engine/crates/ferrod/tests/in_tx_fate_it.rs`:

```rust
//! M1-S9a Task 1 — the finding-2 guard: the tx-scoped EXEC's `in_tx: true` (`services/sql.rs:332`)
//! becomes OBSERVABLE. The M0 core review mutated that field to `false` and the ENTIRE suite
//! stayed green, because no test anywhere kills the backend LINK during an in-tx statement over
//! the wire path (the in-tx chaos tests are all timeout/cancel-shaped and exit via
//! `ExecStep::Deadline`/the 57014 override, never via `classify_fate(ConnectionLost, in_tx:true)`).
//!
//! These tests create exactly that event on BOTH engine families and pin the §19.3 answer:
//! an in-tx statement link-loss is `CONNECTION_LOST{Retryable}` — the whole transaction is dead
//! and NOTHING persisted (proven by read-back), so replay is safe — and NEVER
//! `WRITE_UNCONFIRMED{Indeterminate}`.
//!
//! Task 8 appends the implicit-commit acceptance here: the one case where an in-tx failure must
//! STOP being Retryable, because earlier statements in the tx HAVE persisted.
//!
//! Every test SKIPS (does not fail) when its `FERRO_TEST_*_URL` is unset — same discipline as
//! `chaos_fate_it.rs`.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::{TestClient, exec_err, exec_ok, exec_server, mysql_url, pg_url, req};
use ferro_proto::consts::{branch, errc, flags, method_sql, method_tx, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::tx::{BeginRequest, BeginResponse};
use ferro_proto::value::Value;

static UNIQUE: AtomicU64 = AtomicU64::new(0);

/// A per-test-run unique string — the counter-row key AND the processlist marker, so concurrent
/// runs on the shared testkit database can never collide or mis-target each other's statements.
fn unique_key(prefix: &str) -> String {
    let n = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    format!("{prefix}_{}_{nanos}_{n}", std::process::id())
}

/// A tx-scoped, write-declared EXEC request. `readonly = false` is load-bearing: under the
/// named mutation (`sql.rs:332` `in_tx: false`) the ConnectionLost classification falls through
/// to the `sent && !readonly && !in_tx` arm and becomes WRITE_UNCONFIRMED — which is exactly what
/// these tests must catch.
fn tx_req(sql: &str, tx_id: u64) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r.tx_id = Some(tx_id);
    r
}

/// An autocommit, write-declared EXEC request (setup DDL / seed rows).
fn write_req(sql: &str) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r
}

/// Open a transaction via the TX service and return its `tx_id` (the `chaos_fate_it.rs:328`
/// helper, copied — `tests/*.rs` are separate crates and cannot import each other's helpers).
async fn begin(
    client: &mut TestClient,
    rid: u32,
    pool: &str,
    isolation: Option<u8>,
    readonly: bool,
) -> u64 {
    let breq = BeginRequest {
        pool: pool.to_string(),
        isolation,
        readonly,
    };
    client
        .send_request(rid, service::TX, method_tx::BEGIN, breq.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(t.header.request_id, rid, "BEGIN terminal echoes the rid");
    assert_eq!(t.header.flags & flags::END, flags::END);
    match Outcome::decode(&t.payload).expect("decode BEGIN Outcome") {
        Outcome::Ok(body) => {
            BeginResponse::decode(&body)
                .expect("decode BeginResponse")
                .tx_id
        }
        other => panic!("BEGIN expected Outcome::Ok(BeginResponse), got {other:?}"),
    }
}

/// A raw side connection to the SAME MySQL/MariaDB, entirely OUTSIDE ferrod's pool — used only to
/// poll `information_schema.processlist` and to `KILL` the pinned connection.
async fn raw_mysql(url: &str) -> mysql_async::Conn {
    mysql_async::Conn::new(mysql_async::Opts::from_url(url).expect("mysql url"))
        .await
        .expect("in_tx_fate harness: raw side connection to MySQL")
}

/// Poll the processlist until a statement whose text carries `marker` is PROVABLY in flight
/// (excluding this poll's own connection — the poll text also carries the marker), then return
/// that connection's processlist id. Panics loudly after 5s: a kill landing before dispatch is
/// silently ineffective, and a test built on it would pass for the wrong reason.
async fn wait_for_active_conn(side: &mut mysql_async::Conn, marker: &str) -> u64 {
    use mysql_async::prelude::Queryable;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let ids: Vec<u64> = side
            .exec(
                "SELECT id FROM information_schema.processlist \
                 WHERE info LIKE :m AND id <> CONNECTION_ID()",
                mysql_async::params! /* PLAN-VERIFY F1: needs `use mysql_async::params;` at module scope — the macro is unhygienic and `mysql_async::params!{..}` does NOT resolve. Prefer the proven in-tree POSITIONAL form (mysql_chaos_it.rs:193): `("%marker%".to_string(),)` with `?`. MEASURED: `cargo test -p ferrod --test in_tx_fate_it --no-run` fails at HEAD with "cannot find macro `params` in this scope". */  { "m" => format!("%{marker}%") },
            )
            .await
            .expect("processlist poll");
        if let Some(id) = ids.into_iter().next() {
            return id;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "statement carrying marker {marker:?} never became visible in the processlist"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

/// PG half of the guard. `SELECT pg_terminate_backend(pg_backend_pid())` kills the session it
/// runs on: the statement is DISPATCHED (it executes), the backend dies mid-answer, tokio-postgres
/// surfaces a FATAL-severity error, `is_session_fatal` maps it to `PoolError::ConnectionLost`, and
/// the tx-scoped Err arm classifies it with `in_tx: true` — the branch this file exists to pin.
#[tokio::test]
async fn pg_in_tx_link_loss_is_connection_lost_retryable_never_indeterminate() {
    let Some(url) = pg_url() else {
        eprintln!("skip: FERRO_TEST_PG_URL not set");
        return;
    };
    let server = exec_server(url);
    let mut c = server.connect().await;
    c.hello(1).await;

    exec_ok(
        &mut c,
        2,
        &write_req("CREATE TABLE IF NOT EXISTS ferro_s9a_intx (k text PRIMARY KEY, n int NOT NULL)"),
    )
    .await;
    let key = unique_key("pg_intx");

    let tx_id = begin(&mut c, 3, "default", None, false).await;
    exec_ok(
        &mut c,
        4,
        &tx_req(&format!("INSERT INTO ferro_s9a_intx VALUES ('{key}', 1)"), tx_id),
    )
    .await;

    let ep = exec_err(
        &mut c,
        5,
        &tx_req("SELECT pg_terminate_backend(pg_backend_pid())", tx_id),
    )
    .await;

    assert_eq!(
        ep.code,
        errc::CONNECTION_LOST,
        "an in-tx statement link-loss is CONNECTION_LOST (the whole tx is dead, §19.3), \
         got {:#06x}: {}",
        ep.code,
        ep.message
    );
    assert_eq!(ep.branch, branch::RETRYABLE);
    assert_ne!(
        ep.code,
        errc::WRITE_UNCONFIRMED,
        "an in-tx statement loss with NOTHING persisted must never be reported Indeterminate"
    );

    // Retryable is HONEST: the INSERT ran inside the killed transaction, so nothing persisted.
    // Read back over a FRESH autocommit checkout (the dead conn is evicted at that checkout).
    let ok = exec_ok(
        &mut c,
        6,
        &req(&format!(
            "SELECT count(*)::int8 FROM ferro_s9a_intx WHERE k = '{key}'"
        )),
    )
    .await;
    assert_eq!(
        ok.rows[0][0],
        Value::I64(0),
        "the in-tx INSERT must have died with its transaction — Retryable licenses replay, so \
         this MUST be 0"
    );
}

/// MySQL half: the same event via a side-connection `KILL <processlist id>` fired while the
/// tx-scoped statement is PROVABLY in flight (`SELECT SLEEP(5)` + processlist marker).
#[tokio::test]
async fn mysql_in_tx_link_loss_is_connection_lost_retryable_never_indeterminate() {
    let Some(url) = mysql_url() else {
        eprintln!("skip: FERRO_TEST_MYSQL_URL not set");
        return;
    };
    let mut side = raw_mysql(&url).await;
    let server = exec_server(url);
    let mut c = server.connect().await;
    c.hello(1).await;

    exec_ok(
        &mut c,
        2,
        &write_req(
            "CREATE TABLE IF NOT EXISTS ferro_s9a_intx (k VARCHAR(128) PRIMARY KEY, n INT NOT NULL)",
        ),
    )
    .await;
    let key = unique_key("my_intx");

    let tx_id = begin(&mut c, 3, "default", None, false).await;
    exec_ok(
        &mut c,
        4,
        &tx_req(&format!("INSERT INTO ferro_s9a_intx VALUES ('{key}', 1)"), tx_id),
    )
    .await;

    // Dispatch the victim statement WITHOUT awaiting its terminal, prove it in flight, kill it.
    let marker = unique_key("my_intx_kill");
    let victim = tx_req(&format!("SELECT SLEEP(5) /* {marker} */"), tx_id);
    c.send_request(5, service::SQL, method_sql::EXEC, victim.encode())
        .await;
    let id = wait_for_active_conn(&mut side, &marker).await;
    {
        use mysql_async::prelude::Queryable;
        side.query_drop(format!("KILL {id}")).await.expect("KILL");
    }

    let t = c.recv().await;
    assert_eq!(t.header.request_id, 5);
    assert_eq!(t.header.flags & flags::END, flags::END);
    let ep = match Outcome::decode(&t.payload).expect("decode Outcome") {
        Outcome::Error(ep) => ep,
        other => panic!("expected Outcome::Error, got {other:?}"),
    };

    assert_eq!(
        ep.code,
        errc::CONNECTION_LOST,
        "an in-tx statement link-loss is CONNECTION_LOST{{Retryable}}, got {:#06x}: {}",
        ep.code,
        ep.message
    );
    assert_eq!(ep.branch, branch::RETRYABLE);
    assert_ne!(ep.code, errc::WRITE_UNCONFIRMED);

    let ok = exec_ok(
        &mut c,
        6,
        &req(&format!(
            "SELECT CAST(count(*) AS SIGNED) FROM ferro_s9a_intx WHERE k = '{key}'"
        )),
    )
    .await;
    assert_eq!(
        ok.rows[0][0],
        Value::I64(0),
        "InnoDB rolls the killed connection's open transaction back — nothing may persist"
    );
}
```

- [ ] **Step 2: Run — both tests must PASS against HEAD**

```bash
cd /home/abdullak/projects/ferro && \
FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
cargo test -p ferrod --test in_tx_fate_it -- --nocapture
```

Expected: `test result: ok. 2 passed`. (If the MySQL test's `Value::I64` assert fails on the CAST shape, check the terminal's actual tag with `--nocapture` — the count read-back must compare an `I64`, per the §9.1 type policy.) Also run OFFLINE (`cargo test -p ferrod --test in_tx_fate_it` with no env) and confirm both print `skip: ...` and pass — the no-skip gate greps for exactly that.

- [ ] **Step 3: THE NAMED MUTATION — prove these are the first tests that can catch the field**

Edit `engine/crates/ferrod/src/services/sql.rs:332`: `in_tx: true,` → `in_tx: false,` (the tx-scoped EXEC Err arm). Re-run Step 2's command.

Expected: BOTH tests FAIL with `assertion ... left: 0x1005` -shaped output — the terminal arrives as `WRITE_UNCONFIRMED`/`INDETERMINATE` instead of `CONNECTION_LOST`/`RETRYABLE`. This is the exact mutation the M0 review ran against the whole suite and got green; record the RED output in the commit message. **Restore the line** and re-run to green.

- [ ] **Step 4: Regression proof for the sound list**

This task adds tests only. Run the neighbouring live suites once to prove the new file's setup DDL doesn't collide with them: `cargo test -p ferrod --test chaos_fate_it --test mysql_chaos_it` (with the env vars). Expected: green.

- [ ] **Step 5: Append the delta-ledger entry and commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md` (create the file with this entry if absent):

```markdown
### Task 1
- §19.3: no text change; record in §22.2 that the in-tx ConnectionLost cell is now live-guarded on
  both engine families (`in_tx_fate_it.rs`), closing the "guard that cannot fail" finding on the
  defining safety property.
```

```bash
git add engine/crates/ferrod/tests/in_tx_fate_it.rs docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "test(m1-s9a): make the tx-scoped in_tx fate observable — in-tx link-kill on PG + MySQL

Mutation-proven: sql.rs:332 in_tx:true->false turns both tests RED
(WRITE_UNCONFIRMED/INDETERMINATE instead of CONNECTION_LOST/RETRYABLE);
the whole pre-existing suite stays green under the same mutation."
```

---

## Task 2: `ferro_classify::implicit_commit_hazard` — the pre-dispatch lexical assist

> **PLAN-VERIFY: leading `COMMIT` is correctly NOT a hazard, and here is WHY — do not "fix" this.**
> The suspicion was that a raw `COMMIT` through tx-scoped EXEC is a loss PAST the commit point, so
> omitting it would leave a hole in the very blocker this slice closes. It is structurally
> unreachable: the TX actor's Exec runs `co.query(..)` (`tx/actor.rs:344`), the GUARDED entry;
> `Checkout::query` calls `guard_tx_control` (`ferro-pool/src/pool.rs:680`); `COMMIT` is in
> `BOUNDARY_SINGLE` (`pin.rs:107`) → `TxControlClass::Boundary` → rejected `Unsupported` ("use the TX
> service instead"), and that classification is pinned by tests at `pin.rs:373`, `:392` and `:559`
> including comment-prefixed forms. A `COMMIT` never reaches the wire through EXEC, so marking it a
> hazard would only make every guarded refusal masquerade as one.


A statement LOST mid-flight emits no OK packet and no tracker, and MySQL's implicit commit fires **before** the statement executes (hazard 2b) — so for the loss-of-the-DDL-itself case the ONLY available signal is lexical and pre-dispatch. This function is assist-not-authority in exactly the S2 pattern: it can only make a later loss-classification MORE conservative (Retryable → Indeterminate), never less, and the protocol latch (Task 8) corrects any false positive the moment the statement COMPLETES.

**Files:**
- Modify: `engine/crates/ferro-classify/src/lib.rs` (the public fn + test corpus)
- Modify: `engine/crates/ferro-classify/src/rules.rs` (the per-statement MySQL rule, beside `classify_one_mysql`)
- Test: `lib.rs` `#[cfg(test)]` (same file)

**Interfaces:**
- Consumes: `scan::split_top_level_statements`, `scan::leading_keyword`, `scan::next_token_after_keyword`, `scan::contains_identifier_ci` (all `pub(crate)`, `scan.rs:296-412` — total, panic-safe, uppercasing); `rules::create_is_temp` (`rules.rs:367`, private to `rules.rs` — the new rule lives in the same module).
- Produces: `pub fn implicit_commit_hazard(sql: &str, dialect: Dialect) -> bool` — consumed by Task 8 (`tx/actor.rs`) as `ferro_classify::implicit_commit_hazard(&sql, co.dialect())`.

- [ ] **Step 1: Write the failing test corpus**

Append to `engine/crates/ferro-classify/src/lib.rs`'s `mod tests`:

```rust
// ---- M1-S9a: implicit_commit_hazard (the finding-1 pre-dispatch assist) ---------------------

mod implicit_commit {
    use super::*;

    fn hz(sql: &str) -> bool {
        implicit_commit_hazard(sql, Dialect::MySql)
    }

    /// The documented implicit-commit families (MySQL manual, "Statements That Cause an
    /// Implicit Commit"), plus CALL/DO (a routine may run DDL — the S6 unconditional-CALL-pin
    /// precedent). Losing any of these mid-flight may already be past the commit point.
    #[test]
    fn implicit_commit_families_are_hazards() {
        for sql in [
            "CREATE TABLE t (x INT)",
            "ALTER TABLE t ADD COLUMN y INT",
            "DROP TABLE t",
            "RENAME TABLE t TO u",
            "TRUNCATE TABLE t",
            "GRANT SELECT ON *.* TO 'a'@'%'",
            "REVOKE SELECT ON *.* FROM 'a'@'%'",
            "ANALYZE TABLE t",
            "OPTIMIZE TABLE t",
            "FLUSH TABLES",
            "LOCK TABLES t WRITE",
            "UNLOCK TABLES",
            "START SLAVE",
            "XA START 'x'",
            "CALL migrate()",
            "DO migrate()",
            "SET autocommit = 1",
            "SET @@autocommit = 1",
            "SET PASSWORD FOR 'a'@'%' = 'b'",
        ] {
            assert!(hz(sql), "{sql:?} must be an implicit-commit hazard");
        }
    }

    /// Plain DML, reads, diagnostics and tx-internal verbs never implicitly commit — these must
    /// stay Retryable-eligible, or the hazard swallows the useful §19.3 in-tx branch whole.
    #[test]
    fn dml_and_tx_internal_verbs_are_not_hazards() {
        for sql in [
            "SELECT 1",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 1",
            "DELETE FROM t",
            "REPLACE INTO t VALUES (1)",
            "WITH c AS (SELECT 1) SELECT * FROM c",
            "SAVEPOINT s1",
            "ROLLBACK TO SAVEPOINT s1",
            "RELEASE SAVEPOINT s1",
            "SHOW TABLES",
            "EXPLAIN SELECT 1",
            "USE ferro",
        ] {
            assert!(!hz(sql), "{sql:?} must NOT be a hazard");
        }
    }

    /// The two edges the manual carves out: TEMPORARY objects do not commit; their persistent
    /// twins do. A hazard that marks CREATE TEMPORARY cries wolf on every S2 temp-table shape.
    #[test]
    fn temporary_objects_do_not_commit_but_persistent_twins_do() {
        assert!(!hz("CREATE TEMPORARY TABLE t (x INT)"));
        assert!(!hz("create temporary table t (x int)"));
        assert!(!hz("DROP TEMPORARY TABLE t"));
        assert!(hz("CREATE TABLE t (x INT)"));
        assert!(hz("DROP TABLE t"));
    }

    /// Plain SET is tx-safe (it may TAINT via the S2 lexer, but it does not commit); the two
    /// committing SET shapes are autocommit assignment and SET PASSWORD.
    #[test]
    fn plain_set_is_safe_the_two_committing_sets_are_not() {
        assert!(!hz("SET SESSION sql_mode = ''"));
        assert!(!hz("SET @user_var = 1"));
        assert!(hz("SET autocommit = 0"));
        assert!(hz("SET PASSWORD = 'x'"));
    }

    /// The §19.3 directional default: an UNKNOWN leading keyword is a hazard. A false hazard
    /// costs one cry-wolf Indeterminate iff that statement is then LOST mid-flight; a missed
    /// hazard licenses replay of persisted writes (at-least-once).
    #[test]
    fn unknown_leading_keyword_defaults_to_hazard() {
        assert!(hz("FROBNICATE THE THING"));
    }

    /// Multi-statement input: any hazardous top-level statement makes the whole text hazardous
    /// (mirrors `classify`'s split semantics); comment-only/empty input dispatches nothing.
    #[test]
    fn multi_statement_any_hazard_wins_and_empty_is_safe() {
        assert!(hz("INSERT INTO t VALUES (1); CREATE TABLE u (x INT)"));
        assert!(!hz("INSERT INTO t VALUES (1); UPDATE t SET x = 2"));
        assert!(!hz("/* nothing */"));
        assert!(!hz(""));
    }

    /// PostgreSQL DDL is transactional and SQLite has no implicit-commit class of this shape:
    /// the hazard is DIALECT-SCOPED and must never fire there (PG behavior stays byte-identical).
    #[test]
    fn postgres_and_sqlite_never_hazard() {
        for d in [Dialect::Postgres, Dialect::Sqlite] {
            assert!(!implicit_commit_hazard("CREATE TABLE t (x int)", d));
            assert!(!implicit_commit_hazard("LOCK TABLES t WRITE", d));
            assert!(!implicit_commit_hazard("FROBNICATE", d));
        }
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ferro-classify implicit_commit`
Expected: COMPILE ERROR — `cannot find function implicit_commit_hazard in this scope`.

- [ ] **Step 3: Implement**

In `engine/crates/ferro-classify/src/lib.rs`, below `classify`:

```rust
/// M1-S9a (finding 1): the PRE-dispatch implicit-commit hazard — a dialect-scoped lexical ASSIST,
/// never an authority (SPEC §7.1's pattern, same species as [`classify`]).
///
/// On MySQL/MariaDB an implicit-commit statement (DDL, `LOCK TABLES`, `SET autocommit`, …) inside
/// an explicit transaction COMMITS everything before it — and the commit fires BEFORE the
/// statement executes, so a statement LOST mid-flight may already be past the commit point with
/// NO protocol signal ever arriving (no OK packet, no tracker). The tx actor therefore consults
/// this BEFORE dispatching each in-tx statement: if the statement is hazard-shaped and is then
/// interrupted/lost, the loss is classified as if the transaction's earlier writes persisted
/// (`OpContext.tx_writes_persisted`) — the branch that never licenses replay.
///
/// Directional by design (charter rule 5 / §19.3): a FALSE hazard costs one conservative
/// `Indeterminate` iff that statement is then lost mid-flight (the protocol latch corrects it on
/// completion); a MISSED hazard licenses replay of persisted writes. So the unknown leading
/// keyword defaults to HAZARD, and only the documented never-committing families are exempt.
/// PostgreSQL DDL is transactional and SQLite has no implicit commit — both dialects are
/// unconditionally `false`, so PG behavior is byte-identical.
///
/// TOTAL: never panics on any input (inherits `scan.rs`'s panic-safety).
pub fn implicit_commit_hazard(sql: &str, dialect: Dialect) -> bool {
    match dialect {
        Dialect::Postgres | Dialect::Sqlite => false,
        Dialect::MySql => scan::split_top_level_statements(sql)
            .into_iter()
            .any(rules::mysql_implicit_commit_hazard),
    }
}
```

In `engine/crates/ferro-classify/src/rules.rs`, beside `classify_one_mysql`:

```rust
/// One top-level MySQL statement: does it CAUSE AN IMPLICIT COMMIT? (MySQL manual, "Statements
/// That Cause an Implicit Commit".) See `lib.rs::implicit_commit_hazard` for the directional
/// rationale — this list enumerates the SAFE side; everything else, including an unknown leading
/// keyword, is a hazard.
pub(crate) fn mysql_implicit_commit_hazard(stmt: &str) -> bool {
    let Some(kw) = scan::leading_keyword(stmt) else {
        // Empty/comment-only: nothing dispatchable, nothing can have committed.
        return false;
    };
    match kw.as_str() {
        // Never implicitly commit: plain DML, reads, diagnostics, tx-internal verbs, and the
        // server-side prepared-statement verbs (they may TAINT via classify_one_mysql — that is
        // a different, orthogonal question).
        "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "WITH" | "TABLE" | "VALUES"
        | "SHOW" | "EXPLAIN" | "DESCRIBE" | "DESC" | "USE" | "HANDLER" | "SAVEPOINT"
        | "RELEASE" | "ROLLBACK" | "COMMIT" | "PREPARE" | "EXECUTE" | "DEALLOCATE" => false,
        // CREATE/DROP TEMPORARY do NOT commit; every other CREATE/DROP does.
        "CREATE" => !create_is_temp(stmt),
        "DROP" => scan::next_token_after_keyword(stmt).as_deref() != Some("TEMPORARY"),
        // Plain SET never commits; SET PASSWORD and any autocommit assignment DO.
        "SET" => {
            scan::next_token_after_keyword(stmt).as_deref() == Some("PASSWORD")
                || scan::contains_identifier_ci(stmt, "autocommit")
        }
        // The documented committing families (ALTER/RENAME/TRUNCATE/GRANT/REVOKE/ANALYZE/CHECK/
        // FLUSH/OPTIMIZE/REPAIR/RESET/CACHE/LOAD/INSTALL/UNINSTALL/LOCK/UNLOCK/START/BEGIN/XA/
        // CHANGE/STOP/PURGE), CALL/DO (a routine may run DDL), and every UNKNOWN keyword.
        _ => true,
    }
}
```

(If `create_is_temp`'s signature disagrees — it is `fn create_is_temp(stmt: &str) -> bool` at `rules.rs:367` — adapt the call, not the rule.)

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p ferro-classify`
Expected: all tests pass, including the pre-existing `classify` corpus untouched (this task must not edit any existing rule or `SAFE_LEADING_KEYWORDS` — the regression proof for the S2 assist is that its whole suite is untouched and green).

- [ ] **Step 5: NAMED MUTATIONS**

1. Flip the final arm `_ => true` to `_ => false` → `unknown_leading_keyword_defaults_to_hazard` AND most of `implicit_commit_families_are_hazards` go RED (the families reach the `_` arm). Restore.
2. Change `"CREATE" => !create_is_temp(stmt)` to `"CREATE" => true` → `temporary_objects_do_not_commit_but_persistent_twins_do` goes RED. Restore.
3. In `implicit_commit_hazard`, change `Dialect::Postgres | Dialect::Sqlite => false` to fall through to the MySQL body → `postgres_and_sqlite_never_hazard` goes RED. Restore.

- [ ] **Step 6: Delta ledger + commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md`:

```markdown
### Task 2
- §7.1: add the implicit-commit hazard as a SECOND assist signal (pre-dispatch, MySQL-dialect
  only, unknown→hazard), same assist-not-authority contract as the S2 lexer.
```

```bash
git add engine/crates/ferro-classify docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "feat(m1-s9a): implicit_commit_hazard — the pre-dispatch MySQL assist for the finding-1 fix"
```

---

## Task 3: `checkout_timeout` covers the WHOLE checkout — and the idle-mutex stops being an abort

`checkout_timeout` wraps only the semaphore acquire (`pool.rs:104`); the fresh dial at `:123` is a bare `.await`, so `max_size` wedged dials take the pool to zero usable capacity PERMANENTLY (hazard 12 — confirmed by execution). The module and method docs already promise the correct behavior; this task makes the code match them with ONE deadline. It also converts every idle-mutex `unwrap()` to the poison-recovering idiom (`hazard 25`) — same file, same owner.

**Files:**
- Modify: `engine/crates/ferro-pool/src/pool.rs` (`checkout` at `:100-201`; locks at `:114`, `:210`, `:973`; a `#[doc(hidden)]` poison hook beside `poison_idle_for_test` at `:209`)
- Modify: `engine/crates/ferro-pool/src/fake.rs` (add `block_connect`/`release_connect`/`connects_waiting`, mirroring the `block_pings` trio at `:440-455` + the park in `ping`)
- Create: `engine/crates/ferro-pool/tests/checkout_bound.rs`

**Interfaces:**
- Consumes: `PoolConfig { max_size, checkout_timeout, .. }` (`ferro-pool/src/config.rs:13-31`, `Default`: `max_size: 8`, `checkout_timeout: 5s`); `tokio::time::timeout_at`; `std::sync::PoisonError`.
- Produces: `FakeBackend::block_connect(&self)`, `release_connect(&self)`, `connects_waiting(&self) -> u64`; `Pool::poison_idle_mutex_for_test(&self)` (`#[doc(hidden)]`). Checkout semantics: `Err(PoolError::Timeout)` when the WHOLE checkout (acquire + recycle cleanup + dial) exceeds `checkout_timeout`. Task 9 later rewrites this file's `PoolError::ConnectionLost` literal — leave it as-is here.

- [ ] **Step 1: Write the failing tests**

Create `engine/crates/ferro-pool/tests/checkout_bound.rs`:

```rust
//! M1-S9a Task 3 — `checkout_timeout` bounds the WHOLE checkout (finding 4a), and a poisoned
//! idle mutex no longer turns `Checkout::drop` into a process abort (finding 7b).
//!
//! The wedged-dial shape was CONFIRMED by the M0 review: a backend that accepts TCP but never
//! completes the startup handshake parked `checkout()` forever while holding a permit; `max_size`
//! such callers took the pool to zero usable capacity permanently. Every await here is wrapped in
//! an outer 600s bound so a regression fails LOUDLY instead of hanging the suite (under
//! `start_paused`, auto-advance makes the outer bound fire instantly when nothing else can run).

use std::time::Duration;

use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pool::Pool;

fn cfg(max_size: usize, checkout_ms: u64) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_millis(checkout_ms),
        ..PoolConfig::default()
    }
}

async fn bounded<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(600), fut)
        .await
        .expect("BOUND EXCEEDED: the operation under test is unbounded (the finding-4a hang)")
}

#[tokio::test(start_paused = true)]
async fn a_wedged_dial_is_bounded_by_checkout_timeout() {
    let backend = FakeBackend::new();
    backend.block_connect();
    let pool = Pool::new(backend, cfg(2, 50));

    let r = bounded(pool.checkout()).await;
    assert!(
        matches!(r, Err(PoolError::Timeout)),
        "a checkout whose dial wedges must resolve Err(Timeout) within checkout_timeout, got {r:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn capacity_returns_after_wedged_dials_time_out() {
    let backend = FakeBackend::new();
    backend.block_connect();
    let pool = Pool::new(backend, cfg(2, 50));

    // Both permits' worth of checkouts wedge in the dial and time out.
    let (r1, r2) = bounded(async { tokio::join!(pool.checkout(), pool.checkout()) }).await;
    assert!(matches!(r1, Err(PoolError::Timeout)), "got {r1:?}");
    assert!(matches!(r2, Err(PoolError::Timeout)), "got {r2:?}");

    // The permits were RELEASED on the Err returns (the wedged futures were dropped). Once the
    // backend recovers, the pool serves again — the review's confirmed shape was that it never
    // did, even after recovery.
    pool.backend().release_connect();
    let co = bounded(pool.checkout())
        .await
        .expect("the pool must not be permanently wedged after the backend recovers");
    drop(co);
}

/// Finding 7b: a poisoned idle mutex must not abort the daemon. With the poison-recovering lock,
/// `Checkout::drop` (and the next `checkout()`) proceed; with the old `.unwrap()`, the drop
/// panics — and in production that panic during another unwind is `std::process::abort()`.
#[tokio::test]
async fn a_poisoned_idle_mutex_does_not_panic_drop_or_checkout() {
    let pool = Pool::new(FakeBackend::new(), cfg(2, 50));
    let co = pool.checkout().await.expect("checkout before poisoning");

    pool.poison_idle_mutex_for_test();

    let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(co)));
    assert!(
        dropped.is_ok(),
        "Checkout::drop must recover a poisoned idle mutex, not panic (abort in production)"
    );

    // And the pool still works end to end.
    let co2 = pool.checkout().await.expect("checkout after poisoning");
    drop(co2);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ferro-pool --test checkout_bound`
Expected: COMPILE ERROR first (`block_connect`/`poison_idle_mutex_for_test` do not exist). After Step 3's fake/hook additions but BEFORE the checkout change, the first two tests FAIL with `BOUND EXCEEDED: the operation under test is unbounded` and the poison test FAILS with the drop panic.

- [ ] **Step 3: Implement**

**(a) `fake.rs` — the connect gate**, mirroring the ping gate. Fields on `FakeBackend` (beside `ping_gate`/`pings_waiting`):

```rust
    /// When `Some`, every `connect()` call parks on this `Notify` until `release_connect()` —
    /// models a backend that accepts TCP but never completes the startup handshake (M1-S9a
    /// finding 4a). NOTE: if the parked future is DROPPED (a caller-side timeout), the
    /// `connects_waiting` decrement is skipped — tests must compare against a captured baseline,
    /// never assert an absolute count.
    connect_gate: Mutex<Option<Arc<Notify>>>,
    /// Number of `connect()` calls currently (or last observed) parked on `connect_gate`.
    connects_waiting: AtomicU64,
```

Initialize both in `new()` (`connect_gate: Mutex::new(None), connects_waiting: AtomicU64::new(0)` — `Default` delegates to `new()`, so nothing else changes). Methods, verbatim shape of `block_pings`/`release_pings`/`pings_waiting`:

```rust
    /// Arms every subsequent `connect()` call to park until `release_connect()` — the
    /// wedged-dial model for the finding-4a checkout bound.
    pub fn block_connect(&self) {
        *self.connect_gate.lock().unwrap() = Some(Arc::new(Notify::new()));
    }

    /// Releases every parked `connect()` and clears the gate for future calls.
    pub fn release_connect(&self) {
        if let Some(notify) = self.connect_gate.lock().unwrap().take() {
            notify.notify_waiters();
        }
    }

    /// Number of `connect()` calls currently parked on the gate (see the field note on drops).
    pub fn connects_waiting(&self) -> u64 {
        self.connects_waiting.load(Ordering::SeqCst)
    }
```

And in `PoolBackend::connect` for `FakeBackend`, AFTER the `fail_connect_remaining` check and BEFORE constructing the `FakeConn`:

```rust
        // Test-only gate (see `block_connect`/`release_connect`): if armed, park until released —
        // mirrors the ping gate at the top of `ping()`.
        let gate = self.connect_gate.lock().unwrap().clone();
        if let Some(notify) = gate {
            self.connects_waiting.fetch_add(1, Ordering::SeqCst);
            notify.notified().await;
            self.connects_waiting.fetch_sub(1, Ordering::SeqCst);
        }
```

**(b) `pool.rs` — the one-deadline checkout.** Replace the body of `checkout()` (`:100-201`):

```rust
    /// Checks out a connection. **`checkout_timeout` bounds the WHOLE checkout** (M1-S9a,
    /// finding 4a): the permit acquire, any recycle cleanup on popped idle conns, and a fresh
    /// dial all run under ONE deadline — the guarantee the module doc has always claimed.
    /// `queue_us` on the returned `Checkout` covers the whole wait.
    ///
    /// On deadline: `Err(PoolError::Timeout)`. A dial that outlives the budget is DROPPED (the
    /// permit releases on return, so capacity comes back even against a black-holed backend); a
    /// recycle cleanup that outlives it EVICTS the conn (never hand out a conn whose cleanup did
    /// not complete — the spurious-eviction cost when the CALLER's budget ran out is one
    /// reconnect, the safe direction, charter rule 5).
    pub async fn checkout(&self) -> Result<Checkout<B>, PoolError> {
        let start = Instant::now();
        let deadline = start + self.inner.config.checkout_timeout;

        let acquire = Arc::clone(&self.inner.semaphore).acquire_owned();
        let permit = match tokio::time::timeout_at(deadline, acquire).await {
            Ok(Ok(permit)) => permit,
            // The semaphore is never explicitly closed in M0; treat it as a (non-retryable) pool
            // shutdown rather than panicking.
            Ok(Err(_)) => return Err(PoolError::Closed),
            Err(_) => return Err(PoolError::Timeout),
        };

        loop {
            // Budget exhausted while recycling: answer Timeout rather than spin-evicting healthy
            // conns with an already-expired per-conn cleanup bound.
            if Instant::now() >= deadline {
                return Err(PoolError::Timeout);
            }

            let popped = {
                let mut idle = self
                    .inner
                    .idle
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                idle.pop()
            };

            let Some(mut idle_conn) = popped else {
                // No idle connection: connect fresh, WITHIN the remaining budget. A connect
                // failure surfaces immediately (v2/M5) — no hidden retry loop (charter rule 3);
                // `permit` drops on every return, releasing capacity.
                return match tokio::time::timeout_at(deadline, self.inner.backend.connect()).await
                {
                    Ok(Ok(conn)) => {
                        let queue_us = start.elapsed().as_micros() as u64;
                        Ok(Checkout::new(
                            conn,
                            Instant::now(),
                            permit,
                            Arc::clone(&self.inner),
                            queue_us,
                        ))
                    }
                    Ok(Err(_)) => Err(PoolError::ConnectionLost),
                    // The dial outlived the caller's whole budget: drop the wedged future and
                    // answer Timeout. THIS is the finding-4a fix — the permit releases here.
                    Err(_) => Err(PoolError::Timeout),
                };
            };

            // (eviction check unchanged: too_old / is_closed → continue)
            let too_old = idle_conn.created_at.elapsed() > self.inner.config.max_lifetime;
            if too_old || self.inner.backend.is_closed(&idle_conn.conn) {
                continue;
            }

            if idle_conn.tx_open
                || idle_conn.tainted
                || self.inner.backend.clean_reset_profile().is_some()
            {
                let cleanup = async {
                    // (body byte-identical to HEAD: ROLLBACK if tx_open, then the profile reset)
                    if idle_conn.tx_open {
                        self.inner
                            .backend
                            .simple_query(&mut idle_conn.conn, "ROLLBACK")
                            .await?;
                        idle_conn.tx_open = false;
                    }
                    let profile = if idle_conn.tainted {
                        Some(ResetProfile::Full)
                    } else {
                        self.inner.backend.clean_reset_profile()
                    };
                    if let Some(p) = profile {
                        self.inner.backend.reset(&mut idle_conn.conn, p).await?;
                        idle_conn.tainted = false;
                    }
                    Ok::<(), PoolError>(())
                };
                // Bounded by the SAME deadline (was: a fresh full `checkout_timeout` per conn,
                // which let total checkout latency exceed the knob by max_size×budget). Expiry
                // EVICTS — a wedged conn must never be handed out or pushed back.
                match tokio::time::timeout_at(deadline, cleanup).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => continue,
                    Err(_) => continue,
                }
            }

            let queue_us = start.elapsed().as_micros() as u64;
            return Ok(Checkout::new(
                idle_conn.conn,
                idle_conn.created_at,
                permit,
                Arc::clone(&self.inner),
                queue_us,
            ));
        }
    }
```

Update the module doc (`pool.rs:2-7`) sentence to: "Checkout is semaphore-bounded (`max_size`) and **the whole of it — permit wait, recycle cleanup, fresh dial — runs under ONE `checkout_timeout` deadline** (M1-S9a)."

**(c) the poison-recovering locks + hook.** At `pool.rs:210` (`poison_idle_for_test`) and `:973` (`Checkout::drop`), replace `.lock().unwrap()` with `.lock().unwrap_or_else(std::sync::PoisonError::into_inner)` (the `ferrod/src/pools.rs` `PoolEntry::lock` idiom — the mutex only guards trivial pop/push, so recovery is correct). Beside `poison_idle_for_test`:

```rust
    /// Test-support hook: POISONS the idle-stack mutex (panics on a scratch thread while holding
    /// it), so tests can prove the pool's lock sites recover instead of double-panicking into a
    /// process abort (M1-S9a finding 7b). Not intended for production callers.
    #[doc(hidden)]
    pub fn poison_idle_mutex_for_test(&self) {
        let inner = Arc::clone(&self.inner);
        let _ = std::thread::spawn(move || {
            let _guard = inner
                .idle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            panic!("deliberate poison (test hook)");
        })
        .join();
    }
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p ferro-pool` — the new file green AND the whole pre-existing pool suite green (the recycle-order, pin, reaper and hygiene tests are the regression proof that the deadline rewrite did not change eviction semantics: the sound-list items "pin authority + Err-arm fail-safes" live in this crate's tests).

- [ ] **Step 5: NAMED MUTATIONS**

1. Remove the `timeout_at` around `self.inner.backend.connect()` (restore the bare `.await`) → `a_wedged_dial_is_bounded_by_checkout_timeout` AND `capacity_returns_after_wedged_dials_time_out` go RED via the 600s `BOUND EXCEEDED` expect. Restore.
2. In `Checkout::drop`, restore `.lock().unwrap()` → `a_poisoned_idle_mutex_does_not_panic_drop_or_checkout` goes RED (`dropped.is_ok()` fails). Restore.
3. (Sanity, not a guard) flip the loop-top deadline check to `>` a far-future instant — the first test STILL passes via the dial bound; record that the loop-top check is belt-and-braces for the recycle path, covered functionally by the whole-suite green, not by a dedicated test.

The `health.rs` lock sites are Task 4's file; the same idiom lands there WITHOUT a dedicated poison test (poisoning mid-reap is not deterministically reachable) — inherited coverage from mutation 2, declared here so nobody mistakes it for a tested guard.

- [ ] **Step 6: Delta ledger + commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md`:

```markdown
### Task 3
- §7: checkout latency is now genuinely bounded by `checkout_timeout` (acquire + cleanup + dial
  under one deadline); docs/followups/2026-08-10-unbounded-backend-dial.md can be closed.
```

```bash
git add engine/crates/ferro-pool docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "fix(m1-s9a): checkout_timeout bounds the whole checkout — wedged dials return capacity; idle locks recover from poison"
```

---

## Task 4: The reaper survives a wedged ping

`reap_once` pings an idle conn with NO bound while holding an owned permit (`health.rs:123-125`): one half-dead backend kills the reaper for the pool's lifetime and leaks the permit for good (hazard 13, confirmed by execution). Bound the ping with `config.checkout_timeout` — the same bound the checkout-time recycle already uses; a ping that can't answer inside a checkout budget is dead for every purpose the reaper cares about. No new knob.

**Files:**
- Modify: `engine/crates/ferro-pool/src/health.rs` (`reap_once` at `:98-137`; the three `.lock().unwrap()` at `:99,112,132` → poison-recovering, same idiom as Task 3)
- Create: `engine/crates/ferro-pool/tests/reaper_bound.rs`

**Interfaces:**
- Consumes: `FakeBackend::{block_pings, pings_waiting}` (exist, `fake.rs:440-455`); `PoolConfig::reap_interval: Option<Duration>`.
- Produces: behavior only — a ping that exceeds `checkout_timeout` counts as DEAD → evict, release permit, keep ticking.

- [ ] **Step 1: Write the failing test**

Create `engine/crates/ferro-pool/tests/reaper_bound.rs`:

```rust
//! M1-S9a Task 4 — the reaper's ping is bounded (finding 4b). The M0 review CONFIRMED the
//! unbounded shape: `block_pings()` + `max_size=1` parked the reaper forever holding the sole
//! permit — two successive checkouts both `Err(Timeout)`, no idle conn ever evicted again.

use std::time::Duration;

use ferro_pool::config::PoolConfig;
use ferro_pool::fake::FakeBackend;
use ferro_pool::pool::Pool;

async fn bounded<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(600), fut)
        .await
        .expect("BOUND EXCEEDED: the reaper ping is unbounded (the finding-4b hang)")
}

#[tokio::test(start_paused = true)]
async fn a_wedged_ping_evicts_the_conn_and_the_reaper_keeps_ticking() {
    let backend = FakeBackend::new();
    let pool = Pool::new(
        backend,
        PoolConfig {
            max_size: 1,
            checkout_timeout: Duration::from_millis(50),
            reap_interval: Some(Duration::from_millis(5)),
            ..PoolConfig::default()
        },
    );

    // Park one idle conn, then freeze pings so the next reaper tick wedges on it.
    let co = pool.checkout().await.expect("first checkout");
    drop(co);
    pool.backend().block_pings();
    while pool.backend().pings_waiting() == 0 {
        tokio::time::sleep(Duration::from_millis(1)).await; // paused clock: auto-advances
    }
    let parked_once = pool.backend().pings_waiting();

    // THE property: the wedged ping is evicted within the bound, the permit comes back, and a
    // checkout succeeds (it dials fresh — connect is not gated here).
    let co = bounded(pool.checkout())
        .await
        .expect("the reaper must release its permit when the ping times out");
    drop(co);

    // And the reaper LOOP is still alive: the conn we just returned to idle gets picked up and
    // pinged by a LATER tick (the counter moves past its captured baseline — the parked future's
    // drop skips its decrement, so compare against the baseline, never an absolute value).
    // PLAN-VERIFY F3: this condition was `<= parked_once - 1`, which is FALSE at entry
    // (parked_once >= 1, so `1 <= 0`), so the loop never ran and the assert below fired
    // immediately as `1 > 1` — a test that could not pass against a CORRECT implementation.
    // Loop until the counter EXCEEDS the baseline; the inner if/break is then redundant.
    while pool.backend().pings_waiting() <= parked_once {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        pool.backend().pings_waiting() > parked_once,
        "the reaper must survive a wedged ping and keep examining idle conns on later ticks"
    );
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ferro-pool --test reaper_bound`
Expected: FAIL — `BOUND EXCEEDED: the reaper ping is unbounded` (the checkout after the wedge times out forever because the reaper holds the only permit).

- [ ] **Step 3: Implement**

In `health.rs::reap_once`, replace the `dead` computation (`:123-125`):

```rust
        let stale = idle_conn.created_at.elapsed() > inner.config.max_lifetime;
        // Short-circuited exactly like before: a conn past max_lifetime is evicted without
        // spending a round trip. The ping itself is BOUNDED by `checkout_timeout` (M1-S9a,
        // finding 4b — the same bound the checkout-time recycle uses): a backend that cannot
        // answer a ping inside a checkout budget is dead for every purpose the reaper has, and
        // an UNBOUNDED ping here parks the reaper forever while it holds an owned permit.
        let dead = !stale
            && (inner.backend.is_closed(&idle_conn.conn)
                || !matches!(
                    tokio::time::timeout(
                        inner.config.checkout_timeout,
                        inner.backend.ping(&mut idle_conn.conn),
                    )
                    .await,
                    Ok(Ok(()))
                ));
```

And the three `.lock().unwrap()` sites (`:99,112,132`) → `.lock().unwrap_or_else(std::sync::PoisonError::into_inner)` (Task 3's idiom; declared-untested defensive hygiene, see Task 3 Step 5).

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p ferro-pool` — new test green, whole pool suite green (the existing reaper tests — permit-per-pinged-conn, budget bound — are the regression proof).

- [ ] **Step 5: NAMED MUTATION**

Remove the `tokio::time::timeout(...)` wrapper (restore the bare `ping(...).await.is_err()`) → `a_wedged_ping_evicts_the_conn_and_the_reaper_keeps_ticking` goes RED at the `BOUND EXCEEDED` expect. Restore.

- [ ] **Step 6: Delta ledger + commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md`:

```markdown
### Task 4
- §7.6/§16 (reaper): the liveness ping is bounded by `checkout_timeout`; a wedged idle conn is
  evicted, never a permanent permit leak.
```

```bash
git add engine/crates/ferro-pool docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "fix(m1-s9a): bound the reaper ping — a wedged idle conn is evicted, the permit returns, the reaper lives"
```

---

## Task 5: The codec stops reserving 16 MiB per declared header

`FrameCodec::decode` reserves `payload_len` bytes the instant a header arrives (`codec.rs:70`), so a header declaring `MAX_FRAME_PAYLOAD` plus ONE body byte pins ~16 MiB until the frame completes or the socket closes (hazard 17, confirmed by execution). Reserve in bounded 64 KiB steps instead: memory then grows only with bytes actually RECEIVED — an attacker must pay for what they pin, and a legitimate large frame still decodes identically (`BytesMut` keeps growing as data lands; `reserve` is an amortization hint, not a correctness requirement).

**Files:**
- Modify: `engine/crates/ferrod/src/session/codec.rs` (the `decode` reserve at `:70`; add `#[cfg(test)] mod tests` — the file has none)

**Interfaces:**
- Consumes: `ferro_proto::header::{Header, HEADER_LEN}`, `ferro_proto::consts::MAX_FRAME_PAYLOAD`.
- Produces: behavior only. Task 11 later adds the `ReadProgress` counters to this same struct — sequential owners.

- [ ] **Step 1: Write the failing test**

Append to `engine/crates/ferrod/src/session/codec.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*; // codec.rs already imports Header + MAX_FRAME_PAYLOAD at the top

    fn header(payload_len: u32) -> Header {
        Header {
            flags: 0,
            service: 0,
            method: 0,
            request_id: 7,
            payload_len,
        }
    }

    /// M1-S9a finding 5: a header DECLARING the 16 MiB maximum plus one body byte must not make
    /// the decoder pre-reserve the whole declared payload — that is the one-local-client
    /// memory-amplification vector (N connections × 16 MiB for N × 17 bytes sent). The buffer may
    /// only grow in bounded steps as bytes actually arrive.
    #[test]
    fn a_partial_frame_reserves_in_bounded_steps_not_the_declared_payload() {
        let mut codec = FrameCodec::default();
        let mut src = bytes::BytesMut::new();
        src.extend_from_slice(&header(MAX_FRAME_PAYLOAD).encode());
        src.extend_from_slice(&[0u8]); // one body byte of 16 MiB declared

        let r = codec.decode(&mut src).expect("partial frame is NeedMore, not an error");
        assert!(r.is_none());
        assert!(
            src.capacity() < 256 * 1024,
            "the decoder reserved {} bytes for a frame of which 1 body byte has arrived — the \
             16 MiB-per-header amplification is back",
            src.capacity()
        );
    }

    /// The step-reserve must not break reassembly: a frame fed in chunks decodes byte-identically.
    #[test]
    fn a_chunked_large_frame_still_decodes_whole() {
        let payload = vec![0xabu8; 300 * 1024]; // several reserve steps
        let mut codec = FrameCodec::default();
        let mut src = bytes::BytesMut::new();
        src.extend_from_slice(&header(payload.len() as u32).encode());

        for chunk in payload.chunks(10 * 1024) {
            assert!(codec.decode(&mut src).expect("NeedMore").is_none());
            src.extend_from_slice(chunk);
        }
        let frame = codec
            .decode(&mut src)
            .expect("decode")
            .expect("the complete frame decodes");
        assert_eq!(frame.header.payload_len as usize, payload.len());
        assert_eq!(&frame.payload[..], &payload[..]);
        assert!(src.is_empty(), "no trailing bytes left behind");
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ferrod --lib session::codec`
Expected: `a_partial_frame_reserves_in_bounded_steps...` FAILS — capacity is ~16 MiB (the full-payload reserve). The chunked test passes before AND after (it is the non-regression half).

- [ ] **Step 3: Implement**

In `decode`:

```rust
/// M1-S9a (finding 5): when a frame is incomplete, reserve at most this much ahead of the bytes
/// actually received. `payload_len` is CLIENT-DECLARED — pre-reserving it in full let one local
/// connection pin 16 MiB with a 17-byte send, held until the frame completed or the socket
/// closed (measured). 64 KiB amortizes reallocation for ordinary frames; a large frame's buffer
/// still grows to size as its bytes genuinely arrive.
const PARTIAL_FRAME_RESERVE_STEP: usize = 64 * 1024;
```

and replace `src.reserve(need - src.len());` with:

```rust
            src.reserve((need - src.len()).min(PARTIAL_FRAME_RESERVE_STEP));
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p ferrod --lib session::codec` → both green. Then the regression proof for the sound list (this file IS the measured-sound wire codec): `cargo test -p ferrod --lib` and `cargo test -p ferrod --test session_rules` (the exactly-one-END / handshake / decode-safety suite) — all green, zero changes to them.

- [ ] **Step 5: NAMED MUTATION**

Restore `src.reserve(need - src.len())` → `a_partial_frame_reserves_in_bounded_steps_not_the_declared_payload` goes RED (capacity ≥ 16 MiB). Restore.

- [ ] **Step 6: Delta ledger + commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md`:

```markdown
### Task 5
- §5.2: note that a partial inbound frame buffers only received bytes (+ ≤64 KiB), never the
  declared payload length.
```

```bash
git add engine/crates/ferrod/src/session/codec.rs docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "fix(m1-s9a): codec reserves in 64KiB steps — a declared 16MiB header no longer pins 16MiB"
```

---

## Task 6: `loggable_scheme` stops leaking `user:pass://…` — allow-list, not slicing

`loggable_scheme` returns everything before the first `://` (`config.rs:95-98`); for `adminuser:s3cretPW://tcp/host` that IS the credential, WARN-logged by `infer_pool_kind` (hazard 24 — confirmed by execution; second occurrence of the S6 leak class). Fix: only the four schemes the daemon actually recognizes are ever echoed; everything else is a fixed placeholder. The existing test asserts `redis` passes through — it must FLIP, not vanish.

**Files:**
- Modify: `engine/crates/ferrod/src/config.rs` (`loggable_scheme` at `:95-98`; the test at `:559-578`)

**Interfaces:**
- Consumes/Produces: internal only (`fn loggable_scheme(dsn: &str) -> &'static str` — note the return type narrows to `&'static str`, all three outcomes are now fixed strings or one of the four constants).

- [ ] **Step 1: Write the failing test**

Replace `loggable_scheme_never_leaks_credentials` (`config.rs:559-578`) with:

```rust
    /// §12 secret hygiene, second round (M1-S9a finding 7a): the value handed to `tracing::warn!`
    /// must never be a slice of a credential-bearing DSN. The S6 fix covered no-`://` strings;
    /// the review found `user:pass://…` — credentials BEFORE the first `://`, no real scheme —
    /// leaking through the scheme slice. Only the four schemes the daemon recognizes are ever
    /// echoed; every other shape logs a fixed placeholder.
    #[test]
    fn loggable_scheme_never_leaks_credentials() {
        // No `://` at all (the S6 shapes): fixed placeholder.
        for dsn in [
            "mysql:/user:secret@db.internal/app",
            "admin:s3cret@tcp(10.0.0.5:3306)/prod",
            "not-a-dsn",
            "",
        ] {
            assert_eq!(loggable_scheme(dsn), "<no scheme>");
        }
        // Credentials BEFORE the first `://` (the M0-review shape): the "scheme" slice IS the
        // secret — it must never be echoed.
        for dsn in [
            "adminuser:s3cretPW://tcp/host",
            "user:secret@host://whatever",
        ] {
            let logged = loggable_scheme(dsn);
            assert_eq!(logged, "<unrecognized scheme>");
            assert!(!logged.contains("s3cretPW") && !logged.contains("secret"));
        }
        // An unrecognized-but-harmless-LOOKING scheme is STILL not echoed — we cannot tell it
        // from a credential without parsing, so the allow-list decides, not a character class.
        assert_eq!(loggable_scheme("redis://user:secret@h:6379"), "<unrecognized scheme>");
        // The four recognized schemes pass through (they are compile-time constants of ours).
        assert_eq!(loggable_scheme("postgres://ferro:pw@h/db"), "postgres");
        assert_eq!(loggable_scheme("postgresql://h/db"), "postgresql");
        assert_eq!(loggable_scheme("mysql://ferro:pw@h/db"), "mysql");
        assert_eq!(loggable_scheme("MariaDB://h/db"), "mariadb");
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ferrod --lib config::tests::loggable_scheme_never_leaks_credentials`
Expected: FAIL — `loggable_scheme("adminuser:s3cretPW://tcp/host")` returns `"adminuser:s3cretPW"` (the leak, reproduced as a test).

- [ ] **Step 3: Implement**

Replace `loggable_scheme`:

```rust
/// The ONLY portion of a DSN that is safe to log (SPEC §12): a scheme token **on the daemon's own
/// allow-list**. Anything else — no `://`, or an unrecognized prefix — logs a fixed placeholder,
/// because an arbitrary prefix can be credential text (`user:pass://…` puts the password BEFORE
/// the first `://`; the M0 core review measured exactly that leaking at WARN). The allow-list is
/// the same four schemes `infer_pool_kind` recognizes; echoing one of them echoes OUR constant,
/// never the operator's string content beyond case.
fn loggable_scheme(dsn: &str) -> &'static str {
    match dsn.split_once("://") {
        None => "<no scheme>",
        Some((scheme, _)) => match scheme.to_ascii_lowercase().as_str() {
            "postgres" => "postgres",
            "postgresql" => "postgresql",
            "mysql" => "mysql",
            "mariadb" => "mariadb",
            _ => "<unrecognized scheme>",
        },
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p ferrod --lib config` — the flipped test green, `infer_pool_kind_from_scheme` and the whole config suite green (behavioral note: `infer_pool_kind`'s WARN arm only ever fires for unrecognized schemes, whose log line now says `<unrecognized scheme>` — the recognized arms never warn, so no recognized-scheme log line changes).

- [ ] **Step 5: NAMED MUTATION**

Restore the old body (`dsn.split_once("://").map_or("<no scheme>", |(scheme, _)| scheme)`) → the test goes RED on the `adminuser:s3cretPW` case. Restore.

- [ ] **Step 6: Delta ledger + commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md`:

```markdown
### Task 6
- §12: scheme logging is allow-list-only ({postgres, postgresql, mysql, mariadb}); every other
  DSN shape logs a fixed placeholder.
```

```bash
git add engine/crates/ferrod/src/config.rs docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "fix(m1-s9a): loggable_scheme is allow-list-only — user:pass://… no longer reaches WARN logs"
```

---
## Task 7: `OpContext.tx_writes_persisted` — the RETRYABLE branch becomes unmintable once earlier tx writes persisted

**The derivation, in full — this cell is NOT simply "Retryable → Indeterminate".** What a client concludes from each branch on an in-tx statement failure: `Retryable` = "the whole transaction will never commit; replaying it from BEGIN is safe" (Doctrine-style retry wrappers replay the WHOLE closure); `NonRetryable` = known fate, nothing licensed; `Indeterminate` = fate unknown, at-most-once forbids replay. Once ANY earlier statement of the tx has persisted (a MySQL implicit commit — which fires BEFORE its statement executes, hazard 2), "replaying the tx is safe" is FALSE for every subsequent failure **regardless of which errc it rides**: `TxDeadline{Retryable}`, `ConnectionLost{Retryable}`, and a retryable `Sql` passthrough (a 1213 deadlock on the post-DDL statement) ALL license the replay that double-applies (hazard 3). So the rule is branch-shaped: **when `in_tx && tx_writes_persisted`, any outcome that would carry `branch::RETRYABLE` is replaced by `WriteUnconfirmed{Indeterminate}`** with an implicit-commit message — at the unit the client acts on (the transaction), "may or may not have applied" is the accurate at-most-once instruction: part provably applied, the rest did not or is unknown. Known-fate NonRetryable outcomes (23505 …) pass through verbatim — they license nothing and their statement-level fate is honestly known. One recorded cost: the documented "a client-declared-readonly statement never becomes `Indeterminate`" gains an exception (an in-tx read loss after an implicit commit describes the TRANSACTION's earlier writes, not the read) — Task 13's §22.2 delta states it; it is not silently changed.

This task adds the field and the post-filter with **every call site passing `false`** — zero behavior change, proven by the whole suite (including Task 1's armed guard) staying green. Task 8 threads the real values.

**Files:**
- Modify: `engine/crates/ferrod/src/services/fate.rs` (the struct at `:40-44`, the fn at `:71`, the unit tables)
- Modify: `engine/crates/ferrod/src/services/sql.rs` (add `tx_writes_persisted: false` to every `OpContext` literal — sites at `:329`, `:423`, `:552`, `:638`, `:660`, `:755`, `:1290`, `:1309`, `:1487`; the compiler enumerates any this list missed)
- Test: `fate.rs` unit tests (same file)

**Interfaces:**
- Produces: `OpContext { readonly: bool, sent: bool, in_tx: bool, tx_writes_persisted: bool }`; `pub(crate) fn persisted_tx_payload() -> ErrorPayload` (consumed by Task 8's `ExecReply::Deadline` and tombstone mappings — it and the post-filter must be THE one mint of this payload, preserving "classify_fate is the ONE place a PoolError becomes a wire ErrorPayload" in spirit: the two extra callers reuse the same constructor, never a hand-rolled copy).
- Consumes: `ferro_proto::consts::branch::RETRYABLE`; everything already in `fate.rs`.

- [ ] **Step 1: Write the failing tests**

Append to `fate.rs`'s `mod tests` (and extend the `ctx` helper):

```rust
    fn ctx(readonly: bool, sent: bool, in_tx: bool) -> OpContext {
        OpContext {
            readonly,
            sent,
            in_tx,
            tx_writes_persisted: false,
        }
    }

    /// An in-tx context whose transaction has (or may have) already persisted earlier writes —
    /// the M1-S9a implicit-commit cell.
    fn ctx_p(readonly: bool, sent: bool) -> OpContext {
        OpContext {
            readonly,
            sent,
            in_tx: true,
            tx_writes_persisted: true,
        }
    }

    /// THE finding-1 rule: once earlier tx writes persisted, `branch::RETRYABLE` is unmintable —
    /// whatever errc it would have ridden (TxDeadline via the 57014 override, ConnectionLost,
    /// a retryable Sql passthrough like a 1213 deadlock, even the unreachable-in-tx PoolTimeout).
    /// Every such outcome becomes WriteUnconfirmed{Indeterminate}; every already-non-retryable
    /// outcome is untouched. Exhaustive over (err × readonly × sent).
    #[test]
    fn the_retryable_branch_is_unmintable_once_tx_writes_persisted() {
        let retryable_shaped: Vec<PoolError> = vec![
            PoolError::ConnectionLost,
            sql_57014(),
            sql(errc::DEADLOCK, branch::RETRYABLE, "40001"),
            PoolError::Timeout,
        ];
        for err in &retryable_shaped {
            for readonly in [false, true] {
                for sent in [false, true] {
                    let ep = classify_fate(err.clone(), ctx_p(readonly, sent));
                    assert_ne!(
                        ep.branch,
                        branch::RETRYABLE,
                        "{err:?} readonly={readonly} sent={sent}: a persisted tx must never \
                         license replay"
                    );
                    assert_eq!(ep.code, errc::WRITE_UNCONFIRMED, "{err:?}");
                    assert_eq!(ep.branch, branch::INDETERMINATE, "{err:?}");
                    assert!(
                        ep.message.contains("implicit commit"),
                        "the message must name the mechanism, got: {}",
                        ep.message
                    );
                }
            }
        }
    }

    /// Known-fate NonRetryable outcomes pass through VERBATIM even when persisted — they license
    /// nothing, and their statement-level fate is honestly known (a 23505 is a 23505).
    #[test]
    fn known_fate_nonretryable_passes_through_even_when_persisted() {
        let dup = PoolError::Sql {
            code: errc::UNIQUE,
            branch: errc::UNIQUE_BRANCH,
            sqlstate: Some("23000".to_string()),
            errno: Some(1062),
            message: "Duplicate entry".to_string(),
        };
        let ep = classify_fate(dup, ctx_p(false, true));
        assert_eq!(ep.code, errc::UNIQUE);
        assert_eq!(ep.branch, branch::NON_RETRYABLE);
        assert_eq!(ep.errno, Some(1062), "the passthrough keeps its vendor identity");
    }

    /// The flag is inert outside a transaction: an autocommit call site that wrongly sets it
    /// changes NOTHING (`in_tx` gates the filter), so no autocommit fate can regress.
    #[test]
    fn persisted_without_in_tx_is_inert() {
        for readonly in [false, true] {
            for sent in [false, true] {
                let with = classify_fate(
                    PoolError::ConnectionLost,
                    OpContext {
                        readonly,
                        sent,
                        in_tx: false,
                        tx_writes_persisted: true,
                    },
                );
                let without = classify_fate(PoolError::ConnectionLost, ctx(readonly, sent, false));
                assert_eq!((with.code, with.branch), (without.code, without.branch));
            }
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ferrod --lib services::fate`
Expected: COMPILE ERROR — `OpContext` has no field `tx_writes_persisted` (every literal in the file and in `sql.rs` now fails to build, which IS the enumeration of call sites).

- [ ] **Step 3: Implement**

In `fate.rs`: add the field with its doc, rename the existing `classify_fate` body to a private `classify_fate_unfiltered`, add the wrapper + the payload constructor:

```rust
    /// M1-S9a (finding 1): set by an in-tx call site when statements EARLIER in this transaction
    /// have already PERSISTED — either OBSERVED (the live pin state `Checkout::tx_open()` read
    /// false after a completed statement: a MySQL implicit commit ended the tx behind the client's
    /// back) or MUST-ASSUME (the in-flight statement itself was implicit-commit-shaped, whose
    /// pre-commit may have fired before the loss — `ferro_classify::implicit_commit_hazard`).
    /// Meaningful only alongside `in_tx: true`; every autocommit and control site passes `false`.
    /// Effect: `branch::RETRYABLE` becomes unmintable (see `classify_fate`) — a partially
    /// committed transaction must never be replayed.
    pub tx_writes_persisted: bool,
```

```rust
pub fn classify_fate(err: PoolError, ctx: OpContext) -> ErrorPayload {
    let ep = classify_fate_unfiltered(err, ctx);
    // M1-S9a (finding 1): the post-filter. Once earlier statements of this transaction have
    // persisted, NO outcome may carry the Retryable branch — a Retryable in-tx terminal is read
    // by clients as "replaying the whole transaction is safe", and here it demonstrably is not
    // (measured on MySQL 8.4: BEGIN; INSERT; CREATE TABLE; <loss> left the INSERT durably
    // committed with no COMMIT ever sent). Branch-shaped on purpose: TxDeadline, ConnectionLost
    // AND a retryable Sql passthrough (1213/40001) all license the same replay.
    if ctx.in_tx && ctx.tx_writes_persisted && ep.branch == ferro_proto::consts::branch::RETRYABLE
    {
        return persisted_tx_payload();
    }
    ep
}

/// The ONE mint of the partially-committed-transaction terminal (M1-S9a finding 1) — reused by
/// the `ExecReply::Deadline` handler and the tombstone mapping (Task 8) so the wording and the
/// (code, branch) pair can never fork. Rides `WRITE_UNCONFIRMED{Indeterminate}` — the branch
/// whose contract is "do not replay" — because at the unit the client acts on (the transaction),
/// part has provably applied and the rest is unknown or refused. Deliberately NOT a new /proto
/// code (charter rule 2); a dedicated TX_PARTIALLY_COMMITTED is a recorded deferred candidate.
pub(crate) fn persisted_tx_payload() -> ErrorPayload {
    payload(
        errc::WRITE_UNCONFIRMED,
        errc::WRITE_UNCONFIRMED_BRANCH,
        "an earlier statement in this transaction caused an implicit commit (MySQL DDL, LOCK \
         TABLES, SET autocommit, ...): statements before it HAVE PERSISTED, so the transaction \
         must not be replayed; the failed statement's own outcome is unconfirmed (§19.3 \
         indeterminate — the engine never retries; retry only via an idempotent manifest)",
    )
}
```

(`classify_fate_unfiltered` is the existing body verbatim — no arm changes in this task.) In `sql.rs`, add `tx_writes_persisted: false,` to every `OpContext` literal the compiler flags (the nine sites listed under **Files**).

- [ ] **Step 4: Run to verify pass — and the zero-behavior-change proof**

Run: `cargo test -p ferrod` (offline) → green including every pre-existing fate table. Then the live regression sweep — this file IS the §19.3 authority, so the proof is the FULL fate surface: `cargo test -p ferrod --test chaos_fate_it --test mysql_chaos_it --test in_tx_fate_it --test tx_it --test sql_exec_it` (env vars set) → green, zero diffs. Task 1's guard green here is what proves the in-tx cell did not move while the field landed.

- [ ] **Step 5: NAMED MUTATIONS**

1. In the post-filter, replace the condition with `false` (or delete the block) → `the_retryable_branch_is_unmintable_once_tx_writes_persisted` goes RED on every row. Restore.
2. Drop `ctx.in_tx &&` from the condition → `persisted_without_in_tx_is_inert` goes RED. Restore.
3. In `persisted_tx_payload`, swap `errc::WRITE_UNCONFIRMED` for `errc::TX_DEADLINE` → the `code` assertion in test 1 goes RED (proves the test pins the code, not just the branch). Restore.

- [ ] **Step 6: Delta ledger + commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md`:

```markdown
### Task 7
- §19.3: NEW RULE — once a transaction's earlier statements have persisted (implicit commit),
  the Retryable branch is unmintable for that transaction's failures; such outcomes ride
  WriteUnconfirmed{Indeterminate}. States the readonly-invariant exception explicitly.
- §22.2: deferred /proto candidate TX_PARTIALLY_COMMITTED recorded.
```

```bash
git add engine/crates/ferrod/src/services docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "feat(m1-s9a): tx_writes_persisted — the Retryable branch is unmintable for a partially committed tx (all sites false; behavior unchanged)"
```

---

## Task 8: The actor consults the live pin state — latch + hazard + every terminal path, with the live at-least-once acceptance

The engine has the detection signal and drops it (hazard 1). This task makes the tx actor read it: a **latch** from the protocol authority (`Checkout::tx_open()` false after a completed statement ⇒ the tx implicitly committed ⇒ persisted, forever), plus the **pre-dispatch hazard** (Task 2) for the loss-of-the-committing-statement-itself case where no protocol signal can exist (hazard 2b). The flag then travels EVERY in-tx terminal path (hazard 4): the `Completed(Err)` classification, the `Deadline` reply, the streamed ctx, the teardown-drained queued statements, and the tombstone.

**Files:**
- Modify: `engine/crates/ferro-pool/src/pool.rs` (ONE accessor: `Checkout::dialect()`)
- Modify: `engine/crates/ferro-pool/src/fake.rs` (ONE knob: `arm_tx_status_after_next_query`)
- Modify: `engine/crates/ferrod/src/tx/mod.rs` (`ExecReply`, `TxEntry::Tombstoned`, `TxRegistry::tombstone`, `TxLookupErr::Tombstoned`)
- Modify: `engine/crates/ferrod/src/tx/actor.rs` (the latch + hazard in `run`; `run_tx_streamed` call; `drain_buffered_on_teardown`; `teardown`'s tombstone call; the in-file tests)
- Modify: `engine/crates/ferrod/src/services/sql.rs` (the `ExecReply` consumption at `:313-345`; `run_tx_streamed`'s signature + ctx at `:739-759`; every `TxLookupErr::Tombstoned` mapping)
- Test: `actor.rs` `mod tests` (offline latch/hazard/tombstone); `engine/crates/ferrod/tests/in_tx_fate_it.rs` (APPEND the live acceptance — Task 1's file, sequential ownership)

**Interfaces:**
- Consumes: `ferro_classify::implicit_commit_hazard(sql: &str, dialect: Dialect) -> bool` (Task 2); `fate::persisted_tx_payload()` + `OpContext.tx_writes_persisted` (Task 7); `Checkout::tx_open() -> bool` (`pool.rs:330`, public); Task 1's test file helpers (`begin`, `tx_req`, `write_req`, `unique_key`, `raw_mysql`, `wait_for_active_conn`).
- Produces:
  - `Checkout::dialect(&self) -> ferro_classify::Dialect` (pool.rs — `self.pool.backend.dialect()`, the expression `apply_classify` already uses).
  - `FakeBackend::arm_tx_status_after_next_query(&self, st: TxStatus)` — one-shot: after the next successful `query()`, the conn reports `st` (models the OK-packet `SERVER_STATUS_IN_TRANS` dropping).
  - `ExecReply::Completed { result: Result<QueryResult, PoolError>, exec_us: u64, tx_writes_persisted: bool }`, `ExecReply::Deadline { tx_writes_persisted: bool }`.
  - `TxRegistry::tombstone(&self, tx_id: u64, tx_writes_persisted: bool)`; `TxEntry::Tombstoned { owner, tx_writes_persisted }`; `TxLookupErr::Tombstoned { tx_writes_persisted: bool }`.
  - `run_tx_streamed(..., tx_writes_persisted: bool) -> StreamEnded` (parameter appended LAST).

- [ ] **Step 1: Write the failing offline tests**

Append to `actor.rs`'s `mod tests` (these use the file's REAL helpers: `test_pool_config()`, `spawn_actor(pool, registry, owner, idle, max)` at `actor.rs:831` — plus `FakeBackend::set_dialect` and the new knob):

```rust
    /// M1-S9a finding 1, the LATCH: after a statement completes with the authority reading
    /// tx_open == false (an implicit commit ended the tx behind the pool's back), every later
    /// in-flight loss must carry tx_writes_persisted = true to the fate call site.
    #[tokio::test]
    async fn an_implicit_commit_latches_persisted_for_every_later_loss() {
        let backend = FakeBackend::new();
        backend.set_dialect(ferro_pool::backend::Dialect::MySql);
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        let (tx_id, cmd_tx, _done) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        // Statement 1: plain DML, tx stays open — the reply must NOT be persisted-marked.
        let (r1_tx, r1_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "INSERT INTO t VALUES (1)".into(),
                params: vec![],
                timeout_ms: None,
                cancel: CancellationToken::new(),
                reply: r1_tx,
            })
            .await
            .expect("send");
        match r1_rx.await.expect("reply 1") {
            ExecReply::Completed {
                tx_writes_persisted,
                result,
                ..
            } => {
                assert!(result.is_ok());
                assert!(!tx_writes_persisted, "no implicit commit has happened yet");
            }
            other => panic!("expected Completed, got {other:?}"),
        }

        // Statement 2: completes OK but the authority reads Idle afterwards — the fake models a
        // MySQL DDL's OK packet with SERVER_STATUS_IN_TRANS dropped.
        pool.backend()
            .arm_tx_status_after_next_query(ferro_pool::backend::TxStatus::Idle);
        let (r2_tx, r2_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "INSERT INTO t VALUES (2)".into(), // deliberately NOT hazard-shaped: the
                params: vec![],                          // LATCH must come from the authority,
                timeout_ms: None,                        // not the lexer
                cancel: CancellationToken::new(),
                reply: r2_tx,
            })
            .await
            .expect("send");
        assert!(matches!(
            r2_rx.await.expect("reply 2"),
            ExecReply::Completed { result: Ok(_), .. }
        ));

        // Statement 3: lost mid-flight (armed ConnectionLost) — the reply MUST be persisted-marked.
        pool.backend().arm_next_query_err(PoolError::ConnectionLost);
        let (r3_tx, r3_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "INSERT INTO t VALUES (3)".into(),
                params: vec![],
                timeout_ms: None,
                cancel: CancellationToken::new(),
                reply: r3_tx,
            })
            .await
            .expect("send");
        match r3_rx.await.expect("reply 3") {
            ExecReply::Completed {
                tx_writes_persisted,
                result,
                ..
            } => {
                assert!(result.is_err());
                assert!(
                    tx_writes_persisted,
                    "the latch must mark every loss after the observed implicit commit"
                );
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    /// M1-S9a finding 1, the HAZARD: a statement that is ITSELF implicit-commit-shaped and is
    /// interrupted mid-flight must be persisted-marked even though the latch never observed
    /// anything (the pre-commit fires BEFORE execution; no protocol signal ever arrives).
    /// Control: the same interruption on plain DML stays unmarked (Retryable survives).
    #[tokio::test(start_paused = true)]
    async fn a_lost_implicit_commit_statement_is_hazard_marked_and_plain_dml_is_not() {
        for (sql, want_persisted) in [
            ("CREATE TABLE ferro_ic (x INT)", true),
            ("INSERT INTO t VALUES (1)", false),
        ] {
            let backend = FakeBackend::new();
            backend.set_dialect(ferro_pool::backend::Dialect::MySql);
            backend.block_query(); // freeze the statement mid-flight
            let pool = Pool::new(backend, test_pool_config());
            let registry = TxRegistry::new(Duration::from_secs(5));
            let owner = registry.next_session_id();
            let (tx_id, cmd_tx, _done) = spawn_actor(
                &pool,
                &registry,
                owner,
                Duration::from_secs(600),
                Duration::from_secs(600),
            )
            .await;

            let (r_tx, r_rx) = oneshot::channel();
            cmd_tx
                .send(TxCommand::Exec {
                    sql: sql.into(),
                    params: vec![],
                    timeout_ms: Some(10), // the per-statement deadline interrupts the frozen query
                    cancel: CancellationToken::new(),
                    reply: r_tx,
                })
                .await
                .expect("send");
            match r_rx.await.expect("reply") {
                ExecReply::Deadline { tx_writes_persisted } => {
                    assert_eq!(
                        tx_writes_persisted, want_persisted,
                        "{sql:?}: hazard marking wrong"
                    );
                }
                other => panic!("{sql:?}: expected Deadline, got {other:?}"),
            }
            // And the tombstone carries the same answer for every LATER op on this tx_id.
            match registry.lookup(tx_id, owner) {
                Err(TxLookupErr::Tombstoned { tx_writes_persisted }) => {
                    assert_eq!(tx_writes_persisted, want_persisted, "{sql:?}: tombstone wrong");
                }
                other => panic!("{sql:?}: expected Tombstoned, got {other:?}"),
            }
        }
    }

    /// PG stays byte-identical: the hazard never fires on Dialect::Postgres, and PG's RFQ can
    /// never read Idle inside an actor-owned tx (transactional DDL) — so a Postgres-dialect
    /// interruption is never persisted-marked.
    #[tokio::test(start_paused = true)]
    async fn postgres_dialect_never_marks_persisted() {
        let backend = FakeBackend::new(); // Dialect::Postgres is the default
        backend.block_query();
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        let (_tx_id, cmd_tx, _done) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        let (r_tx, r_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "CREATE TABLE t (x INT)".into(), // hazard-shaped ON MYSQL; inert on PG
                params: vec![],
                timeout_ms: Some(10),
                cancel: CancellationToken::new(),
                reply: r_tx,
            })
            .await
            .expect("send");
        assert!(matches!(
            r_rx.await.expect("reply"),
            ExecReply::Deadline {
                tx_writes_persisted: false
            }
        ));
    }
```

And in `sql.rs`'s `mod tests` (the Deadline mapping is the one in-tx exit that bypasses `classify_fate` — it needs its own pin):

```rust
    /// M1-S9a: the Deadline reply's terminal choice. Persisted ⇒ the persisted payload (never
    /// Retryable); otherwise the classic TxDeadline{Retryable}. This is what stops the handler
    /// arm from quietly ignoring the flag (named mutation 3).
    #[test]
    fn persisted_deadline_reply_maps_to_the_persisted_payload() {
        let p = deadline_terminal(true);
        assert_eq!(p.code, errc::WRITE_UNCONFIRMED);
        assert_eq!(p.branch, ferro_proto::consts::branch::INDETERMINATE);
        assert!(p.message.contains("implicit commit"), "got: {}", p.message);

        let d = deadline_terminal(false);
        assert_eq!(d.code, errc::TX_DEADLINE);
        assert_eq!(d.branch, ferro_proto::consts::branch::RETRYABLE);
    }
```

And APPEND the live acceptance to `engine/crates/ferrod/tests/in_tx_fate_it.rs` (Task 1's helpers):

```rust
/// M1-S9a finding 1 — THE measured at-least-once shape, now honestly reported. BEGIN → INSERT →
/// CREATE TABLE (implicit commit: the INSERT persists) → a later statement is killed mid-flight.
/// The OLD engine said `Retryable` ("the transaction was rolled back") — licensing a replay that
/// double-applies the INSERT. The terminal must now be WRITE_UNCONFIRMED{Indeterminate} naming
/// the implicit commit, and the read-back proves WHY: the INSERT is durably there with no COMMIT
/// ever sent. Task 1's no-DDL sibling pins that the plain-DML tx KEEPS its honest Retryable.
#[tokio::test]
async fn mysql_in_tx_loss_after_an_implicit_commit_is_indeterminate_never_retryable() {
    let Some(url) = mysql_url() else {
        eprintln!("skip: FERRO_TEST_MYSQL_URL not set");
        return;
    };
    let mut side = raw_mysql(&url).await;
    let server = exec_server(url);
    let mut c = server.connect().await;
    c.hello(1).await;

    exec_ok(
        &mut c,
        2,
        &write_req(
            "CREATE TABLE IF NOT EXISTS ferro_s9a_intx (k VARCHAR(128) PRIMARY KEY, n INT NOT NULL)",
        ),
    )
    .await;
    let key = unique_key("my_ic");
    let ddl_table = format!("ferro_s9a_ic_{}", unique_key("t"));

    let tx_id = begin(&mut c, 3, "default", None, false).await;
    exec_ok(
        &mut c,
        4,
        &tx_req(&format!("INSERT INTO ferro_s9a_intx VALUES ('{key}', 1)"), tx_id),
    )
    .await;
    // The implicit commit: a real DDL inside the explicit transaction.
    exec_ok(
        &mut c,
        5,
        &tx_req(&format!("CREATE TABLE {ddl_table} (x INT)"), tx_id),
    )
    .await;

    // Kill the pinned conn while a LATER statement is provably in flight (Task 1's machinery).
    let marker = unique_key("my_ic_kill");
    let victim = tx_req(&format!("SELECT SLEEP(5) /* {marker} */"), tx_id);
    c.send_request(6, service::SQL, method_sql::EXEC, victim.encode())
        .await;
    let id = wait_for_active_conn(&mut side, &marker).await;
    {
        use mysql_async::prelude::Queryable;
        side.query_drop(format!("KILL {id}")).await.expect("KILL");
    }

    let t = c.recv().await;
    assert_eq!(t.header.request_id, 6);
    let ep = match Outcome::decode(&t.payload).expect("decode Outcome") {
        Outcome::Error(ep) => ep,
        other => panic!("expected Outcome::Error, got {other:?}"),
    };
    assert_eq!(
        ep.code,
        errc::WRITE_UNCONFIRMED,
        "a loss after an implicit commit must be Indeterminate — Retryable here licenses the \
         measured double-apply. got {:#06x}: {}",
        ep.code,
        ep.message
    );
    assert_eq!(ep.branch, branch::INDETERMINATE);
    assert!(ep.message.contains("implicit commit"), "got: {}", ep.message);

    // WHY Retryable would have been a lie: the pre-DDL INSERT persisted, no COMMIT ever sent.
    let ok = exec_ok(
        &mut c,
        7,
        &req(&format!(
            "SELECT CAST(count(*) AS SIGNED) FROM ferro_s9a_intx WHERE k = '{key}'"
        )),
    )
    .await;
    assert_eq!(
        ok.rows[0][0],
        Value::I64(1),
        "the implicit commit durably applied the pre-DDL INSERT — this is the at-least-once \
         mechanism the terminal above must confess to"
    );

    // Cleanup the per-run DDL table (autocommit; ignore result shape).
    exec_ok(&mut c, 8, &write_req(&format!("DROP TABLE IF EXISTS {ddl_table}"))).await;
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p ferrod --lib tx::actor` → COMPILE ERROR (`ExecReply::Completed` has no field `tx_writes_persisted`; `TxLookupErr::Tombstoned` is a unit variant; `arm_tx_status_after_next_query`/`Checkout::dialect` do not exist). That compile error IS the site enumeration.

- [ ] **Step 3: Implement**

**(a) `pool.rs`** (one accessor, placed beside `tx_open()`):

```rust
    /// The backend's SQL dialect (the assist lexer's `ferro_classify::Dialect`) — exposed so the
    /// tx actor can consult dialect-scoped PRE-dispatch assists (M1-S9a:
    /// `implicit_commit_hazard`). Same expression `apply_classify` already uses internally.
    pub fn dialect(&self) -> ferro_classify::Dialect {
        self.pool.backend.dialect()
    }
```

**(b) `fake.rs`** — field `tx_status_after_query: Mutex<Option<TxStatus>>` (init `None` in `new()`), knob + consumption:

```rust
    /// One-shot (M1-S9a): after the NEXT successful `query()`, the conn reports `st` from
    /// `tx_status()` — models a MySQL implicit-commit statement's OK packet, whose
    /// SERVER_STATUS_IN_TRANS drops while the pool believed a transaction was open.
    pub fn arm_tx_status_after_next_query(&self, st: TxStatus) {
        *self.tx_status_after_query.lock().unwrap() = Some(st);
    }
```

and in `PoolBackend::query`'s success path (immediately before returning `Ok`): `if let Some(st) = self.tx_status_after_query.lock().unwrap().take() { conn.tx_status = st; }`.

**(c) `tx/mod.rs`**:

```rust
pub enum ExecReply {
    Completed {
        result: Result<QueryResult, PoolError>,
        exec_us: u64,
        /// M1-S9a finding 1: earlier statements of this tx have persisted (latch) or must be
        /// assumed to (in-flight hazard) — the fate call site threads this into `OpContext`.
        tx_writes_persisted: bool,
    },
    Deadline {
        /// Same flag for the deadline/cancel exit, which bypasses `classify_fate` (the handler
        /// answers `persisted_tx_payload()` instead of `tx_deadline(...)` when set).
        tx_writes_persisted: bool,
    },
}
```

`TxEntry::Tombstoned { owner, tx_writes_persisted }`; `pub fn tombstone(&self, tx_id: u64, tx_writes_persisted: bool)` (inserts the flag; the eviction logic unchanged); `TxLookupErr::Tombstoned { tx_writes_persisted: bool }` and `lookup`'s arm forwards the stored flag. Fix the in-file tests the compiler flags (they assert `TxLookupErr::Tombstoned` — they become `Tombstoned { tx_writes_persisted: false }`, since nothing in them persists).

**(d) `actor.rs`** — in `run`, after `let mut sp = SavepointStack::new();`:

```rust
    // M1-S9a (finding 1): has this transaction ALREADY persisted anything? Latched from the
    // protocol AUTHORITY — `co.tx_open()` reading false after a completed statement means an
    // implicit commit ended the tx behind the client's back (MySQL DDL et al; on PG this can
    // never happen — DDL is transactional). Monotonic: once true, stays true.
    let mut tx_writes_persisted = false;
```

In `TxCommand::Exec` (before `let query_fut = ...`):

```rust
                // Pre-dispatch hazard (M1-S9a): if THIS statement is implicit-commit-shaped, its
                // pre-commit may fire before any loss we could observe — so for the DURATION of
                // this statement, a loss classifies as if the tx already persisted. The authority
                // (the post-statement tx_open read below) corrects a false positive the moment
                // the statement COMPLETES; a lexical false positive therefore costs one
                // conservative Indeterminate iff the statement is LOST mid-flight.
                let hazard = tx_writes_persisted
                    || ferro_classify::implicit_commit_hazard(&sql, co.dialect());
```

The `ExecStep::Completed` arm:

```rust
                    ExecStep::Completed(result, exec_us) => {
                        if let Err(e) = &result
                            && fate::is_57014(e)
                        {
                            if hazard {
                                tx_writes_persisted = true;
                            }
                            let _ = reply.send(ExecReply::Deadline {
                                tx_writes_persisted: hazard,
                            });
                            break 'actor TxEnd::Deadline;
                        }
                        if result.is_ok() {
                            // The AUTHORITY: a clean post-statement read. tx_open false inside an
                            // actor-owned tx == the implicit commit happened. (On the Err arm the
                            // Rule-A fail-safe forces tx_open true — stale-untrustworthy — so the
                            // latch reads ONLY the Ok arm; an errored implicit-commit statement is
                            // covered by the hazard below.)
                            if !co.tx_open() {
                                tx_writes_persisted = true;
                            }
                        } else if hazard {
                            // An implicit-commit statement that ERRORED server-side: MySQL's
                            // pre-commit had already fired before execution — latch.
                            tx_writes_persisted = true;
                        }
                        let _ = reply.send(ExecReply::Completed {
                            result,
                            exec_us,
                            tx_writes_persisted: hazard,
                        });
                    }
```

Wait — the flag the REPLY carries for an `Err` completion must be `hazard` (the pre-dispatch view: was the tx possibly-persisted when THIS statement failed), while for an `Ok` it is irrelevant (never consulted). `hazard` is exactly right in both. The `Deadline`/`Abort` arms:

```rust
                    ExecStep::Deadline => {
                        cancel_handle.cancel().await;
                        let _ = query_fut.await;
                        if hazard {
                            tx_writes_persisted = true;
                        }
                        let _ = reply.send(ExecReply::Deadline {
                            tx_writes_persisted: hazard,
                        });
                        break 'actor TxEnd::Deadline;
                    }
                    ExecStep::Abort => {
                        cancel_handle.cancel().await;
                        let _ = query_fut.await;
                        if hazard {
                            tx_writes_persisted = true;
                        }
                        drop(reply);
                        break 'actor TxEnd::Abort;
                    }
```

(Task 10 wraps these two drains in the budget — do not do it here.) In `TxCommand::ExecStreamed`, compute the same `hazard` before dispatch and pass it: `run_tx_streamed(&mut co, &sql, &params, responder, &child, deadline, readonly, hazard).await`; after `StreamEnded::Intact`, `if !co.tx_open() { tx_writes_persisted = true; }`; on `Broken`, `if hazard { tx_writes_persisted = true; }` before the break. After the loop: `teardown(...)` unchanged, then `drain_buffered_on_teardown(&mut cmd_rx, end, tx_writes_persisted);` and in `teardown`, `TxEnd::Deadline => registry.tombstone(tx_id, tx_writes_persisted)` — thread the flag as a new `teardown` parameter. In `drain_buffered_on_teardown`, the `Deadline | Abort` arm answers `crate::services::fate::persisted_tx_payload()` when the flag is set, the existing `tx_deadline(...)` text otherwise.

**(e) `sql.rs`** — the consumption:

```rust
                Ok(ExecReply::Completed {
                    result,
                    exec_us,
                    tx_writes_persisted,
                }) => match result {
                    Ok(qr) => match build_terminal_body(qr, req.fetch, 0, exec_us) {
                        Ok(body) => responder.end_ok(Bytes::from(body)),
                        Err(ep) => responder.end_error(ep),
                    },
                    Err(e) => responder.end_error(fate::classify_fate(
                        e,
                        OpContext {
                            readonly: req.readonly,
                            sent: true,
                            in_tx: true,
                            tx_writes_persisted,
                        },
                    )),
                },
                Ok(ExecReply::Deadline { tx_writes_persisted }) => {
                    responder.end_error(deadline_terminal(tx_writes_persisted))
                }
```

with the choice extracted into a pure, testable fn beside `tx_deadline`:

```rust
/// The terminal for an in-tx deadline/cancel exit — the ONE in-tx path that bypasses
/// `classify_fate` (M1-S9a). Persisted ⇒ the persisted payload (Retryable unmintable);
/// otherwise the classic TxDeadline{Retryable}.
fn deadline_terminal(tx_writes_persisted: bool) -> ErrorPayload {
    if tx_writes_persisted {
        fate::persisted_tx_payload()
    } else {
        tx_deadline(
            "transaction deadline exceeded mid-statement; the statement was cancelled and the \
             transaction rolled back (retryable — the engine never re-runs)",
        )
    }
}
```

`run_tx_streamed` gains the trailing parameter and threads it into its `ctx` (`tx_writes_persisted` replaces the Task-7 `false` literal at `sql.rs:755`). Every `TxLookupErr::Tombstoned` mapping (the compiler lists them; the known ones are the tx-scoped EXEC lookup and the TX-service control lookups in this file) becomes:

```rust
                Err(TxLookupErr::Tombstoned { tx_writes_persisted: true }) => {
                    responder.end_error(fate::persisted_tx_payload());
                    return;
                }
                Err(TxLookupErr::Tombstoned { tx_writes_persisted: false }) => {
                    responder.end_error(tx_deadline(/* the existing tombstone message, verbatim */));
                    return;
                }
```

- [ ] **Step 4: Run to verify pass**

Offline: `cargo test -p ferrod` and `cargo test -p ferro-pool` → green (the new actor tests included). Live: the FULL fate surface again — `cargo test -p ferrod --test in_tx_fate_it --test chaos_fate_it --test mysql_chaos_it --test tx_it --test mysql_it --test stream_it` (env vars set). Task 1's two tests STILL GREEN is the headline regression proof: the plain-DML in-tx loss KEEPS its honest `Retryable` on both engines (the latch/hazard fire only on implicit-commit shapes), and the new acceptance test proves the DDL shape flipped to `Indeterminate`. PG's suites green prove dialect-scoping (hazard: PG behavior byte-identical).

- [ ] **Step 5: NAMED MUTATIONS**

1. Delete the latch read (`if !co.tx_open() { tx_writes_persisted = true; }` in the Ok arm) → `an_implicit_commit_latches_persisted_for_every_later_loss` goes RED (reply 3 unmarked) AND the live `mysql_in_tx_loss_after_an_implicit_commit_...` goes RED (terminal comes back `CONNECTION_LOST{Retryable}`). Restore.
2. Replace the hazard computation with `let hazard = tx_writes_persisted;` → `a_lost_implicit_commit_statement_is_hazard_marked_and_plain_dml_is_not` goes RED on the CREATE row. Restore.
3. In `deadline_terminal`, ignore the flag (always the `tx_deadline(...)` arm) → `persisted_deadline_reply_maps_to_the_persisted_payload` goes RED. (Why this mapping needs its own pin: the live acceptance's loss is a KILL — `Completed(Err)` via `classify_fate` — so it cannot catch the Deadline arm; the hazard test asserts the REPLY, not the wire choice. The pure fn is what makes the arm falsifiable without a `Responder`.) Restore.
4. Swap `registry.tombstone(tx_id, tx_writes_persisted)` to pass `false` → the tombstone half of the hazard test goes RED. Restore.

- [ ] **Step 6: Delta ledger + commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md`:

```markdown
### Task 8
- §19.3 + §7.1: the tx actor consults the live pin state (latch) and the pre-dispatch
  implicit-commit hazard; every in-tx terminal path (statement Err, deadline, streamed, queued
  teardown, tombstone) carries tx_writes_persisted. MySQL-only by construction; PG byte-identical.
- CLAUDE.md: the S9a current-state paragraph documents the drop-in consequence: on MySQL, a
  transaction that ran DDL reports later failures as Indeterminate, never Retryable.
```

```bash
git add engine/crates/ferro-pool engine/crates/ferrod docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "fix(m1-s9a): the MySQL implicit-commit at-least-once — latch + hazard make tx_writes_persisted real on every in-tx terminal path

Live acceptance: BEGIN;INSERT;CREATE TABLE;<kill> now answers WRITE_UNCONFIRMED/INDETERMINATE
naming the implicit commit, with the INSERT durably persisted and replay unlicensed; the
plain-DML tx keeps its honest Retryable (Task 1's guard, still green)."
```

---

## Task 9: `ConnectionLost { dispatched }` — a statement that provably never left the process is never `Indeterminate`

A conn that died BEFORE dispatch is reported `WriteUnconfirmed{Indeterminate}` — reproduced live with the write provably unapplied; §19.3 reads `Retryable` (hazard 9). The service's `sent` is honest about the CALL SITE (post-checkout), but the ERROR knows more: pg's `query`/`query_stream` are prepare-THEN-dispatch, and a prepare-phase loss means the Execute was never sent. Carry that in the error: `ConnectionLost { dispatched: bool }`, compile-forcing every construction site to choose (hazard 10), defaulting `true` (the cry-wolf-safe direction, hazard 11). This also closes M1-S8b's unreproduced pre-HEAD-Indeterminate sighting with a cause.

**Files:**
- Modify: `engine/crates/ferro-pool/src/error.rs` (the variant + `undispatched()` + the `taxonomy_branch`/`errc` patterns)
- Modify: `engine/crates/ferro-pool/src/pool.rs:134` (`dispatched: false` — a connect failure never carried a statement), `engine/crates/ferro-pool/src/fake.rs:542` (inside `connect()` — the `arm_fail_connect` refusal: `dispatched: false`, a connect failure never carried a statement), `fake.rs:563` (a post-checkout loss model: `dispatched: true`)
- Modify: `engine/crates/ferro-backend-pg/src/error_map.rs:28` (`dispatched: true`), `engine/crates/ferro-backend-pg/src/query.rs:69-71,178-180` (the two prepare sites → `.map_err(|e| error_map::map(&e).undispatched())`), `engine/crates/ferro-backend-pg/src/conn.rs:114,162,235` (the `connect()`-path sites → `dispatched: false`; any non-connect site there stays `true` — read each site's context, the compiler forces the visit)
- Modify: `engine/crates/ferro-backend-mysql/src/error_map.rs:66,84` (`dispatched: true`), `engine/crates/ferro-backend-mysql/src/conn.rs:195,233,256,317` (connect-path → `false`, statement-path → `true`), `engine/crates/ferro-backend-mysql/src/bind.rs:405,742` (patterns → `ConnectionLost { .. }`; **verify at implementation** whether the `prep()` error is a distinct await that can take `.undispatched()` — if the mysql_async prepare/execute error cannot be phase-attributed, it STAYS `dispatched: true` (the safe direction) and the delta records the refinement as PG-only)
- Modify: `engine/crates/ferrod/src/services/fate.rs` (the arm + tables), `engine/crates/ferrod/src/services/sql.rs:1178` (`send_err_to_pool_error`: `LinkLost => ConnectionLost { dispatched: true }`)
- Create: `engine/crates/ferro-backend-pg/tests/pre_dispatch_fate_it.rs`

**Interfaces:**
- Produces: `PoolError::ConnectionLost { dispatched: bool }`; `PoolError::undispatched(self) -> PoolError` (marks a `ConnectionLost` as provably-pre-dispatch; identity on every other variant).
- Consumes: Task 7's `OpContext` (4 fields). New classify rule: Indeterminate iff `ctx.sent && dispatched && !ctx.readonly && !ctx.in_tx` (then Task 7's post-filter on top).

- [ ] **Step 1: Write the failing tests**

In `fate.rs`'s `mod tests`, add (and expect the whole file to stop compiling — that is the site enumeration):

```rust
    /// M1-S9a finding 3: the stream-OPEN and buffered paths PRE-BUILD `ctx.sent = true`, but a
    /// PREPARE-phase loss (Parse/Describe only — the Execute never sent) is a provable
    /// did-not-apply. The error now carries the phase; a `dispatched: false` loss is Retryable
    /// even under a sent-true ctx. This is the reproduced-live false-Indeterminate (and the
    /// mechanism behind M1-S8b's unreproduced pre-HEAD sighting).
    #[test]
    fn a_pre_dispatch_loss_is_retryable_even_when_ctx_says_sent() {
        let ep = classify_fate(
            PoolError::ConnectionLost { dispatched: false },
            ctx(false, true, false),
        );
        assert_eq!(ep.code, errc::CONNECTION_LOST);
        assert_eq!(ep.branch, branch::RETRYABLE);
        assert_ne!(ep.branch, branch::INDETERMINATE);
    }

    /// The conservative default direction: a dispatched (or unattributed) loss on a sent write
    /// stays Indeterminate — the refinement may only ever SHRINK the Indeterminate set.
    #[test]
    fn a_dispatched_loss_on_a_sent_write_stays_indeterminate() {
        let ep = classify_fate(
            PoolError::ConnectionLost { dispatched: true },
            ctx(false, true, false),
        );
        assert_eq!(ep.code, errc::WRITE_UNCONFIRMED);
        assert_eq!(ep.branch, branch::INDETERMINATE);
    }
```

Create `engine/crates/ferro-backend-pg/tests/pre_dispatch_fate_it.rs` (mirrors `pg_pool_it.rs`'s setup — `PgBackend::new(url)`, its `test_url()`/`config()` helpers copied):

```rust
//! M1-S9a Task 9 — the live proof: a statement on a connection that died BEFORE dispatch is a
//! provable did-not-apply, and the error now says so. This is the M0-review repro
//! (`m0_fate_probe_it.rs`, run green and deleted), promoted to a committed guard.

use std::time::Duration;

use ferro_backend_pg::{PgBackend, Value};
use ferro_pool::config::PoolConfig;
use ferro_pool::error::PoolError;
use ferro_pool::pool::Pool;

fn test_url() -> Option<String> {
    std::env::var("FERRO_TEST_PG_URL").ok().filter(|s| !s.is_empty())
}

fn config(max_size: usize) -> PoolConfig {
    PoolConfig {
        max_size,
        checkout_timeout: Duration::from_secs(5),
        ..PoolConfig::default()
    }
}

async fn raw_connect(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("raw side connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn a_conn_dead_before_dispatch_is_undispatched_connection_lost_and_the_write_unapplied() {
    let Some(url) = test_url() else {
        eprintln!("skip: FERRO_TEST_PG_URL not set");
        return;
    };
    let side = raw_connect(&url).await;
    side.simple_query("CREATE TABLE IF NOT EXISTS ferro_s9a_predispatch (k text)")
        .await
        .expect("setup ddl");

    let pool = Pool::new(PgBackend::new(url.clone()), config(1));
    let mut co = pool.checkout().await.expect("checkout");

    // Learn the pinned conn's pid THROUGH the checkout, then kill it from the side.
    let pid_res = co.query("SELECT pg_backend_pid()", &[]).await.expect("pid");
    let Value::I64(pid) = pid_res.rows[0][0] else {
        panic!("pg_backend_pid must read back as I64, got {:?}", pid_res.rows[0][0]);
    };
    side.execute("SELECT pg_terminate_backend($1)", &[&(pid as i32)])
        .await
        .expect("terminate");
    // Wait until the server has really torn the session down (never sleep-and-hope).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let rows = side
            .query("SELECT 1 FROM pg_stat_activity WHERE pid = $1", &[&(pid as i32)])
            .await
            .expect("pg_stat_activity");
        if rows.is_empty() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "backend never died");
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // The write on the dead conn: the PREPARE round trip fails — the INSERT's Execute was never
    // sent. The error must say `dispatched: false`, and §19.3 then reads Retryable, not
    // Indeterminate.
    let key = format!("k_{}_{}", std::process::id(), std::time::UNIX_EPOCH.elapsed().map(|d| d.as_nanos()).unwrap_or(0));
    let err = co
        .query(
            &format!("INSERT INTO ferro_s9a_predispatch VALUES ('{key}')"),
            &[],
        )
        .await
        .expect_err("a query on a terminated backend must fail");
    assert_eq!(
        err,
        PoolError::ConnectionLost { dispatched: false },
        "a prepare-phase loss is a PROVABLE did-not-apply"
    );

    // And provably unapplied.
    let n = side
        .query_one(
            &format!("SELECT count(*) FROM ferro_s9a_predispatch WHERE k = '{key}'"),
            &[],
        )
        .await
        .expect("read-back");
    assert_eq!(n.get::<_, i64>(0), 0, "the write must not have applied");
}
```

(`std::time::UNIX_EPOCH.elapsed()` — if the helper reads awkwardly, reuse Task 1's `unique_key` shape locally; the uniqueness is what matters.)

- [ ] **Step 2: Run to verify failure**

`cargo build -p ferro-pool` → COMPILE ERRORS at every construction/pattern site (hazard 10's list). Work through them with the per-site values from **Files** above. Then `cargo test -p ferrod --lib services::fate` → `a_pre_dispatch_loss_is_retryable_even_when_ctx_says_sent` FAILS (before the classify arm changes) — RED observed.

- [ ] **Step 3: Implement**

`error.rs`:

```rust
    /// The backend connection died (transport failure, FATAL/PANIC session end, driver task
    /// gone). `dispatched` records whether the USER STATEMENT this error is being reported for
    /// had already been TRANSMITTED when the loss surfaced:
    /// - `true` (the DEFAULT direction for any site that cannot attribute the phase): the
    ///   statement may be executing — Indeterminate-eligible on a sent write (§19.3).
    /// - `false` (PROVABLE pre-dispatch only — a connect failure, or a PREPARE-phase loss where
    ///   Parse/Describe failed and the Execute never left the process): a known did-not-apply,
    ///   always Retryable, never Indeterminate.
    /// The asymmetry is deliberate: a wrong `true` cries wolf; a wrong `false` licenses replay
    /// of a possibly-applied write. Only mark `false` where non-dispatch is provable.
    #[error("connection to backend lost (dispatched: {dispatched})")]
    ConnectionLost { dispatched: bool },
```

```rust
    /// Mark a `ConnectionLost` as PROVABLY pre-dispatch (see the variant doc); identity on every
    /// other variant, so `error_map::map(&e).undispatched()` composes at the phase-aware call
    /// site without re-deriving the classification.
    pub fn undispatched(self) -> PoolError {
        match self {
            PoolError::ConnectionLost { .. } => PoolError::ConnectionLost { dispatched: false },
            other => other,
        }
    }
```

`taxonomy_branch`/`errc` patterns become `PoolError::ConnectionLost { .. }`. `fate.rs`'s arm:

```rust
        PoolError::ConnectionLost { dispatched } => {
            if ctx.sent && dispatched && !ctx.readonly && !ctx.in_tx {
                payload(/* WRITE_UNCONFIRMED, unchanged message */)
            } else {
                payload(/* CONNECTION_LOST, message gains: "…, or the loss preceded dispatch" */)
            }
        }
```

Every other site per the **Files** list; the 57014-override's `sent=false` arm's doc note stays valid. Existing tests: every `PoolError::ConnectionLost` literal in test code becomes `{ dispatched: true }` EXCEPT checkout-failure fixtures, which become `{ dispatched: false }` — the (code, branch) expectations all stay identical because `ctx.sent` already gated those rows.

- [ ] **Step 4: Run to verify pass**

Offline: `cargo test --workspace`. Live: `cargo test -p ferro-backend-pg --test pre_dispatch_fate_it --test pg_pool_it --test pg_query_it` and the full `ferrod` live set (`in_tx_fate_it` included — Task 1/8's guards must not move; their losses are execute-phase, `dispatched: true`). Also `php` untouched: the wire payload for the refined case is the EXISTING `CONNECTION_LOST{Retryable}` — no client change.

- [ ] **Step 5: NAMED MUTATIONS**

1. Remove `.undispatched()` from the BUFFERED prepare site (`query.rs:69-71`) → the live `pre_dispatch_fate_it` test goes RED (`assert_eq!(err, ConnectionLost { dispatched: false })` sees `true`). Restore.
2. In `fate.rs`, drop `dispatched &&` from the condition → `a_pre_dispatch_loss_is_retryable_even_when_ctx_says_sent` goes RED. Restore.
3. Flip `error_map::map`'s default to `dispatched: false` → `a_dispatched_loss_on_a_sent_write_stays_indeterminate` stays green (unit fixture) but the LIVE chaos suite's `ConnectionLost`-Indeterminate rows (`chaos_fate_it`'s kill-mid-write) go RED — run it to confirm, restore. (This mutation is what proves the default direction is load-bearing, not decorative.)

- [ ] **Step 6: Delta ledger + commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md`:

```markdown
### Task 9
- §19.3: ConnectionLost carries the dispatch phase; a provable pre-dispatch loss is Retryable
  even on a declared write. CLOSES the §19.3 OPEN note (spec line ~489): the pre-HEAD
  Indeterminate terminal was the stream OPEN's prepare-phase loss under a pre-built sent:true.
- §22.2: MySQL phase-attribution recorded as done/not-done per the bind.rs verification.
```

```bash
git add engine/crates docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "fix(m1-s9a): ConnectionLost{dispatched} — a pre-dispatch death is Retryable, closing the false-Indeterminate and the S8b pre-HEAD sighting"
```

---

## Task 10: The post-cancel drain is bounded — the terminal always arrives, the conn always re-enters tainted

On a timeout/cancel win, both EXEC paths do `cancel_handle.cancel().await; (&mut query_fut).await` with no bound (hazard 14): against a wedged backend the handler parks forever holding its permit and NEVER declares a terminal — charter rule 4 broken. Bound the drain with `CANCEL_DRAIN_BUDGET = 5s` (the `VERSION_DRAIN_BUDGET` precedent); on expiry the dropped-mid-statement conn is TAINTED (hazard 15) so the bounded recycle resets or evicts it.

**Files:**
- Modify: `engine/crates/ferrod/src/services/sql.rs` (`CANCEL_DRAIN_BUDGET`; `run_autocommit_exec` — inner-scope the pinned future so the taint can live INSIDE the fn; its `mod tests`)
- Modify: `engine/crates/ferrod/src/tx/actor.rs` (the `ExecStep::Deadline`/`Abort` drains; its `mod tests`)
- Modify: `engine/crates/ferro-pool/src/fake.rs` (the cancel-immune `wedge_queries` gate — `block_query`'s gate IS released by the fake cancel, so it cannot model a wedged backend, hazard 16)

**Interfaces:**
- Produces: `pub(crate) const CANCEL_DRAIN_BUDGET: Duration = Duration::from_secs(5);` (in `services/sql.rs`, imported by `tx/actor.rs`); `FakeBackend::{wedge_queries(&self), release_wedged_queries(&self), wedged_queries_waiting(&self) -> u64}`.
- Consumes: `stream_cancel_error()` (the existing 57014 mint, `sql.rs:1157`); `Checkout::{set_tainted, tainted}`; Task 8's `ExecReply::Deadline { tx_writes_persisted }` shape (this task edits the same arms — hence the ordering).

- [ ] **Step 1: Write the failing tests**

`fake.rs` knob first (it is test infrastructure): field `wedge_gate: Mutex<Option<Arc<Notify>>>` + `wedged_queries_waiting: AtomicU64` (init in `new()`); methods mirroring `block_pings`; in `PoolBackend::query`, park on `wedge_gate` BEFORE the `query_gate` check — and the `FakeCancelHandle` must NOT touch it (that is its whole point):

```rust
    /// Arms every subsequent `query()` to park on a gate the fake CANCEL does NOT release —
    /// models a WEDGED backend (accepts TCP, never answers, ignores CancelRequest), the
    /// finding-4c shape. `block_query`'s gate is released by `FakeCancelHandle::cancel`, so it
    /// models a HEALTHY backend honoring a cancel; this models the opposite.
    pub fn wedge_queries(&self) {
        *self.wedge_gate.lock().unwrap() = Some(Arc::new(Notify::new()));
    }
```

Then in `sql.rs`'s `mod tests`:

```rust
    /// M1-S9a finding 4c: the post-cancel drain is BOUNDED, returns the 57014 cancel shape, and
    /// taints the checkout (the dropped-mid-statement conn must never re-enter the pool clean —
    /// `Checkout::query`'s own Err-arm fail-safe never ran, the future was dropped).
    #[tokio::test(start_paused = true)]
    async fn a_wedged_post_cancel_drain_is_bounded_and_taints_the_conn() {
        let backend = ferro_pool::fake::FakeBackend::new();
        backend.wedge_queries();
        let pool = ferro_pool::pool::Pool::new(
            backend,
            ferro_pool::config::PoolConfig {
                max_size: 1,
                checkout_timeout: Duration::from_secs(5),
                reap_interval: None,
                ..ferro_pool::config::PoolConfig::default()
            },
        );
        let mut co = pool.checkout().await.expect("checkout");
        assert!(!co.tainted());

        let cancel = CancellationToken::new();
        let (result, _exec_us) = tokio::time::timeout(
            Duration::from_secs(600),
            run_autocommit_exec(&mut co, "UPDATE t SET x = 1", &[], Some(10), &cancel),
        )
        .await
        .expect("BOUND EXCEEDED: the post-cancel drain is unbounded (finding 4c)");

        assert!(
            matches!(&result, Err(PoolError::Sql { sqlstate: Some(s), .. }) if s == "57014"),
            "a wedged drain reports the cancel shape (write→Indeterminate via the override), got {result:?}"
        );
        assert!(
            co.tainted(),
            "the conn was dropped mid-statement — it MUST re-enter the pool tainted"
        );
    }
```

And in `actor.rs`'s `mod tests`:

```rust
    /// M1-S9a finding 4c, tx side: a wedged in-tx drain still replies Deadline within the budget
    /// (the handler's terminal — charter rule 4), and the conn never reaches the next tenant
    /// un-reset: teardown's bounded ROLLBACK wedges too (block_simple_query), taints, and the
    /// next checkout EVICTS it (a fresh conn is dialed — observable as total_connected == 2).
    #[tokio::test(start_paused = true)]
    async fn a_wedged_tx_drain_replies_within_budget_and_the_conn_never_reaches_a_tenant() {
        let backend = FakeBackend::new();
        backend.wedge_queries();
        backend.block_simple_query(); // the teardown ROLLBACK wedges as a real wedged backend's would
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        // NOTE: spawn_actor passes a 600s teardown bound — fine: it is BOUNDED, and paused time
        // auto-advances through it.
        let (_tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        let (r_tx, r_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "UPDATE t SET x = 1".into(),
                params: vec![],
                timeout_ms: Some(10),
                cancel: CancellationToken::new(),
                reply: r_tx,
            })
            .await
            .expect("send");

        let reply = tokio::time::timeout(Duration::from_secs(600), r_rx)
            .await
            .expect("BOUND EXCEEDED: the tx post-cancel drain is unbounded (finding 4c)")
            .expect("reply arrives");
        assert!(matches!(reply, ExecReply::Deadline { .. }));

        // Wait for teardown to finish (done flips true), then prove the wedged conn is evicted,
        // not reused: the next checkout dials a SECOND conn.
        done_rx.changed().await.expect("done");
        assert!(*done_rx.borrow());
        let co2 = tokio::time::timeout(Duration::from_secs(600), pool.checkout())
            .await
            .expect("bounded")
            .expect("checkout after teardown");
        assert_eq!(
            pool.backend().total_connected(),
            2,
            "the mid-statement conn must be evicted by the bounded recycle, never handed out"
        );
        drop(co2);
    }
```

- [ ] **Step 2: Run to verify failure**

`cargo test -p ferrod --lib a_wedged` → both new tests FAIL at `BOUND EXCEEDED` (the drains are unbounded).

- [ ] **Step 3: Implement**

In `sql.rs`:

```rust
/// M1-S9a (finding 4c): bound on the post-cancel DRAIN — the fire-the-out-of-band-cancel +
/// await-the-query sequence — on BOTH EXEC paths. Precedent: `pools.rs`'s `VERSION_DRAIN_BUDGET`
/// (5s) bounds the identical shape on the version probe, and 5s is long enough for any healthy
/// backend to answer its own CancelRequest while freeing the permit ~25x sooner than the OS TCP
/// timeout a wedged backend otherwise imposes. On expiry the query future is DROPPED
/// mid-statement: the conn is TAINTED (the bounded recycle then resets or evicts it) and the
/// drained result is the engine-minted 57014 cancel shape — `classify_fate`'s override then
/// routes it exactly like a completed drain (write→Indeterminate, read→Cancelled): honest, the
/// outcome is genuinely unobserved.
pub(crate) const CANCEL_DRAIN_BUDGET: Duration = Duration::from_secs(5);
```

`run_autocommit_exec` — confine the pinned future to an inner block so the taint compiles inside the fn (NLL: the `&mut co` borrow ends with the block), keeping the `(result, exec_us)` return shape:

```rust
    let cancel_handle = co.cancel_handle();
    let exec_start = Instant::now();
    let (result, wedged) = {
        let query_fut = co.query(sql, params);
        tokio::pin!(query_fut);
        let mut wedged = false;
        let result = tokio::select! {
            biased;
            r = &mut query_fut => r,
            () = sleep_opt(timeout_ms) => {
                match tokio::time::timeout(CANCEL_DRAIN_BUDGET, async {
                    cancel_handle.cancel().await;
                    (&mut query_fut).await
                })
                .await
                {
                    Ok(r) => r,
                    Err(_elapsed) => {
                        wedged = true;
                        Err(stream_cancel_error())
                    }
                }
            }
            () = cancel.cancelled() => {
                match tokio::time::timeout(CANCEL_DRAIN_BUDGET, async {
                    cancel_handle.cancel().await;
                    (&mut query_fut).await
                })
                .await
                {
                    Ok(r) => r,
                    Err(_elapsed) => {
                        wedged = true;
                        Err(stream_cancel_error())
                    }
                }
            }
        };
        (result, wedged)
    };
    if wedged {
        // The drain budget expired: the query future was DROPPED mid-statement, so the
        // instrumented Err-arm fail-safe never ran — NOTHING else taints this conn. Without this
        // line the next tenant inherits a mid-protocol connection (charter rule 6).
        co.set_tainted(true);
    }
    (result, exec_start.elapsed().as_micros() as u64)
```

In `actor.rs`, the two arms (Task 8's flag lines stay exactly where Task 8 put them):

```rust
                    ExecStep::Deadline => {
                        // BOUNDED (M1-S9a finding 4c). On expiry the future is dropped
                        // mid-statement; the taint mechanism here is TEARDOWN, which runs
                        // immediately below via the break: its bounded ROLLBACK wedges or errors
                        // on the desynced conn → `set_tainted(true)` → the recycle evicts. The
                        // reply is sent REGARDLESS — the handler's terminal must not depend on a
                        // wedged backend answering (charter rule 4).
                        let _ = tokio::time::timeout(
                            crate::services::sql::CANCEL_DRAIN_BUDGET,
                            async {
                                cancel_handle.cancel().await;
                                let _ = (&mut query_fut).await;
                            },
                        )
                        .await;
                        if hazard {
                            tx_writes_persisted = true;
                        }
                        let _ = reply.send(ExecReply::Deadline {
                            tx_writes_persisted: hazard,
                        });
                        break 'actor TxEnd::Deadline;
                    }
```

(`ExecStep::Abort` mirrors it, keeping `drop(reply)`.)

- [ ] **Step 4: Run to verify pass**

Offline `cargo test -p ferrod` + `cargo test -p ferro-pool`. Live regression: `cargo test -p ferrod --test chaos_fate_it --test mysql_chaos_it --test in_tx_fate_it --test tx_it` — the HEALTHY-backend cancel/timeout behavior must be byte-identical (their drains complete inside the budget; every fate row unchanged). That suite is the regression proof that the budget did not change any completed-drain fate — the sound-list Err-arm fail-safes are exercised by the same runs.

- [ ] **Step 5: NAMED MUTATIONS**

1. In `run_autocommit_exec`, delete `co.set_tainted(true)` under `wedged` → `a_wedged_post_cancel_drain_is_bounded_and_taints_the_conn` goes RED on the `co.tainted()` assert. Restore.
2. Remove the `tokio::time::timeout(CANCEL_DRAIN_BUDGET, ...)` wrapper on the autocommit timeout arm (restore the bare drain) → same test RED at `BOUND EXCEEDED`. Restore.
3. Remove the wrapper in the actor's `Deadline` arm → `a_wedged_tx_drain_replies_within_budget_...` RED at `BOUND EXCEEDED`. Restore.

- [ ] **Step 6: Delta ledger + commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md`:

```markdown
### Task 10
- §5.2/§19.3: the post-cancel drain is bounded (5s); a wedged drain still yields exactly one
  terminal (57014 shape → the standard override routing) and a tainted conn. Charter rule 4
  restored on the wedged-backend path.
```

```bash
git add engine/crates/ferrod engine/crates/ferro-pool docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "fix(m1-s9a): bound the post-cancel drain — the terminal always arrives and a wedged conn re-enters tainted"
```

---
## Task 11: The availability knobs — `max_connections`, `frame_read_timeout`, `idle_timeout`

One local client can pin the host-wide daemon (hazard 17). Task 5 killed the memory amplification; this task caps the connection count, puts a deadline on a STALLED partial frame, and gives operators an (off-by-default) idle reaper. **The defaults are the design** (hazard 18): `max_connections = 1024` (PHP-FPM totals per host are low hundreds; 1024 × ≤64 KiB partial buffers ≈ ≤64 MiB worst case; standard under systemd `LimitNOFILE`); `frame_read_timeout = 30s` ON (a legit local UDS frame completes in microseconds — 30s tolerates pathological starvation while bounding hostage-holding); `idle_timeout = DISABLED` (the sync PHP client cannot ping while blocked between requests, so ANY nonzero default severs every quiet worker on the host — the new-outage-class the brief warns about; the knob exists for operators who know their fleet).

**Files:**
- Modify: `engine/crates/ferrod/src/config.rs` (three fields + env parsing + defaults + tests)
- Modify: `engine/crates/ferrod/src/session/codec.rs` (`ReadProgress` + `FrameCodec::with_progress`; `pub struct FrameCodec;` becomes a `Default` struct — the two construction sites become `FrameCodec::default()`)
- Modify: `engine/crates/ferrod/src/session/mod.rs` (the tick arm: stall + idle enforcement)
- Modify: `engine/crates/ferrod/src/serve.rs` (the accept-time cap; `FrameCodec::default()` at `:142`)
- Modify: `engine/crates/ferrod/src/session/error.rs` (`SessionError::overloaded`)
- Create: `engine/crates/ferrod/tests/availability_it.rs` (offline — no database needed)

**Interfaces:**
- Produces: `Config { max_connections: usize, frame_read_timeout: Duration, idle_timeout: Option<Duration>, .. }` (env: `FERRO_MAX_CONNECTIONS`, `FERRO_FRAME_READ_TIMEOUT_MS`, `FERRO_IDLE_TIMEOUT_MS` — `0`/unset ⇒ disabled for the last); `session::codec::ReadProgress { started: AtomicU64, completed: AtomicU64 }`; `FrameCodec::with_progress(Arc<ReadProgress>) -> FrameCodec`; `SessionError::overloaded(message: String) -> SessionError` (a `Fatal` carrying `POOL_TIMEOUT`/`POOL_TIMEOUT_BRANCH` — hazard 23: an existing registry pairing, NO /proto change).
- Consumes: Task 5's codec (sequential owner); `serve`'s `sessions: JoinSet<()>` (`JoinSet::len()` is the live count, hazard 20); the reader loop's `supervisors: JoinSet<()>` (`is_empty()` is the in-flight veto — a completed-but-unreaped supervisor delays an idle close by one tick, which is harmless).

- [ ] **Step 1: Write the failing tests**

Create `engine/crates/ferrod/tests/availability_it.rs`:

```rust
//! M1-S9a Task 11 — the availability knobs (finding 5). All offline: the attacks are wire-shaped,
//! no database involved. Timing here is REAL (the sessions do real socket IO), so the configs use
//! sub-second knobs and the assertions use generous multiples — never equality on elapsed time.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{connect, spawn_one_session_with_config, spawn_serve_with_config};
use ferro_proto::consts::{MAX_FRAME_PAYLOAD, errc, flags};
use ferro_proto::header::Header;
use ferro_proto::messages::Outcome;
use ferrod::config::Config;
use ferrod::epoch::{EpochSource, RandomEpoch};
use ferrod::shutdown::Drain;

/// The Unsupported-stub handler shape every pure-session test uses (there is no shared
/// `default_handler` in `common/` — each test file defines its own; this one answers every
/// request with an empty Ok terminal so the session-liveness properties are isolated).
fn stub_handler() -> ferrod::session::HandlerFn {
    use futures::FutureExt;
    Arc::new(|_frame, responder, _cancel| {
        async move {
            responder.end_ok(bytes::Bytes::new());
        }
        .boxed()
    })
}

fn header_bytes(payload_len: u32) -> Vec<u8> {
    Header {
        flags: 0,
        service: 0,
        method: 0,
        request_id: 9,
        payload_len,
    }
    .encode()
    .to_vec()
}

/// finding 5c: a partial frame that stops making progress is session-fatal within
/// `frame_read_timeout` (+ the 1s tick granularity) — the connection can no longer be held
/// hostage by a 17-byte send.
#[tokio::test]
async fn a_stalled_partial_frame_is_session_fatal_within_the_deadline() {
    let config = Config {
        frame_read_timeout: Duration::from_millis(200),
        ..Config::default()
    };
    let epoch = RandomEpoch.epoch();
    let (sock, _task) = spawn_one_session_with_config(config, epoch, stub_handler());
    let mut c = connect(&sock).await;
    c.hello(1).await;

    // A header declaring the 16 MiB maximum, one body byte, then silence.
    let mut bytes = header_bytes(MAX_FRAME_PAYLOAD);
    bytes.push(0);
    c.send_raw_bytes(&bytes).await;

    // Within tick(1s) + deadline(200ms) + slack: ONE rid=0 fatal frame, then EOF.
    let fatal = c
        .recv_or_none(Duration::from_secs(4))
        .await
        .expect("the stalled partial frame must be answered with a session-fatal frame");
    assert_eq!(fatal.header.request_id, 0);
    assert_eq!(fatal.header.flags & flags::END, flags::END);
    match Outcome::decode(&fatal.payload).expect("Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::PROTOCOL),
        other => panic!("expected Outcome::Error, got {other:?}"),
    }
    c.recv_eof().await;
}

/// finding 5c, the OTHER half: `idle_timeout` defaults DISABLED — a quiet-but-healthy worker
/// connection (the normal PHP-FPM shape) is NEVER reaped by default.
#[tokio::test]
async fn idle_timeout_defaults_off_and_a_quiet_session_survives() {
    assert!(
        Config::default().idle_timeout.is_none(),
        "idle_timeout MUST default disabled: the sync PHP client cannot ping while blocked \
         between requests, so a nonzero default severs every quiet worker on the host"
    );

    let epoch = RandomEpoch.epoch();
    let (sock, _task) =
        spawn_one_session_with_config(Config::default(), epoch, stub_handler());
    let mut c = connect(&sock).await;
    c.hello(1).await;
    tokio::time::sleep(Duration::from_millis(2500)).await; // several ticks of silence
    // Still alive: a PING answers.
    c.ping(2, 77).await;
}

/// When an operator DOES enable it, an idle session with nothing in flight closes silently, and
/// a session with an in-flight request is NOT idle (streaming consumes no inbound frames).
#[tokio::test]
async fn an_enabled_idle_timeout_reaps_quiet_sessions_but_never_inflight_ones() {
    use futures::FutureExt;
    let config = Config {
        idle_timeout: Some(Duration::from_millis(300)),
        ..Config::default()
    };
    let epoch = RandomEpoch.epoch();

    // (a) quiet session: closed within tick + idle + slack.
    let (sock, _task) =
        spawn_one_session_with_config(config.clone(), epoch, stub_handler());
    let mut quiet = connect(&sock).await;
    quiet.hello(1).await;
    tokio::time::timeout(Duration::from_secs(4), quiet.recv_eof())
        .await
        .expect("an enabled idle_timeout must close a quiet session");

    // (b) in-flight session: a handler that takes 2.5s to declare its terminal keeps the session
    // alive well past the 300ms idle knob — in-flight work vetoes the idle close.
    let slow: ferrod::session::HandlerFn = Arc::new(|_frame, responder, _cancel| {
        async move {
            tokio::time::sleep(Duration::from_millis(2500)).await;
            responder.end_ok(bytes::Bytes::new());
        }
        .boxed()
    });
    let (sock2, _task2) = spawn_one_session_with_config(config, epoch, slow);
    let mut busy = connect(&sock2).await;
    busy.hello(1).await;
    busy
        .send_request(2, ferro_proto::consts::service::SQL, 1, Vec::new())
        .await;
    let t = tokio::time::timeout(Duration::from_secs(6), busy.recv())
        .await
        .expect("the in-flight request's terminal must arrive — idle reaping must not cut it");
    assert_eq!(t.header.request_id, 2);
}

/// finding 5b: the `max_connections` cap — connection N+1 gets one loud, RETRYABLE frame
/// (POOL_TIMEOUT — an existing registry pairing, no /proto change) and a close; it never becomes
/// a session.
#[tokio::test]
async fn the_connection_cap_rejects_the_overflow_connection_loudly() {
    let config = Config {
        max_connections: 1,
        ..Config::default()
    };
    let epoch = RandomEpoch.epoch();
    let drain = Drain::new();
    let (sock, _task) = spawn_serve_with_config(config, epoch, drain, stub_handler());

    let mut first = connect(&sock).await;
    first.hello(1).await; // occupies the one slot

    let mut second = connect(&sock).await;
    let frame = second
        .recv_or_none(Duration::from_secs(3))
        .await
        .expect("the overflow connection must be answered, not silently dropped (SPEC G-4)");
    assert_eq!(frame.header.request_id, 0);
    match Outcome::decode(&frame.payload).expect("Outcome") {
        Outcome::Error(ep) => {
            assert_eq!(ep.code, errc::POOL_TIMEOUT, "retryable overload, got {:#06x}", ep.code);
        }
        other => panic!("expected Outcome::Error, got {other:?}"),
    }
    second.recv_eof().await;

    // The first session is untouched.
    first.ping(2, 42).await;
}
```

(`TestClient::ping` asserts its PONG internally — see `common/mod.rs:458`. The slow-handler closure in test (b) reuses the exact `stub_handler` shape with a sleep prepended.)

- [ ] **Step 2: Run to verify failure**

`cargo test -p ferrod --test availability_it` → COMPILE ERROR (`frame_read_timeout`/`idle_timeout`/`max_connections` fields do not exist). After adding the Config fields alone, the stall/cap tests FAIL (no reply, `expect` on `recv_or_none` panics) — RED observed.

- [ ] **Step 3: Implement**

**(a) `config.rs`:**

```rust
/// Default cap on concurrent client connections (M1-S9a finding 5b). PHP-FPM worker totals per
/// host are low hundreds; 1024 leaves headroom for multi-app hosts while bounding fds and (with
/// the codec's 64 KiB reserve step) worst-case partial-frame memory at ~64 MiB. Overflow
/// connections get ONE loud POOL_TIMEOUT{Retryable} frame, never a silent drop.
const DEFAULT_MAX_CONNECTIONS: usize = 1024;

/// Default deadline for a PARTIAL frame to make progress (M1-S9a finding 5c). A legitimate local
/// UDS frame completes in microseconds; 30s tolerates pathological scheduler starvation while
/// ending the hold-a-16MiB-header-hostage shape. Enforced at 1s tick granularity.
const DEFAULT_FRAME_READ_TIMEOUT: Duration = Duration::from_secs(30);
```

Fields on `Config` (+ `Default` + `from_env`):

```rust
    /// Max concurrent client connections. From `FERRO_MAX_CONNECTIONS`, default 1024.
    pub max_connections: usize,
    /// Deadline for a partial inbound frame to complete. From `FERRO_FRAME_READ_TIMEOUT_MS`,
    /// default 30s.
    pub frame_read_timeout: Duration,
    /// Idle-session reaping. `None` (the DEFAULT — from `FERRO_IDLE_TIMEOUT_MS`, where unset or
    /// `0` means disabled): the sync PHP client cannot ping while blocked between requests, so a
    /// nonzero default would sever every quiet worker on the host. A session is idle only when
    /// BOTH no inbound frame has arrived for the duration AND nothing is in flight.
    pub idle_timeout: Option<Duration>,
```

```rust
        if let Ok(v) = std::env::var("FERRO_MAX_CONNECTIONS")
            && let Ok(n) = v.trim().parse::<usize>()
            && n > 0
        {
            cfg.max_connections = n;
        }
        if let Ok(v) = std::env::var("FERRO_FRAME_READ_TIMEOUT_MS")
            && let Ok(ms) = v.trim().parse::<u64>()
            && ms > 0
        {
            cfg.frame_read_timeout = Duration::from_millis(ms);
        }
        if let Ok(v) = std::env::var("FERRO_IDLE_TIMEOUT_MS")
            && let Ok(ms) = v.trim().parse::<u64>()
        {
            cfg.idle_timeout = (ms > 0).then(|| Duration::from_millis(ms));
        }
```

**(b) `codec.rs`:**

```rust
/// Shared decode-progress counters (M1-S9a finding 5c). The codec bumps `started` ONCE when a
/// frame goes partial (header seen, body incomplete) and `completed` when that frame finishes;
/// the session's tick arm reads the pair — an unchanged `started > completed` for longer than
/// `frame_read_timeout` is a stalled partial frame ⇒ session-fatal. Frames that arrive whole in
/// one read never touch the counters.
#[derive(Debug, Default)]
pub struct ReadProgress {
    pub started: std::sync::atomic::AtomicU64,
    pub completed: std::sync::atomic::AtomicU64,
}

#[derive(Default)]
pub struct FrameCodec {
    progress: Option<std::sync::Arc<ReadProgress>>,
    /// True while the current frame is partially buffered — makes `started` fire once per frame.
    mid_frame: bool,
}

impl FrameCodec {
    pub fn with_progress(progress: std::sync::Arc<ReadProgress>) -> Self {
        FrameCodec {
            progress: Some(progress),
            mid_frame: false,
        }
    }
}
```

and in `decode` (around Task 5's capped reserve):

```rust
        if src.len() < need {
            if !self.mid_frame {
                self.mid_frame = true;
                if let Some(p) = &self.progress {
                    p.started.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            src.reserve((need - src.len()).min(PARTIAL_FRAME_RESERVE_STEP));
            return Ok(None);
        }
        if self.mid_frame {
            self.mid_frame = false;
            if let Some(p) = &self.progress {
                p.completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
```

The two construction sites (`session/mod.rs:265`, `serve.rs:142`) become `FrameCodec::default()`; the session's becomes `FrameCodec::with_progress(...)` below.

**(c) `session/mod.rs`:** create the progress handle + tick before the reader loop:

```rust
        let progress = Arc::new(ReadProgress::default());
        let framed = Framed::new(stream, FrameCodec::with_progress(Arc::clone(&progress)));
```

```rust
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // (started counter value, first Instant it was seen ahead of completed)
        let mut stall_since: Option<(u64, tokio::time::Instant)> = None;
        let mut last_frame_at = tokio::time::Instant::now();
```

New select arm in the reader loop (alongside the supervisors-reap arm):

```rust
                _ = tick.tick() => {
                    use std::sync::atomic::Ordering;
                    let s = progress.started.load(Ordering::Relaxed);
                    let c = progress.completed.load(Ordering::Relaxed);
                    if s > c {
                        match stall_since {
                            Some((seen, since)) if seen == s => {
                                if since.elapsed() >= config.frame_read_timeout {
                                    let fatal = SessionError::protocol_fatal(
                                        "partial frame stalled past frame_read_timeout",
                                    )
                                    .into_out_frame();
                                    let _ = control_tx.send(ControlMsg::bare(fatal)).await;
                                    break;
                                }
                            }
                            _ => stall_since = Some((s, tokio::time::Instant::now())),
                        }
                    } else {
                        stall_since = None;
                    }
                    if let Some(idle) = config.idle_timeout
                        && last_frame_at.elapsed() >= idle
                        && supervisors.is_empty()
                    {
                        // A quiet session with nothing in flight: close silently — the client's
                        // resilience loop reconnects on next use. NEVER while anything is in
                        // flight (a streaming response consumes no inbound frames).
                        break;
                    }
                    continue;
                }
```

and set `last_frame_at = tokio::time::Instant::now();` where a `Classification::Frame` is obtained (right before the CANCEL check).

**(d) `serve.rs` + `error.rs`:** the cap check FIRST in the accept arm (cheapest decision, protects fds fastest):

```rust
            accepted = listener.accept() => {
                let stream = match accepted { /* unchanged */ };

                if sessions.len() >= config.max_connections {
                    tracing::warn!(
                        limit = config.max_connections,
                        "connection limit reached: rejecting new connection"
                    );
                    deny_connection(
                        stream,
                        SessionError::overloaded(format!(
                            "connection limit ({}) reached; retry shortly",
                            config.max_connections
                        )),
                    )
                    .await;
                    continue;
                }
                /* peercred gate + spawn, unchanged */
```

`error.rs`:

```rust
    /// M1-S9a finding 5b: the accept-time connection-cap rejection. Reuses POOL_TIMEOUT
    /// (Retryable — "resource momentarily unavailable, retry per your policy"); a dedicated
    /// ERR_OVERLOADED wire code is a recorded, deferred /proto candidate (charter rule 2).
    pub fn overloaded(detail: impl Into<String>) -> Self {
        SessionError::Fatal(error_payload(
            errc::POOL_TIMEOUT,
            errc::POOL_TIMEOUT_BRANCH,
            detail,
        ))
    }
```

- [ ] **Step 4: Run to verify pass**

`cargo test -p ferrod --test availability_it` → 4 green. Then the sound-list regression: `cargo test -p ferrod --lib` + `--test session_rules` + `--test shutdown` + the codec vector tests — the session state machine and exactly-one-END suites must be untouched-green (the tick arm `continue`s and the two new `break`s route through the EXISTING cleanup path, adding no new terminal-less exit).

- [ ] **Step 5: NAMED MUTATIONS**

1. Delete the cap check in `serve.rs` → `the_connection_cap_rejects_the_overflow_connection_loudly` RED (the second client's HELLO would succeed / no frame arrives). Restore.
2. In the tick arm, delete the stall `break` (keep the bookkeeping) → `a_stalled_partial_frame_is_session_fatal_within_the_deadline` RED (recv_or_none returns None). Restore.
3. Drop `&& supervisors.is_empty()` from the idle condition → `an_enabled_idle_timeout_reaps_quiet_sessions_but_never_inflight_ones` RED (the busy session's terminal never arrives — EOF first). Restore.
4. Change `idle_timeout`'s default to `Some(Duration::from_secs(30))` → `idle_timeout_defaults_off_and_a_quiet_session_survives` RED at the first assert. Restore.
5. In `codec.rs`, increment `started` on EVERY partial-decode call (drop the `mid_frame` once-guard) → the stall detector sees a moving counter for one stalled frame… which would make the stall test RED (never fires) — run it, confirm RED, restore. (Proves the once-per-frame discipline is load-bearing.)

- [ ] **Step 6: Delta ledger + commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md`:

```markdown
### Task 11
- §5/§5.2: engine-side liveness — max_connections (1024), frame_read_timeout (30s, tick-enforced),
  idle_timeout (default OFF, in-flight-vetoed). Overflow reject rides POOL_TIMEOUT{Retryable};
  ERR_OVERLOADED recorded as a deferred /proto candidate.
```

```bash
git add engine/crates/ferrod docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "feat(m1-s9a): max_connections + frame_read_timeout + idle_timeout — one client can no longer pin the daemon"
```

---

## Task 12: SIGTERM's graceful drain actually reaches sessions, pools, and tx actors

`Drain` has exactly two consumers, both in the accept loop (hazard 21): every restart is a 5s-delayed `abort_all()` that kills sessions WITHOUT their cleanup — no terminals, no engine-side rollback, the writer droppable mid-frame. This task wires it through: sessions observe the drain, refuse NEW checkout-acquiring work (autocommit EXEC, BEGIN — hazard 22) with `POOL_TIMEOUT{Retryable}`, let pinned transactions keep working through the window (§18's "let pins finish"), and at `drain_deadline` exit through the NORMAL cleanup path (`cancel_all` → `abort_session` → `drain_supervisors` → writer flush).

**Files:**
- Modify: `engine/crates/ferrod/src/session/mod.rs` (`run_with_handler(.., drain: Drain)`; the drain arm + deadline in the tick arm; `Session::run` mints `Drain::new()`)
- Modify: `engine/crates/ferrod/src/serve.rs` (pass `drain.clone()` into each session; the backstop becomes `drain_deadline + SESSION_DRAIN_GRACE`)
- Modify: `engine/crates/ferrod/src/services/sql.rs` (`make_handler(.., drain: Drain)`; the refusal in `handle_exec`'s autocommit arm and in `handle_begin`; `fn draining_refusal() -> ErrorPayload`)
- Modify: `engine/crates/ferrod/src/main.rs` (thread `drain.clone()` into `make_handler` — note `main` already creates the `Drain` AFTER `make_handler`; move the `Drain::new()` up)
- Modify: `engine/crates/ferrod/tests/common/mod.rs` (the `spawn_one_session*`/`spawn_serve*` helpers pass a `Drain` through to `run_with_handler` — `spawn_serve*` already take one)
- Modify: `engine/crates/ferrod/tests/shutdown.rs` (compile fixes only; its accept-refusal + hard-close assertions must keep passing UNCHANGED)
- Create: `engine/crates/ferrod/tests/drain_it.rs` (the live acceptance)

**Interfaces:**
- Produces: `Session::run_with_handler(stream, config, epoch, pool_registry, tx_registry, factory, drain: Drain)` (parameter appended LAST); `sql::make_handler(registry, tx_registry, idle_in_tx, max_tx, teardown_timeout, drain: Drain)`; `pub(crate) const SESSION_DRAIN_GRACE: Duration = Duration::from_secs(3);` (serve.rs — sessions get `drain_deadline` for their own wind-down, serve hard-aborts only `grace` later).
- Consumes: `shutdown::Drain` (`is_draining()`, `wait()` — a level, resolves repeatedly); Task 11's tick arm (same file — hence the ordering); Task 1's test-helper shapes (`begin`, `tx_req`, `write_req`) and `chaos_fate_it.rs:391`'s `commit(client, rid, tx_id) -> Outcome` helper (copied — separate test crates).

- [ ] **Step 1: Write the failing live test**

Create `engine/crates/ferrod/tests/drain_it.rs`:

```rust
//! M1-S9a Task 12 — SIGTERM's graceful drain, live (finding 6). Before this slice the Drain
//! token stopped `accept()` and nothing else: sessions kept dispatching new work for the whole
//! window, then were `abort_all()`ed mid-flight — no terminals, no engine-side rollback. §18's
//! contract: refuse new checkouts, let pins finish up to drain_deadline, then hard-close.

mod common;

use std::time::Duration;

use common::{TestClient, connect, exec_err, exec_ok, pg_url, req};
use ferro_proto::consts::{branch, errc, flags, method_tx, service};
use ferro_proto::messages::Outcome;
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::tx::{BeginRequest, BeginResponse, TxControl};
use ferrod::config::{Config, PoolSpec, infer_pool_kind};
use ferrod::epoch::{EpochSource, RandomEpoch};
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::shutdown::Drain;
use ferrod::tx::TxRegistry;
use std::sync::Arc;

fn tx_req(sql: &str, tx_id: u64) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r.tx_id = Some(tx_id);
    r
}

fn write_req(sql: &str) -> ExecRequest {
    let mut r = req(sql);
    r.readonly = false;
    r
}

async fn begin(client: &mut TestClient, rid: u32, pool: &str) -> u64 {
    let breq = BeginRequest {
        pool: pool.to_string(),
        isolation: None,
        readonly: false,
    };
    client
        .send_request(rid, service::TX, method_tx::BEGIN, breq.encode())
        .await;
    let t = client.recv().await;
    assert_eq!(t.header.flags & flags::END, flags::END);
    match Outcome::decode(&t.payload).expect("decode BEGIN Outcome") {
        Outcome::Ok(body) => BeginResponse::decode(&body).expect("BeginResponse").tx_id,
        other => panic!("BEGIN expected Ok, got {other:?}"),
    }
}

async fn commit(client: &mut TestClient, rid: u32, tx_id: u64) -> Outcome {
    client
        .send_request(rid, service::TX, method_tx::COMMIT, TxControl { tx_id }.encode())
        .await;
    let t = client.recv().await;
    Outcome::decode(&t.payload).expect("decode COMMIT Outcome")
}

/// The full §18 story on one session: drain fires mid-transaction → new autocommit work is
/// refused RETRYABLY, the pinned tx keeps working and COMMITs, and the session closes ITSELF
/// (EOF after a clean wind-down) inside drain_deadline + grace — never an abort with frames lost.
#[tokio::test]
async fn drain_refuses_new_work_lets_the_pinned_tx_finish_and_closes_cleanly() {
    let Some(url) = pg_url() else {
        eprintln!("skip: FERRO_TEST_PG_URL not set");
        return;
    };

    // Hand-assembled serve (the exec_server helper does not expose its Drain): real pools, real
    // handler, injected drain — the tests/shutdown.rs pattern with a pool-bearing config.
    let sock = std::env::temp_dir().join(format!("ferro-s9a-drain-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);
    let config = Config {
        socket_path: sock.clone(),
        drain_deadline: Duration::from_secs(2),
        pools: vec![PoolSpec {
            name: "default".to_string(),
            kind: infer_pool_kind(&url),
            dsn: url,
            pin_functions: Vec::new(),
            pin_on_unknown: true,
        }],
        ..Config::default()
    };
    let listener = ferrod::listener::bind_uds(&config).expect("bind");
    let registry = PoolRegistry::build(&config);
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let drain = Drain::new();
    let factory = sql::make_handler(
        registry.clone(),
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
        drain.clone(),
    );
    let epoch = RandomEpoch.epoch();
    let serve_task = tokio::spawn(ferrod::serve::serve(
        listener,
        config,
        epoch,
        drain.clone(),
        registry,
        tx_registry,
        factory,
    ));

    let mut c = connect(&sock).await;
    c.hello(1).await;
    let tx_id = begin(&mut c, 2, "default").await;
    exec_ok(&mut c, 3, &tx_req("SELECT 1", tx_id)).await;

    drain.trigger();

    // (a) NEW checkout-acquiring work is refused, retryably, with a terminal (rule 4 held).
    let ep = exec_err(&mut c, 4, &write_req("SELECT 1")).await;
    assert_eq!(ep.code, errc::POOL_TIMEOUT, "draining refusal, got {:#06x}: {}", ep.code, ep.message);
    assert_eq!(ep.branch, branch::RETRYABLE);

    // (b) …and a NEW BEGIN likewise.
    c.send_request(
        5,
        service::TX,
        method_tx::BEGIN,
        BeginRequest {
            pool: "default".to_string(),
            isolation: None,
            readonly: false,
        }
        .encode(),
    )
    .await;
    let t = c.recv().await;
    match Outcome::decode(&t.payload).expect("Outcome") {
        Outcome::Error(ep) => assert_eq!(ep.code, errc::POOL_TIMEOUT),
        other => panic!("a BEGIN during drain must be refused, got {other:?}"),
    }

    // (c) The PINNED transaction is untouched: statements and COMMIT still work.
    exec_ok(&mut c, 6, &tx_req("SELECT 1", tx_id)).await;
    match commit(&mut c, 7, tx_id) {
        Outcome::Ok(_) => {}
        other => panic!("COMMIT during the drain window must succeed, got {other:?}"),
    }

    // (d) The session winds ITSELF down: clean EOF at ~drain_deadline (2s). The bound is 3s —
    // deliberately TIGHTER than drain_deadline + grace (5s), so a session that only dies to
    // serve's grace-abort (the pre-S9a behavior, and the named mutation that deletes the drain
    // arm) fails this assert instead of passing on the backstop.
    tokio::time::timeout(Duration::from_secs(3), c.recv_eof())
        .await
        .expect("the session must close ITSELF at drain_deadline, not die to the grace abort");

    serve_task.await.expect("serve returns after the drain");
}
```

- [ ] **Step 2: Run to verify failure**

`cargo test -p ferrod --test drain_it` (env set) → COMPILE ERROR (`make_handler` takes 5 args; `serve`'s sessions don't observe drain). After the signatures exist but before the behavior: (a) FAILS — the autocommit EXEC during drain succeeds; (d) FAILS — no EOF inside the bound (the session never observes the drain). RED observed on both.

- [ ] **Step 3: Implement**

**(a) `session/mod.rs`:** append `drain: Drain` to `run_with_handler`; `Session::run` passes `Drain::new()`. Before the reader loop: `let mut draining = false; let mut drain_deadline_at: Option<tokio::time::Instant> = None;`. New select arm (ABOVE the tick arm, below the supervisors reap):

```rust
                () = drain.wait(), if !draining => {
                    // SIGTERM (or an injected drain). Do NOT break: the reader must keep serving
                    // CANCEL/WINDOW_UPDATE for in-flight streams and the pinned transactions'
                    // statements through the window (§18 "let pins finish"). New checkout-
                    // acquiring work is refused at the SERVICE layer (handle_exec/handle_begin),
                    // where tx_id is parsed. The `if !draining` guard matters: `wait()` is a
                    // LEVEL — unguarded it would win every select iteration forever.
                    tracing::info!("session drain started");
                    draining = true;
                    drain_deadline_at =
                        Some(tokio::time::Instant::now() + config.drain_deadline);
                    continue;
                }
```

and in Task 11's tick arm, FIRST:

```rust
                    if let Some(at) = drain_deadline_at
                        && tokio::time::Instant::now() >= at
                    {
                        // Window over: exit through the NORMAL cleanup path below — cancel_all,
                        // abort_session (actors roll back + release), drain_supervisors
                        // (terminals flush), writer drained. THIS is what the pre-S9a abort_all
                        // skipped.
                        break;
                    }
```

**(b) `serve.rs`:** pass `drain.clone()` as the new last argument of `Session::run_with_handler`; add `pub(crate) const SESSION_DRAIN_GRACE: Duration = Duration::from_secs(3);` and change the final wait to `drain_sessions(sessions, config.drain_deadline + SESSION_DRAIN_GRACE).await;` with a doc line: the sessions now own the `drain_deadline` wind-down; the abort is a belt-and-braces backstop `grace` later, no longer the mechanism.

**(c) `services/sql.rs`:** `make_handler(..., drain: Drain)`, cloned into the per-connection closure and into `handle`. In `handle_exec`'s autocommit (`None`) arm, FIRST:

```rust
        None => {
            if drain.is_draining() {
                responder.end_error(draining_refusal());
                return;
            }
```

and identically at the top of `handle_begin`. The mint:

```rust
/// M1-S9a finding 6: the drain-window refusal for NEW checkout-acquiring work (autocommit EXEC,
/// BEGIN). POOL_TIMEOUT{Retryable} — an existing registry pairing whose meaning is exactly right
/// ("resource momentarily unavailable; retry per your policy"): the client's resilience loop
/// reconnects against the socket-activated successor. Tx-scoped work is NOT refused — a pinned
/// transaction already holds its connection and must be allowed to finish (§18).
fn draining_refusal() -> ErrorPayload {
    ErrorPayload {
        code: errc::POOL_TIMEOUT,
        branch: errc::POOL_TIMEOUT_BRANCH,
        sqlstate: None,
        errno: None,
        message: "engine is draining for shutdown; new work refused — reconnect and retry"
            .to_string(),
        detail: None,
        retry_after_ms: None,
    }
}
```

**(d) `main.rs`:** move `let drain = Drain::new();` above the `make_handler` call and pass `drain.clone()`.

**(e) the test harness:** `common/mod.rs`'s `spawn_one_session*` gain a `Drain::new()` pass-through (their sessions never drain); `spawn_serve*` pass their existing `drain` parameter into `run_with_handler` via `serve` (no signature change — `serve` already owned it). `tests/shutdown.rs`: compile only.

- [ ] **Step 4: Run to verify pass**

Live: `cargo test -p ferrod --test drain_it` → green. Regression: `cargo test -p ferrod --test shutdown --test session_rules` UNCHANGED-green (accept-refusal + hard-close semantics intact; exactly-one-END intact — the drain path exits through the existing cleanup, and the refusals are ordinary terminals). Full live `ferrod` suite once (`in_tx_fate_it`, `chaos_fate_it`, `mysql_chaos_it`, `tx_it`, `sql_exec_it`, `stream_it`, `availability_it`): a drain never triggers in them, so any diff is a regression.

- [ ] **Step 5: NAMED MUTATIONS**

1. Delete the `drain.is_draining()` refusal in `handle_exec` → step (a) of `drain_it` RED (the EXEC succeeds). Restore.
2. Delete the session's drain select arm (leave the service refusal) → step (d) RED. Why the test can catch it: an aborted session ALSO ends in EOF, but only at serve's grace backstop (drain_deadline 2s + grace 3s = 5s), while a session that observes the drain closes ITSELF at ≈2s — and step (d)'s bound is 3s for exactly this reason (see the test comment). Under the mutation the EOF arrives at 5s → the 3s bound fails. Restore.
3. In `serve.rs`, drop `+ SESSION_DRAIN_GRACE` (backstop at bare `drain_deadline`) → `drain_it` becomes racy-red (the abort can beat the session's own deadline-tick by up to one tick); run `tests/shutdown.rs`'s hard-close test to confirm it still passes, then restore. (Records WHY the grace exists.)

- [ ] **Step 6: Delta ledger + commit**

Append to `docs/followups/2026-08-11-s9a-spec-deltas.md`:

```markdown
### Task 12
- §18: the drain contract is now implemented as specified — accept stops, new checkout-acquiring
  work is refused POOL_TIMEOUT{Retryable}, pinned transactions run to completion inside
  drain_deadline, sessions exit through their own cleanup (terminals + engine-side rollback +
  writer flush), hard-abort only as a +3s backstop. ERR_SHUTTING_DOWN recorded as a deferred
  /proto candidate.
```

```bash
git add engine/crates/ferrod docs/followups/2026-08-11-s9a-spec-deltas.md
git commit -m "feat(m1-s9a): SIGTERM drain reaches sessions, pools and tx actors — refuse new work, let pins finish, wind down clean"
```

---

## Task 13: The SPEC-DELTA batch — one author, at the end

Parallel spec authorship produced the S8a §22.2 (u)/(v) contradiction; this slice therefore serialized every spec edit into this task. Input: `docs/followups/2026-08-11-s9a-spec-deltas.md` (Tasks 1–12 appended one block each). **This is the ONLY task that edits `ferro-spec-v0.2.md` or `CLAUDE.md`.**

**Files:**
- Modify: `ferro-spec-v0.2.md` (§5/§5.2, §7/§7.1, §12, §18, §19.3 — including CLOSING the §19.3 OPEN note at ~line 489 with the Task-9 cause — and new §22.2 lettered entries for: the persisted-tx cell + its readonly-invariant exception; the `ConnectionLost{dispatched}` refinement and its MySQL phase-attribution status; the CANCEL_DRAIN_BUDGET; the three availability knobs and their defaults; the drain semantics; the two deferred /proto candidates `TX_PARTIALLY_COMMITTED` + `ERR_OVERLOADED`/`ERR_SHUTTING_DOWN`)
- Modify: `CLAUDE.md` (the "Current state" gains the M1-S9a paragraph — findings closed, the two drop-in-visible consequences stated plainly: MySQL DDL-in-tx failures now report `Indeterminate`, and a pre-dispatch death now reports `Retryable`; the "Next up" list updated)
- Modify: `docs/followups/2026-08-11-m0-core-review-findings.md` (a one-line "closed by M1-S9a, plan + commits" header note); `docs/followups/2026-08-10-unbounded-backend-dial.md` (closed by Task 3)
- Consume: `docs/followups/2026-08-11-s9a-spec-deltas.md`

**Interfaces:** none — prose only, no code, no tests.

- [ ] **Step 1:** Read every `### Task N` block in the delta ledger against the actual merged diffs (`git log --oneline` since the wave-A base); reject any ledger claim the code does not substantiate — the DoD rule is "the relevant SPEC section still tells the truth", not "matches the plan".
- [ ] **Step 2:** Apply the §19.3 amendments in ONE edit (the persisted-tx rule, the dispatched refinement, the closed OPEN note, the readonly exception) so the section reads as one coherent contract; then the smaller sections; then the §22.2 letters (continue the existing letter sequence); then `CLAUDE.md`.
- [ ] **Step 3:** Cross-check for contradictions the S8a way: grep every §22.2 entry this slice added against every section it cites; two statements about the same behavior must be one statement referenced twice.
- [ ] **Step 4:** `cargo test --workspace` offline one last time (docs-only change — the gate is that nothing else snuck in), then commit:

```bash
git add ferro-spec-v0.2.md CLAUDE.md docs/followups
git commit -m "docs(m1-s9a): the spec-delta batch — §19.3/§18/§12/§7/§5 now tell the truth about the hardened core"
```

---

## Self-review (performed while writing; recorded so reviewers can re-run it)

**Coverage:** every finding in the brief maps to a task (see the Findings coverage map): (1)→2/7/8, (2)→1, (3)→9, (4a/b/c)→3/4/10, (5)→5/11, (6)→12, (7)→6+3, spec-truth→13. The review's "what is sound" list is protected by named regression re-runs in Tasks 1, 5, 7, 8, 9, 10, 11, 12 — not by avoidance alone.

**Placeholder scan:** no TBDs; every test/impl step carries real code. Three deliberate verify-at-implementation points are called out AS decisions, not gaps: Task 9's MySQL prep-phase attribution (safe fallback `dispatched: true` specified), Task 11's `stub_handler()` (defined in the test file itself — `common/` has no shared stub), Task 12's mutation-2 bound (the 3s bound is written into the test BECAUSE of the mutation analysis).

**Type consistency:** `OpContext { readonly, sent, in_tx, tx_writes_persisted }` (T7) matches every later constructor (T8/T9); `ExecReply::Completed { result, exec_us, tx_writes_persisted }` / `Deadline { tx_writes_persisted }` (T8) match the `sql.rs` consumption (T8) and the T10 arms; `TxLookupErr::Tombstoned { tx_writes_persisted }` matches T8's lookup mapping; `PoolError::ConnectionLost { dispatched }` + `undispatched()` (T9) match the fate arm and every enumerated site; `run_tx_streamed(.., tx_writes_persisted: bool)` appended LAST (T8) matches the actor call; `CANCEL_DRAIN_BUDGET` is `pub(crate)` in `services/sql.rs` and imported by `actor.rs` (T10); `Session::run_with_handler(.., drain: Drain)` and `make_handler(.., drain: Drain)` both append LAST (T12); `FakeBackend` gains `block_connect`/`release_connect`/`connects_waiting` (T3), `arm_tx_status_after_next_query` (T8), `wedge_queries`/`release_wedged_queries`/`wedged_queries_waiting` (T10) — three different tasks, all sequential owners of `fake.rs` (T3 wave A; T8, T10 wave B serial).
