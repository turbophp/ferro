# Ferro M1 · Slice S4 — Full error taxonomy + write-fate matrix + chaos suite Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.
> **v2** — adversarial plan-verification (`wf_57fcbb38-5d3`, FIX_FIRST) folded: 3 blockers + 6 majors + 1 minor. Strategy confirmed sound (centralized `fate.rs`, drain-don't-drop, `/proto` untouched). Fixes: declare_ctl/CTL pass `in_tx:false` (lost-COMMIT must stay Indeterminate — double-apply risk); the autocommit `select!` is `biased` query-first so `sent` is HONEST (a pre-dispatch cancel is Retryable, never a false Indeterminate); the tx path gets a per-request `cancel` carrier DISTINCT from session-abort; the drained future is a `Result` (a cancel can lose the race → `Ok` → success terminal, not a fabricated error); a cancelled READ is `Cancelled/NonRetryable` (not the impossible "Retryable"); the 57014 rule tests `in_tx` before `readonly` and is total over `sent`; an app-`statement_timeout` 57014 inside a tx routes through the rollback+tombstone+Retryable exit; the chaos no-re-dispatch proof is a counter (`UPDATE ... n=n+1 RETURNING n`), not a FakeBackend log; T5's `cause()` claims only what the wire can distinguish; the existing `cancel_in_flight_exec` live test is rewritten (T2 changes its outcome).

**Goal:** Make the §9.2 error taxonomy + §19.3 write-fate rules *complete and enforced end-to-end*. The wire registry + branches + the PHP client are already done (M0); S4 adds the **behavior**: actually enforce `ExecRequest.timeout_ms` and the per-request `CANCEL` flag on both EXEC paths (today decoded/routed but **no-ops**), and classify a cancelled/timed-out **autocommit write whose non-execution is unconfirmed** to `Indeterminate` (today hard-mapped to `NonRetryable{Cancelled}`), while an in-transaction timeout/cancel rolls the tx back → `Retryable`. Centralize every fate decision in one pure `fate.rs` matrix module, and prove the whole matrix live with a chaos suite that kills/times-out/cancels mid-write and asserts the branch **and that the statement is applied at most once / never re-dispatched** (§3, §19.3).

**Architecture:** A new `ferrod/src/services/fate.rs` holds `classify_fate(err: PoolError, ctx: OpContext) -> ErrorPayload` — the single `(error × readonly × sent × in_tx) → branch` decision, replacing the ad-hoc `pool_error_to_payload`. Both EXEC paths gain a **`biased` `tokio::select!`** (query future FIRST) over the query vs. (a) a `timeout_ms` timer and (b) the per-request `CancellationToken`, firing the existing out-of-band `Cancel`-over-side-connection primitive and **draining (not dropping)** the query — then routing the drained **`Result`** (Ok → success terminal; Err → `classify_fate`). A cancelled/timed-out autocommit write → `Indeterminate`; in a tx → the actor's existing rollback+tombstone path → `Retryable`. No `/proto` change.

**Tech Stack:** Rust (edition 2024, tokio); `ferrod` services + tx actor; reuses the S6 `Cancel`/`cancel_handle()` + biased-select/drain pattern (actor.rs ~263-296); live PG chaos via `testkit`; the M0 PHP `FateClassifier` (already branch-generic) gains only an optional `cause()` accessor scoped to what the wire distinguishes.

## Global Constraints (verbatim from SPEC §9.2 / §19.3 / §20.3 + the mechanism map + verification — every task assumes these)

- **Three branches + never-retry (§9.2, §3, §19.3):** `Retryable | Indeterminate | NonRetryable`. **The engine NEVER transparently retries a user statement.** A wrong fate — esp. a write that should be `Indeterminate` reported as `NonRetryable`/`Retryable`, OR any transparent retry — is Critical.
- **The wire code/branch pairings are IMMUTABLE** (from `proto/errors.toml`, unchanged): `Cancelled = 0x3008 / branch NonRetryable`; `WriteUnconfirmed = 0x2001 / branch Indeterminate`; `ConnectionLost = 0x1001 / branch Retryable`; `TxDeadline = 0x1003 / branch Retryable`; `Deadlock = 0x1004`, `SerializationFailure = 0x1005` / Retryable. **There is no `Cancelled/Retryable` — a cancelled READ is `Cancelled{NonRetryable}`** (a read is *safe to re-run*, but the client decides that off the `NonRetryable{Cancelled}` label; if truly retryable-read semantics are wanted they ride `ConnectionLost`, NOT a fabricated Cancelled/Retryable). Never hand-set an invalid (code,branch) pair.
- **The fate rules S4 makes real (§19.3):**
  - **Autocommit write** cancelled/timed-out, **dispatched** (`sent`) with unconfirmed non-execution → `WriteUnconfirmed{Indeterminate}`.
  - **Autocommit write NOT yet dispatched** (`sent == false` — a cancel that fired before the statement reached the backend) → `Retryable{ConnectionLost}` (an unsent write has no unknown fate). **`sent` MUST be honest (see the biased-select constraint), never hardcoded true.**
  - **Read** cancelled/timed-out → `Cancelled{NonRetryable}` (never `Indeterminate`).
  - **In a transaction**, any statement cancel/timeout → the engine rolls the tx back + tombstones the tx_id → `TxDeadline{Retryable}`. Never `Indeterminate` for an in-tx *statement*.
  - **Lost `COMMIT`** (sent, no response) → `Indeterminate{WriteUnconfirmed}` — **the commit boundary is classified with `in_tx:false`** (it is a write whose fate is unknown, NOT an in-tx statement; passing `in_tx:true` here would wrongly downgrade it to Retryable → double-apply). Already done in M0 — do not regress.
  - Deadlock (40P01) / serialization (40001) → `Retryable`. Already mapped — S4 adds a live proof.
- **`/proto` UNTOUCHED (charter rule 2 does not trigger).** All codes/branches + `WriteUnconfirmed` already exist. `cause` (`link_lost|timeout|engine_restart`) is a **client-side inference**, NOT a wire field — add none. If a task believes a wire change is needed, STOP and raise it.
- **Biased select, query FIRST (verification BLOCKER):** both EXEC paths use `tokio::select! { biased; r = &mut query_fut => …; () = timeout => …; () = cancel.cancelled() => … }` — the query future is polled at least once before the timeout/cancel arms, so by the time a cancel/timeout arm wins the statement has been dispatched and `sent = true` is *honest*. A cancel that fires before the query is ever polled (arriving during `checkout().await`, or a session `cancel_all()`) must resolve with `sent = false` → `Retryable`. Mirror the S6 actor's proven `biased` select (actor.rs ~263-273).
- **Drain, don't drop — and the drained value is a `Result` (verification BLOCKER+MAJOR):** when a timeout/cancel arm fires, obtain the side-connection `cancel_handle()` (captured before the query borrow), fire it, then **await the query future to completion** (never drop it). The drained value is `Result<QueryResult, PoolError>`: **`Ok(qr)` (the cancel LOST the race — the statement actually completed) → build the normal success terminal** (never fabricate a cancel/error for a completed statement — §5.2); **`Err(e)` → `classify_fate(e, ctx)`**. On the autocommit path there is no rollback, so the Ok branch is a genuine committed result and MUST be returned as success.
- **Exactly one END per request (§5.2)** across every new path; the session supervisor is the sole terminal sender. A branch-carrying `Outcome::Error` (not the fate-less `Outcome::Cancelled`) reports any cancel/timeout that has a fate. `Outcome::Cancelled` is retired from the SQL/TX handlers (document; verify nothing else depends on it).
- **Post-cancel connection hygiene:** a cancelled statement returns `Err(57014)`, so the S1 Err-arm (`r.is_err() → tainted`) already taints the conn → the S3 recycle `DISCARD ALL`s it before the next tenant. Confirm this covers the autocommit path; the tx path rolls back + tombstones. No conn is handed out dirty.
- **Preserve every DONE behavior (mechanism map §3):** ConnectionLost sent×readonly split, lost-COMMIT→Indeterminate, lost-ROLLBACK→Retryable, engine idle/max deadline→`TxDeadline{Retryable}`, known-fate bind/pre-validation errors never Indeterminate, deadlock/serialization→Retryable, and a NON-cancel in-tx statement error (e.g. 23505) is reported WITHOUT auto-rollback (the S6 behavior). Their unit tests move into `fate.rs`/stay green — not deleted.
- **Charter gates** green; live/chaos tests skip without `FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro`; `cargo test --workspace` stays green offline.

## File Structure

```
engine/crates/ferrod/src/services/
  fate.rs      NEW — classify_fate(err, OpContext{readonly,sent,in_tx}); the §9.2/§19.3 matrix incl. the 57014 override
               (in_tx tested BEFORE readonly; total over sent); ports the pool_error_to_payload tests as its table
  sql.rs       handle_exec: BIASED select! (query first) over timeout_ms + per-request cancel; drain -> Result (Ok=success,
               Err=classify_fate); route via classify_fate(OpContext{readonly, sent:HONEST, in_tx:false}); retire end_cancelled.
               declare_ctl/CTL: switch to classify_fate but pass in_tx:FALSE (commit boundary) — lost-COMMIT stays Indeterminate.
engine/crates/ferrod/src/tx/
  mod.rs       TxCommand::Exec gains BOTH `timeout_ms: Option<u32>` AND `cancel: CancellationToken` (per-request, DISTINCT from
               the actor's session-level `abort`)
  actor.rs     TxCommand::Exec inner select!: add a per-statement timeout arm + the per-request `cancel` arm -> the EXISTING
               Deadline rollback+tombstone+Retryable exit. ExecStep::Completed Err branch: detect sqlstate 57014 -> SAME
               rollback+tombstone+Retryable exit (an app statement_timeout). `abort` (session cancel_all) stays biased-first /
               its own path (deregister+drop-reply), NOT the new per-request rollback+reply path.
engine/crates/ferrod/src/session/
  mod.rs       confirm cancel_all() at session death still routes through `abort` (session/mod.rs ~464-469), unchanged
engine/crates/ferrod/tests/
  chaos_fate_it.rs   NEW — live chaos suite; no-re-dispatch proven via a per-test counter (UPDATE ctr SET n=n+1 RETURNING n)
  (rewrite) sql_e2e_scenarios.rs::cancel_in_flight_exec — now asserts Outcome::Error{Cancelled/NonRetryable}, not Ok
php/client/src/Client/Error/
  IndeterminateException.php   (T5) + cause(): link_lost | engine_restart | engine_reported (NOT a bogus "timeout")
```
(No `/proto/*`, no `error_map.rs` SQLSTATE-table change, no `ferro-pool`/`ferro-backend-pg` error-enum change — key off the existing `Sql{sqlstate:"57014"}`, adding NO new `PoolError` variant. If a task needs one, STOP and raise the exhaustive-match fan-out.)

---

### Task 1: `fate.rs` — the write-fate matrix as one pure function + exhaustive table

**Files:** Create `engine/crates/ferrod/src/services/fate.rs`; Modify `engine/crates/ferrod/src/services/sql.rs` (replace `pool_error_to_payload` at its call sites — `handle_exec` AND `declare_ctl` — with `fate::classify_fate`; move its tests into fate.rs's table).

**Interfaces produced:**
- `pub struct OpContext { pub readonly: bool, pub sent: bool, pub in_tx: bool }`.
- `pub fn classify_fate(err: PoolError, ctx: OpContext) -> ferro_proto::messages::ErrorPayload`.

- Port `pool_error_to_payload(err, readonly, sent)` (sql.rs ~614) into `classify_fate(err, ctx)`, PRESERVING every existing rule, and add the **57014/cancel override**, ordered `in_tx` FIRST (verification MAJOR — the (57014, in_tx, readonly) cell must be Retryable, not NonRetryable):
  ```rust
  // PoolError::Sql where sqlstate == "57014" (== errc::CANCELLED): a cancel or statement_timeout.
  // §19.3: in a tx -> the tx is dead -> Retryable; a read -> Cancelled{NonRetryable}; an autocommit
  // write that was DISPATCHED with unconfirmed non-execution -> WriteUnconfirmed{Indeterminate}.
  if is_57014(&err) {
      if ctx.in_tx        { return payload(errc::TX_DEADLINE, RETRYABLE, ..); }      // in_tx BEFORE readonly
      if ctx.readonly     { return payload(errc::CANCELLED, NONRETRYABLE, ..); }
      if ctx.sent         { return payload(errc::WRITE_UNCONFIRMED, INDETERMINATE, ..); }
      /* autocommit write, NOT sent */ return payload(errc::CONNECTION_LOST, RETRYABLE, ..); // unsent -> Retryable
  }
  ```
  Make the 57014 match **total over `sent`** (verification MINOR): any 57014 cell that is not `(sent && !readonly && !in_tx)` resolves to a defined branch above (never falls through to a stray Indeterminate). Add a one-line note that reaching a 57014 at all means the statement was on the wire (PG only emits it for a running query), so the `sent=false` case only arises when the cancel arm won before dispatch (handled by the biased select in T2/T3, which passes `sent=false`).
- **ConnectionLost** (unchanged): `sent && !readonly && !in_tx → WriteUnconfirmed{Indeterminate}` else `ConnectionLost{Retryable}`.
- **The `OpContext.in_tx` value is per-CALL-SITE and must be set precisely (verification BLOCKER — getting :245 vs :293 wrong is a fate bug).** There are exactly these `classify_fate` call sites in `sql.rs` (grep-confirmed the current `pool_error_to_payload` sites):
  - `handle_exec` autocommit EXEC (T2) → `OpContext{ readonly: req.readonly, sent: <honest>, in_tx: false }`.
  - the **in-tx statement** EXEC reply mapping (~sql.rs:245, a `co.query` inside an open tx) → `in_tx: true` (a link-loss on an in-tx statement → the whole tx is dead → `Retryable`, which `in_tx:true` yields — do NOT report Indeterminate for an in-tx statement).
  - the **COMMIT / ROLLBACK control** mapping (~sql.rs:293 / `declare_ctl` ~388-401) → `in_tx: false` — the commit boundary is NOT an in-tx statement; a lost COMMIT (`readonly:false, sent:true, in_tx:false`) MUST stay `Indeterminate` (double-apply risk if downgraded to Retryable). A lost ROLLBACK stays `Retryable` as today (it is not a lost write).
  Spell out each site's `OpContext` in the code + a comment; the ported `declare_ctl_maps_replies_including_commit_loss_indeterminate` test must still assert `Indeterminate` THROUGH the real call site.
- All other `Sql` (Syntax/Constraint/deadlock/serialization) → pass through the classified `(code, branch)` verbatim. `Timeout`→PoolTimeout/Retryable; `Closed`→ConnectionLost/Retryable; `Unsupported`/`Backend` as today.
- **TDD (the fate-matrix table — the exec-design S4 unit gate):** exhaustive `(PoolError × readonly × sent × in_tx) → (code, branch)`. MUST include: `Sql{57014}`×{write,sent,!in_tx}→`WRITE_UNCONFIRMED/Indeterminate`; `Sql{57014}`×read→`CANCELLED/NonRetryable`; `Sql{57014}`×in_tx→`TX_DEADLINE/Retryable`; `Sql{57014}`×{write,!sent}→`CONNECTION_LOST/Retryable`; **`Sql{57014}`×{in_tx,readonly}→Retryable** (proves in_tx-before-readonly); ConnectionLost×{sent,write,!in_tx}→Indeterminate; ×read→Retryable; ×!sent→Retryable; ×in_tx→Retryable; lost-COMMIT (via the `in_tx:false` declare path)→Indeterminate; `Sql{40001/40P01}`→Retryable; `Sql{23505}`→NonRetryable; `Timeout`→PoolTimeout. Port `connection_lost_indeterminate_only_when_sent_and_write` + `declare_ctl_*` assertions and keep them green THROUGH the real call sites.
- **Gate:** `cargo test --workspace` (fate table + the existing sql/tx/declare tests, now via `classify_fate`, green); build/fmt/clippy; `/proto` untouched.
- **Commit** `feat(m1-s4): fate.rs write-fate matrix — classify_fate(err,OpContext) (57014 in_tx-first override, sent-total, declare_ctl in_tx:false) + exhaustive table`.

---

### Task 2: enforce `timeout_ms` + per-request CANCEL on the autocommit EXEC path

**Files:** Modify `engine/crates/ferrod/src/services/sql.rs` (`handle_exec` + the handler closure that currently ignores `_cancel`); rewrite the existing live test `engine/crates/ferrod/tests/sql_e2e_scenarios.rs::cancel_in_flight_exec`.

**Interfaces consumed:** `fate::classify_fate`, `OpContext` (T1); the `Cancel`/`cancel_handle()` primitive; the per-request `CancellationToken` (handler's 3rd arg, `session/registry.rs InFlight{cancel}`).

- Stop ignoring the cancel token (`_cancel` at sql.rs ~91). Around `co.query(...)`:
  ```rust
  let mut query_fut = co.query(sql, &params);   // pin; obtain cancel_handle from co BEFORE if borrow requires
  let outcome = tokio::select! {
      biased;
      r = &mut query_fut => r,                                  // query polled first -> sent honest
      () = sleep_opt(req.timeout_ms) => { fire_cancel(); (&mut query_fut).await }  // drain
      () = cancel.cancelled()        => { fire_cancel(); (&mut query_fut).await }  // drain
  };
  // `sent` = whether the query was actually dispatched. With biased-first, if a timeout/cancel arm
  // ran we already polled the query >=1 -> dispatched -> sent=true; a pre-poll cancel (checkout race /
  // cancel_all) is not possible here because query_fut is polled first. If checkout itself was cancelled
  // upstream, that path already yields Retryable (unsent) — preserve it.
  match outcome {
      Ok(qr)  => respond success terminal (build_terminal_body),   // cancel LOST the race — real result
      Err(e)  => respond Outcome::Error(classify_fate(e, OpContext{ readonly: req.readonly, sent: true, in_tx: false })),
  }
  ```
  Use `sleep_opt` = a `tokio::time::sleep(Duration::from_millis(ms))` only when `timeout_ms` is `Some` (a `None` arm must be a never-ready future, NOT a 0 ms timer). `fire_cancel` uses `cancel_handle()` over the side connection (does not need the pinned conn) — capture it before the mutable query borrow (mirror the actor). Retire `end_cancelled` here (a fated cancel rides `Outcome::Error`; document — nothing else in the SQL handler needs `Outcome::Cancelled`).
- **Rewrite the existing live test** `cancel_in_flight_exec` (sql_e2e_scenarios.rs ~230): it currently asserts `Outcome::Ok` for a cancelled in-flight autocommit READ (the "CANCEL is a no-op in M0" behavior). T2 makes that request a cancelled read → **exactly one `Outcome::Error{code: CANCELLED, branch: NonRetryable}`** (never Ok, never Indeterminate) + exactly-one-END + session stays alive. Update the stale header comment.
- **TDD:**
  - a `timeout_ms`-elapsed autocommit WRITE → `Outcome::Error{WRITE_UNCONFIRMED, Indeterminate}` (assert wire branch/code).
  - a `timeout_ms`-elapsed READ → `Outcome::Error{CANCELLED, NonRetryable}` (never Indeterminate).
  - a per-request CANCEL racing an in-flight autocommit write → exactly-one-END, `Indeterminate`.
  - **the cancel-LOSES-the-race case:** arrange the query to complete before the cancel lands → `Outcome::Ok` (the real result), NOT a fabricated cancel/error. (The Ok drain branch.)
  - `timeout_ms == None` → no timer arm, unchanged behavior.
  - post-cancel conn is tainted (Err-arm) → recycled (assert via a follow-up checkout being clean, or the taint bit).
- **Gate:** `cargo test --workspace` (incl. the rewritten `cancel_in_flight_exec`, now green with the new expectation) + live where the harness allows; build/fmt/clippy; exactly-one-END holds.
- **Commit** `feat(m1-s4): enforce timeout_ms + per-request CANCEL on autocommit EXEC (biased drain->Result; cancelled write->Indeterminate, read->Cancelled, lost-race->Ok)`.

---

### Task 3: enforce `timeout_ms` + CANCEL on the tx-scoped EXEC path (in-tx → rollback+tombstone → Retryable)

**Files:** Modify `engine/crates/ferrod/src/tx/mod.rs`, `engine/crates/ferrod/src/tx/actor.rs`, `engine/crates/ferrod/src/services/sql.rs` (thread `req.timeout_ms` + the per-request cancel token into the tx-scoped dispatch); confirm `session/mod.rs` `cancel_all()` unchanged.

**Interfaces consumed:** the actor's existing `abort`/`idle_deadline`/`max_deadline` biased-select + its cancel→drain→rollback→tombstone machinery (actor.rs ~174-296); `fate::classify_fate`.

- `tx/mod.rs`: `TxCommand::Exec` gains BOTH `timeout_ms: Option<u32>` AND `cancel: CancellationToken` (the per-request token, DISTINCT from the actor's session-level `abort` — verification BLOCKER: reusing `abort` would route through `ExecStep::Abort → drop-reply → Protocol/NonRetryable`, a WRONG fate for a client CANCEL of an in-flight tx Exec).
- `actor.rs` `TxCommand::Exec` inner select! (keep `biased`): add (a) a per-statement `timeout_ms` arm and (b) the per-request `cancel.cancelled()` arm — BOTH exit through the EXACT existing path the engine's own `max_deadline` uses: fire the out-of-band cancel, **drain the query future** (match its `Result` — an `Ok` that raced to completion still means the tx must be handled; but per §19.3 the client asked to cancel/timed-out, and the safe uniform action inside a tx is roll back → the client restarts — so on both Ok-raced and Err, roll the tx back), **ROLLBACK**, **tombstone the tx_id**, and reply `TxDeadline{Retryable}`. (Draining before ROLLBACK matches the actor's proven ordering.)
- **The app-`statement_timeout` (bare 57014) path** (verification MAJOR): an app-set `statement_timeout` fires on its own and resolves through `ExecStep::Completed` (actor.rs ~276) with `Err(57014)`, NOT the deadline/cancel arms. In that Err branch, detect `sqlstate == "57014"` (`== errc::CANCELLED`) → route through the SAME rollback+tombstone+`TxDeadline{Retryable}` exit (a cancelled statement leaves the tx aborted — 25P02 on the next statement — so it must be rolled back, not forwarded as a bare Retryable the client can't act on). A **NON-cancel** statement error (e.g. 23505 constraint) stays reported to the client WITHOUT auto-rollback (preserve the S6 behavior + its tx_it tests).
- Confirm `session/mod.rs` `cancel_all()` at session death (~464-469) still routes through the actor's `abort` (deregister + drop-reply), NOT the new per-request rollback+reply path — the new `cancel` arm is per-request only; `abort` stays a distinct signal `cancel_all` fires. (Verify no collision.)
- **TDD (FakeBackend, deterministic):**
  - `TxCommand::Exec` with an elapsing `timeout_ms` → actor records `ROLLBACK`, tombstones, replies `Retryable`; the tx_id is then unusable (`Tombstoned → TxDeadline`).
  - a per-request CANCEL of an in-flight tx Exec → same rollback+tombstone+Retryable; exactly-one-END.
  - **synthetic bare 57014 via `ExecStep::Completed`** (the query completes with a 57014 error, no deadline/cancel arm fired) → asserts the recorded `ROLLBACK` + tombstone + Retryable.
  - a NON-cancel statement error (23505) inside a tx → NonRetryable reported, NO auto-rollback (S6 regression guard).
  - `cancel_all()`/session-death still aborts via the `abort` path (existing test stays green).
- **Gate:** `cargo test --workspace` (existing `tx_it`/actor tests still green); build/fmt/clippy.
- **Commit** `feat(m1-s4): enforce timeout_ms + per-request CANCEL on tx-scoped EXEC (+ app-statement_timeout 57014) -> rollback+tombstone->Retryable; abort path unchanged`.

---

### Task 4: the live chaos suite (§20.3 acceptance for G7)

**Files:** Create `engine/crates/ferrod/tests/chaos_fate_it.rs` (reuse `tests/common/mod.rs`'s `exec_server`/`req`/`exec_ok`/`exec_err`; skip without `FERRO_TEST_PG_URL`).

- **No-re-dispatch proof — one uniform mechanism for EVERY write case (verification MAJOR):** a per-test counter row `UPDATE ferro_s4_ctr SET n = n + 1 WHERE key = $UNIQUE RETURNING n` (seed `n=0`, unique key per test). After the chaos event, read `n` from a FRESH checkout: it must be `0` (the write never applied) or `1` (applied exactly once) — **never ≥2** (which would prove a silent re-dispatch). This proves effect-applied-at-most-once against live PG (the FakeBackend `recorded` log does not exist live — do NOT rely on it).
- Cases (each asserts the terminal `Outcome` branch/code + exactly-one-END + the counter invariant + that the engine did NOT transparently retry):
  - **kill mid-write** — autocommit `UPDATE ...ctr... RETURNING n` with a concurrent `pg_terminate_backend(pid)` → `WriteUnconfirmed{Indeterminate}`; counter read-back ∈ {0,1}.
  - **kill mid-read** — autocommit `SELECT pg_sleep(...)` (`readonly=true`) killed → `ConnectionLost{Retryable}` (never Indeterminate).
  - **statement-timeout autocommit write** — `UPDATE ...ctr... ; pg_sleep` with `timeout_ms` < sleep → `Indeterminate`; counter ∈ {0,1}.
  - **statement-timeout tx-scoped write** — inside a `BEGIN`, `UPDATE ...ctr... ; pg_sleep` with `timeout_ms` < sleep → tx rolled back, reply `Retryable`, tx_id unusable, counter == 0 (rolled back).
  - **CANCEL race mid-write** — `CANCEL {request_id}` for an in-flight autocommit `UPDATE ...ctr...` → exactly-one-END; branch `Indeterminate` (write, if cancel won) OR `Ok` (if the write raced to completion — assert the terminal + counter are consistent: Indeterminate⇒{0,1}, Ok⇒1); no re-dispatch either way.
  - **deadlock / serialization** — two concurrent txs provoking `40P01`/`40001` → `Retryable{Deadlock/SerializationFailure}` live through `ferrod`.
- **Gate:** live `cargo test -p ferrod --test chaos_fate_it` (with `FERRO_TEST_PG_URL`); `cargo test --workspace` green offline (skips). Idempotent (unique keys/`DROP ... IF EXISTS`/`CREATE ... IF NOT EXISTS` for the counter table; each test seeds its own row). fmt/clippy.
- **Commit** `feat(m1-s4): live chaos fate suite — kill/timeout/cancel/deadlock assert the §19.3 branch + counter proves at-most-once (no re-dispatch)`.

---

### Task 5 (small): PHP `IndeterminateException::cause()` (scoped to what the wire distinguishes) + never-retry test

**Files:** Modify `php/client/src/Client/Error/IndeterminateException.php` (+ `FateClassifier`/`ErrorMapper`/`Connection` where the loss is classified); Test `php/client/tests/`.

- The wire carries NO `cause`; it is client-side inference (verification MAJOR — do NOT claim "timeout" specificity the wire can't carry). Expose `cause(): string` scoped to what is actually distinguishable:
  - `engine_restart` — when the epoch changed on reconnect (the `ReconnectLoop` resolved a new `boot_epoch`).
  - `link_lost` — a transport-death write with no response (`classifyLoss`, `$server === null`).
  - `engine_reported` — any `Indeterminate` decoded from an engine reply `Outcome::Error{code: WRITE_UNCONFIRMED}` via `ErrorMapper::fromOutcome` (this is the S4 timeout/cancel-Indeterminate case; the wire cannot distinguish it from other engine-reported WriteUnconfirmed, so use the honest generic label, NOT `timeout`).
- **TDD (PHPUnit):** an engine-replied `Outcome::Error{WRITE_UNCONFIRMED, Indeterminate}` → `FateClassifier::mayRetry(...) == false` (never auto-retried, even with `retry_reads=true`), and `IndeterminateException::cause() == 'engine_reported'`; a no-response link-lost write → `cause() == 'link_lost'`; an epoch-change loss → `cause() == 'engine_restart'`; confirm a manifest `idempotent:true` is NOT consulted (that's M3). The three-branch mapping stays intact (additive).
- **Gate:** `(cd php/client && composer test)` (PHPUnit green) + `phpstan analyse --level 9 src`; no wire change.
- **Commit** `feat(m1-s4): PHP IndeterminateException::cause() (link_lost|engine_restart|engine_reported) + never-retry Indeterminate test`.

---

## Self-Review (author against SPEC §9.2/§19.3/§20.3 + exec-design S4 + mechanism map + verification)

- **Spec coverage:** the fate matrix as a pure module + exhaustive table with the corrected 57014 ordering + declare_ctl `in_tx:false` (T1); cancel/timeout enforced on both EXEC paths with a biased-first select, honest `sent`, and drain→`Result` (Ok=success) (T2 autocommit→Indeterminate/read→Cancelled/NonRetryable; T3 in-tx→rollback+tombstone→Retryable incl. the bare-57014 path); the chaos suite with a counter proving at-most-once (T4); the honest client-side `cause()` (T5). Deadlock/serialization proven live. Exactly-one-END + never-retry held.
- **Verification FIX_FIRST folded (v2):** [B1] lost-COMMIT stays Indeterminate (declare `in_tx:false`); [B2] biased select → honest `sent` (unsent write = Retryable); [B3] tx per-request `cancel` carrier distinct from `abort`; [M1] drained `Result` Ok→success; [M2] rewrite `cancel_in_flight_exec`; [M3] bare-57014-in-tx → rollback; [M4] read = Cancelled/NonRetryable (no impossible Retryable pairing); [M5] `cause()` scoped to distinguishable labels; [M6] counter-based no-re-dispatch; [minor] 57014 total-over-`sent` + in_tx-first.
- **`/proto` untouched; no new `PoolError` variant** (key off `Sql{sqlstate:"57014"}`) — no exhaustive-match fan-out.
- **Every DONE behavior preserved** (ConnectionLost split, lost-COMMIT, engine-deadline, deadlock/serialization, non-cancel-in-tx-error-no-rollback) with its tests moved/kept green.
- **Plan-verification DONE → v2** (`wf_57fcbb38-5d3`, FIX_FIRST, 3 blockers + 6 majors + 1 minor, all folded above). The remaining risk surface for the per-task reviews: the biased-select/drain-Result correctness under a real cancel-loses-race (T2/T3), the tx `cancel`-vs-`abort` non-collision (T3), and the chaos counter being a genuine at-most-once proof (T4).

## Execution Handoff

Subagent-driven: fresh implementer per task (TDD, gates), two-stage review after each. **T2 and T3 are concurrency-critical (biased select / cancel / drain-Result / rollback-tombstone) — review them on a capable model**, probing fate-branch correctness, the Ok-lost-race branch, honest `sent`, the `cancel`/`abort` non-collision, and exactly-one-END. Whole-branch review before S5. Live chaos tests against the testkit Docker PG.
