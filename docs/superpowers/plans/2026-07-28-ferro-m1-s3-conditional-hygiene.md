# Ferro M1 · Slice S3 — Conditional hygiene at checkout (Full vs targeted profile) Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.
> **v2** — adversarial plan-verification (`wf_9a7df47f-5f4`, FIX_FIRST) folded: 1 blocker + 3 majors + 1 minor. Mechanism confirmed sound (tainted-predicate, awaited-at-checkout, pipelining deferred; §7.4 blind-spot tests genuine). Fixes: the targeted profile MUST add `CLOSE ALL` (a `WITH HOLD` cursor under `pin_on_unknown=false` leaks cross-tenant) + `SET SESSION AUTHORIZATION DEFAULT` (match `DISCARD ALL` minus prepare-destruction); the `ferrod` actor test + `cargo test --workspace` gate were missing; `pin_stub.rs` was mis-cited (audit exact-recorded-sequence tests, not grep "RESET"); the prepares-survive gate needs a genuine test; `FakeBackend` `Default` must be hand-impl'd.

**Goal:** Replace the pool's single-profile checkout hygiene (`DISCARD ALL` iff `tainted`, else nothing) with SPEC §7.2's **conditional** model: a **tainted** connection → full reset (PG `DISCARD ALL`); a **non-tainted** recycled PG connection → the **targeted profile** `RESET ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES;` (releases advisory locks / temp / listens / GUCs — including session mutations the S2 assist lexer CANNOT see, e.g. `SELECT set_config('search_path',…,false)` or an in-`DO`-body lock — while preserving the engine's future namespaced prepares). Drive the choice from the S1/S2 pin state (`tainted`), not a flag.

**Scope decision (user-confirmed):** S3 delivers the **conditional profiles only**. The §7.2 "pipelined ahead of the first user statement" optimization is **DEFERRED** to a later perf slice — it is a cross-cutting change (relaxing the backend trait to `&Self::Conn`, `FakeConn` interior mutability, `try_join!` sequencing that must respect the fork's single-slot RFQ atomic invariant) whose main payoff (preserving cached prepared statements) does not exist yet (§7.3 statement cache is unbuilt — every query currently re-`prepare`s). Deferring speculative optimization aligns with charter rule 5. Hygiene continues to run **at checkout, awaited** (as today), only now with the correct per-state profile. The exec-design S3 gate + SPEC §22 are amended to record this deferral (Task 4).

**Architecture:** `PoolBackend::reset` gains a `ResetProfile { Full, Targeted }` parameter; PG maps `Full → "DISCARD ALL"`, `Targeted → "CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES;"` (one `batch_execute`, one round trip — `DISCARD ALL` minus the prepare-destroying `DEALLOCATE ALL`/`DISCARD PLANS`). A new `PoolBackend::clean_reset_profile(&self) -> Option<ResetProfile>` expresses the per-backend policy for a **non-tainted** recycled conn (PG → `Some(Targeted)`; MySQL later → `None` when the S6 tracker says known-clean). The pool's existing checkout recycle block picks: `tx_open → ROLLBACK`; then `tainted → reset(Full)` else `reset(clean_reset_profile())` — inside the existing bounded-timeout/evict wrapper. No pipelining, no new pin-state bit.

**Tech Stack:** Rust (edition 2024, tokio); `ferro-pool` recycle path + per-backend reset profiles; live PG via `testkit`.

## Global Constraints (verbatim from SPEC §7.2 / the M1 exec-design / the mechanism map + scope decision)

- **§7.2 conditional hygiene:** tainted → full reset (`DISCARD ALL`); non-tainted recycled PG conn → targeted profile (preserves prepares, releases advisory/temp/listen/GUC); known-clean (MySQL tracker, S6) → skip. SQLite N/A.
- **The targeted profile string (exact, v2 — SPEC §7.2 list + verification fix):** `CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES;` — run as ONE `batch_execute` (simple protocol, one round trip, one trailing `ReadyForQuery`). This is exactly PostgreSQL's `DISCARD ALL` **minus** the two prepare-destroying statements (`DEALLOCATE ALL`, `DISCARD PLANS`), which stay omitted so the engine's future namespaced prepares survive. **`CLOSE ALL` and `SET SESSION AUTHORIZATION DEFAULT` were ADDED beyond SPEC §7.2's authored 5-statement list (verification BLOCKER):** without `CLOSE ALL`, a `WITH HOLD` cursor (session-scoped, survives `COMMIT`) declared under a `pin_on_unknown=false` pool — where `DECLARE` (not safe-listed) classifies `!tainted` — leaks to the next tenant, who can `FETCH` it (a cross-tenant data leak `DISCARD ALL` catches; `CLOSE ALL` closes only cursors/portals, orthogonal to prepared statements, so it does NOT undermine "preserve prepares"). `SET SESSION AUTHORIZATION DEFAULT` matches `DISCARD ALL`'s coverage of `role`/`session_authorization` set via the `set_config(...,false)` blind spot (`DISCARD ALL` lists it separately from `RESET ALL`, implying `RESET ALL` alone is insufficient; `…DEFAULT` is a reset-to-self, always permitted, never errors). Task 3 verifies each class on live PG; the deviation from the §7.2 list is recorded in §22 (Task 4). Do NOT add `DEALLOCATE ALL`/`DISCARD PLANS`.
- **The profile predicate is `tainted`, NOT "ever in a tx" (design decision, resolves the mechanism-map §2 tension).** A cleanly-committed transaction (`BEGIN; SELECT; COMMIT`) does NOT set `tainted` (S1: `commit_tx` clears `tx_open`, leaves `tainted` untouched; a clean commit never taints), so it is treated as a non-tainted recycled conn → **targeted** profile (not `DISCARD ALL`). This deviates from §7.2's literal "a connection that was *pinned* during its last lease is tainted → full reset" by NOT force-full-resetting a clean read/write tx — which is BETTER (preserves prepares for clean txs; the targeted profile still covers every leak class) and needs NO new `ever_pinned` bit. A tx that DID mutate session state was tainted by the S2 lexer (or by an error/aborted-tx via the S1 Err-arm/RFQ-`E`) → Full. Record this reading in SPEC §22 (Task 4). The existing unit test `rfq_pin.rs::begin_pins_intx_then_commit_idles_and_leaves_reusable` (asserts a clean commit → NO reset) legitimately changes to assert the **targeted** reset now runs (Task 2).
- **The targeted profile is the §7.4 blind-spot backstop (S3's core VALUE).** The S2 lexer cannot see session mutations inside a safe-listed statement's function/`DO` body (`SELECT set_config('search_path',…,false)`, `DO $$ … pg_advisory_lock … $$`) — those return `!tainted`. TODAY (post-S1/S2) a `!tainted` conn skips hygiene entirely → the mutation LEAKS to the next tenant. S3's targeted profile runs on every non-tainted recycled PG conn → `RESET ALL`/`pg_advisory_unlock_all()` release it. The headline live test (Task 3) is a `set_config` search_path mutation that LEAKS before S3 and is CLEAN after (RED→GREEN).
- **Hygiene runs on the SAME popped idle conn handed to the caller** (verified: recycle cleans in place, then `Checkout::new` gets that conn — `pg_backend_pid` identical). It runs ONLY on recycled (previously-leased) idle conns; a brand-new `connect()` conn is pristine and skips the recycle. Keep the bounded-timeout/evict-on-error wrapper unchanged (a hanging cleanup must not stall checkout).
- **NO pipelining in S3** (deferred — see scope). Hygiene stays awaited-at-checkout. Do NOT relax `&mut Self::Conn` → `&Self::Conn`, do NOT add `FakeConn` interior mutability, do NOT touch the fork's single-slot RFQ atomic. Those belong to the deferred perf slice.
- **The S6 TX actor needs no change** — a tx-pinned conn returns via the same `Checkout::drop → IdleConn → recycle` path; whatever profile logic S3 adds covers it automatically.
- **Charter DoD — pin-cause assertion / leak tests.** §19/§7.2 leak-prevention tests are the acceptance bar. Every existing hygiene test that hardcodes the `"RESET"` fake-record string updates to the profile-aware string.
- **Charter rule 2 — `/proto` untouched.** S3 is a pool-internal hygiene change; no wire change.
- **Charter gates** green; live PG tests skip without `FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro`.

## File Structure

```
engine/crates/ferro-pool/src/
  backend.rs         + `pub enum ResetProfile { Full, Targeted }`; change `reset(conn)` -> `reset(conn, ResetProfile)`;
                       + `fn clean_reset_profile(&self) -> Option<ResetProfile>` (policy for a NON-tainted recycled conn)
  pool.rs            recycle block: tx_open->ROLLBACK; tainted->reset(Full) else reset(clean_reset_profile()?);
                       inside the existing bounded-timeout/evict wrapper (unchanged)
  fake.rs            FakeBackend::reset records the profile ("RESET:Full"/"RESET:Targeted"); clean_reset_profile settable (default Some(Targeted))
engine/crates/ferro-backend-pg/src/
  conn.rs            reset(Full)="DISCARD ALL"; reset(Targeted)="CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES;"; clean_reset_profile()=Some(Targeted)
engine/crates/ferrod/src/tx/actor.rs               (TEST-only) actor.rs:690 asserts recorded.contains("RESET") — update to "RESET:Full" (Task 1)
engine/crates/ferro-pool/tests/            update pin_stub.rs / rfq_pin.rs hardcoded "RESET" -> "RESET:Full"; + clean-commit test now expects Targeted; + profile-selection unit tests
engine/crates/ferro-backend-pg/tests/pg_pool_it.rs   + targeted-profile leak-closed live tests (incl. the set_config blind-spot RED->GREEN); tainted still Full
docs/superpowers/specs/2026-07-28-ferro-m1-execution-design.md   S3 gate amended: pipelining deferred (Task 4)
ferro-spec-v0.2.md §22    + note: S3 profile predicate = tainted (not ever-pinned); pipelining deferred (Task 4)
```

---

### Task 1: `ResetProfile` + `PoolBackend::reset(conn, profile)` + per-backend profiles

**Files:** Modify `engine/crates/ferro-pool/src/{backend.rs,fake.rs}`, `engine/crates/ferro-backend-pg/src/conn.rs`; update the ferro-pool tests that hardcode the fake `"RESET"` record string.

**Interfaces produced:**
- `pub enum ResetProfile { Full, Targeted }` (in `backend.rs`; derive `Debug, Clone, Copy, PartialEq, Eq`).
- `PoolBackend::reset(&self, conn: &mut Self::Conn, profile: ResetProfile) -> Result<(), PoolError>` (was `reset(&self, conn)`).
- `PoolBackend::clean_reset_profile(&self) -> Option<ResetProfile>` — the profile to apply to a recycled conn that is NOT tainted (PG → `Some(Targeted)`; `None` means "skip hygiene for a clean conn", the MySQL-tracker case in S6).

- `backend.rs`: add the enum; change the `reset` signature to take `profile`; add `clean_reset_profile`. Update the doc-comment on `reset` (it currently says "e.g. `DISCARD ALL`").
- `ferro-backend-pg/src/conn.rs`: `reset(conn, Full)` → `batch_execute("DISCARD ALL")`; `reset(conn, Targeted)` → `batch_execute("CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES;")` (map errors as today → `PoolError::ConnectionLost`, with a `tracing::warn!` naming the profile). `clean_reset_profile()` → `Some(ResetProfile::Targeted)`.
- `fake.rs`: `FakeBackend::reset(conn, profile)` records `format!("RESET:{profile:?}")` (i.e. `"RESET:Full"` / `"RESET:Targeted"`) instead of the bare `"RESET"`; keep clearing `conn.tx_open`. Add a settable `clean_reset_profile` field (+ a setter) so pool tests can drive the PG-like policy AND the skip case; `clean_reset_profile()` returns it. **MINOR (verification): `FakeBackend` is `#[derive(Debug, Default)]` AND has a hand-written `new()` — a new `Option<ResetProfile>` field would default (via the derive) to `None` while `new()` sets `Some(Targeted)`, a silent divergence (verification MINOR). DROP `#[derive(Default)]` and hand-impl `Default for FakeBackend` delegating to `new()` (or otherwise guarantee BOTH constructors set the field to `Some(Targeted)`). State this in the code.**
- **Update the tainted-path `"RESET"` literals → `"RESET:Full"` (verification MAJOR — corrected sites):** the ONLY literal `"RESET"` sites are in `ferro-pool/tests/rfq_pin.rs` (the `err_arm_forces_cleanup_*` sequence assertions `["…","ROLLBACK","RESET"]` and `failed_then_rollback…`, ~lines 207/260) AND `ferrod/src/tx/actor.rs:690` (`recorded.contains(&"RESET".to_string())`, the teardown-ROLLBACK-timeout tainted path). Change each to `"RESET:Full"` (all these paths are TAINTED → Full; ordering unchanged). **`pin_stub.rs` has NO `"RESET"` literal — do NOT touch it here; its tests break in the *Targeted* direction and are handled in Task 2.** (`actor.rs:690` lives in `ferrod`, which the old gate did not run — see the gate fix.)
- **TDD:** unit — `PgBackend::clean_reset_profile() == Some(Targeted)`; a `FakeBackend` reset with `Full`/`Targeted` records the right string; `FakeBackend` with `clean_reset_profile` set to `None` returns `None`; **`FakeBackend::default().clean_reset_profile() == FakeBackend::new().clean_reset_profile()` (proves the Default/new divergence fix).** (Live PG that the two batch strings execute without error lands in Task 3.)
- **Gate:** `cargo test --workspace` (NOT just `-p ferro-pool -p ferro-backend-pg` — the `ferrod` actor test at actor.rs:690 must be exercised; the old per-crate gate would MISS it, verification MAJOR); `cargo build --workspace`; fmt/clippy; `/proto` untouched.
- **Commit** `feat(m1-s3): ResetProfile{Full,Targeted} + PoolBackend::reset(profile) + clean_reset_profile (PG targeted batch)`.

---

### Task 2: the conditional recycle decision in `Checkout` checkout

**Files:** Modify `engine/crates/ferro-pool/src/pool.rs` (the recycle block ~130-157); Tests `engine/crates/ferro-pool/tests/{pin_stub.rs,rfq_pin.rs,pool_semantics.rs}`.

**Interfaces consumed:** `ResetProfile`, `PoolBackend::{reset, clean_reset_profile}` (Task 1).

- `pool.rs`: rewrite the cleanup closure inside the existing bounded-timeout/evict wrapper (KEEP the `tokio::time::timeout(checkout_timeout, cleanup)` + `continue`-on-err/timeout eviction exactly as is):
  ```rust
  let cleanup = async {
      if idle_conn.tx_open {
          self.inner.backend.simple_query(&mut idle_conn.conn, "ROLLBACK").await?;
          idle_conn.tx_open = false;
      }
      // §7.2 conditional profile: a tainted conn (detected session mutation, error, or aborted tx)
      // gets the FULL reset; a non-tainted recycled conn gets the backend's clean profile (PG:
      // Targeted — the §7.4 blind-spot backstop; MySQL later: None when the tracker says clean).
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
  ```
  **Change the guarding `if`:** today the whole cleanup runs only `if idle_conn.tx_open || idle_conn.tainted`. Now a NON-tainted conn also needs the targeted profile, so the cleanup must run whenever `tx_open || tainted || clean_reset_profile().is_some()`. Simplest: drop the outer `if` and always enter the timeout-wrapped cleanup (the cleanup itself no-ops when `!tx_open && profile==None`), OR guard on `idle_conn.tx_open || idle_conn.tainted || self.inner.backend.clean_reset_profile().is_some()`. Prefer the explicit guard so a `None`-clean-profile backend (future MySQL known-clean) still skips the timeout wrapper entirely when nothing needs doing. Keep the bounded-timeout/evict semantics identical.
- **Audit + update EVERY fake-driven test that drops a NON-tainted conn and re-checks it out asserting an exact recorded sequence** (verification MAJOR — the "grep for RESET" heuristic is insufficient; a non-tainted conn now records `"RESET:Targeted"` where it previously recorded nothing). The known sites to update (audit for more):
  - `rfq_pin.rs::begin_pins_intx_then_commit_idles_and_leaves_reusable` — asserts a clean commit leaves the conn reusable with NO reset. Now records `"RESET:Targeted"` at the next checkout; update the expectation (conn is still reusable — just defensively targeted-reset) and reword its "no reset should run" message.
  - `pin_stub.rs::pin_stub_tx_cause` (~lines 34-38) — asserts `recorded == ["BEGIN","COMMIT"]` after a clean commit. Now `["BEGIN","COMMIT","RESET:Targeted"]`; update.
  - `pin_stub.rs::defensive_rollback_on_next_checkout` (~lines 111-116) — asserts `recorded.last() == Some("ROLLBACK")` after a dropped OPEN (non-tainted — a bare `BEGIN` taints nothing) tx. Now the recycle appends `"RESET:Targeted"` AFTER the ROLLBACK, so `.last()` is `"RESET:Targeted"`; change to assert the `ROLLBACK`-then-`RESET:Targeted` SEQUENCE (not `.last()==ROLLBACK`).
  This is the Global-Constraints design decision (non-tainted → targeted), not a regression — update expectations, do NOT revert the behavior. Grep `pool_semantics.rs`/`tx_api.rs`/`query_guard.rs` too for any "clean conn records no RESET" assumption and fix likewise.
- **TDD (unit, FakeBackend):**
  - a `tainted` conn (e.g. via a lexer `Set` or the Err-arm) → next checkout records `"RESET:Full"`.
  - a NON-tainted recycled conn (a plain `SELECT 1`, or a clean `BEGIN;…;COMMIT`) → next checkout records `"RESET:Targeted"` (with the fake's `clean_reset_profile == Some(Targeted)`).
  - a `tx_open` conn → `"ROLLBACK"` precedes the reset (ordering preserved).
  - a fake with `clean_reset_profile == None` (the MySQL-known-clean analog) → a non-tainted conn records NOTHING (skip) — proves the skip path.
  - the bounded-timeout eviction still fires (reuse/adapt the existing timeout test).
  - assert the pin-cause DoD where applicable.
- **Gate:** `cargo test --workspace` (exercises `ferro-pool` + the `ferrod` fake-driven actor/tx suites — the latter is where a missed non-tainted-sequence assert would otherwise escape); existing S1/S4/S6 fake-driven suites green after the audit updates; `cargo build --workspace`; fmt/clippy.
- **Commit** `feat(m1-s3): conditional checkout hygiene — tainted->Full, non-tainted PG->Targeted (drop the skip-when-clean for PG)`.

---

### Task 3: live-PG leak-prevention tests (the acceptance bar, incl. the §7.4 backstop)

**Files:** Tests `engine/crates/ferro-backend-pg/tests/pg_pool_it.rs` (extend the existing hygiene tests).

- All tests: `max_size=1` pool + assert `pg_backend_pid()` identical across the two checkouts (so the reset is observed on the SAME recycled conn — no fresh-conn false green), skip without `FERRO_TEST_PG_URL`. Idempotent against the persistent testkit DB.
- **(a) The headline §7.4-backstop test (RED before S3, GREEN after) — the S3 value proof:** on checkout 1, run an autocommit statement that mutates session state THROUGH A FUNCTION the lexer cannot see: `Checkout::query("SELECT set_config('search_path', 'ferro_s3_leak', false)", &[])`. Assert the conn is `!tainted()` (the lexer safe-lists leading `SELECT` → the mutation is invisible → NOT tainted — this is the blind spot). Drop the checkout; on checkout 2 (same pid) assert `SHOW search_path` is NOT `ferro_s3_leak` (the **targeted** profile's `RESET ALL` reset it). Add a comment: this exact statement LEAKS before S3 (a `!tainted` conn skipped hygiene) and is closed by S3's targeted profile — the concrete §7.4 backstop.
- **(b) advisory lock via the same blind spot:** checkout 1 `Checkout::exec("DO $$ BEGIN PERFORM pg_advisory_lock(42); END $$")` (an in-`DO`-body lock — the lexer masks dollar-quote bodies → `!tainted`). Drop; from an INDEPENDENT connection assert `pg_try_advisory_lock(42)` returns true after the recycle (the targeted `pg_advisory_unlock_all()` released it). Release from the independent conn afterward (idempotency).
- **(c) temp + listen via the targeted profile:** a `!tainted` conn that (through a function/DO the lexer misses, OR directly for coverage) left a temp table / a LISTEN — assert gone/unsubscribed on checkout 2 (same pid) via the targeted `DISCARD TEMP` / `UNLISTEN *`. (If a clean way to create these invisibly is awkward, it's acceptable to assert the targeted profile clears a directly-created temp/LISTEN on a conn forced non-tainted, to prove the profile's coverage — the point is the TARGETED string covers all four leak classes.)
- **(d) tainted still gets Full:** a `tainted` statement (e.g. `Checkout::exec("SET search_path TO ferro_s3_full")` — lexer taints `Set`) → checkout 2 records/observes the FULL `DISCARD ALL` cleared it (search_path back to default). (Distinguishes Full from Targeted at the live level; if the fake-record distinction is enough, keep this light — the key is both profiles clear the leak.)
- **(e) prepares-survive — a GENUINE test (verification MAJOR; do NOT downgrade to a smoke check):** prove the Targeted-vs-Full distinction MATTERS. Via the sanctioned `conn_mut()` raw-client side-door in a test: `PREPARE ferro_s3_ps AS SELECT 1` on the raw conn, then `backend.reset(conn, ResetProfile::Targeted)`, then `EXECUTE ferro_s3_ps` → SUCCEEDS (the prepared statement survived — Targeted omits `DEALLOCATE ALL`). Contrast: `PREPARE` again, `backend.reset(conn, ResetProfile::Full)` (`DISCARD ALL`), then `EXECUTE` → FAILS (prepared statement gone). This proves Targeted preserves prepares while Full destroys them — the entire rationale for the conditional model. (The §7.3 statement cache is unbuilt, but the primitive property — a named prepare survives a Targeted reset — is testable NOW and IS the gate's "prepares survive checkout" sub-clause.)
- **(f) role / session_authorization via the blind spot (confirms the added `SET SESSION AUTHORIZATION DEFAULT` + `RESET ALL` coverage):** on checkout 1 run `Checkout::query("SELECT set_config('search_path','ferro_s3_x',false)", &[])` (already covered in (a)) — additionally, if the test role permits, verify a `set_config('role', …, false)` blind-spot mutation is reset on checkout 2 (same pid). If the testkit role cannot meaningfully change `role`/`session_authorization`, assert instead that the Targeted batch string containing `SET SESSION AUTHORIZATION DEFAULT` executes cleanly on a live conn (it is a reset-to-self, always permitted) — and note the belt-and-braces intent in a comment.
- **Gate:** live `cargo test -p ferro-backend-pg` (with `FERRO_TEST_PG_URL`); the (a) test must be a genuine RED-before/GREEN-after (verify by noting it would fail against the pre-S3 skip-when-clean behavior); existing pg_pool_it hygiene tests still green (some now observe Targeted instead of skip for non-tainted paths — update expectations, do NOT weaken). `cargo build --workspace`; fmt/clippy.
- **Commit** `feat(m1-s3): live leak-prevention tests — targeted profile closes the set_config/DO-body §7.4 blind spot; tainted still Full`.

---

### Task 4: amend the exec-design S3 gate + SPEC §22 (record the pipelining deferral + the tainted-predicate reading)

**Files:** Modify `docs/superpowers/specs/2026-07-28-ferro-m1-execution-design.md` (S3 section), `ferro-spec-v0.2.md` (§22).

- **exec-design S3 gate:** amend the S3 slice's gate line: the **conditional/targeted hygiene + leak-prevention + prepares-survive-checkout ship in S3** (the leak gate and prepares-survive are MET — see Task 3(a)-(e)); the **"measurably pipelined" optimization is DEFERRED** to a later perf slice (gated on a recorded bench number + the §7.3 statement cache existing), with the rationale (charter rule 5; the fork single-slot RFQ atomic; the cache to benefit is unbuilt). Do NOT drop the leak-prevention or prepares-survive sub-clauses — only pipelining is deferred.
- **SPEC §22 (implementation deviations):** add a note: (1) S3's hygiene profile predicate is `tainted` (a cleanly-committed tx is NOT force-full-reset; it gets the targeted profile), a deliberate refinement of §7.2's literal "was-pinned → full reset" that preserves prepares for clean txs while the targeted profile still closes every leak class; (2) **the targeted profile string DEVIATES from §7.2's authored list — it ADDS `CLOSE ALL` (holdable cursors leak cross-tenant under `pin_on_unknown=false` otherwise — a real gap in the §7.2 list) and `SET SESSION AUTHORIZATION DEFAULT` (role/session-auth coverage), making it exactly `DISCARD ALL` minus the prepare-destroying `DEALLOCATE ALL`/`DISCARD PLANS`; consider updating §7.2's list upstream**; (3) the §7.2 "pipelined ahead of the first user statement" optimization is deferred post-S3 (hygiene runs awaited-at-checkout for now) — reference the exec-design.
- **TDD:** N/A (docs). Verify the amended sections read truthfully and the exec-design S3 gate no longer claims pipelining as an S3 deliverable.
- **Gate:** `/proto` untouched; the docs tell the truth (charter DoD); no code change.
- **Commit** `docs(m1-s3): amend exec-design S3 gate (defer pipelining) + SPEC §22 (tainted-predicate hygiene, pipelining deferral)`.

---

## Self-Review (author against SPEC §7.2 + exec-design S3 + mechanism map + scope decision)

- **Spec coverage (exec-design S3 gate, minus deferred pipelining):** conditional Full-vs-targeted profiles (T1); the recycle decision driven by `tainted` (T2); live leak-prevention incl. the §7.4 backstop (T3); the pipelining deferral + tainted-predicate reading recorded in the docs (T4). The exec-design's leak gate ("a conn that held an advisory lock / temp table / LISTEN is CLEAN for the next tenant") is met by T3; the "measurably pipelined" gate item is explicitly deferred (T4) per the user-confirmed scope.
- **S3's real value is the §7.4 backstop:** the targeted profile closes a leak that S1+S2 CANNOT (a `set_config`/in-`DO` session mutation the lexer can't see) — proven by the T3(a) RED-before/GREEN-after test. This is why S3 is correctness, not just optimization.
- **No new pin-state bit; no pipelining; no fork-invariant changes** — the predicate is the existing `tainted`; hygiene stays awaited-at-checkout; the bounded-timeout/evict wrapper is unchanged.
- **Existing-test updates are behavior-truthful:** the `"RESET"`→`"RESET:Full"` changes are mechanical (those paths are tainted); the clean-commit test change (→Targeted) reflects the intended new behavior, not a weakening.
- **Plan-verification DONE → this is v2 (FIX_FIRST, folded).** The adversarial pass (`wf_9a7df47f-5f4`, 4 probes) confirmed the mechanism sound (tainted-predicate, awaited-at-checkout, pipelining deferred; the §7.4 blind-spot tests genuine RED→GREEN against rules.rs/scan.rs) and caught: **(BLOCKER)** the targeted profile omitted `CLOSE ALL` → a `WITH HOLD` cursor leaks cross-tenant under `pin_on_unknown=false` (`DECLARE` confirmed NOT in `SAFE_LEADING_KEYWORDS`) → added `CLOSE ALL` + `SET SESSION AUTHORIZATION DEFAULT` (targeted = `DISCARD ALL` minus prepare-destruction), §22-recorded; **(MAJOR)** `ferrod/src/tx/actor.rs:690`'s `contains("RESET")` assert breaks + no old gate ran `ferrod` → added it to the rename list + `cargo test --workspace` gate; **(MAJOR)** `pin_stub.rs` mis-cited (zero `"RESET"` literal) → reframed to "audit every non-tainted-conn re-checkout sequence assertion", named `pin_stub_tx_cause`/`defensive_rollback_on_next_checkout`; **(MAJOR)** the prepares-survive gate → a genuine PREPARE→reset(Targeted)→EXECUTE-succeeds vs reset(Full)→gone test (Task 3e); **(MINOR)** `FakeBackend` `Default`/`new()` divergence → hand-impl `Default`.

## Execution Handoff

Subagent-driven: fresh implementer per task (TDD, gates), two-stage review after each (probe the targeted-profile completeness + the tainted-predicate leak-safety + the §7.4-backstop test being a genuine RED→GREEN), whole-branch review before S4. Live tests against the testkit Docker PG.
