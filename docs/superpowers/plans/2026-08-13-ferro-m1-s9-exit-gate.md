# Ferro M1-S9 — The M1 Exit Gate Implementation Plan

> **ADVERSARIAL VERIFICATION RAN (Fable, 2026-08-13) — 1 BLOCKER + 5 MAJORS, all applied inline
> below as `PLAN-VERIFY` blocks. Verdict before the fixes: "M1 should NOT exit on this plan as
> written." After them the reviewer judged the skeleton sound — corrections C1–C3, the mutation set,
> the fail-closed runner design and the triage assignments survived every attack, and all three of
> the plan's declared "settle at task time" unknowns are now SETTLED (upstream TestUtil's surface
> read at a real 3.6.8 clone; `DEFAULT_POOL_MAX_SIZE = 16` at `pools.rs:45`, so the killer's second
> checkout needs no knob; and the MySQL processlist placeholder question measured on BOTH engines —
> a param-bound `info LIKE ?` poll sees the parked sleeper and never its own bound value).
> Journal, with every measurement: `.superpowers/sdd/2026-08-13-ferro-m1-s9-exit-gate/plan-verify.md`.
>
> **MINORS to fix in passing, each measured:** Task 4's recording loops pipe through `tee` without
> `set -o pipefail`, so the runner's exit status is discarded — a failed run would record silently;
> the ORM runner pins DBAL but NOT PHPUnit, and the results manifest omits the resolved PHPUnit
> version (research-orm's own conclusion was "pin both"); Global constraint 8 says
> `php/doctrine-dbal 291`, stale at this plan's own start (296 after `d436f83`); Task 3 Step 3's
> pool-size pointer is `pools.rs:45`, not `config.rs`; Task 2 Step 7's `sed` is a no-op sandwich
> carried only by its prose; and Wave A's "disjoint files" premise is violated by the shared
> spec-deltas ledger that all three tasks commit to — serialize that file or give each task its own.
>
> **One reviewer result worth carrying into Task 4:** the DBAL baseline was re-run at HEAD and
> MATCHES exactly (PG 730/828/3/7/354/2), so `d436f83` did not move it — the open question in
> hazard 11 is closed for PG.

> **STATUS (controller, 2026-08-13): Task 1 is ALREADY DONE — landed as `d436f83` before this plan
> was committed.** The ORM research probe found the `fetchFirstColumn` boolean-`false` truncation
> while the plan was still being written; it is silent data loss in a shipped path, so it was
> reproduced, fixed and mutation-proven immediately rather than queued. Measured before the fix:
> `[[true],[false],[true]]` returned `[true]`, and a LEADING `false` returned `[]`. The guard
> (`tests/Unit/FetchFirstColumnBooleanTest.php`, 5 cases incl. the NULL control and a
> falsy-but-not-false row) goes RED on 3 of 5 when `FetchUtils::fetchFirstColumn` is restored.
> **Start at Task 2.** Task 1's steps remain below as the record of what was done and why.
>
> This plan was authored by Fable and its adversarial verification pass was NOT run — the agent was
> killed by a session limit after writing the plan. Its own self-review (mutation no-op audit, type
> consistency, known-unknowns) is at the foot of the file and is the only verification this plan has
> had. Treat its "known unknowns an implementer must settle at task time" list as binding: STOP and
> report rather than assuming, since every unverified plan in this project has contained at least
> one defect that execution found.

> **PLAN STATUS: v1, not yet adversarially verified.** Every slice of this milestone that received a
> pre-build adversarial pass had real plan defects caught (S5: 6 blockers + 5 majors; S9a: 3 named
> mutations corrected, one test that could not pass). This plan corrected THREE defects in its own
> research inputs before writing a task (see "Corrections to the research" below) — treat that as
> evidence the pattern holds, and run a verification pass if budget allows.
>
> **Three corrections to `research-bar.md`'s §20.3 draft wording, measured against the client code —
> where this plan and the research journal disagree, THIS PLAN is right:**
> **(C1)** The draft's chaos cell (2) — "the §22.2 (ai) implicit-commit exception additionally
> asserted on MySQL/MariaDB" — is **unbuildable as drafted**: the engine's `tx_writes_persisted`
> latch dies with the SIGKILLed daemon and **no wire field carries it**, so the client structurally
> cannot report it. The corrected cell asserts the safety floor (never `Indeterminate`,
> prefix-unpersisted proven by read-back) and a separate MEASUREMENT test pins the implicit-commit
> residual (Task 3, cell 2b).
> **(C2)** The draft's cell (4) (open stream) is **PostgreSQL-only**: MySQL-family `fetch:stream`
> returns a clean `Unsupported` (§22.2 (n)) — there is no MySQL stream to kill.
> **(C3)** The draft assumed a reconnect-exhausted read surfaces `Retryable{ConnectionLost}`;
> the code throws the RAW last dial error (`ReconnectLoop.php:108`,
> `throw $lastError ?? new ConnectionLostException(...)`). Cell (3) is therefore designed
> restart-then-observe (deterministic) instead of racing the backoff window.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** exit M1 on a bar that cannot be met by a false green — renegotiate the exit bar in the
spec (baseline exact-match + five-category triage, SQLite formally removed, D12 re-anchored), run
the Doctrine ORM functional suite for the first time ever and record it under that bar, build the
§20.3 kill-`ferrod` chaos harness the spec already assigns to this gate, and fix the one
category-(a) driver defect (silent `fetchFirstColumn` truncation at a boolean `false`) the ORM
probe confirmed.

**Architecture:** Three parallel foundation tasks (the driver fix, the ORM harness, the kill-daemon
harness — disjoint files), then a strictly serial recording-and-documentation chain: the ORM
acceptance runs produce the committed baselines and results document; the doc-hygiene task feeds
`docs/known-incompatibilities.md` and the follow-up ledger from what was measured; ONE spec author
applies every amendment in a single task (the S8a (u)/(v) contradiction came from parallel spec
authorship); a final task re-runs every gate at the exit HEAD and writes the maintainer ledger.
**The bar itself is renegotiated IN THIS PLAN** (the exact wording lives in Task 6 and is the
contract from the moment this plan is accepted); the spec edit lands LAST only so the amendment can
cite recorded numbers instead of predicted ones.

**Tech Stack:** Bash (`testkit/orm-suite.sh`, mirroring `testkit/dbal-suite.sh`), PHP ≥ 8.2
(PHPUnit 11, PHPStan L9) for the driver fix and the chaos harness, `doctrine/orm` 3.6.8 pinned
clone against `doctrine/dbal` 4.4.4, live Dockerized PG 17 / MySQL 8.4 / MariaDB 11.8.
**No Rust change. No `/proto` change. The ONLY production-code change in this slice is
`php/doctrine-dbal/src/Result.php::fetchFirstColumn()`.**

**Spec:** `ferro-spec-v0.2.md` (§14, §16.1, §17, §19.3, §20.3, §21, §22.2) — plus the three research
journals this plan argues from:
`.superpowers/sdd/2026-08-13-ferro-m1-s9-exit-gate/research-{bar,orm,residuals}.md`
(the summaries in the task prompt are lossy; the journals are the record).

---

## Global Constraints

Every task's requirements implicitly include this section. Facts below were verified at HEAD
`08aad1c` (branch `m1-build`) this session or measured live by the research probes (journal paths
above).

### Contract rules (non-negotiable, from `CLAUDE.md`)

- **Charter rule 1** — SPEC §21 decisions are binding. D-S8b-5 (no `lastInsertId()` emulation on
  PG; ORM-on-PG configures SEQUENCE) is *applied* by this slice, never re-litigated.
- **Charter rule 2** — `/proto` is the single source of truth. **This slice makes NO `/proto`
  change.** The one wire signal it discovers it WANTS (a per-statement `tx_writes_persisted` flag
  on the in-transaction EXEC terminal, Task 3 cell 2b) is recorded as a DEFERRED candidate by
  Task 6, not hand-rolled. If any task finds itself needing a new wire constant: STOP and raise it.
- **Charter rule 3** — the engine never transparently retries. The chaos harness ASSERTS this from
  the client side (an `Indeterminate` write is never re-issued; the at-most-once read-back).
- **Charter rule 4** — exactly one terminal per in-flight request. Cell 4 of the chaos harness is
  this rule's client-side mirror: a killed stream surfaces exactly one thrown terminal, never a
  hang, never a silent clean end.
- **Charter rule 6** — no read/write inference from SQL text. This is WHY Task 3's cell 2b is a
  measurement, not a fix: closing the client-side implicit-commit residual without a wire signal
  would require a PHP mirror of `ferro_classify`, which this rule forbids at the client tier.
- **Charter rule 7** — `php/client` stays dependency-free at runtime. The chaos killer sidecar and
  its `shell_exec('kill -9 …')` live under `tests/` (dev-time only) and use `ferro/client` itself
  for its poll — no `ext-pdo`, no `ext-posix`, no `ext-pcntl`.

### Slice-wide rules

1. **NO SPEC EDITS in Tasks 1–5 and 7.** Each task records the spec delta it forces in
   `docs/followups/2026-08-13-s9-spec-deltas.md` (append-only, one `### Task N` block each);
   Task 6's single author applies them all. Hard rule — parallel spec authorship produced the S8a
   (u)/(v) contradiction, and this milestone has repaired two such contradictions.
2. **JOURNAL AS YOU GO — MANDATORY.** Each task appends findings/measurements to
   `.superpowers/sdd/2026-08-13-ferro-m1-s9-exit-gate/task-<N>-notes.md` AS THEY HAPPEN. Eleven
   agent runs in this milestone died on session limits; the journal is what survives. A review or
   acceptance run whose evidence lives only in context is worth nothing.
3. **Every guard is mutation-proven.** Apply the named production mutation, run the guard, record
   RED output in the journal, restore. Each mutation below was traced through the real code while
   writing this plan (three of one earlier slice's four named mutations were no-ops — do not skip
   the RED run, and REPORT a mutation that comes back green instead of shrugging).
4. **DO NOT run `ci/local-gate.sh --live`** — its EXIT trap runs `docker compose down -v` and
   destroys the shared containers. Run the individual gates by hand (Task 7 lists them).
   Do not tear down or restart the testkit containers, ever.
5. **Live environment** (containers are UP and SHARED):
   ```
   FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro"
   FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro"
   FERRO_TEST_MARIADB_URL="mysql://ferro:ferro@127.0.0.1:33061/ferro"
   ```
6. **Concurrency hazard (§21 open item 3):** `php/doctrine-dbal`'s live tier uses FIXED-NAME
   fixtures in the shared `ferro` database, and the recorded acceptance runs (Tasks 4, 7) are
   themselves sensitive to concurrent load. **No other agent may run this repo's live tiers while
   Task 4 or Task 7 records numbers.** If a recorded run misbehaves, diagnose interference FIRST.
7. **Gates that must hold at every commit:** `php/client` and `php/doctrine-dbal` PHPUnit offline
   green; PHPStan level 9 clean on both `src/` trees; no Rust or `/proto` diff ever appears in
   `git status` (nothing in this slice compiles Rust beyond `cargo build -p ferrod` for harness
   binaries).
8. **Gate baselines at slice entry** (the numbers Task 7 must reproduce or explain): Rust
   **863 passed / 0 failed** live on PG 17 + MySQL 8.4 + MariaDB 11.8; `php/client` 727 tests
   (live 64/919, zero-skip); `php/doctrine-dbal` 291 tests (live 63/592, zero-skip);
   `testkit/dbal-suite.sh` exit 0 with baseline MATCH on all three backends
   (PG `730/828/3/7/354/2`, MySQL `730/871/2/9/341/4`, MariaDB `730/869/2/9/342/4` — re-verified
   at this HEAD by research-residuals, six runs).

### The renegotiated bar (DECIDED HERE; Task 6 carries the exact spec wording)

The M1 exit gate has four parts, and the word "green" appears in none of them (a green result line
was measured satisfiable with zero Ferro contact — §22.2 (z)):

1. **DBAL:** the §14 bar met on PG + MySQL + MariaDB — two runs per backend reproducing the
   identical result line AND identical ordered non-passing set, byte-matching
   `docs/dbal-suite/baseline/`, five-category triage with **categories (a) and (e) EMPTY**.
   (Already true at this HEAD; Task 7 re-proves it at the exit HEAD.)
2. **ORM:** the Doctrine ORM functional suite RUN and RECORDED on PG + MySQL (MariaDB additionally
   recorded) under the same runner discipline — **a measurement bar, not a green bar**: recorded
   stock-PDO comparator, two-run baseline exact-match, full five-category triage;
   **category (a) must be EMPTY** (a driver defect found by the run is FIXED before recording —
   hence Task 1); **category-(e) rows do not block exit** but each carries a filed follow-up with
   an explicit milestone assignment recorded in §22.2.
3. **Chaos:** the §20.3 kill-`ferrod` harness built and green for the named minimum cell set
   (Task 3), each cell mutation-proven RED.
4. **D12:** formally re-anchored by recorded amendment (§16.1 status note + §17) — never silently
   treated as passed.

**SQLite is REMOVED from every M1 acceptance sentence by amendment** (reason recorded, v0.1 words
quoted): no `ferro-backend-sqlite` exists, `AnyPool` is `{ Pg | Mysql }`; it re-enters with M2's
SQLite engine-owned mode, in the same exact-match + triage form.

**Deliberately NOT closed in this slice** (decision, recorded not buried): the PG bind-matrix
directions the ORM run measures (`F64 → numeric`, `I64 → float8`, `TEXT → x` — 10 tests) and the
sub-second `TIMESTAMPTZ` read refusal (16 tests). The first is engine work at the exit gate (it
would re-arm the "S9a is unreviewed" caveat on the fate-adjacent bind path); the second is a
§22.2 (ab) POLICY decision, not a bug fix. Both are triaged, filed with milestone assignments
(M2-entry), and named in §22.2 (ap). The `fetchFirstColumn` defect IS closed (Task 1) because it
is silent data loss in the driver tier and category (a) must be empty.

---

## Verified hazards — a naive implementation is WRONG

**The ORM harness (Tasks 2, 4)**

1. **Upstream ORM `TestUtil` discards `driverClass` silently and throws without `db_driver`**
   (`tests/Tests/TestUtil.php`, `mapConnectionParameters()` maps only `driver`;
   measured, research-orm step 1). Setting `db_driverClass` alone → throw; setting BOTH →
   `db_driver` wins and the suite runs **stock PDO under a Ferro-labeled banner**. The TestUtil
   must be REPLACED and the replacement grep-verified, exactly as `testkit/dbal-suite.sh:146-148`
   does.
2. **`TestUtil::mapConnectionParameters()` line 243 HARDCODES
   `$parameters['wrapperClass'] = DbalExtensions\Connection::class`** — the suite's own QueryLog
   wrapper that `OrmFunctionalTestCase`'s query-count assertions depend on, and
   `TestUtil::getConnection()` asserts that exact instance. Ferro's REQUIRED wrapper (§22.2 (ah))
   competes for the same single slot. Resolution (measured working): ONE line in
   `tests/Tests/DbalExtensions/Connection.php` — the parent import swapped from
   `Doctrine\DBAL\Connection` to `Ferro\DBAL\Wrapper\FerroConnection` (`FerroConnection` declares
   no constructor, so the QueryLog wiring is inherited unchanged). Without it either the
   query-count assertions die or `transactional()` masks `IndeterminateWriteException`.
3. **An unreset ORM database turns 48 non-passing into 227 with a triage that blames the driver**
   (measured — research-orm step 6b: 96 Schema-Tool + 34 dup-key + 7 NonUniqueResult). The probe's
   invalid "run 2" was a STALE DAEMON on a REUSED socket path over an unreset DB, and every log
   line looked right. Countermeasures, all mandatory: fail-closed container-side reset BEFORE
   ferrod launch (the `dbal-suite.sh` `run_reset` shape), a FRESH `mktemp -u` socket path per run
   (a stale daemon cannot own it), and the recordable banner.
4. **Unconfigured ORM-on-PG is catastrophic and NOT a harness bug: 1229 errors (~35% of the
   suite).** With the one-line suite-wide
   `setIdentityGenerationPreferences([PostgreSQLPlatform::class => GENERATOR_TYPE_SEQUENCE])`
   (hooked in `TestUtil::configureProxies()`, which receives the ORM `Configuration`): 47E + 1F.
   This configuration is **within-bar** — it is the documented D-S8b-5 adoption path, and
   upstream's own deprecation text recommends exactly it. The runner applies it for the PG leg
   only (`FERRO_ORM_PG_SEQUENCE=1`).
5. **Stock is NOT green on MySQL**: pdo_mysql vs MySQL 8.4 = 4 failures (ORM-3.6.8-vs-DBAL-4.4.4
   platform SQL-string drift, driver-independent; Ferro reproduces them byte-identically). The
   comparator is a recorded stock baseline, never the word "green".
6. **Version alignment was LUCK, not law**: the ORM clone resolved `doctrine/dbal` to exactly
   4.4.4 and PHPUnit 11.5.56 this month. The runner PINS `doctrine/dbal:4.4.4` by explicit
   `composer require` and asserts the resolved version — the `dbal-suite.sh:137-142` shape.
7. **ONE vendor tree, the CLONE's** (inverse of `dbal-suite.sh`): two Composer autoloaders in one
   process = PHPUnit version collision (measured in S8b). Ferro packages enter the clone's vendor
   via path repositories, and BOTH must be required explicitly at `@dev` (a dependency's path-repo
   does not propagate).
8. **`ferrod` refuses long socket paths** (`sun_path` 108 bytes) — sockets live in `/tmp` via
   `mktemp -u`, never under a session scratch dir.
9. **The MySQL `ferro` user has NO `CREATE DATABASE`** (deliberate, `testkit/mysql-init.sql`) —
   the reset SQL runs as root container-side and GRANTs `doctrine_orm_tests.*` to `ferro`.

**The driver defect (Task 1)**

10. **`Ferro\DBAL\Result::fetchFirstColumn()` silently truncates at a first-column boolean
    `false`** (`php/doctrine-dbal/src/Result.php:358-360` delegates to
    `FetchUtils::fetchFirstColumn`, whose loop is `while (($v = $result->fetchOne()) !== false)` —
    a VALUE collides with the end-of-data sentinel). Measured: `[false, true]` → `[]` through
    Ferro; `[false, true]` correct through stock pdo_pgsql (whose driver overrides the method
    natively — exactly the fix shape). The docblock at `Result.php:336-338` currently ARGUES FOR
    the delegation and must be rewritten with the fix, not left contradicting it. Blast radius:
    driver-level `fetchFirstColumn()` and everything over it (ORM `getSingleColumnResult`, DBAL
    `Connection::fetchFirstColumn`) — silent wrong answers, not exceptions. DBAL's wrapper-level
    `iterateColumn()` has the same wart UPSTREAM (stock truncates there too) — out of scope, note
    only.
11. **The fix may move recorded baselines.** The DBAL subset's 32 baseline non-passing tests do
    not touch `fetchFirstColumn` (verified list: ExceptionTest / WriteTest / TransactionTest /
    MySQLSchemaManagerTest / replica), and currently-passing uses see identical values for
    non-`false` columns — but Task 7's `dbal-suite.sh` re-run is the proof, not this argument.
    The ORM baseline is recorded AFTER the fix (Task 4 depends on Task 1) so GH9230 bool=false
    lands as a PASS, not a category-(a) row.

**The kill-`ferrod` harness (Task 3)**

12. **`TxHandle::run()` catches ONLY `CodecException`** (`TxHandle.php:220-224`) — an
    in-transaction transport loss surfaces the RAW `ConnectionLostException`/`TransportException`;
    `OpKind::TxStatement` exists but has ZERO call sites (grep at HEAD). The client-side §19.3
    in-tx row is implemented as an unbranched connection-shaped error. The harness asserts the
    SAFETY FLOOR (never `IndeterminateException`; prefix unpersisted on plain DML) and Task 6
    RECORDS the class fact in §22.2 (aq) — do not "fix" the class in this slice.
13. **`ReconnectLoop::reconnect()` throws the RAW last dial error at exhaustion**
    (`ReconnectLoop.php:108`), and it is called from INSIDE `dispatchAutocommit`'s catch arm
    (`Connection.php:~1109`) — so a read that dies while the daemon stays down surfaces
    `TransportException`/`ConnectionLostException`, not the classified `RetryableException`
    §19.2's prose suggests. Cell 3 therefore asserts never-`Indeterminate` at the kill, and proves
    §19.2 (transparent reconnect + changed epoch) deterministically AFTER `restartFerrod()`.
14. **The client cannot know `tx_writes_persisted` across a dead daemon** — the latch dies with
    the engine and no wire field carries it. Cell 2b MEASURES the consequence on MySQL (the
    implicitly-committed prefix survives with no COMMIT ever sent, while the client reports a
    non-`Indeterminate` connection error) and PINS it as a test, the same way §22.2 (ac)'s
    cry-wolf is pinned. The fix is a `/proto` deferral candidate, filed by Task 5, recorded by
    Task 6.
15. **In-flight proof discipline is not optional** (§20.3, learned live in S6/S9a): the marker is
    a string-literal predicate (`WHERE '<marker>' <> ''` — MariaDB strips comments from
    `processlist.INFO`), the MySQL poll filters `COMMAND IN ('Execute','Query')`, and the poll
    must not match ITSELF — closed here by PARAM-BINDING the `LIKE` pattern (a prepared
    statement's `pg_stat_activity.query`/`processlist.INFO` shows placeholder text, not values)
    plus a `NOT LIKE` belt. Task 3's smoke step VERIFIES the self-match immunity on both backends
    before any cell relies on it.
16. **The killer polls THROUGH the same daemon** (a second session; `ferro/client` itself, no PDO)
    — this requires the pool to hand out a second connection while the sleeping statement pins the
    first. Verify the launched pool's `max_size` default covers ≥ 2 concurrent checkouts
    (`engine/crates/ferrod/src/config.rs`); if it does not, set the pool-size env knob in
    `LiveTestCase::launchFerrod`'s env for this test class only (subclass override), and journal
    it.
17. **`proc_terminate` can only signal a process the CALLER owns**, and `LiveTestCase::$proc` is
    private. Task 3 adds a `protected function ferrodPid(): int` accessor (via
    `proc_get_status()['pid']`) — the killer sidecar and the in-test `kill -9` both use the raw
    pid. `restartFerrod()` already copes with an externally-killed daemon
    (`procStatus()['running'] === false` → skips terminate, `proc_close` reaps).
18. **`Connection::stream()` declares `readonly: true` on the wire** (`Connection.php:452`) — cell
    4's never-`Indeterminate` assertion is the §22.2 (ac) guarantee's client-side vantage, valid
    only because of that declaration. The pump's loss surface is `Connection.php:493-519`
    (`readStreamFrame` catch at :496-499, `sendWindowUpdate` catch at :513-518).

**Recording discipline (Tasks 4, 7)**

19. **The S8c DBAL numbers HOLD at this HEAD** (research-residuals: six runs, byte-identical) —
    if Task 7's re-run drifts, something in THIS slice did it (candidate: Task 1's fix, hazard
    11); diagnose against the ordered non-passing diff the runner prints, never shrug.
20. **A run with any narrowing argument is a DEBUG run** — the runner refuses to compare or update
    baselines (the `dbal-suite.sh:83-96` banner logic, copied). A recorded number comes only from
    a full, reset, recordable run, twice.

---

## File Structure

**Created**
- `testkit/orm-suite.sh` — the ORM acceptance runner (Task 2; the deliverable the ORM half of the
  bar is judged by).
- `testkit/orm/TestUtil.ferro.php` — the replacement TestUtil (Task 2; honours `db_driverClass`
  with the stock `db_driver` comparator branch; no-op `initializeDatabase`; SEQUENCE identity
  preference hook).
- `testkit/orm/bootstrap.php` — upstream TestInit + the contact/wrapper/round-trip assertions,
  mode-aware (Task 2).
- `testkit/orm/reset-pg.sql`, `testkit/orm/reset-mysql.sql` — the fail-closed container-side
  resets (Task 2).
- `docs/orm-suite/baseline/{pg,mysql,mariadb}.txt` and `.../baseline/stock-{pg,mysql,mariadb}.txt`
  — the committed non-passing baselines (Task 4).
- `docs/orm-suite/2026-08-13-results.md` — the recorded ORM results + triage (Task 4).
- `php/client/tests/Live/DaemonKillFateLiveTest.php` — the §20.3 kill-`ferrod` harness (Task 3).
- `php/client/tests/Support/chaos_killer.php` — the in-flight-observing SIGKILL sidecar (Task 3).
- `docs/followups/2026-08-13-s9-spec-deltas.md` — append-only spec-delta ledger (all tasks;
  Task 6 consumes).
- `docs/followups/2026-08-13-orm-timestamptz-subsecond-read-refusal.md` (Task 5).
- `docs/followups/2026-08-13-client-side-implicit-commit-daemon-death.md` (Task 5).
- `docs/followups/2026-08-13-m1-exit-maintainer-items.md` — the human-decision ledger (Task 7).

**Modified**
- `php/doctrine-dbal/src/Result.php` (:336-360 region ONLY) + `tests/Unit/ResultTest.php` (Task 1)
  — the slice's only production-code change.
- `php/client/tests/Live/LiveTestCase.php` — additive `ferrodPid()` accessor (Task 3).
- `docs/followups/2026-08-11-i64-above-2e32-unreadable-in-php-client.md`,
  `docs/followups/2026-08-11-pg-bind-matrix-narrower-than-libpq.md`,
  `docs/followups/2026-08-11-pg-int2vector-blocks-the-schema-manager.md`,
  `docs/followups/2026-08-10-s8b-nil-server-version-decision.md` — closure/status annotations
  (Task 5).
- `docs/known-incompatibilities.md` — ORM section + daemon-death additions (Task 5).
- `testkit/migrations/cli-config.php` — the missing `wrapperClass` (Task 5).
- `UPSTREAM_PR.md` — verify/add the fourth vendored accessor (Task 5).
- `ferro-spec-v0.2.md` — §2, §14, §15, §16.1, §17, §19.3, §20.3, §21, §22.2 (Task 6 ONLY).
- `CLAUDE.md` — Current state + Next up (Task 6 ONLY).

**Explicitly NOT modified**
- `engine/` (any crate), `/proto`, `vendor forks`, `php/client/src/`, `php/doctrine-dbal/src/`
  beyond `Result.php` — if a task believes it needs to, STOP and raise it.
- `testkit/dbal-suite.sh` and `docs/dbal-suite/baseline/` — the DBAL bar is already met; Task 7
  re-proves it unchanged.

---

## Sequencing — who may run when, and why

| Wave | Tasks | Mode | Reason |
|---|---|---|---|
| A | 1, 2, 3 | **PARALLEL** (up to 3 implementers) | Disjoint files: T1 owns `php/doctrine-dbal`; T2 owns `testkit/orm*`; T3 owns `php/client/tests`. |
| B | 4 | after T1 AND T2 merged | The recorded ORM baseline must include T1's fix (or GH9230 lands as a category-(a) row and the bar's (a)-EMPTY clause is violated on day one); it runs T2's runner. |
| C | 5 | after T3 AND T4 merged | `known-incompatibilities.md` and the follow-up filings cite what T3 measured (the daemon-death residual) and what T4 recorded (temp-table cluster, bind directions, TIMESTAMPTZ cluster). |
| D | 6 | after ALL of 1–5, **single author** | The spec amendment cites recorded numbers and filed follow-ups; serialized spec authorship is a hard rule. |
| E | 7 | LAST | Re-runs every gate at the exit HEAD; writes the maintainer ledger. |

---

## Jobs → tasks coverage map

| Exit-gate job | Task(s) | Acceptance |
|---|---|---|
| Renegotiate the bar (§17/§14 wording; "green" retired) | plan §"renegotiated bar" + 6 | spec text matches this plan verbatim; §22.2 (ao) quotes what it retracts |
| Run the ORM functional suite (first ever) | 2, 4 | two-run exact-match baselines committed for PG+MySQL+MariaDB, ferro + stock comparator; triage with (a) EMPTY |
| Formally remove SQLite | 6 | §14 amendment quoting the v0.1 words + §17 + §22.2 (ao) |
| §20.3 kill-`ferrod` harness (spec already assigns it to this bar) | 3 | 4 cells + 1 pinned residual, live on PG (+ MySQL where the cell exists), each mutation-proven RED |
| Category-(a) driver defect from the ORM probe | 1 | `[false,true]` survives `fetchFirstColumn`; mutation RED; ORM GH9230 flips to pass in T4 |
| D12 re-anchor, D7 surface, fork/CVE ledger | 5, 6, 7 | §16.1 status note; maintainer ledger doc with named owners |
| Doc-truth hygiene (closed followups, cli-config wrapperClass) | 5 | each file's status header matches reality |

---

## Task 1: Fix the `fetchFirstColumn` boolean-`false` silent truncation (category (a))

The ORM probe confirmed silent data loss in the driver tier: a first-column boolean `false`
collides with DBAL `FetchUtils`' end-of-data sentinel (`while (($v = fetchOne()) !== false)`) and
truncates the result — `[false, true]` → `[]`, measured, no exception (research-orm step 5). Stock
pdo drivers are immune because they override `fetchFirstColumn()` natively; this task gives Ferro
the same override, built on `fetchNumeric()` (whose `false` return is unambiguous: a ROW is never
`false`).

**Files:**
- Modify: `php/doctrine-dbal/src/Result.php:336-360` (the `fetchOne`-family docblock + the
  `fetchFirstColumn` body)
- Test: `php/doctrine-dbal/tests/Unit/ResultTest.php`

**Interfaces:**
- Consumes: `Ferro\DBAL\Result::fetchNumeric(): array|false` (existing, one of the two native
  cursor methods); `Result::buffered(list<string> $cols, list<list<mixed>> $rows, int $affected)`
  (the existing unit-test constructor, `ResultTest.php:32`).
- Produces: `Result::fetchFirstColumn(): list<mixed>` — same signature, no longer delegating to
  `FetchUtils`. Task 4 relies on ORM `GH9230Test`'s bool=false dataset now passing.

- [ ] **Step 1: Write the failing test**

Append to `php/doctrine-dbal/tests/Unit/ResultTest.php` (inside the class):

```php
    /**
     * M1-S9 Task 1 — the GH9230 silent-truncation defect, measured by the first-ever ORM suite
     * run (research-orm step 5): DBAL `FetchUtils::fetchFirstColumn()`'s loop is
     * `while (($v = fetchOne()) !== false)`, so a first-column boolean FALSE is indistinguishable
     * from end-of-data and the result silently truncates — `[false, true]` came back as `[]`
     * through Ferro while stock pdo_pgsql (which overrides the method natively) returned it
     * correctly. Silent wrong ANSWERS, not errors: the worst defect class this project has.
     *
     * The fix builds the column off {@see Result::fetchNumeric()}, whose `false` is unambiguous
     * (a ROW is never the value false). NULL cells must survive too — `null !== false` kept them
     * alive even under FetchUtils, so a fix that drops them would be a regression.
     */
    public function testFetchFirstColumnSurvivesABooleanFalseFirstColumn(): void
    {
        self::assertSame(
            [false, true],
            Result::buffered(['b'], [[false], [true]], 2)->fetchFirstColumn(),
            'a leading boolean false must not truncate the column',
        );
        self::assertSame(
            [true, false, true],
            Result::buffered(['b'], [[true], [false], [true]], 3)->fetchFirstColumn(),
            'an interior boolean false must not truncate the column',
        );
        self::assertSame(
            [null, false, 0, ''],
            Result::buffered(['x'], [[null], [false], [0], ['']], 4)->fetchFirstColumn(),
            'every falsy value is a VALUE; only cursor exhaustion ends the column',
        );
    }
```

- [ ] **Step 2: Run it to verify it fails with the measured shape**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal && composer install --no-interaction --quiet \
  && ./vendor/bin/phpunit --filter testFetchFirstColumnSurvivesABooleanFalseFirstColumn
```

Expected: FAIL — `Failed asserting that two arrays are identical` with actual `[]` against expected
`[false, true]` (the exact truncation the probe measured). If it PASSES, stop: the defect is not
what the probe recorded — re-measure before touching anything.

- [ ] **Step 3: Implement the override**

In `php/doctrine-dbal/src/Result.php`, replace the `fetchFirstColumn` body (`:358-360`):

```php
    public function fetchFirstColumn(): array
    {
        // NOT FetchUtils::fetchFirstColumn(): its loop is `while (($v = fetchOne()) !== false)`,
        // and a first-column boolean FALSE is a VALUE that collides with that end-of-data
        // sentinel — the column silently truncates there (measured: [false, true] → [], while
        // stock pdo_pgsql returns it correctly because its driver overrides this method
        // natively, which is exactly what this override is). `fetchNumeric()`'s false is
        // unambiguous: a ROW is never the value false. DBAL's wrapper-level iterateColumn()
        // carries the same wart UPSTREAM (stock truncates there too) — that one is Doctrine's
        // to fix, not ours.
        $values = [];
        while (($row = $this->fetchNumeric()) !== false) {
            $values[] = $row[0];
        }

        return $values;
    }
```

Then fix the now-stale docblock above `fetchOne()` (`Result.php:336-338`): it currently cites the
`FetchUtils::fetchFirstColumn()` loop as the REASON to delegate. Rewrite that sentence to say the
`false`-vs-`null` distinction still matters for `fetchOne()` itself (end-of-result vs NULL cell),
and that `fetchFirstColumn()` is deliberately NOT delegated — pointing at the new override.

- [ ] **Step 4: Run the test + the full offline suite + PHPStan**

```bash
cd /home/abdullak/projects/ferro/php/doctrine-dbal \
  && ./vendor/bin/phpunit --filter testFetchFirstColumnSurvivesABooleanFalseFirstColumn \
  && ./vendor/bin/phpunit \
  && ./vendor/bin/phpstan analyse src --level 9
```

Expected: all green (291+ tests; the existing `testTheWholeFetchFamily` at `ResultTest.php:45`
asserts `[1, 2]` and must still pass — it proves the override didn't change the non-`false` path).

- [ ] **Step 5: MUTATION M1 — prove the guard can fail**

Revert the body to `return FetchUtils::fetchFirstColumn($this);`, run Step 2's command. Expected:
RED with actual `[]`. Journal the output
(`.superpowers/sdd/2026-08-13-ferro-m1-s9-exit-gate/task-1-notes.md`), restore the fix, re-run
green.

- [ ] **Step 6: Record the spec delta + journal**

Append to `docs/followups/2026-08-13-s9-spec-deltas.md`:

```markdown
### Task 1
- §22.2 (ap) must record: ONE category-(a) driver defect found by the first ORM run and FIXED
  before recording — `Ferro\DBAL\Result::fetchFirstColumn()` truncated at a first-column boolean
  false (FetchUtils sentinel collision); fix = native override off fetchNumeric(), the same
  immunity DBAL's own PDO drivers have. DBAL's wrapper-level iterateColumn() has the same wart
  UPSTREAM (out of scope, recorded).
```

- [ ] **Step 7: Commit**

```bash
cd /home/abdullak/projects/ferro && git add php/doctrine-dbal docs/followups/2026-08-13-s9-spec-deltas.md \
  && git commit -m "fix(m1-s9): fetchFirstColumn no longer truncates at a boolean false — a value is not a sentinel"
```

---

## Task 2: The ORM harness — runner, TestUtil, bootstrap, resets (no recorded runs yet)

Build the machinery that makes an ORM number RECORDABLE: a runner with the same fail-closed
discipline as `testkit/dbal-suite.sh` (read that file IN FULL first — every guard in it was paid
for), the replacement TestUtil, the mode-aware bootstrap with the contact assertions, and the
container-side resets. **This task ends with smoke runs (one filtered test per mode), NOT with
recorded numbers** — recording is Task 4, after Task 1's fix lands.

**Files:**
- Create: `testkit/orm-suite.sh`, `testkit/orm/TestUtil.ferro.php`, `testkit/orm/bootstrap.php`,
  `testkit/orm/reset-pg.sql`, `testkit/orm/reset-mysql.sql`
- Test: the smoke + fail-closed steps below (the runner is itself the test artifact)

**Interfaces:**
- Consumes: `testkit/dbal-suite.sh` (the model — copy its guard shapes, not its DBAL specifics);
  `Ferro\DBAL\Driver`, `Ferro\DBAL\Wrapper\FerroConnection`, `Ferro\Client\Connection` (existing).
- Produces: `testkit/orm-suite.sh` with env contract
  `FERRO_ORM_SVC={pg|mysql|mariadb}`, `FERRO_ORM_MODE={ferro|stock}`, `FERRO_ORM_TAG` (default
  `3.6.8`), `FERRO_ORM_DBAL_PIN` (default `4.4.4`), `FERRO_ORM_BASELINE=update`, `--no-reset`;
  exit 0 ⇔ recordable run whose non-passing set byte-matches
  `docs/orm-suite/baseline/{stock-}?<svc>.txt`. Task 4 and Task 7 call it exactly this way.

- [ ] **Step 1: The reset SQL files**

`testkit/orm/reset-pg.sql`:

```sql
-- M1-S9: the ORM suite's fail-closed reset. Runs container-side as the ferro superuser against
-- the maintenance DB (never doctrine_orm_tests itself — you cannot drop the database you are in).
-- WITH (FORCE) severs any backend still pinning it: the measured failure mode was a stale ferrod
-- holding 2 pooled connections, the DROP silently refused, and 48 non-passing becoming 227 with
-- a triage that blamed the driver (research-orm step 6b).
DROP DATABASE IF EXISTS doctrine_orm_tests WITH (FORCE);
CREATE DATABASE doctrine_orm_tests OWNER ferro;
```

`testkit/orm/reset-mysql.sql`:

```sql
-- Runs as root: the ferro user deliberately has NO CREATE DATABASE (testkit/mysql-init.sql).
DROP DATABASE IF EXISTS doctrine_orm_tests;
CREATE DATABASE doctrine_orm_tests;
GRANT ALL PRIVILEGES ON doctrine_orm_tests.* TO 'ferro'@'%';
FLUSH PRIVILEGES;
```

- [ ] **Step 2: The replacement TestUtil**

Clone the pinned ORM source once so the file can be DERIVED, not invented (the runner will re-use
this clone):

```bash
mkdir -p /home/abdullak/projects/ferro/.orm-suite \
  && git clone --depth 1 --branch 3.6.8 https://github.com/doctrine/orm.git \
       /home/abdullak/projects/ferro/.orm-suite/orm-3.6.8 2>/dev/null || true
cp /home/abdullak/projects/ferro/.orm-suite/orm-3.6.8/tests/Tests/TestUtil.php \
   /home/abdullak/projects/ferro/testkit/orm/TestUtil.ferro.php
```

Read the copied file IN FULL, then apply exactly these edits (everything else stays verbatim —
the file must keep upstream's public API: `getConnection`, `getPrivilegedConnection`,
`configureProxies`, and whatever private helpers the kept methods call). **If the private-method
surface differs materially from what is described below (research-orm step 1 measured it at tag
3.6.8: `getTestConnectionParameters()` throws without `db_driver`; `mapConnectionParameters()`
maps only `driver` and hardcodes `wrapperClass` at :243) — STOP and report, do not improvise.**

(a) A marker header comment as the FIRST line inside the class docblock:

```php
 * FERRO ORM HARNESS TESTUTIL (M1-S9) — replaces upstream tests/Tests/TestUtil.php. Changed vs
 * 3.6.8: getTestConnectionParameters() honours db_driverClass (Ferro) with a db_driver stock
 * branch (the recorded comparator); initializeDatabase() is a no-op (the container-side reset in
 * testkit/orm-suite.sh owns idempotence — PHP holds no credentials, SPEC §12/D8);
 * configureProxies() applies the documented D-S8b-5 SEQUENCE identity preference when
 * FERRO_ORM_PG_SEQUENCE=1. Everything else is upstream verbatim.
```

(b) Replace the connection-parameter derivation (upstream's `getTestConnectionParameters()` +
`mapConnectionParameters()` pair) so it builds the array DIRECTLY — both modes keep the suite's
own `DbalExtensions\Connection` wrapper slot (upstream behavior; in ferro mode the runner's parent
patch makes that class extend `FerroConnection`, which is how §22.2 (ah) is satisfied WITHOUT
losing the QueryLog):

```php
    /** @return array<string, mixed> */
    private static function getTestConnectionParameters(): array
    {
        if (isset($GLOBALS['db_driverClass'])) {
            // The Ferro branch. Upstream's mapConnectionParameters() maps ONLY 'driver' — a
            // db_driverClass was silently DISCARDED, which is how a Ferro-labeled run silently
            // measures a stock PDO driver (research-orm step 1). Built directly instead.
            return [
                'driverClass'   => $GLOBALS['db_driverClass'],
                'wrapperClass'  => DbalExtensions\Connection::class,
                'unix_socket'   => $GLOBALS['db_unix_socket'],
                'dbname'        => $GLOBALS['db_dbname'] ?? 'doctrine_orm_tests',
                'driverOptions' => json_decode(
                    (string) ($GLOBALS['db_driver_options'] ?? '{}'),
                    true,
                    512,
                    JSON_THROW_ON_ERROR,
                ),
            ];
        }

        if (isset($GLOBALS['db_driver'])) {
            // The STOCK comparator branch (pdo_pgsql / pdo_mysql) — the baseline any Ferro number
            // is judged against. Same wrapper slot, same reset discipline.
            return [
                'driver'       => $GLOBALS['db_driver'],
                'wrapperClass' => DbalExtensions\Connection::class,
                'host'         => $GLOBALS['db_host'],
                'port'         => (int) $GLOBALS['db_port'],
                'user'         => $GLOBALS['db_user'],
                'password'     => $GLOBALS['db_password'],
                'dbname'       => $GLOBALS['db_dbname'],
            ];
        }

        throw new InvalidArgumentException(
            'neither db_driverClass (Ferro) nor db_driver (stock comparator) is set — this '
            . 'harness refuses to guess a driver; testkit/orm-suite.sh sets exactly one',
        );
    }
```

Route the privileged-connection parameter helper (zero external call sites in ORM — measured) to
the same array, and make `initializeDatabase()` a documented no-op (keep the signature; body =
one comment pointing at the runner's reset).

> **PLAN-VERIFY MAJOR — the ferro branch omits `'driver'`, and the KEPT upstream `getConnection()`
> reads it unconditionally.** Upstream's `getConnection()` (which this step keeps verbatim) does a
> SQLite check on `$connectionParameters['driver']` with no `isset`. In ferro mode that key is
> absent, so every one of the suite's thousands of `getConnection()` calls emits an
> "Undefined array key 'driver'" E_WARNING under `error_reporting(E_ALL)` (ORM's `TestInit` sets
> it). The research probe's own replacement evidently guarded this — its recorded result lines are
> clean — but the shape SPECIFIED here does not, and "everything else stays verbatim" forbids the
> implementer from quietly fixing it.
> **Required:** name the guard explicitly in this step — change that one read to
> `$connectionParameters['driver'] ?? null` (or add `'driver' => null` to the ferro array, but the
> `?? null` is the smaller deviation from verbatim and does not risk a stock-driver code path
> mistaking a null driver for a configured one).

(c) In `configureProxies(Configuration $config)` KEEP the upstream body (proxy dir + namespace)
and insert the block below **AT THE TOP OF THE METHOD, BEFORE ANY OTHER STATEMENT.**

> **PLAN-VERIFY BLOCKER (MEASURED, and this is not a style preference).** The original instruction
> here said "APPEND". Upstream `configureProxies()` at ORM 3.6.8 opens with
> `if (PHP_VERSION_ID >= 80400 && $enableNativeLazyObjects) { …enableNativeLazyObjects(true); return; }`
> — and the harness environment is PHP 8.4.18 with native lazy objects ON (research-orm's own
> manifest). An appended block therefore sits AFTER an early `return` and **never executes**. The
> measured cost is the D-S8b-5 number itself: **1229 errors, ~35% of the suite** — and it surfaces
> with the exact D-S8b-5 error text, so the failing run looks like a genuine finding about PG
> identity strategy rather than a broken harness. That mis-triage is the real damage; the crash is
> the cheap part. Insert FIRST, and have Step 7's smoke assert the preference actually took effect
> rather than merely that the file parses.

```php
        if (getenv('FERRO_ORM_PG_SEQUENCE') === '1') {
            // D-S8b-5, the documented ORM-on-PostgreSQL adoption path (SPEC §14): IDENTITY is
            // ORM's DBAL-4 default for PG, IdentityGenerator::generateId() is
            // `(int) $conn->lastInsertId()`, and PG reports no generated key through Ferro BY
            // DESIGN. Upstream's own deprecation text recommends exactly this preference. This is
            // WITHIN-BAR: testing the product as documented, not rigging the harness. Measured
            // cost of omitting it: 1229 errors (~35% of the suite) — research-orm step 6.
            $config->setIdentityGenerationPreferences([
                \Doctrine\DBAL\Platforms\PostgreSQLPlatform::class
                    => \Doctrine\ORM\Mapping\ClassMetadata::GENERATOR_TYPE_SEQUENCE,
            ]);
        }
```

(Verify the exact constant name against the clone — `ClassMetadata::GENERATOR_TYPE_SEQUENCE` at
3.6.8; `ClassMetadataFactory::determineIdGeneratorStrategy()` consults the preference FIRST,
`src/Mapping/ClassMetadataFactory.php:622`.)

- [ ] **Step 3: The bootstrap**

`testkit/orm/bootstrap.php`:

```php
<?php // testkit/orm/bootstrap.php — M1-S9

declare(strict_types=1);

// ONE autoloader: the ORM CLONE's vendor tree (the inverse of testkit/dbal/bootstrap.php, and for
// the same measured reason — two Composer autoloaders answer for two PHPUnit builds and the
// runner dies before the first test). The clone's vendor carries doctrine/orm's dev deps,
// doctrine/dbal at the asserted pin, AND ferro/client + ferro/doctrine-dbal-driver through the
// path repositories testkit/orm-suite.sh configures.
$src = getenv('FERRO_ORM_SRC');
if ($src === false || $src === '') {
    fwrite(STDERR, "FERRO_ORM_SRC is unset\n");
    exit(1);
}

// Upstream's own bootstrap: clone-vendor autoload + proxy dir setup.
require $src . '/tests/Tests/TestInit.php';

$mode = getenv('FERRO_ORM_MODE') ?: 'ferro';

// -------------------------------------------------------------------------------------------------
// THE CONTACT ASSERTION — the whole reason this file exists. The S8b gate measured a green run
// (`OK (105 tests, 211 assertions)`) against in-memory SQLite with ZERO Ferro contact; ORM's
// TestUtil throws instead of falling back, but a db_driver var smuggled into any config runs stock
// PDO under a Ferro-labeled banner just as silently. Only asking the connection what it IS closes
// the class.
// -------------------------------------------------------------------------------------------------
$conn   = Doctrine\Tests\TestUtil::getConnection();
$native = $conn->getNativeConnection();

if ($mode === 'ferro') {
    if (! $native instanceof Ferro\Client\Connection) {
        fwrite(STDERR, sprintf(
            "FERRO CONTACT ASSERTION FAILED: the suite's connection is a %s, not a Ferro one.\n"
            . "Refusing to run: a green result here would mean nothing.\n",
            get_debug_type($native),
        ));
        exit(1);
    }
    // The WRAPPER assertion, transitive form: the suite's own QueryLog wrapper
    // (Doctrine\Tests\DbalExtensions\Connection — hardcoded by TestUtil, and OrmFunctionalTestCase
    // asserts that exact class) must EXTEND Ferro's REQUIRED FerroConnection (§22.2 (ah)). This
    // single check also proves the runner's one-line parent patch actually applied — an unpatched
    // clone fails HERE, before the first test, not 3485 tests later in a masked fate.
    if (! is_subclass_of(Doctrine\Tests\DbalExtensions\Connection::class, Ferro\DBAL\Wrapper\FerroConnection::class)) {
        fwrite(STDERR,
            "FERRO WRAPPER ASSERTION FAILED: Doctrine\\Tests\\DbalExtensions\\Connection does not "
            . "extend Ferro\\DBAL\\Wrapper\\FerroConnection — the DbalExtensions parent patch did "
            . "not apply. Without it transactional() masks IndeterminateWriteException (§22.2 (ah)).\n");
        exit(1);
    }
} else {
    // STOCK comparator: assert the DUAL, so a mislabeled run can never publish under the wrong
    // banner (both directions of the same lie).
    if (! $native instanceof PDO || $native instanceof Ferro\Client\Connection) {
        fwrite(STDERR, sprintf(
            "STOCK COMPARATOR ASSERTION FAILED: expected a PDO native connection, got %s.\n",
            get_debug_type($native),
        ));
        exit(1);
    }
    fwrite(STDOUT, "[ferro-orm] MODE: STOCK COMPARATOR — this run measures the PDO baseline, not Ferro\n");
}

// The round trip: a connection object proves wiring; a row proves an engine.
$one = $conn->fetchOne('SELECT 1');
if ((int) $one !== 1) {
    fwrite(STDERR, "SELECT 1 round trip failed (got " . var_export($one, true) . ")\n");
    exit(1);
}

fwrite(STDOUT, sprintf(
    "[ferro-orm] contact: native=%s wrapper=%s platform=%s\n",
    get_debug_type($native),
    get_debug_type($conn),
    get_debug_type($conn->getDatabasePlatform()),
));
```

- [ ] **Step 4: The runner**

`testkit/orm-suite.sh` — the `dbal-suite.sh` guard set, re-derived for ORM. Write it in full;
the load-bearing differences from the DBAL runner are flagged `# ORM-DIFF`:

```bash
#!/usr/bin/env bash
# M1-S9: run doctrine/orm's own functional suite against Ferro (mode=ferro) or the stock PDO
# comparator (mode=stock). Modeled on testkit/dbal-suite.sh — read that file first; every guard
# here was paid for there or by the M1-S9 ORM probe (research-orm.md).
#
# NO `docker compose down` TRAP OF ANY KIND. The only EXIT trap kills the ferrod THIS script
# started and removes its socket.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${FERRO_ORM_TAG:-3.6.8}"
dbal_pin="${FERRO_ORM_DBAL_PIN:-4.4.4}"
pool="${FERRO_ORM_POOL:-default}"
svc="${FERRO_ORM_SVC:-pg}"
mode="${FERRO_ORM_MODE:-ferro}"

case "$svc" in
  pg)      want_scheme=postgres; want_port=55432; pdo_driver=pdo_pgsql ;;
  mysql)   want_scheme=mysql;    want_port=33060; pdo_driver=pdo_mysql ;;
  mariadb) want_scheme=mysql;    want_port=33061; pdo_driver=pdo_mysql ;;
  *) echo "::error:: unknown FERRO_ORM_SVC=$svc (expected: pg | mysql | mariadb)"; exit 1 ;;
esac
case "$mode" in ferro|stock) ;; *) echo "::error:: FERRO_ORM_MODE must be ferro|stock"; exit 1 ;; esac

# The svc/DSN agreement guard, verbatim shape from dbal-suite.sh step 0 (the measured failure it
# closes: reset one backend, test another, publish a third number under the wrong name).
dsn="${FERRO_ORM_DSN:-$want_scheme://ferro:ferro@127.0.0.1:$want_port/doctrine_orm_tests}"
dsn_scheme="${dsn%%://*}"
dsn_authority="${dsn#*://}"; dsn_authority="${dsn_authority%%/*}"
dsn_hostport="${dsn_authority##*@}"
dsn_port="${dsn_hostport##*:}"
if [ "$dsn_port" = "$dsn_hostport" ]; then dsn_port=""; fi
if [ "$dsn_scheme" != "$want_scheme" ] || [ "$dsn_port" != "$want_port" ]; then
  echo "::error:: FERRO_ORM_SVC and FERRO_ORM_DSN disagree about which backend this is."
  exit 1
fi
echo "[ferro-orm] backend: $svc via ${dsn_scheme}://${dsn_hostport}/doctrine_orm_tests · mode: $mode"

work="${FERRO_ORM_WORK:-$root/.orm-suite}"
src="$work/orm-$tag"
reset=1
args=()
for a in "$@"; do
  case "$a" in
    --no-reset) reset=0 ;;
    *) args+=("$a") ;;
  esac
done

# THE RECORDING BANNER, printed FIRST (dbal-suite.sh step: a --filter run keeps every checklist
# line true while executing 1/3485 of the suite — the log must say which kind of run this was).
narrowing=()
for a in "${args[@]+"${args[@]}"}"; do
  case "$a" in
    --display-*|--colors|--colors=*|--testdox|-v|--verbose|--debug|--log-*|--no-progress) ;;
    *) narrowing+=("$a") ;;
  esac
done
if [ ${#narrowing[@]} -eq 0 ] && [ "$reset" = 1 ]; then
  echo "[ferro-orm] recordable: yes (whole suite, reset applied, mode=$mode)"
else
  why=""
  if [ ${#narrowing[@]} -gt 0 ]; then why="run narrowed by: ${narrowing[*]}"; fi
  if [ "$reset" != 1 ]; then why="${why:+$why; }--no-reset"; fi
  echo "[ferro-orm] recordable: NO — $why"
fi

mkdir -p "$work"

# 1. The PINNED clone; tests/ restored to the tag on every invocation, refuse residual drift
#    (an ADDED test file survives checkout and silently changes the acceptance number —
#    measured in the S8b whole-branch review).
if [ ! -d "$src" ]; then
  git clone --depth 1 --branch "$tag" https://github.com/doctrine/orm.git "$src"
fi
git -C "$src" checkout -f "$tag" -- tests
dirty="$(git -C "$src" status --porcelain -- tests)"
if [ -n "$dirty" ]; then
  echo "::error:: the pinned ORM clone's tests/ differs from tag $tag after a hard restore:"
  echo "$dirty"; echo "          Delete $src and let the runner re-clone."; exit 1
fi
src_sha="$(git -C "$src" rev-parse HEAD)"

# 2. ORM-DIFF: ONE vendor tree — the CLONE's (the driver package's vendor has no ORM). Ferro
#    packages enter via path repositories; BOTH must be required explicitly at @dev (a path repo
#    does not propagate through a dependency), and doctrine/dbal is PINNED by explicit require —
#    the probe's exact-4.4.4 resolution was luck, not law (research-orm step 1).
(cd "$src" \
  && composer config repositories.ferro-client path "$root/php/client" \
  && composer config repositories.ferro-dbal path "$root/php/doctrine-dbal" \
  && composer require --no-interaction --no-progress --quiet --with-all-dependencies \
       "doctrine/dbal:$dbal_pin" "ferro/client:@dev" "ferro/doctrine-dbal-driver:@dev")
installed="$(cd "$src" && composer show doctrine/dbal 2>/dev/null | awk '$1=="versions" {print $NF}')"
if [ "$installed" != "$dbal_pin" ]; then
  echo "::error:: doctrine/dbal resolved to '$installed' in the ORM clone but the pin is '$dbal_pin'."
  echo "          The suite would test ORM-$tag against a DBAL the driver was never gated on."
  exit 1
fi

# 3. Mode patches. tests/ was just restored, so a stock run is UPSTREAM-CLEAN except TestUtil
#    (ours serves both modes — the stock branch keeps everything stock-shaped) and a ferro run
#    additionally re-parents the suite's QueryLog wrapper onto FerroConnection (§22.2 (ah); ONE
#    line; FerroConnection declares no constructor so the QueryLog wiring is inherited).
cp "$root/testkit/orm/TestUtil.ferro.php" "$src/tests/Tests/TestUtil.php"
grep -q 'FERRO ORM HARNESS TESTUTIL' "$src/tests/Tests/TestUtil.php" \
  || { echo "::error:: TestUtil patch did not apply"; exit 1; }
if [ "$mode" = ferro ]; then
  perl -0pi -e 's/use Doctrine\\DBAL\\Connection as BaseConnection;/use Ferro\\DBAL\\Wrapper\\FerroConnection as BaseConnection;/' \
    "$src/tests/Tests/DbalExtensions/Connection.php"
  grep -q 'FerroConnection as BaseConnection' "$src/tests/Tests/DbalExtensions/Connection.php" \
    || { echo "::error:: the DbalExtensions parent patch did not apply (did upstream rename the import?)"; exit 1; }
fi

# 4. THE RESET — fail-closed, BEFORE ferrod, dedicated database. Measured cost of skipping /
#    half-running it: 48 non-passing → 227 with a triage that blames the driver (research-orm 6b).
run_reset() { # <service> <client-binary> <sql-file>   (verbatim shape from dbal-suite.sh)
  local service="$1" client="$2" sql="$3" out status
  set +e
  out="$(docker compose -f "$root/testkit/docker-compose.yml" exec -T "$service" \
         "$client" -uroot -pferro < "$sql" 2>&1)"
  status=$?
  set -e
  printf '%s\n' "$out" | grep -v 'Using a password' | grep -v '^$' || true
  if [ "$status" -ne 0 ]; then
    echo "::error:: the $service reset FAILED (exit $status). Not recordable; refusing to continue."
    exit 1
  fi
}
if [ "$reset" = 1 ]; then
  case "$svc" in
    pg)
      docker compose -f "$root/testkit/docker-compose.yml" exec -T pg \
        psql -v ON_ERROR_STOP=1 -U ferro -d postgres -q < "$root/testkit/orm/reset-pg.sql"
      ;;
    mysql)   run_reset mysql   mysql   "$root/testkit/orm/reset-mysql.sql" ;;
    mariadb) run_reset mariadb mariadb "$root/testkit/orm/reset-mysql.sql" ;;
  esac
  echo "[ferro-orm] reset: $svc/doctrine_orm_tests"
else
  echo "[ferro-orm] reset: SKIPPED (--no-reset) — this run's numbers MUST NOT be recorded"
fi

# 5. ONE ferrod (ferro mode only), on a FRESH random socket — a stale daemon cannot own a path
#    that did not exist until now (the probe's run-2 disaster is structurally impossible here).
sock=""
if [ "$mode" = ferro ]; then
  cargo build -p ferrod --manifest-path "$root/Cargo.toml"
  sock="$(mktemp -u /tmp/ferro-orm-XXXXXX.sock)"
  env FERRO_SOCK="$sock" FERRO_POOLS="$pool" \
      "FERRO_POOL_$(echo "$pool" | tr '[:lower:]-' '[:upper:]_')_DSN=$dsn" \
      "$root/target/debug/ferrod" >"$work/ferrod.log" 2>&1 &
  ferrod_pid=$!
  trap 'kill "$ferrod_pid" 2>/dev/null || true; rm -f "$sock"' EXIT
  for _ in $(seq 1 100); do [ -S "$sock" ] && break; sleep 0.1; done
  [ -S "$sock" ] || { echo "::error:: ferrod did not create $sock"; cat "$work/ferrod.log"; exit 1; }
fi

# 6. The generated phpunit config. ORM-DIFF: upstream CI's own group exclusions (performance,
#    locking_functional) — the probe's 3485-test denominator is defined by them.
cfg="$work/phpunit.generated.xml"
{
  echo '<?xml version="1.0" encoding="UTF-8"?>'
  echo '<phpunit bootstrap="'"$root"'/testkit/orm/bootstrap.php" colors="true" cacheDirectory="'"$work"'/.phpunit.cache">'
  echo '  <testsuites><testsuite name="ferro-orm-functional">'
  echo "    <directory>$src/tests/Tests/ORM</directory>"
  echo '  </testsuite></testsuites>'
  echo '  <groups><exclude><group>performance</group><group>locking_functional</group></exclude></groups>'
  echo '  <php>'
  echo '    <env name="FERRO_ORM_SRC" value="'"$src"'"/>'
  echo '    <env name="FERRO_ORM_MODE" value="'"$mode"'"/>'
  if [ "$mode" = ferro ]; then
    echo '    <var name="db_driverClass" value="Ferro\DBAL\Driver"/>'
    echo '    <var name="db_unix_socket" value="'"$sock"'"/>'
    echo '    <var name="db_dbname" value="doctrine_orm_tests"/>'
    echo '    <var name="db_driver_options" value="{&quot;pool&quot;:&quot;'"$pool"'&quot;}"/>'
    if [ "$svc" = pg ]; then
      # D-S8b-5, the documented adoption path — WITHIN-BAR (SPEC §14; measured: 1229 errors without).
      echo '    <env name="FERRO_ORM_PG_SEQUENCE" value="1"/>'
    fi
  else
    echo '    <var name="db_driver" value="'"$pdo_driver"'"/>'
    echo '    <var name="db_host" value="127.0.0.1"/>'
    echo '    <var name="db_port" value="'"$want_port"'"/>'
    echo '    <var name="db_user" value="ferro"/>'
    echo '    <var name="db_password" value="ferro"/>'
    echo '    <var name="db_dbname" value="doctrine_orm_tests"/>'
  fi
  echo '  </php>'
  echo '</phpunit>'
} > "$cfg"

repo_sha="$(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo unknown)"
repo_dirty=""
if [ -n "$(git -C "$root" status --porcelain 2>/dev/null)" ]; then repo_dirty=" +local-changes"; fi
echo "[ferro-orm] tree: $repo_sha$repo_dirty · orm tests: $tag @ ${src_sha:0:12} · dbal: $dbal_pin · backend: $svc · mode: $mode"

# 7. Run with the CLONE's phpunit (one vendor tree). --log-junit always on: the junit is what the
#    baseline diff reads.
junit="$work/junit-$mode-$svc.xml"
set +e
"$src/vendor/bin/phpunit" -c "$cfg" --log-junit "$junit" "${args[@]+"${args[@]}"}"
phpunit_status=$?
set -e

# 8. THE BASELINE DIFF (verbatim mechanism from dbal-suite.sh step 8, including the extractor):
#    exit status becomes "does this match what we recorded", drift in EITHER direction reported,
#    only a recordable run may compare or update. ORM-DIFF: stock mode gates against its own
#    committed comparator baseline (stock-<svc>.txt) — "stock is not green on MySQL" is a
#    recorded, checkable artifact, not a sentence.
baseline_dir="$root/docs/orm-suite/baseline"
prefix=""; [ "$mode" = stock ] && prefix="stock-"
baseline="$baseline_dir/$prefix$svc.txt"
observed="$work/nonpassing-$mode-$svc.txt"

cat > "$work/nonpassing.php" <<'EXTRACTOR'
<?php
$xml = simplexml_load_file($argv[1]);
if ($xml === false) { fwrite(STDERR, "unreadable junit xml: {$argv[1]}\n"); exit(1); }
$out = [];
foreach ($xml->xpath('//testcase[failure or error]') as $tc) {
    $cls = (string) $tc['class'];
    $name = (string) $tc['name'];
    $out[] = $cls !== '' ? "$cls::$name" : $name;
}
$out = array_values(array_unique($out));
sort($out, SORT_STRING);
echo implode("\n", $out), $out === [] ? '' : "\n";
EXTRACTOR

if [ ${#narrowing[@]} -eq 0 ] && [ "$reset" = 1 ] && [ -f "$junit" ]; then
  php "$work/nonpassing.php" "$junit" > "$observed"
  observed_n=$(grep -c . "$observed" || true)
  if [ "${FERRO_ORM_BASELINE:-}" = "update" ]; then
    mkdir -p "$baseline_dir"
    cp "$observed" "$baseline"
    echo "[ferro-orm] baseline: UPDATED $prefix$svc.txt ($observed_n non-passing) — commit it with the results file"
    phpunit_status=0
  elif [ ! -f "$baseline" ]; then
    echo "::error:: no baseline at docs/orm-suite/baseline/$prefix$svc.txt ($observed_n non-passing observed)."
    echo "          Record one with: FERRO_ORM_BASELINE=update FERRO_ORM_SVC=$svc FERRO_ORM_MODE=$mode $0"
    exit 1
  elif diff -u "$baseline" "$observed" > "$work/baseline-$mode-$svc.diff" 2>&1; then
    echo "[ferro-orm] baseline: MATCHES docs/orm-suite/baseline/$prefix$svc.txt ($observed_n non-passing, exactly as recorded)"
    phpunit_status=0
  else
    echo "::error:: the non-passing set DRIFTED from docs/orm-suite/baseline/$prefix$svc.txt"
    sed -n '3,$p' "$work/baseline-$mode-$svc.diff"
    echo "          If intended: FERRO_ORM_BASELINE=update FERRO_ORM_SVC=$svc FERRO_ORM_MODE=$mode $0"
    phpunit_status=1
  fi
else
  echo "[ferro-orm] baseline: not compared (this run is not recordable)"
fi

exit "$phpunit_status"
```

```bash
chmod +x /home/abdullak/projects/ferro/testkit/orm-suite.sh
```

- [ ] **Step 5: Smoke — one test through Ferro on PG**

```bash
cd /home/abdullak/projects/ferro && FERRO_ORM_SVC=pg FERRO_ORM_MODE=ferro ./testkit/orm-suite.sh \
  --filter 'testBasicUnitsOfWorkWithOneToManyAssociation'
```

Expected: `[ferro-orm] recordable: NO — run narrowed by: --filter …`, the contact banner with
`native=Ferro\Client\Connection`, `OK (1 test, …)`, `baseline: not compared`. If the contact
assertion fires instead, the TestUtil or parent patch did not apply — fix before proceeding.

- [ ] **Step 6: Smoke — the stock comparator on PG**

```bash
cd /home/abdullak/projects/ferro && FERRO_ORM_SVC=pg FERRO_ORM_MODE=stock ./testkit/orm-suite.sh \
  --filter 'testBasicUnitsOfWorkWithOneToManyAssociation'
```

Expected: the `MODE: STOCK COMPARATOR` banner, `OK (1 test, …)`.

- [ ] **Step 7: The fail-closed proof (the reason to trust the harness)**

Run the FERRO-mode bootstrap against a STOCK-configured suite — hand-build a config that sets
`db_driver=pdo_pgsql` (no `db_driverClass`) but `FERRO_ORM_MODE=ferro`, and run one test with it:

```bash
sed 's/<var name="db_driverClass".*$//; s/name="FERRO_ORM_MODE" value="ferro"/name="FERRO_ORM_MODE" value="ferro"/' \
  /home/abdullak/projects/ferro/.orm-suite/phpunit.generated.xml > /tmp/ferro-orm-sabotage.xml
# then edit /tmp/ferro-orm-sabotage.xml: remove the db_driverClass/db_unix_socket/db_driver_options
# vars, add the five stock db_* vars (copy from a stock-mode generated config), KEEP mode=ferro.
/home/abdullak/projects/ferro/.orm-suite/orm-3.6.8/vendor/bin/phpunit -c /tmp/ferro-orm-sabotage.xml \
  --filter 'testBasicUnitsOfWorkWithOneToManyAssociation'; echo "exit=$?"
```

Expected: `FERRO CONTACT ASSERTION FAILED … not a Ferro one` and `exit=1` — **zero tests run.**
Journal the output verbatim: this is the artifact that distinguishes this harness from the one
S8b caught lying. Remove `/tmp/ferro-orm-sabotage.xml`.

- [ ] **Step 8: Record the spec delta + journal + commit**

Append to `docs/followups/2026-08-13-s9-spec-deltas.md`:

```markdown
### Task 2
- §20.3 upstream-suites bullet must name testkit/orm-suite.sh beside testkit/dbal-suite.sh, and
  must state the harness interventions BY NAME (replacement TestUtil; one-line DbalExtensions
  parent re-parenting onto FerroConnection; D-S8b-5 SEQUENCE preference on PG) — the claim is
  "upstream suite, replaced test HARNESS, documented configuration", never more.
```

```bash
cd /home/abdullak/projects/ferro && git add testkit/orm-suite.sh testkit/orm docs/followups/2026-08-13-s9-spec-deltas.md \
  && git commit -m "feat(m1-s9): the ORM acceptance harness — contact-asserted, fail-closed reset, pinned + baseline-gated"
```

---

## Task 3: The §20.3 kill-`ferrod` chaos harness (PHP-client vantage)

The spec already assigns this to the M1 bar (§20.3: "Killing `ferrod` itself is the untested half
and belongs in the M1 exit gate's bar") and every fate test in the tree observes the engine with
the daemon ALIVE. This task creates the missing event — SIGKILL mid-request — and asserts §19.3's
client half from the only vantage a production caller has. Four assertion cells + one pinned
MEASUREMENT (the client-side implicit-commit residual, correction C1). **No `php/client/src`
change: if a cell fails in a way that looks like a client defect, STOP, journal the evidence, and
raise it — that is a FINDING, the harness doing its job.**

**Files:**
- Create: `php/client/tests/Live/DaemonKillFateLiveTest.php`,
  `php/client/tests/Support/chaos_killer.php`
- Modify: `php/client/tests/Live/LiveTestCase.php` (additive `ferrodPid()` accessor only)
- Test: the created file (this task is all test)

**Interfaces:**
- Consumes: `LiveTestCase` (`setUp` spawns a private ferrod per test; `restartFerrod()` copes
  with an externally-killed daemon; `connectConnection(?RetryPolicy, string $pool)`;
  `requireMysqlPool()`; `MYSQL_POOL`; `$socketPath`); `Ferro\Ferro::connect(string, string,
  float, float, ?RetryPolicy)`; `Ferro\Client\Connection::{exec, scalar, begin, stream,
  currentEpoch, reconnectCount, lastReconnectEpochChanged, session}`;
  `Ferro\Client\Error\{FerroException, IndeterminateException}`.
- Produces: `DaemonKillFateLiveTest` (Task 6 cites its cells in §20.3/§22.2 (aq); Task 5 files the
  cell-2b follow-up); `LiveTestCase::ferrodPid(): int` (the killer's target).

- [ ] **Step 1: The `ferrodPid()` accessor**

In `php/client/tests/Live/LiveTestCase.php`, after `procStatus()`:

```php
    /**
     * The launched ferrod's OS pid — for the M1-S9 chaos tests, which must SIGKILL the daemon
     * OUT-OF-BAND while this process is blocked inside a client call ({@see $proc} is private and
     * `proc_terminate` needs the owning handle; a raw pid is the one name both the test and its
     * killer sidecar can share).
     */
    protected function ferrodPid(): int
    {
        if ($this->proc === null || !is_resource($this->proc)) {
            self::fail('ferrodPid(): no running ferrod process handle');
        }
        $s = proc_get_status($this->proc);
        return (int) $s['pid'];
    }
```

- [ ] **Step 2: The killer sidecar**

`php/client/tests/Support/chaos_killer.php`:

```php
<?php // /php/client/tests/Support/chaos_killer.php
declare(strict_types=1);

// M1-S9 chaos sidecar: watch for $marker as a PROVABLY IN-FLIGHT statement, then SIGKILL ferrod.
// Never sleep-and-hope — a kill that lands before dispatch proves nothing and passes for the
// wrong reason (§20.3 chaos discipline, learned live in S6/S9a). Exit codes:
//   0 = observed in flight, kill delivered · 2 = budget expired, NOTHING killed · 3 = usage/connect
//
// The poll rides a SECOND session through the SAME daemon (ferro/client itself — no ext-pdo; the
// daemon multiplexes, and the pool hands the poll its own connection while the sleeping statement
// pins another). Discipline notes, each measured:
//   - the marker is a string-literal predicate in the WATCHED statement (never a comment —
//     MariaDB strips comments from processlist INFO);
//   - the MySQL poll filters COMMAND IN ('Execute','Query') — without it the PREPARE phase
//     matches (the S6 C14 flake, and a §20.3-documented time bomb);
//   - the LIKE pattern is PARAM-BOUND so this poll can never match its own statement text (both
//     backends show placeholder text for a prepared statement), with a NOT LIKE belt besides.

require __DIR__ . '/../../vendor/autoload.php';

use Ferro\Ferro;

if ($argc !== 7) {
    fwrite(STDERR, "usage: chaos_killer.php <socket> <pool> <pg|mysql> <marker> <ferrod-pid> <budget-sec>\n");
    exit(3);
}
[, $socket, $pool, $family, $marker, $pid, $budget] = $argv;

try {
    $conn = Ferro::connect($socket, $pool, 2.0, 5.0);
} catch (\Throwable $e) {
    fwrite(STDERR, 'killer connect failed: ' . $e->getMessage() . "\n");
    exit(3);
}

$sql = $family === 'pg'
    ? "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE \$1"
        . " AND query NOT LIKE '%pg_stat_activity%'"
    : "SELECT count(*) FROM information_schema.processlist WHERE command IN ('Execute', 'Query')"
        . " AND info LIKE ? AND info NOT LIKE '%processlist%'";
$pattern = '%' . $marker . '%';

$deadline = microtime(true) + (float) $budget;
while (microtime(true) < $deadline) {
    try {
        if ((int) $conn->scalar($sql, [$pattern]) >= 1) {
            shell_exec('kill -9 ' . (int) $pid . ' 2>/dev/null');
            fwrite(STDOUT, "killed {$pid} after observing {$marker} in flight\n");
            exit(0);
        }
    } catch (\Throwable $e) {
        fwrite(STDERR, 'killer poll error: ' . $e->getMessage() . "\n");
        exit(3);
    }
    usleep(50_000);
}
fwrite(STDERR, "budget expired without observing {$marker} — NOTHING was killed\n");
exit(2);
```

- [ ] **Step 3: Verify the poll's self-match immunity and pool capacity (hazards 15, 16) BEFORE
  writing cells**

With the containers up, launch a throwaway ferrod (any existing live test does) or use the smoke
below; from `php -r`, connect two sessions, park `SELECT pg_sleep(5) WHERE 'probemark' <> ''` on
one via a background `php -r` process, and run the killer's poll SQL by hand on the other with
`['%probemark%']`: expected count ≥ 1 while the sleeper runs, 0 after; repeat the MySQL shape
against the mysql pool. Also confirm `engine/crates/ferrod/src/config.rs`'s default pool
`max_size` ≥ 2 (read the source). Journal both facts. If a pool-size knob must be set, override
`extraPoolDsns`/env in the test class per hazard 16 and journal the knob's name.

- [ ] **Step 4: The harness test file**

`php/client/tests/Live/DaemonKillFateLiveTest.php`:

```php
<?php // /php/client/tests/Live/DaemonKillFateLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\IndeterminateException;

/**
 * SPEC §20.3's kill-`ferrod` harness — the M1 exit bar's chaos part (M1-S9), and the FIRST test
 * anywhere in this repository in which the DAEMON dies mid-request (every prior chaos suite kills
 * the BACKEND link and the daemon survives to classify). The vantage is the PHP client through
 * the §19.2 reconnect loop, because that is what a production caller actually observes.
 *
 * Cells (each kill is PROVEN in flight — the sidecar observes the marker in
 * pg_stat_activity/processlist before killing; the stream cell is in flight by construction,
 * DATA frames already received):
 *   1. autocommit non-readonly EXEC  → IndeterminateException, never re-issued, at most once.
 *   2. in-transaction EXEC (plain DML) → NEVER Indeterminate; prefix proven unpersisted.
 *   2b. MySQL implicit-commit prefix  → MEASUREMENT pinning the client-side residual: the prefix
 *       IS durable (no COMMIT was ever sent) while the client cannot know (§22.2 (aq)).
 *   3. readonly-declared read → NEVER Indeterminate; after the daemon returns, the next read
 *      reconnects transparently with boot_epoch asserted CHANGED (§19.1/§19.2).
 *   4. open stream (PG only — MySQL-family streaming does not exist, §22.2 (n)) → exactly one
 *      thrown terminal, never a hang, never a clean end that silently truncates.
 */
> **PLAN-VERIFY MAJOR (MEASURED) — these cells must NOT use `LiveTestCase::connectConnection()`,
> or they can pass with the daemon ALIVE.**
>
> `LiveTestCase::connectConnection` hardcodes a **5.0 s io timeout** (`LiveTestCase.php:136-139`).
> `Transport.php:94-95` surfaces a read timeout as `TransportException('read timed out…')`;
> `Connection.php:1097` catches that **identically to a daemon death**, and `classifyLoss(Write)`
> mints the **same `IndeterminateException`**. Every cell parks a 30 s sleep and then races the
> killer (PHP CLI start + autoload + connect + 50 ms poll) against that 5 s client timeout. If the
> killer is slow — a loaded CI box, which is exactly where flakes live — the client times out
> first, the assertion passes, and the daemon is still running at classification time. The killer
> then still finds its marker inside its 20 s budget, kills, and exits 0, so nothing anywhere
> reports a problem. Cell 3 is worse under the same race: it reconnects to the LIVE daemon and
> re-issues the sleep (`maxAttempts` defaults to 3).
>
> This is the acceptance harness for the property the whole design exists to protect, so a cell
> that can pass for an unrelated reason is not a weak test — it is a false gate.
>
> **Required shape:** connect with an io timeout that EXCEEDS the parked sleep, so a client-side
> timeout is impossible inside a cell and the only two exits are the kill (throw) or completion
> (`self::fail`):
> `Ferro::connect($this->socketPath, $pool, 2.0, 60.0, $policy)`.
> **Belt as well as braces:** at catch time, assert the killer process has already EXITED — an
> `IndeterminateException` raised while the killer is still running has not been earned.

final class DaemonKillFateLiveTest extends LiveTestCase
{
    private const KILLER_BUDGET_SEC = '20';

    /** @var list<resource> killer handles to reap if a test dies between spawn and await */
    private array $killers = [];

    protected function tearDown(): void
    {
        foreach ($this->killers as $proc) {
            if (is_resource($proc)) {
                @proc_terminate($proc, 9);
                @proc_close($proc);
            }
        }
        $this->killers = [];
        parent::tearDown();
    }

    // ---- cell 1: autocommit write → Indeterminate, at most once --------------------------------

    public function testAutocommitWriteKilledMidFlightIsIndeterminateOnPg(): void
    {
        $this->runAutocommitWriteCell('pg', 'default');
    }

    public function testAutocommitWriteKilledMidFlightIsIndeterminateOnMysql(): void
    {
        $this->runAutocommitWriteCell('mysql', $this->requireMysqlPool());
    }

    private function runAutocommitWriteCell(string $family, string $pool): void
    {
        // PLAN-VERIFY MAJOR: NOT `connectConnection()` — see the note under this class.
        $conn = Ferro::connect($this->socketPath, $pool, 2.0, 60.0);
        $this->setupTable($conn, $family);
        $k = self::uniq('cell1');
        $marker = self::uniq('m1');

        $this->spawnKiller($family, $marker, $pool);
        $caught = null;
        try {
            // readonly=false (exec's default) — OpKind::Write on the loss path.
            $conn->exec($this->sleepingInsertSql($family, $marker), [$k]);
            self::fail('the write completed — the killer never fired (see its log in sys_get_temp_dir())');
        } catch (IndeterminateException $e) {
            // Any OTHER class propagates and errors the test — that IS the guard: §19.3's
            // autocommit-write row is IndeterminateException, nothing softer.
            $caught = $e;
        }
        $this->assertKillerObservedAndKilled();
        self::assertInstanceOf(IndeterminateException::class, $caught);

        // The daemon returns; the classifier must not have "resolved" the fate by re-issuing
        // (charter rule 3, client half): at most one application, ever.
        $this->restartFerrod();
        $fresh = $this->connectConnection(pool: $pool);
        self::assertLessThanOrEqual(
            1,
            $this->countKey($fresh, $family, $k),
            'at-most-once violated: an Indeterminate write was re-applied',
        );
        $fresh->session()->close();
    }

    // ---- cell 2: in-tx plain DML → never Indeterminate, prefix unpersisted ---------------------

    public function testInTxStatementKilledMidFlightIsNeverIndeterminateOnPg(): void
    {
        $this->runInTxPlainDmlCell('pg', 'default');
    }

    public function testInTxStatementKilledMidFlightIsNeverIndeterminateOnMysql(): void
    {
        $this->runInTxPlainDmlCell('mysql', $this->requireMysqlPool());
    }

    private function runInTxPlainDmlCell(string $family, string $pool): void
    {
        $conn = $this->connectConnection(pool: $pool);
        $this->setupTable($conn, $family);
        $k1 = self::uniq('cell2_prefix');
        $k2 = self::uniq('cell2_inflight');
        $marker = self::uniq('m2');

        $conn->begin();
        $conn->exec($this->plainInsertSql($family), [$k1]); // the prefix — must die with the tx

        $this->spawnKiller($family, $marker, $pool);
        $caught = null;
        try {
            $conn->exec($this->sleepingInsertSql($family, $marker), [$k2]);
            self::fail('the in-tx statement completed — the killer never fired');
        } catch (FerroException $e) {
            $caught = $e;
        }
        $this->assertKillerObservedAndKilled();

        // THE §19.3 SAFETY FLOOR: the transaction died with the daemon, nothing persisted (proven
        // below) — an Indeterminate here would be a false alarm on the one branch that must never
        // cry wolf. What the class concretely IS today (a RAW ConnectionLost/Transport —
        // TxHandle::run classifies nothing, OpKind::TxStatement has zero call sites) is a
        // RECORDED fact for §22.2 (aq), not an assertion: asserting the exact class would turn a
        // future honest client improvement into a chaos-harness failure.
        self::assertNotInstanceOf(IndeterminateException::class, $caught);

        $this->restartFerrod();
        $fresh = $this->connectConnection(pool: $pool);
        self::assertSame(
            0,
            $this->countKey($fresh, $family, $k1),
            'the plain-DML prefix persisted across a daemon kill — replay would double-apply (§19.3)',
        );
        self::assertSame(0, $this->countKey($fresh, $family, $k2));
        $fresh->session()->close();
    }

    // ---- cell 2b: the PINNED RESIDUAL — MySQL implicit commit across daemon death --------------

    /**
     * NOT an assertion of desired behavior — a PIN of a measured residual (the same instrument as
     * the §22.2 (ac) cry-wolf guard). MySQL's implicit commit makes the prefix INSERT durable the
     * moment CREATE TABLE runs, engine latch or no engine latch; a SIGKILL then destroys the
     * engine's `tx_writes_persisted` latch, NO wire field carries it, and the client — which
     * cannot know — reports a connection-shaped, non-Indeterminate loss whose replay would
     * re-apply k1. If this test ever FAILS on the last assertion, the client grew a wire signal:
     * update §19.3's residual note, §22.2 (aq), and this pin together.
     * Follow-up: docs/followups/2026-08-13-client-side-implicit-commit-daemon-death.md (Task 5).
     */
    public function testImplicitCommitPrefixSurvivesDaemonKillOnMysqlPinnedResidual(): void
    {
        $pool = $this->requireMysqlPool();
        $conn = $this->connectConnection(pool: $pool);
        $this->setupTable($conn, 'mysql');
        $k1 = self::uniq('cell2b_prefix');
        $k2 = self::uniq('cell2b_inflight');
        $marker = self::uniq('m2b');
        $ddlTable = 'chaos_ic_' . strtolower(self::uniq('t'));

        try {
            $conn->begin();
            $conn->exec($this->plainInsertSql('mysql'), [$k1]);
            $conn->exec("CREATE TABLE {$ddlTable} (id INT)"); // implicit commit: k1 is now durable

            $this->spawnKiller('mysql', $marker, $pool);
            $caught = null;
            try {
                $conn->exec($this->sleepingInsertSql('mysql', $marker), [$k2]);
                self::fail('the post-DDL statement completed — the killer never fired');
            } catch (FerroException $e) {
                $caught = $e;
            }
            $this->assertKillerObservedAndKilled();

            $this->restartFerrod();
            $fresh = $this->connectConnection(pool: $pool);

            self::assertSame(
                1,
                $this->countKey($fresh, 'mysql', $k1),
                'the measured premise moved: the implicitly-committed prefix did NOT survive — '
                    . 're-measure before touching the residual documentation',
            );
            self::assertSame(0, $this->countKey($fresh, 'mysql', $k2));
            self::assertNotInstanceOf(
                IndeterminateException::class,
                $caught,
                'the client reported Indeterminate for the in-tx loss — it appears to have grown '
                    . 'a persisted-writes signal; update §19.3 / §22.2 (aq) and this pin TOGETHER',
            );
            $fresh->session()->close();
        } finally {
            try {
                $cleanup = $this->connectConnection(pool: $pool);
                $cleanup->exec("DROP TABLE IF EXISTS {$ddlTable}");
                $cleanup->session()->close();
            } catch (\Throwable) {
                // an assertion-failure path may leave the daemon mid-restart; the table is
                // uniquely named and harmless if orphaned.
            }
        }
    }

    // ---- cell 3: readonly read → never Indeterminate; §19.2 recovery with changed epoch --------

    public function testReadonlyReadKilledMidFlightNeverIndeterminateAndRecoversOnPg(): void
    {
        $this->runReadonlyCell('pg', 'default');
    }

    public function testReadonlyReadKilledMidFlightNeverIndeterminateAndRecoversOnMysql(): void
    {
        $this->runReadonlyCell('mysql', $this->requireMysqlPool());
    }

    private function runReadonlyCell(string $family, string $pool): void
    {
        $conn = $this->connectConnection(pool: $pool);
        $marker = self::uniq('m3');
        $sleepRead = $family === 'pg'
            ? "SELECT count(*) FROM pg_sleep(30) WHERE '{$marker}' <> ''"
            : "SELECT SLEEP(30) FROM DUAL WHERE '{$marker}' <> ''";
        $epochBefore = $conn->currentEpoch();

        $this->spawnKiller($family, $marker, $pool);
        $caught = null;
        try {
            $conn->scalar($sleepRead); // scalar() declares readonly=true on the wire
            self::fail('the read completed — the killer never fired');
        } catch (FerroException $e) {
            $caught = $e;
        }
        $this->assertKillerObservedAndKilled();
        // A DECLARED read must never be Indeterminate (§19.3; the §22.2 (ac) guarantee's client
        // vantage). The concrete class with the daemon still DOWN is the raw last dial error
        // (ReconnectLoop.php:108 rethrows it at exhaustion) — recorded for §22.2 (aq), not
        // asserted; the deterministic §19.2 proof is the restart phase below (correction C3).
        self::assertNotInstanceOf(IndeterminateException::class, $caught);

        $this->restartFerrod();
        self::assertSame(
            1,
            (int) $conn->scalar('SELECT 1'),
            'the post-restart read did not transparently reconnect and re-issue (§19.2)',
        );
        self::assertGreaterThanOrEqual(1, $conn->reconnectCount());
        self::assertTrue(
            $conn->lastReconnectEpochChanged(),
            'boot_epoch must CHANGE across a SIGKILL restart (§19.1) — engine state void',
        );
        self::assertNotSame($epochBefore, $conn->currentEpoch());
        $conn->session()->close();
    }

    // ---- cell 4: open stream → exactly one terminal, never a hang, never a silent clean end ----

    public function testOpenStreamKilledMidFlightThrowsExactlyOnceAndNeverHangsOnPg(): void
    {
        // PG ONLY: MySQL-family fetch:stream is Unsupported (§22.2 (n)) — there is no stream to
        // kill there (correction C2).
        $conn = $this->connectConnection();
        $total = 200000;
        $rows = 0;
        $threw = null;
        $start = microtime(true);
        try {
            foreach ($conn->stream("SELECT g, repeat('x', 64) FROM generate_series(1, {$total}) g") as $row) {
                ++$rows;
                if ($rows === 5) {
                    // In flight BY CONSTRUCTION: DATA frames are arriving. The generator hands
                    // control back between rows, so the kill needs no sidecar.
                    shell_exec('kill -9 ' . $this->ferrodPid() . ' 2>/dev/null');
                }
            }
        } catch (\Throwable $e) {
            $threw = $e;
        }
        $elapsed = microtime(true) - $start;

        self::assertNotNull($threw, sprintf(
            'the stream ended CLEANLY after %d of %d rows — a mid-stream daemon death must '
                . 'surface as exactly one thrown terminal, never a silent truncation '
                . '(charter rule 4, client half)',
            $rows,
            $total,
        ));
        self::assertLessThan($total, $rows, 'the kill landed after the stream completed — not a mid-stream test');
        self::assertNotInstanceOf(
            IndeterminateException::class,
            $threw,
            'a streamed DECLARED read must never be Indeterminate (stream() declares readonly=true)',
        );
        self::assertLessThan(20.0, $elapsed, 'the loss must surface within the io timeout — never a hang');

        $this->restartFerrod();
        self::assertSame(1, (int) $conn->scalar('SELECT 1'));
        self::assertTrue($conn->lastReconnectEpochChanged());
        $conn->session()->close();
    }

    // ---- plumbing -------------------------------------------------------------------------------

    private static function uniq(string $prefix): string
    {
        return $prefix . '_' . getmypid() . '_' . bin2hex(random_bytes(6));
    }

    private function setupTable(Connection $conn, string $family): void
    {
        // No PK on `k` — deliberately: a unique constraint would make the at-most-once read-back
        // (count <= 1) TRUE BY SCHEMA and the assertion vacuous. Fixed table name + unique keys is
        // the shared-database discipline the Rust chaos suites use.
        $conn->exec($family === 'pg'
            ? 'CREATE TABLE IF NOT EXISTS chaos_kill_fate (k text, pad text)'
            : 'CREATE TABLE IF NOT EXISTS chaos_kill_fate (k VARCHAR(191), pad TEXT)');
    }

    private function plainInsertSql(string $family): string
    {
        return $family === 'pg'
            ? 'INSERT INTO chaos_kill_fate (k) VALUES ($1)'
            : 'INSERT INTO chaos_kill_fate (k) VALUES (?)';
    }

    private function sleepingInsertSql(string $family, string $marker): string
    {
        // The marker is a STRING-LITERAL predicate riding the statement text (§20.3: never a
        // comment — MariaDB strips comments from processlist INFO); the sleep keeps the statement
        // provably in flight until the sidecar observes it.
        return $family === 'pg'
            ? "INSERT INTO chaos_kill_fate (k, pad) SELECT \$1, pg_sleep(30)::text WHERE '{$marker}' <> ''"
            : "INSERT INTO chaos_kill_fate (k, pad) SELECT ?, CONCAT('', SLEEP(30)) FROM DUAL WHERE '{$marker}' <> ''";
    }

    private function countKey(Connection $conn, string $family, string $k): int
    {
        return (int) $conn->scalar(
            $family === 'pg'
                ? 'SELECT count(*) FROM chaos_kill_fate WHERE k = $1'
                : 'SELECT count(*) FROM chaos_kill_fate WHERE k = ?',
            [$k],
        );
    }

    private function spawnKiller(string $family, string $marker, string $pool): void
    {
        $log = sys_get_temp_dir() . '/ferro-chaos-killer-' . getmypid() . '.log';
        $proc = proc_open(
            [
                PHP_BINARY,
                __DIR__ . '/../Support/chaos_killer.php',
                $this->socketPath,
                $pool,
                $family,
                $marker,
                (string) $this->ferrodPid(),
                self::KILLER_BUDGET_SEC,
            ],
            [1 => ['file', $log, 'a'], 2 => ['file', $log, 'a']],
            $pipes,
        );
        self::assertIsResource($proc, 'failed to spawn chaos_killer.php');
        $this->killers[] = $proc;
    }

    /** The last spawned killer must exit 0 = "observed IN FLIGHT, then killed" — never 2/3. */
    private function assertKillerObservedAndKilled(): void
    {
        $proc = array_pop($this->killers);
        self::assertIsResource($proc);
        $status = proc_close($proc); // blocks until the sidecar exits
        self::assertSame(
            0,
            $status,
            'chaos_killer did not observe the marker in flight before killing — the kill proves '
                . 'nothing (see ferro-chaos-killer-*.log in sys_get_temp_dir())',
        );
    }
}
```

- [ ] **Step 5: Run the harness live**

```bash
cd /home/abdullak/projects/ferro && cargo build -p ferrod && cd php/client && composer install --no-interaction --quiet \
  && FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
     FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
     ./vendor/bin/phpunit tests/Live/DaemonKillFateLiveTest.php --fail-on-skipped
```

Expected: 8 tests green. **If any cell fails, do NOT adjust the assertion to match — diagnose:
either the harness has a defect (fix it) or the client does (STOP, journal, raise as a finding).**
Journal every cell's observed exception class verbatim (Task 6's §22.2 (aq) cites them).

- [ ] **Step 6: MUTATIONS M2–M6 — one RED run per guard, journalled**

Each: apply, run ONLY the named test(s), record RED output, restore, re-run green.

- **M2 (cell 1):** `php/client/src/Client/FateClassifier.php:131` — change
  `if ($opKind === OpKind::Write) {` to `if (false && $opKind === OpKind::Write) {`.
  Expected RED: `testAutocommitWriteKilledMidFlight…` errors with `RetryableException` where
  `IndeterminateException` was expected (traced: fall-through classifies Retryable;
  `mayRetry(write, no idempotent)` refuses the retry; the raw Retryable surfaces).
- **M3 (cell 2a):** `php/client/src/Client/TxHandle.php` `run()` — wrap the `sendRequest` call
  (`:220-224`) with an added
  `catch (\Ferro\Client\Error\TransportException | \Ferro\Client\Error\ConnectionLostException $e) { return ['cols' => [], 'rows' => [], 'affected' => 0, 'last_insert_id' => null]; }`
  — the "driver pretends success" silent-loss shape. Expected RED: both
  `testInTxStatementKilledMidFlight…` tests fail at `self::fail('the in-tx statement completed…')`.
- **M4 (cell 3, recovery half):** `FateClassifier.php:58` — `return $this->retryReads;` →
  `return false;`. Expected RED: both readonly-cell tests error on the post-restart
  `scalar('SELECT 1')` with a surfaced `RetryableException` instead of a transparent retry.
- **M5 (cell 3, kill half):** `FateClassifier.php:140-144` — make the fall-through arm return
  `new IndeterminateException(self::payload(C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, '…'), IndeterminateException::CAUSE_LINK_LOST)`
  instead of `RetryableException`. Expected RED: both readonly-cell tests fail
  `assertNotInstanceOf(IndeterminateException…)` at the kill phase (traced: classifyLoss's result
  is thrown directly when `mayRetryException` refuses). NOTE: this mutation does NOT reach cell 2a
  (TxHandle never calls `classifyLoss`) — cell 2a's falsifiability is M3 plus the 2a/2b read-back
  CONTRAST (same helper asserts 0 on PG plain-DML and 1 on the MySQL implicit-commit residual).
- **M6 (cell 4):** `php/client/src/Client/Connection.php` stream pump — replace BOTH inner
  `throw $e;` statements (`:498` after the `readStreamFrame` catch AND `:517` after the
  `sendWindowUpdate` catch) with `return;` — "treat any mid-stream wire failure as a clean end".
  Expected RED: `testOpenStreamKilled…` fails `assertNotNull($threw…)` with the
  'ended CLEANLY after N of 200000 rows' message. (BOTH sites, deliberately: mutating only `:498`
  can survive green because the WINDOW_UPDATE write to the dead socket throws first — that
  near-no-op is exactly the trap three of an earlier slice's four mutations fell into.)

- [ ] **Step 7: Offline skip-cleanliness + full client gates**

```bash
cd /home/abdullak/projects/ferro/php/client && ./vendor/bin/phpunit \
  && ./vendor/bin/phpstan analyse --level 9 src
```

Expected: offline run green with the new class SKIPPED cleanly (no env), PHPStan clean
(`tests/` is not analysed at L9 but keep the file warning-free anyway).

- [ ] **Step 8: Record the spec delta + journal + commit**

Append to `docs/followups/2026-08-13-s9-spec-deltas.md`:

```markdown
### Task 3
- §20.3 chaos bullet: "Not yet built" is now false — replace per the plan's Task 6 wording (the
  four cells + the pinned 2b measurement, PG scope for the stream cell, mutation-RED discipline).
- §19.3: add the client-side limit of amendment (1) — across a dead daemon the client cannot
  consult tx_writes_persisted; measured class facts: in-tx loss surfaces RAW
  ConnectionLost/Transport (TxHandle::run classifies nothing; OpKind::TxStatement has zero call
  sites); reconnect exhaustion rethrows the raw last dial error (ReconnectLoop.php:108).
- §22.2 (aq): the harness record + the /proto deferral candidate (per-statement persisted flag on
  the in-tx EXEC terminal).
- Observed exception classes per cell (fill from the journal): cell1=…, cell2=…, cell2b=…,
  cell3-kill=…, cell4=….
```

```bash
cd /home/abdullak/projects/ferro && git add php/client/tests docs/followups/2026-08-13-s9-spec-deltas.md \
  && git commit -m "feat(m1-s9): the kill-ferrod chaos harness — §19.3's client half finally has its event"
```

---

## Task 4: The recorded ORM acceptance runs — baselines + results document

The first-ever recorded ORM numbers, under the bar this plan set: stock comparator + two-run
exact-match + five-category triage with (a) EMPTY. **Requires Tasks 1 and 2 merged.** Nothing else
may run this repo's live tiers while these record (Global constraint 6).

**Files:**
- Create: `docs/orm-suite/baseline/{pg,mysql,mariadb}.txt`,
  `docs/orm-suite/baseline/stock-{pg,mysql,mariadb}.txt`, `docs/orm-suite/2026-08-13-results.md`
- Test: the runner's exit-0 baseline gate, twice per backend per mode

**Interfaces:**
- Consumes: `testkit/orm-suite.sh` (Task 2's env contract); the Task 1 fix (GH9230 bool=false must
  now pass).
- Produces: the committed baselines Task 7 re-verifies; the results doc Task 5 and Task 6 cite.
  Expected numbers (probe, pre-Task-1): stock PG `3485 / 0E 0F 52S`, stock MySQL `3485 / 0E 4F 57S`;
  ferro PG `47E + 1F` (expect **-1** from the Task 1 fix → ~47 total), ferro MySQL `6E + 4F`;
  MariaDB legs are FIRST MEASUREMENTS. Any drift beyond the fetchFirstColumn delta is investigated
  before recording, never absorbed.

- [ ] **Step 1: Record the stock comparators (two runs each — reproducibility applies to the
  comparator too)**

```bash
cd /home/abdullak/projects/ferro
for svc in pg mysql mariadb; do
  FERRO_ORM_SVC=$svc FERRO_ORM_MODE=stock FERRO_ORM_BASELINE=update ./testkit/orm-suite.sh | tee /tmp/orm-stock-$svc-1.log
  FERRO_ORM_SVC=$svc FERRO_ORM_MODE=stock ./testkit/orm-suite.sh | tee /tmp/orm-stock-$svc-2.log
done
```

Expected: run 1 `baseline: UPDATED stock-<svc>.txt`, run 2 `baseline: MATCHES` + exit 0, identical
result lines within each pair. Journal all six result lines. Expected content: PG empty baseline
(0 non-passing), MySQL/MariaDB the 4 platform-SQL-drift failures (driver-independent).

- [ ] **Step 2: Record the Ferro legs (two runs each)**

```bash
cd /home/abdullak/projects/ferro
for svc in pg mysql mariadb; do
  FERRO_ORM_SVC=$svc FERRO_ORM_MODE=ferro FERRO_ORM_BASELINE=update ./testkit/orm-suite.sh | tee /tmp/orm-ferro-$svc-1.log
  FERRO_ORM_SVC=$svc FERRO_ORM_MODE=ferro ./testkit/orm-suite.sh | tee /tmp/orm-ferro-$svc-2.log
done
```

Expected: exit 0 on every second run with `baseline: MATCHES`. Sanity against the probe: PG
non-passing ≈ 47 (48 minus GH9230 bool=false, which Task 1 fixed — VERIFY it is absent from the
baseline; if it is still present, Task 1's fix does not cover the suite's path: STOP and diagnose).
MySQL = 10 (6 Ferro-attributable + 4 stock-identical). MariaDB: first measurement — journal
whatever it is and triage it fully; the expectation (mirrors MySQL) is an inference, not a fact.

- [ ] **Step 3: Triage every non-passing test**

From the junit files (`.orm-suite/junit-ferro-*.xml`), cluster by failure-message signature
(the probe's clusters, for PG: 16× sub-second TIMESTAMPTZ read refusal; 10× hard-coded IDENTITY
fixtures; 10× PG bind matrix `F64→numeric`/`I64→float8`/`TEXT→x`; 6× multi-table DQL temp tables;
5× DDC832 sequence-cleanup collateral). Categories (definitions imported to §14 by Task 6):
(a) driver defect — **must be EMPTY**; (b) documented Ferro semantic; (c) upstream assumption
Ferro structurally cannot satisfy; (d) explicitly out of scope for M1; (e) an engine gap this run
measured and did not close. Assignments this plan has already decided (justify each in the doc):

| cluster | category | why |
|---|---|---|
| sub-second TIMESTAMPTZ read refusal (16) | (b) | §22.2 (ab) rule 2 — documented refuse-what-PDO-corrupts semantic; a RELAXATION is a filed policy decision (Task 5 follow-up), not a bug |
| hard-coded `GeneratedValue(IDENTITY)` fixtures (10) | (b) | D-S8b-5, binding; only fixture patching could close them, which changes the claim |
| multi-table DQL temp tables (6, also MySQL's 6) | (b) | §7.4 transaction-mode semantic, pgbouncer-identical; documented by Task 5 |
| DDC832 sequence-cleanup collateral (5) | (c) | upstream tearDown assumes the stock IDENTITY default (its sequence cleanup is DBAL-3-gated) |
| PG bind matrix (10) | **(e)** | real engine gap, measured, deliberately not closed at the gate — follow-up reopened with milestone M2-entry (Task 5) |
| stock-identical MySQL/MariaDB failures (4) | (c) | ORM-3.6.8-vs-DBAL-4.4.4 drift, driver-independent — byte-identical through stock (the baseline proves it) |

- [ ] **Step 4: Write `docs/orm-suite/2026-08-13-results.md`**

Follow `docs/dbal-suite/2026-08-11-s8c-results.md`'s structure. MUST contain, each as its own
section: (1) the headline table (both modes × three backends, executed/passed/E/F/S/I); (2) the
environment manifest (repo SHA, orm tag + clone SHA, resolved dbal version, PHP version, container
image versions via `SELECT version()`, wall times); (3) **the harness interventions, NAMED** — the
replacement TestUtil, the one-line DbalExtensions parent patch, the D-S8b-5 SEQUENCE preference
(with the measured 1229-error cost of omitting it), the container-side reset — "this is upstream's
suite under a replaced test HARNESS and the documented configuration", stated plainly; (4) the
two-run reproducibility evidence (result line + ordered-set match, per backend per mode); (5) the
per-backend triage tables (category totals per Step 3 with per-test rows); (6) the category-(e)
follow-ups with milestone assignments; (7) what this run does NOT cover (locking_functional +
performance groups excluded to match upstream CI; second-level-cache job not run; day-over-day
reproducibility untested).

- [ ] **Step 5: Commit**

```bash
cd /home/abdullak/projects/ferro && git add docs/orm-suite \
  && git commit -m "feat(m1-s9): the Doctrine ORM functional suite, run and recorded for the first time — stock comparator, two-run exact-match, triaged with (a) empty"
```

Append the headline numbers + triage totals to
`docs/followups/2026-08-13-s9-spec-deltas.md` under `### Task 4` (Task 6's §22.2 (ap) cites them).

---

## Task 5: Doc truth — known-incompatibilities, follow-up ledger hygiene, the missed wrapperClass

Everything measured must land where an operator will find it, and the follow-up directory must
stop claiming closed defects are open (a reader today believes 3–4 closed defects are open —
research-residuals). **Requires Tasks 3 and 4.**

**Files:**
- Modify: `docs/known-incompatibilities.md`;
  `docs/followups/2026-08-11-i64-above-2e32-unreadable-in-php-client.md`;
  `docs/followups/2026-08-11-pg-bind-matrix-narrower-than-libpq.md`;
  `docs/followups/2026-08-11-pg-int2vector-blocks-the-schema-manager.md`;
  `docs/followups/2026-08-10-s8b-nil-server-version-decision.md`;
  `testkit/migrations/cli-config.php`; `UPSTREAM_PR.md`
- Create: `docs/followups/2026-08-13-orm-timestamptz-subsecond-read-refusal.md`;
  `docs/followups/2026-08-13-client-side-implicit-commit-daemon-death.md`

**Interfaces:**
- Consumes: Task 4's results doc + triage; Task 3's journalled cell-2b measurement.
- Produces: the follow-up filings §22.2 (ap)/(aq) will cite BY PATH (Task 6), and the incompat
  entries §14's compat bullet points at.

- [ ] **Step 1: Closure annotations (one header line each, above the first heading, pointing at
  the closing slice + evidence):**

  - `2026-08-11-i64-above-2e32-…` → `> **RESOLVED (M1-S8c).** The defect was never a 2^32 boundary but the whole 0xcf unsigned family; fixed in PurePacker (turnover now PHP_INT_MAX), conformance test shipped. Kept for the record.`
  - `2026-08-11-pg-int2vector-…` → `> **RESOLVED (M1-S8c)** by the §9.1 PostgreSQL TEXT FALLBACK (§22.2 (ae)); doctrine/migrations runs end-to-end (testkit/migrations-e2e.sh).`
  - `2026-08-10-s8b-nil-server-version-decision.md` → replace the `DECISION REQUIRED` headline with `> **DECIDED (D-S8b-1, M1-S8b)** — resolution order handshake → one SELECT version() → loud ServerVersionUnavailable; recorded in SPEC §14.`
  - `2026-08-11-pg-bind-matrix-…` → `> **PARTIALLY RESOLVED (M1-S8c) / REOPENED (M1-S9).** The S8c widening closed I64→text/bool. The first ORM run measured the REMAINING directions: F64→numeric (7 tests), I64→float8 (2), TEXT→<n> (1) — ordinary stock-Doctrine shapes, reproduced minimally ([3.14] into NUMERIC(10,2); [2] into DOUBLE PRECISION). Category (e) in docs/orm-suite/2026-08-13-results.md; milestone assignment: M2-entry. Closing it is S8c-shaped engine work and was deliberately NOT done at the exit gate.`

- [ ] **Step 2: The two new follow-ups** — each with: measured facts (from research-orm step 5b /
  Task 3's journal), the decision required, the milestone assignment (M2-entry), and what NOT to
  do (no blanket relaxation of (ab) rule 2; no client-side SQL inference):

  - `2026-08-13-orm-timestamptz-subsecond-read-refusal.md`: 16 ORM tests (DQL `DATE_ADD`/`DATE_SUB`
    return microsecond timestamptz; stock hands the app a RAW STRING because DQL functions have no
    type mapping — DBAL's own `DateTimeTzType` would refuse it too; Ferro's engine types the
    column so §22.2 (ab) rule 2 refuses at decodeRow). Stricter than stock ON READ; the candidate
    relaxation (pass through sub-second canonical text as string exactly when no type conversion
    is requested — what PDO de facto does) is a POLICY decision for M2-entry, not a bug fix.
  - `2026-08-13-client-side-implicit-commit-daemon-death.md`: Task 3 cell 2b's measurement
    (prefix durable, client reports non-Indeterminate connection loss, replay would double-apply);
    why it cannot be fixed client-side (charter rule 6); the `/proto` deferral candidate — a
    per-statement `tx_writes_persisted` flag on the in-transaction EXEC terminal, letting the
    client latch WITHOUT SQL inference; milestone: with the next `/proto`-touching slice
    (alongside `affected`-on-stream-terminal and `TxNotFound`).

- [ ] **Step 3: `docs/known-incompatibilities.md` additions** (match the file's existing voice —
  operator-facing, measured, workaround-first):

  - New `## Doctrine ORM` section: (i) multi-table DQL bulk `UPDATE`/`DELETE` on JOINED
    inheritance fails on a transaction-mode pool — the executor's temp tables are wiped between
    autocommit checkouts by hygiene, identical to pgbouncer transaction mode (6 tests per backend,
    the entire Ferro-attributable MySQL delta); workaround: run such bulk operations inside an
    explicit transaction (one pinned connection) or restructure; (ii) ORM on PostgreSQL requires
    the SEQUENCE identity preference — now with the measured number (1229 of 3485 tests error
    without it) and the one-line configuration; (iii) DQL `DATE_ADD`/`DATE_SUB` on PG return
    sub-second `TIMESTAMPTZ` that Ferro refuses on read (pointer to the follow-up).
  - Extend the existing `## MySQL/MariaDB: an implicit commit changes what a lost statement
    reports` section with the DAEMON-death case (cell 2b): after an implicit commit, a daemon
    crash mid-transaction surfaces a plain connection error and the already-committed prefix
    SURVIVES — a retry-the-transaction wrapper re-applies it; pointer to the follow-up.

- [ ] **Step 4: `testkit/migrations/cli-config.php`** — add
  `'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class,` to the connection params (the
  §22.2 (ah) REQUIRED wrapper; this file is the template operators copy, and
  `all_or_nothing`+transactional migrations is exactly the shape that needs it). Then re-run
  `testkit/migrations-e2e.sh` if quick (< 5 min) to prove the template still works; otherwise
  journal that it was config-only and Task 7's gates cover it.

- [ ] **Step 5: `UPSTREAM_PR.md`** — verify the drop-condition checklist names ALL FOUR
  `tokio-postgres` accessors (`transaction_status`, `parameter`,
  `clear_typeinfo_statement_cache`, per-column `Bind` result formats (§22.2 (af))). Add the fourth
  if missing (research-residuals flagged it as unverified).

- [ ] **Step 6: Journal, spec-delta entry (`### Task 5`: list every filed path so (ap)/(aq) can
  cite them), commit**

```bash
cd /home/abdullak/projects/ferro && git add docs testkit/migrations/cli-config.php UPSTREAM_PR.md \
  && git commit -m "docs(m1-s9): the measured truth lands where operators read — ORM incompatibilities, follow-up ledger hygiene, the missed wrapperClass"
```

---

## Task 6: The spec amendment — ONE author, every site, in one commit

Applies the renegotiated bar to `ferro-spec-v0.2.md` and updates `CLAUDE.md`. **No other task
edits the spec.** Read `docs/followups/2026-08-13-s9-spec-deltas.md` (every task's entries),
`docs/orm-suite/2026-08-13-results.md`, and the research-bar journal §2 (the drafts below are its
wording with corrections C1–C3 applied — where they differ, THIS TASK's text wins). Anchor edits
by SEARCH STRING, not line number. Where a number below is the probe's, replace it with Task 4's
RECORDED value if it differs.

**Files:**
- Modify: `ferro-spec-v0.2.md` (§2 G2, §14, §15, §16.1, §17, §19.3, §20.3 ×2, §21, §22.2),
  `CLAUDE.md`

**Interfaces:**
- Consumes: everything above. Produces: the spec text Task 7 verifies told-the-truth against.

- [ ] **Step 1: §17 — replace the M1 bullet** (anchor: `- **M1** — pin engine as specified`):

```markdown
- **M1** — pin engine as specified (§7.1–7.2: protocol signals, trackers, assist lexer, conditional hygiene) + MySQL/MariaDB backend + tracker verification test; canonical type coverage (§9); full error taxonomy incl. `Indeterminate`; Doctrine DBAL 4 driver (§14); core hardening (§19.3 amendments, §5.2, §18). **Exit gate (M1-S9) — four parts, none of them the word "green", because a green result line was measured satisfiable with zero Ferro contact (§22.2 (z)):** **(1) DBAL:** §14's acceptance bar met on PostgreSQL, MySQL and MariaDB — two runs per backend reproducing the identical result line AND the identical ordered non-passing set, byte-matching the committed `docs/dbal-suite/baseline/`, every non-passing test triaged into §14's five categories, with **categories (a) and (e) EMPTY**. **(2) ORM:** the Doctrine ORM functional suite RUN and RECORDED on PostgreSQL + MySQL (MariaDB additionally recorded) under §14's runner discipline — a **measurement bar, not a green bar**: a recorded stock-PDO comparator, two-run baseline exact-match, the five-category triage; **category (a) must be EMPTY** (a driver defect found by the run is fixed before recording, not filed); category-(e) rows do not block exit, but each carries a filed follow-up with an explicit milestone assignment, recorded in §22.2 (ap). **(3) Chaos:** the §20.3 kill-`ferrod` harness built and green for its named minimum cell set, each cell mutation-proven RED (§22.2 (aq)). **(4) D12:** formally adjudicated or formally re-anchored by recorded amendment (§16.1's status note) — never silently treated as passed; no accelerator work landed in M1 because the conditional never evaluated (the M0 measurement is `provisional: true, reference: false`, §22.1). **SQLite is REMOVED from every M1 acceptance sentence by amendment** (§14; §22.2 (ao)): no `ferro-backend-sqlite` exists and `AnyPool` is `{ Pg | Mysql }`; it re-enters with M2's SQLite engine-owned mode, in the same exact-match + triage form.
```

- [ ] **Step 2: §14 — replace from `The bar is therefore:` to the end of that acceptance bullet**
  with three paragraphs: (i) the DBAL half in the falsifiable form, now naming the five categories
  NORMATIVELY — (a) a driver defect to fix now; (b) a documented Ferro semantic; (c) an upstream
  assumption Ferro structurally cannot satisfy; (d) explicitly out of scope for M1; (e) an engine
  gap this run measured and did not close — with (a) and (e) EMPTY, the S8c recorded numbers, and
  the `read both, the comparison is the artifact` pointer kept (use research-bar §2.2's draft
  verbatim for this paragraph); (ii) the ORM half — measurement bar as drafted in research-bar
  §2.2 PLUS: category (a) EMPTY required; MariaDB additionally recorded; the runner named
  (`testkit/orm-suite.sh`); the recorded result
  (`docs/orm-suite/2026-08-13-results.md`: PG ⟨recorded⟩ non-passing of 3485 under the D-S8b-5
  SEQUENCE configuration, MySQL ⟨recorded⟩ of which 4 are stock-identical, MariaDB ⟨recorded⟩;
  category (e) = the PG bind-matrix rows, filed with an M2-entry assignment); and the honest
  asymmetry stated: *the DBAL half is a compatibility claim; the ORM half at M1 is an
  honest-measurement claim*; (iii) the SQLite removal paragraph (research-bar §2.2's third
  paragraph verbatim — quotes the v0.1 words, names the re-entry point).

- [ ] **Step 3: §20.3 — two bullets.** Replace the chaos bullet's final sentence (anchor:
  `Killing `ferrod` itself is the untested half`) with:

```markdown
Killing `ferrod` itself was the untested half and is **part of the M1 exit bar (§17 part 3) — BUILT at M1-S9** as `php/client`'s `DaemonKillFateLiveTest` (the vantage is the PHP client through the §19.2 reconnect loop, because that is what a production caller observes when the daemon dies; each test launches its own `ferrod` and SIGKILLs it by pid). The minimum cell set, stated so "built" is falsifiable — each kill is proven in flight (the sidecar observes the marker per the discipline above; the stream cell is in flight by construction, DATA frames already received), and `boot_epoch` is asserted CHANGED across the restart: **(1)** an autocommit non-readonly EXEC → `IndeterminateException`, never re-issued, applied at most once (read-back proven); **(2)** an in-transaction plain-DML EXEC → never `Indeterminate`, the transaction's earlier writes proven UNPERSISTED by read-back (PostgreSQL and MySQL); **(3)** a `readonly`-declared read → never `Indeterminate`, and after the daemon returns the next read reconnects and succeeds transparently; **(4)** an open stream → exactly one thrown terminal, never a hang, never a clean end that silently truncates (PostgreSQL only — MySQL-family streaming does not exist, §22.2 (n)). Each cell is mutation-proven RED; cells 1–3 run on PostgreSQL and MySQL. **What the harness deliberately MEASURES rather than asserts:** the §22.2 (ai) implicit-commit exception cannot be asserted across a dead daemon — the engine's `tx_writes_persisted` latch dies with it and no wire field carries it — so the MySQL cell that runs DDL inside the transaction PINS the honest residual instead: the implicitly-committed prefix survives (read-back proven) while the client, which cannot know, reports a connection-shaped non-`Indeterminate` error (§19.3's client-side limit; §22.2 (aq)). Cells beyond these are recorded residuals, not implied coverage.
```

Replace the upstream-suites bullet (anchor: `green is the M1/M2 acceptance bar, run in CI
nightly`) with research-bar §2.4's draft, amended to name BOTH runners
(`testkit/dbal-suite.sh` AND `testkit/orm-suite.sh`, each with its committed baseline directory)
and to name the ORM harness interventions per Task 2's delta entry.

> **PLAN-VERIFY MAJOR — this amendment as originally worded manufactures the THIRD §22.2-class
> contradiction of the milestone, by the same mechanism as the two already repaired.**
> research-bar §2.4's draft demands "a five-category triage with categories (a) and (e) empty".
> Applied verbatim to BOTH runners, §20.3 would require (e) EMPTY of the ORM suite while §14 and
> §17 — amended in this same commit — say ORM (e) rows do not block, and the recorded ORM triage
> has a non-empty (e) BY DESIGN (the 10 bind-matrix rows). (u)/(v) and (aj) were both exactly this:
> one draft blanket-applied across sites that needed different scopes.
> **Required:** the (a)-and-(e)-EMPTY clause is scoped to the **DBAL runner only**; the ORM half
> cross-references §14's measurement bar (category (a) EMPTY, (e) filed-not-blocking with a
> milestone assignment). Before committing Step 3, read §14, §17 and §20.3's new text TOGETHER and
> confirm they agree on what each suite must show — the contradiction is only visible across sites.

> **PLAN-VERIFY MAJOR — the chaos-bullet replacement does not reach the stale sentence.**
> §20.3's chaos bullet contains "**Not yet built** (the M0 core review, 2026-08-11): what exists
> instead is per-slice live chaos against the BACKEND link …" TWO SENTENCES BEFORE the final
> sentence this step replaces. Replacing only the last sentence leaves the bullet asserting both
> "Not yet built" and "BUILT at M1-S9". Task 3's spec-delta entry defers to "the plan's Task 6
> wording", and Task 6 replaces only the final sentence — circular, so neither instruction edits it.
> **Required:** replace from `**Not yet built**` through the end of the bullet, so the whole claim
> is rewritten in one piece.

- [ ] **Step 4: §16.1 — append the D12 status note** (research-bar §2.5 verbatim).

- [ ] **Step 5: §19.3 — insert the client-side limit paragraph** after the
  `**The `readonly` invariant gains ONE stated exception.**` paragraph:

```markdown
**Client-side limit of amendment (1), measured at M1-S9 (§22.2 (aq)).** Across a DEAD daemon the client cannot consult `tx_writes_persisted` — the latch dies with the engine and no wire field carries it. An explicit-transaction caller on MySQL/MariaDB whose transaction ran an implicitly-committing statement and then lost the daemon therefore receives a connection-shaped error whose replay re-applies the already-committed prefix. Measured (the `DaemonKillFateLiveTest` pinned residual): the prefix INSERT survives a SIGKILL with no `COMMIT` ever sent, while the client reports the loss with no `Indeterminate` marking. A per-statement persisted flag on the in-transaction EXEC terminal is the recorded `/proto` deferral candidate that would close this without client-side SQL inference (which §3/charter rule 6 forbids at the tiers); until then it is a documented residual (`docs/known-incompatibilities.md`, the implicit-commit section; `docs/followups/2026-08-13-client-side-implicit-commit-daemon-death.md`).
```

- [ ] **Step 6: §22.2 — three new entries + two labelled corrections.**
  **(ao)**: research-bar §2.6's draft verbatim (the retraction quotes are already correct).
  **(ap)**: the ORM record — write it from Task 4's results doc: the four headline numbers, the
  named harness interventions, the (a)-EMPTY statement with the ONE category-(a) defect found and
  FIXED (Task 1's `fetchFirstColumn`, per Task 1's delta entry), the category assignments table
  (16 TIMESTAMPTZ = (b), 10 IDENTITY = (b), 6 temp-table = (b), 5 DDC832 = (c), 4 stock-identical
  = (c), bind matrix = **(e)** with the filed follow-up + M2-entry assignment), and the
  not-covered list (locking_functional/performance/second-level-cache).
  **(aq)**: the chaos-harness record — the five cells with their OBSERVED exception classes (from
  Task 3's journal), the two class facts (TxHandle raw loss; ReconnectLoop exhaustion rethrow),
  the pinned residual, and the `/proto` deferral candidate.
  **Corrections** (per §22.2's rules): in the renumbering entry after
  `(**still the M1 exit gate**, unchanged in substance)` insert research-bar §2.6's first
  labelled correction; in (z)'s closing paragraph after `and only then the allow-list argument.`
  insert the second.

- [ ] **Step 7: Consequential one-liners.** §2 G2 (anchor `Acceptance: upstream test suites
  pass`) → research-bar §2.7's sentence. §15 acceptance line → append
  `(Bar form to be restated per §14's exact-match + triage form at M2 planning; "green" is not a bar this project uses — §22.2 (ao).)`
  §21: add engineering open item 4 (the client-side implicit-commit daemon-death residual, one
  paragraph, pointing at the follow-up and §22.2 (aq)). Do NOT edit the D7/D12 rows — they are
  maintainer-owned; Task 7's ledger surfaces them.

- [ ] **Step 8: CLAUDE.md.** Append an **M1-S9 is complete** paragraph to "Current state" carrying:
  the renegotiated bar (four parts, and that "green" is retired with the reason); the first-ever
  ORM numbers with the stock comparator and the (e)-filed rows; the kill-ferrod harness (cells +
  the pinned client-side residual); the fetchFirstColumn fix; SQLite's formal removal; D12's
  re-anchor; and the gates at the boundary (fill from Task 7 — coordinate: leave the gate-numbers
  sentence as `⟨Task 7 fills the recorded gate numbers⟩` and Task 7 completes it in its commit).
  Replace the "Next up" paragraph: M1 has exited; M2 planning inherits the named carries (the
  streaming abort drain (ak), the ORM (e) rows, the client-side daemon-death residual + its
  `/proto` candidate, MySQL `query_stream`, `affected`-on-stream-terminal, `TxNotFound`, R2,
  LARGE_OBJECT, the Laravel tier, the SQLite backend re-entry, D7/D12 maintainer items, and the
  standing caveat: **S9a and S9 have had no whole-branch adversarial pass**).

- [ ] **Step 9: Verify + commit**

Re-read every edited section END TO END for internal contradiction (the S8a lesson — check
especially that §14, §17, §20.3 and §22.2 (ao) state the SAME bar, and that no sentence still says
"green" as a bar, `grep -n 'suite green' ferro-spec-v0.2.md` must return only quoted retractions).

```bash
cd /home/abdullak/projects/ferro && git add ferro-spec-v0.2.md CLAUDE.md \
  && git commit -m "docs(m1-s9): the bar renegotiated in place — exact-match + triage everywhere, SQLite removed by amendment, D12 re-anchored, the harness and the ORM run recorded"
```

---

## Task 7: The exit-gate verification run + the maintainer ledger

Everything, at the exit HEAD, with the outputs journalled — this task produces the numbers the
milestone is judged by, so nothing here is optional and nothing is cited from memory.

**Files:**
- Create: `docs/followups/2026-08-13-m1-exit-maintainer-items.md`
- Modify: `CLAUDE.md` (fill Task 6's gate-numbers slot)

**Interfaces:**
- Consumes: everything. Produces: the recorded gate outputs (journal + ledger).

- [ ] **Step 1: Rust + no-skip (NEVER via `ci/local-gate.sh --live`)**

```bash
cd /home/abdullak/projects/ferro && cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings \
  && FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
     FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
     FERRO_TEST_MARIADB_URL="mysql://ferro:ferro@127.0.0.1:33061/ferro" \
     cargo test --workspace -- --nocapture 2>&1 | tee /tmp/s9-live.log \
  && ./ci/assert-no-skips.sh /tmp/s9-live.log
```

Expected: **863 passed / 0 failed** (this slice adds no Rust tests; any drift is investigated).

- [ ] **Step 2: PHP gates**

```bash
cd /home/abdullak/projects/ferro/php/client && composer install --no-interaction --quiet && ./vendor/bin/phpunit \
  && FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
     FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
     ./vendor/bin/phpunit tests/Live --fail-on-skipped \
  && ./vendor/bin/phpstan analyse --level 9 src
cd /home/abdullak/projects/ferro/php/doctrine-dbal && composer install --no-interaction --quiet && ./vendor/bin/phpunit \
  && FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
     FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
     FERRO_TEST_MARIADB_URL="mysql://ferro:ferro@127.0.0.1:33061/ferro" \
     ./vendor/bin/phpunit tests/Live --fail-on-skipped \
  && ./vendor/bin/phpstan analyse src --level 9
```

Expected: zero-skip live lanes including the new `DaemonKillFateLiveTest` (client counts grow by
its 8 tests + Task 1's; journal exact totals).

- [ ] **Step 3: `/proto` regeneration zero-diff.**

  > **PLAN-VERIFY MAJOR — there is no proto-regen CI job to find.** `.github/workflows/ci.yml` has
  > five jobs (rust, integration, php, deny, fuzz-smoke) and `ci/local-gate.sh` has none either, so
  > "replicate the CI step" sends the implementer looking for something that does not exist. The
  > real mechanism is to run the two generators and assert the tree did not move:
  > ```bash
  > cargo run -p ferro-proto --bin gen-registry-lock
  > php proto/tools/gen-php.php
  > git status --porcelain   # must be EMPTY
  > ```
  > (A `touch proto/*.toml && cargo build -p ferro-proto` also re-runs `build.rs` for the Rust
  > constants.) Run the generators, then
  `git status --porcelain` must be EMPTY. This slice touched no `/proto` input, so any diff is a
  stop-and-diagnose.

- [ ] **Step 4: The two acceptance suites at the exit HEAD**

```bash
cd /home/abdullak/projects/ferro
for svc in pg mysql mariadb; do FERRO_DBAL_SVC=$svc ./testkit/dbal-suite.sh || exit 1; done
for svc in pg mysql mariadb; do FERRO_ORM_SVC=$svc FERRO_ORM_MODE=ferro ./testkit/orm-suite.sh || exit 1; done
```

Expected: six exit-0 runs, every one printing `baseline: MATCHES`. A DBAL drift here is hazard 11's
scenario (Task 1's fix) — diagnose against the printed non-passing diff, journal, and only then
decide whether it is a re-record (a test newly PASSING via the fix) or a defect.

- [ ] **Step 5: The maintainer ledger** — `docs/followups/2026-08-13-m1-exit-maintainer-items.md`:

```markdown
# M1 exit — items that need a HUMAN decision (none of these can be discharged by an agent)

Filed at the M1-S9 exit gate. Each item names its §21/§22 anchor and what "done" means.

1. **D7 — naming/trademark check (crates.io / Packagist / trademark), flagged "before M1".**
   M1 is exiting WITHOUT it. Decide: do it now, or re-date the row (maintainer edit to §21).
2. **D12 — re-anchored, not adjudicated (§16.1 status note; §17 part 4).** The reference-hardware
   re-run is the named M2-ENTRY decision point; the §21 open item "reference-hardware sign-off
   for §16" is now load-bearing. Decide: procure/schedule, or consciously carry.
3. **Vendored-fork CVE exposure.** FIVE fork edits now ride production (four tokio-postgres
   accessors + mysql_async CLIENT_SESSION_TRACK); UPSTREAM_PR.md / UPSTREAM_PR_MYSQL_ASYNC.md are
   drafted, NOT filed — filing needs human authorization. Every un-filed month grows the CVE-lag.
4. **The standing caveat moved again:** M1-S9a AND M1-S9 have had no whole-branch adversarial
   pass. Every slice that received one produced confirmed defects (S8b: 6 blockers with all
   gates green). Decide: schedule the pass at M2 entry, or accept the risk in writing.
5. **License selection + security review scheduling** (§21 maintainer open items) — unchanged,
   still open, restated so exit does not bury them.
```

- [ ] **Step 6: Fill CLAUDE.md's gate-numbers slot** (from Steps 1–4's journalled outputs),
  final journal entry (the full command outputs live in
  `.superpowers/sdd/2026-08-13-ferro-m1-s9-exit-gate/task-7-notes.md`), commit:

```bash
cd /home/abdullak/projects/ferro && git add docs/followups/2026-08-13-m1-exit-maintainer-items.md CLAUDE.md \
  && git commit -m "chore(m1-s9): the exit-gate verification record + the ledger of decisions only a human can make"
```

---

## Self-review notes (performed while writing; kept for the executor)

- **Coverage:** the three prompt jobs map to plan §"renegotiated bar"+Task 6 (job 1), Tasks 2+4
  (job 2), Task 6 (job 3); the research recommendation "M1 must not exit without the §20.3
  harness" is honored as Task 3; the ORM probe's category-(a) defect is Task 1; D7/D12/forks
  surface in Task 7's ledger. §15/§2 consequential edits are Task 6 Step 7.
- **Mutation no-op audit:** M2–M6 each traced through the code paths quoted in the hazards; M6
  deliberately mutates TWO sites because the single-site version is a probable no-op; M5 is
  explicitly documented as NOT covering cell 2a (its coverage is M3 + the 2a/2b contrast) — do not
  let a reviewer "simplify" that note away.
- **Type consistency:** `ferrodPid(): int` (Task 3 Step 1) matches its uses in Steps 2/4;
  `Result::buffered(cols, rows, affected)` matches `ResultTest.php:32`; the runner's env contract
  in Task 2's Produces block matches every invocation in Tasks 4 and 7.
- **Known unknowns an implementer must settle at task time, not assume:** upstream ORM TestUtil's
  private-method surface (Task 2 Step 2 — STOP if it differs); the ferrod default pool size ≥ 2
  (Task 3 Step 3); whether MySQL's processlist shows placeholder text for COM_STMT_EXECUTE param
  markers (Task 3 Step 3 verifies before any cell relies on it).
