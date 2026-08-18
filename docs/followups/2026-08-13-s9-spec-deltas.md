# M1-S9 spec deltas — append-only ledger

Tasks 1–5 and 7 do NOT edit `ferro-spec-v0.2.md`, `proto/PROTOCOL.md` or `CLAUDE.md`; each records
here what it forces, and **Task 6's single author applies them all** (parallel spec authorship
produced the S8a (u)/(v) contradiction, and this milestone has repaired three).

Append your own `### Task N` block. The file is SHARED — expect to rebase if a concurrent task
touched it.

### Task 3

**§20.3 chaos bullet.** "**Not yet built**" is now FALSE. Per the plan verification, replace the
bullet FROM "**Not yet built**" TO the end (keep the first sentence), folding the old
backend-link-chaos context in as history. What exists now:
`php/client/tests/Live/DaemonKillFateLiveTest.php` — **10 live tests, `OK (10 tests, 110
assertions)`** against PG 17 + MySQL 8.4, the FIRST tests in this repository in which the DAEMON
dies mid-request (every prior chaos suite kills the BACKEND link and the daemon survives to
classify). Four assertion cells + one pinned measurement + **two pinned defects**:
- cell 1 autocommit write (pg, mysql) → `IndeterminateException`, at most once after restart;
- cell 2 in-transaction plain DML (pg, mysql) → never `Indeterminate`, prefix proven unpersisted;
- cell 2b MySQL implicit-commit prefix → MEASUREMENT pin (premise HOLDS: `k1` survived, `k2` did not);
- cell 3 declared read (pg, mysql) → never `Indeterminate`, plus §19.1 epoch-changed across SIGKILL;
- cell 4 open stream (PG only, §22.2 (n)) → exactly one thrown terminal, mid-stream, no hang.
Each mutation-proven RED (M2, M3, M5, M6, plus new M7/M8); the kill is proven IN FLIGHT by a
sidecar polling `pg_stat_activity`/`information_schema.processlist`, **and proven to PRECEDE the
client's classification by a timestamp ordering assertion** — see the anti-false-green note below.

**§19.3 — the client-side limit of amendment (1), and the measured class facts.**
Across a DEAD daemon the client cannot consult `tx_writes_persisted`: the latch dies with the
engine and no wire field carries it. Measured classes, verbatim (cite these):
| cell | observed |
|---|---|
| 1 pg / 1 mysql | `IndeterminateException` :: `autocommit write lost mid-flight — fate unknown (§19.3 Indeterminate): unexpected EOF after 0 of 16 bytes (code=8193, branch=2)` |
| 2 pg / 2 mysql / 2b mysql | `TransportException` :: `unexpected EOF after 0 of 16 bytes` |
| 3 pg / 3 mysql | `TransportException` :: `connect failed to unix:///tmp/ferro-test-….sock: Connection refused (errno 111)` |
| 4 pg | `TransportException` :: `write failed after 0 of 23 bytes`, after 2048 of 200 000 rows |
- hazard 12 CONFIRMED: `TxHandle::run` classifies nothing (catches only `CodecException`), so an
  in-transaction loss surfaces the RAW `TransportException`; `OpKind::TxStatement` still has zero
  call sites.
- correction C3 CONFIRMED: at reconnect exhaustion the client surfaces the RAW last DIAL error, not
  a classified `RetryableException`.
- cell 4's terminal arrives from the `sendWindowUpdate` site, not `readStreamFrame` — which is why
  a mutation of only `Connection.php:498` survives GREEN (re-measured, not cited).

**§22.2 (aq) — the harness record + the `/proto` deferral candidate.** The wire signal this slice
discovered it WANTS but did not hand-roll (charter rule 2): a per-statement `tx_writes_persisted`
flag on the in-transaction EXEC terminal, so a client can distinguish "the transaction died" from
"an implicitly-committed prefix is already durable" across a daemon death. Recorded as a DEFERRED
candidate. Follow-up: `docs/followups/2026-08-13-client-side-implicit-commit-daemon-death.md`
(Task 5).

**NEW — §22.2 needs an entry for TWO measured client defects this harness FOUND** (both HIGH, both
reproduced independently of the harness, both leaving a `Connection` permanently unusable after an
ordinary `ferrod` restart, both surfacing OUTSIDE the fate taxonomy so `catch (FerroException)`
never sees them). Full write-up:
`docs/followups/2026-08-13-client-recovery-unreachable-after-daemon-death.md`.
- **A:** `ReconnectLoop::reconnect()` closes the session then, on exhaustion, rethrows without
  replacing it → every later call is `TypeError: fwrite(): supplied resource is not a valid stream
  resource`. Default budget exhausts in **under ~0.35 s**, i.e. faster than any real restart.
- **B:** a mid-stream wire failure never clears `Session::$streamOpen` → every later request is
  `ProtocolException: a stream (request_id=N) is open on this session; drive it to its terminal`,
  which is impossible (the socket is dead).
- Neither was caught before because `RestartLiveTest` restarts the daemon BEFORE issuing its read,
  so its reconnect succeeds on attempt 1 and exhaustion is never reached — defect species (c).
- **Consequence for §19.2's wording:** its transparent-recovery promise currently holds ONLY for
  the case where the daemon is back before the client notices. Say so, or fix the client.
- Both are PINNED (`…PinnedDefect` tests) rather than fixed — Task 3 changes no `php/client/src`.
  Cells 3 and 4 therefore assert the §19.3 kill-phase floors and §19.1's epoch change (from a fresh
  connection); the §19.2 recovery halves the plan put in those cells are pinned instead.

**NEW — §20.3 should record the anti-false-green discipline this task had to invent**, because it
is the difference between an acceptance test and a coincidence detector. A cell that kills the
daemon can pass with the daemon ALIVE: a client-side read timeout is `TransportException`, which
`classifyLoss(Write)` turns into the SAME `IndeterminateException` a daemon death mints. Two
defences, both measured necessary: (1) an io timeout that EXCEEDS the parked sleep, asserted in
`setUp` rather than commented; (2) the sidecar stamps `microtime(true)` immediately BEFORE
signalling and the cell asserts `killedAt <= caughtAt`. **The weaker form of (2) — "has the killer
exited by now?" — was measured INSUFFICIENT** (the killer catches up inside the grace), and with
the ordering assertion disabled under a slow-killer mutation, cell 1 reports
`OK (1 test, 14 assertions)` while certifying `Indeterminate` with ferrod alive for another 0.94 s.

### Task 2

**§20.3 upstream-suites bullet** must name `testkit/orm-suite.sh` beside `testkit/dbal-suite.sh`,
and must state the harness interventions BY NAME — the claim is "upstream suite, replaced test
HARNESS, documented configuration", never more:
1. the replacement `tests/Tests/TestUtil.php` (honours `db_driverClass`, which upstream's
   `mapConnectionParameters()` silently DISCARDS; no-op `initializeDatabase()` because PHP holds no
   credentials, SPEC §12/D8, and the container-side reset owns idempotence);
2. the ONE-line re-parenting of the suite's own QueryLog wrapper
   `Doctrine\Tests\DbalExtensions\Connection` onto `Ferro\DBAL\Wrapper\FerroConnection` — the
   `wrapperClass` slot is single-occupancy and §22.2 (ah) makes Ferro's wrapper REQUIRED, so
   inheritance is the only composition that keeps BOTH the query-count assertions and the
   `transactional()` `IndeterminateWriteException` reporting;
3. the D-S8b-5 SEQUENCE identity preference on the PostgreSQL leg
   (`setIdentityGenerationPreferences([PostgreSQLPlatform::class => GENERATOR_TYPE_SEQUENCE])`,
   one suite-wide `Configuration` call, no fixture patching) — the documented ORM-on-PG adoption
   path, which upstream's own deprecation text recommends;
4. **`COLUMNS=120` is pinned for the phpunit process.** Not cosmetic: nine
   `ORM\Tools\Console\Command\*` tests assert Symfony Console output verbatim and Symfony wraps to
   the terminal width, so an unpinned baseline would encode the terminal the recording agent sat in
   (measured this task: unset -> 9 failures, 100 -> 4, 120 -> 0). It applies identically to the
   ferro and stock legs.

**§20.3 / §14 wording caution (already flagged by the plan verification):** the "(a) and (e) EMPTY"
clause belongs to the DBAL runner only; the ORM runner is a MEASUREMENT bar — (a) empty, (e) filed
and not blocking.

**No `/proto` change and no new wire constant were needed by this task.**

**Fact for §14 / the results doc:** the ORM harness's contact discipline is now three fail-closed
assertions, not two — driver identity, wrapper ancestry, **and that the D-S8b-5 preference actually
took effect** (upstream `configureProxies()` returns early on PHP >= 8.4 with native lazy objects,
so a preference applied after that line is dead code and the suite would report ~1229 errors
carrying the exact D-S8b-5 wording — a broken harness that reads as a genuine PostgreSQL finding).

### Task 4

**The recorded ORM numbers, for §22.2 (ap) and §14.** First run of the Doctrine ORM 3.6.8 functional
suite against Ferro on any backend. Full document + triage:
`docs/orm-suite/2026-08-13-results.md`; committed non-passing baselines in
`docs/orm-suite/baseline/`.

| backend | mode | executed | passed | E | F | S | I | non-passing |
|---|---|---|---|---|---|---|---|---|
| PostgreSQL 17.10 | stock `pdo_pgsql` | 3485 | 3431 | 0 | 0 | 52 | 2 | **0** |
| PostgreSQL 17.10 | **Ferro** | 3485 | 3381 | 47 | 0 | 55 | 2 | **47** |
| MySQL 8.4.11 | stock `pdo_mysql` | 3485 | 3422 | 0 | 4 | 57 | 2 | **4** |
| MySQL 8.4.11 | **Ferro** | 3485 | 3416 | 6 | 4 | 57 | 2 | **10** |
| MariaDB 11.8.8 | stock `pdo_mysql` | 3485 | 3420 | 0 | 0 | 63 | 2 | **0** |
| MariaDB 11.8.8 | **Ferro** | 3485 | 3414 | 6 | 0 | 63 | 2 | **6** |

Triage totals — **category (a) is EMPTY on all three backends**:

| backend | (a) | (b) | (c) | (d) | (e) |
|---|---|---|---|---|---|
| PostgreSQL | **0** | 32 | 5 | 0 | **10** |
| MySQL | **0** | 6 | 4 | 0 | 0 |
| MariaDB | **0** | 6 | 0 | 0 | 0 |

PG (b) = 16 sub-second `TIMESTAMPTZ` read refusals (§22.2 (ab)) + 10 D-S8b-5 `lastInsertId`
(fixtures hard-code `strategy: 'IDENTITY'`, overriding the suite-wide SEQUENCE preference) + 6
multi-table-DQL temp tables (§7.4). PG (c) = 5 `DDC832Test` orphan-sequence collateral (upstream's
sequence cleanup is DBAL-3-gated; controlled against stock, which leaves no sequences and passes all
6). PG (e) = 10 PG bind-matrix refusals (`F64→numeric` ×7, `I64→float8` ×2, `TEXT→int2` ×1).
MySQL/MariaDB (b) = the same 6 temp-table tests; MySQL (c) = the 4 platform-SQL-drift tests, whose
full failure text was diffed between the stock and Ferro JUnit files and is **byte-identical**.

**Facts §14 / §22.2 should carry that the plan did not predict:**

1. **Stock is not green on MySQL but IS clean on MariaDB.** The 4 `MySQL84Platform` DDL-drift
   failures do not reproduce under `MariaDB110700Platform`. Any "byte-identical to stock" sentence
   must be per BACKEND, never per family. MariaDB's Ferro column consequently has zero category-(c)
   rows and 6 non-passing, not the 10 the plan inferred by mirroring MySQL.
2. **The PostgreSQL gap against stock is 50 tests, not 47.** Three tests that RUN on stock are
   SKIPPED on Ferro — `DDC3634Test::testSaves{Integer,VeryLargeInteger}AutoGeneratedValue*` — by
   UPSTREAM'S OWN guard (*"Need a post-insert ID generator in order to make this test work
   correctly"*), because the D-S8b-5 adoption path makes the PG identity strategy SEQUENCE, a
   pre-insert generator. They are three `lastInsertId()`-shaped tests the Ferro PG column does not
   exercise. A result line alone cannot show this; only the ordered SKIP sets can, and MySQL's and
   MariaDB's are identical between modes.
3. **The §7.4 multi-table-DQL cluster now has a MEASURED workaround**, not an inferred one: the same
   statement sequence fails in autocommit (`relation "…" does not exist`) and succeeds inside one
   explicit transaction, measured through `ferro/client`. Worth stating in
   `docs/known-incompatibilities.md` (Task 5) as *wrap the DQL in a transaction*.
4. **Category (e) assignment:** the 10 PG bind-matrix rows reopen
   `docs/followups/2026-08-11-pg-bind-matrix-narrower-than-libpq.md` at **M2-entry**. The 16
   sub-second `TIMESTAMPTZ` rows are category (b), not (e) — a documented policy — and carry a
   separate **policy-decision** follow-up at M2-entry.

**Recording-discipline delta (a real defect fixed in this task's commit, not a spec wish):**
`testkit/orm-suite.sh` wrote `--log-junit` to a FIXED path per `(mode, backend)` regardless of
recordability, so a narrowed DEBUG run silently overwrote a recorded run's JUnit evidence while the
baseline gate correctly reported "not compared". The baseline is the contract; the JUnit is the
evidence the whole triage is derived from and is NOT re-derivable from the baseline. A
non-recordable run now writes `junit-<mode>-<svc>.debug.xml`; proven by re-running the exact
diagnostic that caused the damage (recorded file byte-identical under `md5sum -c`). If §20.3 gains
runner-discipline wording, "a non-recordable run may not overwrite a recorded run's artifacts"
belongs beside "a non-recordable run may not compare or update the baseline".

**No `/proto` change, no engine change, no `php/*/src` change in this task.**
