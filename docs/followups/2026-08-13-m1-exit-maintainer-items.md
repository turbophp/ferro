# M1 exit — the verification record, and the items only a HUMAN can decide

**Written at:** the M1-S9 exit gate (Task 7), on branch `m1-build`.
**What this file is:** the artifact a maintainer reads to decide whether M1 exits. It states each
of §17's four exit-gate parts as **MET** or **NOT MET** with the evidence and where that evidence
lives, then lists every carry M1 is exiting with, flagged **SAFETY** or **FEATURE**, then states
what M1 does **not** claim.

It is written without flattery on purpose. This project has twice measured a gate that passed on
paper — a `OK (105 tests, 211 assertions)` run against in-memory SQLite with zero Ferro contact
(§22.2 (z)), and an `in_tx: true` fate field so unobservable that flipping it to `false` left eight
live suites and 142 lib tests green (§22.2 (ai)) — the project's dominant defect class landing on
its defining safety property. A gate met on paper is the failure mode this whole slice was designed
against, so nothing below is asserted from memory: every number was produced by the run recorded in
`.superpowers/sdd/2026-08-13-ferro-m1-s9-exit-gate/task-7-journal.md`, at the exit HEAD.

---

## Part 0 — the gates, re-run at the exit HEAD

Every gate below was executed in this task, with each command's exit status captured separately
rather than chained, so a failure would be recorded rather than short-circuiting the record.

**Provenance, stated exactly rather than rounded to "at the exit HEAD".** The gates in the table
were run TWICE — once at `917adf4`, the tree this task inherited, and again on this task's own
documentation commit — with **identical results both times**; the table reports the second. The
twelve acceptance runs in Part 1 were executed at `917adf4` only. The diff between `917adf4` and
this commit is three files: `CLAUDE.md`, this ledger, and one corrected caveat paragraph in
`docs/orm-suite/2026-08-13-results.md`. **No Rust file, no `php/*/src` file and no `/proto` input
differs**, and no suite, generator or gate reads any of the three. Re-running twelve acceptance
legs over a documentation diff was therefore not the honest check; saying which tree produced which
number is.

| gate | command | result | exit |
|---|---|---|---|
| Rust format | `cargo fmt --all --check` | clean | 0 |
| Rust lint | `cargo clippy --workspace --all-targets -- -D warnings` | clean | 0 |
| Rust tests, LIVE against PG 17 + MySQL 8.4 + MariaDB 11.8 | `cargo test --workspace -- --nocapture` | **863 passed / 0 failed / 0 ignored** across 84 result lines | 0 |
| no-skip | `./ci/assert-no-skips.sh` | `no-skip gate: every live suite made database contact` | 0 |
| `php/client` | `phpunit` | `Tests: 737, Assertions: 3064, Skipped: 63` | 0 |
| `php/client` live | `phpunit tests/Live --fail-on-skipped` | **`OK (74 tests, 1029 assertions)`** | 0 |
| `php/client` static | `phpstan analyse --level 9 src` | `[OK] No errors` | 0 |
| `php/doctrine-dbal` | `phpunit` | **`OK (296 tests, 1259 assertions)`** | 0 |
| `php/doctrine-dbal` live | `phpunit tests/Live --fail-on-skipped` | **`OK (63 tests, 592 assertions)`** | 0 |
| `php/doctrine-dbal` static | `phpstan analyse src --level 9` | `[OK] No errors` | 0 |
| `/proto` regeneration | `gen-registry-lock` + `gen-php.php` + `touch proto/*.toml && cargo build -p ferro-proto` | `git status --porcelain` **EMPTY** | 0 |

**Two things about that table are not "all green" and are stated here rather than in a footnote.**

1. **`php/client` moved 727 → 737 tests.** The ten are M1-S9's own `DaemonKillFateLiveTest`; the
   live lane moved 64/919 → 74/1029, i.e. +10 tests / +110 assertions, byte-consistent with the
   harness's recorded `OK (10 tests, 110 assertions)`. Expected growth, not drift. Rust 863/0 and
   `php/doctrine-dbal` 296 are exact matches of the slice-entry baselines.
2. **63 `php/client` tests were SKIPPED, and 42 of them are real coverage that did not run.**
   Enumerated, not assumed: 21 are structural (`decode-only for the client in S1 (no message
   encoder)`), and 42 are `ext-msgpack` absent on this host (`php -m` lists no `msgpack`). Charter
   rule 7 makes the extension optional and runtime-detected, so its absence on a dev host is
   legitimate — but the ext-vs-pure packer conformance arm genuinely did not execute, and that
   test's own skip text says why it matters: *"This is a COVERAGE HOLE, not a pass … that gap has
   already shipped a silent corrupt write (`ExtPacker::packBin` emitting `str` instead of `bin`)."*
   GitHub CI's `php` job installs the extension; `FERRO_REQUIRE_EXT_MSGPACK=1` makes the skip a
   failure. **Recorded as a caveat on this run, not as a pass.**

There is no `/proto`-regeneration CI job to replicate — `.github/workflows/ci.yml` has `rust`,
`integration`, `php`, `deny`, `fuzz-smoke` and nothing else — so the check above runs both
generators against an already-clean tree and asserts the tree does not move. Both rewrote their
outputs; the tree did not move.

---

## Part 1 — the four exit-gate parts (SPEC §17, M1 bullet)

### (1) DBAL — **MET**

*Bar (§17 part 1):* §14's acceptance form met on PostgreSQL, MySQL and MariaDB — two runs per
backend reproducing the identical result line AND the identical ordered non-passing set, that set
byte-matching `docs/dbal-suite/baseline/`, five-category triage with **(a) and (e) EMPTY**.

*Evidence, produced at the exit HEAD in this task* — `testkit/dbal-suite.sh`, six runs, all exit 0:

| backend | run 1 | run 2 | baseline |
|---|---|---|---|
| PostgreSQL 17.10 | `Tests: 730, Assertions: 828, Errors: 3, Failures: 7, Skipped: 354, Incomplete: 2.` | identical | `MATCHES docs/dbal-suite/baseline/pg.txt (10 non-passing, exactly as recorded)` |
| MySQL 8.4.11 | `Tests: 730, Assertions: 871, Errors: 2, Failures: 9, Skipped: 341, Incomplete: 4.` | identical | `MATCHES … mysql.txt (11 non-passing)` |
| MariaDB 11.8.8 | `Tests: 730, Assertions: 869, Errors: 2, Failures: 9, Skipped: 342, Incomplete: 4.` | identical | `MATCHES … mariadb.txt (11 non-passing)` |

The ORDERED non-passing set is proven by the runner itself, which exits non-zero on any drift — a
matching result line alone would not prove it. The runner also refuses to run a test until it has
asserted `getNativeConnection() instanceof Ferro\Client\Connection` **and** the required
`wrapperClass`, and round-tripped a real `SELECT 1`; that assertion is why these numbers mean
anything (§22.2 (z)).

*Triage:* `docs/dbal-suite/2026-08-11-s8c-results.md` — every non-passing test in categories (b) and
(c); **"No category (a) driver defect was found in this pass. No category (e) engine gap remains."**

*One thing this re-run was specifically watching for and did NOT see:* the plan's hazard 11, that
Task 1's `fetchFirstColumn` fix might move a DBAL baseline. All three result lines are byte-identical
to the slice-entry values.

### (2) ORM — **MET, as the measurement bar it is**

*Bar (§17 part 2):* the Doctrine ORM functional suite RUN and RECORDED on PostgreSQL + MySQL
(MariaDB additionally) under the same runner discipline — recorded stock comparator, two-run
baseline exact-match, five-category triage, **category (a) EMPTY**, category-(e) rows non-blocking
provided each carries a filed follow-up with an explicit milestone assignment.

*Evidence, produced at the exit HEAD in this task* — `testkit/orm-suite.sh`, six runs, all exit 0:

| backend | run 1 | run 2 | baseline |
|---|---|---|---|
| PostgreSQL 17.10 | `Tests: 3485, Assertions: 11857, Errors: 47, Skipped: 55, Incomplete: 2.` | identical | `MATCHES docs/orm-suite/baseline/pg.txt (47 non-passing, exactly as recorded)` |
| MySQL 8.4.11 | `Tests: 3485, Assertions: 11991, Errors: 6, Failures: 4, Skipped: 57, Incomplete: 2.` | identical | `MATCHES … mysql.txt (10 non-passing)` |
| MariaDB 11.8.8 | `Tests: 3485, Assertions: 11970, Errors: 6, Skipped: 63, Incomplete: 2.` | identical | `MATCHES … mariadb.txt (6 non-passing)` |

Contact banner on every leg: `native=Ferro\Client\Connection`,
`wrapper=Doctrine\Tests\DbalExtensions\Connection` (re-parented onto `FerroConnection`),
platforms `PostgreSQL120Platform` / `MySQL84Platform` / `MariaDB110700Platform`.

*Recorded document + comparator + triage:* `docs/orm-suite/2026-08-13-results.md`, with the stock
`pdo_*` comparator legs and **three** runs per leg at recording time (the bar asks two).
**Stated so the scope of this re-run is not overread: the six runs above are the FERRO legs only.**
The stock comparator legs were not re-executed at the exit HEAD, because nothing either leg executes
changed after `3bd026e` — the two commits since are documentation — and the comparator's value is
the recorded artifact it already is. If that reasoning is ever wrong, the runner will say so: a
`FERRO_ORM_MODE=stock` run compares against `docs/orm-suite/baseline/stock-<svc>.txt` the same way.

*Category (a) is EMPTY on all three backends,* and that is a measured claim rather than an
assumption: the one category-(a) defect this suite found — `Ferro\DBAL\Result::fetchFirstColumn()`
truncating at a first-column boolean `false`, silent wrong ANSWERS — was fixed in `d436f83`
**before** the numbers were recorded, and its ORM witness (`GH9230Test` data set `bool=false`) is
absent from the PG non-passing set while the same test's three `float=` data sets remain.

*Category (e) is 10 tests on PostgreSQL, 0 on MySQL and MariaDB.* Enumerated with follow-up and
milestone in Part 2 below. **Under the renegotiated bar that does not block exit — but read
Part 5 item 2 before quoting this part as a compatibility result.**

### (3) Chaos — **MET for its named cell set, and the harness immediately found two HIGH defects**

*Bar (§17 part 3):* the §20.3 kill-`ferrod` harness BUILT for its named minimum cell set — four
assertion cells, every one mutation-proven RED, plus one pinned measurement whose residual is
recorded rather than asserted.

*Evidence:* `php/client/tests/Live/DaemonKillFateLiveTest.php`, recorded
**`OK (10 tests, 110 assertions)`**, re-executed in this task's `php/client` live lane
(`OK (74 tests, 1029 assertions)`, zero-skip — the +10/+110 delta is exactly this file). These are
the first tests in this repository in which the DAEMON dies mid-request; every prior chaos suite
(`chaos_fate_it.rs`, `mysql_chaos_it.rs`, `in_tx_fate_it.rs`, the two `pre_dispatch_fate_it.rs`)
kills the BACKEND LINK and the daemon survives to classify.

| cell | assertion | backends | mutation |
|---|---|---|---|
| 1 | autocommit non-readonly EXEC → `IndeterminateException`, never re-issued, applied at most once (read-back) | PG, MySQL | RED |
| 2 | in-transaction plain-DML EXEC → never `Indeterminate`; earlier writes proven UNPERSISTED by read-back | PG, MySQL | RED |
| 3 | `readonly`-declared read → never `Indeterminate`; §19.1 `boot_epoch` asserted CHANGED across the SIGKILL restart | PG, MySQL | RED |
| 4 | open stream → exactly one thrown terminal, mid-stream, never a hang, never a silent clean end | PG only (§22.2 (n)) | RED |
| 2b | MySQL implicit-commit prefix survives while the client reports a connection-shaped non-`Indeterminate` error | MySQL | **measurement pin, no named mutation — counted as neither** |

*The anti-false-green discipline is the part worth a maintainer's attention,* because it is the
difference between an acceptance test and a coincidence detector: a cell that kills the daemon can
pass with the daemon ALIVE, since a client-side read timeout is a `TransportException` and the write
classifier turns that into the SAME `IndeterminateException` a daemon death mints. Two defences,
both measured necessary — an io timeout exceeding the parked sleep, asserted in `setUp`; and the
killer stamping `microtime(true)` immediately BEFORE signalling with the cell asserting
`killedAt <= caughtAt`. The weaker form ("has the killer exited by now?") was measured INSUFFICIENT:
with the ordering assertion disabled under a slow-killer mutation, cell 1 reported
`OK (1 test, 14 assertions)` while certifying `Indeterminate` with `ferrod` alive for another 0.94 s.
The ordering proof applies to the sidecar-killed cells (1, 2, 2b, 3); cell 4 kills inline from its
own row loop with DATA frames already received, so it is in flight by construction.

*Stated limits, so "MET" is not read as more than it is:* **MariaDB is not covered** (the harness
launches only the PG and MySQL pools), and the stream cell is PostgreSQL-only.

*And the result that matters most:* **the harness's FIRST live run found two HIGH `php/client`
defects** — carries S1 and S2 in Part 3 — both of which leave a `Connection` permanently unusable
after an ordinary `ferrod` restart, and both of which surface OUTSIDE the fate taxonomy. They are
PINNED, not fixed. That is the argument for having built the harness, and it is also the reason
this part being "MET" is not a statement that daemon-death handling is good.

### (4) D12 — **RE-ANCHORED by recorded amendment. NOT adjudicated, and not passed.**

*Bar (§17 part 4):* formally adjudicated **or** formally re-anchored by recorded amendment — never
silently treated as passed.

*Evidence:* §16.1 now carries an explicit status note, and §17's M1 bullet retires the v0.1
conditional ("Accelerator work lands here iff D12 gate failed") in favour of an M2-entry decision
point. So the bar as written is **MET** — the amendment exists and is recorded.

**What is NOT met, and must not be inferred from M1's exit:** the gate itself. The only recording
is `bench/results/20260727T145858Z-wsl2.json`, `provisional: true, reference: false`, taken on
WSL2 against Docker-proxied PostgreSQL. Its numbers point the wrong way against §16.1's targets:

| | target | measured (JIT off) | measured (JIT on) |
|---|---|---|---|
| p50 | 60 µs | **840 µs** | 848 µs |
| p99 | 200 µs | **1 619 µs** | 1 633 µs |
| overhead vs local PDO | — | +163 µs p50 / +109 µs p99 | +170 / +124 |

The honest reading is that the environment dominates and the targets were **neither met nor
meaningfully failed** — not that they were met. No accelerator work landed in M1, so the v0.1
conditional never evaluated; and charter rule 5 forbids optimising against anything but recorded
numbers, so nothing in M1 was tuned against these. **This is a decision deferred with its cost
stated, not a gate passed** — see Part 4 item 2.

---

## Part 2 — every category-(e) row, with its follow-up and its milestone

Category (e) is *"an engine gap this run measured and did not close"*. It is the category that
separates **measured and scheduled** from **quietly dropped**, so it is enumerated here in full
rather than summarised.

**DBAL runner — (e) is EMPTY on all three backends.** The bar requires it, and it is met.
S8b's three (e) rows (the `pg_index.indkey` `int2vector` blocking the PostgreSQL schema manager,
the PG bind matrix, and the `I64` above 2^32 unreadable in `php/client`) were all closed at M1-S8c;
see `docs/dbal-suite/2026-08-11-s8c-results.md`.

**ORM runner — (e) is EMPTY on MySQL and MariaDB, and is 10 tests on PostgreSQL.** Under the
renegotiated bar (§17 part 2) an (e) row does not block M1 exit **provided** it carries a filed
follow-up and an explicit milestone assignment. Every row does:

| # | cluster | tests | backend | follow-up | milestone | who decides |
|---|---|---|---|---|---|---|
| e1 | PG bind matrix narrower than libpq — `F64 → numeric` (7), `I64 → float8` (2), `TEXT → int2` (1) | **10** | PostgreSQL | `docs/followups/2026-08-11-pg-bind-matrix-narrower-than-libpq.md` (reopened at M1-S9 with these ORM rows and minimal repros) | **M2-entry** | engineering |

That is the complete list. Two things about it must not be flattened:

- **`F64 → numeric` is not the same shape as the `I64 → text` case M1-S8c closed.** A binary float
  carries no display scale, so widening it means deciding what `0.1` means in a `NUMERIC(10,2)`
  slot — which is the coercion class §9.1 exists to refuse. Widening this is a **policy decision**,
  not a mechanical bind-table extension, and it should be taken as one.
- **A sixteen-test PostgreSQL cluster sits NEXT to this list and is deliberately not on it.** The
  sub-second `TIMESTAMPTZ` read refusal is category **(b)** — a documented §22.2 (ab) policy
  working as designed, not a gap — but it is nonetheless worth *deciding* rather than assuming, so
  it carries its own filed follow-up
  (`docs/followups/2026-08-13-orm-timestamptz-subsecond-read-refusal.md`) as a **policy decision**,
  also assigned **M2-entry**. Read its inversion before deciding: stock `pdo_pgsql` does not pass
  those tests because it is more careful — it passes because the value never reaches Doctrine's
  type layer at all. One claim in that file is code-derived and **not live-measured** (that the
  same refusal reaches a MySQL `TIMESTAMP(6)` column, since the rule has no backend branch); it is
  labelled as such there and must not be promoted to a measured claim.

---

## Part 3 — every open carry M1 is exiting with

**SAFETY** = it can produce a wrong answer, a lost write, a replayed write, a fatal in a caller, or
an outage. **FEATURE** = it is missing capability. A SAFETY item crossing this gate must be crossed
*knowingly*; that is the entire reason this section exists.

### SAFETY carries

| # | carry | severity | where | status |
|---|---|---|---|---|
| S1 | **`ReconnectLoop::reconnect()` leaves a permanently dead `Connection`.** On exhaustion it closes the session, rethrows the raw last dial error and never replaces the session; every later call raises a raw PHP **`TypeError: fwrite(): supplied resource is not a valid stream resource`** — *outside* the §9.2/§19.3 taxonomy, so `catch (FerroException)` never sees it and under PHP-FPM it is an uncaught fatal (500). The default budget (`maxAttempts = 3`, 0.05 s base, instant `ECONNREFUSED`) exhausts in **under ~0.35 s**, i.e. faster than any real `systemctl restart ferrod` — so this is the COMMON case for a restart, not a corner. | **HIGH** | `php/client/src/Client/ReconnectLoop.php:108`; §21 engineering open item 5; §22.2 (aq); pinned by `DaemonKillFateLiveTest::testReconnectExhaustionLeavesTheConnectionPermanentlyDeadPinnedDefect` | **PINNED, NOT FIXED. No milestone assigned — the next planning pass must place it.** Write-up + fix sketch: `docs/followups/2026-08-13-client-recovery-unreachable-after-daemon-death.md` |
| S2 | **A mid-stream wire failure poisons the `Session`.** `Session::$streamOpen` is never cleared, so every later request on that connection is a `ProtocolException` demanding that a stream be driven to a terminal it can no longer reach (the socket is dead). Same permanent-death outcome as S1, different mechanism. | **HIGH** | `php/client/src/Client/Session.php`, `Connection.php`; §21 item 5; §22.2 (aq); pinned by `DaemonKillFateLiveTest::testMidStreamDaemonDeathPoisonsTheSessionPinnedDefect` | **PINNED, NOT FIXED. No milestone assigned.** Same follow-up file. |
| S3 | **The streaming abort path's drain is UNBOUNDED** — the last known charter-rule-4 hole. The abort awaits the row-stream handle's `finish()` (which drains the remainder) before declaring its terminal, with no bound; against a wedged backend the request receives **no terminal at all**. M1-S9a closed exactly this shape on both buffered EXEC paths (`CANCEL_DRAIN_BUDGET = 5 s`) and deliberately left the streaming one, because it touches the measured-sound exactly-one-END ordering machinery. | **MEDIUM-HIGH** (violates charter rule 4) | §22.2 (ak), "KNOWN, UNFIXED, same class" | Open. Wants a plan entry, not an implementer's discretion. |
| S4 | **Across a DEAD daemon a client cannot learn its transaction's earlier writes already persisted.** §19.3 amendment (1) makes the engine withdraw `Retryable` once `tx_writes_persisted` is set, but that latch is engine-side pool state and dies with the process. On MySQL/MariaDB a caller whose transaction ran an implicitly-committing statement and then lost `ferrod` gets a connection-shaped, **non-`Indeterminate`** error over an already-durable prefix. Measured: `k1` durable with no `COMMIT` ever sent. | **MEDIUM** (it is honest-but-uninformative, not a wrong verdict) | §21 item 4; §22.2 (aq); §19.3's client-side limit | **PINNED** (`DaemonKillFateLiveTest::testImplicitCommitPrefixSurvivesDaemonKillOnMysqlPinnedResidual`). Fix is a `/proto` change, deliberately NOT hand-rolled (charter rule 2) — see FEATURE F1. |
| S5 | **The implicit-commit hazard cries wolf** for an in-transaction statement that errored WITHOUT running: it reports `WRITE_UNCONFIRMED{Indeterminate}` where the transaction is in fact known-dead. Safe direction (a wrong "yes" costs a retry that does not happen; a wrong "no" licenses replay), and the alternative is exactly the SQL inference charter rule 6 refuses. | LOW (safe direction) | §22.2 (ai)'s residual paragraph; §19.3 amendment (1) | Recorded, not closed. |
| S6 | **The accept loop hot-retries under `EMFILE`/`ENFILE`.** `accept(2)` errors are warn-and-continue and fd exhaustion is level-triggered, so the loop spins. The `max_connections = 512` default keeps the daemon off that cliff; the loop still wants a bounded backoff. | LOW-MEDIUM (availability) | §22.2 (al), "KNOWN, UNFIXED" | Open. |
| S7 | **A drain-deadline terminal for an in-flight tx statement reports `PROTOCOL{NonRetryable}`** where `TxDeadline{Retryable}` would inform better. A pre-existing teardown-ordering race, safe either way — the caller is told the write did not commit, which is true. | LOW (informational) | §22.2's M1-S4 session-death-teardown entry, refined in place by M1-S9a Task 12 (the two entries had recorded opposite arms; both are true of different instants) | Recorded, not closed. |
| S8 | **§21 open item 1 — the pre-`HEAD` `Indeterminate` sighting: the QUESTION is answered, the OBSERVATION is not.** *Can* a terminal produced before the first row reach the fate matrix's `sent` arm? **Yes** — the stream OPEN pre-builds `sent: true` — and §19.3 amendment (2) makes that cell `ConnectionLost{Retryable}`, live-proven on all three backends. But the original sighting captured **no message text** and never reproduced in 16 further runs, so nothing proves it was this mechanism. | LOW (mechanism fixed; provenance unknown) | §21 engineering open item 1 | Open as an observation. |
| S9 | **§21 open item 2 — a 1-in-12 flake** in `StreamingLiveTest::testABoundIteratorThatIsAbandonedStillTransfersTheRemainder`, on a path the M1-S8b stream fix does not touch, not reproducible in 16 further runs. Item 1 reads that observation as a fate question; item 2 reads it as a stability question, and **closing either does not close the other**. | LOW | §21 engineering open item 2 | Open. |
| S10 | **§21 open item 3 — `php/doctrine-dbal`'s live tier uses FIXED-NAME fixtures in the SHARED `ferro` database** (`s8b_retryable`, `s8b_inter`, …), so two concurrent runs clobber each other. It cost real diagnosis time twice in this milestone. The fix pattern is already in the tree (`testkit/dbal-suite.sh` gives each family its own `doctrine_tests` database). | LOW (test infrastructure, not production) | §21 engineering open item 3 | Open. |
| S11 | **NEITHER M1-S9a NOR M1-S9 has had a whole-branch adversarial pass by a second model.** This is the standing caveat and it has moved, not gone. Every slice that HAS had such a pass produced confirmed defects — including the ones where every gate was green: M1-S8b (6 blockers, 15 majors, all gates green), the M0 core review (a silent at-least-once data-corruption blocker), and M1-S9a's own review (a blocker that had RESURRECTED the exact bug that slice existed to close). S9a changed the fate matrix, the pool and the session layer. | **the largest unquantified risk at this gate** | §21 maintainer items; CLAUDE.md | **Decide: schedule the pass at M2 entry, or accept the risk in writing.** |

### FEATURE carries

| # | carry | where | status |
|---|---|---|---|
| F1 | **A `/proto` change is owed and deliberately deferred, batched.** Three wire candidates this milestone WANTED and refused to hand-roll (charter rule 2): a per-statement `tx_writes_persisted` flag on the in-transaction EXEC terminal (S4's fix), `affected` on the stream terminal (without it the prepared path cannot stream — the last gap in §14's never-buffer clause), and a `TxNotFound` code (so `rollBack()` need not swallow `ERR_PROTOCOL`). Batched because one `/proto` change costs the registry, the golden vectors and BOTH codecs in one change set. | §21 item 4; §22.2 (aq) | **With the next `/proto`-touching slice.** |
| F2 | **MySQL/MariaDB `query_stream` does not exist** — `fetch:stream` returns a clean `Unsupported` there, and the DBAL tier buffers. | §22.2 (n) | M2 or later. |
| F3 | **The Eloquent / Laravel tier does not exist.** `php/laravel` is not a directory. §15's own acceptance bar still says "green" and carries a flag that it must be restated in §14's exact-match + triage form at M2 planning — and it also still names **SQLite**, which does not exist until F8 lands, so restating that bar has a prerequisite as well as a form. | §15; §17 M2 | M2. |
| F4 | **Observability does not exist.** §13 specifies OTLP spans, Prometheus (pin-cause counters, `indeterminate_total`, the `boot_epoch` gauge), a slow log and `ferro top`. `grep -r 'opentelemetry\|prometheus\|otlp' engine --include=*.toml` returns nothing. An operator running Ferro today has the daemon's logs and no metrics. | §13; §17 M2 | M2. |
| F5 | **TLS to upstream databases does not exist, and it is in NO milestone bullet.** §12 specifies rustls, per-pool CA/cert config and `sslmode` equivalents; `grep -rn 'rustls\|native-tls\|sslmode' engine/crates` returns nothing. §17's M0–M5 bullets do not name it. This is a stated security-model property with no owner and no date — the gap that matters is the *absence from the roadmap*, not the absence from M1. | §12 | **UNSCHEDULED — needs a milestone.** |
| F6 | **Packaging does not exist** — no deb/rpm, no container sidecar, no systemd templated or socket-activated units, though §18 specifies the deployment model and §22.2 (am) has already changed what `drain_deadline` means for a unit's `TimeoutStopSec`. | §18; §17 M5 | M5 per §17 — worth re-dating, since the deployment semantics are already shipping. |
| F7 | **Nightly CI execution of the upstream suites is UNBUILT.** `.github/workflows/ci.yml` has five jobs (`rust`, `integration`, `php`, `deny`, `fuzz-smoke`), no upstream-suite job and no `schedule:` trigger. The DBAL and ORM numbers are reproduced by hand. Recorded in §20.3 as an aspiration, not cited as a property. | §20.3 | Unscheduled. |
| F8 | **SQLite has no backend** — `AnyPool` is `{ Pg \| Mysql }`. It was REMOVED from the M1 acceptance sentence by amendment and re-enters with M2's engine-owned mode. | §14; §17 M2; §22.2 (ao) | M2. |
| F9 | Smaller named carries, each already in §22.2 or the M2 list: the tracker-clean hygiene `None`-skip (R2), chunked `LARGE_OBJECT` bind, savepoint verbs in the assist-lexer safe-list, the DBAL `^3.8` bridge (D2), the `Ferro\Pg\Copy` API §14 names as not existing yet, and the full per-package incompatibility catalogue (`docs/known-incompatibilities.md` is the stub). | §14, §17 M2, §22.2 | M2+. |

---

## Part 4 — items that need a HUMAN decision (no agent can discharge these)

1. **D7 — naming / trademark check (crates.io, Packagist, trademark), flagged in §21 as a
   maintainer task "before M1". M1 is exiting WITHOUT it.** "Ferro" is still a placeholder in the
   decision log while it is already the package name in every `composer.json`, the crate prefix across
   the seven-crate Cargo workspace, the PHP root namespace and the on-disk socket path. **Decide: do it now, or re-date
   the row in §21 to say when.** Doing it later is more expensive every month, and it is a rename
   that reaches the wire (`/run/ferro/{schema_hash}.sock`), not just the docs.
2. **D12 — RE-ANCHORED, not adjudicated (§16.1 status note; §17 part 4).** The only recording is
   `bench/results/20260727T145858Z-wsl2.json`, `provisional: true, reference: false`, on WSL2 with
   Docker-proxied Postgres. Its numbers point the WRONG way against §16.1's targets and should be
   read as environment-dominated rather than as a verdict: target p50 **60 µs** / p99 **200 µs**;
   measured Ferro p50 **840 µs** / p99 **1 619 µs** (JIT off; JIT on is within noise of that), with
   Ferro's overhead over local PDO at **+163 µs p50 / +109 µs p99**. **Nothing in M1 improved it and
   nothing in M1 was allowed to be optimised against it** (charter rule 5 optimises only against
   recorded numbers). The v0.1 conditional "accelerator work lands in M1 iff the M0 p99 gate fails"
   never evaluated and is retired. **Decide: procure or schedule a reference environment (the §21
   maintainer item "reference-hardware sign-off for §16" is now load-bearing), or consciously carry
   an unadjudicated performance gate into M2.**
3. **Vendored-fork CVE exposure — FIVE fork edits ride production and NEITHER upstream PR is
   filed.** `[patch.crates-io]` redirects both `tokio-postgres` 0.7.18 (four accessors:
   `transaction_status`, `parameter`, `clear_typeinfo_statement_cache`, and the per-column `Bind`
   result-format pair `set_result_format_policy` + `Column::result_format`) and `mysql_async` 0.37.0
   (`CLIENT_SESSION_TRACK` in the negotiated capabilities). `UPSTREAM_PR.md` and
   `UPSTREAM_PR_MYSQL_ASYNC.md` are both stamped **"DRAFT — not yet submitted (pending maintainer
   sign-off / human authorization)"** with an empty PR URL. **Filing needs human authorization; every
   un-filed month grows the CVE lag,** because a security release upstream does not reach a
   `[patch.crates-io]` path dependency — someone has to re-apply the patch by hand and notice that
   they must.
4. **Schedule the whole-branch adversarial pass for M1-S9a and M1-S9** (carry S11 above), or accept
   the risk in writing. This is the single largest unquantified risk crossing this gate.
5. **License selection — §21 lists it as OPEN, and the tree currently states TWO DIFFERENT
   LICENCES.** Measured at this HEAD, not inferred: the root `LICENSE` file is **MIT**
   (`Copyright (c) 2026 turbophp`, 21 lines, no `NOTICE`, no `LICENSE-*` companions), while
   **every package manifest declares `Apache-2.0`** — `Cargo.toml:17` (`[workspace.package]`, so it
   is inherited by all seven crates), `php/client/composer.json:5` and
   `php/doctrine-dbal/composer.json:5`. Packagist and crates.io publish the MANIFEST field, so as it
   stands the ecosystem would be told Apache-2.0 while the repository ships MIT, and the copyright
   holder named in the file (`turbophp`) is not the name any package uses. This is not an agent's
   call to make — the two are not interchangeable (Apache-2.0 carries an explicit patent grant and a
   NOTICE obligation that MIT does not). **Decide the licence, then make the file, the three
   manifests and the copyright holder agree**, and close §21's row.
6. **Security-review scheduling before any public beta** — §21's standing maintainer open item,
   restated here so that exiting a milestone does not bury it. Note what has accumulated since it
   was filed: two credential-hygiene leaks found and fixed in logging alone (§22.2 (an)), an
   availability class where one local client could pin the host-wide daemon (§22.2 (al)), and a
   security model (§12) whose TLS clause is unimplemented and unscheduled (carry F5).

---

## Part 5 — what M1 does NOT claim

Stated plainly, because the numbers above are easy to read as more than they are.

1. **SQLite is not supported. There is no SQLite backend at all** — not a partial one, not a
   degraded one. `AnyPool` is `{ Pg | Mysql }`. The v0.1 bar's third leg ("DBAL 4 functional test
   suite green on PG + MySQL + **SQLite**") was removed by amendment rather than narrowed away,
   precisely because the upstream suite's own fallback demonstrates a *green SQLite run with zero
   Ferro contact* (§22.2 (z)) — leaving those words standing was actively dangerous.
2. **The ORM half of the bar is a MEASUREMENT bar, not a green bar, and this is not a rhetorical
   softening.** The driver Ferro replaces is not green either: **stock `pdo_mysql` fails 4 of 3485
   tests** on MySQL 8.4.11 (ORM-3.6.8-vs-DBAL-4.4.4 `MySQL84Platform` DDL-string drift), and those
   same 4 fail byte-identically through Ferro. Against that comparator: **PostgreSQL passes 50
   fewer tests than stock** (47 errors plus 3 that upstream itself skips under the SEQUENCE
   configuration Ferro requires), MySQL 6 fewer, MariaDB 6 fewer. **47 non-passing on PostgreSQL is
   what the product measures at, not what it should ship at.** The DBAL half is a compatibility
   claim; the ORM half at M1 is an honest-measurement claim, and the two must not be quoted as if
   they were the same kind of statement.
3. **Drop-in is config-only for DBAL and explicitly NOT config-only for Doctrine ORM on
   PostgreSQL** (D-S8b-5). `IdentityGenerator::generateId()` is `(int) $conn->lastInsertId()` and
   PostgreSQL reports no generated key through Ferro by design, so a PG ORM application configures
   the SEQUENCE identity strategy. Measured cost of NOT configuring it: **1229 errors, ~35% of the
   suite.** And a per-platform preference cannot override an entity that hard-codes
   `strategy: 'IDENTITY'` — 10 of PostgreSQL's 47 are exactly that.
4. **"3485 tests" is not parity.** Two upstream groups are excluded (`performance`,
   `locking_functional`, matching upstream's own CI), the second-level-cache job is not run, and
   streaming is not exercised AS streaming on the MySQL family because it does not exist there.
   *One caveat the results document states has been PARTLY discharged by this gate and is corrected
   here rather than repeated:* it says day-over-day reproducibility is untested because all runs
   were made in one session. The recording commit `3bd026e` is dated **2026-08-18**; this exit-gate
   run re-executed all six Ferro legs on **2026-09-08**, in a different session and after the
   testkit containers had been stopped and restarted (same containers, same image digests as the
   recorded manifest), and reproduced **every result line and every ordered non-passing set
   exactly**. The DBAL legs likewise. What is still single-session is the **stock comparator**,
   which was not re-run — so the Ferro columns are now cross-day reproducible and the comparator
   columns are not.
5. **The chaos harness covers PostgreSQL and MySQL. MariaDB is NOT covered** — the harness launches
   only the PG and MySQL pools — and the stream cell is PostgreSQL-only, because MySQL-family
   streaming does not exist (§22.2 (n)). Recorded, not implied.
6. **`php/client`'s §19.2 transparent-recovery promise currently holds only for the case where the
   daemon is back before the client notices.** Carries S1 and S2 are why. That is a smaller promise
   than §19.2's prose reads, and the harness that proved it is the same one this gate cites.
7. **This gate ran on a host without `ext-msgpack`.** 42 of `php/client`'s 63 skipped tests are the
   extension's arm of the packer-conformance and bind gates, including the ext-vs-pure conformance
   test whose own skip message calls this "a COVERAGE HOLE, not a pass" — that gap once shipped a
   silent corrupt write (`ExtPacker::packBin` emitting `str` instead of `bin`). GitHub CI's `php`
   job installs the extension; this local exit-gate run did not exercise it.
8. **No performance claim of any kind is made by M1.** See item 2 of Part 4.
