# C3 — the SQLite backend: scope, and the one spec tension that has to be settled first

**Status:** SCOPING ONLY. No `ferro-backend-sqlite` code exists, and none is added here.
**The §1 tension is SETTLED (2026-09-14): the owner chose Option C**, recorded as **SPEC D13** with
§7.6 amended to match (§22.2 (az)). §1 below is kept as written — it is the reasoning the decision
rests on — with the outcome noted at the end of it.

**C3-1 IS DONE (2026-09-14): all four premises were spiked and ALL FOUR HOLD** — see
`engine/crates/ferro-sqlite-spike/` (a spike crate, NOT the backend) and §8 below. Every
`UNVERIFIED` marker in this document is now resolved, including §1's `SQLITE_BUSY_SNAPSHOT`
reproduction, which D13's own revisit note required before any code slice. **C3-2 (the
`compose_begin_sql` SQLite arm) is unblocked.**
**Why now:** C3 is what BOTH acceptance bars are still missing — §14's names SQLite, §15's names
SQLite, and both were closed with that column recorded as unreachable. Nothing else in M2 blocks
those bars.

Every premise below is marked **VERIFIED** (read out of the tree at `ce8e3b8`) or **UNVERIFIED**
(believed, not checked). The habit is deliberate: this phase has repeatedly found that the premise
was the defect.

---

## 1. The tension: §7.6 asks for something charter rule 6 forbids

SPEC §7.6, in full:

> The engine owns the file. WAL mode, one writer serialized engine-side, many readers. This removes
> PHP's worst SQLite failure mode (`SQLITE_BUSY` storms under FPM) and makes SQLite a legitimate
> small-service backend. Online backup API exposed via admin service for snapshots.

**"One writer serialized engine-side" requires knowing which statements write.** Charter rule 6 and
SPEC §3 forbid exactly that: *no read/write inference*. §1's own bullet says it again for the
neighbouring case — *"No read/write inference from SQL text for replica routing in v1 — routing is
explicit (§7.6)"*.

So §7.6 cannot be implemented as literally written. This is not a small ambiguity to paper over: it
decides the backend's core shape, and picking one quietly is how a rule gets eroded by
implementation. Three ways out, none of which infers anything.

### Option A — `busy_timeout` and nothing else

Open every connection with WAL and `PRAGMA busy_timeout = <checkout_timeout>`; let SQLite's own
writer lock do the serializing.

- Needs no new signal and no new seam.
- Delivers the *stated benefit* (`SQLITE_BUSY` storms) for the ordinary case.
- **Does NOT cover the deferred-upgrade case.** A transaction that begins as a reader and later
  writes can get `SQLITE_BUSY_SNAPSHOT`, which `busy_timeout` does **not** retry — the transaction
  must be rolled back and replayed, and charter rule 3 forbids the engine replaying it.
  ***VERIFIED 2026-09-14 (C3-1, `premises_it.rs::p4a`)*** — reproduced against a real WAL database
  on SQLite 3.x via `rusqlite` 0.40: the upgrade fails `SQLITE_BUSY_SNAPSHOT` (extended code 517)
  and returns in **under 500 ms with a 5-SECOND `busy_timeout` armed on the very connection that
  fails**, i.e. the retry loop is never entered. The elapsed-time assertion is the load-bearing
  half; observing the error alone would be equally consistent with "busy_timeout retried for 5 s
  and gave up", a far more benign claim. `p4b` is its CONTROL on the same build and the same API:
  with the lock taken at `BEGIN IMMEDIATE`, contention moves to the other connection, surfaces as a
  PLAIN `SQLITE_BUSY` (5), and `busy_timeout` genuinely parks for the full duration — so the knob
  works, and is simply inapplicable to an upgrade.

### Option B — every explicit transaction takes the write lock (`BEGIN IMMEDIATE`)

Treat every transaction as a writer, because the engine may not ask which it is.

- Needs no new signal: it is a change to `compose_begin_sql`'s SQLite arm, which today emits a bare
  `BEGIN` and refuses isolation/readonly outright (**VERIFIED**, `ferrod/src/tx/actor.rs`).
- Eliminates the deferred-upgrade class **by construction** — the lock is held from BEGIN, so no
  transaction ever upgrades.
- Costs read-only transactions their concurrency. That is the *same conservative trade* the driver
  tiers already make and record: §22.2 (ac), "every statement is fate-declared a WRITE", accepted
  because it never costs safety.

### Option C — honour the client's own `readonly` DECLARATION

The wire already carries `readonly` per request, and it is already load-bearing for §19.3.

- **VERIFIED at the transaction level:** the TX service composes BEGIN from the request's
  `readonly`, and PostgreSQL already emits `START TRANSACTION READ ONLY` from it. So a
  declared-read-only SQLite transaction could take a deferred (reader) lock and everything else
  `IMMEDIATE`, with no inference anywhere — the client declares, the engine obeys.
- **VERIFIED as a gap for autocommit statements:** `readonly` reaches `ferrod`'s SQL service
  (`services/sql.rs`) but **not** `ferro-pool` — the pool has no `readonly` anywhere except two
  comments. Routing an autocommit statement to a reader connection therefore needs a new seam
  through `Pool::checkout`.
- Inherits (ac)'s cost honestly: the Doctrine tier declares every statement a write, so it would get
  no read concurrency at all. Conservative, never wrong, and it improves on its own as tiers learn
  to declare.

### Recommendation

**B as the floor, C as the improvement, A as neither.** B alone is correct and needs no new seam;
C is B with the client's existing declaration honoured where it exists. A is rejected as a *design*
because it leaves a failure class the engine is forbidden to resolve — though the `busy_timeout` and
WAL pragmas from A are wanted regardless.

**This should be recorded as a D-series decision (SPEC §21) and §7.6 amended to match**, because
§7.6 as written cannot be implemented and a reader has no way to know that.

### DECIDED 2026-09-14 — Option C (SPEC D13)

The owner chose **C**. Recorded as **D13** in §21, with §7.6 rewritten from "one writer serialized
engine-side" to the declaration rule, and the contradiction itself documented in §22.2 (az) so the
next reader learns that §7.6 was wrong rather than inheriting a silent reinterpretation.

**Implementation ordering** (not a re-litigation — C's own floor): the `compose_begin_sql` SQLite
arm goes first, since C's behaviour for an UNDECLARED request is exactly B's, and that half stands
alone as the correctness floor. The `readonly` seam through `Pool::checkout` is a SEPARATE slice —
it is the genuinely new plumbing, it is what earns C over B, and it is not SQLite-specific: single-
flight read coalescing and read-your-writes replica routing each need the same seam. Sequence the
work so a failure in the second slice cannot be confused with a failure in the first.

**Two things this decision does NOT license.** It does not license inferring `readonly` when the
client did not declare it — that is the whole point. And it does not license skipping the
`SQLITE_BUSY_SNAPSHOT` reproduction: the premise is marked UNVERIFIED above and is the crux of the
rejected option, so it is proven before the first slice, not waved through because the decision
already went the other way.

---

## 2. What already exists (VERIFIED)

| piece | state |
| --- | --- |
| `ferro_classify::Dialect::Sqlite` | exists; `classify` routes to `rules::classify_one_sqlite` |
| `classify_one_sqlite` | real but minimal — pins on `ATTACH`/`PRAGMA` and on the `pin_functions` escape hatch, otherwise the shared safe-leading-keyword list and `pin_on_unknown` |
| `compose_begin_sql(Dialect::Sqlite, …)` | emits `BEGIN`; **refuses** isolation and `readonly` with a message that says the arm exists so one cannot silently inherit PG syntax |
| `ferro-backend-sqlite` | **does not exist** |
| `AnyPool` | two arms (`Pg`, `Mysql`); a third is required |

## 3. What the `PoolBackend` seam demands, and where SQLite does not fit it (VERIFIED against the trait)

The trait is per-connection and has no notion of a read-vs-write checkout, which is the *good* news
for §1: nothing about it forces inference. The genuine mismatches are elsewhere.

- **`tx_status(&self, conn) -> TxStatus` is SYNCHRONOUS and no-round-trip**, documented as mirroring
  PostgreSQL's `ReadyForQuery` byte. SQLite has no protocol at all — it is a library — so there is
  no byte to read. `sqlite3_get_autocommit()` is the natural equivalent and is likewise cheap and
  synchronous, so the seam fits; but it is a *different mechanism*, and the pin engine's authority
  documentation (§7.1) says "protocol signals" throughout and would need amending rather than
  quietly re-reading.
- **`Failed` (an aborted transaction) has no SQLite equivalent**, exactly as it has none for MySQL —
  so the same "never produced from a backend signal, only via `error_map`" rule applies.
- **Everything is synchronous.** `rusqlite` is blocking, so every trait method has to cross
  `spawn_blocking`, and `query_stream`'s incremental pull (`BackendRows`) has to pull rows across
  that boundary. This is the largest single piece of work and has no analogue in either existing
  backend. *(UNVERIFIED: whether `RowStream: BackendRows + Send` can be satisfied by a
  `spawn_blocking`-fed channel without buffering the whole result — that is the property §14's
  never-buffer clause needs, and it must be proven with a spike before the slice is planned.)*
  ***VERIFIED 2026-09-14 (C3-1, `premises_it.rs::p2`)*** — 100 000 rows cross a capacity-1
  `tokio::sync::mpsc` from a `spawn_blocking` task that owns the `Connection`; after the consumer
  takes 10 and then idles for 250 ms the producer has still produced fewer than 64, i.e. it is
  parked on backpressure. The blocking task then hands the `Connection` BACK and it is usable and
  `is_autocommit()` — the park/unpark shape B2b-2a already built for MySQL, so the existing
  `reclaim_stream` seam fits and a stream need not cost a discarded connection.
- **`cancel_handle` / `Cancel`.** PostgreSQL and MySQL both cancel over a SIDE connection. SQLite's
  equivalent is `sqlite3_interrupt()`, which is callable from another thread on the same handle —
  *(UNVERIFIED against `rusqlite`'s API surface: whether an `InterruptHandle` is obtainable and
  `Send + 'static` as the trait's supertrait bound requires.)*
  ***VERIFIED 2026-09-14 (C3-1, `premises_it.rs::p3`)*** — `Connection::get_interrupt_handle()` is
  public and the handle is `Send + Sync + 'static` (asserted at COMPILE time, which is the premise
  that was in doubt) and actually stops a running statement (asserted at RUN time, because a handle
  that satisfied the bounds but silently did nothing would give the pool a cancel that never
  cancels). The statement ends `SQLITE_INTERRUPT` (9).
- **Hygiene.** There is no `DISCARD ALL`. The reset profile has to be assembled from what SQLite
  actually leaks between tenants: temp tables, `PRAGMA`s, `ATTACH`ed databases, prepared statements.
  `classify_one_sqlite` already pins on `ATTACH`/`PRAGMA`, so the taint signal exists; the reset does
  not.

## 4. What §7.6 asks for beyond the backend

- **"The engine owns the file"** — a deployment property, not code: the DSN names a path, and the
  pool's connections are the only writers. Worth stating in §18 rather than implementing.
- **Online backup via the admin service** — `sqlite3_backup_*`. A separate slice; it touches the
  admin surface (§13), not `PoolBackend`.

## 5. Effort, and why it is not smaller than M1-S6

Comparable to or larger than the MySQL backend, for three reasons that are structural rather than
volume:

1. The **pin-authority mechanism is different in kind** (a library call, not a protocol byte), so
   §7.1's own framing needs amending, not just a new arm.
2. **Synchronous driver** — every method crosses `spawn_blocking`, and streaming has to pull rows
   across it without buffering.
3. **The §7.6 tension above has to be settled first**, and it is a spec change, not a coding choice.

Against that, three things are *easier* than M1-S6: no vendored driver fork is needed so far
(***VERIFIED 2026-09-14 (C3-1, `premises_it.rs::p1`)*** — and stated honestly: every capability the
`PoolBackend` seam demands is reachable on rusqlite's PUBLIC API — `is_autocommit()` (the
`ReadyForQuery`/`SERVER_STATUS_IN_TRANS` analogue, synchronous and round-trip-free),
`get_interrupt_handle()`, `busy_timeout()`, `last_insert_rowid()`, `execute()`'s affected count, and
extended error codes. That is a checkable claim; "no fork will ever be needed" is not. S1 and S6
both turned out to need a fork, and the assumption was proven FALSE for `mysql_async` before any
code was written, so it was checked here rather than assumed); no server to run in
CI, so every gate is reachable in a plain container; and the type matrix is small, since SQLite has
five storage classes.

## 6. Proposed slicing

- **C3-0** — settle §1 as a D-series decision and amend §7.6. *No code.*
- **C3-1** — a spike, not a slice: prove (a) `rusqlite` gives a `Send + 'static` interrupt handle,
  (b) a `spawn_blocking`-fed `BackendRows` streams without buffering, (c) no driver fork is needed,
  and (d) §1's `SQLITE_BUSY_SNAPSHOT` behaviour, which D13 requires reproduced. **DONE 2026-09-14 —
  all four HOLD**, `engine/crates/ferro-sqlite-spike/`. See §8.
- **C3-2** — the `compose_begin_sql` SQLite arm. **DONE 2026-09-14** — see §9.
- **C3-3a** — the crate, `SqliteConn`, and connection setup: open, WAL, `busy_timeout` bounded by
  `checkout_timeout`, `query_only` arm/disarm, plus `connect`/`ping`/`is_closed`/`dialect`.
  **DONE 2026-09-15** — see §11.
- **C3-3b** — `tx_status` (via `is_autocommit`), `reset`/`clean_reset_profile`, `simple_query`.
- **C3-3c** — `query` + the row/value mapping (SQLite's five storage classes → the §9 tags).
- **C3-3d** — `cancel_handle` off `InterruptHandle`, and the `error_map` fate table.
- **C3-3e** — the third `AnyPool` arm: the backend becomes reachable at runtime here, and ONLY
  here. Everything above is unit-testable in-crate; nothing before this point changes `ferrod`.
- **C3-4** — the `readonly` seam through `Pool::checkout` (the autocommit half of D13).
- **C3-5** — `query_stream` + `reclaim_stream` (until then, a clean `Unsupported`, exactly as the
  MySQL backend shipped at M1-S6).
- **C3-6** — the acceptance columns: the DBAL suite's SQLite column (§14) and the Illuminate suite's
  (§15), each with the C2e control column alongside it.
- **C3-7** — the online backup admin surface (§7.6's last sentence).

### Why this order changed on 2026-09-15 (C3-3 planning)

**The `readonly` seam was scheduled before the backend, and that was wrong — it has no consumer.**
`readonly` genuinely does not reach `ferro-pool` (verified: it occurs in `ferro-pool/src` exactly
twice, both in comments). But no backend would READ it: PostgreSQL has nowhere to apply a
per-statement readonly outside a transaction, MySQL likewise, and the SQLite backend does not exist.
Building it first would mean adding a parameter nothing consumes and testing only that it arrives —
scaffolding whose shape is guessed rather than fitted. It is now **C3-4**, after the backend that
gives it a consumer.

**And the backend was one undifferentiated slice, which measurement says it cannot be.** The two
existing backends are **5623 lines (PG)** and **4040 lines (MySQL)**; SQLite will be smaller — five
storage classes instead of two type systems, and the spike already proved every mechanic — but not
by the order of magnitude that would make it one iteration. Left as a single item it would be read
by a future firing as one slice and either half-built or not started. The a–e split above is by
**what can be tested without the next piece existing**, which is why `AnyPool` is last: it is the
step that makes the backend reachable from `ferrod`, so a failure before it cannot be confused with
a failure in the daemon.

## 7. What this document does NOT decide

The **sequencing** — whether C3 comes before the remaining M2 engine backlog (the MySQL `CALL`
blind spot, TCP keepalive, B6b chunked `LARGE_OBJECT`, B7's tracker-coverage proof). C3 is the only
one of them that unblocks an acceptance bar; the `CALL` blind spot is the only one that unblocks a
documented incompatibility (`selectResultSets()`). That is a call for the project owner, and it was
already flagged as open when the SQLite question was first asked.

---

## 8. C3-1 spike results (2026-09-14) — all four premises HOLD

Proven in `engine/crates/ferro-sqlite-spike/tests/premises_it.rs` against real `rusqlite` 0.40
(`bundled` SQLite) and a real on-disk WAL database. **SQLite is the first backend whose spike needs
no server**, so unlike the PG and MySQL lanes every assertion runs in an ordinary container — no
Docker, no compose file, and CI is not the authority for any of it.

| premise | verdict |
| --- | --- |
| P1 — no vendored driver fork needed | **HOLDS** for every capability the seam demands (see §3's marker for the list) |
| P2 — `BackendRows` streams across `spawn_blocking` without buffering | **HOLDS**, and the connection is handed back usable |
| P3 — `InterruptHandle` is `Send + 'static` and interrupts | **HOLDS** (compile-time bound + runtime effect) |
| P4 — `busy_timeout` does not retry a deferred upgrade | **HOLDS** — and `p4b` is its control |

**Two findings worth carrying into C3-2, both of which are about the TESTS rather than SQLite.**

**(1) The first version of the non-buffering proof asserted nothing, and the mutation caught it.**
It read the produced-row counter immediately after taking 10 rows. That passes just as happily with
an unbounded channel — the producer is on another thread and simply has not had time to run away
yet. Widening the channel to 200 000 (the mutation that should have broken it) PASSED. The fix is a
**stall probe**: sleep 250 ms with the consumer idle first, so an unconstrained producer has ample
wall-clock to finish all 100 000 rows; a counter still under 64 afterwards can only mean
backpressure. Re-run against the same mutation it now fails with exactly `100000`. The general
lesson, already in the loop's process notes and now paid for again: *a timing-sensitive assertion
that has not been mutation-proven is usually measuring scheduling latency, not the property.*

**(2) `ffi::ErrorCode::X as i32` is NOT SQLite's numeric code, and it compiles cleanly.**
`ffi::ErrorCode` is an ordinary Rust enum whose discriminants are its own declaration order:
`DatabaseBusy` is **3** while `SQLITE_BUSY` is **5**; `OperationInterrupted` is **7** while
`SQLITE_INTERRUPT` is **9**. Two of this spike's own assertions were written that way and failed
loudly only because the premises held and produced the real codes. **The SQLite `error_map` that
C3-2 writes will key on exactly these values** (the §9.2 fate matrix keys on SQLSTATE/errno pairs),
so compare the typed `code` field against the `ErrorCode` VARIANT, or compare `extended_code`
against a spelled-out number — never cast the enum.

**Still open for C3-2, recorded so it is not rediscovered:** §7.1's pin-authority framing says
"protocol signals" throughout, and SQLite's authority is a library call (`sqlite3_get_autocommit()`
via `Connection::is_autocommit()`). It fits the seam — synchronous and round-trip-free, exactly as
the trait documents — but it is a different MECHANISM, so §7.1 needs amending rather than quietly
re-reading. That is a spec edit C3-2 owes, not a blocker.

---

## 9. C3-2: the `compose_begin_sql` SQLite arm (2026-09-14) — DONE

The first D13 code slice, and deliberately the smaller half. `ferrod::tx::actor::compose_begin_sql`
had a placeholder SQLite arm that emitted a bare `"BEGIN"` for an undeclared request and REFUSED
both `readonly` and every isolation level. It now emits:

| request | composed |
| --- | --- |
| undeclared, or declared a write | `BEGIN IMMEDIATE` |
| declared `readonly` | `BEGIN DEFERRED` |

**The undeclared arm is the load-bearing change, and the old value was not merely weaker — it was
wrong.** SQLite reads a bare `BEGIN` as DEFERRED, so the placeholder emitted precisely the
deferred-upgrade shape C3-1's `p4a` proved is unretryable. Mutation-proven both ways: reverting to
`"BEGIN"` fails the undeclared assertion, and flipping the readonly arm to `IMMEDIATE` fails the
other.

**The isolation level is accepted and emits nothing — a decision, not an oversight.** SQLite has no
`SET TRANSACTION` to emit a level with, and under D13 there is nothing left for a client to ask for:
every write transaction is serialized against every other by the lock it takes at BEGIN, and a
reader sees a consistent snapshot, so no two writers ever interleave and the strongest level a
client can name is what the discipline already provides. *That precision is deliberate — "SQLite is
serializable" is the loose claim, since WAL readers get snapshot isolation and snapshot isolation
alone permits write skew; it is D13's BEGIN-time lock making writers mutually exclusive that closes
the gap.* The level also cannot change blocking behaviour, since `readonly` alone decides the lock
mode.
Refusing would break drop-in for an app that merely configures a level and would buy no safety. An
unknown isolation BYTE is still a client error on every dialect — that is a protocol fault, not a
capability gap. The tests assert the full isolation × readonly cross-product, because a level that
silently flipped `IMMEDIATE` to `DEFERRED` would be a correctness bug no isolation-only assertion
would catch.

**A fifth premise was proven first, because this slice is what creates the hazard (`p5`).** D13's
readonly arm hands a client a DEFERRED transaction, which is `p4a`'s exact setup — so a client that
declares `readonly` and then WRITES walks into the one failure class charter rule 3 forbids the
engine resolving, and it would surface only under concurrency. `PRAGMA query_only=ON` converts it
into an up-front `SQLITE_READONLY` (8): deterministic, provably not executed (so `NonRetryable`,
never `Indeterminate`), reproduced on `p4a`'s own fixture so the two are directly comparable, and
shown to hold with NO contention at all — without that second half, the first would be equally
consistent with "READONLY happened to win the race". The pragma is reversible on the connection,
which is what makes it usable on a pooled one.

**Therefore C3-4 (the backend crate) OWES `PRAGMA query_only=ON`** on a connection checked out for a
declared-readonly request, and must disarm it on recycle. Until it does, a lying `readonly`
declaration is the one remaining way to reach the failure class D13 exists to remove.

**One gap this slice does NOT close, stated rather than implied.** The unit test asserts the engine
EMITS `BEGIN IMMEDIATE`; the spike's `p4b` asserts that string takes the writer lock. Nothing links
them, so changing the emitted spelling would update one and leave the other passing on its own
literal. The MySQL lane solved the same problem with a live lockstep test
(`ferro-backend-mysql/tests/begin_dialect_it.rs`); the SQLite equivalent is not writable until a
SQLite pool exists, so **C3-4 owes it too**. `Dialect::Sqlite` is unreachable at runtime until then,
which is why a unit test is the only gate this slice can honestly offer.

---

## 10. C3-3 planning (2026-09-15) — the sequencing was wrong, and §7.1 is amended

No backend code this iteration. Two things were produced instead, and both were blocking work that
would otherwise have been done badly.

**1. The order was corrected (see §6's note).** The `readonly` seam had no consumer and the backend
was an unslice-able 4000–5600-line block. Both are fixed above.

**2. §7.1 is amended, and the amendment is backed by a new premise (`p6`).** §7.1 said pin decisions
come from "backend protocol signals" and had no SQLite paragraph for pin AUTHORITY at all — SQLite
appeared only in the assist-lexer list. It now names the mechanism (`sqlite3_get_autocommit()` via
`Connection::is_autocommit()`, synchronous and round-trip-free) and generalises the framing: the
invariant is *authority from the backend, never inference from statement text*, and the form of that
report is a protocol byte on PG, a status flag plus trackers on MySQL, and a library call on SQLite.

**`p6` is why the paragraph can say the signal must be read after EVERY statement rather than
tracked.** SQLite ends transactions by itself: a constraint declared `ON CONFLICT ROLLBACK` rolls the
whole transaction back on violation, with nothing in the SQL text saying so, and an engine tracking
its own `BEGIN`/`COMMIT` would hold a pin for a transaction that no longer exists. The **control** is
the same duplicate insert against a plain unique constraint (default `ON CONFLICT ABORT`), which
fails only the statement and leaves the signal reporting in-transaction — mutation-proven by making
the control table `ON CONFLICT ROLLBACK` too, which fails it. Without that half, the first would be
equally consistent with "any error ends a transaction".

**This premise is load-bearing for C3-3b**, which implements `tx_status`: it says the implementation
is a read of the live signal and must never be a cached flag the pool maintains.

---

## 11. C3-3a: the backend crate and connection setup (2026-09-15) — DONE

`engine/crates/ferro-backend-sqlite` exists. `ferro-sqlite-spike` is untouched and stays the
premises suite; the two are separate crates on purpose.

**There is deliberately NO `impl PoolBackend` yet.** The trait has a dozen required methods and this
slice builds four, so implementing it now would mean eight bodies returning `Unsupported` — and a
stub returning `Unsupported` is indistinguishable at the type level from a finished method, so the
incompleteness would stop being visible exactly when it matters. Without the impl the compiler
states the obvious: this is not a backend the pool can hold yet. The methods carry the trait's
signatures, so C3-3b/c adds the impl mechanically and the compiler checks them then.

**The park/unpark bridge is the part that matters**, because every later slice's statement runner
goes through it. `rusqlite` is synchronous and the trait is async, so each call must cross
`spawn_blocking`; `Connection` is `Send` but not `Sync`, so it is MOVED in and MOVED back — the
shape the spike's `p2` proved, and the same one `MysqlConn` uses for streaming.

**What connection setup actually checks:**

- **WAL is verified, not requested and hoped for.** `PRAGMA journal_mode=WAL` RETURNS the mode
  actually in force and SQLite can decline the change, so the return value is compared and a
  non-WAL result is a hard error. *Stated honestly: the failure arm is not reachable in this
  container* — declining WAL needs a filesystem without shared-memory support. The test proves the
  connection IS in WAL; it does not exercise the guard.
- **An in-memory DSN is REFUSED, and the refusal is justified rather than asserted.** SQLite gives
  every `:memory:` connection its own private database, so a pooled `:memory:` DSN would silently
  hand different tenants different databases. The test demonstrates that first — two in-memory
  connections cannot see each other's table — and only then asserts the refusal, across five
  spellings including `mode=memory`. A refusal with no proof behind it is superstition a later
  reader deletes.
- **`busy_timeout` is armed and read back.** It defaults to 5s, matching `PoolConfig`'s default
  `checkout_timeout`. **Wiring debt: a backend cannot see `PoolConfig`, so C3-3e must pass the
  pool's real `checkout_timeout`** to `with_busy_timeout`; until then a non-default value is not
  reflected.
- **`query_only` arm/disarm is built here although nothing calls it yet.** C3-2 shipped
  `BEGIN DEFERRED` for a declared-`readonly` transaction, which is `p4a`'s setup — so this is the
  mitigation for a hazard that already exists in the tree. C3-4's seam decides *when* to arm it.

### Two pieces of my own speculative state, removed on review

Both were found by adversarial re-reading and mutation rather than by the tests passing, and both
are worth recording because the second is the same failure C3-1 had.

1. **A `dead: bool` beside the `Option<Connection>` was unobservable.** `is_closed` already reports
   true from `conn.is_none()`, and the only way to see a `None` is after a lost handle, since a
   caller holds `&mut` across every blocking window. No test could tell the two apart. C3-5 may
   genuinely need the distinction — a row stream parks the connection across a window the pool CAN
   see, which is why `MysqlConn` carries it — and it should be reintroduced *then*, with a test that
   distinguishes them.
2. **An explicit `lose_handle()` in the panic arm was DEAD CODE, and the test was green either
   way.** Deleting it left the panic-contract test passing, because `park` had already moved the
   handle out and the error arm simply never unparks it. So the contract holds *by construction*,
   the test is a regression guard on the behaviour rather than proof that a particular line is
   load-bearing, and the test now says so. This is C3-1's non-buffering lesson again: a green test
   is not evidence that the code under it does anything.

### Open, and deliberately not decided here

**SQLite defaults `foreign_keys` to OFF; Laravel and Doctrine both turn it ON.** That is a drop-in
behaviour decision with an acceptance-suite consequence, not a connection-setup detail, so it is
left to C3-6 rather than slipped in silently. Whichever way it goes, it should be recorded with the
suite evidence behind it.
