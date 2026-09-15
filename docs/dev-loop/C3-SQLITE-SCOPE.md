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
  **DONE 2026-09-15** — see §12.
- **C3-3c** — `query` + the row/value mapping (SQLite's five storage classes → the §9 tags).
  **DONE 2026-09-15** — see §13.
- **C3-3d** — `cancel_handle` off `InterruptHandle`, the `error_map` fate table, and
  `impl PoolBackend`. **DONE 2026-09-15** — see §14.
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

---

## 12. C3-3b: pin authority, hygiene reset, affected count (2026-09-15) — DONE

**`tx_status` is a live read**, and `p6`'s hazard is now proven through the real backend with its
control: an `ON CONFLICT ROLLBACK` violation ends the transaction underneath the engine and the
signal reports `Idle`, while the identical duplicate against a plain unique constraint fails only
the statement and leaves it `InTx`. Mutation-proven — a `tx_status` reporting what the engine's own
`BEGIN` implied fails the first assertion.

`Failed` is returned in exactly one case: a connection with no live handle. That is the ABSENCE of a
signal, not a SQLite signal, and §7.1 was amended to say so precisely rather than left to imply
otherwise.

### The affected count: both of SQLite's counters are wrong, in opposite directions

Measured, not reasoned about:

| | `changes()` | `total_changes()` delta |
| --- | --- | --- |
| 3 rows inserted, then `BEGIN` | **3 — STALE** | 0 ✓ |
| INSERT 1 row, `AFTER INSERT` trigger writes 1 | 1 ✓ | **2 — INFLATED** |
| DELETE 1 parent, 2 `ON DELETE CASCADE` children | 1 ✓ | **3 — INFLATED** |
| batch `INSERT 1; INSERT 2` | 2 (last stmt) | 3 (sum) |

The staleness is not a corner case: `simple_query` is exactly the path the pin hook runs
`BEGIN`/`COMMIT`/`ROLLBACK` through, so a naive `changes()` would report the previous statement's
row count on every transaction boundary.

**So the `total_changes()` delta decides WHETHER anything changed and `changes()` reports HOW MANY.**
That keeps `BEGIN` at 0 and a triggered INSERT at the statement's own 1 — the value PostgreSQL and
MySQL both report for those shapes. Both failure modes are mutation-proven.

**Trade-off stated:** a multi-statement batch reports its LAST statement's count rather than the
sum. PG and MySQL do the same for a simple-query batch, so this is family behaviour.

### `reset`: an explicit list, because there is no `DISCARD ALL` to port

Four things a pooled SQLite connection can carry into the next tenant, and what undoes each:

1. **An open transaction** → `ROLLBACK`, gated on the live signal (it errors when none is open).
2. **`PRAGMA query_only`** armed by a declared-`readonly` checkout → disarmed.
3. **ATTACHed databases** → `DETACH`. §7.1's assist lexer already names `ATTACH` pin-worthy, which
   is the admission that it leaves connection state behind.
4. **Temp objects** → dropped. `CREATE TEMP TABLE` lives in the per-connection `temp` schema and
   survives until the CONNECTION closes, so it outlives the checkout that made it.

**Both profiles do the same thing, and that is stated rather than hidden.** PostgreSQL uses
`Targeted` to avoid `DISCARD ALL` destroying its prepared statements; SQLite's reset is in-process,
issues no round trip, and destroys no prepared statements, so a `Targeted` differing from `Full`
would be a distinction with no behaviour behind it. `clean_reset_profile` is `Some(Full)`. `None`
(skip hygiene) is NOT taken even though SQLite has no stored procedures and therefore none of the
§7.4 function-body blind spot — that is the same optimization still deferred for MySQL (R2).

### `impl PoolBackend` — still absent, but the line is now drawn

Nine of twelve methods are real. `cancel_handle`, `query`, `query_stream` and `reclaim_stream` are
not, and two associated types have no implementation. **The trait lands at C3-3d**, when only the
STREAMING pair would be `Unsupported` — which is exactly the shape the MySQL backend shipped at
M1-S6, so it is a precedent rather than an excuse.

### Process note

A test here was written as "a lost handle reports `Failed`" and asserted `Idle`, because a failing
statement does not lose the handle — only a panicking blocking task does. It was renamed to what it
actually verifies, and the real contract moved to a unit test where the private blocking bridge is
reachable. A test whose name and body disagree is worse than no test: the name is what a later
reader greps for.

---

## 13. C3-3c: `query` and the storage-class mapping (2026-09-15) — DONE

**The mapping keys on the VALUE's storage class, not the column's declared type.** Measured first:

```text
DECL: i:INTEGER | r:REAL | tx:TEXT | b:BLOB | n:NUMERIC | none_col:<NONE>
ROW1: INT       | REAL   | TEXT    | BLOB   | INT       | INT
ROW2: TEXT      | TEXT   | TEXT    | TEXT   | TEXT      | TEXT
```

Two facts rule out a per-column mapping between them: **a single column's storage class changes
between rows**, and **every expression column reports no declared type** (`1+1`, `'lit'`,
`count(*)` all `<NONE>`) — a large, ordinary class of queries for which the declared type carries
nothing at all.

### The objection, and why it dissolves

Per-value tagging would make the `cols` header a lie for a heterogeneous column — except
**`ColMeta.tag` has no consumer**. The PHP client drops it deliberately in BOTH paths and says so
(`Connection.php`, F25/hazard 47): *"The decode authority is the PER-CELL tag … not the column
metadata."* Nothing in `ferrod` reads it. Every cell is self-describing on the wire, so per-cell
tagging is what the client already relies on rather than a compromise.

`ColMeta.tag` is filled from the first row's storage class (NULL for an empty result) — describing
the data returned rather than a declaration SQLite does not enforce — and documented as advisory.

*This corrects the framing this slice was handed*, which posed the header as the central cost. It
is not, and checking for a consumer before designing around it is the same discipline that stopped
C3-3 building a seam nothing would read.

### Nine of fourteen §9 tags are unreachable BY CONSTRUCTION

| tag | why |
| --- | --- |
| `BOOL` | No boolean type; `0`/`1` read back `I64`. A column declared `BOOLEAN` is not enforced. |
| `U64` | INTEGER is SIGNED 64-bit. |
| `DECIMAL` | NUMERIC affinity stores INTEGER or REAL — `42` arrives as INTEGER. |
| `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ` | No date-time storage class; dates are TEXT/INTEGER/REAL by convention. |
| `UUID` | No UUID type. |
| `JSON` | The JSON functions operate on TEXT. |

Named rather than filled with an invented mapping, per S7's `UUID` precedent. **Drop-in
consequence for C3-6:** a SQLite column holding an ISO date reads back as `TEXT`, not a §9 `Date` —
what PDO's SQLite driver also does, but a real asymmetry against the other two backends.

The BIND direction is deliberately not symmetric: all fourteen bind, with the canonical-text tags
landing in TEXT. `U64` above `i64::MAX` is refused pre-send rather than wrapped (§9.1).

### `last_insert_rowid()` is sticky too

Exactly like `changes()` — after a SELECT it still reports the previous INSERT's rowid. So it is
reported only when the statement actually MOVED it; carrying it over would be a silently wrong key,
which §22.2 (m) already records (from PG's `lastval()`) as strictly worse than no key. Residual: an
INSERT explicitly reusing the previous rowid reports `None`, erring safe. Mutation-proven, as is
the `U64` refusal.

### Process note

The `column_decltype` feature was enabled to run the probe and **removed again once the decision
went the other way** — an unused feature flag is dependency surface with no caller. Also: the
mutation round briefly corrupted `rowmap.rs` because it was still UNTRACKED, so `git checkout`
could not restore it. **`git add` new files before mutation testing**, or the safety net is not there.

---

## 14. C3-3d: fate table, cancel handle, `impl PoolBackend` (2026-09-15) — DONE

**The trait landed on the line C3-3b drew** — when only the streaming pair would be `Unsupported`,
the shape MySQL shipped at M1-S6. `SqliteRowStream` wraps `Infallible`, so `query_stream`'s
impossibility is enforced by the type system rather than a runtime `unimplemented!()` nobody has run.

### `SQLITE_BUSY` (5) vs `SQLITE_BUSY_SNAPSHOT` (517)

Both map to `SerializationFailure`/Retryable, distinguished on the wire by the extended code in
`errno`. **The 517 reasoning did not start there.** The obvious argument is NonRetryable: `p4a`
proved `busy_timeout` never retries it, and re-sending the statement in the same open transaction
fails forever because the snapshot does not advance.

But that is exactly PG's `40001`, which this engine already calls Retryable — it also cannot be
fixed by re-sending, and also needs the caller to replay the transaction. `Branch`'s own doc settles
it: *"Retryable only licenses the CALLER to retry per its own policy."* Calling 517 NonRetryable
would say something different about SQLite than the engine says about PG for the same semantic.

Under D13 a 517 is unreachable for a correctly-declared transaction, so reaching it means a
declared-`readonly` transaction wrote — and the message says so, because that is a client bug.

### The finding worth more than the slice: a runaway `spawn_blocking` cannot be timed out

A mutation that never fires the cancel was expected to hit the test's 10-second timeout and fail.
**It hung and had to be killed.** `tokio::time::timeout` drops the outer future, but the statement
runs on a blocking thread nothing in the async world can reclaim, and tokio waits for blocking tasks
at shutdown.

**So the interrupt handle is not a convenience on this backend — it is the only thing that can stop
a statement.** A per-request `timeout_ms` that merely abandons the future would leak a thread per
runaway query. `ferrod` must fire the cancel. Recorded in §22.2 (bg) rather than left in a test
comment, because it constrains how the daemon may use timeouts here.

The test's own comment was corrected too: it claimed the timeout made a missed cancel fail rather
than hang, and that claim was false.

### `cancel_handle` bounds nothing, deliberately

PG and MySQL bound theirs at 2 s because both open a SIDE CONNECTION to deliver the cancel. SQLite's
`sqlite3_interrupt()` is an in-process flag set on a handle already owned — nothing opened, nothing
awaited. Copying the constant would bound nothing and imply a hazard that does not exist.

### Constants are asserted, not computed

Every extended code (`1555`, `2067`, `1299`, `275`, `787`, `517`, `8`, `9`) is proven against an
error a real SQLite produced. A companion test demonstrates the trap: `ErrorCode::DatabaseBusy as
i32` is **3**, while `SQLITE_BUSY` is **5**.

## 15. C3-3e: the third `AnyPool` arm (2026-09-15) — DONE

The SQLite pool is reachable from `ferrod`. `sqlite://` infers `PoolKind::Sqlite`, the registry
builds `AnyPool::Sqlite(Pool<SqliteBackend>)`, and the SQL/TX services dispatch to the same generic
bodies they already had. SPEC §22.2 (bh).

### The seam held, which is the point of the slice

Nothing about the registry's shape changed to take a backend that is a LIBRARY rather than a wire
protocol. `run_exec_on_pool` and `begin_on_pool` were already generic over `B: PoolBackend`; the arm
is three `match` lines and a constructor. That is the check `PoolBackend` had to pass to have been
worth defining, and both previous backends were wire protocols, so it had never been tested against
anything else.

### `sqlite://` is required, and the narrowing is deliberate

`resolve_path` also accepts a bare filesystem path, but a bare path carries no scheme and so lands in
`infer_pool_kind`'s warn-and-default arm. Widening to "looks like a path" is the repair NOT taken: it
is inference rather than declaration, and it would swallow precisely the typo'd DSNs that arm exists
to surface. Asserted as its own case, so the narrowing is a decision on the record rather than an
omission.

### The two debts, discharged

**C3-3a's wiring debt.** The pool's `checkout_timeout` is now the connection's `busy_timeout`. The
two fail in opposite directions if they disagree — a longer busy wait parks a tenant past the
deadline meant to bound it, a shorter one gives up on contention the pool would still wait out.

The registry-level equality assertion is **vacuous today and says so**: `DEFAULT_BUSY_TIMEOUT` and
`DEFAULT_POOL_CHECKOUT_TIMEOUT` are both 5 s, so it would pass whether or not the wiring existed. It
pins intent and breaks the moment either constant moves. The proof is behavioural — 250 ms against
the 5 s default, a twentyfold gap — and mutation-proven: dropping `.with_busy_timeout(...)` parks for
5.01 s and fails the ceiling. **Follow-up, recorded not smuggled:** closing the gap outright needs a
per-pool `checkout_timeout` knob, which `daemon_pool_config` does not have.

**C3-2's lockstep debt.** See below — it is the more interesting half.

### A premise from M1-S6, measured false

`VERSION_SQL`'s comment read *"so no per-backend method is needed"*. SQLite has no `version()`:
`SELECT version()` answers `no such function` (extended code 1). The probe is now per-family, in a
SEPARATE tuning field rather than a branch, so a test can steer either statement — a single field
would have left the SQLite arm permanently un-steerable, which is how the arm nobody can exercise
becomes the arm nobody notices is wrong.

Without it every probe on a SQLite pool fails and `HELLO_ACK` advertises a nil `server_version`.
Safe — both driver tiers refuse a nil version loudly by naming the pool (D-S8b-1) — but it would have
surfaced at C3-6 as a mystery. The test asserts the real version and re-runs the identical registry
with `SELECT version()` substituted, so the claim is measured, not argued.

### The lockstep test was green under the mutation it existed to catch

This is the slice's lesson, and it is lesson (12) landing on a test written *specifically* to be a
mechanism proof.

The first version ran the composed BEGIN, then issued a write inside the transaction, then asked a
raw side connection whether the writer lock was held. Respelling the undeclared arm to a bare
`BEGIN` — the exact string C3-2 replaced — left it **passing**.

Why: a write inside a DEFERRED transaction UPGRADES it to a writer, and succeeds when nothing else
is contending at that instant. After any statement has run, both spellings hold the writer lock. The
observation could not distinguish them.

**D13's claim is not "the transaction ends up holding the lock" — it is that the lock is taken AT
BEGIN**, so no upgrade is ever needed and `SQLITE_BUSY_SNAPSHOT` is unreachable by construction. That
is visible only in the window between BEGIN and the first statement. The side connection now looks
there, the write moved to after the observation (where it also shows the writer never upgrades), and
both arms are mutation-proven:

| mutation | arm that fails |
|---|---|
| undeclared → `"BEGIN"` | `engines_undeclared_begin_holds_the_writer_lock` |
| readonly → `"BEGIN IMMEDIATE"` | `engines_declared_readonly_begin_leaves_the_writer_lock_free` |

The test names no BEGIN string: dialect from the pool, string from the composer, run through the same
`Checkout::begin_tx_with` the TX service uses. A respelling changes what is emitted and the
observation changes with it.

### Found by a failing test: WAL cannot be switched under a lock

`PRAGMA journal_mode=WAL` cannot change a database's journal mode while another connection holds a
lock on it. When the side connection created the file as a rollback-journal database and took the
writer lock before the pool had dialled, the backend's WAL verification failed and the checkout
returned `ConnectionLost` rather than parking.

Not a hazard in the ordinary case — an already-WAL database does not hit it, and a pool dials at
startup with contention arriving later — but a SQLite pool's FIRST connection should not be raced
against an external writer, and it is why the timeout test warms the pool before contending.

### The adversarial pass earned its keep: a defect this arm made reachable

`PoolBackend::supports_row_streaming` defaults to **`true`**, and `SqliteBackend` was inheriting it
while its `query_stream` is `Unsupported` until C3-5.

`ferrod` reads that single method as the streaming-capability authority (M1-S8a) so a `fetch:stream`
against a backend that cannot stream is refused BEFORE any checkout, rather than surfacing as an
error part-way through a result set the client has already started consuming. Inheriting the default
would therefore have turned a clean refusal into a mid-stream one — live from the moment this slice
made a SQLite pool constructible, and unreachable before only because nothing could build one.

Overridden to `false`, with the capability/`query_stream` pairing asserted and mutation-proven
(deleting the override fails the test). Both halves flip together at C3-5, and the test says so.

## 16. C3-4: the `readonly` / `Pool::checkout` seam (2026-09-15) — DONE

`Pool::checkout_declared(readonly)` → `PoolBackend::apply_readonly` → SQLite's `PRAGMA query_only`.
SPEC §22.2 (bi). `set_query_only`, written at C3-3a with no caller, finally has one.

### The item was verified before it was built, and it had changed

C3-3 planning moved this slice *after* the backend because `readonly` genuinely did not reach
`ferro-pool` but **no backend would have read it**. Re-checked at the top of this slice: still two
occurrences in `ferro-pool`, both comments. What changed is the consumer — the SQLite backend now
exists, and `query_only` is the thing that reads it.

### Three call sites, not two

`ferrod` reaches `checkout()` from the buffered autocommit exec, the STREAM path, and the tx BEGIN.
A plan saying "the exec path and the begin path" would have wired two and left streamed
declared-readonly requests unenforced — the same shape as M1-S8b's `setTransactionIsolation` hole,
where the plan said two entry points and the third was found by measurement.

### Both checkout exits, and the ordering

`checkout_declared` returns from a fresh dial and from a recycled connection. The recycled arm must
apply **after** the hygiene reset, which would otherwise undo the arming. Enforcement on one exit
only would depend on pool occupancy: green wherever a test warms the pool, absent for exactly the
requests a cold or saturated pool serves with a new connection.

| mutation | test that fails | tests that stay green |
|---|---|---|
| delete the fresh-dial arm | `a_freshly_dialled_connection_gets_the_declaration` | the other three |
| delete the recycled arm | `a_recycled_connection_is_re_declared_each_checkout` (step 2) | the other three |
| move the arm above the cleanup block | `a_recycled_connection_is_re_declared_each_checkout` (step 2) | the other three |
| delete `query_only=OFF` from `reset` | `a_user_issued_query_only_pragma_does_not_leak_to_the_next_tenant` | the other three |

### Two of this slice's own tests were proven wrong, and that is the slice's value

**One claimed to exercise the fresh-dial exit through `ferrod`.** It does not, and cannot: the HELLO
handshake calls `PoolRegistry::pool_info`, whose version probe checks a connection out and returns
it, so every EXEC in that directory is served by a recycled connection. Measured, not reasoned — the
mutation left it green. The proof moved to pool level, where the test decides which connection serves
a checkout.

**One claimed the hygiene reset is what disarms between tenants.** Deleting `PRAGMA query_only=OFF`
from `reset` left it green. The real mechanism is that every checkout re-declares:
`checkout_declared(false)` disarms as surely as `true` arms, so in the ordinary path the seam
prevents the leak and hygiene never gets a turn.

That does **not** make the reset line dead, and finding out why is the useful part.
`apply_readonly` short-circuits on its own tracked flag, so a tenant that arms the pragma ITSELF — by
running `PRAGMA query_only=ON` as a statement, declaring nothing — leaves the flag reading `false`,
and the next checkout's `apply_readonly(conn, false)` does nothing at all. The connection reaches the
next tenant read-only with no declaration anywhere explaining why their writes fail. That is the §7.4
blind-spot shape, it is what the unconditional reset covers, and it now has the test it was wrongly
believed to already have.

### Recorded, not built: the PostgreSQL arm

The default `apply_readonly` is a no-op, correct today because neither wire backend has a
connection-scoped read-only mode outside a transaction. But PostgreSQL's session-scoped
`default_transaction_read_only`, armed per checkout and cleared by the existing hygiene `RESET ALL`,
would enforce the same declaration there. Not this slice — C3-4 is the seam plus the backend that
already needed it — but the seam is what turns it into a small slice, and it is the first thing since
§22.2 (ac) that would make an honest `readonly` declaration cost something on PostgreSQL.
