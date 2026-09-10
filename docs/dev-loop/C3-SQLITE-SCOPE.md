# C3 — the SQLite backend: scope, and the one spec tension that has to be settled first

**Status:** SCOPING ONLY. No `ferro-backend-sqlite` code exists, and none is added here.
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
  must be rolled back and replayed, and charter rule 3 forbids the engine replaying it. *(UNVERIFIED
  against SQLite's current source; this is the documented behaviour and it is the crux of the
  option, so it must be reproduced before Option A is either chosen or dismissed.)*

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
- **`cancel_handle` / `Cancel`.** PostgreSQL and MySQL both cancel over a SIDE connection. SQLite's
  equivalent is `sqlite3_interrupt()`, which is callable from another thread on the same handle —
  *(UNVERIFIED against `rusqlite`'s API surface: whether an `InterruptHandle` is obtainable and
  `Send + 'static` as the trait's supertrait bound requires.)*
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
(**UNVERIFIED** — S1 and S6 both turned out to need one, and that assumption was proven false for
`mysql_async` before any code was written, so it must be checked, not assumed); no server to run in
CI, so every gate is reachable in a plain container; and the type matrix is small, since SQLite has
five storage classes.

## 6. Proposed slicing

- **C3-0** — settle §1 as a D-series decision and amend §7.6. *No code.*
- **C3-1** — a spike, not a slice: prove (a) `rusqlite` gives a `Send + 'static` interrupt handle,
  (b) a `spawn_blocking`-fed `BackendRows` streams without buffering, (c) no driver fork is needed.
  Each of the three is currently UNVERIFIED and each can invalidate the plan.
- **C3-2** — the crate and the `PoolBackend` impl minus streaming: connect/ping/`tx_status` via
  `sqlite3_get_autocommit`/`simple_query`/`query`/hygiene, plus the third `AnyPool` arm.
- **C3-3** — `query_stream`.
- **C3-4** — the acceptance columns: the DBAL suite's SQLite column (§14) and the Illuminate suite's
  (§15), each with the C2e control column alongside it.
- **C3-5** — the online backup admin surface (§7.6's last sentence).

## 7. What this document does NOT decide

The **sequencing** — whether C3 comes before the remaining M2 engine backlog (the MySQL `CALL`
blind spot, TCP keepalive, B6b chunked `LARGE_OBJECT`, B7's tracker-coverage proof). C3 is the only
one of them that unblocks an acceptance bar; the `CALL` blind spot is the only one that unblocks a
documented incompatibility (`selectResultSets()`). That is a call for the project owner, and it was
already flagged as open when the SQLite question was first asked.
