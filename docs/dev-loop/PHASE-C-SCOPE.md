# Phase C (M2 — the Eloquent milestone): slice plan

**Status: SCOPE ONLY. No Phase C code has been written.** This document exists so C1 is sliced
before it is started, the way B2 was — that split (fork edit → reclaim seam → bound → parkable conn
→ stream → driver flip) is why six risky changes landed without a rollback, each proven in CI before
the next built on it.

Every claim below is marked **[verified]** (checked against the code this iteration) or
**[UNVERIFIED]** (a premise the owning slice must check FIRST). Phase B produced four items whose
stated premise did not survive contact; the marking is there so that cost is not paid again.

## The shape of the milestone

§15's mechanism is the same shape as §14's and the same charter rule 6 constraint applies: register
`ferro-{mysql,pgsql,…}` via `Illuminate\Database\Connection::resolverFor()`, subclass the matching
Illuminate connection class, and override **the execution layer only** — stock Grammar, stock
Processor, stock Schema builder.

**S8a/S8b is the precedent to copy, including its split.** S8b could not start until S8a had built
the seven engine/client carries it needed. The same question decides C1's first slice: *what does
the Illuminate tier need that `ferro/client` does not have yet?*

## Carries: what the Eloquent tier needs from below

| Need (§15) | Status | Notes |
|---|---|---|
| `select()`, `statement()`, `affectingStatement()`, `unprepared()` | **[verified]** present | `fetchRaw` / `fetch:none` + `affected`, all built for S8b. |
| `cursor()` → `LazyCollection` | **[verified]** present | `streamRaw()` streams on BOTH families since Phase B item B2; the pool-kind gate is gone. This was the largest C1 dependency and it is already paid. |
| `beginTransaction`/`commit`/`rollBack` + savepoints | **[verified]** present | Imperative trio (S8a) + savepoint SQL passthrough (§22.2 (r)). |
| `lastInsertId` | **[verified]** present | On the wire since S8a; on stream terminals since B2c. |
| `getAttribute(SERVER_VERSION)` for the PDO shim | **[verified]** present | `poolInfo()` carries `server_version` (S8a, `HELLO_ACK` v2). |
| **`selectResultSets()` — MULTIPLE result sets** | **GAP [verified], but NOT the binding constraint** | `ExecOk` carries exactly ONE `cols` + ONE `rows`, so several result sets have no wire representation; §15 lists the method without noting this. **The deeper blocker, also [verified]:** a MySQL `CALL` returns no usable rows today — a prepared `CALL` declares zero result columns even when the procedure emits a result set, so the streamed path discards the rows and the buffered path yields N cell-less rows. Fixing the wire without fixing that would ship a feature that still returns nothing. See C1a. |
| `DB::transaction($fn, attempts: 3)` retry mapping | **[UNVERIFIED]** | The fate branches exist (§19.3); what is unchecked is whether Illuminate's `ManagesTransactions` retry loop can be driven from them without reimplementing it. Check before slicing. |
| `read`/`write` split → a second pool | **[UNVERIFIED]** | §15 shows `'read' => ['pool' => 'main_ro']`. Whether `ferrod` exposes replica pools usably today is unchecked. |
| `FerroPdoShim` (`quote`, `lastInsertId`, `inTransaction`, `exec`, `getAttribute`) | **[UNVERIFIED]** | `quote()` is the one to think hardest about: it is a SQL-generation-adjacent API, and charter rule 6 forbids SQL rewriting. Decide what it may legitimately do before writing it. |

## Proposed slices

- **C1a — the `selectResultSets()` wire gap. DECIDED while scoping: documented incompatibility for
  M2, because the wire is not its real blocker.** The primary consumer of `selectResultSets()` is a
  stored procedure, and **a MySQL `CALL` cannot return usable rows today at all** — a prepared `CALL`
  reports ZERO result columns even when the procedure emits a result set at run time. Verified in
  code (`ferro-backend-mysql/src/stream.rs`), and the two paths round the same blind spot off
  differently: the STREAMED path takes the no-rows arm and discards the rows, while the BUFFERED path
  maps every row through the empty prepared-column list and yields **N rows with no cells**. So
  carrying multiple result sets on the wire would be building on sand: the feature would still return
  nothing usable on the backend that motivates it.
  **The dependency order is therefore CALL-blind-spot FIRST, multi-result-set SECOND**, and both are
  engine work, not tier work. Recorded so the tier is not blocked on a `/proto` slice that would not
  have helped. The blind-spot fix is its own investigation (likely: read column metadata from the
  EXECUTE response rather than the PREPARE response, or route `CALL` over the text protocol) and is
  not scheduled here.
- **C1b — package skeleton + service provider + one connection class, `select()` only.** The
  smallest thing that can execute a real query through a real Illuminate connection. Its exit gate
  is the S8b lesson: a HARD CONTACT ASSERTION (`getNativeConnection() instanceof …` + a round-tripped
  `SELECT 1`) before a single suite test runs. Upstream's `TestUtil` silently fell back to SQLite
  and reported a green 105-test run with zero Ferro contact; that must not be re-learned.
- **C1c — writes + transactions**, including the `attempts` mapping (after its premise check).
- **C1d — `cursor()`/`LazyCollection`**, which should be small given B2.
- **C1e — the PDO shim**, scoped by what C1b–C1d actually turn out to need, not by §15's list
  up front.
- **C2 — the `illuminate/database` suite**, modelled on `testkit/dbal-suite.sh`.

## The bar, stated honestly up front

§15's acceptance is "the `illuminate/database` integration test suite green on MySQL, PG, SQLite via
Ferro connections", plus a demo app. **SQLite has no backend (C3), so the SQLite column is
unreachable until C3 lands** — exactly the situation §14's bar was in, and the honest recording rule
from the S8b close applies: report the columns that ran, name the one that could not, and do not
restate the bar as if it were met. Whether C3 should therefore come BEFORE C2 is an open sequencing
question, not a settled one.
