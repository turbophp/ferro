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
| **`selectResultSets()` — MULTIPLE result sets** | **GAP [verified]** | `ExecOk` carries exactly ONE `cols` + ONE `rows`. There is no wire representation for a statement returning several result sets (a stored procedure's). §15 lists the method without noting this. **This is a `/proto` change** — registry + golden vectors + both codecs (charter rule 2) — and it is the one true C1 carry found so far. |
| `DB::transaction($fn, attempts: 3)` retry mapping | **[UNVERIFIED]** | The fate branches exist (§19.3); what is unchecked is whether Illuminate's `ManagesTransactions` retry loop can be driven from them without reimplementing it. Check before slicing. |
| `read`/`write` split → a second pool | **[UNVERIFIED]** | §15 shows `'read' => ['pool' => 'main_ro']`. Whether `ferrod` exposes replica pools usably today is unchecked. |
| `FerroPdoShim` (`quote`, `lastInsertId`, `inTransaction`, `exec`, `getAttribute`) | **[UNVERIFIED]** | `quote()` is the one to think hardest about: it is a SQL-generation-adjacent API, and charter rule 6 forbids SQL rewriting. Decide what it may legitimately do before writing it. |

## Proposed slices

- **C1a — the `selectResultSets()` wire gap.** Either carry multiple result sets on the wire, or
  decide it is a documented incompatibility. Doing it is a `/proto` slice; NOT doing it is a
  one-line entry in `docs/known-incompatibilities.md`. **Decide before building the tier**, because
  it is the only found carry and its answer changes whether C1b needs a new client method.
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
