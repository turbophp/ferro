# C3-6a: the DBAL subset's SQLite column, with its control — recorded results

**Date:** 2026-09-15 · **Slice:** C3-6a (ledger Phase C, item C3-6) · **Runner:**
`testkit/dbal-suite.sh` with the new `FERRO_DBAL_SVC=sqlite` family, twice per column, plus the new
`FERRO_DBAL_CONTROL=1` control column, twice.

**What is being measured:** the `claude/dev-loop/20260915-1700-c3-6a-dbal-sqlite-column` tree — the
first one on which `Ferro\DBAL\Driver` knows the `sqlite` family at all. Baseline for the other
three families: `docs/dbal-suite/2026-09-09-a5-results.md` (M1-S9 A5). There is no SQLite baseline;
this is the first.

## Environment manifest

- **This dev container, not a GitHub runner** — and for this family that is not a compromise.
  SQLite needs no server, so the SQLite column has no container, no host port and no
  `docker compose` step; the workflow skips both for it. CI can reproduce it, but CI is not the
  authority for it (the same property C3-1's spike had).
- **doctrine/dbal 4.4.4** — clone tag and vendor version asserted equal by the runner, as always.
- **PHP 8.4.19**, pure-PHP msgpack packer (`ext-msgpack` is not loaded in this container).
- **Two different SQLite libraries, and the difference is the point of the control.** The Ferro
  column runs SQLite **3.53.2** (`rusqlite`'s `bundled` build, statically linked into `ferrod`);
  the control runs **3.45.1** (PHP's `pdo_sqlite`, linked against the system library). They are
  therefore not byte-identical engines — stated here rather than discovered later, because it is
  the one confound this control carries. It did not produce any of the differences below: all 28
  are explained by Ferro's request model, and each was reproduced directly.
- **Contact assertion: PASSED in all four runs.** The two Ferro runs print
  `[ferro] driver=Ferro\DBAL\Driver platform=Doctrine\DBAL\Platforms\SQLitePlatform server=3.53.2`;
  the two control runs print `[control] driver=Doctrine\DBAL\Driver\PDO\SQLite\Driver … server=3.45.1`.
  Reset lines present in all four.
- **The CI lane's exact invocation was exercised too.** The `dbal-suite` workflow passes
  `FERRO_DBAL_DSN=""` for both SQLite columns and lets the runner choose the file path (the script
  reads it with `${FERRO_DBAL_DSN:-…}`, whose `:-` treats empty as absent). A third Ferro run made
  that way produced the identical result line and the identical ordered failure set — so the
  recorded numbers are reproducible through the path CI will take, not only the one used here.
- **Reset:** the database FILE is deleted, with its `-wal` and `-shm` sidecars, BEFORE `ferrod`
  opens it. That is strictly more thorough than either SQL reset — no schema, sequence, view or
  leftover row can survive it.

## Results

“Executed” = tests − skipped − incomplete; “passed” = executed − errors − failures (the A5
convention, unchanged).

| column | result line (both runs, identical) | executed | passed |
|---|---|---|---|
| **SQLite 3.53.2 through Ferro** | `Tests: 729, Assertions: 662, Errors: 28, Skipped: 361, Incomplete: 11` | 357 | **329** |
| **CONTROL — SQLite 3.45.1 through `pdo_sqlite`** | `Tests: 729, Assertions: 742, Skipped: 361, Incomplete: 11` | 357 | **357** |

**Two-run reproducibility: verified on both columns.** Result line AND the full ordered non-passing
list extracted from both runs and compared — identical (28 entries per Ferro run, 0 per control
run).

**The control is clean, so every one of the 28 is Ferro-attributable.** That is the useful shape: it
removes "the suite does not like SQLite" and "3.53 vs 3.45" as explanations, and leaves two causes.

## The 28, triaged — two causes, neither a driver defect

### (A) 24 tests · a TEMP table does not survive to the next request (SPEC §7.4)

Every one fails with `no such table: __temp__<name>`. `SQLitePlatform::getAlterTableSQL()` rebuilds
a table through a five-statement sequence that begins
`CREATE TEMPORARY TABLE __temp__x AS SELECT … FROM x`, and `AbstractSchemaManager::alterTable()`
runs those five as five SEPARATE `executeStatement()` calls **outside any transaction**. On a
transaction-mode pool each is an independent request, and the hygiene reset drops TEMP objects
between checkouts (C3-3b's explicit 4-item list) — so statement 2 is served a connection on which
statement 1's temp table does not exist.

**This is not a SQLite gap. It is the §7.4 contract, and it was measured on PostgreSQL too:**

| family | `CREATE TEMPORARY TABLE …` then `SELECT` — autocommit | the same, inside one transaction |
|---|---|---|
| PostgreSQL 16.13 | GONE (`relation "tt_tmp" does not exist`, `42P01`) | survived |
| SQLite 3.53.2 | GONE (`no such table: tt_tmp`, errno 1) | survived |

(The PostgreSQL side was measured against this container's local PostgreSQL **16.13**, not the
testkit's 17.10 — it is a statement about the request model, not about a server version.)

So the behaviour is uniform; SQLite is simply the first family whose STOCK Doctrine schema manager
depends on session-scoped state between statements. PostgreSQL's does not, which is why this cost
never appeared before.

**The remedy is verified, not proposed.** The identical five-statement sequence wrapped in
`$conn->transactional(…)` succeeds and leaves the rebuilt table with its rows — the transaction
pins the connection to the `tx_id` (§4/§7), so the temp table is still there for statements 2–5.
SQLite has transactional DDL, so the wrap costs nothing. Measured directly through the driver, both
arms in one script.

**Consequence to state plainly in the incompatibilities doc (C6):** a non-simple `ALTER TABLE`
through DBAL's SQLite schema manager — which is to say most schema diffing and much of
`doctrine/migrations` — must run inside a transaction when the connection is a Ferro one. Whether a
given migration runner already does that is its own configuration and is NOT asserted here.

### (B) 4 tests · several statements in one `executeStatement()`

`SQLiteSchemaManagerTest::testNoWhitespaceInForeignKeyReference` and its three siblings send a
heredoc containing **two** `CREATE TABLE` statements in a single call. Ferro prepares every
statement, so it refuses — and **PostgreSQL refuses identically**, measured through the same client
in the same script:

- PostgreSQL: `cannot insert multiple commands into a prepared statement` (`42601`)
- SQLite: `Multiple statements provided` (rusqlite, no SQLSTATE)

One statement per request is a uniform Ferro contract, not a SQLite shortfall. Category (c):
structurally unsatisfiable for this suite, no action.

## The two decisions C3-6 owed, each with the evidence

### 1. `foreign_keys` — ON, and now DECLARED rather than inherited

C3-3a left this open with the note *"SQLite defaults `foreign_keys` OFF while Laravel and Doctrine
both turn it ON"*. Measuring it through the driver produced a different fact: a Ferro SQLite
connection **already reported `PRAGMA foreign_keys = 1`** and already refused an orphan INSERT with
errno 787 — with no pragma anywhere in `ferro-backend-sqlite`. It comes from the BUILD:
`rusqlite`'s `bundled` feature compiles libsqlite3-sys with `SQLITE_DEFAULT_FOREIGN_KEYS=1`, while
`pdo_sqlite` against the system library reports `0`.

An engine-wide integrity guarantee resting on a dependency's compile flag is one `cargo update`
away from silently inverting, so `open_configured` now sets and READS BACK the pragma, refusing the
connection if the build cannot enforce foreign keys. The VALUE is ON, which changes nothing
observable today. The test that guards it asserts the raw bundled default too, so if that flag ever
moves the measurement fails rather than the guarantee.

**And the half that is a genuine finding: neither tier's own mechanism can work here.** Doctrine
ships an opt-in `AbstractSQLiteDriver\Middleware\EnableForeignKeys` that runs `PRAGMA
foreign_keys=ON` at driver-connect; Laravel's SQLite connector sets the pragma itself. Both apply a
SESSION setting to whichever pooled connection happens to serve that one call, and the next request
gets another — the same §7.4 shape as (A). A per-connection setting that must hold for every tenant
belongs at dial, which is where it now is, and which is the same place and reasoning as the MySQL
family's `time_zone = '+00:00'` (S7). Measured: with the middleware registered and without it, the
pragma reads `1` and the orphan INSERT is refused in both — i.e. the middleware is a no-op here,
and the guarantee comes from the pool.

The DBAL suite does not discriminate: the control passes 357/357 with foreign keys OFF, so nothing
in the subset requires enforcement either way.

### 2. Dates read back as `TEXT` — no action, and the suite says why

C3-3c recorded that nine of the fourteen §9 tags are unreachable on SQLite by construction, so an
ISO date in a SQLite column arrives as `TEXT` rather than a §9 `Date`. Measured cost at the Doctrine
tier: **zero**. `Functional/TypeConversionTest` passes every temporal row on the SQLite column —
`datetime`, `datetimetz`, `date`, `time` — because DBAL's own types parse the platform's format
strings out of strings anyway, and `SQLitePlatform` declares them all as plain `Y-m-d H:i:s` /
`Y-m-d` / `H:i:s`. The control agrees. This is also what `pdo_sqlite` does, so it is not even an
asymmetry a user could observe through Doctrine.

Recorded, not closed: the asymmetry is real against PG/MySQL for a caller using `ferro/client`
directly, and C3-6b must re-ask the question for Illuminate, whose date handling is its own.

## What this column does NOT establish

- **§14's bar is still not met, and adding SQLite does not change that.** The bar is "green on PG +
  MySQL + SQLite"; this column is 329/357 with 28 triaged non-passes, and the ORM suite is still
  not run at all.
- **`php/doctrine-dbal`'s own live tier has no SQLite lane.** `LiveTestCase` spawns a `ferrod` with
  a mandatory PostgreSQL pool and an optional MySQL one; adding a third is a `php/client` harness
  change and was deliberately not done in this slice. The live evidence for the SQLite family is
  this column — which is stronger, but it does not run in `composer test`.
