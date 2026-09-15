# C3-6b — the Illuminate suite's SQLite column (2026-09-15)

`laravel/framework` **v11.51.0**, `tests/Integration/Database/` driver-agnostic tree
(`testkit/laravel/allowlist.txt`, 633 cases), run through `testkit/laravel-suite.sh`.

Three columns, one database file, one allowlist. The control is upstream's own `pdo_sqlite` opening
the **same file** with **no ferrod running at all**, and the bootstrap's contact assertion INVERTS
for it — it refuses to run if the connection turns out to be a Ferro one.

| column | `FERRO_LARAVEL_DRIVER` | result line |
| --- | --- | --- |
| **Ferro, own driver name** | `ferro-sqlite` | `Tests: 633, Assertions: 2062, Errors: 3, Failures: 1, Skipped: 54, Risky: 1` → **575 / 579 executed** |
| **Ferro, stock driver name (alias)** | `sqlite` | `Tests: 633, Assertions: 1991, Errors: 45, Failures: 1, Skipped: 38, Risky: 1` → **549 / 595 executed** |
| **CONTROL — upstream `pdo_sqlite`** | `stock-sqlite` | `Tests: 633, Assertions: 2103, Errors: 2, Skipped: 45, Risky: 1` → **586 / 588 executed** |

SQLite versions differ between the columns and that is inherent, not a harness slip: Ferro's engine
carries **3.53.2** (rusqlite `bundled`), PHP's `pdo_sqlite` carries **3.45.1**. The same split exists
in the DBAL lane.

## Reproducibility, and the one thing that is not reproducible

Every column was run **twice**. In all three the **outcome counts and the ORDERED failure set are
identical** between runs.

The **assertion count drifts by one or two** in every column — `ferro-sqlite` 2062 / 2061,
`stock-sqlite` 2103 / 2102, the alias 1991 / 1989. **It drifts in the CONTROL too, which has no
Ferro in it at all**, so it is upstream's own — a case whose assertion count is data-dependent, not
an outcome difference and not something this tier can affect. Recorded rather than rounded away.

## PostgreSQL regression check

The alias-scoping change below touches the shared harness, so the PostgreSQL column was re-run
against this tree: `Tests: 633, Assertions: 2062, Errors: 6, Failures: 3, Skipped: 54, Risky: 1` —
**the same outcome counts as the recorded C2e column** (570 / 579). It ran against the authoring
container's PostgreSQL **16.13** rather than the testkit's 17.10, so it is a regression check and
not a re-record.

## What the runner guarantees

* **The reset is a file delete, and it runs BEFORE the daemon opens the file** — with the `-wal`/
  `-shm` sidecars, because every Ferro SQLite connection is in WAL mode and a stale WAL beside a
  deleted database restores the rows the reset was meant to remove. The file is then recreated
  EMPTY, because the control cannot open a missing one: Illuminate's `SQLiteConnector::connect()`
  does `realpath($database) ?: realpath(base_path($database))` and throws when both fail.
* **The SQLite path is derived in the RUNNER, with the engine's own rule** (`strip_prefix
  ("sqlite://")`). Not in PHP: `parse_url('sqlite:///abs/path')` returns **`false`** outright
  (measured), so a harness that reached for the obvious URL parser would refuse every valid DSN the
  engine accepts.
* **The control starts no ferrod.** It does not merely avoid Ferro — it has no Ferro to reach, so a
  mis-set variable cannot quietly route through one.
* **The contact assertion cannot be satisfied by the trap it exists for.** On PostgreSQL "something
  answered and it said PostgreSQL" is enough; on SQLite the testbench fallback IS SQLite, so the
  probe additionally reads `PRAGMA database_list` and requires the `main` database to be open on the
  expected FILE. Mutation-proven three ways: pointed at a decoy path it names both paths and
  refuses; pointed at `:memory:` it refuses with "that is the testbench SQLite-fallback trap this
  assertion exists for"; and with `ferro-sqlite` resolving the stock connection class it refuses on
  the class check. `sqlite_version()` replaces `version()`, which SQLite does not have at all.

## Triage — `ferro-sqlite`, all four non-passes

**None is a driver defect, and the control is what makes that checkable rather than assertable.**

| case | cause |
| --- | --- |
| `EloquentModelRefreshTest::testItRefreshesModelExcludedByGlobalScope` (failure) | **Reproduces in the CONTROL.** Upstream's own on this framework/SQLite pair. |
| `SchemaBuilderTest::testCompoundPrimaryWithAutoIncrement` | `if ($this->driver === 'sqlite') markTestSkipped(…)` — the skip never fires under a different driver NAME, so the case runs against a limitation SQLite genuinely has. **It SKIPS in the alias column**, which is the proof rather than the argument. |
| `SchemaBuilderTest::testAddingAutoIncrementColumn` | Same, same proof. |
| `SchemaBuilderTest::testAlteringTableWithForeignKeyConstraintsEnabled` | **The one real incompatibility.** See below. |

## The one real incompatibility: `PRAGMA foreign_keys` across a column change

`SQLiteGrammar::compileAlter()` emits six statements for a column change and `Blueprint::build()`
runs them as six separate `$connection->statement()` calls — six checkouts on a transaction-mode
pool:

```
PRAGMA foreign_keys = OFF
create table "__temp__parents" (…)
insert into "__temp__parents" (…) select … from "parents"
drop table "parents"                 <-- FOREIGN KEY constraint failed (errno 787)
alter table "__temp__parents" rename to "parents"
PRAGMA foreign_keys = ON
```

`PRAGMA foreign_keys` is CONNECTION-scoped, so the `OFF` never reaches the `drop`, and SQLite's
`DROP TABLE` performs an implicit `DELETE FROM` that the child row's foreign key refuses. This is
the §7.4 class exactly — the same shape as C3-6a's `__temp__` finding in Doctrine, arriving through
a different mechanism (a pragma rather than a temp table).

**Three remedies were RULED OUT by measurement, not reasoning** (probe: the six statements above
against a real pool, with the child row present):

1. **As Illuminate runs it** — refused, `errno 787`. Reproduces the suite failure exactly.
2. **All six inside ONE transaction** — still refused. `PRAGMA foreign_keys` is a no-op inside a
   transaction, so pinning the statements to one connection does not help. This is what makes the
   case different from C3-6a's `__temp__` remedy and from `dropAllTables()`, where a transaction IS
   the whole fix.
3. **Transaction + `PRAGMA defer_foreign_keys = ON`** (which IS transaction-legal) — still refused.
   `DROP TABLE`'s implicit delete is not covered by deferral.

So there is no execution-layer remedy, and inventing one would mean the tier substituting SQL the
stock grammar did not emit — charter rule 6. Recorded as a documented incompatibility for the C6
doc, with the mechanism named and the obvious fixes eliminated. A Ferro application that needs a
SQLite column CHANGE on a table with an inbound foreign key has to drop and recreate the constraint
itself.

## Why the alias column is worse here than on PostgreSQL, and what that means

On PostgreSQL the `pgsql` alias was the headline: identical to `pdo_pgsql`, 605/607 on both. On
SQLite the same alias costs **42 extra errors**, and the reason is structural rather than a defect.

`FerroConnections::register()` documents that an alias hijacks EVERY connection whose `driver` is
that name. Upstream's own tests use throwaway **SQLite** connections as a convenience regardless of
the family under test — `DatabaseConnectionsTest` builds them dynamically, the
`EloquentTransactionWithAfterCommit*` family uses a second connection, the migrate/refresh command
tests use one — and all of those carry no `ferro_socket`. Under the PostgreSQL columns nothing
hijacked the `sqlite` name, so they passed there and still do.

**Consequence for the product, stated plainly:** on SQLite the stock-name alias is not a drop-in
escape hatch the way it is on PostgreSQL, and `ferro-sqlite` is the column that matters. The cost of
that is the two name-gated skips above.

One related change came out of this: the harness now registers the alias **only in the column that
actually runs under the stock name**. Registering it while measuring `ferro-sqlite` hijacked
testbench's stock `sqlite` connection for 40 errors that had nothing to do with the configuration
under test. The PostgreSQL column was re-run afterwards as a regression check.

## Control-only non-passes

Two cases fail under `pdo_sqlite` and not under Ferro — recorded for completeness, not claimed as a
Ferro advantage:

* `SchemaBuilderTest::testSetJournalModeOnSqlite`
* `TimestampTypeTest::testChangeDatetimeColumnToTimestampColumn`

One runner artifact worth naming rather than cleaning up quietly: the columns that use a **stock**
`SQLiteBuilder` leave an empty file named `laravel_tests` in the working directory, because
`refreshDatabaseFile()` writes to whatever `getDatabaseName()` returns and some of upstream's tests
reconfigure that to a plain name. It is the same hazard `FerroSQLiteBuilder` refuses, observed from
the outside — the `ferro-sqlite` column never produces it.

## What the measurement demanded, in the order it demanded it

The tier was built to the minimum that lets a connection open, and then the failures named the rest
— the C3-6a discipline, and it earned its keep twice:

1. **`getPdo()->lastInsertId()`** — 11 of 11 cases in the first smoke run died on the shim's
   `__call` refusal. `PostgresProcessor` uses `insert … returning id` and never touches it; SQLite
   goes through the BASE `Processor::processInsertGetId`, which calls PDO directly.
2. **`dropAllTables()`** — `table "users" already exists` on the second case, exactly as predicted:
   `refreshDatabaseFile()` truncates a file named after the config LABEL. The mutation round then
   produced the defect as a physical artifact: with the stock builder restored, the live tests left
   two empty files named `laravel_tests` and `ferro_sqlite_label` in the REPOSITORY ROOT — the
   config labels of the two connections involved — while every table survived.
3. **`lastInsertId()` stickiness** — 182 of the first full run's 225 errors. The client's value is
   per-STATEMENT and cleared on the way in to every request (right for the engine, since a pooled
   statement can land on another backend connection); PDO's belongs to the HANDLE.
   `Connection::insert()` fires `QueryExecuted` before Illuminate reads the id, and any listener that
   runs a query clears it. Fixed in the PDO shim, which is where PDO semantics belong — the engine,
   the client and the Doctrine tier are untouched.
4. **The alias hijack** — 40 errors, described above.

## Not established

* §15's bar names **MySQL**, which this tier still does not register at all.
* The **ORM** suite is not run, on any family.
* `php/laravel`'s own dev dependencies could not be installed in the authoring container (no GitHub
  authentication for `composer install`), so its unit, live and PHPStan gates were run through the
  HARNESS vendor tree and `php/client`'s PHPStan binary rather than this package's pinned ones. That
  is a weaker gate than CI's and is stated as such — though it still earned its place: PHPStan L9
  caught four `method.notFound` calls and one statically dead guard in the new schema builder before
  the push.
