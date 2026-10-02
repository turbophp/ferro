# C5b: upstream doctrine/dbal 3.10.6's functional subset through the DBAL 3 bridge — recorded results

**Date:** 2026-10-02 · **Slice:** C5b (ledger Phase C, item C5) · **Runner:** `testkit/dbal-suite.sh`
with `FERRO_DBAL_TAG=3.10.6` — the runner derives the DBAL **major** from the pinned tag and selects
everything major-specific from it: the driver package's `vendor-dbal3/` tree (its DBAL 3 lane,
`composer.dbal3.json`), the replacement `testkit/dbal/TestUtil.ferro.dbal3.php` and
`driverClass = Ferro\DBAL\Dbal3\Driver`. Same 11-entry allowlist as the DBAL 4 runs, unchanged —
every entry exists verbatim in the 3.10.6 tree.

**What is being measured:** the C5a bridge (`Ferro\DBAL\Dbal3\Driver`, PR #67) against upstream's
OWN 3.x functional tests — the M1-S8b acceptance shape, one major down. Baselines: DBAL 4's SQLite
column in `docs/dbal-suite/2026-09-15-c3-6a-sqlite-results.md`, and its PG/MySQL/MariaDB columns in
`docs/dbal-suite/2026-09-09-a5-results.md`.

## Environment manifest

- **SQLite columns: this dev container**, which for this family is not a compromise (no server, no
  container, no host port). **PostgreSQL / MySQL / MariaDB columns: the on-demand `dbal-suite`
  workflow on GitHub runners** (`dbal: 3.10.6`), two runs per family, the testkit's digest-pinned
  images — the A5 protocol.
- **doctrine/dbal 3.10.6** — the runner asserts the clone tag and `vendor-dbal3`'s installed version
  are equal before running anything.
- **PHPUnit 11.5.56** (the driver package's), not the 9.6 the 3.10.6 tree pins: the runner uses ONE
  vendor tree on purpose (two autoloaders answer for two PHPUnit builds — the M1-S8b measurement). The
  tree runs under it unmodified; its 148 "PHPUnit Deprecations" are 3.10.6's doc-comment metadata
  (`@dataProvider`), which PHPUnit 11 still honours. Every data provider in the tree is static.
- **PHP 8.4.19**, pure-PHP msgpack packer.
- **SQLite libraries:** the Ferro column runs **3.53.2** (`rusqlite` `bundled`, inside `ferrod`), the
  control **3.45.1** (`pdo_sqlite`, the system library) — the same, stated confound as C3-6a.
- **Contact assertion: PASSED in all four SQLite runs.**
  `[ferro] driver=Ferro\DBAL\Dbal3\Driver platform=Doctrine\DBAL\Platforms\SqlitePlatform server=3.53.2`
  and `[control] driver=Doctrine\DBAL\Driver\PDO\SQLite\Driver platform=…SqlitePlatform server=3.45.1`.
  Reset lines present in all four.

## Results

"Executed" = tests − skipped − incomplete; "passed" = executed − errors − failures (the A5
convention, unchanged).

| column | result line (both runs, identical) | executed | passed |
|---|---|---|---|
| **SQLite 3.53.2 through Ferro (DBAL 3.10.6)** | `Tests: 842, Assertions: 621, Errors: 42, Failures: 1, Skipped: 575` | 267 | **224** |
| **CONTROL — SQLite 3.45.1 through `pdo_sqlite`** | `Tests: 842, Assertions: 799, Errors: 1, Failures: 4, Skipped: 575` | 267 | **262** |
| PostgreSQL 17 through Ferro | *pending — workflow run 36987841534* | | |
| MySQL 8.4 through Ferro | *pending — workflow run 36987841534* | | |
| MariaDB 11.8 through Ferro | *pending — workflow run 36987841534* | | |

**Two-run reproducibility: verified on both SQLite columns** — result line AND the full ordered
non-passing list compared between runs, identical.

## The SQLite column, triaged — 43 non-passes, none a driver defect

**One is upstream's own, and the control proves it:** `SqliteSchemaManagerTest::
testListForeignKeysFromExistingDatabase` fails identically through `pdo_sqlite` (`table user already
exists`). Verified in the two trees rather than inferred: the shared base class's reserved-keyword
introspection tests (`createReservedKeywordTables()`) create a `user` table and leave it, and 4.4.4's
copy of this test begins with `DROP TABLE IF EXISTS user` — a line 3.10.6's copy does not have.

The other 42 are Ferro-only, and fall into four groups:

### (A) 36 tests · a TEMP table does not survive to the next request (SPEC §7.4)

Every one fails with `no such table: __temp__<name>`: `SqlitePlatform::getAlterTableSQL()` rebuilds a
table through a `__temp__` copy in several statements outside a transaction, and on a
transaction-mode pool each statement is its own checkout. It is C3-6a's group (A) exactly — 24 there,
36 here because 3.10.6's tree carries more ALTER tests — with the same verified remedy (wrap the
ALTER in a transaction) and the same cross-family fact (PostgreSQL loses an autocommit TEMP table
identically). Recorded on the incompatibilities page since C6.

### (B) 4 tests · two statements in one `executeStatement()`

`Multiple statements provided` — one statement per request is a uniform Ferro contract, refused
identically on PostgreSQL (`42601`). C3-6a's group (B), unchanged.

### (C) 1 test · the driver's version-less platform refusal, as designed

`SchemaManagerFunctionalTestCase::testDispatchEventWhenDatabasePlatformIsExplicitlyPassed` calls
`$connection->getDriver()->getDatabasePlatform()` with no version. Before a connection exists the
driver does not know even the backend family, so `Ferro\DBAL\Dbal3\Driver` refuses rather than guess
a SQL dialect (SPEC §22.2 (by), review F11 — the same call DoctrineBundle makes, whose documented fix
is `charset`). DBAL 4.4.4's tree has no such test.

### (D) 1 test · a database FILE at a client-chosen path

`SqliteSchemaManagerTest::testCreateAndDropDatabase` asserts that `createDatabase($path)` creates a
file at a path the TEST chooses. Under Ferro the database is the engine pool's (SPEC §12 / D8, and
D14 confines any file the engine opens), so a client cannot create one by naming it: structurally
unsatisfiable, category (c) in the S8b triage. DBAL 4.4.4's tree has no such test.

### And four the CONTROL fails that Ferro passes

`ExceptionTest::testForeignKeyConstraintViolationExceptionOn{Insert,Update,Delete,Truncate}` fail
through `pdo_sqlite` because its connection does not enforce foreign keys (this runner, like the
DBAL 4 one, does not install upstream's `EnableForeignKeys` middleware); Ferro's SQLite pool sets and
verifies `foreign_keys = ON` itself (SPEC §22.2 (bl)), so the violations are raised as upstream
expects.

## Not established

- The server families' numbers — pending the workflow run above; this file is updated in the same
  slice when they land.
- Doctrine ORM 2 has not been run through the bridge.
