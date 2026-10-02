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

- **Every column: the on-demand `dbal-suite` workflow on GitHub runners**, two runs per family, the
  testkit's digest-pinned images — the A5 protocol. **Run 36989384844** (`dbal: 3.10.6`) and **run
  36989388348** (`dbal: 4.4.4`, the re-measurement below), both on commit `ab8458f`. The SQLite
  columns were first measured in this dev container and the runner reproduced them exactly (result
  lines and ordered non-passing lists).
- **doctrine/dbal 3.10.6** — the runner asserts the clone tag and `vendor-dbal3`'s installed version
  are equal before running anything.
- **PHPUnit 11.5.56** (the driver package's), not the 9.6 the 3.10.6 tree pins: the runner uses ONE
  vendor tree on purpose (two autoloaders answer for two PHPUnit builds — the M1-S8b measurement). The
  tree's SOURCE runs unmodified; its 148 "PHPUnit Deprecations" are 3.10.6's doc-comment metadata
  (`@dataProvider`), which PHPUnit 11 still honours, and every data provider in the tree is static.
  **One behaviour does change, and it costs two tests (C5b review F2): test-METHOD ORDER.** PHPUnit
  9.6 runs a class's own methods before the ones it inherits; PHPUnit 11 runs the inherited ones
  first. 3.10.6's tree relies on the old order in two places — upstream's own comment on
  `PostgreSQLSchemaManagerTest::testDropWithAutoincrement` says it *"needs to be executed before the
  ones it conflicts with, so it has to be declared in the same class"* — and both fail under the
  reversed order in the control as well as through Ferro (below). Each passes when run alone, and
  fails when run after the inherited test it conflicts with, through the stock drivers; measured.
- **PHP 8.4** from `setup-php` with `ext-msgpack` on the runners; the dev container's SQLite runs used
  PHP 8.4.19 with the pure-PHP packer, and the two produced identical results.
- **SQLite libraries:** the Ferro column runs **3.53.2** (`rusqlite` `bundled`, inside `ferrod`), the
  control **3.45.1** (`pdo_sqlite`, the system library) — the same, stated confound as C3-6a.
- **Backends:** PostgreSQL `17.10 (Debian 17.10-1.pgdg13+1)`, MySQL `8.4.11`, MariaDB
  `11.8.8-MariaDB-ubu2404`.
- **Contact assertion: PASSED in every run of every column**, and the platform it names is the
  right one for the backend: `Ferro\DBAL\Dbal3\Driver` with `PostgreSQL120Platform`,
  `MySQL84Platform`, `MariaDb110700Platform` and `SqlitePlatform`; the control's inverted line,
  `[control] driver=Doctrine\DBAL\Driver\PDO\SQLite\Driver … server=3.45.1`, after `[control] no
  ferrod started`. **Reset lines present in every run.**

## Results

"Executed" = tests − skipped − incomplete; "passed" = executed − errors − failures (the A5
convention, unchanged).

| column | result line (both runs, identical) | executed | passed |
|---|---|---|---|
| **SQLite 3.53.2 through Ferro (DBAL 3.10.6)** | `Tests: 842, Assertions: 621, Errors: 42, Failures: 1, Skipped: 575` | 267 | **224** |
| **CONTROL — SQLite 3.45.1 through `pdo_sqlite`** | `Tests: 842, Assertions: 799, Errors: 1, Failures: 4, Skipped: 575` | 267 | **262** |
| **PostgreSQL 17.10 through Ferro** | `Tests: 842, Assertions: 989 / 967, Errors: 5, Failures: 4, Skipped: 528` | 314 | **305** |
| **MySQL 8.4.11 through Ferro** | `Tests: 842, Assertions: 914, Errors: 2, Failures: 5, Skipped: 539` | 303 | **296** |
| **MariaDB 11.8.8 through Ferro** | `Tests: 842, Assertions: 923, Errors: 2, Failures: 5, Skipped: 535` | 307 | **300** |

**Two-run reproducibility: verified on all five columns** — result line AND the full ordered
non-passing list compared between runs, identical. The one difference is PostgreSQL's ASSERTION
count (989 in run 1, 967 in run 2), and it is upstream's: `testListTablesExcludesViews` asserts once
per table it lists, so the count follows whatever the reset leaves; a local `pdo_pgsql` control
drifts the same way between its own two runs (997 / 993).

These PostgreSQL numbers include the two fixes this slice made (below). Before them, the first
dispatch measured **313 / 304**: one test fewer executed, and that test was the finding.

## The SQLite column, triaged — 43 non-passes, none a driver defect

**One is the HARNESS's, and the control shares it:** `SqliteSchemaManagerTest::
testListForeignKeysFromExistingDatabase` fails identically through `pdo_sqlite` (`table user already
exists`), because it runs AFTER the inherited `testIntrospectReservedKeywordTableViaListTables`, which
creates a `user` table and leaves it. That is the PHPUnit 11 method-order reversal in the manifest:
alone it passes, after that test it fails, through `pdo_sqlite`. (4.4.4's copy of the test begins with
`DROP TABLE IF EXISTS user`, a line 3.10.6's lacks.) The control proves the failure is not Ferro's;
it does not make it upstream's.

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

### (C) 1 test · the driver's version-less platform refusal, a policy

`SchemaManagerFunctionalTestCase::testDispatchEventWhenDatabasePlatformIsExplicitlyPassed` calls
`$connection->getDriver()->getDatabasePlatform()` with no version, AFTER the shared connection has
connected — so here the driver does know the backend family, and the refusal is a POLICY rather than
a necessity (C5b review F3). The stock answer would be the family's OLDEST platform, which is not
the one the connection uses (that is chosen from the server version); `Ferro\DBAL\Dbal3\Driver`
will not hand a caller a second, older dialect for the same database, before a connect or after
(SPEC §22.2 (by) F11, (bz); the same call DoctrineBundle makes before connecting, whose documented
fix is `charset`). DBAL 4.4.4's tree has no such test.

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

## The server columns, triaged — no driver defect, and one real DBAL-3-only incompatibility

**PostgreSQL's 9.** One is the HARNESS's: `testDropWithAutoincrement` fails identically through a
`pdo_pgsql` control — the recorded run's text is `2BP01 cannot drop table view_test_table because
other objects depend on it` — because under PHPUnit 11 it runs after the inherited
`testCreateAndListViews`, which leaves a view on that table; upstream's own comment says it must run
first (see the manifest). Alone it passes, through `pdo_pgsql`. One is the version-less platform
refusal, as on SQLite: `testDispatchEventWhenDatabasePlatformIsExplicitlyPassed`. One is a real
incompatibility, below. The other six are S8b's categories, unchanged from DBAL 4:

- **(b) PostgreSQL reports no generated key**, and the driver will not emulate `lastInsertId()` with
  a follow-up query, which on a transaction-mode pool would run on another connection and return a
  wrong key (D-S8b-5): `WriteTest::testLastInsertId`, `testEmptyIdentityInsert`, and the DBAL-3-only
  `testLastInsertIdSequence` (the deprecated sequence-name form, `currval()` behind the scenes).
- **(c) No credentials in PHP** (SPEC §12 / D8): `ExceptionTest::testInvalidUserName`,
  `testInvalidPassword`, `testInvalidHost`.

**The real one — `PostgreSQLSchemaManagerTest::testGetSearchPath` (C5b review F1).** It expects
`['public']` and gets `['"$user"', 'public']`, and the first triage filed that under (c) as a test
artifact. It is a DBAL-3-only SCHEMA bug. DBAL 3's PostgreSQL schema manager decides the CURRENT
schema by substituting the connection's `user` PARAMETER for `"$user"` in `SHOW search_path` and
taking the first schema that exists; a Ferro connection has no `user` param, so the literal
`"$user"` matches nothing. When the role has a schema of its own name — PostgreSQL's documented
secure-schema-usage pattern — DBAL 3 then believes the current schema is the next one in the path,
names the role's tables schema-qualified and `public`'s unqualified, and the comparator plans a
CREATE of the unqualified name plus a DROP of the qualified one: a destructive migration. DBAL 4
asks the server (`SELECT current_schema()`) and is not affected. **Workaround:** set DBAL's `user`
parameter to the backend role — it is inert for Ferro's own connection, which takes no credentials —
or keep `"$user"` out of the pool's `search_path`. `SearchPathLiveTest` pins all three cells (DBAL 3
without `user`: wrong; with it: right; DBAL 4: right) against PostgreSQL's own `current_schema()`;
recorded on the incompatibilities page.

**MySQL's and MariaDB's 7 — the same 7, in the same order.** `testDispatchEventWhenDatabase
PlatformIsExplicitlyPassed` (the refusal); and category (c): `MySQLSchemaManagerTest::
testListDatabases` (it creates a database — the pool's least-privilege user cannot; DBAL 4's
`testIntrospectDatabaseNames` is the same test), `TransactionTest::testCommitFalse` (it sets a
session `wait_timeout`, which on a transaction-mode pool lands on a different checkout than the one
it means to expire — SPEC §7.4), the three `ExceptionTest` credential overrides, and
`PrimaryReadReplicaConnectionTest::testInheritCharsetFromPrimary` (the requested `charset` is inert;
the engine's is reported).

## The finding: a test that SKIPPED under Ferro and ran under `pdo_pgsql`

The first PostgreSQL triage above was clean, and it was incomplete. It classified every NON-PASS,
and a skip is not a non-pass. Diffing the Ferro column's SKIP set against a `pdo_pgsql` control's
found one test the control ran and Ferro skipped: `PostgreSQLSchemaManagerTest::
testListTableColumnsOidConflictWithNonTableObject`, which gates itself on

```php
if (version_compare($wrappedConnection->getServerVersion(), '12.0', '<')) {
    self::markTestSkipped('Manually setting the Oid is not supported in Postgres 11 and earlier');
}
```

The driver answered `getServerVersion()` with the backend's raw banner, `PostgreSQL 17.10 (…)`, and
PHP's `version_compare()` reads a string that begins with a word as OLDER than every version
(measured: `true` for that banner, `false` for `pdo_pgsql`'s `17.10 (…)`). The same gate exists in
4.4.4's tree, so **A5's recorded PostgreSQL column skipped it too** — that doc now carries a
correction. It is the defect C2c found and fixed in the Laravel tier (SPEC §22.2 (ao)), still live
here because this tier had normalised the version it CONSUMED (platform selection) and not the one
it ANSWERED.

Fixed in two steps, each exposing the next:

1. **`getServerVersion()` returns the normalised version** (SPEC §22.2 (bz)) — PostgreSQL's product
   name stripped, the MySQL family byte-identical. The test then RAN, and failed:
   `canonical I64 cannot bind to PG type oid`.
2. **An `I64` binds an `oid`**, value-gated to `0..=4294967295` (SPEC §22.2 (ca)). The test reads
   `pg_class.oid` — which Ferro has returned as an integer since M1-S8a — and binds it straight back;
   the read had no bind mirror. It now passes on both majors.

**After both fixes, measured locally against PostgreSQL 16 with a `pdo_pgsql` control on BOTH
majors, the Ferro column's skip set is IDENTICAL to the control's** — compared as SETS, not counts,
by `testkit/dbal/compare-columns.php` (3.10.6: 528 and 528, the same digest; 4.4.4: 358 and 358, the
same digest, counting the 2 incomplete tests JUnit encodes as skipped). The only failure the two
share is the harness's test-order artifact `testDropWithAutoincrement` (3.10.6 only), and every
Ferro-only failure is in the triage above. The reports, with the exact invocation that produced
each, are in `docs/dbal-suite/2026-10-02-c5b-skip-sets/`: the runner gained server-family controls
for this (`FERRO_DBAL_CONTROL=1` with the family's DSN — the stock driver needs credentials, which a
HARNESS column may hold, as the Laravel runner's `stock-pgsql` column has since C2e), and the
comparison script exits non-zero when a test the control runs is skipped under Ferro. Fed the
pre-fix Ferro column, it names `testListTableColumnsOidConflictWithNonTableObject` and exits 1.

The MySQL family's version string was never transformed, so this class cannot occur there, and the
same comparison against a local MariaDB 10.11 with a `pdo_mysql` control confirms it: **no test the
control runs is skipped under Ferro, on either major** (3.10.6: 535 and 535, the same digest). The
one difference runs
the other way: on 4.4.4 Ferro RUNS — and passes — `StatementTest::testExecWithRedundantParameters`,
which upstream skips for `PDO\MySQL\Driver` because PDO's MySQL driver does not report a redundant
parameter. Locally the control also fails two tests of its own: `testListDatabases` /
`testIntrospectDatabaseNames`, because the local least-privilege user cannot create a database
(consistent with triaging it (c)), and `testInvalidUserName`, because MariaDB 10.11 answers an
unknown user with errno 1698, which DBAL does not map to `ConnectionException` (the runner's MySQL
8.4 and MariaDB 11.8 are not affected).

## DBAL 4.4.4, re-measured with both fixes

Run 36989388348, same commit, same protocol; A5's column for comparison.

| column | result line (both runs, identical) | executed | passed | A5 |
|---|---|---|---|---|
| **PostgreSQL 17.10** | `Tests: 730, Assertions: 830, Errors: 3, Failures: 7, Skipped: 353, Incomplete: 2` | 375 | **365** | 364/374 |
| **MySQL 8.4.11** | `Tests: 730, Assertions: 871, Errors: 2, Failures: 9, Skipped: 341, Incomplete: 4` | 385 | **374** | 374/385 |
| **MariaDB 11.8.8** | `Tests: 730, Assertions: 869, Errors: 2, Failures: 9, Skipped: 342, Incomplete: 4` | 384 | **373** | 373/384 |
| **SQLite 3.53.2** | `Tests: 730, Assertions: 668, Errors: 28, Skipped: 358, Incomplete: 11` | 361 | **333** | C3-6a: 329/357 |
| **CONTROL — `pdo_sqlite`** | `Tests: 730, Assertions: 748, Skipped: 358, Incomplete: 11` | 361 | **361** | C3-6a: 357/357 |

PostgreSQL gains exactly the one test: one fewer skipped, one more executed, one more passed, and its
10 non-passes are A5's 10 by name. MySQL and MariaDB are unchanged, as expected. The SQLite pair
executes 4 more tests than C3-6a's dev-container run in BOTH columns (730 tests against 729, 358
skipped against 361) — an environment difference, not Ferro: the gap between the columns is 28,
exactly as at C3-6a, and the 28 are C3-6a's two groups.

## Not established

- §14's bar for the bridge, as for DBAL 4, names SQLite and the ORM suite: SQLite runs (224/267
  against a control at 262/267), Doctrine ORM 2 has not been run through the bridge at all.
- The skip comparisons are LOCAL — PostgreSQL 16.13 and MariaDB 10.11 in the dev container — not the
  runners' PostgreSQL 17.10 / MySQL 8.4 / MariaDB 11.8. The runner can now produce them for any
  family (`FERRO_DBAL_CONTROL=1` with the family's DSN); the `dbal-suite` workflow does not yet run a
  server-family control column.
