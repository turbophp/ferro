# C1f: the framework suite's MySQL-family columns — `ferro-mysql` and `ferro-mariadb`, each with its alias and its PDO control

**Date:** 2026-10-02 · **Slice:** M2-C1f · **Runner:** `testkit/laravel-suite.sh` with
`FERRO_LARAVEL_SVC={mysql,mariadb,mysql-local}` — the same curated `laravel/framework` v11.51.0
allowlist (`testkit/laravel/allowlist.txt`, 89 driver-agnostic files, 633 cases) the PostgreSQL and
SQLite columns run.

**What is being measured:** the two MySQL-family connections new in this slice, against upstream's
own tests — three columns per family, as on PostgreSQL (SPEC §22.2 (am), (ar)):

| family | Ferro column | alias column | CONTROL |
|---|---|---|---|
| MySQL (server: MySQL 8.4) | `ferro-mysql` (`FerroMySqlConnection`) | `mysql` | `stock-mysql`: `pdo_mysql`, driver `mysql` |
| MariaDB (server: MariaDB 11.8) | `ferro-mariadb` (`FerroMariaDbConnection`) | `mariadb` | `stock-mariadb`: `pdo_mysql`, driver `mariadb` |

The Ferro column is §15's one-word config change; the alias registers the same engine under the
stock driver NAME (`FerroConnections::register(['mysql' => 'ferro-mysql'])`); the control is
upstream's own `pdo_mysql` at the same server and database, at `timezone => '+00:00'` like the
engine's pinned sessions, and its contact assertion inverts. Laravel 11 resolves MariaDB through its
OWN driver (`MariaDbConnection`, `MariaDbGrammar`, `MariaDbBuilder`), which is why MariaDB is its own
family here — see *What the review changed*.

## Recorded numbers

The on-demand `laravel-suite` workflow, `family: mysql`, run
[37002856371](https://github.com/turbophp/ferro/actions/runs/37002856371) on commit `0d5e5a8`, two
runs per column. Passed / executed (executed = 633 − skipped):

| server | column | result line (both runs) | executed | passed |
|---|---|---|---|---|
| MySQL 8.4.11 | `ferro-mysql` | `Tests: 633, Failures: 1, Skipped: 54` | 579 | **578** |
| | `mysql` (alias) | `Tests: 633, Errors: 2, Skipped: 46` | 587 | **585** |
| | `stock-mysql` (control) | `Tests: 633, Skipped: 46` | 587 | **587** |
| MariaDB 11.8.8 | `ferro-mariadb` | `Tests: 633, Failures: 1, Skipped: 54` | 579 | **578** |
| | `mariadb` (alias) | `Tests: 633, Errors: 2, Skipped: 45` | 588 | **586** |
| | `stock-mariadb` (control) | `Tests: 633, Skipped: 45` | 588 | **588** |

(Each line also carries `Risky: 1`, on every column including the controls.) **Reproducible:** each
column's run 2 was compared with its run 1 by `compare-columns.php` — the skip set and the failure
set are identical in all six, with the same digests. The Ferro and alias columns' assertion counts
are identical across runs too; the two controls' differ by 2 between runs (2080/2082 on MySQL,
2083/2081 on MariaDB) with identical outcomes, a test whose assertion count varies under `pdo_mysql`.
The contact lines name the server each column reached (`8.4.11 (MySQL Community Server - GPL)`,
`11.8.8-MariaDB-ubu2404`), now checked against the server the job started.

**Every non-pass, by cause** — the same on both servers, with the same digests as the local MariaDB
10.11 columns:

- **The Ferro column's 1 failure**, `TimestampTypeTest::testChangeDatetimeColumnToTimestampColumn`,
  picks its expected value with `match ($this->driver)`, and **its 8 (MySQL) / 9 (MariaDB) extra
  skips** are `#[RequiresDatabase]` resolved from the driver name — the driver-NAME artifact. The
  alias column, running the same code under the stock name, runs every one of them.
- **The alias column's 2 errors** are `QueryBuilderUpdateTest::testBasicUpdateForJson` data sets #0
  and #1, both `LogicException: Ferro: this pool (default) does not advertise
  literals_are_standard=true` — Laravel's `castAsJson()` testing helper reaching `quote()`. Both pass
  through `pdo_mysql`. That is a real Ferro gap (C1g), not an artifact.
- **The controls are clean.** Nothing upstream fails on either server under its own family.

## The measurement named the work — three times

Built first, before any tier surface beyond registering the driver. Local, against MariaDB 10.11 in
the dev container under the `mysql` family — a regression aid, **not** a recorded number:

| run | `ferro-mysql` result line | executed | passed |
|---|---|---|---|
| 1: registration only | `Tests: 633, Errors: 456, Failures: 11, Skipped: 54` | 579 | 112 |
| 2: + `insert()` | `Tests: 633, Errors: 9, Failures: 4, Skipped: 54` | 579 | 566 |
| 3: + unique detection, `TIMESTAMP` rendering | `Tests: 633, Failures: 2, Skipped: 54` | 579 | **577** |

1. **443 of 456 errors were one line.** Stock `MySqlConnection::insert()` executes through
   `getPdo()->prepare()` — which the PDO shim refuses, since there is no PDO — and then stores
   `getPdo()->lastInsertId()` for `MySqlProcessor::processInsertGetId()`. The override keeps stock's
   two load-bearing details: the key is read INSIDE `run()`, before `QueryExecuted` fires, and it is
   the STATEMENT's own key — `pdo_mysql`'s contract, measured: `"0"` after an insert that generated
   none, after a `SELECT` and after an `UPDATE`. That is the opposite of SQLite, where `pdo_sqlite`
   keeps the key on the handle and so does this tier (§22.2 (bn)).
2. **9 `createOrFirst()` errors were one detector.** Stock `MySqlConnection::isUniqueConstraintError()`
   matches the message text `Integrity constraint violation: 1062` — `pdo_mysql`'s wording, which a
   Ferro error does not carry. So `createOrFirst()` re-threw the very duplicate it exists to catch.
   The override decides it from the error's errno (1062, exactly the code the stock pattern names),
   the rule the tier already applies to lost connections and concurrency errors (§22.2 (bw)).
   (The first version of this file said "8 failures"; the review re-measured with only this override
   reverted: `Errors: 9`, all `createOrFirst()`.)
3. **2 failures were the `TIMESTAMP` rendering.** `$table->timestamp()`/`timestamps()` create MySQL
   `TIMESTAMP` columns, which Ferro reads as `TIMESTAMPTZ`; the raw policy handed up the canonical
   RFC3339 `2017-11-12T13:14:15Z` where `pdo_mysql` returns `2017-11-12 13:14:15`.
   `QueryBuilderTest::testPluck` and `EloquentBelongsToManyTest::testCustomPivotClassUpdatesTimestamps`
   compare the raw string. The tier now hands up the naive UTC wall clock on this family — SPEC
   §22.2 (cb) for why that is the right call on MySQL even though PostgreSQL keeps RFC3339. It is
   `pdo_mysql`'s string to the SECOND: a fractional column renders the canonical fraction (none, or
   six digits), not the column's precision, which a live test pins.

## Skip sets and causes, compared against the control (the §22.2 (bz) rule)

`testkit/dbal/compare-columns.php`, each Ferro column against its family's control.

- **The Ferro column SKIPS tests the control runs — exit 1, as it should.** Each carries a
  `#[RequiresDatabase]` naming the family, which testbench resolves from `getDriverName()` —
  `ferro-mysql` or `ferro-mariadb` under these columns. On the MySQL family that is 8: 4 name
  `['mysql', 'mariadb']` (`SchemaBuilderTest::testChangeToTextColumn`,
  `testChangeTextColumnToTextColumn`, `testModifyNullableColumn`,
  `TimestampTypeTest::testChangeStringColumnToTimestampColumn`), 3 are `testBasicUpdateForJson`'s data
  sets under `['sqlite', 'mysql', 'mariadb']`, and 1 is `testGetFullTextIndexes` under
  `['mysql', 'mariadb', 'pgsql']`. The MariaDB family adds `testSystemVersionedTables`
  (`'mariadb'`), for 9. Plus one Ferro-only FAILURE on each,
  `TimestampTypeTest::testChangeDatetimeColumnToTimestampColumn`, whose expected value is chosen by
  `match ($this->driver)`. This is the driver-NAME artifact C2e measured on PostgreSQL
  (§22.2 (am), (ar)), shown by the alias column rather than by argument.
- **Under the alias the skip sets are identical to the control's — but the columns are not.** Two
  `QueryBuilderUpdateTest::testBasicUpdateForJson` data sets fail through Ferro and pass through PDO:
  the test seeds its expectation through Laravel's `castAsJson()` testing helper, which reaches
  `DB::escape()` → `FerroPdoShim::quote()`, refused on a MySQL-family pool because the engine does not
  yet advertise `literals_are_standard` for it (§22.2 (at); C1g). Measured locally on MariaDB 10.11
  under the `mariadb` family: `mariadb` 586/588, `stock-mariadb` 588/588.

## What the review changed (SPEC §22.2 (cb))

The first version of this file reported the alias column **identical** to the control on MariaDB —
the same 3 failures, the same digests — and triaged those 3 as "upstream's own on this
framework/server pair". The adversarial review showed that was a coincidence of NAMES:

- The MariaDB columns ran under the `mysql` family — Illuminate's MySQL GRAMMAR against a MariaDB
  server — and so did the control. `ConnectionThreadsCountTest::testGetThreadsCount` (`threadCount()`
  is `null` under the MySQL grammar) and the two `testBasicUpdateForJson` data sets (`castAsJson()`
  compiles to `cast(? as json)`, a MariaDB syntax error) fail through stock `pdo_mysql` under driver
  `mysql` and PASS under driver `mariadb`. So they were the grammar's, not the server's, and a Laravel
  11 application on MariaDB does not run that grammar.
- The two `testBasicUpdateForJson` failures the alias column "shared" with the control failed for a
  DIFFERENT reason: the control on that syntax error, Ferro on the `quote()` refusal. The comparison
  matched test names, not causes. `compare-columns.php` now also compares the exception TYPE of every
  failure the two columns share, and exits 1 on a difference — against the first recorded pair it
  exits 1 where it exited 0.

Local re-measurement under the corrected harness, MariaDB 10.11 (regression aid, not recorded):

| column | result line | executed | passed |
|---|---|---|---|
| `ferro-mariadb` | `Tests: 633, Failures: 1, Skipped: 54` | 579 | **578** |
| `mariadb` (alias) | `Tests: 633, Errors: 2, Skipped: 45` | 588 | **586** |
| `stock-mariadb` (control) | `Tests: 633, Skipped: 45` | 588 | **588** |

The first CI run ([36996251660](https://github.com/turbophp/ferro/actions/runs/36996251660), commit
`166c385`, before the review) measured both servers under the `mysql` family: MySQL 8.4
`ferro-mysql` 578/579, `mysql` 585/587, `stock-mysql` 587/587; MariaDB 11.8 `ferro-mysql` 577/579,
`mysql` 584/587, `stock-mysql` 584/587. On MySQL 8.4 the alias was already two short of the control —
`cast(? as json)` is valid there — which is the review's prediction, measured. They are superseded
by the table above, which also carries the review's code changes.

## Not established

- **§15's bar.** It asks for the integration suite green; neither MySQL-family Ferro column is, and
  the alias column is short of its control by the `quote()` refusal (C1g).
- What the framework suite cannot see, because its own fixtures avoid it — the review found two by
  hand: `dropAllTables()` on a schema whose FK parent sorts before its child (fixed, live-tested in
  `MySqlSchemaLiveTest`), and the MySQL session keys Laravel's connector would have issued
  (`strict`, `isolation_level`, `timezone`, `charset`/`collation`), which the control does not set
  and which Ferro ignores (documented in `docs/known-incompatibilities.md`).
- The Eloquent ORM's own test suite is not run on any family.
