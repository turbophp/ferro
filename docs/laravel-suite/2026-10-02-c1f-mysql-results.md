# C1f: the framework suite's MySQL column — `ferro-mysql`, the `mysql` alias and the `pdo_mysql` control

**Date:** 2026-10-02 · **Slice:** M2-C1f · **Runner:** `testkit/laravel-suite.sh` with
`FERRO_LARAVEL_SVC={mysql,mariadb,mysql-local}` — the same curated `laravel/framework` v11.51.0
allowlist (`testkit/laravel/allowlist.txt`, 89 driver-agnostic files, 633 cases) the PostgreSQL and
SQLite columns run.

**What is being measured:** `Ferro\Laravel\FerroMySqlConnection` (driver `ferro-mysql`), new in this
slice, against upstream's own tests — in three columns, as on PostgreSQL (SPEC §22.2 (am), (ar)):

| column | what it is |
|---|---|
| `ferro-mysql` | §15's one-word config change |
| `mysql` | the same engine registered under the stock driver NAME (`FerroConnections::register(['mysql' => 'ferro-mysql'])`) |
| `stock-mysql` | **THE CONTROL**: upstream's own `pdo_mysql` at the same server and database, at `timezone => '+00:00'` like the engine's pinned sessions; its contact assertion inverts |

## Recorded numbers

*Pending — the on-demand `laravel-suite` workflow with `family: mysql` (MySQL 8.4.11 and MariaDB
11.8.8, two runs per column). This file is updated in the same slice when they land.*

## The measurement named the work — three times

Built first, before any tier surface beyond registering the driver. Local, against MariaDB 10.11 in
the dev container — a regression aid, **not** a recorded number:

| run | `ferro-mysql` result line | executed | passed |
|---|---|---|---|
| 1: registration only | `Tests: 633, Errors: 456, Failures: 11, Skipped: 54` | 579 | 112 |
| 2: + `insert()` | `Tests: 633, Errors: 9, Failures: 4, Skipped: 54` | 579 | 566 |
| 3: + unique detection, `TIMESTAMP` rendering | `Tests: 633, Failures: 2, Skipped: 54` | 579 | **577** |
| alias `mysql`, run 3's code | `Tests: 633, Errors: 2, Failures: 1, Skipped: 46` | 587 | **584** |
| control `stock-mysql` | `Tests: 633, Errors: 2, Failures: 1, Skipped: 46` | 587 | **584** |

1. **443 of 456 errors were one line.** Stock `MySqlConnection::insert()` executes through
   `getPdo()->prepare()` — which the PDO shim refuses, since there is no PDO — and then stores
   `getPdo()->lastInsertId()` for `MySqlProcessor::processInsertGetId()`. `FerroMySqlConnection`
   overrides it on the shared write path, keeping stock's two load-bearing details: the key is read
   INSIDE `run()`, before `QueryExecuted` fires, and it is the STATEMENT's own key — `pdo_mysql`'s
   contract, measured: `"0"` after an insert that generated none, after a `SELECT` and after an
   `UPDATE`. That is the opposite of SQLite, where `pdo_sqlite` keeps the key on the handle and so
   does this tier (§22.2 (bn)).
2. **8 `createOrFirst()` failures were one detector.** Stock `MySqlConnection::isUniqueConstraintError()`
   matches the message text `Integrity constraint violation: 1062` — `pdo_mysql`'s wording, which a
   Ferro error does not carry. So `createOrFirst()` re-threw the very duplicate it exists to catch.
   The override decides it from the error's errno (1062, exactly the code the stock pattern names),
   the rule the tier already applies to lost connections and concurrency errors (§22.2 (bw)).
3. **2 failures were the `TIMESTAMP` rendering.** `$table->timestamp()`/`timestamps()` create MySQL
   `TIMESTAMP` columns, which Ferro reads as `TIMESTAMPTZ`; the raw policy handed up the canonical
   RFC3339 `2017-11-12T13:14:15Z` where `pdo_mysql` returns `2017-11-12 13:14:15`.
   `QueryBuilderTest::testPluck` and `EloquentBelongsToManyTest::testCustomPivotClassUpdatesTimestamps`
   compare the raw string. The tier now hands up the naive UTC wall clock on this family — see SPEC
   §22.2 (cb) for why that is the CORRECT call on MySQL even though PostgreSQL keeps RFC3339.

## Skip sets, compared as sets (the §22.2 (bz) rule)

`testkit/dbal/compare-columns.php`, Ferro column vs the control:

- **`ferro-mysql`: 8 tests the control runs are SKIPPED under Ferro** — the comparison exits 1, as it
  should. All 8 carry `#[RequiresDatabase(['mysql', 'mariadb'])]`, which testbench resolves from
  `$connection->getDriverName()` — `ferro-mysql` under this column. Plus one Ferro-only FAILURE,
  `TimestampTypeTest::testChangeDatetimeColumnToTimestampColumn`, whose expected value is chosen by
  `match ($this->driver)`.
- **`mysql` (the alias): IDENTICAL to the control** — the same 46 skips and the same 3 failures, the
  same digests on both, 584/587 on both. Under the stock driver name, on this server, Ferro is
  indistinguishable from `pdo_mysql` on all 633 cases.

That is the driver-NAME artifact C2e measured on PostgreSQL (§22.2 (am), (ar)), shown here the same
way — by the alias column, not by argument.

## Triage — the 3 failures every column shares

All three fail identically through `pdo_mysql`, so they belong to this framework/server pair, not to
Ferro: `ConnectionThreadsCountTest::testGetThreadsCount` (`threadCount()` answers `null` on this
MariaDB 10.11) and `QueryBuilderUpdateTest::testBasicUpdateForJson` data sets #0 and #1. The latter
two are absent from the `ferro-mysql` column's failures only because that column SKIPS them — they
are among the 8 name-gated tests — which is the trap the skip comparison exists for: a column that
looked better than its control did so by running less.

## Not established

- **The recorded columns** (MySQL 8.4, MariaDB 11.8) — pending the workflow run above.
- `DB::escape()` / `toRawSql()` remain refused on a MySQL pool: the engine does not advertise
  `literals_are_standard` for the MySQL family (it would have to learn `NO_BACKSLASH_ESCAPES` from
  the session's `sql_mode`), and the shim refuses rather than guesses (§21 D5, §22.2 (at)).
- The Eloquent ORM's own test suite is not run on any family.
