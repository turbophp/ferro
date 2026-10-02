# doctrine/orm functional suite through Ferro — RECORDED on CI runners (2026-10-02)

The recorded columns that `2026-10-02-local-results.md` said would supersede its dev-container
numbers. SPEC §22.2 (ci); the M2 exit record (§22.3) cites this file.

- Run **37067176160** of the on-demand `orm-suite` workflow, on `main` at `7902867`.
- `doctrine/orm` **3.7.3**, `tests/Tests/ORM/Functional`: 1627 tests, minus upstream CI's own
  excluded groups (`performance`, `locking_functional`). `doctrine/dbal` **4.4.4**.
- Servers: PostgreSQL 17, MySQL 8.4, MariaDB 11.8 (the `testkit` images).
- **Every column ran twice.** The workflow's assert step requires, for every run:
  - a PHPUnit result line;
  - the `[ferro] reset:` line;
  - the contact assertion: `native=Ferro.Client.Connection` for Ferro, a PDO class for the control;
  - the right server family (`server=` not MariaDB on the MySQL job, MariaDB on the MariaDB job);
  - run 2's fail and skip sets equal to run 1's, compared by sha256.

  It raised no error in any of the three jobs. The result lines below are identical between run 1
  and run 2 in every column.

## Results

passed / executed, where executed = 1627 − skipped.

| Column | Ferro | Stock-PDO control | Skip sets | Ferro-only non-passes | Shared with control |
|---|---|---|---|---|---|
| PostgreSQL 17, **stock ORM config** | **1571 / 1597** | 1597 / 1597 | equal (30, sha256 `8c86681461b37441`) | 26 | 0 |
| PostgreSQL 17, SEQUENCE preference | 1563 / 1594 | 1589 / 1594 | equal (33) | 26 | 5 |
| MySQL 8.4 | **1583 / 1594** | 1590 / 1594 | equal (33, sha256 `216a5b8c7b98310a`) | 7 | 4 |
| MariaDB 11.8 | **1579 / 1586** | 1586 / 1586 | equal (41, sha256 `f5f10fc4260a8710`) | 7 | 0 |

The raw result lines (run 1 = run 2 in every column):

```
pg  ferro-stock      Tests: 1627, Assertions: 6615, Errors: 26, Skipped: 30.
pg  control-stock    Tests: 1627, Assertions: 6688, Skipped: 30.
pg  ferro-sequence   Tests: 1627, Assertions: 6607, Errors: 31, Skipped: 33.
pg  control-sequence Tests: 1627, Assertions: 6680, Errors: 5, Skipped: 33.
my  ferro            Tests: 1627, Assertions: 6685, Errors: 7, Failures: 4, Skipped: 33.
my  control          Tests: 1627, Assertions: 6718, Failures: 4, Skipped: 33.
ma  ferro            Tests: 1627, Assertions: 6644, Errors: 7, Skipped: 41.
ma  control          Tests: 1627, Assertions: 6677, Skipped: 41.
```

**No test that the control runs is skipped under Ferro, on any family.** The skip sets are equal as
sets, compared with `testkit/dbal/compare-columns.php`.

## The Ferro-only non-passes, matched against the local triage

These are the same tests, by name, that `2026-10-02-local-results.md` triaged. That doc has the cause
of each; none is a driver defect.

- **Six tests fail Ferro-only on all three families**, for one cause: §7.4, a TEMP table used across
  statements outside a transaction.
  - `AdvancedDqlQueryTest::testUpdateAs` / `testDeleteAs`
  - `ClassTableInheritanceTest::testCRUD`, `testBulkUpdateIssueDDC368`, `testBulkUpdateNonScalarParameterDDC1341`
  - `DDC2090Test::testIssue`
- **PostgreSQL adds 20** (6 + 20 = 26):
  - 16 `QueryDqlFunctionTest` date-arithmetic cases read a sub-second `CURRENT_TIMESTAMP()`, which the
    driver refuses at fetch (§22.2 (ab)).
  - 4 `ReadonlyPropertiesTest` cases call `lastInsertId()` outside any transaction.
- **MySQL and MariaDB add 1** (6 + 1 = 7): `NewOperatorTest::testShouldSupportNullLiteralExpression`.
  `SELECT NULL` is refused before execution (`MYSQL_TYPE_NULL`).

**The shared failures:**

- **PostgreSQL SEQUENCE column, 5 shared:** `DDC832Test`'s quoted table names fail through
  `pdo_pgsql` too. They throw the same exception type, and only the message wording differs. The
  harness's SEQUENCE hook causes them, as recorded locally.
- **MySQL 8.4, 4 shared:** the compare script does not print their names (it prints names only for a
  type mismatch), and the run artifact cannot be fetched from this container. They fail identically
  through `pdo_mysql`, with the same exception type. That makes them **not Ferro's**. **It does not
  make them upstream's**, and they are not further attributed here.

## Not established

- **None of the four columns is green.** That includes the two controls with shared failures.
- **SQLite has no ORM column.**
- The DBAL 3 lane of the ORM suite is not run.
- The PostgreSQL SEQUENCE preference column is a harness configuration, not stock ORM config. The
  stock-config column is the one §14 is measured against.
