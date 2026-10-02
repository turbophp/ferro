# doctrine/orm functional suite through Ferro — first measurement (2026-10-02, LOCAL)

SPEC §14's acceptance named "ORM functional suite green on PG + MySQL" and every record since
M1-S8b said it was **not run**. This is its first run. SPEC §22.2 (ci).

**These are LOCAL numbers** (the dev container: PostgreSQL 16.13 and MariaDB 10.11.14, a debug
`ferrod`), not the recorded CI columns. The on-demand `orm-suite` workflow measures PostgreSQL 17,
MySQL 8.4 and MariaDB 11.8 twice per column; its numbers supersede these once dispatched on `main`.

## What ran

- `doctrine/orm` **3.7.3**, `tests/Tests/ORM/Functional` — **1627 tests**, minus upstream CI's own
  excluded groups (`performance`, `locking_functional`).
- `doctrine/dbal` **4.4.4** (the driver's own lock), PHPUnit 11.5, PHP 8.4.
- Runner: `testkit/orm-suite.sh`, with a replacement `TestUtil` (upstream's maps `db_driver` only, and
  creates/drops the database through a privileged connection) and a bootstrap **contact assertion**:
  the Ferro column's native connection must be a `Ferro\Client\Connection`, the control's a PDO
  (inverted), both must round-trip `SELECT 1` in the database the runner reset.
- Every column has a **stock-PDO control** (`pdo_pgsql` / `pdo_mysql`) against the same server, and is
  compared with it as SETS — failures AND skips — with `testkit/dbal/compare-columns.php`.

## Results

| Column | Ferro | Control | Skips equal? | Ferro-only non-passes |
|---|---|---|---|---|
| PostgreSQL, **stock ORM config** | **1571 / 1597** | 1597 / 1597 | yes (30 = 30) | 26 |
| PostgreSQL, SEQUENCE preference | 1563 / 1594 | 1589 / 1594 | yes (33 = 33) | 26 |
| MariaDB 10.11 | **1580 / 1586** | 1586 / 1586 | yes (41 = 41) | 6 |

(x / y = passed / executed, executed = 1627 − skipped.) The SEQUENCE column's 5 shared failures
(`DDC832Test`, quoted table names) fail identically through `pdo_pgsql` — the harness change itself
causes them, not Ferro.

**Before this slice the stock PostgreSQL column was 397 / 1594** (the feasibility scout, same tree):
1185 of its non-passes were `NoIdentityValue`, because DBAL 4 makes ORM 3 map `AUTO` to `IDENTITY` on
PostgreSQL and Ferro refused `lastInsertId()` there outright. It now answers INSIDE a transaction with
`lastval()` on the pinned connection — what `pdo_pgsql` runs — and the unit of work always inserts in
one. **ORM adoption on PostgreSQL is config-only now**, which D-S8b-5 recorded as impossible.

## Every Ferro-only non-pass, triaged

| Count | Families | Cause | Class |
|---|---|---|---|
| 16 | PG | `QueryDqlFunctionTest::testDateAdd`/`testDateSub` (7 units each) and the two `…WithColumnInterval` tests read `CURRENT_TIMESTAMP()` — a sub-second `TIMESTAMPTZ` — which the driver's value policy refuses rather than truncating (§22.2 (ab)). The refusal is at FETCH, so it applies to any `now()` read through the DBAL driver on PostgreSQL, typed or not. | documented (b) |
| 6 | PG + MariaDB | `AdvancedDqlQueryTest::testUpdateAs`/`testDeleteAs`, `ClassTableInheritanceTest` ×3, `DDC2090Test`: `MultiTable{Update,Delete}Executor` creates a TEMP table and uses it in later statements outside a transaction; on a transaction-mode pool those land on other connections (§7.4). Remedy: wrap the DQL in a transaction. | documented (b) |
| 4 | PG | `ReadonlyPropertiesTest`: `$conn->insert(…)` then `lastInsertId()` with NO transaction — the one case Ferro still refuses on PostgreSQL, because the autocommit statement's connection has returned to the pool and `lastval()` would run elsewhere. | documented (b) |

## Defects this measurement found and fixed (§22.2 (ci))

1. **`Result::fetchFirstColumn()` silently truncated at a `false` cell** — `FetchUtils` loops on
   `fetchOne() !== false`; a boolean column starting with false came back `[]` (`GH9230Test`). Now a
   `fetchNumeric()` loop.
2. **An `F64` could not bind a PostgreSQL `numeric`** — `DecimalType` binds a PHP float under
   `STRING` (7 tests). The engine now sends it as the shortest round-trip decimal text, which is what
   `pdo_pgsql` sends.
3. **`SELECT NULL` was `Unsupported` on MySQL/MariaDB** (`MYSQL_TYPE_NULL`, `NewOperatorTest`). It reads
   as NULL now.
4. **`lastInsertId()` on PostgreSQL** — inside a transaction, `lastval()` (DBAL 3 with a name:
   `currval(name)`); hygiene's `DISCARD SEQUENCES` is what makes it safe, and a live test proves a
   recycled connection never answers with the previous tenant's `nextval()`.

## Not established

- **MySQL 8.4 is not measured here** (the container has MariaDB only) — the workflow measures it.
- **The DBAL 3 lane** (ORM 2/3 over DBAL 3.10) is not run.
- **SQLite** has no ORM column.
- Two runs per column were not taken locally; the workflow takes them and diffs run 2 against run 1.
