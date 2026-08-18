# Follow-up: the PG bind matrix is narrower than libpq in the `I64 → text/bool` direction

> **PARTIALLY RESOLVED (M1-S8c) / REOPENED (M1-S9).** The S8c widening closed the two directions
> this file was written about — `I64 → text` and `I64 → bool`. The FIRST-EVER Doctrine ORM suite run
> (M1-S9) then measured the directions it did not close, on ordinary stock-Doctrine shapes:
> **`F64 → numeric` (7 tests), `I64 → float8` (2), `TEXT → int2` (1)** — 10 tests, category **(e)**
> in `docs/orm-suite/2026-08-13-results.md`. Minimal reproductions: `[3.14]` into a `NUMERIC(10,2)`
> column, `[2]` into `DOUBLE PRECISION`. **Milestone assignment: M2-entry.** Closing it is
> S8c-shaped engine work on the fate-adjacent bind path and was DELIBERATELY not done at the exit
> gate (SPEC §22.2 (ap)) — M1-S9a has not yet had its whole-branch review, and this path is
> adjacent to it. The sections below describe the two CLOSED directions; the new rows are recorded
> at the foot of the file.

**Found:** M1-S8b Task 14, by the upstream `doctrine/dbal 4.4.4` functional subset — 16 PostgreSQL
tests.
**Belongs to:** `engine/crates/ferro-backend-pg/src/bind.rs`. **Not** a driver defect: the driver
never learns the column's type, and charter rule 6 forbids inferring it from the SQL text.
**Severity:** medium-high. Safety is intact (a loud pre-send `NonRetryable`, never `Indeterminate`),
but two ordinary stock-Doctrine shapes fail on PostgreSQL and work on MySQL.
**Blocks:** every DBAL date-arithmetic expression with a bound interval, and any
`Connection::insert()`/`update()` that writes a boolean without declaring `Types::BOOLEAN`.

## The two measured shapes

**1. `canonical I64 cannot bind to PG type text` (14 tests).**
`DataAccessTest::testDateAddSeconds` and its 13 siblings run
`$platform->getDateAddSecondsExpression('test_datetime', '?')`, whose PostgreSQL form concatenates
the placeholder into a string (`? || ' SECOND'`). PostgreSQL therefore infers **`text`** for the
parameter, while DBAL binds it `ParameterType::INTEGER` — so the driver correctly sends `TAG_I64`
and the pre-flight refuses it. `pdo_pgsql` sends every parameter in text format and PostgreSQL
coerces.

**2. `canonical I64 cannot bind to PG type bool` (2 tests).**
`BooleanType::convertToDatabaseValue(true, PostgreSQL120Platform)` returns **`int(1)`** (measured),
and `TypeConversionTest` calls `Connection::insert()` with **no `$types`**, so DBAL binds it
`ParameterType::STRING`. `ParameterBinder` keys on the pair and passes the `int` through as `TAG_I64`
— which is the only defensible answer at the driver, since a `1` bound as STRING is an ordinary
integer in every other column type. PostgreSQL's `bool` slot refuses it.

Both are the mirror image of the widening M1-S8b Task 4 already performed (canonical `TAG_TEXT` into
PostgreSQL's own text-input types, §22.2 (aa)). MySQL and MariaDB pass both shapes, because they have
no bind pre-flight at all (`COM_STMT_PREPARE` exposes no inferred parameter types).

## Why it is not fixed in M1-S8b

Task 4 was the one engine bind change this slice budgeted, and it was executed with its own lockstep
proof. A second widening carries the same obligations and belongs to a task that can discharge them:

- **SPEC §19.3's directional rule is the hazard.** `bind::check_param`'s pre-flight may be STRICTER
  than the concrete impl but **never looser** — a looser `accepts` lets the failure land in
  `to_sql_checked`, whose error carries no `DbError`, which `is_session_fatal` reads as a lost
  connection, which turns into a **false `Indeterminate`** for a statement that never left the
  process. `accepts` and the impl must move in ONE edit, and the lockstep proof must be re-derived
  (S8a found it structurally blind to two whole classes).
- **The wire FORMAT branches with it.** Task 4 had to branch `encode_format` as well as `to_sql`,
  because text bytes are not the binary bytes for everything the underlying impl accepts. Any
  `I64`→`text` widening has to answer the same question for the integer types.

## The decision a fix must make explicitly

Widening `I64 → bool` is **not** obviously right: it would let a stray integer become a boolean
silently, which is the coercion class §9.1 exists to refuse. The alternative shape — leaving the
refusal and documenting `Connection::insert(..., ['flag' => Types::BOOLEAN])` as required — is worth
weighing against it. `I64 → text` has no such objection; PostgreSQL's own text input for an integer
into a `text` column is unambiguous.

## How to reproduce

```bash
FERRO_DBAL_SVC=pg ./testkit/dbal-suite.sh --filter 'testDateAddSeconds'
FERRO_DBAL_SVC=pg ./testkit/dbal-suite.sh --filter 'testIdempotentConversionToBoolean'
```

---

## REOPENED, M1-S9 — the three directions the S8c widening did not close

**Found:** M1-S9 Task 4, by the first-ever `doctrine/orm 3.6.8` functional-suite run against Ferro
(3485 tests). **10 PostgreSQL tests**, all category **(e)** — *an engine gap this run measured and
did not close* — in `docs/orm-suite/2026-08-13-results.md`. They are LOUD refusals (`code=12298`,
`NonRetryable`, never `Indeterminate`), so no wrong data has ever been produced by them; what they
cost is capability, reached by SQL stock Doctrine emits on its own.

| direction | tests | reached by |
|---|---|---|
| `canonical F64 cannot bind to PG type numeric` | 7 | an ORM `decimal` field bound from a PHP float — `Ticket\DDC1884Test` ×3, `Ticket\GH9230Test::testIssue` data sets `float=0.0`, `float=-0.0`, `float=null`, `TypeTest::testDecimal` |
| `canonical I64 cannot bind to PG type float8` | 2 | an integer literal into a `DOUBLE PRECISION` column — `TypeValueSqlTest::testSelectDQL`, `::testTypeValueSqlWithAssociations` |
| `canonical TEXT cannot bind to PG type int2` | 1 | a PHP string into a `SMALLINT` — `Ticket\DDC2494Test::testIssue` |

Minimal reproductions (no ORM needed): bind `[3.14]` into a `NUMERIC(10,2)` column; bind `[2]` into
a `DOUBLE PRECISION` column.

**Assignment: M2-entry.** Recorded in SPEC §22.2 (ap) as a deliberate non-closure at the M1 exit
gate, for a stated reason: widening this matrix is engine work on the **fate-adjacent bind path**,
and M1-S9a — which rewrote the fate matrix — has not yet had its whole-branch adversarial review.
Doing both in one slice would re-arm that caveat on the code that decides whether a lost write is
`Indeterminate`.

**The decision section above still governs**, and the new directions each need their own answer to
it — in particular `F64 → numeric`, which is the one with a real objection: a binary float carries
no display scale, so widening it means choosing what `0.1` means in a `NUMERIC(10,2)` slot. The
canonical `DECIMAL` tag exists precisely so an application can say. `I64 → float8` and
`TEXT → int2` have the same shape as the closed `I64 → text` case: PostgreSQL's own text input is
unambiguous for both.
