# Follow-up: the PG bind matrix is narrower than libpq in the `I64 → text/bool` direction

> **RESOLVED (M1-S9, 2026-09-09).** Both widenings shipped in `PgInt`, SPEC §22.2 (af):
> `I64 → text` (`Type::TEXT` only — the measured target; decimal rendering, `Format::Text`) and
> `I64 → bool` with the explicit decision this doc demanded made in favour of a **value gate** —
> only 0 and 1 bind (exactly what Doctrine's `BooleanType` emits), every other integer is refused
> pre-send with both routes named, so the §9.1 "stray integer silently becomes boolean" class
> stays refused. `accepts`/`to_sql`/`encode_format` moved in one edit; the directional lockstep
> proof was RE-DERIVED (its fixture-blindness to value-gated pairs is named in §22.2 (af), and
> `every_variant` gained `I64(1)`); the F64 arms are untouched. Proven live in both measured
> shapes. The suite effect is measured at ledger item A5, not asserted here.

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
