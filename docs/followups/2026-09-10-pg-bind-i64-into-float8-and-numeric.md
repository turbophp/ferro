# Follow-up: the PG bind matrix refuses `I64 → float8` and `I64 → numeric`

> **RESOLVED (M2-C2b, 2026-09-10).** Both widenings shipped in `PgInt`, SPEC §22.2 (an), exactly as
> designed below: `NUMERIC` with **no value gate** (arbitrary-precision, so an `i64`'s decimal
> rendering is exact at every magnitude, sent `Format::Text`) and `FLOAT8` **gated on exact
> representability** (refused pre-send otherwise, naming both routes out). `FLOAT4` was deliberately
> NOT widened — no measured caller, per the §22.2 (af) membership rule. `accepts` / `to_sql` /
> `encode_format` moved in one edit; the directional lockstep proof's `every_variant` fixture gained
> `2^53` and `2^53 + 1`, and **that growth is mutation-proven load-bearing**: with the `to_sql`
> backstop mutated stricter than the gate the proof goes RED, and removing just those two fixture
> entries turns it green over the same bug. One correction to the design below: the gate could NOT
> be written as the obvious `(n as f64) as i64 == n`, because Rust's float→int `as` cast saturates
> and so reports TRUE for `i64::MAX` — it compares through `i128`. Proven live against PG 17 with PG
> itself as the oracle. Suite effect measured, and it matched the prediction exactly: **65/71** under
> `ferro-pgsql` and **71/71** under the `pgsql` alias.


**Found:** M2-C2, by the upstream `laravel/framework v11.51.0` integration subset — 3 PostgreSQL
tests, and they are the ONLY non-passing tests in that subset once the driver-name artifacts are
accounted for.
**Belongs to:** `engine/crates/ferro-backend-pg/src/bind.rs` (`PgInt::accepts` / `PgInt::to_sql` /
`PgInt::encode_format`, and the value half in `check_range`). **Not** a tier defect: neither the
Eloquent nor the Doctrine tier learns the column's type, and charter rule 6 forbids inferring it
from the SQL text.
**Severity:** medium-high, and the same class as the M1-S9 `I64 → text`/`bool` widening this
directly continues (§22.2 (af)). Safety is intact — a loud pre-send `NonRetryable`, never
`Indeterminate` — but both shapes are ordinary stock-framework code that works on MySQL.
**Blocks:** any insert of a PHP integer into a `double precision` column, and any comparison against
a PostgreSQL expression whose result type is `numeric`.

## The two measured shapes

**1. `parameter 2: canonical I64 cannot bind to PG type float8` (1 test).**
`QueryBuilderTest::testIncrement` builds `$table->float('wallet_1')` — which Laravel's stock
PostgreSQL grammar compiles to `double precision` — and then inserts `['wallet_1' => 100]`. A PHP
`int` literal in a float column is not an edge case; it is what anyone writes for a round amount,
and Eloquent has no type declaration to say otherwise.

**2. `parameter 0: canonical I64 cannot bind to PG type numeric` (2 tests).**
`QueryBuilderTest::testWhereYear` / `testOrWhereYear` compile to
`where extract(year from "created_at") = ?`. PostgreSQL types `extract(...)` as **`numeric`** (it
changed from `double precision` in PG 14), so the parameter slot is `numeric` while the builder
binds a PHP int. `pdo_pgsql` sends every parameter in text format and PostgreSQL coerces.

## The widening, and what its VALUE gate has to be

The membership rule §22.2 (af) set — *widen only what is MEASURED, name the exact `Type`* — admits
`FLOAT8` and `NUMERIC` and **not** `FLOAT4`: no measured caller reaches `real` (Laravel's
`$table->float()` is `double precision`), and unmeasured widening is how a pre-flight rots.

The two targets need different value handling, and that difference is the whole design:

- **`numeric` is exact at every magnitude.** PostgreSQL's `numeric` is arbitrary-precision, and the
  bind is the integer's decimal rendering in `Format::Text` — the same shape `PgDecimalText` already
  writes. No gate.
- **`float8` is exact only to 2^53.** Above that, an `i64` does not survive the round trip: the
  nearest `f64` differs, and the write is silently wrong. That is precisely the silent-corrupt-write
  class `check_range` already refuses for `F64 → float4` in both directions (overflow to `inf`,
  underflow to `0`). So `I64 → float8` must carry a **pre-send exactness gate**: bind when
  `(n as f64) as i64 == n`, refuse otherwise, naming the two routes (bind an `F64` if the rounding
  is intended, or use a `numeric`/`decimal` column if it is not).

  Note this is STRICTER than `pdo_pgsql`, deliberately: PDO sends the decimal text and PostgreSQL
  rounds it without complaint. Ferro's §9.1 rule is that a non-representable value is loud, never
  coerced, and a `bigint` primary key silently landing 1 apart in a float column is the exact harm
  that rule exists for.

## What shipping it requires

`accepts`, `to_sql` and `encode_format` move in ONE edit (widening one without the others fails the
directional lockstep proof, `s7_accepts_is_never_looser_than_the_boxed_impl`), and the lockstep
proof's `every_variant` fixture needs an `I64` at 2^53 and one just past it — the same
fixture-blindness §22.2 (af) records for the `bool` value gate applies here: a proof whose fixture
holds only small integers cannot tell a value gate from no gate at all. `every_target_type` needs
`FLOAT8` and `NUMERIC` if it does not already carry them.

## Suite effect, once fixed

The M2-C2 subset should go from **62/71 to 65/71 under `ferro-pgsql`** and from **68/71 to 71/71
under the `pgsql` alias** — i.e. a clean sweep of the curated subset in the alias column. Measure it;
do not assert it here.
