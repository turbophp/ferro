# Follow-up (POLICY DECISION, not a bug): a sub-second `TIMESTAMPTZ` is refused on READ, and that is stricter than `pdo_pgsql`

**Found:** M1-S9, by the first-ever `doctrine/orm 3.6.8` functional-suite run against Ferro —
**16 PostgreSQL tests**, the single largest cluster in that run
(`docs/orm-suite/2026-08-13-results.md`, category **(b)**).
**Belongs to:** `php/doctrine-dbal` — `Ferro\DBAL\Value\DbalValuePolicy::timestampTz()`
(`src/Value/DbalValuePolicy.php:164-171`). **Not** an engine matter: the wire payload is lossless
canonical text and the engine renders it correctly. Nothing in `ferro-backend-pg` changes either way.
**Severity:** medium. **No wrong value has ever been produced by this** — the refusal is loud,
raised client-side, and outside the §9.2 fate branches. What it costs is *readability of a column
stock Doctrine can read*.
**Assignment: M2-entry.** It is a **§22.2 (ab) POLICY decision** — *we refuse what PDO corrupts* —
not a defect to fix quietly, and it must be decided rather than drifted into.

## What happens, measured

Every one of the 16 tests fails with the driver's own message:

```
Ferro: the TIMESTAMPTZ value '2026-08-18T07:50:04.684132Z' cannot be handed to Doctrine's type
layer — Doctrine's DateTimeTzType parses only whole seconds on every platform and has no fallback,
so the sub-second part could only be dropped … It is refused rather than converted because
Doctrine's stock converters would accept it SILENTLY and produce a different value.
```

The tests are `QueryDqlFunctionTest::testDateAdd` and `::testDateSub` (7 data sets each — `year`,
`month`, `week`, `day`, `hour`, `minute`, `second`) plus `::testDateAddWithColumnInterval` and
`::testDateSubWithColumnInterval`.

## Why stock passes what Ferro refuses — and it is NOT that stock is more careful

This is the load-bearing half, and it inverts the obvious reading. DBAL 4.4.4's
`DateTimeTzType::convertToPHPValue()` is a strict `createFromFormat('Y-m-d H:i:sO')` with **no
`date_create` fallback**: hand it a microsecond value and it throws. So Doctrine's own type layer
cannot read this value either.

Stock passes because **the value never reaches Doctrine's type layer at all**. A DQL
`DATE_ADD(...)` expression has no type mapping, so `pdo_pgsql` hands the application the **raw
string**, microseconds and all, and the test parses it itself.

Ferro's engine, by contrast, **knows the column's type** — the `timestamptz` OID is on the wire —
so the driver's `ValuePolicy` fires its blanket sub-second refusal at `decodeRow`, on a value that
under stock would have been an untyped string. Ferro is refusing at a point stock never reaches,
for a reason that is real everywhere else.

## The candidate relaxation, and its cost

**Candidate:** pass sub-second `TIMESTAMPTZ` through as its canonical text (a PHP `string`) exactly
when **no type conversion is requested** — i.e. reproduce what PDO de facto does — and keep the
refusal for every path where a Doctrine `Type` will convert the value.

Its cost is what must be decided, not assumed:

- **The driver cannot see the requested `Type`.** `ValuePolicy::decode(int $tag, mixed $data)` is
  handed a per-cell TYPE TAG and nothing else; DBAL's type conversion happens ABOVE the driver, in
  the wrapper `Connection`/`Result`. So "exactly when no conversion is requested" is not currently
  expressible at the site that refuses. Either the refusal moves up a layer, or the policy stops
  refusing and the wrong-value risk moves to `DateTimeTzType` (which throws — loudly — so this may
  be acceptable; that is the decision).
- **Rule 2 of §22.2 (ab) would gain an exception**, and the whole force of that rule is that it has
  none. Whatever is chosen must state which values remain refused and why the line is where it is.
- **A `string` where an application expected a `DateTimeImmutable`** is its own compatibility
  surface — the same one stock has, which is the argument for it.

**Do NOT do:** a blanket relaxation of (ab) rule 2 (that is the rule that stops
`'2026-00-05'` → 2025-12-05 and `'24:00:00'` → `00:00:00`), and do NOT truncate to whole seconds —
silent precision loss is the outcome the refusal exists to prevent.

## Two facts a decider needs that the ORM run does not show

1. **A NAIVE `TIMESTAMP` keeps its microseconds today** — no refusal
   (`DbalValuePolicy.php:135-148`). The asymmetry is not arbitrary: `DateTimeType` HAS a
   `new DateTime($value)` fallback, `DateTimeTzType` has none. So the policy is already
   value-dependent-per-tag, and a relaxation would be making the `TIMESTAMPTZ` tag behave like the
   `TIMESTAMP` tag rather than inventing a new shape.
2. **The refusal has no backend branch.** `timestampTz()` tests `str_contains($t, '.')` BEFORE any
   per-family rendering, and the MySQL backend does render fractional `TIMESTAMPTZ` text (engine
   unit test: `MyValue::Date(2026,8,5,0,0,0,999_999)` → `2026-08-05T00:00:00.999999Z`,
   `engine/crates/ferro-backend-mysql/src/mytext.rs:331-334`). Since SPEC §9 maps MySQL `timestamp`
   → `TIMESTAMPTZ`, a **MySQL `TIMESTAMP(6)` column is refused by the same rule**. The ORM suite
   does not exercise it, so that reach is derived from the code, **not measured live** — measure it
   before writing it into a spec sentence.

## How to reproduce

```bash
FERRO_ORM_SVC=pg FERRO_ORM_MODE=ferro ./testkit/orm-suite.sh --filter testDateAdd   # a DEBUG run
```

Or without the ORM, through the driver alone: `SELECT now()` on PostgreSQL. `now()` is a
`timestamptz` and it does carry microseconds — verified against the testkit container, not assumed:

```
$ docker compose -f testkit/docker-compose.yml exec -T pg \
    psql -U ferro -d ferro -t -A -c "SELECT now()::text, (now()::text ~ '\.') AS has_fraction;"
2026-08-18 08:41:57.777136+00|t
```

so `SELECT now()` is refused through `Ferro\DBAL\Driver` by the rule above while `pdo_pgsql`
returns the string. (The refusal ITSELF is read from `DbalValuePolicy.php:164-171`; only the
microsecond half of this repro was measured here.)
