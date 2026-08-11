# Follow-up: one missing PG catalog type (`int2vector`) blocks the whole stock schema manager

**Found:** M1-S8b Task 14, by the upstream `doctrine/dbal 4.4.4` functional subset — 50 of the 78
non-passing PostgreSQL tests, all with the same root cause.
**Belongs to:** `ferro-backend-pg`'s read path / SPEC §9 type coverage. **Not** a driver defect: the
driver never sees the OID.
**Severity:** high for the DBAL tier. Safety is intact — the refusal is loud, `NonRetryable`, and
never `Indeterminate` — but the capability is absent.
**Blocks:** `doctrine/migrations`, `SchemaTool`, `createSchemaManager()->introspectTable()` and every
schema-diffing workflow on PostgreSQL.

## What happens

DBAL's stock `PostgreSQLSchemaManager::selectIndexColumns()` (`src/Schema/PostgreSQLSchemaManager.php`,
~`:425-449`) selects `i.indkey` from `pg_index`:

```sql
SELECT quote_ident(n.nspname) AS schema_name, …, i.indisunique, i.indisprimary,
       i.indkey, i.indrelid, pg_get_expr(indpred, indrelid) AS "where", quote_ident(attname) AS attname
  FROM pg_index i … JOIN LATERAL UNNEST(i.indkey) WITH ORDINALITY AS keys(attnum, ord) ON TRUE …
```

`pg_index.indkey` is an **`int2vector` (OID 22)**. The engine's PG row reader refuses it:

```
unsupported type for column "indkey": PG type int2vector (OID 22). Supported: NULL/BOOL/I64/F64/
TEXT/BYTES … plus the M1-S7 canonical tags … plus the M1-S8a catalog scalars name and "char" (as
TEXT) and oid, regtype and regclass (as I64 …). Deferred: timetz, arrays (incl. oidvector),
interval, inet, and every enum/composite/range type.
```

Every DBAL path that introspects a table reaches that query, so a single missing scalar amplifies
into 50 failing tests spread over `AlterTableTest`, `ForeignKeyConstraintTest`, `ComparatorTest`,
`PostgreSQLSchemaManagerTest`, `SchemaManagerTest` and `Types\JsonbTest`.

## Why it is not fixed in M1-S8b

It is an **engine type-registry change**, and S8b is scoped to a PHP package plus one already-agreed
PG bind change. Adding it means an OID→tag mapping decision, read-path coverage, and the §9.1 policy
sentence that goes with it — a task, not a step in a docs slice.

## The shape a fix would take

There is already a precedent to copy: M1-S8a added the **catalog scalars** `name` and `"char"` (as
`TEXT`) and `oid`/`regtype`/`regclass` (as `I64`) for exactly this class of introspection traffic.
`int2vector` is the same kind of addition — PostgreSQL's text output for it is a space-separated list
of `int2`s (`1 3`), so mapping it to `TAG_TEXT` needs no new `/proto` tag and therefore **no registry,
golden-vector or codec change** (charter rule 2 is satisfied vacuously).

Open questions a real task must answer, not assume:

1. Does the stock schema manager ever *parse* `indkey`, or does it only carry it? (If it parses, a
   text rendering must match what `pdo_pgsql` produces, byte for byte.)
2. `oidvector` is the same family and appears in other catalog queries — decide both together or
   state why not.
3. The bind direction: is `int2vector` ever a *parameter*? Almost certainly not; say so rather than
   widening `accepts` reflexively (§19.3's directional rule makes a loose `accepts` dangerous).

## How to reproduce

```bash
FERRO_DBAL_SVC=pg ./testkit/dbal-suite.sh --filter 'PostgreSQLSchemaManagerTest::testListTableIndexes'
```

or, with no DBAL at all, against the shared PG container through `ferro/client`:

```sql
SELECT indkey FROM pg_index LIMIT 1;
```
