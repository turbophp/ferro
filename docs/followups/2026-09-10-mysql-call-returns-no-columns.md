# Spike: the MySQL `CALL` blind spot is where Ferro READS the column metadata

**Status: SPIKE ANSWERED.** `engine/crates/ferro-backend-mysql/tests/call_columns_spike_it.rs` ran
green against live MySQL in CI's `integration` lane (run 34466153085, job 102835108802,
2026-09-10). Its output is recorded below, and **the diagnosis is confirmed exactly**. Every claim below is marked **VERIFIED (source)** — read out of this
tree and the vendored driver at `53c984e` — or **UNVERIFIED HERE**, meaning it needs a live
MySQL that this container does not have (no Docker daemon), so CI's integration lane is the
authority.

**Why it matters:** C1a recorded `selectResultSets()` as an M2 incompatibility and named this the
*true* prerequisite — a `/proto` multi-result-set change would otherwise ship a feature that still
returns nothing.

---

## What C1a recorded, and how the wording misleads

> a prepared `CALL` returns no usable rows today, because a prepared `CALL` declares ZERO result
> columns even when the procedure emits a result set (streamed → rows discarded; buffered → N
> cell-less rows).

That reads as a MySQL constraint end to end. **Half of it is; the other half is a Ferro choice, and
that half is the fixable one.**

## The mechanism, exactly

**VERIFIED (source)** — `engine/crates/ferro-backend-mysql/src/query.rs`:

```rust
let columns = stmt.columns();          // line 55 — COM_STMT_PREPARE metadata
…
for (idx, col) in columns.iter().enumerate() { … }   // line 87 — row cells mapped by that list
```

`cols` is built from the **prepared statement's** columns, and each row's cells are then mapped by
iterating that same list. So an empty list does not merely lose the column NAMES — it yields rows
with **zero cells**, which is precisely the "N cell-less rows" symptom. Every row-returning path is
affected because every one of them prepares: `PoolBackend::query` → `query::run`, and
`PoolBackend::query_stream` → `stream::open`. Only `simple_query` uses the text protocol, and it
drains rows without returning them.

**VERIFIED (source)** — the vendored `mysql_async` already exposes what is needed, with no fork:

- `QueryResult::columns()` / `columns_ref()` read the **pending (executed) result set's** metadata,
  not the prepare-time list (`vendor/mysql-async/src/queryable/query_result/mod.rs:412,425`);
- multiple result sets are first-class — **though not through the methods this document first
  named**: `next_set()`, `more_results_exists()` and `next_row_or_next_set()` are PRIVATE. The
  public pattern is `collect()` per set plus `is_empty()` to ask whether another follows, which the
  driver's own doc spells out (*"`SELECT 'foo'; SELECT 'foo','bar';` will produce a QueryResult with
  two result sets in it. One can use `QueryResult::is_empty` …"*). Caught by compiling the spike
  test, before any design leaned on it;
- `next_row` treats an empty column list as "empty, but not yet consumed result set" and advances —
  so an empty prepare-time list is *expected* by the driver, not an error state.

This matters because S1 and S6 both needed a vendored fork for their signals. **This one does not**,
which changes the cost estimate materially.

**VERIFIED LIVE (CI, MySQL 8.4)** — the three questions, answered. Procedure body was
`BEGIN SELECT 7 AS seven, 'x' AS letter; END`:

```
[spike] prepare-time columns: 0 -> []
[spike] set #0: execution-time columns 2 -> ["seven", "letter"]; 1 row(s), first row cells = Some(2)
[spike] total result sets seen: 1
test a_prepared_call_reports_its_columns_only_after_execution ... ok
```

1. **A prepared `CALL`'s `stmt.columns()` is empty** — `0 -> []`. Confirmed.
2. **The executed result set's `columns()` IS populated** — `2 -> ["seven", "letter"]`, and the row
   carries `Some(2)` cells. Confirmed, and it is the whole fix: the metadata exists, one call away
   from where the engine currently looks.
3. **One result set** for a single-`SELECT` procedure. The trailing OK packet is not surfaced as a
   set by `collect()`/`is_empty()`, so a `CALL` with N `SELECT`s should yield N — worth re-checking
   at S2 with a two-`SELECT` procedure, since only the N=1 case is measured here.

Note what the row already proves: through the DRIVER the cells are there (`Some(2)`). Ferro's rows
come back cell-less only because `query.rs` maps them against the prepare-time list. Nothing about
MySQL or `mysql_async` needs to change.

## The option someone will reach for, and why it is closed

"Route `CALL` through the TEXT protocol (`COM_QUERY`), which reports columns at execution" is the
obvious alternative, and the scoping note that preceded this document proposed investigating it. It
is **foreclosed on two counts**, so the investigation was not done:

- **The text protocol takes no parameters.** `CALL p(?)` needs `COM_STMT_PREPARE`/`COM_STMT_EXECUTE`.
  A text-protocol route would work only for parameterless `CALL`s, i.e. it would fix the easy half
  and leave the interesting half broken.
- **It is unnecessary.** The prepared path can read the executed set's metadata too
  (`QueryResult::columns()`), so there is nothing the text protocol offers here that costs less.

It would also mean choosing a protocol from the SQL text, which is a shape charter rule 6 is
pointed at — the assist lexer classifies statements for PINNING, but picking an execution path from
a leading keyword is a different thing and would want its own argument.

## The trade a fix must state rather than gloss

`query.rs`'s own docblock says the current order is deliberate: `cols` is built from the prepared
statement **"before the query runs"**, so an out-of-scope column type is a *loud `Unsupported`
before the statement is sent*. Reading the metadata from the executed result set instead moves that
check to **after** the statement has run — which changes the §19.3 story for a type error from
"never sent" to "sent, then refused". That is not a blocker, but it is a fate-relevant change and
belongs in the design, not in a footnote.

## What this does NOT settle

`selectResultSets()` still needs a `/proto` representation — `ExecOk` carries exactly one
`cols`+`rows` — and that remains a separate change. The point of this spike is only that the
prerequisite is smaller and better-located than C1a's wording suggests: **it is one metadata source,
not a MySQL wall.**

## Suggested slicing

- **S1 (CI only, no engine code): DONE.** `call_columns_spike_it.rs` landed and ran green; output
  recorded above. It could have invalidated everything below and did not.
- **S2:** move the buffered path's `cols` to the executed result set's metadata, with the fate trade
  recorded and a live test that a `CALL` returns real cells.
- **S3:** the same for `query_stream`.
- **S4:** the `/proto` multi-result-set change for `selectResultSets()`, which is only worth doing
  once S2/S3 make a `CALL` return anything at all.
