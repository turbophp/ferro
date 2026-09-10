# Spike: the MySQL `CALL` blind spot is where Ferro READS the column metadata

**Status:** SPIKE. The confirming test is `engine/crates/ferro-backend-mysql/tests/call_columns_spike_it.rs`
— it SKIPS without `FERRO_TEST_MYSQL_URL`, so it is CI's integration lane that answers the three
questions below. **Its output must be recorded here before any engine change.** Every claim below is marked **VERIFIED (source)** — read out of this
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

**UNVERIFIED HERE — and it is the crux, so CI must confirm it before any code:**

1. that a prepared `CALL p()`'s `stmt.columns()` is in fact empty at runtime;
2. that the **executed** result set's `columns()` is populated for the same procedure;
3. how many result sets a `CALL` actually produces (MySQL emits the procedure's sets plus a final
   OK), and whether `affected_rows` lands where the engine expects.

A spike test in the `mysql_chaos_it.rs` style — create a procedure, prepare, execute, print both
column lists — settles all three in one CI run.

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

- **S1 (CI only, no engine code):** `call_columns_spike_it.rs`, LANDED but not yet run against a
  live MySQL. It can invalidate everything below, which is why it lands first; record its printed
  output in this document when CI has run it.
- **S2:** move the buffered path's `cols` to the executed result set's metadata, with the fate trade
  recorded and a live test that a `CALL` returns real cells.
- **S3:** the same for `query_stream`.
- **S4:** the `/proto` multi-result-set change for `selectResultSets()`, which is only worth doing
  once S2/S3 make a `CALL` return anything at all.
