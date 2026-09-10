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
   **DONE at S2:** the spike now also runs a two-`SELECT` procedure and prints its set count. It
   asserts `> 1` rather than a fixed number: what S4 needs is that a multi-`SELECT` procedure really
   does surface more than one set (otherwise a multi-result-set wire change has nothing to carry),
   while the exact count — whether a trailing OK packet surfaces as a set on a given engine — is
   printed for S4 to design against rather than pinned into a per-engine red.

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
- **S2 (buffered path): DONE.** `query.rs` now falls back to the executed result set's metadata
  **when — and only when — the prepared statement declares no columns.** The design is narrower than
  this document proposed, and the narrowing was forced by a test rather than chosen for taste:
  `query_it.rs`'s `out_of_scope_column_is_unsupported` asserts that a deferred column type is
  refused *before the query runs* and that the conn stays clean. A uniform post-drain read would
  have changed that for every buffered statement in the tree; preferring the prepared list confines
  the fate change to the `CALL` shape that was broken anyway, and leaves that test passing
  unchanged. **The fate trade is therefore paid only on the fallback arm**, and it is recorded in
  SPEC §22.2 (av) and in `run`'s own comment: the refusal is still KNOWN-FATE (raised after a full
  drain, conn clean and reusable, never `Indeterminate`), but a procedure that writes *and* selects
  an out-of-scope column type now has its write applied before the client sees `Unsupported`.
  Live guards: `call_returns_real_cells` (mutation-proven — revert the `None` arm and both `cols`
  and the row's cells come back empty) and `no_result_set_statement_is_unchanged_by_the_fallback`,
  which exists because every INSERT/UPDATE reaches the fallback arm too and "inert there" needed an
  assertion rather than an argument. The spike was widened to a TWO-`SELECT` procedure in the same
  change, closing the N=1 gap noted above.
- **S3 (streamed path): DONE.** And the dispatch really was the substance — the fallback could not
  be pasted in, because the signal S2 used does not exist at the moment the park decision is made.
  What resolved it: the **B2a fork already exposes `QueryResult::into_conn`**, whose docblock names
  exactly this case (the owned-route exit for a statement that produced no result set). So the
  empty-prepared-list arm now **parks, RUNS, and then reads `columns_ref()`** — a non-empty executed
  set streams (a `CALL` yields real rows, **incrementally**, not buffered), an empty one hands the
  connection straight back. No new fork edit was needed; the capability was built at B2a and simply
  never used from this direction. The (av) narrowing is preserved exactly: an ordinary `SELECT`
  (non-empty prepared list) takes the same code it always did. **Cost, stated rather than
  discovered later:** a streamed `INSERT` now parks and recovers where it previously ran buffered on
  a borrowed conn, so a *failed* streamed INSERT discards its connection instead of reusing it —
  efficiency, never correctness, and the recovery mechanism was already proven live on both engines
  by `stream_recovery_it.rs`'s `no_result_set_recovers_via_query_result`. `MysqlRowStream::NoRows`'s
  contract changed with it and its docblock says so. Live guard:
  `ferrod`'s `mysql_streamed_call_delivers_rows_and_the_session_survives` — mutation-proven (restore
  the prepare-time dispatch and it goes red with a *clean terminal and zero rows*, which is what
  made the old behaviour so easy to miss) — plus the pre-existing
  `mysql_streamed_insert_reports_its_generated_key`, which is now the regression guard for the
  INSERT arm's park/recover round trip. SPEC §22.2 (aw).
- **S4:** the `/proto` multi-result-set change for `selectResultSets()`. **Its precondition is now
  met** — S2 and S3 make a `CALL` return real rows on both paths — so this is the next real slice
  here. Note what both paths currently do with a multi-`SELECT` procedure: they surface the FIRST
  result set and drain the rest (`drop_result` buffered, `into_conn` streamed). The two-`SELECT`
  set count is printed by the spike in every CI integration run, and S4 should be designed against
  that printed number rather than against an assumption.
