# M1-S9a — the spec-delta ledger (append-only; Task 13 is the single author who applies these)

Tasks 1–12 make NO edit to `ferro-spec-v0.2.md`, `proto/PROTOCOL.md` or `CLAUDE.md`. Each records
here, in its own `### Task N` block, the spec delta its change forces. Task 13's single author
applies the whole batch at the end. This is a hard rule: parallel spec authorship is what produced
the S8a §22.2 (u)/(v) contradiction.

### Task 1

- §19.3: **no text change** — the in-tx `ConnectionLost` cell already reads `Retryable`, and this
  task changed no production code. What changed is that the cell is now OBSERVABLE.
- §22.2: record that the in-tx `ConnectionLost` cell is live-guarded on **all three** engine targets
  (PG 17, MySQL 8.4, MariaDB 11.8) by `engine/crates/ferrod/tests/in_tx_fate_it.rs`, closing the
  M0-review "guard that cannot fail" finding (finding 2) on the defining safety property.
  Mutation-proven in BOTH directions: `services/sql.rs:332` `in_tx: true → false` turns both new
  tests RED on all three backends (`0x2001 WRITE_UNCONFIRMED/Indeterminate` where
  `0x1001 CONNECTION_LOST/Retryable` is required), while 36 pre-existing live tests — including
  BOTH chaos suites (`chaos_fate_it` 8, `mysql_chaos_it` 6, `tx_it` 15, `sql_exec_it` 7) — stay
  green under the same mutation.
- Worth a sentence wherever the chaos-harness discipline is described: the in-flight marker must be
  a **string-literal predicate** (`'<marker>' <> ''`), never a `/* comment */`, because MariaDB
  strips comments from `information_schema.processlist.INFO`; and the processlist poll must filter
  `COMMAND IN ('Execute','Query')` so a PREPARE-phase match can never be mistaken for an in-flight
  statement. Both are pre-existing in-tree rules (`mysql_chaos_it.rs`) that this file now depends on
  for its falsifiability once Task 9 lands `ConnectionLost { dispatched }`.

### Task 2

- **§7.1** — add the implicit-commit hazard as a SECOND assist signal alongside the S2 lexer:
  pre-dispatch, **MySQL/MariaDB-dialect only**, unknown-leading-keyword → HAZARD. Same
  assist-not-authority contract as `classify`: it may only make a later loss-classification MORE
  conservative (Retryable → Indeterminate), never less, and the protocol latch (Task 8) corrects a
  false positive the moment the statement completes. PostgreSQL and SQLite are unconditionally
  `false` (PG DDL is transactional), so PG behaviour is byte-identical.

- **§7.1 / §22.2** — record the measured deviation from the plan's drafted hazard list: **`EXECUTE`
  is a hazard, `PREPARE` and `DEALLOCATE` are not.** The implicit commit is a property of the
  statement that RUNS, not of how it was dispatched, so `PREPARE s FROM 'CREATE TABLE …'; EXECUTE s`
  commits the open transaction at the EXECUTE. Measured live on **MySQL 8.4.11 and MariaDB
  11.8.8**: the transaction's earlier INSERT survives a subsequent `ROLLBACK` on both engines, while
  the identical shape with a prepared DML does not. Unlike the leading `COMMIT` the plan-verify pass
  correctly refused to add, `EXECUTE` is genuinely reachable through tx-scoped EXEC — it is not
  transaction control, so `ferro-pool`'s `guard_tx_control` passes it to the wire. Leaving it on the
  safe list would have left `branch::RETRYABLE` mintable for a statement that had already committed,
  i.e. the exact at-least-once blocker this slice exists to close.

- **§7.1 note (scope honesty, not a behaviour change)** — the hazard is a LEXICAL assist over the
  statement text the client sent. It cannot see inside a stored program, which is why `CALL`/`DO`
  are hazards unconditionally (same reasoning as S6's unconditional CALL/DO pin), and it says
  nothing about whether a statement mutates session state (that stays `classify`'s question).
