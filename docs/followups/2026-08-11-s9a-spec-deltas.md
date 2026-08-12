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
