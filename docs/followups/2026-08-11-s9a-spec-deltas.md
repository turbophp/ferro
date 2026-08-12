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

### Task 5

- **§5.2** — note that a partial inbound frame buffers only the bytes actually RECEIVED (plus at
  most one 64 KiB reserve step ahead of them), never the declared `payload_len`. A header may
  declare up to `MAX_FRAME_PAYLOAD` (16 MiB); before this change the decoder reserved that in full
  the instant the header landed, so one local connection pinned **16 777 233 bytes for a 17-byte
  send** (measured, both directions of the mutation). It now pins **65 553** for the same send — a
  256x reduction — and grows geometrically only as real bytes arrive. Reassembly of a legitimate
  large frame is unchanged (`reserve` is an allocation hint, not a correctness input).

- **§22.2 (availability, finding 5)** — record that this closes the codec half of the M0-review
  availability finding. The remaining halves (`max_connections`, `frame_read_timeout`,
  `idle_timeout`) are Task 11; without them a slow-trickle client still holds a session open
  indefinitely, it simply can no longer amplify 17 bytes into 16 MiB while doing so. Peercred still
  bounds the whole class to local allow-listed uids.

- **No `/proto` change, and none implied**: `MAX_FRAME_PAYLOAD` is untouched, the declared-length
  field keeps its meaning, and the wire is byte-identical in both directions. This is purely how the
  decoder's own buffer is grown.

- **CROSS-TASK CORRECTION for Task 11 (hazard 19 is wrong as written — please fix it in the batch).**
  Hazard 19 says giving `FrameCodec` "a `Default`-preserving optional progress handle keeps both
  call sites and the golden-vector tests compiling." It does not. Both cited sites are
  `Framed::new(stream, FrameCodec)` — the unit-struct **value** expression, not a `Default` call —
  so adding ANY field breaks them regardless of the derive. The enumeration is also short: there is
  a third pre-existing site at `session/classify.rs:221`, plus the three added by this task, for
  **six** construction sites after this commit. (Related, measured here: the plan's Task 5 test body
  used `FrameCodec::default()`, which compiles but FAILS `cargo clippy -D warnings` under
  `clippy::default_constructed_unit_structs` while the struct is still a unit; this task uses the
  bare `FrameCodec` literal, matching all three pre-existing in-tree sites.)

### Task 6

- **§12** — scheme logging is **allow-list-only**. The daemon echoes a DSN scheme into a log line
  only when it is one of `{postgres, postgresql, mysql, mariadb}` (matched ASCII-case-insensitively,
  and what is echoed is the daemon's own constant, so `MariaDB://…` logs `mariadb`). Every other
  shape logs a fixed placeholder: `<no scheme>` when the DSN has no `://` at all,
  `<unrecognized scheme>` otherwise. Nothing derived from the operator's string is ever logged.
- **§12 / §22.2** — record WHY it is an allow-list and not a third slicing rule. M1-S6 fixed the
  schemeless leak by slicing ("everything before the first `://` is the scheme, and a scheme cannot
  carry credentials"); the M0-core review then measured the second member of the same class —
  `loggable_scheme("adminuser:s3cretPW://tcp/host")` returned `"adminuser:s3cretPW"` straight into
  `infer_pool_kind`'s WARN, because a malformed DSN can put the credentials BEFORE the first `://`
  where no real scheme exists. Two occurrences of one class in one call path is the signal that the
  rule was wrong, not that it needed another case. The guarantee is now carried by the TYPE:
  `loggable_scheme` returns `&'static str`, which cannot borrow from the DSN, so "log a slice of the
  operator's string" is a compile error rather than a defect someone can reintroduce.
- **§22.2 (behaviour change worth one line)** — an unrecognized-but-harmless-looking scheme
  (`redis://…`, `sqlite://…`) is no longer echoed either. We cannot distinguish a typo'd scheme from
  credential text without parsing, so the allow-list decides, not a character class. The M1-S6 test
  that asserted `redis` passed through is INVERTED, not deleted. Operationally the two placeholders
  stay distinguishable, so a misconfiguration is still diagnosable from the log: "you gave me no
  `://`" and "you gave me a scheme I do not know" remain different messages.
- **No §22.2 claim beyond this function.** Measured while here, so the next reviewer need not:
  `loggable_scheme` is the ONLY DSN-derived value that reaches a log line in `ferrod`/`ferro-pool`/
  either backend (`PoolSpec`'s manual `Debug` already redacts `dsn`). The two adjacent WARNs that
  format a library error built from the DSN — `ferro-backend-mysql/src/conn.rs:193` (`UrlError`) and
  `ferro-backend-pg/src/conn.rs:110-115` (connection-string parse) — echo a scheme, a query-parameter
  name/value, or an option name, never the userinfo component.

### Task 7

- **§19.3 — NEW RULE (the finding-1 fate cell).** `OpContext` gains `tx_writes_persisted`. Once a
  transaction's earlier statements have PERSISTED (a MySQL/MariaDB implicit commit, observed by the
  protocol latch or assumed from the pre-dispatch hazard), the **`Retryable` branch is unmintable**
  for that transaction's failures: any outcome that would carry `branch::RETRYABLE` is replaced by
  `WriteUnconfirmed{Indeterminate}`. The rule is **BRANCH-shaped, not errc-shaped** — three distinct
  terminals license a client to replay an in-tx failure (`TxDeadline` via the 57014 override,
  `ConnectionLost`, and a retryable `Sql` passthrough such as 1213/40001 on the post-DDL statement),
  and an errc-shaped rule would leave the third one licensing the replay (mutation-proven: the
  errc-shaped variant turns the 40001 and 1213 rows RED). Known-fate NonRetryable outcomes (23505,
  42601, a bind `Unsupported`) pass through VERBATIM — they license nothing and their statement-level
  fate is honestly known.

- **§19.3 — the readonly invariant gains a stated exception.** "A client-declared-readonly statement
  never becomes `Indeterminate`" (§22.2 (ac)) no longer holds in one cell: an in-tx statement loss in
  a partially-committed transaction is `Indeterminate` regardless of `readonly`, because the terminal
  describes the TRANSACTION — whose earlier writes have persisted — not the read. Deliberate, and
  stated rather than silently changed.

- **§9.2 / §22.2 — the persisted terminal carries no `sqlstate` and no `errno`.** Same species as
  the 57014 override's documented field-dropping (§22.2 (o)) and load-bearing for the same reason:
  the dominant filtered case is a MySQL 1213, which DBAL converts to `DeadlockException` — a class
  carrying Doctrine's `RetryableException` marker, i.e. exactly the replay license this cell exists
  to withdraw.

- **§22.2 — deferred `/proto` candidate recorded, not hand-rolled (charter rule 2):** a dedicated
  `TX_PARTIALLY_COMMITTED` code would say what happened more precisely than `WRITE_UNCONFIRMED`
  does. It is a registry + golden-vectors + BOTH-codecs change set, so this slice reuses
  `WRITE_UNCONFIRMED{Indeterminate}` — whose branch contract ("do not replay") is already exactly
  right — and defers the code.

- **Scope note (no behaviour change in Task 7 itself):** all nine `OpContext` literals in
  `services/sql.rs` pass `tx_writes_persisted: false`, so no cell on any engine family moves in this
  commit; the offline suite and the full live fate surface — including Task 1's armed in-tx guard —
  are green and unchanged. Task 8 threads the live value into the two in-tx statement sites
  (`sql.rs:329`, `sql.rs:755`). PostgreSQL has no implicit commit, so the flag stays `false` there
  forever and no PG cell can ever move.

### Task 3

- **§7 (pooling) — `checkout_timeout` now means what the docs always claimed.** It bounds the WHOLE
  checkout: the semaphore acquire, any recycle cleanup on popped idle conns, and a fresh dial all
  run under ONE deadline (`tokio::time::timeout_at`). Before this, the knob wrapped only the permit
  acquire while `backend.connect()` was a bare `.await` and `tokio_postgres::connect` has no connect
  timeout of its own — so a backend that accepts TCP and never finishes the startup handshake pinned
  a permit forever, and `max_size` such dials took the pool to ZERO usable capacity permanently, even
  after the backend recovered (confirmed by execution, M0 core review finding 4a).

- **§7 — a stated, deliberate behaviour change on the recycle path.** The checkout-time cleanup
  (defensive `ROLLBACK` + hygiene reset) used to get a FRESH FULL `checkout_timeout` PER popped conn,
  so one checkout's total latency could reach `max_size x checkout_timeout` while the knob claimed
  1x. It is now bounded by the checkout's SHARED deadline, and a cleanup that consumes the whole
  budget ends that checkout in `Err(PoolError::Timeout)` (-> `POOL_TIMEOUT{Retryable}` on the wire)
  instead of silently spending a second budget to dial. The eviction rule is UNCHANGED and still the
  safety property: a conn whose cleanup did not complete is dropped, never handed out and never
  pushed back (no cross-tenant state leak); the next checkout dials fresh. Retry remains client
  policy — the engine adds none (charter rule 3). `ferro-pool/tests/tx_api.rs`'s
  `bounded_recycle_evicts_a_conn_whose_cleanup_blocks` was updated to assert the new contract; both
  of its original properties (bounded, evicted) are still asserted.

- **§7 / §12 (hygiene) — a poisoned idle mutex is no longer a process abort.** All three
  idle-stack lock sites in `pool.rs` (checkout pop, `poison_idle_for_test`, and critically
  `Checkout::drop`) recover with `.unwrap_or_else(std::sync::PoisonError::into_inner)` — the
  in-tree `ferrod/src/pools.rs` `PoolEntry::lock` idiom. `Checkout::drop` runs during unwind, and a
  panic raised while another unwind is in progress is `std::process::abort()`, which would take every
  worker's connections down with it. The lock only ever guards a trivial pop/push, so recovery (not
  eviction) is correct. The same idiom is Task 4's to apply in `health.rs`.

- **Followup closable:** `docs/followups/2026-08-10-unbounded-backend-dial.md` — the ~127s OS-TCP
  black-hole case it documents is now bounded by `checkout_timeout`.

- **No `/proto` change.** A wedged/expired checkout reuses the existing `PoolError::Timeout` ->
  `POOL_TIMEOUT{Retryable}` mapping; no new wire constant was minted.

### Task 4

- **§7.6/§16 (the liveness reaper): the ping is BOUNDED by `checkout_timeout`.** A backend that
  cannot answer a liveness ping inside a checkout budget is dead for every purpose the reaper has.
  No new knob — the same bound the checkout-time recycle already uses. Before this, `reap_once`
  awaited `backend.ping()` unbounded **while holding an owned semaphore permit**, so one half-dead
  backend killed the reaper for the pool's lifetime AND leaked that permit permanently (finding 4b,
  confirmed by execution: `max_size=1` + a frozen ping → two successive checkouts both
  `Err(Timeout)`, no idle conn ever evicted again).

- **On expiry the connection is EVICTED, never returned to `idle`.** A ping whose budget expires had
  its future DROPPED mid-round-trip, so the connection's protocol state is unknown; recycling it
  would trade a wedged reaper for handing a half-pinged connection to the next tenant (the hazard-15
  shape, and a cross-tenant-leak class). Pinned by a connection-IDENTITY assertion, not by absence
  of an error.

- **§12/hygiene: the reaper's three `idle` locks recover from poisoning** (`len`, `pop`, `push` →
  one `lock_idle()` helper carrying `unwrap_or_else(PoisonError::into_inner)`, the in-tree
  `ferrod::PoolEntry::lock` idiom). Recovery rather than pool-eviction is correct because the mutex
  only ever guards trivial `Vec` pop/push of whole `IdleConn` values — no multi-step invariant can
  be left half-applied. A `.lock().unwrap()` at ANY of the three sites kills the reaper task on the
  first tick after a poison, which is the same outage the ping bound exists to prevent, reached by a
  different door.

- **No `/proto` change, no wire-visible change, no new configuration.** Charter rule 3 is untouched:
  the reaper classifies and evicts, it never retries or replays anything.
