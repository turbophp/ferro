# Ferro M0 · Slice S5 — SQL EXEC Service (live `SELECT 1` end-to-end) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** The autocommit `EXEC` path end-to-end: a client sends an `EXEC` frame → `ferrod` checks out a `ferro-pool` connection to the **live Dockerized Postgres** → runs the SQL → maps PG types to the canonical `TypedValue` set → frames `HEAD(cols)+DATA(rows)+terminal END` under the credit windows → releases. Delivers the M0 headline: a real `SELECT 1` traveling client→ferrod→pool→PG→client. `fetch:none` (INSERT → `affected`) and the `?`→`$n` placeholder scanner included; `query_id`/tx-control rejected as `Unsupported`; backend errors classified (v0 SQLSTATE map); **never retried** (charter rule 3).

**Architecture:** `EXEC` is the request-bearing handler S3's dispatch stubbed to `Unsupported`. S5 replaces that stub with the real handler: it holds a per-pool `Pool<PgBackend>` (built at daemon startup from config), checks out a conn (measuring `queue_us`), runs the SQL via `ferro-backend-pg` (measuring `exec_us`), maps PG `Row`/OIDs → `TypedValue`, and streams the result through the S3 writer's DATA channel under per-request credit + the per-session cap, ending with the supervisor's terminal `Outcome::Ok`. The `?`→`$n` scanner (decision M-1) is a fingerprint-cached, literal/comment/dollar-quote/jsonb-operator-aware normalization in `ferro-backend-pg`.

**Tech Stack:** ferro-proto (new SQL messages + `/proto` methods.sql), ferrod (services/sql, pool wiring, DATA-channel producer), ferro-pool/ferro-backend-pg (exec + typed rows + placeholder), Docker PG (live tests). PHP client gets the SQL message vectors byte-matched (the client runtime is S7).

## Global Constraints

- **`/proto` is the source of truth:** add `[methods.sql] EXEC=1` (and any SQL flags/error codes) to the registry; regenerate the lock + both codecs' constants + golden vectors in the same change (charter rule 2). No hand-written SQL method/OID/error numbers in Rust or PHP.
- **M0 scalar TypedValue set only** (decision T-1): `NULL, BOOL, I64, F64, TEXT, BYTES`. A PG column whose OID maps outside this set → `NonRetryable{Unsupported}` (a loud typed error, never a silent miscast). DECIMAL/TIMESTAMP/UUID/JSON/etc. are reserved but unimplemented in S5.
- **`?`→`$n` engine-side scanner** (decision M-1): mechanical parameter-syntax normalization, literal/line-comment/block-comment/dollar-quote AND PG jsonb operator (`?` `?|` `?&`) aware, fingerprint-cached (paid once per distinct SQL, off the p99 path). Rule-6 tension accepted (recorded in §22). Clean fallback documented: client emits native `$n`.
- **`queue_us` vs `exec_us` first-class** (SPEC §6/§16): `queue_us` = pool wait (checkout); `exec_us` = checkout→last row/command-complete, EXCLUDING time blocked on client credit backpressure (a slow consumer must not inflate the KPI).
- **Credit-framed streaming** (F-1): HEAD(cols) + DATA(row batches ~256 KiB) + terminal END, emitted only when both the per-request window (64 frames / 4 MiB) and the per-session 16 MiB cap admit it; `WINDOW_UPDATE` replenishes both. `stats.bytes = Σ DATA payload_len`.
- **No transparent retry** (charter rule 3 / §19.3): a dead conn mid-EXEC → the pool evicts + surfaces `ConnectionLost`/`Indeterminate`; the handler NEVER re-runs the statement.
- **Reject as `Unsupported`:** `query_id` (manifest is M3), tx-control SQL via EXEC (the guarded `Checkout::exec` from S4 rejects it), out-of-M0 param/column types.
- **Exactly-one-END preserved:** the EXEC handler declares its outcome via the S3 Responder; the supervisor sends the single terminal. Streamed DATA frames go through the data channel; the terminal Ok through the reserved control permit.
- **Charter gates** + live PG integration (skip without `FERRO_TEST_PG_URL`) + PHP byte-match of the new SQL vectors.

## File Structure

```
/proto/methods.toml           + [methods.sql] EXEC=1 (regen lock + consts)
/proto/vectors/               + sql_exec_request_select1, sql_exec_response_select1, sql_exec_request_params, sql_exec_response_none
/engine/crates/ferro-proto/src/messages/sql.rs   ExecRequest/ExecResponse/ColMeta/Stats DTOs
/engine/crates/ferro-backend-pg/src/
  exec.rs        run a query -> rows; map tokio_postgres::Row + column OIDs -> Vec<TypedValue> + ColMeta
  rowmap.rs      PG OID -> canonical tag (M0 scalar set; else Unsupported)
  bind.rs        canonical TypedValue params -> tokio_postgres params
  placeholder.rs ?->$n fingerprint-cached scanner (literal/comment/dollar-quote/jsonb-op aware)
  error_map.rs   PG SQLSTATE -> taxonomy v0 (23505 Unique, 40001 Serialization, 40P01 Deadlock, 42xxx Syntax, 08xxx ConnectionLost, 57014 cancel, else NonRetryable) preserving raw SQLSTATE
/engine/crates/ferrod/src/
  pools.rs       build Pool<PgBackend> per config pool name at startup; PoolRegistry the handler resolves
  services/sql.rs  the EXEC handler: decode -> resolve pool -> checkout(queue_us) -> exec(exec_us) -> frame HEAD+DATA+END under credit
  services/sql_frame.rs  HEAD/DATA row-batch framing under the credit window + session cap
/php/client/... (S7 owns the runtime) — S5 only adds the SQL message vectors + PHP encode/decode byte-match
```

---

### Task 1: `/proto` SQL method + `ferro-proto` EXEC messages + golden vectors + PHP byte-match

- Add `[methods.sql] EXEC = 1` to `methods.toml`; regenerate `registry.lock.json` + Rust consts (build.rs) + PHP `Constants.php` (gen-php) — zero-diff regen check.
- `ferro-proto/src/messages/sql.rs`: `ExecRequest { pool: String, sql: Option<String>, query_id: Option<String>, params: Vec<Value>, timeout_ms: Option<u32>, readonly: bool, fetch: u8 /*rows|none|stream*/ }`; `ExecResponse` is streamed, but define `ColMeta { name: String, tag: u8 }`, `Stats { queue_us: u64, exec_us: u64, rows: u64, bytes: u64 }`; the terminal Ok body carries `{ cols: Vec<ColMeta>, affected: u64, last_insert_id: Option<Value>, stats: Stats }` (rows travel as DATA frames, not in the terminal). Positional msgpack via rmp-serde (compact), consistent with S1.
- Golden vectors (Rust-generated): `sql_exec_request_select1`, `sql_exec_request_params`, `sql_exec_response_head_select1` (HEAD cols), `sql_exec_response_none` (fetch:none affected). Rust encode==bytes + PHP `Message`/decode byte-match both paths (extend the S1 conformance harness with the SQL messages).
- **Gate:** `cargo test -p ferro-proto` + regen-zero-diff + PHP conformance green.
- **Commit** `feat(s5): /proto methods.sql EXEC + ferro-proto ExecRequest/Response + SQL golden vectors + PHP byte-match`.

---

### Task 2: `ferro-backend-pg` typed exec — OID→tag, params bind, `?`→`$n` scanner, error map

- `rowmap.rs`: PG OID → canonical tag for the M0 scalar set (bool→BOOL, int2/4/8→I64, float4/8→F64, text/varchar/bpchar→TEXT, bytea→BYTES, unknown/other→`Unsupported`). `bind.rs`: canonical `Value` params → tokio-postgres `&(dyn ToSql)`; `exec.rs`: run `client.query`/`query_raw`, map each `Row` column via OID→tag→`TypedValue`, return `(Vec<ColMeta>, RowStream, affected)`.
- `placeholder.rs`: the fingerprint-cached `?`→`$n` scanner (skip `'...'`/`"..."`/`--`/`/* */`/`$$...$$`; do NOT rewrite jsonb `?`/`?|`/`?&`); a dedicated hazard-case unit corpus (jsonb operators, casts `::`, dollar-quoted bodies, quoted `?`).
- `error_map.rs`: SQLSTATE → taxonomy v0 (as above), preserving raw SQLSTATE; a genuine SQL error (42601 syntax) → `NonRetryable{Syntax}`, a dead conn → `ConnectionLost{Retryable}`. **Never retry.**
- **TDD:** unit — placeholder hazard corpus; OID→tag table incl. Unsupported; error_map SQLSTATE cases. Live (skip without env) — `exec("SELECT 1")` → cols=[{name,I64}], rows=[[I64(1)]]; params round-trip the scalar set; `exec("SELECT $bad")` → Syntax; an out-of-M0 type column → `Unsupported`.
- **Gate + commit** `feat(s5): ferro-backend-pg typed exec (OID->tag, param bind, ?->$n scanner, v0 error map)`.

---

### Task 3: daemon pool wiring + the EXEC handler (checkout → exec → HEAD/DATA/END under credit)

- `ferrod/src/pools.rs`: at startup build a `Pool<PgBackend>` per configured pool (config gains a `[[pool]] name, dsn` list; DSNs from env/secret refs per §12 — the client never sees them); a `PoolRegistry { by_name: HashMap<String, Pool<PgBackend>> }` the handler resolves. ferrod gains deps on `ferro-pool` + `ferro-backend-pg`.
- `services/sql.rs`: the real EXEC handler (replaces S3's `Unsupported` stub for `service=SQL, method=EXEC`): decode `ExecRequest`; reject `query_id` (Unsupported) and an unknown pool (Unsupported); resolve the pool; `checkout()` (record `queue_us`); run `exec()` via the guarded `Checkout::exec` (records `exec_us` from checkout→command-complete); for `fetch:rows`, send `HEAD(cols)` then `DATA` row batches (~256 KiB) through the S3 DATA channel under the per-request `Credit` + per-session cap, replenished by `WINDOW_UPDATE`; declare the terminal `Outcome::Ok{cols, affected, stats}` via the Responder (the supervisor sends it). For `fetch:none`, no DATA — the terminal Ok carries `affected`. A backend error → declare `end_error(mapped ErrorPayload)`; a dead conn → `ConnectionLost`/`Indeterminate`, no retry.
- `services/sql_frame.rs`: HEAD/DATA framing + the credit debit (`stats.bytes = Σ DATA payload_len`, same number for debit/cap/stats); `exec_us` EXCLUDES time blocked on credit backpressure.
- **TDD (live, skip without env):** `exec_select1_shape` — EXEC "SELECT 1" over a real client→ferrod→pool→PG round trip returns cols=[{name,I64}]/row[I64(1)] with populated `queue_us`+`exec_us`, terminating in exactly ONE END (the S3 invariant holds); `exec_fetch_none_affected` — INSERT with fetch:none → affected>0, no DATA frames; `exec_multiframe_backpressure` — a large result under a small window pauses and resumes on WINDOW_UPDATE without exceeding the 16 MiB cap; `exec_error_classification` — a syntax error → `NonRetryable{Syntax}`; `unsupported_type_and_query_id` — out-of-M0 type / query_id → `Unsupported`. Unit: credit debit/replenish; the one-END invariant still holds for the EXEC path (extend S3's session tests).
- **THE MILESTONE:** capture a live `SELECT 1` traversing client→ferrod→pool→Docker PG→client in the report.
- **Gate + commit** `feat(s5): daemon pool wiring + EXEC handler (checkout->exec->HEAD/DATA/END under credit) + live SELECT 1`.

---

### Task 4: shared TypedValue golden vector (close the S1 deferral) + placeholder/credit hardening

- Add the deferred (S1 roll-up) **shared golden vector carrying a TypedValue** in the divergent integer range (e.g. an EXEC response DATA frame with `I64(200)`/`I64(-200)`) so the integer ladder + `[tag,value]` framing are locked by the shared cross-language arbiter (Rust encode==bytes + PHP re-encode==bytes), not just independently-typed asserts.
- Harden: the placeholder scanner hazard corpus (add any cases the review surfaces); credit-window edge cases (zero-credit stall + WINDOW_UPDATE resume); `exec_us`-excludes-backpressure asserted.
- **Gate + commit** `test(s5): shared TypedValue vector locks the integer ladder cross-language + placeholder/credit hardening`.

---

## Self-Review (author against the real toolchain)

- **Spec coverage (design S5 gate):** SQL vectors + PHP byte-match → T1; typed exec + OID map + placeholder + error map → T2; EXEC handler HEAD/DATA/END under credit + queue_us/exec_us split + live SELECT 1 + fetch:none + backpressure + Unsupported → T3; the shared-TypedValue vector (S1 deferral) → T4.
- **Deferred (noted):** full type system (DECIMAL/TIMESTAMP/UUID/JSON hydration + §9.1 policies) → M1+; manifest/query_id → M3; the PHP client RUNTIME that issues EXEC and hydrates DTOs → S7 (S5 only vectors the SQL messages); TX service (BEGIN/COMMIT over the pin) → S6.
- **Verify the plan before executing** (S1/S3/S4 pattern): an adversarial pass on the exec_us-excludes-backpressure accounting, the credit/session-cap interaction, the placeholder scanner hazards, the one-END invariant under a streamed EXEC, the pool-checkout-inside-the-handler lifecycle (the checked-out conn must be released even if the handler panics — interacts with S3's supervisor + S4's Drop), and the OID→tag Unsupported path.

## Execution Handoff

Subagent-driven: fresh implementer per task (TDD, gates), review after each (probe the one-END + no-transparent-retry under EXEC, the queue_us/exec_us accounting, the placeholder hazards), whole-branch review before S6. Live tests against the S2 Docker PG.
