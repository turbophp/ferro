# Ferro M0 · Slice S5 — SQL EXEC Service (live `SELECT 1` end-to-end) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

> **Plan v2 — rewritten after adversarial verification `wf_70051761` (NEEDS_WORK: 4 blockers, 7 majors, 3 minors, all verified against real S1/S3/S4 code).** The v1 plan assumed a windowed streaming DATA-channel producer that S3 never built (S3 built the control-channel terminal path and stubbed the handler seam to `Unsupported`). v2 makes the **charter-rule-5 decision to defer that streaming producer to post-M0** and buffer each result into the single terminal frame — see **Decision D-S5-1**. This dissolves the two streaming blockers + two streaming majors as *speculative optimization*, and keeps every real SQL-correctness fix. The v1→v2 delta is summarized at the end under **v2 verification-fix log**.

**Goal:** The autocommit `EXEC` path end-to-end: a client sends an `EXEC` frame → `ferrod` checks out a `ferro-pool` connection to the **live Dockerized Postgres** → runs the SQL via a new **guarded, row-returning** pool entry → maps PG OIDs to the canonical `TypedValue` set → **buffers** the result and returns it in the **single `Outcome::Ok` terminal frame** → the conn is released. Delivers the M0 headline: a real `SELECT 1` traveling client→ferrod→pool→PG→client. `fetch:none` (INSERT → `affected`) and the `?`→`$n` placeholder scanner included; `query_id`/tx-control/`fetch:stream` rejected as `Unsupported`; backend errors classified (v0 SQLSTATE map, `as_db_error()`-first); a lost conn on a **non-readonly** EXEC surfaces **`Indeterminate{WriteUnconfirmed}`** (§19.3); **never retried** (charter rule 3).

**Architecture:** `EXEC` is the request-bearing handler S3's dispatch stubbed to `Unsupported`. S5 replaces that stub with the real handler: it holds a per-pool `Pool<PgBackend>` (built at daemon startup from config), checks out a conn (measuring `queue_us`), runs the SQL through the **new guarded `Checkout::query`** (measuring `exec_us`), maps PG `Row`/OIDs → `TypedValue`, **buffers `{cols, rows, affected, stats}`**, releases the conn (RAII `Drop`), and declares the terminal `Outcome::Ok(body)` via the S3 Responder — the supervisor sends the single terminal END. **No DATA channel, no credit windows, no session cap in M0** (Decision D-S5-1). The `?`→`$n` scanner (decision M-1) is a bounded-fingerprint-cached, literal/comment/dollar-quote/jsonb-operator-aware normalization in `ferro-backend-pg`.

**Tech Stack:** ferro-proto (new SQL messages via a **bespoke Value-splicing codec**, `/proto` methods.sql), ferrod (services/sql, pool wiring), ferro-pool (guarded row-returning `Checkout::query` + `PoolBackend::query`; gains a `ferro-proto` dep), ferro-backend-pg (OID-strict typed rows + placeholder + error map), Docker PG (live tests). PHP client gets the SQL message vectors byte-matched (the client runtime is S7).

## Decisions (this slice)

- **D-S5-1 — Defer the windowed streaming DATA-channel producer to post-M0 (charter rule 5).** M0's EXEC **buffers** the full result in the guarded pool query entry, releases the conn, and returns `{cols, rows, affected, stats}` in the **single `Outcome::Ok` terminal frame** on the existing S3 control path. `fetch:stream` → `Unsupported`. A result whose encoded terminal body would exceed `MAX_FRAME_PAYLOAD` → `NonRetryable{Unsupported}` ("M0 results must fit one frame; windowed streaming is post-M0"). *Rationale:* a windowed streaming producer (per-request credit wakeup, per-session cap accounting/release, cross-channel terminal ordering, HEAD/DATA framing, a new streaming handler seam) is a large-result **throughput/large-payload** feature; building it before the D12 bench (§16.1) demands it is exactly the speculative optimization charter rule 5 forbids. Buffering keeps `exec_us` honest (times only the DB query), keeps the conn held only for the query (no slow-consumer conn leak), and reuses S3's already-proven exactly-one-END terminal path. Memory stays bounded (each result ≤ one frame; total ≤ `max_inflight × sessions × MAX_FRAME_PAYLOAD`). **Recorded in SPEC §22.** Streaming lands post-M0 iff the bench shows large results matter.

## Global Constraints

- **`/proto` is the source of truth:** add `[methods.sql] EXEC=1` to the registry; regenerate the lock + both codecs' constants + golden vectors in the same change (charter rule 2). `[methods.sql] EXEC=1` is a new entry in the existing `methods: BTreeMap<String,BTreeMap<String,u16>>` — verified **no build.rs/registry-struct change** is needed (unlike the earlier `outcome` top-level-field bootstrap). No hand-written SQL method/OID/error numbers in Rust or PHP.
- **Bespoke Value-splicing codec (BLOCKER-1 fix):** `ExecRequest` and the terminal `Outcome::Ok` body carry `Value`s (`params`, `rows`, `last_insert_id`). `Value` derives only `Debug/Clone/PartialEq` (no `Serialize/Deserialize`; `F64` forbids `Eq`), and the `msg!` macro forces `Eq/Serialize/Deserialize` — so **Value-bearing messages MUST NOT go through `msg!`/rmp-serde**. Give them a hand-written positional codec that splices `Value::encode`/`Value::decode` per element (mirroring how `Outcome::Ok` splices raw body bytes). rmp-serde's derived enum shape would also diverge from PHP's hand-written `[tag,payload]` codec and break the byte-match. Non-`Value` scalar fields may use rmp helpers directly. Golden vectors are generated from this bespoke codec and byte-matched in PHP.
- **M0 scalar TypedValue set only** (decision T-1): `NULL, BOOL, I64, F64, TEXT, BYTES`. A PG column whose OID maps outside this set → `NonRetryable{Unsupported}` (a loud typed error, never a silent miscast). DECIMAL/TIMESTAMP/UUID/JSON/etc. are reserved but unimplemented in S5.
- **OID-strict row extraction (MAJOR-8 fix):** tokio-postgres `FromSql` is OID-strict (`try_get::<i64>` accepts only INT8). `SELECT 1` returns **INT4**, so extraction MUST be driven off the **actual column OID**: int2→`i16`, int4→`i32`, int8→`i64`, float4→`f32`, float8→`f64`, bool→`bool`, text/varchar/bpchar→`String`, bytea→`Vec<u8>`; each via `try_get::<usize, Option<T>>` (NULL → `Value::Null`, never `WasNull`), then widen into the canonical `Value` (`i16/i32/i64`→`Value::I64`, `f32/f64`→`Value::F64`). `ColMeta.tag` comes from the OID→tag map; the row decoder retains the raw OID.
- **Guarded row-returning pool entry (BLOCKER-2 fix):** the only S4 guarded entry `Checkout::exec` runs `simple_query`→`batch_execute` (no rows, no ColMeta, hardcoded `Ok(0)`). Add a **new** `PoolBackend::query(conn, sql, &[Value]) -> QueryResult{cols, rows, affected}` and a **new guarded `Checkout::query`** that calls `pin::is_bare_tx_control` **FIRST** (→ `PoolError::Unsupported`, same as `exec`) then delegates. The handler MUST use `Checkout::query`; it MUST NOT touch `conn_mut().client` for user SQL (that raw path bypasses the pin guard → an `EXEC BEGIN` opens a tx without `tx_open`, and the next tenant inherits it — cross-tenant leak; charter rule 6). `affected` comes from `client.execute()`/`RowStream::rows_affected()`, never `batch_execute`'s `0`. `ferro-pool` gains a `ferro-proto` dependency for `Value`/`ColMeta` (acyclic: ferro-proto has no pool dep).
- **`queue_us` vs `exec_us` first-class** (SPEC §6/§16): `queue_us` = pool wait (checkout); `exec_us` = the buffered `Checkout::query` duration (checkout→result buffered). Because the result is buffered inside the query entry and the conn is released **before** any framing, `exec_us` trivially **excludes** client-side send time — a slow consumer cannot inflate it (MAJOR-7 dissolved by D-S5-1). Assert this.
- **No transparent retry + Indeterminate (charter rule 3 / §19.3, MAJOR-9/10 fix):** a dead conn mid-EXEC → the pool evicts + surfaces `ConnectionLost`; the handler NEVER re-runs the statement. The **service** then layers the wire branch on the client-declared `readonly` flag (no read/write *inference* — charter rule 6): `readonly=true` → wire `ConnectionLost{Retryable}`; `readonly=false` (a possibly-applied write whose fate is unknown) → wire **`WriteUnconfirmed{Indeterminate}`** (code `0x2001`, already in `errors.toml`). This is the service layering Indeterminate on top of the pool's coarse 2-branch taxonomy, exactly as `ferro-pool/error.rs`'s doc mandates. Backend classification branches on **`as_db_error()` FIRST** (MAJOR-9): `None`, or severity `FATAL`/`PANIC` → `ConnectionLost`; only a present non-fatal `DbError` goes to the SQLSTATE table (else `NonRetryable`) — reuse `conn.rs`'s `is_session_fatal` pattern.
- **`?`→`$n` engine-side scanner** (decision M-1, MAJOR-11 fix): mechanical parameter-syntax normalization, literal/line-comment/block-comment/dollar-quote aware. **A bare jsonb `?` cannot be distinguished from a placeholder `?` by a scanner** — adopt the PDO/Doctrine convention: **`??` is an escaped literal `?`** (the jsonb existence operator), a single unescaped `?` is a placeholder `$n`; `?|`/`?&` are kept via one-char lookahead. Bounded fingerprint cache (LRU/fixed cap — MINOR-13; unbounded growth in a long-lived per-host daemon otherwise), paid once per distinct SQL, off the p99 path. Rule-6 tension accepted (recorded in §22). Clean fallback documented: client emits native `$n`.
- **Terminal body size bound (D-S5-1):** the encoded `Outcome::Ok` body MUST fit `MAX_FRAME_PAYLOAD`; the service checks the encoded length and returns `NonRetryable{Unsupported}` if exceeded (honest M0 bound; no unbounded buffer).
- **Reject as `Unsupported`:** `query_id` (manifest is M3), tx-control SQL via EXEC (the guarded `Checkout::query`/`exec` reject it), `fetch:stream` (D-S5-1), out-of-M0 param/column types, unknown pool name.
- **Exactly-one-END preserved:** the EXEC handler declares its single outcome via the S3 Responder; the supervisor sends the one terminal on the control path. **No DATA frames in M0** — so no cross-channel ordering hazard and no new one-END surface (reuses S3's proven path).
- **Charter gates** + live PG integration (skip without `FERRO_TEST_PG_URL`) + PHP byte-match of the new SQL vectors.

## File Structure

```
/proto/methods.toml           + [methods.sql] EXEC=1 (regen lock + consts + gen-php)
/proto/vectors/               + sql_exec_request_select1, sql_exec_request_params,
                                sql_exec_response_select1 (terminal Ok body: cols+rows),
                                sql_exec_response_none (fetch:none affected),
                                sql_exec_response_typedvalue (shared TypedValue arbiter, S1 deferral)
/engine/crates/ferro-proto/src/messages/sql.rs   ExecRequest + ExecOk (terminal body) + ColMeta + Stats
                                (BESPOKE Value-splicing encode/decode — NOT msg!)
/engine/crates/ferro-pool/
  Cargo.toml     + ferro-proto dep
  src/backend.rs + PoolBackend::query(conn, sql, &[Value]) -> QueryResult; + QueryResult{cols,rows,affected}
  src/pool.rs    + guarded Checkout::query (is_bare_tx_control FIRST, then backend.query); FakeBackend query
/engine/crates/ferro-backend-pg/src/
  query.rs       impl PoolBackend::query: bind params, run client.query (rows) / client.execute (none),
                 OID-strict extraction -> Vec<Vec<Value>> + Vec<ColMeta>, affected
  rowmap.rs      PG OID -> canonical tag (M0 scalar set; else Unsupported) + OID -> extraction type
  bind.rs        canonical Value params -> tokio_postgres ToSql
  placeholder.rs ?->$n bounded-fingerprint-cached scanner (?? escape; literal/comment/dollar-quote aware)
  error_map.rs   as_db_error()-FIRST: None/Fatal/Panic -> ConnectionLost; else SQLSTATE -> proto code+branch
/engine/crates/ferrod/src/
  pools.rs       build Pool<PgBackend> per config pool name at startup; PoolRegistry the handler resolves
  services/sql.rs  the EXEC handler: decode ExecRequest -> resolve pool -> checkout(queue_us) ->
                 Checkout::query(exec_us) -> conn released -> buffer -> size-check -> terminal Outcome::Ok
                 (or mapped Outcome::Error, incl. readonly->Indeterminate override)
/php/client/... (S7 owns the runtime) — S5 only adds the SQL message vectors + PHP encode/decode byte-match
```

---

### Task 1: `/proto` SQL method + `ferro-proto` EXEC messages (bespoke Value codec) + golden vectors + PHP byte-match

- Add `[methods.sql] EXEC = 1` to `methods.toml`; regenerate `registry.lock.json` + Rust consts (build.rs) + PHP `Constants.php` (gen-php) — zero-diff regen check. Confirm `method_sql::EXEC` + PHP `METHOD_SQL_EXEC` emit.
- `ferro-proto/src/messages/sql.rs` — **bespoke positional codec (NOT `msg!`)**:
  - `ExecRequest { pool: String, sql: Option<String>, query_id: Option<String>, params: Vec<Value>, timeout_ms: Option<u32>, readonly: bool, fetch: u8 /* 0=rows,1=none,2=stream(reserved) */ }` with hand-written `encode`/`decode`: a fixarray of fields in declaration order; scalar/`Option`/`String` fields via `rmp` helpers, `params` as a fixarray of `Value::encode`/`Value::decode` elements. Reject trailing bytes (mirror `messages.rs::from_slice`).
  - `ColMeta { name: String, tag: u8 }` and `Stats { queue_us: u64, exec_us: u64, rows: u64, bytes: u64 }` — these are `Value`-free, so they MAY use `msg!`.
  - `ExecOk { cols: Vec<ColMeta>, rows: Vec<Vec<Value>>, affected: u64, last_insert_id: Option<Value>, stats: Stats }` — the **terminal `Outcome::Ok` body**; bespoke `encode`/`decode` splicing `ColMeta` (msg!) + `Value` cells. `Outcome::Ok(ExecOk.encode())` composes because `Outcome::Ok` splices raw body bytes.
- Golden vectors (Rust-generated): `sql_exec_request_select1`, `sql_exec_request_params` (round-trips the scalar set incl. divergent-range ints), `sql_exec_response_select1` (terminal body cols+rows), `sql_exec_response_none` (fetch:none affected). Rust `encode==bytes` + PHP `Message`/decode byte-match both directions (extend the S1 conformance harness with the SQL messages).
- **TDD:** unit — `ExecRequest`/`ExecOk` round-trip incl. `Value` cells in the divergent integer range (`I64(200)` = `cc c8`, `I64(-200)` = `d1 ff 38`); trailing-byte rejection; the `msg!`-vs-`Value` split compiles.
- **Gate:** `cargo test -p ferro-proto` + regen-zero-diff + PHP conformance green.
- **Commit** `feat(s5): /proto methods.sql EXEC + ferro-proto ExecRequest/ExecOk (bespoke Value codec) + SQL vectors + PHP byte-match`.

---

### Task 2: guarded row-returning pool entry + `ferro-backend-pg` typed exec (OID-strict, params, `?`→`$n`, error map)

- **ferro-pool (BLOCKER-2):** add `ferro-proto` to `Cargo.toml`. In `backend.rs`: `QueryResult { cols: Vec<ColMeta>, rows: Vec<Vec<Value>>, affected: u64 }` and `async fn query(&self, conn: &mut Self::Conn, sql: &str, params: &[Value]) -> Result<QueryResult, PoolError>`. In `pool.rs`: a **guarded** `Checkout::query(&mut self, sql, params)` that runs `pin::is_bare_tx_control(sql)` FIRST (→ `PoolError::Unsupported`, mirroring `exec`) then delegates to `backend.query`. `FakeBackend` implements `query` (canned rows) so the guarded path is unit-tested without Docker.
- **ferro-backend-pg:**
  - `rowmap.rs`: PG OID → canonical tag for the M0 scalar set (bool→BOOL, int2/4/8→I64, float4/8→F64, text/varchar/bpchar→TEXT, bytea→BYTES; unknown → `Unsupported`) **and** OID → extraction type. `bind.rs`: canonical `Value` params → tokio-postgres `&(dyn ToSql + Sync)`.
  - `query.rs`: impl `PoolBackend::query` — run `placeholder`-normalized SQL via `client.query` (rows) or `client.execute` (fetch:none → affected, no rows); **OID-strict extraction** (`try_get::<usize, Option<T>>` per the actual column OID → widen to `Value`; unknown OID → `PoolError` mapping to `Unsupported`); build `Vec<ColMeta>` from `row.columns()` OIDs; `affected` from `execute()`/`rows_affected()`.
  - `placeholder.rs`: the **bounded**-fingerprint-cached `?`→`$n` scanner (skip `'...'`/`"..."`/`--`/`/* */`/`$tag$...$tag$`; `??`→literal `?`; keep `?|`/`?&`; do NOT rewrite jsonb `?`). A dedicated hazard-case unit corpus: jsonb `?`/`?|`/`?&`, `??` escape, `::` casts, tagged dollar-quoted bodies (`$func$...?...$func$`), `$1` vs `$tag$`, quoted `?`, comments.
  - `error_map.rs`: **`as_db_error()`-FIRST** (MAJOR-9) — `None` or severity `FATAL`/`PANIC` → `ConnectionLost`; a present non-fatal `DbError` → SQLSTATE table (23505 Unique, 40001 SerializationFailure, 40P01 Deadlock, 42xxx Syntax, 08xxx ConnectionLost, 57014 Cancelled, else NonRetryable) → proto `code`+`branch`, preserving raw SQLSTATE. Reuse `conn.rs`'s `is_session_fatal`.
- **TDD:** unit — placeholder hazard corpus (incl. `??`/tagged dollar-quotes/`$1`); OID→tag + extraction-type table incl. `Unsupported`; error_map `as_db_error()`-None→ConnectionLost, Fatal-severity→ConnectionLost, 42601→Syntax, 23505→Unique; the guarded `Checkout::query` rejects bare tx-control (→ Unsupported) via `FakeBackend`. Live (skip without env) — `query("SELECT 1", [])` → cols=[{name,I64}], rows=[[I64(1)]] (**proves OID-strict INT4 extraction**); params round-trip the scalar set; `query("SELECT $bad")` → Syntax; an out-of-M0 type column → `Unsupported`.
- **Gate + commit** `feat(s5): guarded row-returning Checkout::query + ferro-backend-pg typed exec (OID-strict, ?->$n, as_db_error-first map)`.

---

### Task 3: daemon pool wiring + the EXEC handler (checkout → query → buffer → single terminal END)

- `ferrod/src/pools.rs`: at startup build a `Pool<PgBackend>` per configured pool (config gains a `[[pool]] name, dsn` list; DSNs from env/secret refs per §12 — the client never sees them); a `PoolRegistry { by_name: HashMap<String, Pool<PgBackend>> }` the handler resolves. ferrod gains deps on `ferro-pool` + `ferro-backend-pg`. Build the registry **after** the tokio runtime is up (the pool's reaper `tokio::spawn`s — the S4 reaper max_size fix is in place).
- `services/sql.rs`: the real EXEC handler (replaces S3's `Unsupported` stub for `service=SQL, method=EXEC`):
  1. decode `ExecRequest`; reject `query_id` / unknown pool / `fetch:stream` (Unsupported).
  2. resolve the pool; `checkout()` → record `queue_us` from `CheckoutStats`.
  3. run `Checkout::query(sql, params)` (fetch:rows) or the fetch:none path (affected only) → record `exec_us`; **the `Checkout` is dropped here** (RAII → conn returns to the pool) **before** framing.
  4. build `ExecOk{cols, rows, affected, last_insert_id, stats}`; encode; **if the encoded body > `MAX_FRAME_PAYLOAD` → `Outcome::Error(Unsupported)`** ("result exceeds one frame; streaming is post-M0").
  5. declare the terminal `Outcome::Ok(body)` via the Responder (the supervisor sends the single END).
  - Error mapping: a `PoolError` → wire `ErrorPayload`; **`ConnectionLost` + `readonly=false` → `WriteUnconfirmed{Indeterminate}`**, `ConnectionLost` + `readonly=true` → `ConnectionLost{Retryable}`, else the mapped `NonRetryable{...}` code. **No retry, ever.**
- **TDD (live, skip without env):**
  - `exec_select1_shape` — EXEC "SELECT 1" over a real client→ferrod→pool→PG round trip returns cols=[{name,I64}]/row[I64(1)] with populated `queue_us`+`exec_us`, terminating in exactly **ONE** END (the S3 invariant holds).
  - `exec_fetch_none_affected` — INSERT with fetch:none → affected>0, empty rows.
  - `exec_syntax_error` — a syntax error → terminal `Outcome::Error` NonRetryable{Syntax}, conn still usable after.
  - `exec_write_loss_indeterminate` — a non-readonly EXEC whose conn is killed mid-flight → `WriteUnconfirmed{Indeterminate}` (NOT Retryable); a readonly one → `ConnectionLost{Retryable}`; **neither re-runs the statement**.
  - `unsupported_query_id_pool_stream` — `query_id` / unknown pool / `fetch:stream` → Unsupported.
  - Unit: the one-END invariant holds for the EXEC path (extend S3's session tests); the `MAX_FRAME_PAYLOAD` size-cap → Unsupported (a synthetic oversized `ExecOk`).
- **THE MILESTONE:** capture a live `SELECT 1` traversing client→ferrod→pool→Docker PG→client in the report.
- **Gate + commit** `feat(s5): daemon pool wiring + EXEC handler (checkout->query->buffer->single terminal END) + live SELECT 1`.

---

### Task 4: shared TypedValue golden vector (close the S1 deferral) + placeholder/error hardening

- Add the deferred (S1 roll-up) **shared golden vector carrying a TypedValue** in the divergent integer range — `sql_exec_response_typedvalue`: an `ExecOk` whose rows include `I64(200)`/`I64(-200)` (and a `TEXT`/`BYTES`/`NULL`/`BOOL`/`F64`) — so the integer ladder + `[tag,value]` framing are locked by the **shared cross-language arbiter** (Rust `encode==bytes` + PHP re-encode==bytes), not just independently-typed asserts. This is the bespoke Value-splicing codec's cross-language lock.
- Harden: the placeholder scanner hazard corpus (add any cases Task 2 review surfaces — `??`, tagged dollar-quotes, `$1`, `::`, quoted `?`); the bounded-cache eviction (fill past the cap → LRU evicts, no unbounded growth); the `exec_us`-excludes-send assertion (a deliberately slow terminal read does not inflate `exec_us`, since the conn released before framing); the `readonly` Indeterminate-vs-Retryable split (unit-level with a faked `ConnectionLost`).
- **Gate + commit** `test(s5): shared TypedValue vector locks the integer ladder cross-language + placeholder/error hardening`.

---

## Self-Review (author against the real toolchain)

- **Spec coverage (design S5 gate):** SQL vectors + PHP byte-match → T1; guarded row query + OID-strict exec + placeholder + `as_db_error`-first error map → T2; EXEC handler checkout→query→buffer→single-terminal-END + queue_us/exec_us split + live SELECT 1 + fetch:none + Indeterminate-on-write-loss + Unsupported → T3; the shared-TypedValue vector (S1 deferral) + hardening → T4.
- **Deferred (noted):** **the windowed streaming DATA-channel producer** (per-request credit wakeup, per-session cap accounting/release, cross-channel terminal ordering, HEAD/DATA framing, `fetch:stream`, the streaming handler seam) → **post-M0 per charter rule 5 / D-S5-1 / §22** (lands iff the D12 bench shows large results matter); full type system (DECIMAL/TIMESTAMP/UUID/JSON hydration + §9.1 policies) → M1+; manifest/query_id → M3; the PHP client RUNTIME that issues EXEC and hydrates DTOs → S7 (S5 only vectors the SQL messages); TX service (BEGIN/COMMIT over the pin) → S6.
- **Verify the plan before executing** (S1/S3/S4 pattern): a **focused** adversarial pass on the v2-changed surface — the bespoke `ExecRequest`/`ExecOk` codec (Value-splice correctness + PHP byte-match + trailing-byte rejection), the ferro-pool→ferro-proto layering for `Checkout::query`/`PoolBackend::query`, the OID-strict extraction completeness, the two-layer error mapping (backend `PoolError` → service wire `ErrorPayload` + the readonly→Indeterminate override), and the `MAX_FRAME_PAYLOAD` result-cap path.

## Execution Handoff

Subagent-driven: fresh implementer per task (TDD, gates), review after each (probe the one-END + no-transparent-retry + Indeterminate under EXEC, the queue_us/exec_us accounting, the OID-strict extraction, the placeholder hazards), whole-branch review before S6. Live tests against the S2 Docker PG.

---

## v2 verification-fix log (from `wf_70051761`, NEEDS_WORK)

- **BLOCKER-1** (Value can't go through `msg!`/rmp-serde; would not compile + would break PHP byte-match) → **bespoke Value-splicing codec** for `ExecRequest`/`ExecOk` (Global Constraints + T1).
- **BLOCKER-2** (only guarded entry `Checkout::exec` returns no rows/hardcoded 0; raw `client.query` bypasses the pin guard → cross-tenant open-tx leak) → **new guarded `Checkout::query` + `PoolBackend::query`** (Global Constraints + T2).
- **BLOCKER-3** (credit has no async wakeup → handler hangs forever on backpressure → no END + conn/permit leak) → **DISSOLVED by D-S5-1** (no credit path in M0).
- **BLOCKER-4** (streamed terminal END overtakes trailing DATA on the prioritized control channel) → **DISSOLVED by D-S5-1** (no DATA frames in M0; single terminal on the proven S3 path).
- **MAJOR-5** (HandlerFn seam carries no data-sender/credit/cap; writer data mpsc absent) → **DISSOLVED by D-S5-1** (buffered terminal reuses the existing Responder seam).
- **MAJOR-6** (SessionCap has no owner/release path → monotonic climb wedges the session) → **DISSOLVED by D-S5-1** (no session cap in M0).
- **MAJOR-7** (`exec_us` can't exclude backpressure over a lazy stream) → **fixed by D-S5-1**: buffer in the query entry, release conn before framing → `exec_us` = DB query only; asserted (T4).
- **MAJOR-8** (OID-strict `FromSql`; `SELECT 1` is INT4 → `try_get::<i64>` WrongType fails the headline) → **OID-driven extraction** (`try_get::<Option<T>>` per OID, widen) (Global Constraints + T2).
- **MAJOR-9** (`error_map` keyed purely on SQLSTATE → transport failure/`None`/FATAL misclassified NonRetryable) → **`as_db_error()`-first** classification (Global Constraints + T2).
- **MAJOR-10** (no mechanism to produce Indeterminate; a lost write mislabeled Retryable → §19.3 unmet) → **service layers `WriteUnconfirmed{Indeterminate}` on `readonly=false` `ConnectionLost`** (code already in `errors.toml`); no read/write inference (Global Constraints + T3).
- **MAJOR-11** (bare jsonb `?` indistinguishable from placeholder `?`) → **`??`-escape convention** + `?|`/`?&` lookahead (Global Constraints + T2).
- **MINOR-12** (HEAD credit accounting unspecified) → **DISSOLVED by D-S5-1** (no HEAD/DATA frames).
- **MINOR-13** (unbounded fingerprint cache in a long-lived daemon) → **bounded (LRU/fixed cap)** (Global Constraints + T2/T4).
- **MINOR-14** (`fetch:stream` undefined; buffered-vs-lazy `fetch:rows` undecided) → **`fetch:stream` → Unsupported; `fetch:rows` is buffered `client.query`** (Global Constraints + T1/T3).
