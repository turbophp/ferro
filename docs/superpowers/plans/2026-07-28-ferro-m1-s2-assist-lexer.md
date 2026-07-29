# Ferro M1 · Slice S2 — Assist lexer (`ferro-classify`) + `pin_functions` escape hatch Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.
> **v2** — adversarial plan-verification (`wf_990853d6-45d`, FIX_FIRST) folded: 2 blockers + 6 majors + 3 minors. Architecture confirmed sound (RFQ stays tx authority, `apply_classify` only taints, PinCause additive, leaf-crate wiring correct, daemon needs no change). Fixes: quoted-identifiers are CODE not literals; `SET LOCAL` needs exact-token (not second-word) detection; nested block comments; panic-safe UTF-8 scanner; temp = any temp object + `INTO TEMP`; `exec` multi-statement scan; false-green live tests fixed (max_size=1 + pid assert + independent-vantage lock check); env parsing via injected lookup (no `set_var` under edition-2024 `forbid`); `Dialect: Default`. Task 1 split into T1a (scanner) + T1b (rules).

**Goal:** Add the **assist signal** to the M1 pin engine: a new `ferro-classify` crate — a dialect-aware keyword *classifier* (NOT a SQL parser) — that flags statements which mutate protocol-invisible **session** state (`LISTEN`/`UNLISTEN`, non-`_xact` advisory locks, raw `PREPARE`/`EXECUTE`/`DEALLOCATE`, temp-object DDL, non-local `SET`, SQLite `ATTACH`/state `PRAGMA`, plus a per-pool `pin_functions` escape hatch and `pin_on_unknown` conservatism), wired into `Checkout` so such a statement **taints** the connection (→ hygiene at next checkout) and sets a pin-cause label — even though the RFQ protocol byte (S1) stays `Idle`. RFQ remains the transaction authority; the lexer is assist-only (SPEC §7.1).

**Architecture:** `ferro-classify` is a leaf crate (no `ferro-pool` dep). It owns `Dialect` + `PinTrigger` + `classify(sql, dialect, pin_functions, pin_on_unknown) -> Option<PinTrigger>`, built on its OWN literal/comment/dollar-quote-aware, **panic-safe, multi-statement-aware** scanner (the `ferro-pool` lexer helpers are `pub(crate)` and unreachable). `ferro-pool` depends on `ferro-classify`: `PoolBackend` gains a synchronous `dialect()`; `PinCause` gains the 7 assist variants + `From<PinTrigger>`; `PoolConfig` gains `pin_functions`/`pin_on_unknown`; and `Checkout::{exec,query}` call a new private `apply_classify(sql)` that sets `tainted=true` + `last_pin_cause` when `classify` returns a trigger. The daemon needs no code change (both live call sites already funnel through `Checkout::query`); `ferrod` config gains per-pool plumbing for the escape hatch via an **injected lookup closure** (env is read only in `from_env`, never mutated in tests).

**Tech Stack:** Rust (edition 2024, tokio); new `engine/crates/ferro-classify` (leaf crate, std-only — no regex/once_cell); live PG via `testkit`.

## Global Constraints (verbatim from SPEC §7.1–7.2 / the M1 exec-design / the mechanism map + verification — every task assumes these)

- **§7.1 — protocol signals are the AUTHORITY; the lexer is ASSIST.** The RFQ byte (S1) decides transaction pin state; `ferro-classify` only adds session-state taint + a cause label. The classifier NEVER overrides an RFQ decision — `apply_classify` only *adds* `tainted`/`last_pin_cause`, NEVER touches `pin`/`tx_open`. It is a keyword classifier, **NOT a SQL parser**.
- **The assist trigger set (SPEC §7.1) → `PinCause` variants (M1 exec-design S2 line 69):** `LISTEN`/`UNLISTEN` → `Listen`; session advisory-lock functions (`pg_advisory_lock` family **without** `_xact`) → `AdvisoryLock`; raw client `PREPARE`/`EXECUTE`/`DEALLOCATE` → `Prepare`; temp-object DDL → `Temp`; non-local `SET` → `Set`; SQLite `ATTACH`/state `PRAGMA` → `Set`; a statement referencing a `pin_functions` entry → `PinFunction`; unknown/unclassifiable → `Unknown`. Final `PinCause` = `{ Tx, Listen, AdvisoryLock, Prepare, Temp, Set, PinFunction, Unknown }`.
- **`pin_on_unknown = true` default** (SPEC §7.1). Unknown → pin conservatively. **Over-pinning is correctness-safe (an extra hygiene reset), never a leak — prefer a false taint to a missed one (charter rule 5).** This principle decides every ambiguous case below in the *taint* direction.
- **Scanner literal/code regions (verification #7, the safety direction is load-bearing):** treat as NON-code (skip when matching keywords/functions) ONLY: `'...'` string literals (incl. `''` escape and PG `E'...'` backslash-escape), `--` line comments, `/* */` block comments (**NESTED — depth-counted**), and `$tag$...$tag$` dollar-quoted bodies (tag = optional `[A-Za-z_][A-Za-z0-9_]*`; `$1` is a positional param, NOT a dollar-quote opener). Treat `"..."` quoted identifiers as **CODE** — a quoted identifier is executable (`SELECT "pg_advisory_lock"(1)` is a real call); skipping it would MISS a trigger (a leak). An unterminated string/comment/dollar-quote → the statement is malformed (won't execute) but classify still returns `Some(Unknown)` on doubt when `pin_on_unknown` (conservative).
- **PORT the proven scanner, do not reinvent it.** `engine/crates/ferro-backend-pg/src/placeholder.rs` ALREADY implements exactly this region-tracking machinery (its `scan()` is literal/comment/dollar-quote/quoted-ident-aware: nested block-comment **depth counter** at `placeholder.rs:96-105`, `''`/`E'...'` string escapes, `"..."` with `""` escape, `$tag$…$tag$`, and `$1` positional-param handling — for a `?`→`$n` rewrite). `ferro-classify` is a LEAF crate and CANNOT import it (that would cycle: `ferro-backend-pg → ferro-pool → ferro-classify`), so port/adapt that proven region-scanning logic into `scan.rs` (read `placeholder.rs` first) rather than writing a new state machine from scratch. Its test corpus (`placeholder.rs` `#[cfg(test)]`) is a template for the T1a hostile corpus.
- **`SET LOCAL`/`SET TRANSACTION` exclusion by EXACT TOKEN, not second alphabetic word (verification #2, a real leak).** After `SET`, skip comments/whitespace and require the next TOKEN to be EXACTLY `LOCAL` or `TRANSACTION` (the following char is not an identifier char and not `.`) to exclude. `SET local.foo = 'x'` (a dotted session GUC) and `SET/* LOCAL */x = 1` MUST classify as `Set` (they persist). Only true `SET LOCAL x`/`SET TRANSACTION …` are excluded.
- **`_xact` advisory locks do NOT pin; unlock does NOT pin.** Session set that pins: `pg_advisory_lock`, `pg_advisory_lock_shared`, `pg_try_advisory_lock`, `pg_try_advisory_lock_shared`. Excluded (no pin): every `pg_advisory_xact_*` (tx-scoped — covered by RFQ) and every `pg_advisory_unlock*` (releasing is safe).
- **`classify()` is TOTAL — it MUST NEVER panic** (verification #10; it runs on every `exec`/`query`, the hot path). Scan by `char_indices()`/checked slicing (`str::get`/`is_char_boundary`); never slice at a non-char-boundary; multibyte SQL (`SELECT 'café'`) and empty input are safe; unterminated regions consume cleanly to EOF.
- **`classify()` is MULTI-STATEMENT-AWARE** (verification #11 — the S1 exec-batch leak shape). It splits the input into top-level `;`-delimited statements (literal/comment-aware split) and classifies each; if ANY statement is a trigger it taints (deterministic precedence below). This covers `Checkout::exec`'s `batch_execute` path (`"SELECT 1; LISTEN c"` must taint on the `LISTEN`), not just the leading keyword.
- **The hygiene consumer already exists (S1); S2 only produces the taint.** The pool's checkout recycle (`pool.rs` ~130-149) runs `if tainted { backend.reset(conn) }`, and `PgBackend::reset()` is `DISCARD ALL` (verified `conn.rs:135`) — resets `search_path`, prepared statements, temp objects, advisory locks. So S2 setting `tainted` is sufficient to close the leak against the current reset; S3 later makes it conditional/targeted (an optimization). Do NOT add/change hygiene/`reset()` in S2.
- **Charter DoD — pin-cause assertion.** Every assist trigger class gets a `last_pin_cause()` assertion test.
- **Charter rule 2 — `/proto` is the single source of truth.** S2 adds NO wire/proto change. Do NOT touch `/proto`.
- **`ferro-classify` is a LEAF crate** — no `ferro-pool` dep (arrow: `ferro-pool → ferro-classify`). Joins the workspace via the `members = ["engine/crates/*", …]` glob. No `[workspace.dependencies]` — pin versions per-crate, mirroring `ferro-pool/Cargo.toml`. Workspace lints apply (`unsafe_code = "forbid"`, clippy `all = deny`) — the crate needs `[lints] workspace = true` and must contain NO `unsafe`.
- **No `std::env::set_var`/`remove_var` in tests** (verification #1 — they are `unsafe fn` under edition 2024, blocked by `forbid`; and no existing env-isolation test pattern exists). Config parsing takes an injected lookup closure; tests pass a map-backed closure.
- **Do NOT change `is_bare_tx_control`/`leading_tx_verb`** in `ferro-pool/src/pin.rs`. `ferro-classify` implements its own scanner (accepted duplication — different jobs).
- **Charter gates** (`cargo fmt --check`, `clippy --workspace --all-targets -D warnings`, `cargo test --workspace`, `cargo-deny check bans`) green; live PG tests skip without `FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro`.

## File Structure

```
engine/crates/ferro-classify/                 NEW leaf crate (SPEC §20.1)
  Cargo.toml                                   mirror ferro-pool's manifest; std-only deps; [lints] workspace = true
  src/lib.rs                                   pub Dialect (derives Default, #[default] Postgres), pub PinTrigger, pub fn classify(...)
  src/scan.rs                                  literal/comment(NESTED)/dollar-quote-aware, panic-safe, multi-statement scanner + helpers
  src/rules.rs                                 per-Dialect classify_one rules (Postgres full; MySql/Sqlite stubs, only PG wired in S2)
engine/crates/ferro-pool/src/
  backend.rs                                   + `fn dialect(&self) -> ferro_classify::Dialect;` on PoolBackend (sync, like is_closed)
  pin.rs                                       + PinCause::{Listen,AdvisoryLock,Prepare,Temp,Set,PinFunction,Unknown} + From<ferro_classify::PinTrigger>
  config.rs                                    + PoolConfig { pin_functions: Vec<String>, pin_on_unknown: bool (default true) }
  pool.rs                                      + Checkout::apply_classify(&mut self, sql) called from exec()+query()
  fake.rs                                      FakeBackend gains a dialect field (default Postgres via Dialect: Default) + dialect()
engine/crates/ferro-backend-pg/src/
  conn.rs                                      PgBackend::dialect() -> Dialect::Postgres
engine/crates/ferro-pool/Cargo.toml            + ferro-classify path dep
engine/crates/ferrod/src/
  config.rs                                    PoolSpec + pin_functions/pin_on_unknown; parse via injected lookup closure (testable, no env mutation)
  pools.rs                                     daemon_pool_config(&PoolSpec) sets the two new PoolConfig fields per pool
```

---

### Task 1a: `ferro-classify` — the literal/comment/dollar-quote-aware, panic-safe scanner (`scan.rs`)

**Files:** Create `engine/crates/ferro-classify/{Cargo.toml,src/lib.rs (stub),src/scan.rs}`. (`lib.rs` starts as `mod scan;` + minimal re-exports so the crate compiles; the public API lands in T1b.)

**Interfaces produced (all `pub(crate)`, consumed by `rules.rs` in T1b):**
- `fn strip_leading_noise(sql: &str) -> &str` — skip leading whitespace + `--` line comments + `/* */` **nested, depth-counted** block comments (loop). Unterminated block comment → returns `""` (nothing left to classify).
- `fn leading_keyword(sql: &str) -> Option<String>` — after `strip_leading_noise`, the first maximal ASCII-alphabetic run, UPPERCASED.
- `fn next_token_after_keyword(sql: &str) -> Option<String>` — after the leading keyword, skip whitespace/comments, return the next maximal ASCII-alphabetic run UPPERCASED **only if it is a complete token** (the char immediately after the run is NOT an identifier char `[A-Za-z0-9_]` and NOT `.`); if the next token is dotted/underscored/ident-continued (e.g. `local.foo`, `local_x`) return `None` (so `SET local.foo` does NOT read `LOCAL`). Used for the `SET LOCAL`/`SET TRANSACTION` exact-token exclusion.
- `fn contains_identifier_ci(sql: &str, ident: &str) -> bool` — true if `ident` (ASCII, case-insensitive) appears as a WHOLE identifier (word boundaries: neighbor chars not `[A-Za-z0-9_]`) inside a **CODE** region. Code = everything EXCEPT `'...'`/`E'...'` strings, `--`/`/* */`(nested) comments, and `$tag$...$tag$` bodies. **`"..."` quoted identifiers ARE code** (matched). A leading schema qualifier is fine (`pg_catalog.pg_advisory_lock` still matches the bare `pg_advisory_lock`).
- `fn split_top_level_statements(sql: &str) -> Vec<&str>` — split on `;` that is in a CODE region (not inside any string/comment/dollar-quote); trims empties. (For the `exec` batch path.)
- **The scanner is a single `char_indices()` state machine** tracking the current region (Code / SingleQuote{e_string} / LineComment / BlockComment{depth} / DollarQuote{tag}). Dollar-quote open: on `$`, read optional tag `[A-Za-z_][A-Za-z0-9_]*` then require `$`; if it doesn't match (e.g. `$1`, `$` at EOF), stay in Code (ordinary `$`). E-string: an opening `'` preceded (token-adjacently) by `E`/`e` enables `\'` as an escaped quote; standard `''` escape applies in all single-quoted strings. **Never slice at a non-char-boundary; total function (no panic) on any `&str` incl. empty/multibyte/unterminated.**
- **TDD (a dedicated hostile corpus — this is the safety crux, reviewed in isolation):** in `scan.rs` `#[cfg(test)]`, cover at minimum:
  - `contains_identifier_ci` TRUE: `pg_advisory_lock(1)`; `PG_ADVISORY_LOCK(1)` (ci); `pg_catalog.pg_advisory_lock(1)`; `SELECT "pg_advisory_lock"(1)` (quoted ident = code, MUST match); `SELECT/* c */pg_advisory_lock(1)`.
  - `contains_identifier_ci` FALSE: `SELECT 'pg_advisory_lock'` (string); `-- pg_advisory_lock` (line comment); `/* pg_advisory_lock */ SELECT 1` (block comment); `$$ pg_advisory_lock $$` (dollar-quote); `my_pg_advisory_lock` / `pg_advisory_lockx` (not whole ident); `SELECT 'it''s pg_advisory_lock'` (`''`-escaped string still a string); `E'a\' pg_advisory_lock'` (E-string, `\'` does not close).
  - Nested/adjacent: `/* /* */ pg_advisory_lock */ SELECT 1` → FALSE (nested comment; the name is inside the comment); `SELECT 1 -- x` then newline `pg_advisory_lock(1)` on the next line → TRUE.
  - Dollar-quote edge: `$a1$ pg_advisory_lock $a1$` → FALSE (digit-in-tag body is a comment-like literal); `$1 + pg_advisory_lock(1)` → TRUE (`$1` is a positional param, not a dollar-quote, so the name after is code); mismatched `$a$ x $b$ pg_advisory_lock $a$` → FALSE (all inside the `$a$…$a$` body).
  - `leading_keyword` through leading comments/whitespace; `next_token_after_keyword`: `SET LOCAL x` → `Some("LOCAL")`; `SET local.foo` → `None`; `SET local_x` → `None`; `SET/* LOCAL */x` → `None` (the token after SET, skipping the comment, is `x`) — wait: this must yield the token AFTER skipping the comment, which is `X`, so `!= LOCAL` → not excluded (correct). `SET  LOCAL  y` → `Some("LOCAL")`.
  - `split_top_level_statements`: `SELECT 1; LISTEN c` → 2; `SELECT ';'` → 1 (semicolon in string); `SELECT 1 /* ; */ ; SELECT 2` → 2 (the commented `;` is not a split).
  - Panic-safety: `classify`-facing helpers on `""`, `"   "`, `"SELECT 'café'"`, unterminated `"SELECT '"`, unterminated `"/* x"`, unterminated `"$$ x"` — all return without panic.
- **Gate:** `cargo test -p ferro-classify` (scanner corpus); `cargo build --workspace`; fmt/clippy clean; no `unsafe`.
- **Commit** `feat(m1-s2): ferro-classify scanner — literal/nested-comment/dollar-quote-aware, panic-safe, multi-statement`.

---

### Task 1b: `ferro-classify` — `Dialect`, `PinTrigger`, and the `classify()` rules (`rules.rs` + `lib.rs`)

**Files:** Modify `engine/crates/ferro-classify/src/{lib.rs,rules.rs}`.

**Interfaces produced:**
- `pub enum Dialect { Postgres, MySql, Sqlite }` — `#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]` with `#[default] Postgres` (the `Default` derive is required — `FakeBackend` derives `Default`; verification #3).
- `pub enum PinTrigger { Listen, AdvisoryLock, Prepare, Temp, Set, PinFunction, Unknown }` — `#[derive(Debug, Clone, Copy, PartialEq, Eq)]`.
- `pub fn classify(sql: &str, dialect: Dialect, pin_functions: &[String], pin_on_unknown: bool) -> Option<PinTrigger>` — TOTAL (never panics). Splits `sql` into top-level statements (`scan::split_top_level_statements`) and classifies each via a per-dialect `classify_one`; returns the highest-precedence trigger found across statements, or `None` if all are safe. Precedence when multiple statements trigger: `PinFunction > Listen > Prepare > Set > Temp > AdvisoryLock > Unknown` (deterministic; any real trigger beats `Unknown`).

- `classify_one_pg(stmt, pin_functions, pin_on_unknown) -> Option<PinTrigger>` (first match wins):
  1. **`pin_functions` first:** any entry `scan::contains_identifier_ci(stmt, fn)` → `Some(PinFunction)`.
  2. leading keyword `LISTEN`/`UNLISTEN` → `Some(Listen)`.
  3. leading `PREPARE`/`EXECUTE`/`DEALLOCATE` → `Some(Prepare)`.
  4. leading `SET`: if `next_token_after_keyword(stmt) == Some("LOCAL")` OR `== Some("TRANSACTION")` → fall through (NOT a trigger); else → `Some(Set)`.
  5. leading `CREATE`: scan the leading keywords (skip `GLOBAL`/`LOCAL`) — if `TEMP` or `TEMPORARY` appears before the object kind → `Some(Temp)` (ANY temp object: `TABLE`/`VIEW`/`SEQUENCE`/`MATERIALIZED VIEW`/…). Plain `CREATE TABLE`/etc. (no TEMP) → fall through (safe).
  6. `SELECT`/`WITH` containing an `INTO TEMP`/`INTO TEMPORARY` (whole-token, code-region) → `Some(Temp)`. (Covers `SELECT … INTO TEMP t`.)
  7. advisory-lock session family via `contains_identifier_ci`: `pg_advisory_lock`, `pg_advisory_lock_shared`, `pg_try_advisory_lock`, `pg_try_advisory_lock_shared` → `Some(AdvisoryLock)`. (NEVER match `pg_advisory_xact_*` or `pg_advisory_unlock*`.)
  8. known-SAFE leading keyword → `None`: `SELECT, INSERT, UPDATE, DELETE, WITH, VALUES, TABLE, SHOW, EXPLAIN, ANALYZE, VACUUM, FETCH, MOVE, CLOSE, COPY, CALL, DO, TRUNCATE, MERGE, CREATE (non-temp), ALTER, DROP, GRANT, REVOKE, COMMENT, REFRESH, REINDEX, CLUSTER, CHECKPOINT, RESET, LOCK, DISCARD`. (Rationale doc: `RESET x`/`DISCARD …` return session state toward DEFAULT — they do not accrue new cross-tenant state, and a fresh conn is at default anyway; `LOCK TABLE` is tx-scoped. All safe.)
  9. else (unrecognized/empty/unclassifiable) → `Some(Unknown)` if `pin_on_unknown` else `None`.
- `classify_one_sqlite`: leading `ATTACH` → `Some(Set)`; leading `PRAGMA` → `Some(Set)` (conservative — treat any pragma as state-changing; documented); else PG-like safe-list/unknown. (Not wired to a live backend in S2.)
- `classify_one_mysql`: leading `SET` unless `SET LOCAL`/`SET TRANSACTION` (and incl. `SET SESSION`/`SET @@session`) → `Some(Set)` (backstop; real MySQL signal is the S6 tracker); else safe-list/unknown. (Not wired in S2.)
- **TDD (the classify corpus, on the reviewed scanner):** in `rules.rs`/`lib.rs` `#[cfg(test)]`, Postgres: `LISTEN c`→Listen; `UNLISTEN *`→Listen; `PREPARE s AS SELECT 1`→Prepare; `EXECUTE s`→Prepare; `DEALLOCATE s`→Prepare; `SET search_path=a,b`→Set; `SET SESSION x=1`→Set; `SET local.foo='x'`→Set (dotted GUC, NOT excluded); `SET LOCAL x=1`→None; `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`→None; `SET/* LOCAL */x=1`→Set; `CREATE TEMP TABLE t(x int)`→Temp; `CREATE TEMPORARY TABLE t(...)`→Temp; `CREATE GLOBAL TEMPORARY TABLE t(...)`→Temp; `CREATE TEMP VIEW v AS SELECT 1`→Temp; `CREATE TEMPORARY SEQUENCE s`→Temp; `SELECT 1 INTO TEMP t`→Temp; `CREATE TABLE t(...)`→None; `SELECT pg_advisory_lock(1)`→AdvisoryLock; `SELECT pg_try_advisory_lock_shared(1)`→AdvisoryLock; `SELECT pg_advisory_xact_lock(1)`→None; `SELECT pg_advisory_xact_lock_shared(1)`→None; `SELECT pg_advisory_unlock(1)`→None; `SELECT pg_advisory_unlock_all()`→None; `SELECT "pg_advisory_lock"(1)`→AdvisoryLock (quoted ident); `SELECT 'pg_advisory_lock'`→None; `-- pg_advisory_lock`+newline+`SELECT 1`→None; `SELECT 1`→None; `INSERT INTO t VALUES(1)`→None; `WITH x AS (SELECT 1) SELECT * FROM x`→None; `MERGE INTO t …`→None; `RESET search_path`→None; `DISCARD ALL`→None; multi-statement `SELECT 1; LISTEN c`→Listen; `SELECT 1; SELECT 2`→None; `FLUFF nonsense` with `pin_on_unknown=true`→Unknown, with `false`→None; `classify("SELECT app_lock(1)", Postgres, &["app_lock".into()], true)`→PinFunction; `classify("SELECT my_app_lock(1)", …&["app_lock"…], true)`→None (whole-ident, not substring); `classify("SELECT app_lock(1)", Postgres, &[], true)`→None (unflagged, safe SELECT). Panic-safety: `classify("SELECT 'café pg_advisory_lock'", …)`→None; `classify("", …)`→None; unterminated `"SELECT '"`→(Unknown if pin_on_unknown / None) without panic.
- **Gate:** `cargo test -p ferro-classify` (full corpus); `cargo build --workspace`; fmt/clippy clean.
- **Commit** `feat(m1-s2): ferro-classify rules — Dialect/PinTrigger + PG classify() (SET-LOCAL exact-token, temp-any-object, advisory session-only, safe-list, pin_on_unknown)`.

---

### Task 2: pool seam — `PoolBackend::dialect()`, `PinCause` assist variants, `PoolConfig` fields

**Files:** Modify `engine/crates/ferro-pool/Cargo.toml`, `engine/crates/ferro-pool/src/{backend.rs,pin.rs,config.rs,fake.rs}`, `engine/crates/ferro-backend-pg/src/conn.rs`.

**Interfaces consumed:** `ferro_classify::{Dialect, PinTrigger}` (T1a/T1b).
**Interfaces produced:** `PoolBackend::dialect(&self) -> ferro_classify::Dialect`; `PinCause` + 7 variants + `From<ferro_classify::PinTrigger>`; `PoolConfig { …, pin_functions: Vec<String>, pin_on_unknown: bool }`.

- `ferro-pool/Cargo.toml`: add `ferro-classify = { path = "../ferro-classify" }`.
- `backend.rs`: add `fn dialect(&self) -> ferro_classify::Dialect;` to the `PoolBackend` trait (sync). Optionally `pub use ferro_classify::Dialect;`.
- `ferro-backend-pg/src/conn.rs`: `fn dialect(&self) -> Dialect { Dialect::Postgres }`.
- `fake.rs`: add a `dialect: Dialect` field to `FakeBackend` (defaults to `Dialect::Postgres` — works because `Dialect: Default` from T1b, so `#[derive(Default)]` on `FakeBackend` still compiles; verification #3) + a `set_dialect`/builder for tests; `dialect()` returns it.
- `pin.rs`: extend `PinCause` to `{ Tx, Listen, AdvisoryLock, Prepare, Temp, Set, PinFunction, Unknown }` (keep existing derives) + `impl From<ferro_classify::PinTrigger> for PinCause` (same-named 1:1). (Verified: no exhaustive `match` on `PinCause` exists, so existing `Some(PinCause::Tx)` assertions stay green.)
- `config.rs`: add the two fields + `Default` (`pin_functions: Vec::new()`, `pin_on_unknown: true`).
- **TDD:** `PgBackend::dialect()==Postgres`; `FakeBackend::default().dialect()==Postgres` (proves the Default derive still compiles + is correct); a fake with `set_dialect(MySql)` returns MySql; `PinCause::from(t)` for all 7 `PinTrigger`; `PoolConfig::default().pin_on_unknown==true` && `pin_functions.is_empty()`.
- **Gate:** `cargo test -p ferro-pool -p ferro-backend-pg` (existing suites still green — purely additive); `cargo build --workspace`; fmt/clippy clean.
- **Commit** `feat(m1-s2): PoolBackend::dialect + PinCause assist variants + PoolConfig pin_functions/pin_on_unknown`.

---

### Task 3: wire `apply_classify` into `Checkout::exec`/`query` — the assist signal live

**Files:** Modify `engine/crates/ferro-pool/src/pool.rs`; Tests new `engine/crates/ferro-pool/tests/classify_pin.rs` + `engine/crates/ferro-backend-pg/tests/pg_pool_it.rs`.

**Interfaces consumed:** `PoolBackend::dialect`, `PoolConfig::{pin_functions,pin_on_unknown}`, `PinCause::from`, `ferro_classify::classify` (T1/T2).
**Interfaces produced:** a private `Checkout::apply_classify(&mut self, sql: &str)`.

- `pool.rs`: add
  ```rust
  fn apply_classify(&mut self, sql: &str) {
      // Assist signal (SPEC §7.1): RFQ (apply_tx_status) is the tx AUTHORITY; the lexer only ADDS
      // session-state taint + a cause label for protocol-invisible mutations. It NEVER clears a
      // taint and NEVER touches self.pin/tx_open (those are the RFQ's/tx's). classify() is total
      // (never panics) and multi-statement-aware, so exec's batch path is covered.
      if let Some(trigger) = ferro_classify::classify(
          sql,
          self.pool.backend.dialect(),
          &self.pool.config.pin_functions,
          self.pool.config.pin_on_unknown,
      ) {
          self.tainted = true;
          self.last_pin_cause = Some(PinCause::from(trigger));
      }
  }
  ```
  Call `self.apply_classify(sql);` in BOTH `Checkout::exec` and `Checkout::query`, placed AFTER the `is_bare_tx_control` guard, AFTER `apply_tx_status(st)`, and AFTER the `if r.is_err() { … }` Err-force block (so it runs on both arms; a session-mutating statement that errored is still labeled + tainted, and tainting is idempotent). NOT on the `is_bare_tx_control` early-return path (nothing ran). Precedence: `apply_tx_status(Idle)` sets neither a `Tx` cause nor clears `tainted`, so an autocommit session mutation keeps its assist cause; a session mutation inside a tx (RFQ=T) has `cause=Tx` set first then overwritten by the assist cause — acceptable (both safety bits already set; `last_pin_cause` is "most recent cause observed", observability only). Document inline.
- **TDD (unit, FakeBackend — deterministic):** new `engine/crates/ferro-pool/tests/classify_pin.rs` (fake dialect Postgres): `SET search_path=x` → `tainted() && last_pin_cause()==Some(PinCause::Set)` AND (the assist-not-authority invariant) `pin_state()==Unpinned && !tx_open()` when RFQ is Idle; `LISTEN c`→Listen; `SELECT pg_advisory_lock(1)`→AdvisoryLock; `PREPARE s AS SELECT 1`→Prepare; `CREATE TEMP TABLE t(x int)`→Temp; a multi-statement `exec("SELECT 1; LISTEN c")`→tainted+Listen (proves the exec-batch path is covered); plain `SELECT 1`→`!tainted() && last_pin_cause()==None` (the common path must NOT over-taint); with pool `pin_functions=["app_lock"]`, `SELECT app_lock(1)`→PinFunction; `pin_on_unknown=true` + `FLUFF x`→tainted+Unknown; a fake pool with `pin_on_unknown=false` + `FLUFF x`→`!tainted()`. Also: a `SET` while RFQ InTx (drive fake InTx) still ends `tx_open()` true with the assist cause (both bits set). Assert the pin-cause DoD in each.
- **TDD (live PG, skip without `FERRO_TEST_PG_URL`) — closes the S1 deferred leak; each test is designed to fail for its INTENDED reason (verification #4/#5/#6):** Build a `max_size=1` pool so the SAME connection returns (assert `pg_backend_pid()` is identical across the two checkouts — otherwise a fresh conn gives a false green):
  - (a) autocommit `Checkout::exec("SET search_path TO ferro_test_s2")` → RFQ `Idle` (`!tx_open`, `pin==Unpinned`), `tainted()` , `last_pin_cause()==Some(PinCause::Set)`.
  - (b) hygiene end-to-end: return the tainted conn; the NEXT checkout (same pid, asserted) runs `SHOW search_path` and it is NOT `ferro_test_s2` (DISCARD ALL ran). This is the concrete leak-closed proof.
  - (c) `Checkout::query("SELECT pg_advisory_lock(1)", &[])` taints with `AdvisoryLock`; return it; then from an **INDEPENDENT** second connection (a separate `Checkout` or a raw testkit conn) assert `pg_try_advisory_lock(1)` returns `true` after recycle (proving the lock was released by DISCARD ALL — not merely re-entrantly re-acquired on the same session). (Alternatively assert `pg_locks` has no advisory row for the objid.)
  - (d) `Checkout::query("LISTEN ferro_test_chan", &[])` → taints with `PinCause::Listen`, RFQ `Idle`/`Unpinned`; after recycle the next same-pid checkout is NOT subscribed (e.g. `SELECT count(*) FROM pg_listening_channels()` is 0, or a NOTIFY is not delivered).
  - (e) `Checkout::exec("CREATE TEMP TABLE ferro_test_tmp(x int)")` → taints with `PinCause::Temp`; after recycle the next same-pid checkout does NOT see the temp table (`SELECT to_regclass('ferro_test_tmp') IS NULL` is true).
- **Gate:** `cargo test -p ferro-pool` (unit incl. classify_pin) + live `cargo test -p ferro-backend-pg`; the existing S1/S4/S6 suites stay green. **Remediation rule (verification nice-to-have #1):** if a `SELECT`/DML statement NEWLY taints and breaks a suite, the classifier's safe-list is wrong — fix the classifier. If a test that runs a GENUINE trigger (SET/LISTEN/advisory/PREPARE/temp) breaks because the conn is now tainted, the classifier is CORRECT — update the TEST's expectation, NEVER weaken a trigger to keep an old test green. `cargo build --workspace`; fmt/clippy clean.
- **Commit** `feat(m1-s2): wire ferro-classify into Checkout as the assist signal (session-mutation taint + pin-cause; closes the SET search_path/LISTEN/temp/advisory leaks)`.

---

### Task 4: per-pool `pin_functions`/`pin_on_unknown` config from `ferrod` (escape hatch, testable without env mutation)

**Files:** Modify `engine/crates/ferrod/src/config.rs`, `engine/crates/ferrod/src/pools.rs`; Tests in `ferrod/src/config.rs` `#[cfg(test)]`.

**Interfaces consumed:** `PoolConfig::{pin_functions,pin_on_unknown}` (T2).

- `ferrod/src/config.rs`: extend `PoolSpec` (currently `{ name, dsn }`) with `pin_functions: Vec<String>` + `pin_on_unknown: bool`. **Do NOT read `std::env` inline in a way tests must mutate** (verification #1 — `set_var` is `unsafe fn` under edition-2024 `forbid`, and no env-isolation test pattern exists). Instead add a pure parser that takes an injected lookup closure:
  ```rust
  fn parse_pool_pin_config(name: &str, lookup: &impl Fn(&str) -> Option<String>) -> (Vec<String>, bool) {
      let fns = lookup(&format!("FERRO_POOL_{}_PIN_FUNCTIONS", name.to_uppercase()))
          .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
          .unwrap_or_default();
      let pin_on_unknown = lookup(&format!("FERRO_POOL_{}_PIN_ON_UNKNOWN", name.to_uppercase()))
          .map(|s| !matches!(s.trim().to_ascii_lowercase().as_str(), "0" | "false" | "no" | "off"))
          .unwrap_or(true);   // default true (SPEC §7.1)
      (fns, pin_on_unknown)
  }
  ```
  The real `from_env`/`parse_pools` path passes `&|k| std::env::var(k).ok()`; tests pass `&|k| map.get(k).cloned()`. If the existing `parse_pools` reads env directly, thread the same lookup closure through it (minimal refactor) so the whole pool parse is testable — mirror the existing name-uppercasing convention used for `FERRO_POOL_<NAME>_DSN`.
- `ferrod/src/pools.rs`: change `daemon_pool_config()` (zero-arg, identical per pool) to `daemon_pool_config(spec: &PoolSpec) -> PoolConfig` copying `spec.pin_functions.clone()` + `spec.pin_on_unknown` into the returned `PoolConfig` (keep the existing size/timeout/lifetime defaults). Update the call site.
- **TDD (unit, injected map — NO process-env mutation):** `parse_pool_pin_config("main", &lookup)` where `lookup` is a `HashMap`-backed closure: `{"FERRO_POOL_MAIN_PIN_FUNCTIONS": "app_lock, other_fn"}` → `(["app_lock","other_fn"], true)`; `{"FERRO_POOL_MAIN_PIN_ON_UNKNOWN":"0"}` → `(_, false)`; `"false"`/`"off"` → false; empty map → `([], true)`; whitespace/empty entries trimmed+dropped (`"a,,b, "` → `["a","b"]`).
- **Gate:** `cargo test -p ferrod` (config parsing) + `cargo test --workspace`; the daemon still boots with defaults (no env → `pin_on_unknown=true`, `pin_functions=[]`); fmt/clippy clean; `/proto` untouched.
- **Commit** `feat(m1-s2): per-pool pin_functions/pin_on_unknown config (injected-lookup parse -> PoolSpec -> PoolConfig)`.

---

## Self-Review (author against SPEC §7.1 + exec-design S2 + mechanism map + verification)

- **Spec coverage (exec-design S2 gate line 68-69):** full trigger set (T1a scanner + T1b rules); `PinCause::{Listen,AdvisoryLock,Prepare,Temp,Set,PinFunction,Unknown}` (T2); wired as ASSIST with RFQ authority (T3); `pin_on_unknown=true` default (T1b/T2) + per-pool `pin_functions` escape hatch (T4). The gate's required live LISTEN + advisory + temp are all in T3 (b–e).
- **Verification FIX_FIRST folded (v2):** #1 injected-lookup config (no `set_var`); #2 `SET LOCAL` exact-token; #3 `Dialect: Default`; #4 advisory-release proven from an independent connection; #5 search_path proof via `max_size=1` + same-pid; #6 live LISTEN + temp added; #7 quoted identifiers are CODE; #8 temp = any temp object + `INTO TEMP`; #9 nested-depth block comments; #10 panic-safe UTF-8 scanner; #11 multi-statement classify covers `exec`. Minors: gate remediation wording, E-strings/`$1` corpus, `MERGE`/`LOCK`/`DISCARD` safe-list, `_shared`/`_xact` advisory corpus, Task 1 split into scanner (T1a) + rules (T1b).
- **Assist-not-authority + no-over-taint** asserted in T3 (autocommit SET is `Unpinned && !tx_open` but tainted; plain `SELECT 1` never taints). Additive-only `PinCause` (no exhaustive match). Leaf-crate/acyclic deps confirmed.
- **The scanner is the crux** — T1a is a standalone task with its own hostile corpus, reviewed in isolation before the rules build on it.

## Execution Handoff

Subagent-driven: fresh implementer per task (TDD, gates), two-stage review after each (probe the scanner's literal/nesting/UTF-8 correctness + the assist-not-authority invariant + no-over-taint + the live tests failing for their intended reason), whole-branch review before S3. Live tests against the testkit Docker PG.
