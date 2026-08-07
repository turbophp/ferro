# Ferro M1-S8a — Engine + Client DBAL-Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Ferro engine and PHP client able to host a Doctrine DBAL-4 driver — narrowing binds, vendor errno on the wire, `last_insert_id`, savepoint passthrough, catalog/ENUM type coverage, an imperative transaction trio, binary binding, dialect-aware isolation `BEGIN`, and `HELLO_ACK` pool metadata — with **no DBAL dependency anywhere**, every item proven live against PostgreSQL 17, MySQL 8.4 and MariaDB 11.8 on its own.

**Architecture:** Twelve independently-green slices, each a committable deliverable with its own live gate. The engine changes stay inside the existing seams: the `PoolBackend` trait gains two capability/metadata methods, `PoolError::Sql` gains one field that `fate.rs` threads to the wire, `bind::accepts` gains a value-aware range check that keeps the §19.3 directional rule intact, and the two type-classification tables (`oid_extract_type`, `column_kind`) each gain arms in lockstep with their extractors. The one wire change (`HELLO_ACK` pool metadata) is deliberately paired with a `protocol_version` bump so a skewed engine/client pair fails at the **first byte of the first frame**, not late inside `HelloAck::decode`.

**Tech Stack:** Rust (edition 2024, tokio) for `ferro-proto` / `ferro-pool` / `ferro-backend-{pg,mysql}` / `ferrod`; PHP ≥ 8.2 (dependency-free) for `ferro/client`; `/proto` TOML registry + JSON golden vectors as the cross-language lock.

**Revision: v2.** v1 was adversarially verified against the code at HEAD `50c93d4` and against the live databases (PostgreSQL 17 on `:55432`, MySQL 8.4 on `:33060`, MariaDB 11.8 on `:33061`). Two probes found **6 blockers, 13 majors, ~15 minors and 9 unfalsifiable guards**; all are applied here. The findings that changed the shape of the work, not just its wording:

- **Task 5 was rewritten, not edited.** Its v1 design would have manufactured the exact false-`Indeterminate` it exists to prevent (`postgres-types` has zero domain handling), and its own safety proof would have stayed green over the bug.
- **Task 8's live isolation assertion was false**, and its natural repair is a cross-tenant connection-state leak. It is replaced with a lock-conflict proof plus an explicit prohibition.
- **Task 11 needs no `PoolRegistry`; Task 12 needs it threaded through `serve()`.** v1 had this backwards, and omitted a hard compile break in `ferro-e2e`.
- **Task 12's probe sat on the handshake critical path** and could exceed the client's own 5 s `ioTimeout`. It is now bounded as a whole, concurrent, TTL'd and backoff-cached.
- **Three tasks prescribed live tests against a `harness()` API that does not exist.** They are rewritten against the real helpers, and the two genuinely-new pieces of harness are explicit build steps.
- **Every guard the plan adds was re-checked against the "can it fail?" rule**, not only the nine the probes named.

---

## Global Constraints

Every task's requirements implicitly include this section. Each hazard below was verified against the code at HEAD `50c93d4` and carries its `file:line`.

### Contract rules (non-negotiable, copied from `CLAUDE.md`)

- **Charter rule 2 — `/proto` is the single source of truth.** Any protocol change updates the **registry**, the **golden vectors**, and **BOTH** codecs (Rust + PHP) **in the same change set**. Hand-written protocol constants anywhere are a defect.
- **Charter rule 3 — the engine never transparently retries** a user statement. Nothing in this slice may add a retry. The imperative PHP transaction trio (Task 9) exists precisely so DBAL owns the retry decision.
- **Charter rule 4 — every in-flight request terminates in exactly ONE `END` frame.** Every new refusal path in this slice must be checked against that.
- **Charter rule 6 — scope discipline + no silent miscasts.** No SQL rewriting, no ORM semantics in Rust, no read/write inference. An out-of-scope type stays a **loud `PoolError::Unsupported` naming the column and its native type**.
- **Charter rule 7 — the PHP client stays runtime-dependency-free.** No new composer runtime requirement; `ext-msgpack`/`ext-sockets` stay optional and runtime-detected.
- **SPEC §19.3 — the directional bind rule.** A bind pre-flight may be **stricter** than the concrete impl it fronts (a clean, diagnosable pre-send rejection); it must **NEVER be looser**, because a looser pre-flight lets the failure land **post-send**, which is the false-`Indeterminate` path the pre-flight exists to prevent (`engine/crates/ferro-backend-pg/src/bind.rs:215-228`).
- **No cross-tenant connection-state leak.** A statement must never be able to leave state on a pooled connection the next tenant inherits (`engine/crates/ferro-pool/src/pool.rs:571-577`).

### THE dominant defect class in this project — guards that cannot fail

The M1-S7 whole-branch review found **nine** guards that were *structurally incapable of failing*: a dead registry key, a hard-coded count (`assert!(count >= 7)` against 21 vectors), a source-text containment scan defeated twice, a test asserting a signature instead of a behaviour, a CI lane reporting "23 passed" with **no database attached**, and a `grep` whose capitalization did not match what the tests emitted.

**Binding rule for this plan:** every completeness check, exclusion list and "this cannot happen" assertion added by any task must be one that **CAN fail**. Prefer, in order:

1. **Compile-forced** — an exhaustive `match` with no `_ =>` arm (a new variant breaks the build).
2. **Derived** — the required set computed from the registry / from the query under test, never a parallel hard-coded list.
3. **Behavioural** — drive a real value through the real path and assert the observable, never assert that a function exists or has a signature.

**Three traps this plan's own v1 fell into, and the rules that come out of them** (found by adversarial verification before any code was written — treat them as binding, not advisory):

- **"Derived" is not automatically falsifiable.** `every_variant()` (`ferro-backend-pg/src/bind.rs:355-372`) is a **hand-written `Vec`**. Replacing `assert_eq!(…len(), 14)` with `assert_eq!(…len(), every_variant().len())` is a **TAUTOLOGY with respect to a MISSING variant** — it only proves that boxing drops nothing, never that the fixture is complete. The fix is rule 1, not rule 2: a compile-forced `fn _exhaustive(v: &Value) { match v { … } }` with **no `_` arm** living next to the fixture, so a 15th `Value` variant breaks the build (Task 4 Step 6).
- **Some sets CANNOT be compile-forced, and saying so is part of the guard.** `every_target_type()` enumerates `tokio_postgres::types::Type`, which is an **external, open** type (any oid is constructible) — no `match` can be exhaustive over it. Behavioural cross-product coverage is the correct guard there; the obligation is to GROW the fixture with each change and to mutation-prove that the growth was load-bearing (Task 4 Step 8 mutation 2, Task 5 Step 5).
- **A negative built from a hand-made input is a tautology.** Asserting `errno == None` on a `PoolError` the test itself constructed with `errno: None` cannot fail. A negative must be driven from the real producer (a live PG error, the real bind path) or must assert a MIRROR property (output equals input across a table that includes both `Some` and `None`). Task 3 Step 1 applies both.

**Every guard a task adds must be proven by MUTATION**: revert the production change (or flip one line), re-run the guard, and record that it goes RED. A task step that adds a guard without a mutation step is incomplete. This applies to **every** guard in this plan, not only the ones the text calls out.

- **`ci/assert-no-skips.sh` is live and shared** by `.github/workflows/ci.yml:68` (the `integration` job) and `ci/local-gate.sh:46`. In a **live** run a skip is a FAILURE: the script greps the test log for a word-anchored, case-insensitive `skip` (followed by `:`, whitespace, or end of line) and fails the lane if it matches. Two consequences for every new live Rust test: (a) it MUST print `skip: <reason>` on its unconfigured path — that line is what makes the live lane catch a test that made no database contact; and (b) **no new test NAME may contain the bare word `skip`** followed by `:`/space/end — `foo_skips_bar` is fine (the anchor requires `skip` to END the word), `foo_skip_bar` is fine for the same reason, but a printed line reading `skip <something>` is not worth risking. The script additionally fails if the log contains no `test result:` line at all.
- The PHP live lane runs `phpunit tests/Live --fail-on-skipped` (`ci/local-gate.sh:60`, `ci.yml:99`), so a PHP live test that skips **fails CI**. Any live PHP test needing MySQL therefore requires MySQL to be provisioned in the `php` CI job — Task 2 does that.

### Verified hazards — a naive implementation is WRONG

**PG bind (Tasks 4, 5)**

1. **`bind::accepts` is the §19.3 pre-flight and is DIRECTIONAL** (`ferro-backend-pg/src/bind.rs:215-228`, proof at `bind.rs:538-554`). Widening it without widening the boxed impl in lockstep converts a clean pre-send refusal into a **post-send `to_sql_checked` failure** — a false `Indeterminate`. The mirror is arm-for-arm: `value_to_boxed` (`bind.rs:192-213`) ↔ `accepts` (`bind.rs:229-250`).
2. **The lockstep proof iterates two HARD-CODED fixtures** — `every_variant()` (`bind.rs:355-372`) and `every_target_type()` (`bind.rs:376-401`). A newly-admitted target type or a newly-reachable value magnitude that is **not added to those fixtures is silently unproven**. This is exactly the "hard-coded list" failure mode above: both fixtures must grow with the change, and the growth must be mutation-proven. **CORRECTED (probe 1, weak guard 7):** the two fixtures need DIFFERENT guards. `every_variant()` enumerates `ferro_proto::value::Value`, a **closed** in-tree enum, so it CAN and MUST be compile-forced (Task 4 Step 6's `_exhaustive`); asserting its length against itself is a tautology w.r.t. a missing variant. `every_target_type()` enumerates `tokio_postgres::types::Type`, an **external, open** type — no exhaustive match exists, so behavioural growth + a mutation proof is the correct and only available guard there.
3. **Range narrowing must be checked in the PRE-FLIGHT, not in `to_sql`.** `PgInt(2_147_483_648)` against `int4` must be refused by `accepts` (which sees the VALUE), never by `to_sql` (which sees only the `Type`). `bind::accepts(v, ty)` takes the `Value`, so the range check belongs there. **CORRECTED MECHANISM (probe 1, minor):** the reason is **misclassification**, not a half-sent statement. `encode_bind_raw` (`vendor/tokio-postgres/src/query.rs:294-331`) runs every param's `to_sql_checked` into a LOCAL `BytesMut` **before** `start(client, buf)` touches the socket — so a `to_sql` failure means nothing was sent. The damage is what happens to that error: it surfaces as `Error::to_sql(e, idx)`, whose `as_db_error()` is `None`, which `ferro-backend-pg`'s `is_session_fatal` (`conn.rs:241-249`) reads as a transport failure → `PoolError::ConnectionLost` → and `classify_fate` turns a `ConnectionLost` on a **sent, non-readonly, non-in-tx** op into `WriteUnconfirmed{Indeterminate}` (§19.3). A statement that provably never left the process is then reported to the client as a write of UNKNOWN fate. That is the bug; "post-send" is the wrong shorthand for it and must not reach committed source.
4. **PG result format is BINARY and NOT per-statement selectable** (`vendor/tokio-postgres/src/query.rs:324` hardcodes `Some(1)`); **param** format IS per-param via `ToSql::encode_format` (used by the S7 text newtypes at `bind.rs:95`). A narrowing integer bind should therefore write the native **binary** `int2`/`int4` through `<i16/i32 as ToSql>`, not text.
5. **`stmt.params()` reports a DOMAIN's OWN oid**, unlike `RowDescription` which resolves to the base (SPEC §22.2 (g), `ferro-spec-v0.2.md:581`). `Kind::Domain(Type)` exists at `postgres-types-0.2.14/src/lib.rs:392`. A domain over a domain is legal, so the unwrap must be a **bounded loop**, never unbounded recursion. **CORRECTED SCOPE (probe 1, blocker B2 — this is the hazard that broke Task 5 v1):** `postgres-types` has **ZERO** `Kind::Domain` handling anywhere (grep: no hits). Measured live: `<String as ToSql>::accepts(domain_over_text)`, `<bool as ToSql>::accepts(domain_over_bool)` and `<Vec<u8> as ToSql>::accepts(domain_over_bytea)` are **all `false`**. So the unwrap cannot be applied to "the new narrowing arms only" — **every** arm of the pre-flight and **every** boxed impl behind it must resolve, or the pre-flight passes while `to_sql_checked` refuses, which via hazard 3's misclassification chain is a **false `Indeterminate` on a write**: exactly the failure the pre-flight exists to prevent, manufactured by the fix for it.
6. **`f64 → float4` narrowing must reject an out-of-`f32`-range finite value.** `1e39_f64 as f32` is `inf` — a silent corrupt write. Precision loss *within* range is accepted (it is the column's own precision, identical to what PG's own parser would do) and must be documented.

**PG catalog types (Task 6)**

7. **Two PG gates fire at DIFFERENT times and must move in LOCKSTEP** — `oid_to_tag` at cols-build, pre-execution (`ferro-backend-pg/src/rowmap.rs:97`) and `extract_value` per-cell, mid-stream after HEAD is on the wire (`rowmap.rs:168`). Both are matches over the single `oid_extract_type` table (`rowmap.rs:129`) precisely so they cannot drift.
8. **`"char"` (OID 18) is a SINGLE BYTE, not a string.** `postgres-types` reads it as `i8`. PG's `attidentity` is `'\0'` on a non-identity column, which PG's own text output renders as the **empty string**. Rendering must be: `0 → ""`, ASCII byte → a 1-char string, non-ASCII byte → a loud `PoolError::Backend` decode mismatch (never `ConnectionLost` — SPEC §9.1).
9. **`name` (OID 19) needs no new machinery** — `Type::NAME` is already in `String`'s `FromSql::accepts` list (`postgres-types-0.2.14/src/lib.rs:731`, `:1152`), so it folds straight into the existing `ExtractType::Text` arm.
10. **`oid` (OID 26) reads as `u32`** (`postgres-types` `impl FromSql for u32` accepts **only** `Type::OID`) and widens losslessly to `I64`.
11. **`regtype`/`regclass` binary payload is a bare 4-byte OID** (PG's `regtypesend` is literally `oidsend`), so under Ferro's hardcoded binary result format they can only be reported as the numeric OID → `I64`. Rendering them as the type NAME would require a catalog round trip the engine must not make (charter rule 6). Callers wanting the name cast in SQL (`::text` / `format_type(...)`), which DBAL's own introspection already does. **Note for test fixtures:** `2205` is `regclass`'s REAL oid and `2206` is `regtype`'s — a synthetic domain `Type` in a unit test must NOT reuse either (Task 5 uses the `900_00x` band).
12. **MySQL ENUM is rejected in TWO places** — the `ENUM_FLAG` early-return inside the string family (`ferro-backend-mysql/src/rowmap.rs:179-181`) **and** the standalone `ColumnType::MYSQL_TYPE_ENUM` arm (`rowmap.rs:195`). `SET` stays `Unsupported` in both places. **CORRECTED (probe 1, major M2 — the v1 rationale was BACKWARDS):** `MYSQL_TYPE_ENUM` **never reaches the client**. Measured on both engines: `information_schema.COLUMNS.COLUMN_KEY` **and** a user-declared `ENUM('a','b')` column both arrive as `MYSQL_TYPE_STRING` carrying `ENUM_FLAG`. So fixing the **string family alone fully fixes `information_schema`**, and a test aimed at the standalone arm is a guard that **cannot fail**. The standalone arm is still fixed — defence in depth against a server or a driver version that does send the dedicated type code — but it must be described as such and must not carry a test claiming to prove it.
13. **The `DISCARD ALL` typeinfo-cache defect (SPEC §22.2 (m), ticket `docs/followups/2026-08-06-discard-all-typeinfo-cache-poisoning.md`) bites CUSTOM OIDs only.** Every catalog scalar this slice admits (`name`, `"char"`, `oid`, `regtype`, `regclass`) is a **builtin** OID and never triggers a typeinfo lookup, so Task 6 does not trip it. Any new live test that reads a *custom* OID after a taint must expect a bare `26000`, not the loud `Unsupported`.

**errno on the wire (Task 3)**

14. **`ferrod/src/services/fate.rs:120-133` is the ONE place a `PoolError` becomes an `ErrorPayload`** and is the correct fix site. **CORRECTED (probe 1, major M6):** it is **NOT** the only exhaustive `PoolError::Sql` destructure without `..` — `ferro-backend-mysql/src/bind.rs:647-652` (a unit test) also destructures all four fields and will break the build. Every OTHER match does use `..`: `fate.rs:203`, `ferro-backend-mysql/src/bind.rs:334`, `ferro-backend-mysql/src/error_map.rs:156,232,253`, `ferro-backend-pg/tests/pg_pool_it.rs:352,375,505`, `pg_query_stream_it.rs:126`.
15. **`PoolError::Sql` struct-literal sites that break on a new field:** `ferro-backend-pg/src/error_map.rs:37-42`, `ferro-backend-pg/src/query.rs:135-140`, `ferro-backend-mysql/src/error_map.rs:58-63`, `ferro-backend-mysql/src/bind.rs:309-314`, `ferro-pool/src/fake.rs:639`, plus tests at `ferrod/src/tx/actor.rs:1034,1096` and `ferrod/src/services/fate.rs:226-231,238-243,496`. **CORRECTED (probe 1, major M6): two more, both real compile breaks** — `ferrod/src/services/sql.rs:1120-1125` (`stream_cancel_error`, a production site) and `ferrod/src/services/fate.rs:246-253` (the `sql()` test helper, which is ALSO the site whose hard-coded `errno: None` makes the v1 "PG carries no errno" assertion a tautology — see Task 3 Step 1). `PoolError::Sql` derives no `Default`, so unlike the `QueryResult` cascade there is no `..Default::default()` shortcut: every one of these sites gains an explicit `errno`.
16. **MySQL already HAS the errno and discards it** — `ferro-backend-mysql/src/error_map.rs:57` uses `se.code` purely as a classification key. `ServerError.code` is a `u16`; `ErrorPayload.errno` is `Option<i32>` (`ferro-proto/src/messages.rs:49-52`). The widening is lossless — do **not** change the wire type.
17. **PG has NO integer errno.** `ferro-backend-pg/src/error_map.rs:37-42` builds `PoolError::Sql` from the 5-character SQLSTATE; the bind helper (`query.rs:134-141`) sets `sqlstate: None` outright. `errno` stays `None` on PG **forever** — that is the contract documented at `proto/PROTOCOL.md:254` ("raw backend errno, when the backend provides one").
18. **NO golden vector locks a non-null errno.** `proto/vectors/error_protocol.json` carries `"errno":null` (generated at `gen_vectors.rs:332`). `ferro-proto/tests/messages.rs:74` exercises `errno: Some(-5)` but that is a Rust round-trip, **not** a cross-language byte lock. Charter rule 2 requires a new vector in the same change set.
19. **A new vector must be REGISTERED in the accounting guard or it is silently unlocked.** `VectorConformanceTest::testEveryCommittedVectorIsByteLocked` (`php/client/tests/Conformance/VectorConformanceTest.php:371-401`) diffs every committed vector against `$prefixLocked ∪ $namedLocked ∪ CLIENT_ENCODED_MESSAGES`; `error_protocol` is listed by NAME at `:389`. A new `error_*` vector that is not added there makes that test RED — which is the guard working.
20. **PHP `CarriesErrorPayload` has NO `errno()`** (`php/client/src/Client/Error/CarriesErrorPayload.php:16-40`) and the exception MESSAGE built at `:20-26` omits it too.

**`last_insert_id` (Task 2)**

21. **It CANNOT be emulated with a follow-up query in a transaction-mode pool.** Measured live: MySQL `SELECT LAST_INSERT_ID()` after an INSERT returned **`0`**; PG `SELECT lastval()` threw **`55000`** (another session's connection). It must come off the OK packet (`ferro-backend-mysql/src/query.rs:118`, `MysqlConn::last_insert_id` at `conn.rs:99-101`) or `RETURNING`.
22. **The wire + PHP + golden vectors are already complete** — `ferro-proto/src/messages/sql.rs:186` `pub last_insert_id: Option<Value>`, PHP `ExecOk.php:22-24,:57`, vectors `sql_exec_response_lastid.json` / `sql_exec_response_nullid.json`. **No wire change is needed.**
23. **The existing vector locks `Some(Value::I64(200))`** (`proto/vectors/sql_exec_response_lastid.json`, generated at `gen_vectors.rs:444-449`), so the engine must emit `I64` when the id fits `i64` and `U64` only above `i64::MAX`.
24. **A golden-vector `U64` must be ≤ `0xffffffff` OR > `PHP_INT_MAX` — never in `(2^32, 2^63]`** (S7 hazard 7). `PurePacker::be()` returns a decimal STRING for every `0xcf` uint64 while ext-msgpack returns an `int`, so a value in that band makes `testExtPackerDecodeMatchesPureWhenLoaded` (`VectorConformanceTest.php:95`) fail in CI, which provisions ext-msgpack.
25. **`QueryResult` derives `Default`** (`ferro-pool/src/backend.rs:20-25`), so `..Default::default()` shrinks the cascade. Struct-literal sites: `ferro-backend-pg/src/query.rs:121`, `ferro-backend-mysql/src/query.rs:100`, `ferro-pool/tests/query_guard.rs:48`, `ferrod/src/services/sql.rs:1601,1615,1642,1661,1929`.
26. **`ExecCodec::decode` DROPS `last_insert_id`** — it returns only `['cols','rows','affected']` (`php/client/src/Client/ExecCodec.php:108`), so the engine half alone is unusable from PHP.
27. **The PHP live harness is PG-ONLY** — `php/client/tests/Live/LiveTestCase.php:132-152` sets exactly `FERRO_SOCK`, `FERRO_POOLS=default`, `FERRO_POOL_DEFAULT_DSN`, and `:52` skips on a missing `FERRO_TEST_PG_URL`. `ferrod` supports N pools via `FERRO_POOLS` + `FERRO_POOL_<NAME>_DSN` where `<NAME>` is `env_name()`-normalized (`ferrod/src/config.rs:244,332,392-402`); the **kind is inferred from the DSN scheme**, there is no `kind=` knob (`config.rs:88-104`).

**Streaming guard (Task 1)**

28. **The tx-scoped `fetch:stream` arm has NO MySQL guard.** The autocommit arm refuses pre-checkout (`ferrod/src/services/sql.rs:347-353`); the `Some(tx_id)` arm (`sql.rs:249-281`) forwards `TxCommand::ExecStreamed` for **any** backend, so the refusal happens **after checkout + BEGIN**, at `Checkout::query_stream`'s Err arm — which **force-taints the pinned tx connection** (`ferro-pool/src/pool.rs:674-677`).
29. **Two guards, two message strings, one authority missing.** Both shipped messages still say "M1-S7" (`sql.rs:349-351`, `ferro-backend-mysql/src/conn.rs:351-361`) while the spec says S8; and `matches!(pool, AnyPool::Mysql(_))` is a *second* source of truth beside the backend's own capability.
30. **MySQL `query_stream` stays DEFERRED and the reason must be recorded.** There is **no** `into_inner`/accessor to recover the `Conn` from a finished `ResultSetStream` (verified: `grep "into_inner|pub fn into_"` over `vendor/mysql-async/src/queryable/query_result/result_set_stream.rs`, `query_result/mod.rs`, `connection_like/mod.rs`, `conn/mod.rs` yields only three unrelated `mysql_common` hits), and **dropping the stream closes the connection**. Any implementation needs a THIRD vendored-fork divergence plus a restructure of `Checkout::finalize_stream` (`ferro-pool/src/pool.rs:782-810`), which reads `tx_status(&B::Conn)` **synchronously**. The GAT route is not type-expressible (`RowStreamHandle` holds `&'a mut Checkout<B>` **and** `B::RowStream`, `pool.rs:865-874`; in-tree E0505 precedent at `ferrod/src/services/sql.rs:802-809`).
31. **`ResultSetStream::affected_rows()` returns the PREVIOUS statement's count** (measured 3 and 1 in two independent probes) because `setup_stream` snapshots the OK packet before any row is read (`vendor/mysql-async/src/queryable/query_result/result_set_stream.rs:201`, read back at `:98-103`). Any future implementation MUST read `Conn::last_ok_packet()` post-drain.

**Savepoints (Task 7)**

32. **`is_bare_tx_control` is called from THREE guarded entries** — `Checkout::exec` (`ferro-pool/src/pool.rs:531`), `::query` (`pool.rs:579`), `::query_stream` (`pool.rs:641`). All three must route through the new class-aware guard or the hole reopens on one path.
33. **Savepoint verbs do NOT change PG's transaction status** — the in-tree model already says so: `SAVEPOINT`/`RELEASE`/`ROLLBACK TO` classify as **PRESERVE** in `leading_tx_verb` (`ferro-pool/src/pin.rs:196-198`) because real Postgres's RFQ byte does not flip on any of them. That is exactly why they are safe to pass through while boundary verbs are not.
34. **`SINGLE_WORD_TX_CONTROL` mixes both classes** (`pin.rs:87-95`: `BEGIN, SAVEPOINT, COMMIT, END, ROLLBACK, ABORT, RELEASE`) and **`ROLLBACK` is in BOTH** — bare `ROLLBACK` is a boundary verb, `ROLLBACK TO …` is a savepoint verb. `leading_words(sql, 2)` (`pin.rs:106`) extracts exactly the two words needed to tell them apart.
35. **MySQL silently ignores a bare `SAVEPOINT` under autocommit** (no transaction is started, the savepoint has no effect). So a savepoint statement on a **non-transactional** checkout must be refused by Ferro, not delegated to the server — otherwise a driver believes it holds a rollback point that does not exist. **REFINED (probe 1, minor):** the silent-ignore applies to `SAVEPOINT n` **only**. `ROLLBACK TO n` and `RELEASE SAVEPOINT n` under autocommit raise `ERROR 1305 (42000) SAVEPOINT n does not exist`, i.e. they are already loud. The refusal still covers all three — a rule that is loud for one verb and delegated for two others is a rule nobody can reason about, and the PG side is silent for none of them — but the RATIONALE sentence must name `SAVEPOINT` specifically rather than claiming all three are silently ignored.
36. **The engine's own `SavepointStack` tracks only its `sp_N` names** (`ferrod/src/tx/actor.rs:62-130`). A passthrough `SAVEPOINT DOCTRINE_1` is invisible to it, so **mixing** the TX-service savepoint API with passthrough SQL in one transaction makes the stack disagree with the server. Document it; the S8b driver uses passthrough only.

**Dialect `BEGIN` (Task 8)**

37. **`BEGIN READ ONLY` and `BEGIN ISOLATION LEVEL …` are `ERROR 1064` on BOTH MySQL 8.4.11 and MariaDB 11.8.8** (measured), and so is `START TRANSACTION ISOLATION LEVEL …`. The only working forms are `START TRANSACTION [READ ONLY]` and a `SET TRANSACTION …;` prefix. **CORRECTED — HOW YOU MAY AND MAY NOT ASSERT THAT IT TOOK (probe 1, blocker B3):** `SET TRANSACTION …` **without** `SESSION`/`GLOBAL` applies to the **NEXT transaction only** and is **NOT reflected in `@@transaction_isolation`**, which keeps reporting the session default (MySQL renders it `REPEATABLE-READ`, with a **hyphen**). So `assert_eq!(iso, "SERIALIZABLE")` against `SELECT @@transaction_isolation` is **FALSE on two counts** and must not be written. The isolation genuinely takes — proven behaviourally by a **lock conflict** (a concurrent `UPDATE` of a row the transaction has read BLOCKS under the batch and SUCCEEDS under a plain `START TRANSACTION`, because `SERIALIZABLE` implicitly converts a plain `SELECT` into a locking read while `REPEATABLE READ` does not). **`SET SESSION TRANSACTION ISOLATION LEVEL …` is FORBIDDEN in the engine**, and this is not a style preference: the SESSION form persists the level on the pooled connection past `COMMIT`, so the next tenant inherits it — a cross-tenant connection-state leak (charter rule 6), the one class this whole slice must never open. The natural "repair" for the false assertion is therefore the worst available change; it is called out here so no implementer reaches for it.
38. **A STANDALONE `SET TRANSACTION …` TAINTS every MySQL connection.** Measured: `status_flags=(AUTOCOMMIT|SERVER_SESSION_STATE_CHANGED) state_changed=true` with **no** trackers, which under `ferro-backend-mysql/src/tracker.rs:96-99` (`state_changed_flag && !has_sysvar && !has_txstate`) is `is_mutation() == true` → a `PinCause::SessionTracker` taint → a full `COM_RESET_CONNECTION` at the next recycle.
39. **The BATCHED form does NOT taint.** `"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION READ ONLY"` in ONE `query_drop` yields a final OK packet carrying `TransactionState("T_______")` → `has_txstate` gates the bare-flag path off → no taint, `tx_status_from_ok` → `InTx`, and `SERVER_STATUS_IN_TRANS_READONLY` confirms read-only took (a write inside gives `1792`/`25006` on both engines). `CLIENT_MULTI_STATEMENTS` is already negotiated (`vendor/mysql-async/src/opts/mod.rs:1158`) and `query_drop` drains all result sets, so `last_ok_packet` is the LAST statement's.
40. **`begin_tx_with` issues EXACTLY ONE `simple_query`** (`ferro-pool/src/pool.rs:338`) wrapped in the whole pin/RFQ/tracker/Rule-A sequence (`:343-369`). Two calls would re-run that sequence — the batch must be ONE statement string.
41. **`compose_begin_sql_table` pins all 8 PG strings verbatim** (`ferrod/src/tx/actor.rs:588-619`, incl. `"BEGIN READ ONLY"` at `:591` and `"BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY"` at `:613`). A signature change breaks it — that is intended; it must be **extended**, never deleted. Conversely **nothing pins the broken MySQL behaviour** (every MySQL tx test uses `isolation: None` — `ferrod/tests/mysql_it.rs:58-62`, `mysql_chaos_it.rs:364-368`).
42. **`Dialect` is `{Postgres, MySql, Sqlite}`** (`ferro-classify/src/lib.rs:25-30`, re-exported at `ferro-pool/src/backend.rs:10`). An exhaustive `match` is the compile-forced guard; a `_ =>` arm would silently hand a future SQLite backend PG syntax. `pool.backend()` is `pub` (`ferro-pool/src/pool.rs:205`) and `PoolBackend::dialect()` is a synchronous per-backend constant (`backend.rs:165`), so the dialect is reachable inside `begin_on_pool`.

**PHP client (Tasks 9, 10)**

43. **`Connection::transaction()` owns the commit/rollback decision AND the §19.1 retry loop** (`php/client/src/Client/Connection.php:326-437`); `TxHandle`'s docblock says commit/rollback are the runner's to call (`TxHandle.php:22-24`). An imperative trio must **NOT** reuse that loop — DBAL owns retry, and a mid-tx transparent reconnect would void the `tx_id` silently.
44. **`TxHandle::run` is already the bare send-and-classify** with no reconnect (`TxHandle.php:155-170`) — reuse it verbatim for imperative-tx statements rather than adding a second path.
45. **`TxHandle` has NO `stream()`** (live-verified). So `Connection::stream()` while an imperative transaction is open must never silently run **autocommit outside the open transaction**; the engine DOES support tx-scoped streaming (`ferrod/src/services/sql.rs:249-281`), and `ExecCodec::encode` already takes a `?int $txId` (`ExecCodec.php:66`), so threading the id is the correct fix.
46. **`Connection::transaction()` hard-codes `'isolation' => null`** (`Connection.php:337`). PHP-reachable **named** isolation constants would require a new `[isolation]` section in the `/proto` registry (touching `registry.rs` `MethodsToml` + `Registry`, **and `build.rs`'s own `#[serde(deny_unknown_fields)]` `Registry`** — omitting the last makes `cargo build` panic, S7 hazard 38) — explicitly **out of scope for S8a**. DBAL does not need it: `Doctrine\DBAL\Connection::setTransactionIsolation()` emits `SET SESSION TRANSACTION ISOLATION LEVEL …` as plain SQL, not a driver flag.
47. **PSR-4:** `php/client/composer.json:15` maps `"Ferro\\": "src/"`, so `Ferro\Bytes` MUST live at `php/client/src/Bytes.php`.
48. **A binary string cannot ride `TAG_TEXT`.** `Value::Text` encodes msgpack `str` and Rust's `read_str` ends in `String::from_utf8` (`ferro-proto/src/value.rs:113`), so `"\x00\x01\xff"` fails as `invalid utf8` **before** the bind pre-flight — surfacing as a generic "malformed ExecRequest", not a diagnosable bind error.
49. **`ExtPacker::packBin` is `\msgpack_pack($s)`** (`php/client/src/Protocol/Msgpack/ExtPacker.php:47`), which emits msgpack **`str`**, while Rust's `Value::decode` BYTES arm uses `read_bin` (`value.rs:120`), which is **marker-strict** for `0xc4/0xc5/0xc6`. Latent today only because `PackerFactory::forEncode()` always returns `PurePacker` (`PackerFactory.php:7`) and nothing produces a `TAG_BYTES` param. **Task 10 creates the first such call path.** This is the identical class to the already-fixed `packUint` (`ExtPacker.php:22-42`), which delegates to `$this->pure`.
50. **DBAL passes a PHP `resource` for `LARGE_OBJECT`** (`Types/BlobType.php:31-50`), which `ExecCodec::bindOne`'s default arm rejects outright (`ExecCodec.php:307-332`). Materializing a stream is the **driver's** job (S8b); the client offers an explicit constructor, never an implicit `is_resource` arm.

**`HELLO_ACK` metadata (Tasks 11, 12)**

51. **Extending `HelloAck` does NOT move `TYPE_REGISTRY_HASH` by itself.** The hash is FNV-1a over the raw bytes of `proto/registry.lock.json` (`ferro-proto/build.rs:106-114`; PHP mirror `proto/tools/gen-php.php:4,71`), and the lock carries **no message shapes**. So a skewed pair would handshake "successfully" (`ferrod/src/session/handshake.rs:31-36` compares only the hash) and then blow up at `CodecException('HelloAck arity != 5')` (`php/client/src/Protocol/HelloAck.php:40`) — an ugly late failure instead of the clean session-fatal `errc::UNSUPPORTED` (`ferrod/src/session/error.rs:30`).
52. **`PROTOCOL_VERSION` is byte 1 of EVERY frame header** (`ferro-proto/src/header.rs:19`) and `Header::decode` rejects a mismatch with `CodecError::BadVersion` (`header.rs:42-47`). Bumping `protocol_version` in `proto/methods.toml:1` therefore (a) changes `registry.lock.json` → moves `TYPE_REGISTRY_HASH`, **and** (b) changes **every** committed vector's `frame_hex`. Both are intended: the skew then fails at the **first byte of the first frame**, deterministically, in both directions. Vectors are regenerated by `gen_vectors`, **never hand-edited** (S7 hazard 9: there is no regenerate-and-diff guard for vectors).
53. **`hello_ack` is in `CLIENT_ENCODED_MESSAGES`** (`VectorConformanceTest.php:33`), so `php/client/src/Protocol/Message.php:25-31`'s `'hello_ack'` **encoder** must move with the shape or the byte lock fails. `SessionHandshakeTest.php:62,90` also builds `hello_ack` payloads by hand.
54. **`ferrod` knows NO pool's server version today** (greps for `server_version` across both backend crates return zero hits) and **pools are LAZY** — `Pool::new` dials nothing (`ferrod/src/pools.rs:138-142`), and there is no warmup/`min_size` (`ferro-pool/src/health.rs:17`). Today `ferrod` **boots with unreachable DBs**; learning the version must not change that.
55. **The backend KIND is free** — `PoolSpec.kind` is already known at build time (`ferrod/src/pools.rs:61-66`, from `config::infer_pool_kind`) and logged at `pools.rs:67`, but never put on the wire.
56. **`Session::pools()` and `hello()` are NOT on `SessionInterface`** (`php/client/src/Client/Session.php:84,228`), and `Ferro::connect` discards the `HelloAck` (`Ferro.php:46-50`). Reaching pool metadata from `Connection` needs an `instanceof Session` narrowing.

**Added by adversarial verification (probes 1 and 2). Each was measured against the code at HEAD `50c93d4` or against the live databases; none is a hypothesis.**

57. **`tokio_postgres::types::Type` is NOT `Copy`** (`postgres-types-0.2.14/src/lib.rs:315-316` — it derives `Clone, PartialEq, Eq, Hash` only). Compile-proved: `match (v, *ty) { (Value::I64(n), Type::INT4) => … }` is **E0507 (cannot move out of a shared reference)**. The legal spellings are `match (v, ty) { (Value::I64(n), &Type::INT4) if … => … }` (borrow the `Type`, match a reference pattern) or a nested `if let`. `matches!(*ty, Type::INT4 | …)` and a bare `match *ty { Type::INT4 => … }` **do** compile — the move only happens when the `Type` is placed into a tuple. Every value-aware range arm in Tasks 4 and 5 must use the borrowed form.
58. **`postgres-types` has NO `Kind::Domain` handling.** Grep over the crate returns zero hits, and the consequence was measured live on PG 17: for a `CREATE DOMAIN d AS text`, `<String as ToSql>::accepts(&d)` is `false`; likewise `bool` over a domain-of-`bool` and `Vec<u8>` over a domain-of-`bytea`. A pre-flight that resolves the domain while the boxed impl does not is **looser than the impl** — the §19.3 direction that is forbidden — and lands in hazard 3's misclassification chain. See hazard 5's correction.
59. **The session has NO access to the `PoolRegistry`.** `Session::run_with_handler` builds the `HELLO_ACK` from `config.pools` (`ferrod/src/session/mod.rs:342-346`); the registry is built in `ferrod/src/main.rs:35` and `ferro-e2e/src/main.rs:63` and reaches the session **only sealed inside the opaque `HandlerFactory` closure** (`services/sql.rs:98-110`); `serve()` does not take it (`serve.rs:51-58`). Task 11 therefore needs **no registry at all** — `PoolSpec` already carries `name` + `kind` (`config.rs:114-127`, kind from `infer_pool_kind`). Task 12 is where the registry must genuinely be threaded through `serve()` → `run_with_handler` → `run`. `PoolRegistry::build` already returns `Arc<Self>` (`pools.rs:57,70`), so the threading is a parameter addition, not an ownership change.
60. **`ferro-e2e` consumes `HelloAck.pools` as `Vec<String>`** — `pub pools: Vec<String>` at `ferro-e2e/src/client.rs:31`, filled from `ack.pools` at `:103` and printed at `main.rs:111`. Reshaping `pools` without touching that crate is a **hard compile break**, and `cargo test --workspace` is a DoD gate. Also: `proto/vectors/negative/*.bin` (`bad_magic`, `bad_version`, `oversize_len`, `reserved_flag`) are byte fixtures built against the LIVE `PROTOCOL_VERSION` — after a version bump, `reserved_flag.bin` must still be *structurally valid except for the reserved flag*, and `bad_version.bin` must still carry a version that is wrong. They are regenerated, never hand-edited.
61. **PHP API shapes the plan v1 got wrong.** `Ferro\Protocol\ExecRequest` has **no `decode()`** — it exposes `encode(array, PackerInterface)` and `mapFromWire(array)` (`ExecRequest.php:18,42`), so decoding a recorded payload is `ExecRequest::mapFromWire((array) $packer->unpack($payload, $off))` with `$off = 0` by reference. And there is **no `C::FETCH_STREAM`**: no `FETCH_*` constant exists in `Protocol/Generated/Constants.php`; the fetch modes live on the codec (`ExecCodec::FETCH_ROWS = 0`, `FETCH_NONE = 1`, `FETCH_STREAM = 2`, `ExecCodec.php:47-49`). Note also that `ExtPacker::unpack` consumes the WHOLE buffer and sets `$offset = strlen($buf)` — tests that decode one payload must pass `PurePacker`, which is what `PackerFactory::forEncode()` returns anyway.
62. **`Ferro::connect`'s default `$ioTimeout` is 5.0 s** (`Ferro.php:42`) and it is applied to the `HELLO_ACK` read (`Transport.php:74-77`). Anything the engine does between receiving `HELLO` and writing `HELLO_ACK` is inside that budget. A per-pool probe bounded at 2 s and run SERIALLY blows it at three unreachable pools (6 s > 5 s), failing `Ferro::connect` outright — the exact property Task 12 claims to preserve. Additionally, `tokio::sync::OnceCell::get_or_try_init` **serialises concurrent initialisers**, so an FPM reconnect storm after a `boot_epoch` change (§19.1) queues one probe at a time rather than sharing one.
63. **The `ferrod` live suites have NO `harness()` API.** `tests/common/mod.rs` exposes `exec_server(url) -> TestServer` (kind inferred from the DSN scheme), `TestServer::connect() -> TestClient`, `TestClient::hello(rid) -> HelloResult { request_id, ack }`, `req(sql) -> ExecRequest`, and `exec` / `exec_ok` / `exec_err(client, rid, &req)`. Transaction helpers are per-file FREE FUNCTIONS with different signatures: `tx_it.rs` has `begin(client, rid, pool, isolation, readonly)`, `exec_in_tx(client, rid, tx_id, sql, params, fetch, readonly)`, `commit`/`rollback(client, rid, tx_id)`; `mysql_it.rs` has its own `begin(client, rid, pool)` that **hard-codes** `isolation: None, readonly: false` and a `tx_req(tx_id, sql)` with `fetch: 0`. Every live step in this plan is written against those real helpers; the two genuinely-new pieces of harness (a multi-pool `TestServer`, and `mysql_it.rs`'s `begin` growing the two arguments `tx_it.rs`'s already has) are **explicit build steps** in Tasks 6/8/12, not assumptions.
64. **The tx-control guard is bypassable by a COMPOUND statement, today, before this slice.** Measured on MySQL (`CLIENT_MULTI_STATEMENTS` is negotiated): `SELECT 1; COMMIT` returns `Ok` with `tx_status = Idle`, and `SAVEPOINT s2; START TRANSACTION` returns `Ok` with `tx_status = InTx`. `leading_words` only ever inspects the LEADING verb. This is pre-existing and NOT a leak — `apply_tx_status` reads the real post-statement status off the protocol signal, so the pin engine stays honest and the connection is tainted/pinned correctly — but it means §22.2 (o) may **not** claim boundary verbs are "refused unconditionally on every entry". The truthful claim is "refused when they LEAD the statement; a compound statement's later verbs are caught by the pin AUTHORITY, not by this guard".
65. **`unsupported()`'s message tail names every deferred type**, so a containment assertion on a type NAME cannot fail. `ferro-backend-mysql/src/rowmap.rs:401-416` ends every refusal with `"Deferred: YEAR, BIT, ENUM, SET, GEOMETRY and VECTOR."` — so `assert!(msg.contains("SET"))` is green for a `YEAR` column, an `ENUM` column, anything. The discriminating substring is the interpolated head: `"MySQL SET ("`. (Task 6 also REMOVES `ENUM` from that tail, which is itself what makes the head-anchored assertion falsifiable.)
66. **MariaDB's `information_schema.COLUMNS.COLUMN_KEY` is `MYSQL_TYPE_VAR_STRING`, not an ENUM** (measured) — it already reads today, so a MariaDB arm of the ENUM live gate driven through `information_schema` is **GREEN BEFORE THE FIX**. MariaDB's ENUM proof must be driven through a **user table** column declared `ENUM(...)`, which does arrive as `MYSQL_TYPE_STRING | ENUM_FLAG` on both engines.
67. **PG does NOT report a domain for a plain `information_schema` predicate.** Measured: `SELECT … FROM information_schema.columns WHERE table_name = $1` resolves the parameter to `name` (`kind = Simple`), not to the `sql_identifier` domain — PG's `=` operator resolution picks the base-type operator. So `information_schema` introspection is unblocked by Task 6's **`name` (19) admission**, not by Task 5's domain unwrap. Domains are still real and still must be handled — for **user-defined domain columns**, which is what Task 5's live case (c) covers, and which SPEC §22.2 (g) is actually about — but Task 5's motivating sentence must not claim the `information_schema` win.
68. **`FerroException` is the ROOT of the client's exception tree** (`php/client/src/Client/Error/`: `Cancelled`, `ConnectionLost`, `EpochChanged`, `Handshake`, `Hydration`, `Indeterminate`, `NonRetryable`, `Protocol`, `Retryable`, `Transport`, `TypePolicy` all extend it). `expectException(FerroException::class)` therefore passes for **any** Ferro error, including one thrown by the test's own setup DDL — the failure mode that makes a negative test green for the wrong reason. Every `expectException` in this plan names a LEAF class, or asserts on the message, or both.
69. **`PROTOCOL_VERSION` is a generated constant** (`ferro_proto::consts::PROTOCOL_VERSION`, used at `header.rs:19,43,45`). A test asserting `BadVersion { expected: 2, got: 1 }` hard-codes a protocol constant in a test — charter rule 2's "hand-written protocol constants anywhere are a defect". Assert against `consts::PROTOCOL_VERSION`.
70. **`Value::tag()` exists** (`ferro-proto/src/value.rs:44`), which is what makes Task 6's derived HEAD-vs-producer assertion possible without a parallel tag table.

### Definition of done (charter DoD, EVERY task)

- `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace` — green **offline** (live tests skip, never fail, when `FERRO_TEST_PG_URL` / `FERRO_TEST_MYSQL_URL` / `FERRO_TEST_MARIADB_URL` are unset).
- `./ci/local-gate.sh --live` green (all three backends up, the no-skip gate satisfied, the PHP live tier run with `--fail-on-skipped`).
- `(cd php/client && ./vendor/bin/phpunit)` green; `./vendor/bin/phpstan analyse src --level 9` clean.
- `cargo deny check` on any task touching a `Cargo.toml` dependency list.
- Protocol work updates the registry + the golden vectors + BOTH codecs in the **same commit**, plus the vector-index tables in `proto/PROTOCOL.md` (§7 at `:195`, §8.3 at `:269`).
- **Every guard added is mutation-proven** (see "guards that cannot fail" above).
- The relevant SPEC section still tells the truth; a forced deviation is amended in the spec text **plus** a §22.2 line in the same change.

### Live test environment

```
docker compose -f testkit/docker-compose.yml up -d --wait pg mysql mariadb
FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro
FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro
FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro
```

---

## File Structure

**Created**

- `engine/crates/ferrod/tests/catalog_it.rs` — Task 6's live introspection gate. Its own file because it drives *catalog* SQL (`pg_catalog` / `information_schema`) on both engine families through a full `ferrod`, which is a different fixture shape from the per-backend `*_types_it.rs` suites.
- `engine/crates/ferro-backend-mysql/tests/begin_dialect_it.rs` — Task 8's taint proof. Separate from `mysql_types_it.rs` because it asserts on the **session-tracker** signal (`take_session_mutated`) rather than on values, and it is the one place the standalone-vs-batched `SET TRANSACTION` difference is pinned.
- `engine/crates/ferrod/tests/hello_meta_it.rs` — Tasks 11/12's handshake-metadata gate (multi-pool `ferrod`, including the unreachable-pool case).
- `php/client/src/Bytes.php` — the `Ferro\Bytes` explicit binary-bind marker (PSR-4 root, hazard 47).
- `php/client/src/Protocol/PoolInfo.php` — the decoded `HelloAck.pools` element.
- `php/client/src/Client/Error/InvalidTransactionStateException.php` — the LEAF exception every transaction-misuse guard asserts on (hazard 68: `FerroException` is the ROOT and passes for anything, including a test's own setup failure).
- `php/client/tests/Unit/BytesBindTest.php`, `php/client/tests/Unit/PoolInfoTest.php`, `php/client/tests/Client/ConnectionImperativeTxTest.php`.
- `php/client/tests/Live/LastInsertIdLiveTest.php`, `php/client/tests/Live/ImperativeTransactionLiveTest.php`, `php/client/tests/Live/BytesLiveTest.php`, `php/client/tests/Live/ErrnoLiveTest.php`, `php/client/tests/Live/PoolMetadataLiveTest.php`.
- `proto/vectors/error_mysql_errno.json` — the first vector locking a NON-NULL errno (generated, never hand-written).

**Modified**

- `engine/crates/ferro-pool/src/backend.rs` — `QueryResult.last_insert_id`; `PoolBackend::supports_row_streaming`.
- `engine/crates/ferro-pool/src/error.rs` — `PoolError::Sql.errno`.
- `engine/crates/ferro-pool/src/pin.rs` — `TxControlClass` + `tx_control_class`.
- `engine/crates/ferro-pool/src/pool.rs` — the shared `refuse_tx_control` guard on all three entries.
- `engine/crates/ferro-pool/src/fake.rs`, `engine/crates/ferro-pool/tests/query_guard.rs` — compile cascade.
- `engine/crates/ferro-backend-pg/src/{bind.rs,query.rs,rowmap.rs,error_map.rs,pgtext.rs}`.
- `engine/crates/ferro-backend-mysql/src/{query.rs,rowmap.rs,conn.rs,bind.rs,error_map.rs}`.
- `engine/crates/ferrod/src/services/{sql.rs,fate.rs}`; `engine/crates/ferrod/src/tx/{mod.rs,actor.rs}`; `engine/crates/ferrod/src/pools.rs`; `engine/crates/ferrod/src/session/{handshake.rs,mod.rs}`; `engine/crates/ferrod/src/{serve.rs,main.rs}` (Task 12's `Arc<PoolRegistry>` threading only).
- `engine/crates/ferrod/tests/common/mod.rs` — the multi-pool `TestServer` (Task 6) and the `Arc<PoolRegistry>` parameter on the factory spawners (Task 12).
- `engine/crates/ferrod/tests/{mysql_it.rs,tx_it.rs,types_e2e_it.rs}` — the live gates, plus `mysql_it.rs`'s local `begin` growing `isolation`/`readonly` (Task 8).
- `engine/crates/ferro-e2e/src/{client.rs,main.rs}` — `Handshake.pools` follows the `HelloAck.pools` reshape (hazard 60: omitting this is a hard compile break) and `serve()`'s new parameter (Task 12).
- `engine/crates/ferro-proto/src/messages.rs`, `src/bin/gen_vectors.rs`, `tests/{messages.rs,golden_vectors.rs,header.rs}`.
- `proto/methods.toml` (`protocol_version`), `proto/registry.lock.json`, `proto/PROTOCOL.md`, `proto/vectors/*.json` **and `proto/vectors/negative/*.bin`** (all regenerated by Task 11 — the negative fixtures carry the live `PROTOCOL_VERSION` in their header, hazard 60).
- `php/client/src/Client/{Connection.php,ExecCodec.php,TxHandle.php}`; `php/client/src/Client/Error/CarriesErrorPayload.php`; `php/client/src/Protocol/{HelloAck.php,Message.php}`; `php/client/src/Protocol/Msgpack/ExtPacker.php`; `php/client/src/Client/Session.php`.
- `php/client/tests/Live/LiveTestCase.php` — the second (MySQL) pool.
- `.github/workflows/ci.yml` — MySQL in the `php` job.
- `ferro-spec-v0.2.md` §7.1/§9.1/§14/§22.2.

**Explicitly NOT modified**

- `engine/crates/ferro-backend-mysql/src/conn.rs`'s `MysqlRowStream` placeholder and `query_stream` body beyond their message strings — MySQL streaming stays deferred (hazards 30/31), recorded in §22.2 by Task 1.
- `ferro-backend-mysql`'s `clean_reset_profile()` — the tracker-clean `None`-skip stays `Some(Full)` (R2, correctness over the §16 cache target). Task 8 makes it *more* important, not less: the batched BEGIN deliberately relies on `has_txstate` gating the flag path off, which is one of the three concrete holes a future `None`-skip must close.
- `proto/types.toml` `implemented` — S8a adds **no new tags**; every catalog/ENUM type maps onto an existing canonical tag.

---

## Task 1: One streaming-capability authority + close the tx-scoped guard asymmetry (+ the §22.2 deferral entry)

**Files:**
- Modify: `engine/crates/ferro-pool/src/backend.rs` (add `supports_row_streaming` next to `take_session_mutated` at `:193-195`)
- Modify: `engine/crates/ferro-backend-mysql/src/conn.rs:263-265` (next to `dialect`), `:351-361` (`query_stream`'s message)
- Modify: `engine/crates/ferrod/src/tx/mod.rs` (`TxHandle` struct + `TxRegistry::register`)
- Modify: `engine/crates/ferrod/src/services/sql.rs:77-79` (`FETCH_*` become `pub const` so an integration test reads the daemon's constant, not a literal), `:249-281` (tx-scoped arm), `:347-353` (autocommit arm), `:1221-1259` (`begin_on_pool`)
- Modify: `ferro-spec-v0.2.md` §22.2 (new entry `(n)`)
- Test: `engine/crates/ferrod/tests/mysql_it.rs`

**Interfaces:**
- Produces: `PoolBackend::supports_row_streaming(&self) -> bool` (default `true`; `MysqlBackend` overrides to `false`). `ferrod::services::sql::stream_unsupported() -> ErrorPayload` — the ONE constructor for the refusal, used by both arms. `ferrod::tx::TxHandle.streaming: bool`.
- Consumes: nothing from earlier tasks (this is the first task).

- [ ] **Step 1: Write the failing live test (both arms must produce the SAME refusal, and the tx must survive)**

Append to `engine/crates/ferrod/tests/mysql_it.rs`. **There is no `harness()` in this repo** (hazard 63) — this uses the file's REAL helpers: `common::exec_server(url)` (which infers `kind = mysql` from the DSN scheme), `TestServer::connect()`, `TestClient::hello(rid)`, `common::{req, exec_err, exec_ok}`, and `mysql_it.rs`'s own local `begin`/`commit`/`tx_req`:

```rust
/// Both `fetch:stream` arms on a MySQL pool must refuse with the SAME, precise terminal — and the
/// tx-scoped one must refuse BEFORE the actor touches the pinned connection.
///
/// Falsifiable: before this task the tx-scoped arm reached `MysqlBackend::query_stream` and returned
/// the stale `"MySQL streaming lands in M1-S7"` string (a different message from the autocommit
/// arm's), after force-tainting the pinned conn at `ferro-pool/src/pool.rs:674-677`. The
/// byte-equality assertion below is what goes RED if the two arms ever drift again.
#[tokio::test]
async fn mysql_stream_is_refused_identically_on_both_arms_and_the_tx_survives() {
    for (label, url) in mysql_targets() {
        let server = exec_server(url);
        let mut client = server.connect().await;
        client.hello(0).await;

        // (a) autocommit arm — `req` is fetch:rows, so flip it to stream.
        let mut auto_req = req("SELECT 1");
        auto_req.fetch = FETCH_STREAM;
        let auto = exec_err(&mut client, 1, &auto_req).await;

        // (b) tx-scoped arm.
        let tx_id = begin(&mut client, 2, "default").await;
        let mut scoped_req = tx_req(tx_id, "SELECT 1");
        scoped_req.fetch = FETCH_STREAM;
        let scoped = exec_err(&mut client, 3, &scoped_req).await;

        assert_eq!(
            auto.message, scoped.message,
            "[{label}] the autocommit and tx-scoped stream refusals must come from ONE constructor"
        );
        assert_eq!(auto.code, scoped.code, "[{label}] same terminal code");
        assert_eq!(auto.code, errc::UNSUPPORTED, "[{label}] a stream refusal is Unsupported");
        assert!(
            auto.message.contains("§22.2"),
            "[{label}] the refusal must cite the spec deferral, got {:?}",
            auto.message
        );
        assert!(
            !auto.message.contains("M1-S7"),
            "[{label}] the stale slice name must be gone, got {:?}",
            auto.message
        );

        // The tx was never touched: a normal statement still runs and COMMIT succeeds.
        let ok = exec_ok(&mut client, 4, &tx_req(tx_id, "SELECT 7")).await;
        assert_eq!(
            first_i64(&ok),
            7,
            "[{label}] the pinned tx conn must still be usable after a refused stream"
        );
        match commit(&mut client, 5, tx_id).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] COMMIT after a refused tx-scoped stream: {other:?}"),
        }
    }
}
```

`mysql_targets()` already exists at the top of `mysql_it.rs` and prints `skip: <VAR> unset` per unset
engine via `mysql_url()`/`mariadb_url()`, which satisfies the live-lane no-skip gate. `first_i64` is
the file's existing helper.

**One supporting change this step needs:** the fetch modes are private consts inside the daemon
(`ferrod/src/services/sql.rs:77-79`), so an integration test (a separate crate) cannot see them and
would have to write a literal `2` — a hand-written protocol constant in a test, charter rule 2.
Promote the three to `pub const FETCH_ROWS/FETCH_NONE/FETCH_STREAM: u8` on `ferrod::services::sql`
in this task and import them (`use ferrod::services::sql::FETCH_STREAM;`). Nothing else changes;
they keep their current values and their single definition site.

- [ ] **Step 2: Run it and watch it fail**

```
FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
FERRO_TEST_MARIADB_URL=mysql://ferro:ferro@127.0.0.1:33061/ferro \
  cargo test -p ferrod --test mysql_it mysql_stream_is_refused_identically -- --nocapture
```
Expected: FAIL — the two messages differ (`"fetch=stream is not yet supported on MySQL (lands in M1-S7 with the streaming bridge)"` vs `"MySQL streaming lands in M1-S7"`).

- [ ] **Step 3: Add the capability method to the `PoolBackend` trait**

In `engine/crates/ferro-pool/src/backend.rs`, immediately after `take_session_mutated` (`:193-195`):

```rust
    /// Can this backend produce an INCREMENTAL row stream (`PoolBackend::query_stream`) at all?
    ///
    /// The ONE authority for the `fetch:stream` capability. It exists because the SQL service has
    /// TWO dispatch arms (autocommit and tx-scoped) and, before M1-S8a, only the autocommit one
    /// carried a hand-written `matches!(pool, AnyPool::Mysql(_))` check — so a tx-scoped stream
    /// refused LATE (after checkout + BEGIN), force-tainting the pinned connection on the way out
    /// (`Checkout::query_stream`'s Err arm). Both arms now read THIS method, so a backend that
    /// gains streaming flips one line and both arms follow.
    ///
    /// **Default `true`** — Postgres and the `FakeBackend` stream today and are unchanged.
    fn supports_row_streaming(&self) -> bool {
        true
    }
```

- [ ] **Step 4: Override it in the MySQL backend and retire the stale message strings**

In `engine/crates/ferro-backend-mysql/src/conn.rs`, next to `dialect` (`:263-265`):

```rust
    /// MySQL/MariaDB cannot stream rows incrementally in M1 — DEFERRED, SPEC §22.2 (n).
    ///
    /// Not a policy choice: every `mysql_async` streaming entry point BORROWS the connection, and
    /// the owned-`Conn` route (which does type-check) has **no way to get the `Conn` back** — there
    /// is no `into_inner`/accessor on `ResultSetStream`, and dropping it CLOSES the connection. An
    /// implementation therefore needs a THIRD vendored-fork divergence plus a restructure of
    /// `Checkout::finalize_stream`, which reads `tx_status(&B::Conn)` synchronously after the drain.
    fn supports_row_streaming(&self) -> bool {
        false
    }
```

and replace the body of `query_stream` (`conn.rs:351-361`):

```rust
    async fn query_stream(
        &self,
        _conn: &mut Self::Conn,
        _sql: &str,
        _params: &[Value],
    ) -> Result<(Vec<ColMeta>, Self::RowStream), PoolError> {
        // Unreachable through the SQL service: both arms refuse on `supports_row_streaming()`
        // BEFORE dispatch. Kept as a real error (not `unreachable!()`) so a future direct caller
        // gets a clean refusal rather than a daemon panic.
        Err(PoolError::Unsupported(
            "row streaming is not supported on MySQL/MariaDB (deferred — SPEC §22.2 (n))"
                .to_string(),
        ))
    }
```

- [ ] **Step 5: Make both service arms read that ONE authority through ONE message constructor**

In `engine/crates/ferrod/src/services/sql.rs`, add next to `unsupported` (`:1535`):

```rust
/// THE `fetch:stream`-unsupported terminal. One constructor, read by BOTH the autocommit arm and the
/// tx-scoped forwarding arm, so the two can never drift (they did: the tx arm used to fall through to
/// the backend's own string). Cites §22.2 so an operator can find the deferral rationale.
pub(crate) fn stream_unsupported() -> ErrorPayload {
    unsupported(
        "fetch=stream is not supported on this pool's backend (MySQL/MariaDB row streaming is \
         deferred — SPEC §22.2 (n)); re-issue with fetch=rows",
    )
}
```

Replace the autocommit guard (`sql.rs:347-353`):

```rust
            // ONE authority for the streaming capability (M1-S8a): the backend's own
            // `supports_row_streaming()`, not a `matches!(pool, AnyPool::Mysql(_))` restated here.
            let streams = match pool {
                AnyPool::Pg(p) => p.backend().supports_row_streaming(),
                AnyPool::Mysql(p) => p.backend().supports_row_streaming(),
            };
            if req.fetch == FETCH_STREAM && !streams {
                responder.end_error(stream_unsupported());
                return;
            }
```

In `engine/crates/ferrod/src/tx/mod.rs`, add the flag to `TxHandle`:

```rust
    /// Whether the backend this tx is pinned to can stream rows (`PoolBackend::supports_row_streaming`),
    /// captured at BEGIN. The forwarding handler needs it because `TxHandle` is backend-AGNOSTIC:
    /// without it, a tx-scoped `fetch:stream` on MySQL could only be refused INSIDE the actor —
    /// i.e. after checkout + BEGIN, force-tainting the pinned connection.
    pub streaming: bool,
```

In `engine/crates/ferrod/src/services/sql.rs`'s `begin_on_pool` (`:1240-1255`), set it when registering:

```rust
    tx_registry.register(
        tx_id,
        TxHandle {
            owner: session_id,
            cmd_tx,
            abort: abort.clone(),
            done: done_rx,
            streaming: pool.backend().supports_row_streaming(),
        },
    );
```

and guard the tx-scoped arm (`sql.rs:249`, immediately after `resolve_active` returns the handle and **before** the `TxCommand::ExecStreamed` is built):

```rust
            if req.fetch == FETCH_STREAM && !handle.streaming {
                // Refuse BEFORE handing the `Responder` to the actor: the actor would reach
                // `Checkout::query_stream`, whose Err arm force-taints the PINNED tx connection
                // (`ferro-pool/src/pool.rs:674-677`) for a request that never should have been
                // dispatched. Exactly one END either way (charter rule 4).
                responder.end_error(stream_unsupported());
                return;
            }
```

- [ ] **Step 6: Add the compile-forced capability unit test**

In `engine/crates/ferro-backend-mysql/src/conn.rs`'s `#[cfg(test)] mod tests` (next to `clean_reset_profile_is_full` at `:377-381`):

```rust
    /// The capability is FALSE for MySQL and the refusal message names the current deferral, not a
    /// stale slice number. Behavioural, not a signature assertion: it calls the real method.
    #[test]
    fn mysql_does_not_support_row_streaming() {
        let b = MysqlBackend::new("mysql://x/y".to_string());
        assert!(!b.supports_row_streaming());
    }
```

and in `engine/crates/ferro-backend-pg/src/conn.rs`'s tests:

```rust
    /// PG inherits the trait default and DOES stream — so `supports_row_streaming` can never be
    /// "false everywhere" and silently disable the PG producer.
    #[test]
    fn pg_supports_row_streaming() {
        let b = PgBackend::new("postgres://x/y".to_string());
        assert!(b.supports_row_streaming());
    }
```

- [ ] **Step 7: Run the live test — it must now pass**

```
FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
  cargo test -p ferrod --test mysql_it mysql_stream_is_refused_identically -- --nocapture
cargo test -p ferro-backend-mysql row_streaming
cargo test -p ferro-backend-pg row_streaming
```
Expected: PASS.

- [ ] **Step 8: MUTATION-PROVE the guard**

Temporarily delete the tx-scoped guard block added in Step 5, re-run the live test. Expected: RED (the two messages differ again). Restore it. Then temporarily flip `MysqlBackend::supports_row_streaming` to `true` and re-run: expected RED (`mysql_does_not_support_row_streaming` fails **and** the live test now gets a late backend error). Restore.

- [ ] **Step 9: Record the deferral in SPEC §22.2 (the spec-truth obligation)**

Append to `ferro-spec-v0.2.md` §22.2, after entry `(m)`:

```markdown
  **(n) MySQL/MariaDB row streaming (`fetch:stream`) is DEFERRED out of M1-S8a — with its real cause, and one refusal authority.** `iterate*()` therefore **streams on PostgreSQL and BUFFERS on MySQL** — a documented asymmetry, not a defect (buffering is correct, just memory-hungry). The blocker is structural, not effort: every `mysql_async` streaming entry point BORROWS the connection (`QueryResult::stream`, `stream_and_drop`, `Queryable::exec_stream`, `Query::stream`), and while the OWNED-`Conn` route does type-check (`ResultSetStream<'static,'static,'static, Row, BinaryProtocol>` satisfies `Send + 'static`, proven live), there is **no public way to recover the `Conn` from a finished stream** — no `into_inner`, no accessor (`query_result` is a private field, `QueryResult.conn` is private, `Connection.inner` is `pub(crate)`), and **dropping the stream closes the connection**. An implementation therefore needs a THIRD vendored-fork divergence (today the fork carries exactly one Ferro edit) *and* a restructure of `Checkout::finalize_stream`, which reads `tx_status(&B::Conn)` **synchronously** after the drain and has no way to be handed a connection back through the `BackendRows` trait. The GAT alternative is not type-expressible: `RowStreamHandle` holds `&'a mut Checkout<B>` **and** `B::RowStream`, so a conn-borrowing stream makes it self-referential (the in-tree E0505 precedent for this exact class is recorded at `ferrod/src/services/sql.rs:802-809`). **Two measured facts any future implementation must honour:** (1) `ResultSetStream::affected_rows()` returns the **PREVIOUS statement's** count — `setup_stream` snapshots the OK packet *before any row is read*, measured as `3` and `1` in two independent probes — so `BackendRows::rows_affected()` MUST read `Conn::last_ok_packet()` **post-drain**, never the stream's own accessor, or the engine ships a fabricated row count; and (2) the post-drain terminator for a MySQL SELECT reports `affected = 0` where PG's command tag reports the **row count**, a cross-backend divergence for `Statement::rowCount()`. **What M1-S8a DID fix:** the capability is now ONE authority — `PoolBackend::supports_row_streaming()` (default `true`, `false` for MySQL) — read by BOTH the autocommit and the tx-scoped EXEC arms through ONE terminal constructor. Previously only the autocommit arm guarded (on a restated `matches!(pool, AnyPool::Mysql(_))`), so a **tx-scoped** stream was refused AFTER checkout + BEGIN, inside `Checkout::query_stream`'s Err arm, which **force-taints the pinned transaction connection** — a needless full `COM_RESET_CONNECTION` at the next recycle for a request that never should have been dispatched. Proof: `ferrod`'s `mysql_it.rs::mysql_stream_is_refused_identically_on_both_arms_and_the_tx_survives`.
```

- [ ] **Step 10: Full gate + commit**

```bash
./ci/local-gate.sh --live
git add engine/crates/ferro-pool/src/backend.rs \
        engine/crates/ferro-backend-mysql/src/conn.rs \
        engine/crates/ferro-backend-pg/src/conn.rs \
        engine/crates/ferrod/src/tx/mod.rs \
        engine/crates/ferrod/src/services/sql.rs \
        engine/crates/ferrod/tests/mysql_it.rs \
        ferro-spec-v0.2.md
git commit -m "fix(m1-s8a): one streaming-capability authority; a tx-scoped MySQL stream no longer taints the pinned conn

The autocommit EXEC arm guarded MySQL+fetch:stream pre-checkout; the tx-scoped
arm did not, so it refused after checkout+BEGIN inside Checkout::query_stream's
Err arm, force-tainting the pinned transaction connection. Both arms now read
PoolBackend::supports_row_streaming() through one terminal constructor.

Records the MySQL query_stream deferral in SPEC §22.2 (n) with its real cause
(no into_inner on ResultSetStream; a third fork divergence plus a
finalize_stream restructure) and the measured affected_rows() staleness.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 2: `last_insert_id` end to end (engine half + the PHP accessor) + a two-pool PHP live harness

**Files:**
- Modify: `engine/crates/ferro-pool/src/backend.rs:20-25` (`QueryResult`)
- Modify: `engine/crates/ferro-backend-pg/src/query.rs:121-125`; `engine/crates/ferro-backend-mysql/src/query.rs:73`, `:100-104`
- Modify: `engine/crates/ferrod/src/services/sql.rs:1076` (`build_stream_terminal_body`), `:1492-1513` (`build_terminal_body`), `:1601,1615,1642,1661,1929` (test literals)
- Modify: `engine/crates/ferro-pool/tests/query_guard.rs:48`
- Modify: `php/client/src/Client/ExecCodec.php:82-113` (`decode`), `php/client/src/Client/Connection.php`, `php/client/src/Client/TxHandle.php`
- Modify: `php/client/tests/Live/LiveTestCase.php:40-80,:118-152`; `.github/workflows/ci.yml` (`php` job)
- Test: `engine/crates/ferro-backend-mysql/tests/query_it.rs`, `php/client/tests/Live/LastInsertIdLiveTest.php`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `ferro_pool::backend::QueryResult.last_insert_id: Option<u64>`. `ferrod::services::sql::last_insert_id_value(id: Option<u64>) -> Option<ferro_proto::value::Value>`. PHP `ExecCodec::decode()` now returns `array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}`. PHP `Connection::lastInsertId(): int|string|null` and `TxHandle::lastInsertId(): int|string|null`. PHP `LiveTestCase::mysqlPool(): string` (the second pool's name, `'mysql'`).

- [ ] **Step 1: Write the failing engine test**

Append to `engine/crates/ferro-backend-mysql/tests/query_it.rs`:

```rust
/// The OK packet's `LAST_INSERT_ID()` must reach `QueryResult`. It CANNOT be recovered later: in a
/// transaction-mode pool a follow-up `SELECT LAST_INSERT_ID()` lands on a DIFFERENT connection and
/// returns 0 (measured), so this is the only place the value exists.
#[tokio::test]
async fn insert_carries_last_insert_id_on_query_result() {
    let Some(url) = std::env::var("FERRO_TEST_MYSQL_URL").ok() else {
        println!("skip: FERRO_TEST_MYSQL_URL unset");
        return;
    };
    let (backend, mut conn) = connect(&url).await;
    backend
        .simple_query(&mut conn, "DROP TABLE IF EXISTS s8a_lid")
        .await
        .unwrap();
    backend
        .simple_query(
            &mut conn,
            "CREATE TABLE s8a_lid (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)",
        )
        .await
        .unwrap();

    let r1 = backend
        .query(&mut conn, "INSERT INTO s8a_lid (v) VALUES (?)", &[Value::I64(1)])
        .await
        .expect("insert 1");
    let r2 = backend
        .query(&mut conn, "INSERT INTO s8a_lid (v) VALUES (?)", &[Value::I64(2)])
        .await
        .expect("insert 2");

    let id1 = r1.last_insert_id.expect("MySQL INSERT must carry a last_insert_id");
    let id2 = r2.last_insert_id.expect("MySQL INSERT must carry a last_insert_id");
    assert_eq!(id2, id1 + 1, "AUTO_INCREMENT ids must advance ({id1} -> {id2})");

    // A SELECT carries none — the field is not a stale carry-over from an earlier statement.
    let r3 = backend
        .query(&mut conn, "SELECT v FROM s8a_lid ORDER BY id", &[])
        .await
        .expect("select");
    assert_eq!(
        r3.last_insert_id, None,
        "a SELECT must not report a last_insert_id"
    );
}
```

- [ ] **Step 2: Run it and watch it fail to COMPILE**

```
cargo test -p ferro-backend-mysql --test query_it insert_carries_last_insert_id
```
Expected: FAIL — `no field 'last_insert_id' on type 'QueryResult'`.

- [ ] **Step 3: Add the field and fill it**

`engine/crates/ferro-pool/src/backend.rs:20-25`:

```rust
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueryResult {
    pub cols: Vec<ColMeta>,
    pub rows: Vec<Vec<Value>>,
    pub affected: u64,
    /// The auto-generated key this statement produced, when the BACKEND PROTOCOL reports one
    /// (M1-S8a). MySQL/MariaDB fill it from the OK packet's `LAST_INSERT_ID()`; Postgres always
    /// leaves it `None` (PG has no such protocol field — callers use `INSERT … RETURNING`).
    ///
    /// **It cannot be recovered by a follow-up query.** Measured live on a transaction-mode pool:
    /// `SELECT LAST_INSERT_ID()` after an INSERT returned **0**, and PG's `SELECT lastval()` threw
    /// `55000` — the follow-up statement lands on a DIFFERENT pooled connection. So it is carried
    /// here or it is lost.
    pub last_insert_id: Option<u64>,
}
```

`engine/crates/ferro-backend-mysql/src/query.rs:73` — stop discarding it:

```rust
    let (raw_rows, affected, last_insert_id) = match drain(conn, &stmt, bound).await {
        Ok(t) => t,
        Err(e) => return Err(conn.map_stmt_error(&e)),
    };
```

and `query.rs:100-104`:

```rust
    Ok(QueryResult {
        cols,
        rows,
        affected,
        last_insert_id,
    })
```

`engine/crates/ferro-backend-pg/src/query.rs:121-125`:

```rust
    Ok(QueryResult {
        cols,
        rows,
        affected,
        // PG has no LAST_INSERT_ID protocol field; the idiomatic form is `INSERT … RETURNING id`,
        // which arrives as an ordinary row. Explicit `None` (not `..Default::default()`) so a
        // future RETURNING-aware path is a visible edit here.
        last_insert_id: None,
    })
```

Every remaining struct literal (`ferro-pool/tests/query_guard.rs:48`, `ferrod/src/services/sql.rs:1601,1615,1642,1661,1929`) gains `..Default::default()` or an explicit `last_insert_id: None`.

- [ ] **Step 4: Thread it onto the wire terminal through ONE conversion**

In `engine/crates/ferrod/src/services/sql.rs`, add next to `build_terminal_body`:

```rust
/// THE `Option<u64>` → wire `Option<Value>` conversion for `ExecOk.last_insert_id`. One site, so the
/// tag choice cannot diverge between the buffered and the streamed terminal.
///
/// `I64` while the id fits (the overwhelmingly common case, and the shape the golden vector
/// `sql_exec_response_lastid.json` already locks — a plain PHP `int` on the client), `U64` only above
/// `i64::MAX`, which a `BIGINT UNSIGNED` AUTO_INCREMENT can legally reach. Saturating into `I64`
/// there would be a silent wrong key.
fn last_insert_id_value(id: Option<u64>) -> Option<Value> {
    id.map(|n| match i64::try_from(n) {
        Ok(v) => Value::I64(v),
        Err(_) => Value::U64(n),
    })
}
```

`build_terminal_body` (`sql.rs:1507-1513`):

```rust
    let mut exec_ok = ExecOk {
        cols: result.cols,
        rows,
        affected: result.affected,
        last_insert_id: last_insert_id_value(result.last_insert_id),
        stats: Stats { queue_us, exec_us, rows: nrows, bytes: 0 },
    };
```

`build_stream_terminal_body` (`sql.rs:1063-1076`) gains a `last_insert_id: Option<u64>` parameter routed through the **same** helper, and its callers pass the value the stream's `finish()` reported (`None` today on every streaming backend — PG has no such field and MySQL does not stream, Task 1):

```rust
        last_insert_id: last_insert_id_value(last_insert_id),
```

- [ ] **Step 5: Add the conversion's boundary unit test**

In `sql.rs`'s `#[cfg(test)] mod tests`:

```rust
    /// The I64/U64 boundary is exact and a `None` stays `None`. `i64::MAX` is the last I64 value;
    /// `i64::MAX as u64 + 1` is the first U64 one.
    #[test]
    fn last_insert_id_value_picks_the_narrowest_truthful_tag() {
        assert_eq!(last_insert_id_value(None), None);
        assert_eq!(last_insert_id_value(Some(0)), Some(Value::I64(0)));
        assert_eq!(last_insert_id_value(Some(200)), Some(Value::I64(200)));
        assert_eq!(
            last_insert_id_value(Some(i64::MAX as u64)),
            Some(Value::I64(i64::MAX))
        );
        assert_eq!(
            last_insert_id_value(Some(i64::MAX as u64 + 1)),
            Some(Value::U64(9_223_372_036_854_775_808)),
            "above i64::MAX the tag must widen, never saturate"
        );
        assert_eq!(last_insert_id_value(Some(u64::MAX)), Some(Value::U64(u64::MAX)));
    }
```

- [ ] **Step 6: Surface it in the PHP client**

`php/client/src/Client/ExecCodec.php` — extend `decode`'s return shape (`:82-113`):

```php
    /**
     * Decode an `Ok` {@see ExecOk} body into column names + value-policy-decoded rows.
     *
     * `last_insert_id` is returned RAW (`int|string|null`), deliberately NOT through the
     * {@see \Ferro\Client\Value\ValuePolicy}: it is a scalar terminal field, not a column, and the
     * DBAL contract for `lastInsertId()` is `int|string`. A value above `PHP_INT_MAX` arrives as the
     * canonical decimal string (the engine widens the tag to `U64` only there — see
     * `ferrod`'s `last_insert_id_value`).
     *
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}
     */
    public function decode(Outcome $outcome): array
    {
        // ... unchanged up to the return ...
        return [
            'cols' => $cols,
            'rows' => $rows,
            'affected' => SqlValueCodec::toInt($ok['affected'] ?? 0),
            'last_insert_id' => self::rawLastInsertId($ok['last_insert_id'] ?? null),
        ];
    }

    /**
     * Narrow the ALREADY-DECODED `last_insert_id` cell to the raw scalar.
     *
     * **It must NOT call `SqlValueCodec::fromWire` again.** `ExecOk::mapFromWire` has already run it
     * (`ExecOk.php:57`), so what arrives here is a decoded `['tag' => int, 'data' => mixed]` cell,
     * not a wire pair. Re-decoding happens to be idempotent for an `I64`, which is why the mistake
     * is invisible in the common case — but it is a real fault for any cell whose `data` is not
     * itself a valid wire value, and it makes this method's contract a lie about what it receives.
     *
     * `null` (no id) stays null; otherwise the payload is an `int` (I64) or an `int|string` (U64
     * above `PHP_INT_MAX`, which arrives as its canonical decimal string). Deliberately no coercion:
     * a malformed payload is a wire fault, not a silently-zeroed key.
     *
     * @param array{tag: int, data: mixed}|null $cell the decoded cell from {@see ExecOk::mapFromWire}
     */
    private static function rawLastInsertId(?array $cell): int|string|null
    {
        if ($cell === null) {
            return null;
        }
        $data = $cell['data'] ?? null;
        if ($data === null || is_int($data) || is_string($data)) {
            return $data;
        }
        throw new CodecException(
            'ExecOk.last_insert_id: expected an int or decimal string, got ' . get_debug_type($data),
        );
    }
```

The call site is correspondingly `self::rawLastInsertId(is_array($ok['last_insert_id'] ?? null) ? $ok['last_insert_id'] : null)` — PHPStan level 9 will not accept a bare `mixed` here, and the narrowing is exactly the shape assertion this method wants.

`php/client/src/Client/Connection.php` — record it on every EXEC and expose it:

```php
    /** The auto-generated key the LAST statement on this connection reported, or null. */
    private int|string|null $lastInsertId = null;

    /**
     * The auto-generated key produced by the most recent statement on this connection, or `null`
     * when the backend reported none.
     *
     * MySQL/MariaDB report it on the OK packet of an `INSERT` into an `AUTO_INCREMENT` table.
     * **PostgreSQL always reports `null`** — it has no such protocol field; the idiomatic form is
     * `INSERT … RETURNING id`, which comes back as an ordinary row. This is NOT emulated with a
     * follow-up query: on a transaction-mode pool `SELECT LAST_INSERT_ID()` returns 0 and
     * `SELECT lastval()` throws `55000`, because the follow-up lands on a different connection.
     */
    public function lastInsertId(): int|string|null
    {
        return $this->lastInsertId;
    }
```

Every place `Connection` decodes an EXEC terminal (`dispatchAutocommit`, and the tx delegation added in Task 9) sets `$this->lastInsertId = $decoded['last_insert_id'];`. `TxHandle::run` gains the same field + a `lastInsertId()` accessor so a tx-scoped INSERT is reachable too.

- [ ] **Step 7: Give the PHP live harness a second (MySQL) pool**

`php/client/tests/Live/LiveTestCase.php` — replace the single-DSN launch (`:132-152`) and the skip guard (`:52-60`):

```php
    /** The second pool's name, used by tests that need a MySQL-family backend. */
    protected const MYSQL_POOL = 'mysql';

    private string $mysqlUrl = '';

    protected function setUp(): void
    {
        $pgUrl = getenv('FERRO_TEST_PG_URL');
        if (!is_string($pgUrl) || $pgUrl === '') {
            $this->markTestSkipped('FERRO_TEST_PG_URL is unset — skipping live ferrod tests');
        }
        $mysqlUrl = getenv('FERRO_TEST_MYSQL_URL');
        // MySQL is OPTIONAL for the harness (so a PG-only dev loop still runs) but MANDATORY in CI:
        // the `php` job provisions it and `--fail-on-skipped` turns a missing pool into a red lane.
        $this->mysqlUrl = is_string($mysqlUrl) ? $mysqlUrl : '';
        // ... rest unchanged ...
    }

    /** Skip-with-reason helper for a test that needs the MySQL pool. */
    protected function requireMysqlPool(): string
    {
        if ($this->mysqlUrl === '') {
            $this->markTestSkipped('FERRO_TEST_MYSQL_URL is unset — skipping the MySQL-pool live test');
        }
        return self::MYSQL_POOL;
    }

    private function launchFerrod(string $bin, string $pgUrl): void
    {
        $env = getenv();
        $env['FERRO_SOCK'] = $this->socketPath;
        // `ferrod` resolves per-pool DSNs from FERRO_POOL_<env_name(NAME)>_DSN and infers the KIND
        // from the scheme (there is no kind= knob) — engine/crates/ferrod/src/config.rs:88-104,:332.
        $env['FERRO_POOLS'] = $this->mysqlUrl === '' ? 'default' : 'default,' . self::MYSQL_POOL;
        $env['FERRO_POOL_DEFAULT_DSN'] = $pgUrl;
        if ($this->mysqlUrl !== '') {
            $env['FERRO_POOL_MYSQL_DSN'] = $this->mysqlUrl;
        }
        // ... proc_open unchanged ...
    }
```

`launchFerrod` is also called from `restartFerrod` (`:79-84`) — it already re-passes `$this->pgUrl`, and `$this->mysqlUrl` is read from `$this`, so the restart path picks the second pool up unchanged.

- [ ] **Step 8: Write the PHP live test**

`php/client/tests/Live/LastInsertIdLiveTest.php`:

```php
<?php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;

final class LastInsertIdLiveTest extends LiveTestCase
{
    public function testMysqlInsertReportsAnAdvancingLastInsertId(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->connectConnection(null, $pool);

        $c->exec('DROP TABLE IF EXISTS s8a_lid_php');
        $c->exec('CREATE TABLE s8a_lid_php (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)');
        $this->assertNull($c->lastInsertId(), 'DDL reports no generated key');

        $c->exec('INSERT INTO s8a_lid_php (v) VALUES (?)', [1]);
        $first = $c->lastInsertId();
        $this->assertIsInt($first);
        $this->assertGreaterThan(0, $first);

        $c->exec('INSERT INTO s8a_lid_php (v) VALUES (?)', [2]);
        $this->assertSame($first + 1, $c->lastInsertId(), 'AUTO_INCREMENT must advance');

        // A read must not leave a stale key behind.
        $c->query('SELECT v FROM s8a_lid_php ORDER BY id');
        $this->assertNull($c->lastInsertId(), 'a SELECT reports no generated key');
    }

    public function testPostgresReportsNullAndRetainsTheReturningRow(): void
    {
        $c = $this->connectConnection();
        $c->exec('DROP TABLE IF EXISTS s8a_lid_pg');
        $c->exec('CREATE TABLE s8a_lid_pg (id serial PRIMARY KEY, v int)');

        $rows = $c->query('INSERT INTO s8a_lid_pg (v) VALUES (1) RETURNING id');
        $this->assertNull(
            $c->lastInsertId(),
            'PG has no LAST_INSERT_ID protocol field — RETURNING is the documented route',
        );
        $this->assertIsInt($rows[0]['id']);
    }
}
```

(`connectConnection` gains an optional `?string $pool` argument defaulting to `'default'`.)

- [ ] **Step 9: Provision MySQL in the PHP CI lane**

`.github/workflows/ci.yml`, the `php` job (`:78-102`):

```yaml
    env:
      FERRO_TEST_PG_URL: postgres://ferro:ferro@127.0.0.1:55432/ferro
      FERRO_TEST_MYSQL_URL: mysql://ferro:ferro@127.0.0.1:33060/ferro
    steps:
      # ...
      - run: docker compose -f testkit/docker-compose.yml up -d --wait pg mysql
```

- [ ] **Step 10: Run everything, then MUTATION-PROVE**

```
cargo test -p ferro-backend-mysql --test query_it insert_carries_last_insert_id
cargo test -p ferrod last_insert_id_value_picks_the_narrowest
(cd php/client && ./vendor/bin/phpunit tests/Live/LastInsertIdLiveTest.php)
```
Then mutate:

1. Change `last_insert_id_value`'s `U64` arm to `Value::I64(n as i64)` → the boundary unit test goes RED.
2. Change `ferro-backend-mysql/src/query.rs` back to `_last_insert_id` → the engine live test goes RED.
3. Drop `'last_insert_id'` from `ExecCodec::decode`'s return → PHPStan L9 and the PHP live test both go RED.
4. **The PG-`null` guard is a NEGATIVE and needs its own mutation** (weak guard 9): on its own, `assertNull($c->lastInsertId())` on Postgres can only fail if someone later wires PG — so prove it can. Set `last_insert_id: Some(1)` in `ferro-backend-pg/src/query.rs`'s `QueryResult` literal → `testPostgresReportsNullAndRetainsTheReturningRow` goes RED. That is the assertion's whole job: it is the lock that stops a future "helpful" PG emulation (a `lastval()` follow-up) from landing silently, and hazard 21 records why such an emulation is *wrong*, not merely unnecessary.
5. Reinstate the double-decode in `rawLastInsertId` (`SqlValueCodec::fromWire($cell)`) → PHPStan L9 goes RED on the argument type before any test runs.

Restore each.

- [ ] **Step 11: Full gate + commit**

```bash
./ci/local-gate.sh --live
git add engine php .github/workflows/ci.yml
git commit -m "feat(m1-s8a): carry last_insert_id from the OK packet to the client

QueryResult gains the field, MySQL stops discarding it, and ferrod converts it
through one site (I64 while it fits, U64 above i64::MAX). PG stays None — it has
no such protocol field and the value cannot be recovered by a follow-up query on
a transaction-mode pool (measured: LAST_INSERT_ID() -> 0, lastval() -> 55000).

The PHP live harness now launches a second, MySQL pool, and the php CI lane
provisions MySQL alongside PG.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 3: Vendor errno on the wire (engine → `fate.rs` → PHP), with the first non-null-errno golden vector

**Files:**
- Modify: `engine/crates/ferro-pool/src/error.rs:37-43` (`PoolError::Sql`)
- Modify (compile cascade — the FULL list, hazards 14/15 as corrected): `engine/crates/ferro-backend-pg/src/error_map.rs:37-42`, `engine/crates/ferro-backend-pg/src/query.rs:135-140`, `engine/crates/ferro-backend-mysql/src/error_map.rs:58-63`, `engine/crates/ferro-backend-mysql/src/bind.rs:309-314` **and `:647-652`** (an exhaustive destructure with no `..`), `engine/crates/ferro-pool/src/fake.rs:639`, `engine/crates/ferrod/src/services/sql.rs:1120-1125` (`stream_cancel_error`), `engine/crates/ferrod/src/tx/actor.rs:1034,1096`, `engine/crates/ferrod/src/services/fate.rs:226-231,238-243,246-253,496`
- Modify (the fix site): `engine/crates/ferrod/src/services/fate.rs:120-133`
- Modify: `engine/crates/ferro-proto/src/bin/gen_vectors.rs` (new `error_mysql_errno` case)
- Create: `proto/vectors/error_mysql_errno.json` (generated)
- Modify: `proto/PROTOCOL.md` §7 vector index (`:195`)
- Modify: `php/client/src/Client/Error/CarriesErrorPayload.php`
- Modify: `php/client/tests/Conformance/VectorConformanceTest.php:389` (`$namedLocked` becomes DERIVED) + a new byte-lock test
- Test: `engine/crates/ferrod/src/services/fate.rs` unit; `engine/crates/ferro-backend-pg/tests/pg_query_it.rs` (the live PG no-errno gate); `php/client/tests/Live/ErrnoLiveTest.php`

**Interfaces:**
- Consumes: `LiveTestCase::requireMysqlPool()` and `connectConnection(?RetryPolicy, ?string $pool)` from Task 2.
- Produces: `PoolError::Sql { code: u16, branch: u8, sqlstate: Option<String>, errno: Option<i32>, message: String }`. PHP `CarriesErrorPayload::errno(): ?int`.

- [ ] **Step 1: Write the failing fate-matrix unit test**

In `engine/crates/ferrod/src/services/fate.rs`'s `mod tests`:

```rust
    /// `classify_fate` is the ONE place a `PoolError` becomes a wire `ErrorPayload`. A MySQL `Sql`
    /// error's vendor errno must reach the wire VERBATIM alongside the SQLSTATE — DBAL's MySQL
    /// `ExceptionConverter` matches on the errno EXCLUSIVELY, and MySQL's SQLSTATEs cannot
    /// substitute (a duplicate key and a NOT NULL violation both arrive as `23000`).
    #[test]
    fn a_sql_errors_vendor_errno_reaches_the_wire_payload() {
        let dup = PoolError::Sql {
            code: errc::UNIQUE,
            branch: errc::UNIQUE_BRANCH,
            sqlstate: Some("23000".to_string()),
            errno: Some(1062),
            message: "Duplicate entry '1' for key 'PRIMARY'".to_string(),
        };
        let p = classify_fate(dup, ctx(false, true, false));
        assert_eq!(p.errno, Some(1062), "the vendor errno must pass through verbatim");
        assert_eq!(p.sqlstate.as_deref(), Some("23000"));
        assert_eq!(p.code, errc::UNIQUE);
    }

    /// `classify_fate` MIRRORS the errno and never invents one.
    ///
    /// **Why this shape and not `a_postgres_sql_error_carries_no_errno`** (probe 2, weak guard 1):
    /// that test fed in a `PoolError` the test itself built with `errno: None` (via the `sql()`
    /// helper) and asserted `None` came out — a TAUTOLOGY. It could not fail for any change to
    /// `fate.rs`. This one drives BOTH arms of the mirror across a table, so hard-coding either
    /// `errno: None` or a derived value in the `Sql` arm goes RED; and it pins that the arms which
    /// have no backend behind them (`ConnectionLost`, `Timeout`) report `None` — the property that
    /// would break if someone "helpfully" defaulted the field.
    ///
    /// The claim that *PostgreSQL* never produces one is proven where the PG `PoolError` is BUILT,
    /// against a real server — see Step 9's `pg_query_it.rs` gate — not here.
    #[test]
    fn classify_fate_mirrors_the_errno_and_never_invents_one() {
        for want in [Some(1062), Some(1213), Some(1205), None] {
            let e = PoolError::Sql {
                code: errc::UNIQUE,
                branch: errc::UNIQUE_BRANCH,
                sqlstate: Some("23000".to_string()),
                errno: want,
                message: "x".to_string(),
            };
            let p = classify_fate(e, ctx(false, true, false));
            assert_eq!(p.errno, want, "the Sql arm must mirror the errno verbatim");
        }
        // The arms with no backend error behind them report None — they have nothing to report.
        for e in [PoolError::ConnectionLost, PoolError::Timeout] {
            let p = classify_fate(e, ctx(true, false, false));
            assert_eq!(
                p.errno, None,
                "a non-Sql PoolError has no vendor errno and must not fabricate one"
            );
        }
    }
```

`ctx(readonly, sent, in_tx)` is the existing test helper at `fate.rs:255-262`; use it rather than
re-spelling `OpContext { .. }`, so a future field addition breaks one place.

- [ ] **Step 2: Run it and watch it fail to COMPILE**

```
cargo test -p ferrod a_sql_errors_vendor_errno
```
Expected: FAIL — `struct 'PoolError::Sql' has no field named 'errno'`.

- [ ] **Step 3: Add the slot to `PoolError::Sql`**

`engine/crates/ferro-pool/src/error.rs:37-43`:

```rust
    #[error("sql error {code:#06x} (sqlstate {sqlstate:?}, errno {errno:?}): {message}")]
    Sql {
        code: u16,
        branch: u8,
        sqlstate: Option<String>,
        /// The backend's own numeric error code, when it HAS one (M1-S8a).
        ///
        /// MySQL/MariaDB do: `mysql_async::ServerError.code` is a `u16`, widened losslessly here to
        /// match the wire field (`ErrorPayload.errno: Option<i32>`). **PostgreSQL does not** — its
        /// error identity is the five-character SQLSTATE, so this stays `None` there forever, and so
        /// it does on every client-side bind pre-flight rejection (no server ever saw the statement).
        ///
        /// Why it must reach the wire at all: MySQL's SQLSTATEs are far coarser than its errnos —
        /// a duplicate key and a NOT NULL violation BOTH arrive as `23000` (measured) — so a
        /// consumer keyed on SQLSTATE alone cannot tell them apart. `classify_fate` passes it
        /// through verbatim; nothing re-derives a classification from it.
        errno: Option<i32>,
        message: String,
    },
```

`taxonomy_branch()` (`error.rs:76`) and `errc()` (`error.rs:96`) already destructure with `..` — unchanged.

- [ ] **Step 4: Fill it at the ONE producing site and `None` everywhere else**

`engine/crates/ferro-backend-mysql/src/error_map.rs:55-64`:

```rust
        Error::Server(se) => {
            let sqlstate = se.state.clone();
            let (code, branch) = classify_errno(se.code, &sqlstate);
            PoolError::Sql {
                code,
                branch,
                sqlstate: Some(sqlstate),
                // The RAW vendor errno, carried alongside the classification rather than consumed by
                // it (M1-S8a). `u16` -> `i32` is lossless.
                errno: Some(i32::from(se.code)),
                message: se.message.clone(),
            }
        }
```

Also update the module doc at `error_map.rs:25-29`, which currently says the errno is "used here purely as the classification KEY" and defers the wire carry — that statement is now false.

`ferro-backend-pg/src/error_map.rs:37-42` and `ferro-backend-pg/src/query.rs:135-140` gain `errno: None` with the one-line reason (PG has no integer errno / the server never saw the statement). `ferro-backend-mysql/src/bind.rs:309-314` (its pre-send `bind_error`) gains `errno: None` for the same reason. `ferro-pool/src/fake.rs:639` gains `errno: None`. Every test literal in the cascade list gains `errno: None` except where the test is about the errno.

- [ ] **Step 5: Thread it through the ONE `PoolError` → `ErrorPayload` site**

`engine/crates/ferrod/src/services/fate.rs:120-133` — this is the only exhaustive `Sql` destructure with no `..`, which is why the compiler forced you here:

```rust
        PoolError::Sql {
            code,
            branch,
            sqlstate,
            errno,
            message,
        } => ErrorPayload {
            code,
            branch,
            sqlstate,
            // Verbatim. The taxonomy `code`/`branch` were already derived from it upstream
            // (`ferro-backend-mysql`'s `classify_errno`); this carries the RAW value so a consumer
            // that needs vendor-level identity — e.g. a Doctrine MySQL ExceptionConverter, which
            // matches on the errno EXCLUSIVELY — has it. Nothing downstream re-classifies from it.
            errno,
            message,
            detail: None,
            retry_after_ms: None,
        },
```

Every other `ErrorPayload` construction site keeps `errno: None`: `fate.rs:166-174` (`Unsupported`), `:178-186` (`Backend`), `:208-218` (the shared `payload` helper covering `ConnectionLost`/`Timeout`/`Closed`/`57014`), `sql.rs:1535-1571`, `session/mod.rs:618,657-658`, `session/error.rs:62-63`, `session/classify.rs:107`, `session/supervisor.rs:92`. None of them originate at a backend, so none has an errno to report.

- [ ] **Step 6: Add the golden vector (charter rule 2 — same change set)**

In `engine/crates/ferro-proto/src/bin/gen_vectors.rs`, immediately after the `error_protocol` case (`:313-333`):

```rust
    // The FIRST vector locking a NON-NULL errno + a real SQLSTATE together. Shape: a MySQL duplicate
    // key — errno 1062, SQLSTATE 23000 — the pair a Doctrine MySQL ExceptionConverter keys on, and
    // the pair that proves the two fields are independent on the wire (23000 alone cannot
    // distinguish a dup key from a NOT NULL violation).
    let err_mysql = ErrorPayload {
        code: consts::errc::UNIQUE,
        branch: consts::errc::UNIQUE_BRANCH,
        sqlstate: Some("23000".into()),
        errno: Some(1062),
        message: "Duplicate entry '1' for key 'PRIMARY'".into(),
        detail: None,
        retry_after_ms: None,
    };
    write_case(
        "error_mysql_errno",
        flags::END,
        service::SQL,
        method_sql::EXEC,
        21,
        Outcome::Error(err_mysql).encode(),
        serde_json::json!({ "status": consts::outcome::ERROR, "error": {
            "code": consts::errc::UNIQUE, "branch": consts::errc::UNIQUE_BRANCH,
            "sqlstate":"23000", "errno":1062, "message":"Duplicate entry '1' for key 'PRIMARY'",
            "detail":null, "retry_after_ms":null } }),
    );
```

Regenerate and review the diff:

```bash
cargo run -p ferro-proto --bin gen_vectors
git status --short proto/vectors
```
Exactly one new file (`proto/vectors/error_mysql_errno.json`) and no change to any existing vector.

- [ ] **Step 7: Byte-lock it on the PHP side, and REGISTER it in the accounting guard**

`php/client/tests/Conformance/VectorConformanceTest.php` — add the byte-lock test next to `testErrorProtocolVectorByteMatchesBothDirections` (`:304-333`):

```php
    /**
     * `error_mysql_errno`: the first vector carrying a NON-NULL `errno` alongside a SQLSTATE. Locks
     * that PHP's `ErrorPayload` moves BOTH fields, independently, in both directions — the pair a
     * Doctrine MySQL ExceptionConverter keys on (it matches the errno EXCLUSIVELY; MySQL's `23000`
     * covers both a duplicate key and a NOT NULL violation, so the SQLSTATE cannot substitute).
     */
    public function testErrorMysqlErrnoVectorByteMatchesBothDirections(): void
    {
        $v = self::loadVector('error_mysql_errno.json');
        $message = is_array($v['message']) ? $v['message'] : [];
        $errorFields = is_array($message['error'] ?? null) ? $message['error'] : [];
        $payload = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $p = new PurePacker();

        $encoded = Outcome::error(ErrorPayload::fromArray($errorFields))->encode($p);
        $this->assertSame(bin2hex($payload), bin2hex($encoded),
            'PHP Outcome::Error(ErrorPayload) encode must byte-match error_mysql_errno');

        $err = Outcome::decode($payload, $p)->errorPayload();
        $this->assertSame(1062, $err->errno, 'the vendor errno must survive the round trip');
        $this->assertSame('23000', $err->sqlstate, 'the SQLSTATE is carried independently');
        $this->assertEquals($errorFields, $err->toArray());
        $this->assertSame(bin2hex($payload), bin2hex(Outcome::error($err)->encode($p)),
            'error_mysql_errno decode->encode fixpoint');
    }
```

and fix `$namedLocked` at `:389` — **do not just append a string.**

The v1 instruction was to write `$namedLocked = ['tx_begin_response', 'error_protocol', 'error_mysql_errno'];`. That is the project's dominant defect class again (probe 2, major): the list is **NAME-MEMBERSHIP ONLY**, so adding a name is sufficient to make `testEveryCommittedVectorIsByteLocked` green **whether or not a byte-lock test for that vector exists**. The guard would then certify an unlocked vector — the exact "dead registry key" shape the M1-S7 review found.

Derive it from the test source instead, so a name can only be "locked" if a `loadVector('<name>.json')` call actually exists in this file:

```php
        // DERIVED, not a parallel list (charter: a completeness check must be able to fail).
        // Every byte-lock test in this class reaches its fixture through `self::loadVector('X.json')`,
        // so the set of name-locked vectors IS the set of names that call appears with. Adding a
        // name to a hand-written array would have made this guard certify a vector that has NO
        // byte-lock test at all — which is precisely the failure mode it exists to catch.
        $namedLocked = self::namesLoadedByAByteLockTest();
        foreach (self::txRequestVectors() as [$v]) { $namedLocked[] = (string) $v['name']; }
```

```php
    /**
     * The vector names some test in THIS file loads by name, scraped from its own source.
     *
     * Deliberately a source scan and not a constant: the thing being asserted is "a byte-lock test
     * exists for this vector", and the only honest evidence of that is a call site. `__FILE__` keeps
     * it self-referential, so the scan cannot drift from the file it is scanning.
     *
     * @return list<string>
     */
    private static function namesLoadedByAByteLockTest(): array
    {
        $src = (string) file_get_contents(__FILE__);
        $m = [];
        preg_match_all("/loadVector\\(\\s*'([A-Za-z0-9_]+)\\.json'\\s*\\)/", $src, $m);
        return array_values(array_unique($m[1]));
    }
```

**Do the whole edit deliberately, not reflexively, and record the intermediate state:** run the conformance suite right after Step 6 (vector committed, no byte-lock test yet) and confirm `testEveryCommittedVectorIsByteLocked` goes **RED** naming `error_mysql_errno`. Then add the byte-lock test above and confirm it goes green **without** touching any list. That two-step observation is the accounting guard proving it can fail — and it is what a hand-written append would have hidden.

- [ ] **Step 8: Expose `errno()` on the PHP exception surface**

`php/client/src/Client/Error/CarriesErrorPayload.php`:

```php
    public function __construct(private readonly ErrorPayload $errorPayload)
    {
        parent::__construct(sprintf(
            '%s (code=%d, branch=%d%s%s)',
            $errorPayload->message,
            $errorPayload->code,
            $errorPayload->branch,
            $errorPayload->sqlstate !== null ? ', sqlstate=' . $errorPayload->sqlstate : '',
            $errorPayload->errno !== null ? ', errno=' . $errorPayload->errno : '',
        ));
    }

    /**
     * The backend's own numeric error code when it has one, else null.
     *
     * MySQL/MariaDB supply it (`1062` duplicate key, `1213` deadlock, `1205` lock-wait timeout, …);
     * **PostgreSQL never does** — its error identity is the SQLSTATE, so this is `null` on every PG
     * error by construction, not by omission. A consumer that must distinguish MySQL errors keyed on
     * `23000` (duplicate key vs NOT NULL) has to read this, not {@see sqlstate}.
     */
    public function errno(): ?int { return $this->errorPayload->errno; }
```

- [ ] **Step 9: Write the live gates (both engine families, engine-side AND through the real client)**

First, the engine-side PG negative — the guard that replaces the tautology deleted in Step 1. It is driven by a REAL server error, so it fails the moment anyone starts filling `errno` on the PG path. Append to `engine/crates/ferro-backend-pg/tests/pg_query_it.rs`:

```rust
/// PostgreSQL has no integer error code — its error identity is the five-character SQLSTATE — so
/// `PoolError::Sql.errno` is `None` on PG **by construction**, not by omission.
///
/// This is deliberately driven by a real server error rather than a hand-built `PoolError`: an
/// assertion over an input the test itself constructed with `errno: None` cannot fail (probe 2,
/// weak guard 1). Here the value comes off `error_map::map` on a genuine 42601, so wiring any PG
/// errno — a fabricated one, a hash of the SQLSTATE — turns this RED.
#[tokio::test]
async fn a_real_pg_server_error_carries_no_errno() {
    let Some(url) = std::env::var("FERRO_TEST_PG_URL").ok() else {
        println!("skip: FERRO_TEST_PG_URL unset");
        return;
    };
    let (backend, mut conn) = connect(&url).await;
    let err = backend
        .query(&mut conn, "SELEKT 1", &[])
        .await
        .expect_err("a syntax error");
    match err {
        PoolError::Sql { sqlstate, errno, .. } => {
            assert_eq!(sqlstate.as_deref(), Some("42601"), "PG identifies by SQLSTATE");
            assert_eq!(errno, None, "PG has no integer errno — None by construction");
        }
        other => panic!("expected a known-fate Sql error, got {other:?}"),
    }
}
```

Then the client-side gate, `php/client/tests/Live/ErrnoLiveTest.php`:

```php
<?php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Error\NonRetryableException;

final class ErrnoLiveTest extends LiveTestCase
{
    public function testMysqlDuplicateKeyCarriesErrno1062AlongsideSqlstate23000(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->connectConnection(null, $pool);
        $c->exec('DROP TABLE IF EXISTS s8a_errno');
        $c->exec('CREATE TABLE s8a_errno (id INT PRIMARY KEY)');
        $c->exec('INSERT INTO s8a_errno (id) VALUES (1)');

        try {
            $c->exec('INSERT INTO s8a_errno (id) VALUES (1)');
            $this->fail('the duplicate insert must be rejected');
        } catch (NonRetryableException $e) {
            $this->assertSame('23000', $e->sqlstate());
            $this->assertSame(1062, $e->errno(), 'the vendor errno must reach the client');
        }
    }

    public function testMysqlNotNullAndDuplicateShareASqlstateButNotAnErrno(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->connectConnection(null, $pool);
        $c->exec('DROP TABLE IF EXISTS s8a_errno_nn');
        $c->exec('CREATE TABLE s8a_errno_nn (id INT PRIMARY KEY, v INT NOT NULL)');

        $errno = null;
        $sqlstate = null;
        try {
            $c->exec('INSERT INTO s8a_errno_nn (id, v) VALUES (1, NULL)');
        } catch (NonRetryableException $e) {
            $errno = $e->errno();
            $sqlstate = $e->sqlstate();
        }
        // THE reason the errno has to be on the wire at all.
        $this->assertSame('23000', $sqlstate, 'MySQL reuses 23000 for a NOT NULL violation');
        $this->assertSame(1048, $errno, 'only the errno distinguishes it from a duplicate key');
    }

    public function testPostgresCarriesNoErrno(): void
    {
        $c = $this->connectConnection();
        try {
            $c->exec('SELEKT 1');
            $this->fail('syntax error expected');
        } catch (NonRetryableException $e) {
            $this->assertSame('42601', $e->sqlstate());
            $this->assertNull($e->errno(), 'PG has no integer errno — null by construction');
        }
    }
}
```

- [ ] **Step 10: Run, then MUTATION-PROVE**

```
cargo test --workspace
FERRO_TEST_PG_URL=… cargo test -p ferro-backend-pg --test pg_query_it a_real_pg_server_error_carries_no_errno -- --nocapture
cargo run -p ferro-proto --bin gen_vectors && git diff --exit-code proto/vectors
(cd php/client && ./vendor/bin/phpunit tests/Conformance tests/Live/ErrnoLiveTest.php)
```
Mutate:

1. Set `errno: None` in `ferro-backend-mysql/src/error_map.rs` → `a_sql_errors_vendor_errno_reaches_the_wire_payload` *and* both MySQL live assertions go RED.
2. Replace the `fate.rs` `Sql` arm's `errno` with `errno: None` → `classify_fate_mirrors_the_errno_and_never_invents_one` goes RED on the `Some(..)` rows.
3. Make `fate.rs`'s `Sql` arm DERIVE an errno (`errno: Some(i32::from(code))`) → the same test goes RED on the `None` row, which the deleted tautology could not have caught.
4. Set `errno: Some(0)` in `ferro-backend-pg/src/error_map.rs` → `a_real_pg_server_error_carries_no_errno` and `testPostgresCarriesNoErrno` go RED.
5. **Delete the byte-lock test `testErrorMysqlErrnoVectorByteMatchesBothDirections` but leave the vector committed** → `testEveryCommittedVectorIsByteLocked` goes RED, because `$namedLocked` is derived from the `loadVector('…')` call sites and that call site is gone. (Under the v1 hand-written array it would have stayed GREEN — this mutation is the proof the derivation was load-bearing.)

Restore each.

- [ ] **Step 11: Spec truth + commit**

Amend `ferro-spec-v0.2.md:561` (the "deferred to M1-S8" errno note) to record that the errno now rides `ErrorPayload.errno` on MySQL/MariaDB and is `None` on PG by construction, and add the vector to `proto/PROTOCOL.md`'s §7 vector index.

```bash
./ci/local-gate.sh --live
git add engine proto php ferro-spec-v0.2.md
git commit -m "feat(m1-s8a): carry the MySQL vendor errno to the wire and to the PHP client

PoolError::Sql gains an errno slot filled only by ferro-backend-mysql (PG has no
integer errno); fate.rs — the ONE PoolError->ErrorPayload site — passes it
through verbatim. Adds error_mysql_errno, the first golden vector locking a
non-null errno, and a PHP errno() accessor.

Without this a Doctrine MySQL ExceptionConverter is inert: it matches on the
errno EXCLUSIVELY, and MySQL's 23000 covers both a duplicate key and a NOT NULL
violation (proven live in ErrnoLiveTest).

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 4: The PG narrowing bind — `I64` → `int2`/`int4`/`int8`, `F64` → `float4`/`float8` (offline)

**Files:**
- Modify: `engine/crates/ferro-backend-pg/src/bind.rs` (new `PgInt`/`PgFloat` newtypes, `value_to_boxed:192-213`, `accepts:229-250`, the fixtures at `:355-401`, the pinned tests at `:306-324`)
- Modify: `engine/crates/ferro-backend-pg/src/query.rs:90-99` and `:195-204` (both pre-flight loops)
- Test: `engine/crates/ferro-backend-pg/src/bind.rs` unit tests

**Interfaces:**
- Consumes: nothing from Tasks 1–3.
- Produces: `ferro_backend_pg::bind::check_param(v: &Value, ty: &Type) -> Result<(), String>` — the §19.3 pre-flight, returning the operator-facing REASON. `pub fn accepts(v: &Value, ty: &Type) -> bool` is retained as `check_param(v, ty).is_ok()` so every existing S7 test and the lockstep proof keep working unchanged.

**Design note for the implementer — read hazard 3's CORRECTED mechanism first.** The temptation is to widen `<i64 as ToSql>::accepts` by giving `Value::I64` a newtype whose `accepts` returns true for `int2|int4|int8` and letting `to_sql` do `i32::try_from`. **That is the false-`Indeterminate` bug — but not for the reason it is usually stated.** `to_sql` does **not** run "after the statement is on the wire": `encode_bind_raw` (`vendor/tokio-postgres/src/query.rs:294-331`) serialises every param into a LOCAL `BytesMut` **before** `start(client, buf)` writes anything to the socket, so a `to_sql` failure means the statement provably never left the process.

The damage is **MISCLASSIFICATION**. A `to_sql` failure surfaces as `Error::to_sql(e, idx)`, whose `as_db_error()` is `None`; `ferro-backend-pg`'s `is_session_fatal` (`conn.rs:241-249`) treats a `None` DbError as a transport failure and returns `PoolError::ConnectionLost`; and `classify_fate` turns a `ConnectionLost` on a sent, non-readonly, non-in-tx op into `WriteUnconfirmed{Indeterminate}` (§19.3). The client is told a write may have applied when nothing was ever sent. Every doc comment and commit message in this task must say that, because "post-send" is wrong and would be committed as source.

So the range check must live where the VALUE is visible — `bind::check_param(v, ty)` has the `Value`, `ToSql::accepts(ty)` does not — and the pre-flight stays *stricter* than the newtype's own `accepts`, which is the legal direction (hazard 1).

**Compile hazard, proved, not guessed (hazard 57):** `tokio_postgres::types::Type` is **not `Copy`**, so `match (v, *ty) { (Value::I64(n), Type::INT4) => … }` is **E0507 — cannot move out of a shared reference**. `matches!(*ty, …)` and a bare `match *ty` over constant patterns do compile.

**CHOICE MADE (the probe offered the borrowed-pattern repair; this plan takes the equality spelling instead).** Every type test in this task is written as an `==` comparison or `[Type::A, Type::B].contains(ty)`, never as a constant pattern. Two reasons, and it costs nothing: (1) it sidesteps the E0507 tuple-move by construction rather than by remembering an `&` — a subsequent edit cannot reintroduce the error; (2) `Type` is `pub struct Type(Inner)` whose `Inner::Other(Arc<Other>)` arm makes it a non-structural type, so constant-pattern matching on it is at best fragile across rustc versions, while `PartialEq` is a first-class derive on both. It is also **exactly the idiom already in the file** — `pg_canonical_text_param!`'s `accepts` is `[$(Type::$ty),+].contains(ty)` (`bind.rs:90-92`). Consistency with the seven existing newtypes is worth more than terseness here.

- [ ] **Step 1: Write the failing unit tests**

Replace the pinned negatives at `bind.rs:308-313` and add the new proofs:

```rust
    /// M1-S8a: a canonical `I64` binds to EVERY PG integer width, and an `F64` to both float widths.
    /// This is the single highest-frequency DBAL blocker — `Types\IntegerType` returns a PHP `int`,
    /// and `IntegerType`/`SmallIntType` map to PG `INT`/`SMALLINT`, so every insert into a
    /// `serial`/`int4` PK and every identifier lookup binds exactly this pair.
    #[test]
    fn s8a_i64_binds_to_every_integer_width_and_f64_to_both_floats() {
        for ty in [Type::INT2, Type::INT4, Type::INT8] {
            assert!(accepts(&Value::I64(42), &ty), "I64 must bind {ty:?}");
        }
        for ty in [Type::FLOAT4, Type::FLOAT8] {
            assert!(accepts(&Value::F64(1.5), &ty), "F64 must bind {ty:?}");
        }
        // Still NARROW: widening the integer arms must not make an int bindable anywhere else.
        for ty in [Type::TEXT, Type::NUMERIC, Type::DATE, Type::TIMESTAMP, Type::UUID, Type::BOOL] {
            assert!(!accepts(&Value::I64(42), &ty), "I64 must not bind {ty:?}");
            assert!(!accepts(&Value::F64(1.5), &ty), "F64 must not bind {ty:?}");
        }
    }

    /// The range check is a PRE-SEND, known-fate rejection — NOT a `to_sql` failure. A value outside
    /// the target width is refused here, where the statement provably has not been sent, so it can
    /// never mint a false §19.3 `WriteUnconfirmed{Indeterminate}`.
    #[test]
    fn s8a_out_of_range_narrowing_is_refused_before_send() {
        assert!(!accepts(&Value::I64(i64::from(i32::MAX) + 1), &Type::INT4));
        assert!(!accepts(&Value::I64(i64::from(i32::MIN) - 1), &Type::INT4));
        assert!(!accepts(&Value::I64(i64::from(i16::MAX) + 1), &Type::INT2));
        assert!(!accepts(&Value::I64(i64::from(i16::MIN) - 1), &Type::INT2));
        // ...and the in-range boundaries DO bind.
        assert!(accepts(&Value::I64(i64::from(i32::MAX)), &Type::INT4));
        assert!(accepts(&Value::I64(i64::from(i16::MIN)), &Type::INT2));
        // int8 is the full range.
        assert!(accepts(&Value::I64(i64::MAX), &Type::INT8));

        // f64 -> float4: a finite value that OVERFLOWS f32 becomes `inf` — a silent corrupt write.
        assert!(!accepts(&Value::F64(1e39), &Type::FLOAT4));
        assert!(!accepts(&Value::F64(-1e39), &Type::FLOAT4));
        assert!(accepts(&Value::F64(1e38), &Type::FLOAT4));
        // Non-finite values are representable in BOTH widths and stay bindable.
        assert!(accepts(&Value::F64(f64::INFINITY), &Type::FLOAT4));
        assert!(accepts(&Value::F64(f64::NAN), &Type::FLOAT4));
        // float8 never narrows, so nothing is out of range there.
        assert!(accepts(&Value::F64(1e300), &Type::FLOAT8));
    }

    /// The refusal REASON distinguishes "wrong type" from "out of range" — an operator staring at a
    /// failed insert needs to know which. Both are `Sql{Unsupported}` known-fate rejections.
    #[test]
    fn s8a_check_param_reasons_are_distinct_and_actionable() {
        let too_big = check_param(&Value::I64(i64::from(i32::MAX) + 1), &Type::INT4)
            .expect_err("out of range");
        assert!(too_big.contains("out of range"), "{too_big}");
        assert!(too_big.contains("int4"), "{too_big}");
        assert!(too_big.contains("2147483648"), "the offending VALUE must be named: {too_big}");

        let wrong_type = check_param(&Value::Text("x".into()), &Type::INT4).expect_err("mismatch");
        assert!(wrong_type.contains("cannot bind"), "{wrong_type}");
        assert!(wrong_type.contains("TEXT"), "{wrong_type}");
        assert!(!wrong_type.contains("out of range"), "{wrong_type}");
    }
```

- [ ] **Step 2: Run them and watch them fail**

```
cargo test -p ferro-backend-pg bind::tests::s8a_
```
Expected: FAIL — `accepts(&Value::I64(42), &Type::INT4)` is `false` today (pinned by the old `bind.rs:309-310`), and `check_param` does not exist.

- [ ] **Step 3: Add the two narrowing newtypes**

In `engine/crates/ferro-backend-pg/src/bind.rs`, after the canonical-text newtypes:

```rust
/// A canonical `I64` bound against whichever PG integer width the prepared statement inferred
/// (M1-S8a). PG's own `ToSql for i64` accepts `int8` ONLY, so before this every DBAL insert into a
/// `serial`/`int4` PK — and every `$qb->setParameter('id', 5)` against one — was a hard, pre-send
/// `NonRetryable` refusal.
///
/// **Format is BINARY**, not text: PG's param format IS per-param selectable (`encode_format`), but
/// there is nothing to gain here — `<i16/i32/i64 as ToSql>` already writes the exact native binary
/// form, so this delegates rather than re-rendering a decimal string PG would have to re-parse.
///
/// **The range check is NOT here.** `to_sql` runs after `query_raw` has begun sending, so an
/// out-of-range failure at this point is POST-send — precisely the false-`Indeterminate` path the
/// §19.3 pre-flight exists to prevent. `bind::check_param` (which sees the VALUE, unlike
/// `ToSql::accepts`, which sees only the `Type`) rejects it one step earlier. The `try_from`s below
/// are therefore a totality backstop for a caller that skipped the pre-flight — they yield a typed
/// `WrongType`-class error, never a panic.
#[derive(Debug)]
struct PgInt(i64);

impl ToSql for PgInt {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // Equality, not constant patterns — see the task's compile note (hazard 57) and the S7
        // macro's own `[Type::X].contains(ty)` idiom.
        if *ty == Type::INT2 {
            i16::try_from(self.0)?.to_sql(ty, out)
        } else if *ty == Type::INT4 {
            i32::try_from(self.0)?.to_sql(ty, out)
        } else if *ty == Type::INT8 {
            self.0.to_sql(ty, out)
        } else {
            Err(format!("PgInt cannot bind PG type {}", ty.name()).into())
        }
    }

    fn accepts(ty: &Type) -> bool {
        [Type::INT2, Type::INT4, Type::INT8].contains(ty)
    }

    to_sql_checked!();
}

/// A canonical `F64` bound against `float4` or `float8` (M1-S8a). Same shape and same rationale as
/// [`PgInt`]; the range guard for `float4` lives in [`check_param`].
///
/// **Precision loss inside the f32 range is ACCEPTED and is not a miscast**: it is the column's own
/// precision, and PG's own input parser would round a text literal identically. What is NOT accepted
/// is a *finite* `f64` that overflows `f32` and becomes `inf` — a silent corrupt write, refused
/// pre-send by [`check_param`].
#[derive(Debug)]
struct PgFloat(f64);

impl ToSql for PgFloat {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if *ty == Type::FLOAT4 {
            (self.0 as f32).to_sql(ty, out)
        } else if *ty == Type::FLOAT8 {
            self.0.to_sql(ty, out)
        } else {
            Err(format!("PgFloat cannot bind PG type {}", ty.name()).into())
        }
    }

    fn accepts(ty: &Type) -> bool {
        [Type::FLOAT4, Type::FLOAT8].contains(ty)
    }

    to_sql_checked!();
}
```

- [ ] **Step 4: Route both arms through the newtypes, in lockstep**

`value_to_boxed` (`bind.rs:192-213`):

```rust
        Value::I64(n) => Box::new(PgInt(*n)),
        Value::F64(f) => Box::new(PgFloat(*f)),
```

`accepts`'s arms (`bind.rs:229-250`) are replaced wholesale by `check_param`, which keeps the same arm-for-arm mirror plus the value-aware range gate:

```rust
/// The §19.3 bind PRE-FLIGHT for one parameter slot: is this the exact bind `query_raw` will
/// perform, and will it succeed? Returns the operator-facing REASON on refusal.
///
/// The rule is DIRECTIONAL (see the module docs): this may be STRICTER than the concrete `ToSql`
/// impl `value_to_boxed` boxes — which is exactly what the range gate below does — but it must
/// NEVER be looser, or a bind fault lands POST-send and the SQL service can mint a false
/// `WriteUnconfirmed{Indeterminate}` for a write that never happened.
pub fn check_param(v: &Value, ty: &Type) -> Result<(), String> {
    let accepted = match v {
        Value::Null => true,
        Value::Bool(_) => <bool as ToSql>::accepts(ty),
        Value::I64(_) => <PgInt as ToSql>::accepts(ty),
        Value::F64(_) => <PgFloat as ToSql>::accepts(ty),
        Value::Text(_) => <String as ToSql>::accepts(ty),
        Value::Bytes(_) => <Vec<u8> as ToSql>::accepts(ty),
        Value::U64(_) => <PgU64Text as ToSql>::accepts(ty),
        Value::Decimal(_) => <PgDecimalText as ToSql>::accepts(ty),
        Value::Date(_) => <PgDateText as ToSql>::accepts(ty),
        Value::Time(_) => <PgTimeText as ToSql>::accepts(ty),
        Value::Timestamp(_) => <PgTimestampText as ToSql>::accepts(ty),
        Value::TimestampTz(_) => <PgTimestampTzText as ToSql>::accepts(ty),
        Value::Uuid(_) => <PgUuidText as ToSql>::accepts(ty),
        Value::Json(_) => <PgJsonText as ToSql>::accepts(ty),
    };
    if !accepted {
        return Err(format!(
            "canonical {} cannot bind to PG type {}",
            value_kind(v),
            ty.name()
        ));
    }
    check_range(v, ty)
}

/// The VALUE-aware half of the pre-flight. `ToSql::accepts` sees only the target type, so a
/// narrowing overflow is invisible to it; caught here the refusal is KNOWN-FATE and pre-send.
///
/// Split out as its own function so [`check_param`] stays one screen and so Task 5's domain unwrap
/// has exactly ONE place to pass the resolved base type.
///
/// Spelled with `==` rather than constant patterns: `match (v, *ty)` is E0507 (`Type` is not
/// `Copy`), and `Type` is a non-structural type, so equality is both the compiling and the durable
/// form (hazard 57).
fn check_range(v: &Value, ty: &Type) -> Result<(), String> {
    match v {
        Value::I64(n) => {
            if *ty == Type::INT4 && i32::try_from(*n).is_err() {
                return Err(format!(
                    "canonical I64 value {n} is out of range for PG type int4 \
                     (pre-send rejection: the statement was never executed)"
                ));
            }
            if *ty == Type::INT2 && i16::try_from(*n).is_err() {
                return Err(format!(
                    "canonical I64 value {n} is out of range for PG type int2 \
                     (pre-send rejection: the statement was never executed)"
                ));
            }
            Ok(())
        }
        Value::F64(f) => {
            if *ty == Type::FLOAT4 && f.is_finite() && !(*f as f32).is_finite() {
                return Err(format!(
                    "canonical F64 value {f} is out of range for PG type float4 (it would \
                     silently become infinity; pre-send rejection: the statement was never executed)"
                ));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Whether [`check_param`] accepts this pair. Retained as the boolean façade so the directional
/// lockstep proof and every existing call site read the SAME predicate the pre-flight enforces.
pub fn accepts(v: &Value, ty: &Type) -> bool {
    check_param(v, ty).is_ok()
}
```

- [ ] **Step 5: Use the reason in BOTH query pre-flights**

`engine/crates/ferro-backend-pg/src/query.rs:90-99` (buffered) and `:195-204` (streaming) — identical replacement in both (the duplication is deliberate, see `query.rs:156-158`):

```rust
    for (i, (v, ty)) in params.iter().zip(expected).enumerate() {
        if let Err(why) = bind::check_param(v, ty) {
            return Err(bind_error(format!("parameter {i}: {why}")));
        }
    }
```

The old message's parenthetical (`"M0 maps I64->int8 / F64->float8 directly; a narrower column needs a client cast"`) is now false and is removed — that is the whole point of this task.

- [ ] **Step 6: GROW the lockstep fixture — and make its completeness COMPILE-FORCED (hazard 2, weak guard 7)**

`every_variant()` (`bind.rs:355-372`) gains the magnitudes that exercise the range gate; `every_target_type()` (`bind.rs:376-401`) already contains `INT2/INT4/INT8/FLOAT4/FLOAT8`, so it needs no change for THIS task — **verify that rather than assuming it** (Task 5 does grow it, with domains).

**First, the guard that makes the fixture's completeness checkable at all.** `every_variant()` is a hand-written `Vec`, so no assertion *about the vec* can detect a variant that was never added to it. `Value` is a **closed, in-tree enum**, so the honest guard is compile-forced. Add, immediately below the fixture:

```rust
    /// **Compile-forced completeness for [`every_variant`].**
    ///
    /// `every_variant` is a hand-written `Vec`, so `assert_eq!(x.len(), every_variant().len())`
    /// proves only that boxing drops nothing — it is a TAUTOLOGY with respect to a variant that was
    /// never added. This match has **no `_` arm**, so adding a 15th variant to
    /// `ferro_proto::value::Value` breaks THIS FILE's build.
    ///
    /// **When that build break happens, the fix is to add the variant to `every_variant()` above**
    /// (and to give it a real box in `value_to_boxed` and a real arm in `check_param`) — NOT to add
    /// an arm here and move on. The arms below exist only to make the omission impossible to miss.
    #[allow(dead_code)]
    fn _exhaustive(v: &Value) {
        match v {
            Value::Null => (),
            Value::Bool(_) => (),
            Value::I64(_) => (),
            Value::F64(_) => (),
            Value::Text(_) => (),
            Value::Bytes(_) => (),
            Value::U64(_) => (),
            Value::Decimal(_) => (),
            Value::Date(_) => (),
            Value::Time(_) => (),
            Value::Timestamp(_) => (),
            Value::TimestampTz(_) => (),
            Value::Uuid(_) => (),
            Value::Json(_) => (),
        }
    }
```

**`every_target_type()` gets NO such guard, deliberately.** It enumerates `tokio_postgres::types::Type`, which is external and open — any oid constructs one, so no `match` over it can be exhaustive and no compile-forced check exists. Behavioural cross-product coverage is the right and only guard there; the obligation is to grow the fixture with every change and to mutation-prove the growth was load-bearing (Step 8 mutation 2). Say this in a comment above the function so nobody later "fixes" the asymmetry.

Then the fixture itself:

```rust
    fn every_variant() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Bool(true),
            Value::I64(-200),
            // M1-S8a: the magnitudes the narrowing range gate exists for. Without these three the
            // cross-product proof below only ever sees an in-range integer and the gate is UNPROVEN
            // (the hard-coded-fixture failure mode).
            Value::I64(i64::MAX),
            Value::I64(i64::from(i32::MAX) + 1),
            Value::I64(i64::from(i16::MAX) + 1),
            Value::F64(1.5),
            Value::F64(1e39),
            Value::F64(f64::NAN),
            Value::Text("x".to_string()),
            Value::Bytes(vec![0xde, 0xad]),
            Value::U64(u64::MAX),
            Value::Decimal("-12345.6700".to_string()),
            Value::Date("2026-08-05".to_string()),
            Value::Time("24:00:00".to_string()),
            Value::Timestamp("2026-08-05 11:45:07.250000".to_string()),
            Value::TimestampTz("2026-08-05T11:45:07.250000Z".to_string()),
            Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".to_string()),
            Value::Json(r#"{"a":[1,2]}"#.to_string()),
        ]
    }
```

`s7_value_to_boxed_is_total_and_never_panics` asserts `to_boxed_params(&every_variant()).len() == 14` (`bind.rs:571`) — a HARD-CODED COUNT that this task's three new magnitudes break anyway. Replace it:

```rust
        // One boxed ToSql per fixture value. NOT a completeness check — `_exhaustive` above is the
        // guard that a variant cannot go missing; this only pins that boxing is total and drops
        // nothing. Written derived rather than as a literal so the fixture can grow freely.
        assert_eq!(
            to_boxed_params(&every_variant()).len(),
            every_variant().len(),
            "one boxed ToSql per fixture value"
        );
```

**Do not let this line stand in for the completeness guard.** On its own it is a tautology with respect to a missing variant — that is exactly why `_exhaustive` exists, and the comment above says so at the call site so a future reader does not delete one believing the other covers it.

- [ ] **Step 7: Run the whole bind suite**

```
cargo test -p ferro-backend-pg bind
```
Expected: PASS, including the unchanged `s7_accepts_is_never_looser_than_the_boxed_impl` (`bind.rs:539`) now driving the three out-of-range magnitudes across all 22 target types.

- [ ] **Step 8: MUTATION-PROVE the directional rule and the fixtures**

1. Delete the range checks from `check_range` (make it `Ok(())` unconditionally) → `s8a_out_of_range_narrowing_is_refused_before_send` goes RED, **and** `s7_accepts_is_never_looser_than_the_boxed_impl` goes RED (because `to_sql_checked` on `PgInt(i64::MAX)` against `int4` now fails while `accepts` said yes) — that second failure is the directional proof working. Restore.
2. Remove the three new magnitudes from `every_variant()` and re-apply mutation 1 → the lockstep proof stays GREEN. **Record this**: it is the demonstration that the fixture growth in Step 6 was load-bearing, not decoration — and it is the reason `every_target_type()` must be grown by hand in Task 5 too. Restore both.
3. Change `PgInt::accepts` to `[Type::INT8].contains(ty)` → `s8a_i64_binds_to_every_integer_width_and_f64_to_both_floats` goes RED. Restore.
4. **Prove `_exhaustive` is the completeness guard and the length assertion is not.** Comment out ONE arm of `_exhaustive` (say `Value::Json(_)`) → `cargo test -p ferro-backend-pg` fails to **compile** with a non-exhaustive-match error. Then restore it and instead delete `Value::Json(...)` from `every_variant()` → the build stays green and `s7_value_to_boxed_is_total_and_never_panics` **still passes** (its length assertion is derived from the same shrunken vec), while the cross-product proof simply stops covering JSON. Record both observations: the first is the guard working, the second is the tautology the guard exists to compensate for. Restore.

- [ ] **Step 9: Commit (offline-provable; the live gate lands in Task 5)**

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add engine/crates/ferro-backend-pg/src/bind.rs engine/crates/ferro-backend-pg/src/query.rs
git commit -m "feat(m1-s8a): PG narrowing bind — I64 binds int2/int4/int8, F64 binds float4/float8

bind::accepts was Type-identity via the concrete ToSql, so a PHP int against an
int4/serial column was a hard pre-send NonRetryable — the highest-frequency DBAL
blocker (every insert into a serial, every identifier lookup).

Two narrowing newtypes write the native binary form; the RANGE check lives in the
new check_param, which sees the VALUE (ToSql::accepts sees only the Type), so an
overflow stays a KNOWN-FATE pre-send refusal and can never fail post-send into a
false Indeterminate. accepts() is retained as check_param().is_ok() so the
directional lockstep proof reads the same predicate.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 5: Resolve a parameter's DOMAIN to its base type — in EVERY arm, on BOTH sides — and prove both bind fixes live on PG

**Files:**
- Modify: `engine/crates/ferro-backend-pg/src/bind.rs` (a new `resolve_domain`; three new domain-aware wrapper newtypes `PgBool`/`PgText`/`PgBytes`; `resolve_domain` folded into `PgInt`/`PgFloat` and into the `pg_canonical_text_param!` macro body; `value_to_boxed`; `check_param`; the fixtures)
- Test: `engine/crates/ferro-backend-pg/tests/pg_types_it.rs`, `engine/crates/ferrod/tests/types_e2e_it.rs`

**Interfaces:**
- Consumes: `ferro_backend_pg::bind::check_param(v, ty) -> Result<(), String>` and `check_range` (Task 4).
- Produces: no new public API — `check_param` gains the domain unwrap internally, and `value_to_boxed` gains three wrapper newtypes so the boxed side resolves identically.

**Why this is a separate task.** Task 4 changes what values may bind; this changes what TYPE the check is performed against. They fail differently and a reviewer can accept one while rejecting the other. SPEC §22.2 (g) is exactly this: *"`stmt.params()` reports the DOMAIN's own OID and `bind::accepts` matches on `Type` identity, so binding the very value just read back into that column is refused."*

**CORRECTED MOTIVATION (probe 1, major M5).** The v1 text claimed "every `information_schema` column is a domain, so DBAL's introspection binds against domains constantly". **Measured on PG 17: that is false for the shape that matters.** `SELECT … FROM information_schema.columns WHERE table_name = $1` resolves the parameter to `name` (`Kind::Simple`), not to the `sql_identifier` domain — PG's operator resolution picks the base-type `=`. No domain reaches `stmt.params()` there. What actually unblocks `information_schema` introspection is **Task 6's `name` (19) admission**, not this task.

The real, and still sufficient, motivation is **user-defined domain columns**: `CREATE DOMAIN positive_int AS int4 CHECK (VALUE > 0)`, a `positive_int` column, and a `WHERE dom = $1` or an `INSERT … VALUES ($1)` against it. There `stmt.params()` DOES report the domain's own oid, and today Ferro can READ that column (the `RowDescription` resolves to the base) but cannot BIND the value it just read back into it. That asymmetry is what §22.2 (g) records and what live case (c) closes.

**THIS TASK'S v1 WAS BROKEN — read before writing any code (probe 1, blocker B2).** The v1 design resolved the domain in `check_param` and then delegated `Bool`/`Text`/`Bytes` to postgres-types' own `<bool/String/Vec<u8> as ToSql>` impls. Those impls have **ZERO `Kind::Domain` handling** (grep: no hits in the crate), measured live: `accepts(domain_over_text) == false` for `String`, and likewise for `bool` and `Vec<u8>`. So the v1 pre-flight would have said **yes** to a `Value::Text` against a domain-over-`text` while `to_sql_checked` said **no** — a **LOOSER** pre-flight, the one direction §19.3 forbids — and by hazard 3's misclassification chain the client would get `WriteUnconfirmed{Indeterminate}` for a write that was never sent. **The fix for a false `Indeterminate` would have manufactured one.**

Worse, the v1 lockstep proof would have stayed GREEN over it: the fixture only added `dom_int4`/`dom_numeric`, whose bases ARE handled by the (also-domain-aware) `PgInt`/`PgDecimalText`. That is the hard-coded-fixture failure mode this plan bans, inside the plan's own safety guard.

**So the rule for this task is: resolve on BOTH sides, in EVERY arm, or in neither.** Every `Value` arm's boxed impl and its `accepts` must apply the same `resolve_domain`, and `every_target_type()` must carry domains over `text`, `bool` and `bytea` — the three bases the v1 fixture could not have caught — as well as over `int4` and `numeric`.

- [ ] **Step 1: Write the failing offline unit test**

`engine/crates/ferro-backend-pg/src/bind.rs` tests — a domain `Type` can be constructed directly, no server needed:

```rust
    /// PG resolves a domain to its BASE type in the `RowDescription` (so READS already work), but
    /// NOT in `stmt.params()` — a parameter slot reports the DOMAIN's own oid. Matching on `Type`
    /// identity therefore refused binding the very value just read back out of that column
    /// (SPEC §22.2 (g)). The pre-flight must check against the base.
    #[test]
    fn s8a_a_domain_parameter_is_checked_against_its_base_type() {
        use tokio_postgres::types::Kind;
        let dom_int4 = Type::new(
            "positive_int".to_string(),
            123_456,
            Kind::Domain(Type::INT4),
            "public".to_string(),
        );
        assert!(accepts(&Value::I64(7), &dom_int4), "a domain over int4 must accept an I64");
        assert!(
            !accepts(&Value::Text("x".into()), &dom_int4),
            "the base type's strictness must survive the unwrap"
        );
        assert!(
            !accepts(&Value::I64(i64::from(i32::MAX) + 1), &dom_int4),
            "and so must the range gate"
        );

        // Nested: PG allows a domain over a domain.
        let dom_dom = Type::new(
            "small_positive_int".to_string(),
            123_457,
            Kind::Domain(dom_int4.clone()),
            "public".to_string(),
        );
        assert!(accepts(&Value::I64(7), &dom_dom));

        // A domain over an UNSUPPORTED base is still refused — the unwrap widens nothing.
        let dom_timetz = Type::new(
            "tz".to_string(),
            123_458,
            Kind::Domain(Type::TIMETZ),
            "public".to_string(),
        );
        assert!(!accepts(&Value::Time("12:00:00".into()), &dom_timetz));
    }

    /// **The arms the v1 design would have broken (probe 1, blocker B2).** `postgres-types`'
    /// own `ToSql` impls have NO `Kind::Domain` handling, so `<String as ToSql>::accepts`,
    /// `<bool as ToSql>::accepts` and `<Vec<u8> as ToSql>::accepts` are all `false` for a domain
    /// over their base type. Delegating to them behind a domain-resolving pre-flight makes the
    /// pre-flight LOOSER than the impl — the §19.3-forbidden direction, which lands as a false
    /// `Indeterminate` via the `to_sql` → `as_db_error()==None` → `ConnectionLost` chain.
    ///
    /// This test asserts BOTH halves of the mirror explicitly, because a half-applied fix is the
    /// realistic failure and it is invisible to a `bool`-returning `accepts` test alone.
    #[test]
    fn s8a_bool_text_and_bytes_resolve_the_domain_on_both_sides() {
        use tokio_postgres::types::Kind;
        let cases: &[(Value, Type)] = &[
            (
                Value::Text("x".into()),
                Type::new("dom_text".into(), 900_010, Kind::Domain(Type::TEXT), "public".into()),
            ),
            (
                Value::Bool(true),
                Type::new("dom_bool".into(), 900_011, Kind::Domain(Type::BOOL), "public".into()),
            ),
            (
                Value::Bytes(vec![0xde, 0xad]),
                Type::new("dom_bytea".into(), 900_012, Kind::Domain(Type::BYTEA), "public".into()),
            ),
        ];
        for (v, dom) in cases {
            // (1) the PRE-FLIGHT accepts it...
            assert!(accepts(v, dom), "pre-flight must accept {v:?} against {dom:?}");
            // (2) ...and so does the impl `value_to_boxed` actually boxes. If (1) held without (2)
            // the bind would fail at `to_sql_checked` and be reported as a possibly-applied write.
            let boxed = value_to_boxed(v);
            let mut buf = tokio_postgres::types::private::BytesMut::new();
            assert!(
                boxed.to_sql_checked(dom, &mut buf).is_ok(),
                "the BOXED impl must also resolve the domain for {v:?} / {dom:?} — a pre-flight \
                 that is looser than the impl is the false-Indeterminate path"
            );
        }
        // The unwrap widens nothing: the base type's strictness survives it.
        let dom_text =
            Type::new("dom_text".into(), 900_010, Kind::Domain(Type::TEXT), "public".into());
        assert!(!accepts(&Value::Bool(true), &dom_text));
        assert!(!accepts(&Value::Bytes(vec![1]), &dom_text));
    }

    /// The refusal message names BOTH the domain and its base, or an operator staring at
    /// `positive_int` has no idea what the bind actually needed.
    ///
    /// The synthetic oid is in the `900_0xx` band on purpose: `2205` is `regclass`'s REAL oid and
    /// `2206` is `regtype`'s (hazard 11), so reusing either would make the fixture lie about what
    /// PG would have sent.
    #[test]
    fn s8a_a_domain_refusal_names_the_domain_and_its_base() {
        use tokio_postgres::types::Kind;
        let dom = Type::new(
            "positive_int".to_string(),
            900_003,
            Kind::Domain(Type::INT4),
            "public".to_string(),
        );
        let why = check_param(&Value::Text("x".into()), &dom)
            .expect_err("a TEXT cannot bind an int4 domain");
        assert!(why.contains("positive_int"), "names the DOMAIN: {why}");
        assert!(why.contains("int4"), "names the BASE: {why}");
    }
```

- [ ] **Step 2: Run and watch it fail**

```
cargo test -p ferro-backend-pg bind::tests::s8a_a_domain
```
Expected: FAIL — `accepts` matches `Type` identity, so a domain matches nothing.

- [ ] **Step 3: Implement the bounded unwrap**

In `engine/crates/ferro-backend-pg/src/bind.rs`:

```rust
/// Maximum DOMAIN nesting the parameter-type resolver will unwrap. PG itself allows a domain over a
/// domain; the depth is bounded here so a pathological (or hostile, or simply cyclic-by-bug) `Type`
/// can never spin the daemon inside a pre-flight. A `Type` nested deeper than this falls through to
/// the ordinary "cannot bind" refusal — loud and known-fate, never a hang.
const MAX_DOMAIN_DEPTH: usize = 8;

/// Resolve a PARAMETER's declared `Type` to the type the bind must actually satisfy.
///
/// PG resolves a domain to its base when it builds a `RowDescription` (`printtup.c` →
/// `getBaseTypeAndTypmod`), which is why READS need no unwrap at all. It does NOT do that for
/// `stmt.params()`: a parameter slot reports the DOMAIN's own oid, so a `Type`-identity match
/// refuses binding the very value just read back out of that column (SPEC §22.2 (g)). Every
/// `information_schema` column is a domain, and DBAL's schema manager binds against them constantly.
fn resolve_domain(ty: &Type) -> &Type {
    let mut cur = ty;
    for _ in 0..MAX_DOMAIN_DEPTH {
        match cur.kind() {
            tokio_postgres::types::Kind::Domain(inner) => cur = inner,
            _ => return cur,
        }
    }
    cur
}
```

`check_param` resolves once, up front, and reports both names. Written out in full — every arm now
tests `base`, not `ty`, and `check_range` is handed `base`:

```rust
pub fn check_param(v: &Value, ty: &Type) -> Result<(), String> {
    let base = resolve_domain(ty);
    // `ty.name()` for the operator ("positive_int"), `base.name()` for the actual constraint
    // ("int4") — a message naming only one of them is unactionable.
    let named = if std::ptr::eq(base, ty) {
        ty.name().to_string()
    } else {
        format!("{} (domain over {})", ty.name(), base.name())
    };
    // EVERY arm is checked against `base`, and every newtype below ALSO resolves internally, so the
    // pre-flight and the boxed impl agree by construction. Both halves are required: `postgres-types`
    // has no `Kind::Domain` handling at all, so an arm that resolved here and delegated to a raw
    // `<String as ToSql>` would be LOOSER than the impl it fronts (§19.3's forbidden direction).
    let accepted = match v {
        Value::Null => true,
        Value::Bool(_) => <PgBool as ToSql>::accepts(base),
        Value::I64(_) => <PgInt as ToSql>::accepts(base),
        Value::F64(_) => <PgFloat as ToSql>::accepts(base),
        Value::Text(_) => <PgText as ToSql>::accepts(base),
        Value::Bytes(_) => <PgBytes as ToSql>::accepts(base),
        Value::U64(_) => <PgU64Text as ToSql>::accepts(base),
        Value::Decimal(_) => <PgDecimalText as ToSql>::accepts(base),
        Value::Date(_) => <PgDateText as ToSql>::accepts(base),
        Value::Time(_) => <PgTimeText as ToSql>::accepts(base),
        Value::Timestamp(_) => <PgTimestampText as ToSql>::accepts(base),
        Value::TimestampTz(_) => <PgTimestampTzText as ToSql>::accepts(base),
        Value::Uuid(_) => <PgUuidText as ToSql>::accepts(base),
        Value::Json(_) => <PgJsonText as ToSql>::accepts(base),
    };
    if !accepted {
        return Err(format!("canonical {} cannot bind to PG type {named}", value_kind(v)));
    }
    // Task 4's value-aware gate, against the RESOLVED type: a domain over int4 narrows exactly as
    // int4 does.
    check_range(v, base)
}
```

Note the `Value::Null` arm still returns `true` unconditionally — it binds through `PgNull`, whose
`accepts` is universally true because it writes no value bytes at all (`bind.rs:34-54`). That is the
one legitimate use of a universally-true `accepts` and it must not be copied to any other arm.

- [ ] **Step 4: Make the BOXED side resolve too — in every arm (the blocker-B2 fix)**

**This is the step v1 got wrong.** `value_to_boxed` currently boxes `Value::Bool` as a bare `bool`, `Value::Text` as a `String` and `Value::Bytes` as a `Vec<u8>` (`bind.rs:195-199`), and `postgres-types`' impls for those types **do not know what a domain is**. Resolving only in `check_param` would make the pre-flight looser than the impl for exactly those three tags. Give them Ferro-owned wrappers, in the same shape as the S7 newtypes:

```rust
/// Declares a newtype that delegates to `postgres-types`' own `ToSql` for `$inner`, but resolves a
/// DOMAIN to its base type FIRST — in `accepts` **and** in `to_sql`, so the pair stays a mirror.
///
/// **Why a wrapper at all (M1-S8a).** `postgres-types` has ZERO `Kind::Domain` handling: measured
/// live on PG 17, `<String as ToSql>::accepts(domain_over_text)` is `false`, and likewise for
/// `bool`/`Vec<u8>`. `stmt.params()` reports a parameter's DOMAIN oid verbatim (unlike
/// `RowDescription`, which resolves to the base — which is why READS already worked), so without
/// this wrapper a `Value::Text` bound to a `CREATE DOMAIN … AS text` column is refused by the impl.
/// Resolving in the PRE-FLIGHT alone would be worse than not resolving at all: the pre-flight would
/// then be LOOSER than the impl it fronts, `to_sql_checked` would fail with an error carrying no
/// `DbError`, `is_session_fatal` would read that as `ConnectionLost`, and §19.3 would report a
/// **false `Indeterminate`** for a write that was never sent — the precise hazard the pre-flight
/// exists to prevent, created by the fix for it.
macro_rules! pg_domain_aware_param {
    ($(#[$meta:meta])* $name:ident wraps $inner:ty) => {
        $(#[$meta])*
        #[derive(Debug)]
        struct $name($inner);

        impl ToSql for $name {
            fn to_sql(
                &self,
                ty: &Type,
                out: &mut tokio_postgres::types::private::BytesMut,
            ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
                <$inner as ToSql>::to_sql(&self.0, resolve_domain(ty), out)
            }

            fn accepts(ty: &Type) -> bool {
                <$inner as ToSql>::accepts(resolve_domain(ty))
            }

            to_sql_checked!();
        }
    };
}

pg_domain_aware_param! {
    /// `BOOL` → `bool`, plus a domain over one. Binary format, exactly as the bare `bool` was.
    PgBool wraps bool
}

pg_domain_aware_param! {
    /// `TEXT` → `text`/`varchar`/`bpchar`/`name` (whatever `String`'s own `accepts` list holds),
    /// plus a domain over any of them. The inner impl writes the bytes verbatim and ignores the
    /// `Type`, so handing it the RESOLVED type changes nothing about the payload.
    PgText wraps String
}

pg_domain_aware_param! {
    /// `BYTES` → `bytea`, plus a domain over it.
    PgBytes wraps Vec<u8>
}
```

`value_to_boxed` (`bind.rs:195-199`) routes the three tags through them:

```rust
        Value::Bool(b) => Box::new(PgBool(*b)),
        Value::Text(s) => Box::new(PgText(s.clone())),
        Value::Bytes(b) => Box::new(PgBytes(b.clone())),
```

`PgInt` and `PgFloat` (Task 4) resolve in both methods too — `to_sql`'s comparisons and `accepts`
both read the resolved type:

```rust
impl ToSql for PgInt {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // `to_sql_checked!()` hands us the DECLARED type, which for a domain parameter is the
        // domain. `accepts` below resolves identically, so the pair stays a mirror.
        let base = resolve_domain(ty);
        if *base == Type::INT2 {
            i16::try_from(self.0)?.to_sql(base, out)
        } else if *base == Type::INT4 {
            i32::try_from(self.0)?.to_sql(base, out)
        } else if *base == Type::INT8 {
            self.0.to_sql(base, out)
        } else {
            Err(format!("PgInt cannot bind PG type {}", ty.name()).into())
        }
    }

    fn accepts(ty: &Type) -> bool {
        [Type::INT2, Type::INT4, Type::INT8].contains(resolve_domain(ty))
    }

    to_sql_checked!();
}
```

`PgFloat` takes the identical two-line change. The seven canonical-text newtypes get it in ONE place, the `pg_canonical_text_param!` macro body (`bind.rs:90-92`):

```rust
            /// NARROW by construction — only the types listed at the declaration site, or a DOMAIN
            /// over one of them (M1-S8a). `to_sql` writes the canonical text verbatim and ignores
            /// the `Type` entirely, so it needs no matching change: the two stay a mirror because
            /// the text impl accepts every type its `accepts` admits.
            fn accepts(ty: &Type) -> bool {
                [$(Type::$ty),+].contains(resolve_domain(ty))
            }
```

`PgU64Text::accepts` stays `false` for everything — a domain over nothing is still nothing — and `PgNull` stays universally true. Both are correct unchanged, and both must be left alone.

- [ ] **Step 5: Extend the lockstep fixture with domain targets — including the three the v1 fixture missed**

`every_target_type()` (`bind.rs:376-401`) gains **five** domains plus a nested one. The `text`/`bool`/`bytea` domains are not decoration: they are the exact targets whose absence let the broken v1 design pass its own safety proof (probe 1, B2). `every_target_type()` cannot be compile-forced (`tokio_postgres::Type` is external and open — Task 4 Step 6), so growing it by hand and mutation-proving the growth is the only guard available.

```rust
            // M1-S8a: domains, which `stmt.params()` reports VERBATIM for a parameter slot (unlike
            // `RowDescription`, which resolves to the base). Without these the cross-product proof
            // never exercises `resolve_domain` at all.
            //
            // The text/bool/bytea entries are load-bearing, not symmetry: `postgres-types` has no
            // `Kind::Domain` handling, so those three are precisely the arms where a half-applied
            // unwrap (pre-flight resolves, boxed impl does not) is LOOSER than the impl — and a
            // fixture carrying only dom_int4/dom_numeric stays GREEN over that bug, because
            // `PgInt`/`PgDecimalText` are Ferro-owned and resolve on both sides already.
            Type::new("dom_int4".to_string(), 900_001, Kind::Domain(Type::INT4), "public".to_string()),
            Type::new("dom_numeric".to_string(), 900_002, Kind::Domain(Type::NUMERIC), "public".to_string()),
            Type::new("dom_text".to_string(), 900_010, Kind::Domain(Type::TEXT), "public".to_string()),
            Type::new("dom_bool".to_string(), 900_011, Kind::Domain(Type::BOOL), "public".to_string()),
            Type::new("dom_bytea".to_string(), 900_012, Kind::Domain(Type::BYTEA), "public".to_string()),
            // A domain over a domain is legal in PG; this exercises the bounded loop, not just the
            // single-step unwrap.
            Type::new(
                "dom_dom_int4".to_string(),
                900_013,
                Kind::Domain(Type::new(
                    "dom_int4".to_string(),
                    900_001,
                    Kind::Domain(Type::INT4),
                    "public".to_string(),
                )),
                "public".to_string(),
            ),
```

`every_target_type` needs `use tokio_postgres::types::Kind;` in the test module. Add a comment above the function recording that it is behaviourally guarded, not compile-forced, and why.

- [ ] **Step 6: Write the LIVE gate covering Task 4 + Task 5 together**

Append to `engine/crates/ferro-backend-pg/tests/pg_types_it.rs`:

```rust
/// M1-S8a acceptance: the two bind fixes, against a real server.
///
/// (a) a PHP-shaped `int` inserts into `int2`/`int4`/`serial` columns and reads back exactly;
/// (b) an out-of-range narrowing is a KNOWN-FATE pre-send refusal — asserted by a READ-BACK showing
///     the table is unchanged, not merely by the error type (an error alone would not distinguish a
///     pre-send refusal from a post-send failure, which is the whole §19.3 point);
/// (c) a DOMAIN-typed parameter binds, closing SPEC §22.2 (g)'s "readable but not bindable".
#[tokio::test]
async fn s8a_narrowing_and_domain_binds_round_trip() {
    let Some(url) = std::env::var("FERRO_TEST_PG_URL").ok() else {
        println!("skip: FERRO_TEST_PG_URL unset");
        return;
    };
    let (backend, mut conn) = connect(&url).await;
    backend.simple_query(&mut conn, "DROP TABLE IF EXISTS s8a_narrow").await.unwrap();
    backend.simple_query(&mut conn, "DROP DOMAIN IF EXISTS s8a_posint").await.unwrap();
    backend.simple_query(&mut conn, "CREATE DOMAIN s8a_posint AS int4 CHECK (VALUE > 0)").await.unwrap();
    backend
        .simple_query(
            &mut conn,
            "CREATE TABLE s8a_narrow (id serial PRIMARY KEY, small int2, med int4, big int8, \
             r float4, d float8, dom s8a_posint)",
        )
        .await
        .unwrap();

    // (a)
    backend
        .query(
            &mut conn,
            "INSERT INTO s8a_narrow (small, med, big, r, d, dom) VALUES (?, ?, ?, ?, ?, ?)",
            &[
                Value::I64(7),
                Value::I64(2_147_483_647),
                Value::I64(i64::MAX),
                Value::F64(1.5),
                Value::F64(2.25),
                Value::I64(11),
            ],
        )
        .await
        .expect("a PHP int must bind int2/int4/int8 and a domain over int4");

    let back = backend
        .query(&mut conn, "SELECT small, med, big, r, d, dom FROM s8a_narrow", &[])
        .await
        .unwrap();
    assert_eq!(back.rows[0][0], Value::I64(7));
    assert_eq!(back.rows[0][1], Value::I64(2_147_483_647));
    assert_eq!(back.rows[0][2], Value::I64(i64::MAX));
    assert_eq!(back.rows[0][3], Value::F64(1.5));
    assert_eq!(back.rows[0][4], Value::F64(2.25));
    assert_eq!(back.rows[0][5], Value::I64(11));

    // (b) — the pre-send refusal, proven by the read-back.
    let err = backend
        .query(
            &mut conn,
            "INSERT INTO s8a_narrow (med) VALUES (?)",
            &[Value::I64(i64::from(i32::MAX) + 1)],
        )
        .await
        .expect_err("an out-of-range narrowing must be refused");
    match &err {
        PoolError::Sql { code, sqlstate, message, .. } => {
            assert_eq!(*code, ferro_proto::consts::errc::UNSUPPORTED);
            assert_eq!(*sqlstate, None, "no SQLSTATE: the server never saw the statement");
            assert!(message.contains("out of range"), "{message}");
        }
        other => panic!("expected a known-fate Sql refusal, got {other:?}"),
    }
    let after = backend.query(&mut conn, "SELECT count(*) FROM s8a_narrow", &[]).await.unwrap();
    assert_eq!(after.rows[0][0], Value::I64(1), "the refused insert must not have applied");

    // (c) — the domain read/write symmetry SPEC §22.2 (g) said was broken.
    let dom = backend
        .query(&mut conn, "SELECT dom FROM s8a_narrow", &[])
        .await
        .unwrap();
    backend
        .query(&mut conn, "INSERT INTO s8a_narrow (dom) VALUES (?)", &[dom.rows[0][0].clone()])
        .await
        .expect("a value read from a domain column must bind back into it");
}
```

Also update `engine/crates/ferrod/tests/types_e2e_it.rs::pg_domain_reads_but_does_not_bind` — its name and assertion are now false. Rename to `pg_domain_reads_and_binds` and flip the expectation; **do not delete it** (it is the regression lock for §22.2 (g)).

- [ ] **Step 7: Run, then MUTATION-PROVE**

```
FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro \
  cargo test -p ferro-backend-pg --test pg_types_it s8a_narrowing_and_domain -- --nocapture
cargo test -p ferro-backend-pg bind
```
Mutate:

1. Make `resolve_domain` return `ty` unchanged → all three offline domain tests and the live (c) block go RED.
2. **The blocker-B2 mutation — the one that matters most.** Revert `value_to_boxed`'s three arms to the bare `bool` / `String` / `Vec<u8>` (i.e. delete `PgBool`/`PgText`/`PgBytes` from the boxing) while leaving `check_param` resolving → `s8a_bool_text_and_bytes_resolve_the_domain_on_both_sides` goes RED on its `to_sql_checked` half, **and** `s7_accepts_is_never_looser_than_the_boxed_impl` goes RED on `(Value::Text, dom_text)`, `(Value::Bool, dom_bool)` and `(Value::Bytes, dom_bytea)`. That second failure is the directional guard catching precisely the shape the v1 plan prescribed. Restore.
3. **Prove the fixture growth was load-bearing.** Re-apply mutation 2, and this time ALSO remove `dom_text`/`dom_bool`/`dom_bytea` from `every_target_type()` → `s7_accepts_is_never_looser_than_the_boxed_impl` goes **GREEN over the bug**, and only the dedicated `s8a_bool_text_and_bytes_…` test still catches it. **Record this**: it is the demonstration that a fixture carrying only `dom_int4`/`dom_numeric` — the v1 fixture — certifies a broken pre-flight. Restore both.
4. Remove `resolve_domain` from `PgInt::accepts` but keep it in `check_param` → the cross-product proof goes RED on `(Value::I64, dom_int4)` for the same reason. Restore.
5. Remove `resolve_domain` from the `pg_canonical_text_param!` macro body → the proof goes RED on `(Value::Decimal, dom_numeric)`. Restore.
6. Set `MAX_DOMAIN_DEPTH` to `1` → `s8a_a_domain_parameter_is_checked_against_its_base_type`'s nested assertion (`assert!(accepts(&Value::I64(7), &dom_dom))`) goes RED, proving the resolver is a LOOP and not a single unwrap. **Note which guard fires and which does not:** the cross-product proof stays GREEN here, because a refusing `accepts` makes it `continue` — it only ever catches "too loose", never "too strict". That asymmetry is by design (the directional rule), and it is why the nested case needs its own positive assertion.

- [ ] **Step 8: Spec truth + commit**

Amend `ferro-spec-v0.2.md` §22.2 (g) — it currently says the bind-direction fix "is folded into the narrowing-bind carry below" — to record that it SHIPPED here, and §22.2 (k)(2) to mark the narrowing carry closed.

```bash
./ci/local-gate.sh --live
git add engine ferro-spec-v0.2.md
git commit -m "feat(m1-s8a): resolve a parameter's DOMAIN to its base type, on BOTH sides of the bind

stmt.params() reports a domain's OWN oid (RowDescription resolves to the base,
which is why reads already worked), so a value read out of a user-defined domain
column could not be bound back into it — SPEC §22.2 (g).

The unwrap is a bounded loop and is applied in accepts AND to_sql for EVERY tag.
That is not symmetry: postgres-types has no Kind::Domain handling at all, so
resolving in the pre-flight while delegating Bool/Text/Bytes to its own impls
would make the pre-flight LOOSER than the impl it fronts — to_sql_checked then
fails with an error carrying no DbError, is_session_fatal reads that as
ConnectionLost, and §19.3 reports a false Indeterminate for a write that was
never sent. Bool/Text/Bytes therefore box through Ferro-owned wrappers, and the
lockstep fixture gains domains over text/bool/bytea, without which the safety
proof stays green over exactly that bug.

Live gate proves the narrowing bind (Task 4) and the domain unwrap together,
including a READ-BACK showing an out-of-range refusal never applied.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 6: Catalog + ENUM type coverage — the schema-introspection blocker on both engine families

**Files:**
- Modify: `engine/crates/ferro-backend-pg/src/rowmap.rs` (`ExtractType`, `oid_to_tag:97`, `oid_extract_type:129`, `extract_value:168`)
- Modify: `engine/crates/ferro-backend-pg/src/pgtext.rs` (the `"char"` renderer)
- Modify: `engine/crates/ferro-backend-mysql/src/rowmap.rs:170-200` (`column_kind`'s string family + the standalone ENUM arm), `:410` (drop `ENUM` from `unsupported()`'s trailing deferred list), `:628-661` (`deferred_types_stay_unsupported` loses its `ENUM_FLAG` case)
- Create: `engine/crates/ferrod/tests/catalog_it.rs`
- Modify: `ferro-spec-v0.2.md` §9 (the PG/MySQL classification paragraphs) + §22.2 (h)/(k)(3)
- Test: `engine/crates/ferro-backend-pg/src/rowmap.rs` unit; `engine/crates/ferrod/tests/catalog_it.rs`

**Interfaces:**
- Consumes: nothing from Tasks 1–5.
- Produces: `ExtractType::{CharByte, OidU32, RegOid}` in `ferro-backend-pg`'s `rowmap`; `pgtext::char_byte_to_text(b: u8) -> Result<String, PoolError>`. **No new canonical tags and no `/proto` change** — every admitted type maps onto `TEXT` or `I64`.

**The mapping, and why each is the only truthful choice:**

| PG type | OID | canonical tag | how |
|---|---|---|---|
| `name` | 19 | `TEXT` | `Type::NAME` is already in `String`'s `FromSql::accepts` list — it folds into the existing `ExtractType::Text` arm, no new machinery (hazard 9). |
| `"char"` | 18 | `TEXT` | a SINGLE BYTE read as `i8`. `0 → ""` (PG's own text output for `'\0'`, which is what `attidentity` holds on a non-identity column), ASCII → a 1-char string, non-ASCII → a loud `Backend` decode mismatch (hazard 8). |
| `oid` | 26 | `I64` | reads as `u32`, widens losslessly. |
| `regtype`, `regclass` | 2206, 2205 | `I64` | their BINARY payload is a bare 4-byte OID (`regtypesend` **is** `oidsend`). Reporting the type NAME would need a catalog round trip the engine must not make (charter rule 6). Callers wanting the name cast in SQL — which DBAL's own introspection already does via `format_type(...)`, returning `text`. |

MySQL/MariaDB `ENUM` → `TEXT` (or `BYTES` on a binary collation, i.e. the same charset branch every other string type takes). An ENUM cell's binary-protocol value **is** the label string in the column's charset, so this is a lossless representation, not a promotion. **`SET` stays `Unsupported`** — its wire form is a comma-joined multi-value string, which is a different type, not a longer one.

**CORRECTED — WHICH of the two ENUM rejections actually matters (probe 1, major M2; hazard 12).** The v1 text said fixing one site "leaves `information_schema` still broken". That is backwards. Measured on both engines: `MYSQL_TYPE_ENUM` **never reaches the client at all** — `information_schema.COLUMNS.COLUMN_KEY` **and** a user-declared `ENUM('a','b')` column both arrive as `MYSQL_TYPE_STRING` carrying `ENUM_FLAG`. So:

- the **string-family `ENUM_FLAG` early-return** (`rowmap.rs:179-181`) is the one and only fix that unblocks anything, and it fixes `information_schema` completely on its own;
- the **standalone `ColumnType::MYSQL_TYPE_ENUM` arm** (`rowmap.rs:195`) is fixed as **defence in depth** — a server or driver version that does send the dedicated type code must not then hit a stale refusal — but **no test may claim to prove it**, because no live traffic reaches it. A test aimed at that arm is a guard that cannot fail, which is the defect class this plan exists to avoid. It is covered by the offline `column_kind` unit tests only, where the `Column` fixture is constructed directly.

- [ ] **Step 1: Write the failing PG unit tests**

In `engine/crates/ferro-backend-pg/src/rowmap.rs`'s `mod tests`:

```rust
    /// M1-S8a: the catalog scalars DBAL's `AbstractSchemaManager` selects on every introspection.
    /// Both gates must admit them — `oid_to_tag` (cols-build) and `oid_extract_type` (per-cell) are
    /// matches over ONE table precisely so they cannot drift (hazard 7).
    #[test]
    fn s8a_catalog_scalars_are_admitted_by_both_gates() {
        for (ty, want) in [
            (Type::NAME, tag::TEXT),
            (Type::CHAR, tag::TEXT),
            (Type::OID, tag::I64),
            (Type::REGTYPE, tag::I64),
            (Type::REGCLASS, tag::I64),
        ] {
            assert!(
                oid_extract_type(ty.oid()).is_some(),
                "{ty:?} must have an extraction type"
            );
            assert_eq!(
                oid_to_tag("c", &ty).expect("admitted"),
                want,
                "{ty:?} must map to the canonical tag {want}"
            );
        }
    }

    /// The still-deferred neighbours stay LOUD — admitting the catalog family must not quietly
    /// widen anything else.
    #[test]
    fn s8a_catalog_admission_does_not_widen_the_deferred_set() {
        for ty in [Type::TIMETZ, Type::INTERVAL, Type::INET, Type::INT4_ARRAY, Type::OID_VECTOR] {
            assert!(
                oid_to_tag("c", &ty).is_err(),
                "{ty:?} must stay a loud Unsupported"
            );
        }
    }

    /// PG's `"char"` is one BYTE, and `'\0'` — what `attidentity` holds on a non-identity column —
    /// renders as the EMPTY string, exactly as PG's own text output does. A non-ASCII byte has no
    /// canonical text form and is a loud decode mismatch, never a lossy replacement character.
    #[test]
    fn s8a_char_byte_rendering_matches_pg_text_output() {
        assert_eq!(crate::pgtext::char_byte_to_text(0).unwrap(), "");
        assert_eq!(crate::pgtext::char_byte_to_text(b'a').unwrap(), "a");
        assert_eq!(crate::pgtext::char_byte_to_text(b'd').unwrap(), "d");
        let e = crate::pgtext::char_byte_to_text(0xff).expect_err("non-ASCII has no canonical form");
        assert!(matches!(e, PoolError::Backend(_)), "a decode mismatch is Backend, never ConnectionLost");
    }
```

And the MySQL half, in `engine/crates/ferro-backend-mysql/src/rowmap.rs`'s `mod tests` — offline, over the file's existing `col`/`kind` fixture helpers (`rowmap.rs:442-453`), because this is where the **standalone `MYSQL_TYPE_ENUM` arm can honestly be exercised at all** (no live server sends it — hazard 12 as corrected):

```rust
    /// M1-S8a: an ENUM column classifies as its charset's string kind, in BOTH places the type can
    /// be spelled. The FIRST case is the one live traffic actually takes (measured: both engines
    /// send an ENUM as `MYSQL_TYPE_STRING | ENUM_FLAG`); the SECOND can only be reached from a
    /// hand-built `Column`, which is exactly why it is tested here and NOT in the live gate — a
    /// live test aimed at it could never fail.
    #[test]
    fn s8a_enum_classifies_as_a_string_in_both_spellings() {
        // The reachable spelling.
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_STRING, ColumnFlags::ENUM_FLAG, 4, UTF8MB4_MYSQL),
            MyKind::Text
        );
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_STRING, ColumnFlags::ENUM_FLAG, 4, UTF8MB4_MARIA),
            MyKind::Text
        );
        // A binary-collation ENUM takes the SAME charset branch as every other string type.
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_STRING, ColumnFlags::ENUM_FLAG, 4, BIN),
            MyKind::Bytes
        );
        // The unreachable-but-fixed spelling.
        assert_eq!(
            kind(ColumnType::MYSQL_TYPE_ENUM, NO_FLAGS, 4, UTF8MB4_MYSQL),
            MyKind::Text
        );
    }

    /// SET stays out of scope in BOTH spellings, and the refusal names the type in a way that
    /// CANNOT be satisfied by the message's trailing "Deferred: …" list (hazard 65).
    #[test]
    fn s8a_set_stays_unsupported_in_both_spellings() {
        for (ct, flags) in [
            (ColumnType::MYSQL_TYPE_STRING, ColumnFlags::SET_FLAG),
            (ColumnType::MYSQL_TYPE_SET, NO_FLAGS),
        ] {
            let c = col(ct, flags, 12, UTF8MB4_MARIA);
            let msg = match column_kind(&c) {
                Err(PoolError::Unsupported(m)) => m,
                other => panic!("{ct:?} must stay Unsupported, got {other:?}"),
            };
            assert!(
                msg.contains("MySQL SET ("),
                "the refusal must name SET as the OFFENDING type, not merely list it among the \
                 deferred ones: {msg}"
            );
        }
    }
```

**One existing test changes and must not simply be deleted.** `deferred_types_stay_unsupported` (`rowmap.rs:628-661`) currently asserts that `MYSQL_TYPE_STRING | ENUM_FLAG` is `Unsupported`. Remove **that one case** from its table — leaving `YEAR`, `BIT`, `SET`, `GEOMETRY`, `VECTOR`, `NULL` — and leave the rest of the test intact: it is the deferral lock for everything S8a does *not* admit, and shrinking it by one case is exactly the visible, reviewable edit the change deserves.

- [ ] **Step 2: Run and watch it fail**

```
cargo test -p ferro-backend-pg rowmap::tests::s8a_
cargo test -p ferro-backend-mysql rowmap::tests::s8a_
```
Expected: FAIL — `oid_extract_type` has no arm for `NAME`/`CHAR`/`OID`/`REGTYPE`/`REGCLASS`, `char_byte_to_text` does not exist, and both MySQL ENUM spellings still return `Unsupported`.

- [ ] **Step 3: Add the PG renderer**

In `engine/crates/ferro-backend-pg/src/pgtext.rs`:

```rust
/// PG's `"char"` (OID 18) is a SINGLE BYTE, not a string — `postgres-types` reads it as `i8`. Render
/// it the way PG's own `charout` does: `'\0'` is the EMPTY string (which is what `pg_attribute
/// .attidentity` holds for a non-identity column, and what DBAL's schema manager compares against),
/// any ASCII byte is that one character.
///
/// A non-ASCII byte has no canonical-text form (`PROTOCOL.md` §3.2 defines none) and inventing one
/// would differ between the two codecs — so it is a client-side decode mismatch:
/// `PoolError::Backend` (NonRetryable), **never** `ConnectionLost`, so it can never mint a false
/// §19.3 `Indeterminate` (SPEC §9.1).
pub(crate) fn char_byte_to_text(b: u8) -> Result<String, PoolError> {
    match b {
        0 => Ok(String::new()),
        0x01..=0x7f => Ok((b as char).to_string()),
        _ => Err(PoolError::Backend(format!(
            "PG \"char\" byte 0x{b:02x} is not ASCII and has no canonical text form"
        ))),
    }
}
```

- [ ] **Step 4: Add the three extraction kinds and wire both gates**

`engine/crates/ferro-backend-pg/src/rowmap.rs` — `ExtractType` gains:

```rust
    /// `"char"` (OID 18) — ONE byte read as `i8`, rendered by [`crate::pgtext::char_byte_to_text`].
    /// Not `Text`: the payload is a bare byte, so `String`'s `FromSql` would reject it outright.
    CharByte,
    /// `oid` (OID 26) — read as `u32`, widened losslessly to `Value::I64`.
    OidU32,
    /// The `reg*` alias family (`regtype` 2206, `regclass` 2205) — their BINARY payload is a bare
    /// 4-byte OID (`regtypesend` IS `oidsend`), so the only truthful report is the numeric oid.
    /// A caller wanting the NAME casts in SQL (`::text` / `format_type(...)`); resolving it here
    /// would mean a catalog round trip the engine must not make (charter rule 6).
    RegOid,
```

`oid_extract_type` (`rowmap.rs:129`):

```rust
        // ---- M1-S8a catalog scalars. `name` folds into the EXISTING Text arm: `Type::NAME` is
        // already in `String`'s `FromSql::accepts` list, so no raw read is involved.
        o if o == Type::TEXT.oid()
            || o == Type::VARCHAR.oid()
            || o == Type::BPCHAR.oid()
            || o == Type::NAME.oid() => Some(ExtractType::Text),
        o if o == Type::CHAR.oid() => Some(ExtractType::CharByte),
        o if o == Type::OID.oid() => Some(ExtractType::OidU32),
        o if o == Type::REGTYPE.oid() || o == Type::REGCLASS.oid() => Some(ExtractType::RegOid),
```

`oid_to_tag` (`rowmap.rs:97`):

```rust
        Some(ExtractType::Text | ExtractType::CharByte) => Ok(tag::TEXT),
        Some(ExtractType::I16 | ExtractType::I32 | ExtractType::I64
            | ExtractType::OidU32 | ExtractType::RegOid) => Ok(tag::I64),
```

`extract_value` (`rowmap.rs:168`) — new arms, each inside the gate the function-local `RawBytes` is contained by (hazard: `RawBytes::accepts` is universally true, so it must only ever be reached through an arm `oid_extract_type` already selected):

```rust
        Some(ExtractType::CharByte) => Ok(get_opt::<i8>(row, idx)?
            .map_or(Ok(Value::Null), |b| pgtext::char_byte_to_text(b as u8).map(Value::Text))?),
        Some(ExtractType::OidU32) => {
            Ok(get_opt::<u32>(row, idx)?.map_or(Value::Null, |n| Value::I64(i64::from(n))))
        }
        // `u32`'s `FromSql::accepts` is `Type::OID` ONLY, so a `regtype`/`regclass` cannot use it.
        // The payload is nevertheless a 4-byte big-endian oid, read here through the gate-local raw
        // getter and decoded explicitly — never guessed.
        Some(ExtractType::RegOid) => Ok(match get_opt::<RawBytes>(row, idx)? {
            None => Value::Null,
            Some(b) => {
                let arr: [u8; 4] = b.0.try_into().map_err(|_| {
                    PoolError::Backend(format!(
                        "PG reg* payload must be 4 bytes, got {}",
                        b.0.len()
                    ))
                })?;
                Value::I64(i64::from(u32::from_be_bytes(arr)))
            }
        }),
```

- [ ] **Step 5: Admit MySQL/MariaDB `ENUM` in BOTH rejection sites (hazard 12)**

`engine/crates/ferro-backend-mysql/src/rowmap.rs` — factor the charset branch out so both sites share it:

```rust
/// The string/blob family's Text-vs-Bytes decision: the binary collation (63) means a byte string,
/// anything else is character data. Shared by the `STRING`-family arm and the standalone
/// `MYSQL_TYPE_ENUM` arm so the two can never diverge — the ENUM rejection used to live in BOTH
/// places and fixing one would have left `information_schema` broken.
fn string_family_kind(col: &Column) -> MyKind {
    if col.character_set() == BINARY_COLLATION_ID {
        MyKind::Bytes
    } else {
        MyKind::Text
    }
}
```

The string-family arm (`rowmap.rs:170-190`) drops the `ENUM_FLAG` early-return and keeps the `SET_FLAG` one:

```rust
        | ColumnType::MYSQL_TYPE_LONG_BLOB => {
            // M1-S8a: `ENUM_FLAG` is NO LONGER a rejection. An ENUM cell's binary-protocol value IS
            // the label string in the column's charset — carrying it as TEXT is lossless, not a
            // promotion (contrast MariaDB's JSON-as-LONGTEXT, which WOULD be a promotion because the
            // wire cannot distinguish it from a plain LONGTEXT).
            //
            // THIS is the fix that unblocks DBAL's schema manager, and it is sufficient on its own:
            // measured on MySQL 8.4 and MariaDB 11.8, an ENUM column — whether it is
            // `information_schema.COLUMNS.COLUMN_KEY`, `referential_constraints.UPDATE_RULE`, or a
            // user-declared `ENUM('a','b')` — arrives as MYSQL_TYPE_STRING + ENUM_FLAG and lands
            // here. The dedicated MYSQL_TYPE_ENUM code never reaches the client.
            if flags.contains(ColumnFlags::SET_FLAG) {
                // SET stays out of scope: its wire form is a COMMA-JOINED multi-value string, a
                // different type rather than a longer one.
                return Err(unsupported(col, "SET"));
            }
            Ok(string_family_kind(col))
        }
```

and the standalone arm (`rowmap.rs:195`) — **defence in depth, not the unblocking fix**:

```rust
        // Unreachable from any server this project tests against: both engines send an ENUM column
        // as MYSQL_TYPE_STRING + ENUM_FLAG (handled above). Fixed anyway so a server or driver
        // version that DOES send the dedicated code cannot hit a stale refusal — but deliberately
        // NOT the subject of a live test, because no live traffic can reach it and such a test
        // would be a guard that cannot fail. The offline `column_kind` unit tests, which build the
        // `Column` fixture directly, are its only coverage and that is the honest amount.
        ColumnType::MYSQL_TYPE_ENUM => Ok(string_family_kind(col)),
        ColumnType::MYSQL_TYPE_SET => Err(unsupported(col, "SET")),
```

and — spec truth, and the thing that makes the `SET` assertion in Step 6 falsifiable at all — drop `ENUM` from `unsupported()`'s trailing deferred list (`rowmap.rs:410`), which currently reads `"Deferred: YEAR, BIT, ENUM, SET, GEOMETRY and VECTOR."`:

```rust
         Deferred: YEAR, BIT, SET, GEOMETRY and VECTOR.",
```

- [ ] **Step 6: Write the LIVE introspection gate**

`engine/crates/ferrod/tests/catalog_it.rs` — a fresh file. One `exec_server` per engine (the helper infers `kind` from the DSN scheme, so the same constructor covers PG, MySQL and MariaDB); no multi-pool daemon is needed here, and none is built.

**Sourcing the SQL.** These queries are the *catalog columns Doctrine's schema manager selects*, written out here so S8a carries **no DBAL dependency**. Before writing the test, obtain DBAL 4.4.4's verbatim statements to confirm the column list:

```bash
mkdir -p /tmp/dbal4 && cd /tmp/dbal4 \
  && composer require doctrine/dbal:^4.4 --no-interaction \
  && grep -n "typname\|attidentity\|COLUMN_KEY\|UPDATE_RULE" \
       vendor/doctrine/dbal/src/Schema/PostgreSQLSchemaManager.php \
       vendor/doctrine/dbal/src/Schema/MySQLSchemaManager.php
```

If the network is unavailable, the test below still stands on its own — it selects each newly-admitted catalog type explicitly — and S8b substitutes DBAL's verbatim SQL then.

**There is no `harness_pg`/`harness_mysql` and no `harness()` anywhere in this repo (hazard 63).** What exists in `tests/common/mod.rs` is `exec_server(url) -> TestServer` — which already infers `kind` from the DSN scheme, so ONE constructor serves both engine families — plus `TestServer::connect()`, `TestClient::hello(rid)`, `req(sql)`, and `exec`/`exec_ok`/`exec_err(client, rid, &req)`. `catalog_it.rs` uses those directly; it introduces no new harness. The only helper it adds is a two-line local `fn ddl(sql: &str) -> ExecRequest` — `req(sql)` with `readonly = false` and `fetch = FETCH_NONE` — for the setup statements, plus a copy of `mysql_it.rs`'s `mysql_targets()`.

```rust
/// M1-S8a acceptance: schema introspection works on BOTH engine families.
///
/// The assertion is DERIVED, not a hard-coded list: for every column the query returns, the tag
/// `HEAD` promised (`cols[i].tag`) must equal the tag the producer emitted (`rows[0][i].tag()`).
/// That is the hazard-7 lockstep proof driven by real catalog data rather than by a fixture.
#[tokio::test]
async fn pg_catalog_introspection_columns_are_readable() {
    let Some(url) = pg_url() else { return }; // prints `skip: FERRO_TEST_PG_URL unset`
    let server = exec_server(url);
    let mut c = server.connect().await;
    c.hello(0).await;

    exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_cat")).await;
    exec_ok(
        &mut c,
        2,
        &ddl("CREATE TABLE s8a_cat (id int GENERATED ALWAYS AS IDENTITY, plain text)"),
    )
    .await;

    // The catalog columns DBAL's PostgreSQLSchemaManager::selectTableColumns selects:
    //   t.typname     -> name    (OID 19)
    //   a.attidentity -> "char"  (OID 18)   -- '\0' on a NON-identity column
    //   c.oid         -> oid     (OID 26)
    //   a.atttypid    -> oid
    //   a.atttypid::regtype      (OID 2206)
    let r = exec_ok(
        &mut c,
        3,
        &req(
            "SELECT t.typname, a.attidentity, c.oid, a.atttypid, a.atttypid::regtype \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE c.relname = 's8a_cat' AND a.attnum > 0 \
             ORDER BY a.attnum",
        ),
    )
    .await;

    assert_eq!(r.cols.len(), 5);
    assert!(!r.rows.is_empty(), "the table has columns");
    for (i, col) in r.cols.iter().enumerate() {
        assert_eq!(
            col.tag,
            r.rows[0][i].tag(),
            "HEAD promised tag {} for {:?} but the producer emitted {}",
            col.tag,
            col.name,
            r.rows[0][i].tag()
        );
    }
    // The identity column reports 'a'; a plain one reports the EMPTY string, not "\0".
    let identity: Vec<&Value> = r.rows.iter().map(|row| &row[1]).collect();
    assert!(identity.contains(&&Value::Text("a".to_string())), "the IDENTITY column reports 'a'");
    assert!(identity.contains(&&Value::Text(String::new())), "a plain column reports \"\"");
}

/// MySQL/MariaDB ENUM columns read as their label.
///
/// **The user-table column is the PRIMARY case, not the secondary one (probe 1, major M3).** v1
/// drove the whole proof through `information_schema.COLUMNS.COLUMN_KEY`, which on **MariaDB is
/// `MYSQL_TYPE_VAR_STRING`, not an ENUM** — so the MariaDB arm was GREEN BEFORE THE FIX and proved
/// nothing there. A user-declared `ENUM(...)` column arrives as `MYSQL_TYPE_STRING | ENUM_FLAG` on
/// BOTH engines (measured) and is therefore the assertion that can fail on both. The
/// `information_schema` read stays as well, because it is the actual DBAL traffic
/// (`MySQLSchemaManager::selectTableColumns` selects `COLUMN_KEY`;
/// `selectForeignKeyColumns` selects `referential_constraints.UPDATE_RULE`/`DELETE_RULE`) — but it
/// is asserted as a smoke test of readability, not as the ENUM proof.
#[tokio::test]
async fn mysql_enum_columns_read_as_their_label() {
    for (label, url) in mysql_targets() {
        let server = exec_server(url);
        let mut c = server.connect().await;
        c.hello(0).await;

        // (1) THE proof, on both engines: a user ENUM column.
        exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_enum")).await;
        exec_ok(
            &mut c,
            2,
            &ddl("CREATE TABLE s8a_enum (id INT PRIMARY KEY, mood ENUM('sad','ok','happy'))"),
        )
        .await;
        exec_ok(&mut c, 3, &ddl("INSERT INTO s8a_enum VALUES (1, 'happy')")).await;

        let r = exec_ok(&mut c, 4, &req("SELECT mood FROM s8a_enum WHERE id = 1")).await;
        assert_eq!(
            r.rows[0][0],
            Value::Text("happy".to_string()),
            "[{label}] an ENUM cell's wire value IS its label string"
        );
        assert_eq!(
            r.cols[0].tag,
            r.rows[0][0].tag(),
            "[{label}] HEAD/producer tag disagreement on the ENUM column"
        );

        // (2) The real DBAL traffic still reads end to end. Derived HEAD-vs-producer assertion over
        // every column the query returns — no parallel tag table.
        exec_ok(&mut c, 5, &ddl("DROP TABLE IF EXISTS s8a_cat")).await;
        exec_ok(&mut c, 6, &ddl("CREATE TABLE s8a_cat (id INT PRIMARY KEY, v INT)")).await;
        let r = exec_ok(
            &mut c,
            7,
            &req(
                "SELECT COLUMN_NAME, COLUMN_KEY, IS_NULLABLE FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 's8a_cat' \
                 ORDER BY ORDINAL_POSITION",
            ),
        )
        .await;
        assert!(!r.rows.is_empty(), "[{label}] the table has columns");
        for (i, col) in r.cols.iter().enumerate() {
            assert_eq!(
                col.tag,
                r.rows[0][i].tag(),
                "[{label}] HEAD/producer tag disagreement on {:?}",
                col.name
            );
        }
        assert_eq!(
            r.rows[0][1],
            Value::Text("PRI".to_string()),
            "[{label}] COLUMN_KEY must read as its label (an ENUM on MySQL, a VAR_STRING on \
             MariaDB — either way the value is the same and both must work)"
        );

        // (3) SET is still out of scope, and still LOUD — asserted on the OFFENDING-type head of
        // the message, not on the trailing "Deferred: …" list, which contains every deferred type
        // name and would make a bare `contains("SET")` unfalsifiable (hazard 65).
        exec_ok(&mut c, 8, &ddl("DROP TABLE IF EXISTS s8a_set")).await;
        exec_ok(&mut c, 9, &ddl("CREATE TABLE s8a_set (s SET('a','b'))")).await;
        let e = exec_err(&mut c, 10, &req("SELECT s FROM s8a_set")).await;
        assert_eq!(e.code, errc::UNSUPPORTED, "[{label}] SET stays Unsupported");
        assert!(
            e.message.contains("MySQL SET ("),
            "[{label}] the refusal must name SET as the offending type: {}",
            e.message
        );
        assert!(
            !e.message.contains("ENUM"),
            "[{label}] ENUM must be gone from the deferred list too: {}",
            e.message
        );
    }
}
```

`ddl(sql)` is the two-line local helper mentioned above (`req(sql)` with `readonly = false` and
`fetch = FETCH_NONE`); `mysql_targets()` is copied from `mysql_it.rs:38-47` (it prints `skip: <VAR>
unset` per unset engine, satisfying the live-lane gate).

- [ ] **Step 7: Run, then MUTATION-PROVE**

```
FERRO_TEST_PG_URL=… FERRO_TEST_MYSQL_URL=… FERRO_TEST_MARIADB_URL=… \
  cargo test -p ferrod --test catalog_it -- --nocapture
cargo test -p ferro-backend-pg rowmap
cargo test -p ferro-backend-mysql rowmap
```
Mutate:

1. Remove `Type::NAME` from the `Text` arm → the PG live test goes RED with `unsupported type for column "typname"`.
2. Add `CharByte` to `oid_extract_type` but NOT to `oid_to_tag`'s `TEXT` group → the build breaks (the match is exhaustive over `ExtractType`) — that is the lockstep guard being compile-forced.
3. **Restore the `ENUM_FLAG` early-return in the string family, leaving the standalone `MYSQL_TYPE_ENUM` arm fixed** → `mysql_enum_columns_read_as_their_label` goes RED **on both engines**, at the user-table assertion. This is the mutation the corrected design turns on: it proves the string-family fix is the one doing the work.
4. **The inverse, and record the result honestly:** restore the standalone `MYSQL_TYPE_ENUM => Err(unsupported(…))` arm while leaving the string family fixed → **every live test stays GREEN**, and only the offline `s8a_enum_classifies_as_a_string_in_both_spellings` goes RED. **Record this**: it is the measurement behind hazard 12's correction — no live traffic reaches that arm, so a live test claiming to prove it would be a guard that cannot fail. The v1 plan asserted the opposite.
5. Leave `ENUM` in `unsupported()`'s trailing `Deferred: …` list → the live SET test's `!contains("ENUM")` assertion goes RED, and (had the assertion been the v1 bare `contains("SET")`) nothing would have caught it.

Restore each.

- [ ] **Step 8: Spec truth + commit**

Amend `ferro-spec-v0.2.md` §9's PG paragraph (add the catalog scalars), §9's MySQL classification paragraph (`ENUM` moves out of the out-of-scope list into `TEXT`; `SET` stays), §22.2 (h) (remove `ENUM` from the deferred MySQL list) and §22.2 (k)(3) (mark the catalog carry closed, and record the `reg*`-reports-the-numeric-oid decision).

```bash
./ci/local-gate.sh --live
git add engine ferro-spec-v0.2.md
git commit -m "feat(m1-s8a): catalog + ENUM coverage — schema introspection works on PG and MySQL

PG name(19)/\"char\"(18)/oid(26)/regtype/regclass map onto the existing TEXT and
I64 tags (no new tags, no /proto change). \"char\" is one BYTE: '\\0' renders as
the empty string, exactly as PG's own text output does, which is what
attidentity holds on a non-identity column. reg* report the numeric oid — their
binary payload IS an oid, and resolving the name would need a catalog round trip
the engine must not make.

MySQL/MariaDB ENUM classifies as TEXT (its wire value IS the label string) in
BOTH rejection sites; SET stays a loud Unsupported.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 7: Savepoint passthrough — split the tx-control guard into BOUNDARY vs SAVEPOINT

**Files:**
- Modify: `engine/crates/ferro-pool/src/pin.rs:86-100` (the verb tables), `:149-170` (`is_bare_tx_control`)
- Modify: `engine/crates/ferro-pool/src/pool.rs:530-536`, `:578-584`, `:640-646` (all three guarded entries)
- Modify: `ferro-spec-v0.2.md` §7 + §22.2 (new entry `(o)`)
- Test: `engine/crates/ferro-pool/src/pin.rs` unit; `engine/crates/ferro-pool/tests/query_guard.rs`; `engine/crates/ferrod/tests/tx_it.rs`; `engine/crates/ferrod/tests/mysql_it.rs`

**Interfaces:**
- Consumes: nothing from Tasks 1–6.
- Produces, **crate-internal to `ferro-pool`** (`pub(crate)`, reachable from `pin.rs`'s own `mod tests` and from `pool.rs`, **not** from the `tests/query_guard.rs` integration crate): `pin::TxControlClass { Boundary, Savepoint }`, `pin::tx_control_class(sql: &str) -> Option<TxControlClass>`, and `pin::is_bare_tx_control(sql) -> bool` retained as the boolean façade so every existing caller keeps working.
- Produces, **externally reachable**: `Checkout::tx_open(&self) -> bool` — a plain `pub fn`, deliberately not `#[cfg(test)]`, because `tests/query_guard.rs` is a separate crate and cannot see `#[cfg(test)]` items. It is the only new public surface in this task, and the integration test asserts through it plus `exec`/`query`/`query_stream`, never through the classifier directly.

**Why savepoints are different — the analysis this task turns into code.** The refusal at `pool.rs:531/579/641` exists because a bare transaction-control statement run through the guarded entries would **bypass the pin authority**: it can open a transaction the pool believes is not open, on a connection that then returns to the pool and is handed to the next tenant — a cross-tenant leak (charter rule 6), with no `tx_id`, no actor, no deadline and no rollback-on-session-death. **A savepoint statement does none of that:**

1. **It cannot change transaction status.** The in-tree model already encodes this — `SAVEPOINT`, `RELEASE [SAVEPOINT]` and `ROLLBACK TO [SAVEPOINT]` classify as **PRESERVE** in `leading_tx_verb` (`ferro-pool/src/pin.rs:196-198`) *"since real Postgres's RFQ byte does not flip on any of them"*. The pin AUTHORITY is untouched by construction.
2. **It cannot outlive the transaction.** Both engines destroy every savepoint when the enclosing transaction commits or rolls back, and the transaction is owned by the tx actor, which rolls back on deadline, on session death and on drop. There is no state left for the next tenant.
3. **It is only meaningful inside one.** Which is precisely why the refusal must be *conditional*, not removed.

**What must still be refused:**
- **Every boundary verb that LEADS the statement, on every entry** — `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ABORT`, a bare `ROLLBACK`, `PREPARE TRANSACTION`.
- **A savepoint statement on a NON-transactional checkout.** Not for symmetry — because **MySQL silently ignores a bare `SAVEPOINT` under autocommit**: no transaction is started and the savepoint has no effect, so delegating to the server would hand a driver a rollback point that does not exist. (`ROLLBACK TO n` and `RELEASE SAVEPOINT n` under autocommit are already loud — `ERROR 1305 (42000)` — so the silent case is `SAVEPOINT` alone; the refusal still covers all three, because a rule that is loud for one verb and delegated for two is a rule nobody can reason about, and PG is silent for none of them. Hazard 35 as refined.)

**And what this guard does NOT do — stated here so §22.2 (o) can be true (hazard 64).** The guard inspects the LEADING verb only (`leading_words(sql, 2)`), so a **compound** statement slips past it: measured on MySQL, `SELECT 1; COMMIT` returns `Ok` with `tx_status = Idle`, and `SAVEPOINT s2; START TRANSACTION` returns `Ok` with `tx_status = InTx`. This is **pre-existing** — the guard has always worked this way — and it is **not a leak**: `apply_tx_status` reads the real post-statement status off the protocol signal (the RFQ byte / the OK-packet flags), so the pin engine still knows a transaction is open and still pins or taints the connection accordingly. The connection never returns to the pool believing it is idle when it is not.

What the two-class split changes is that a compound statement can now also lead with a savepoint verb inside a transaction. That is the same exposure, on the same guard, with the same authority behind it — but it is a **new vector at that guard**, so the classifier test below carries explicit compound cases pinning the current behaviour, and §22.2 (o) says "refused when they LEAD the statement" rather than the v1 draft's false "refused unconditionally on every entry".

- [ ] **Step 1: Write the failing classifier unit tests**

In `engine/crates/ferro-pool/src/pin.rs`'s `mod tests`:

```rust
    /// The split that makes savepoint passthrough safe. `ROLLBACK` is in BOTH classes and is the
    /// only verb whose SECOND word decides — bare `ROLLBACK` ends the transaction, `ROLLBACK TO …`
    /// does not (real PG's RFQ byte does not flip on the latter, `leading_tx_verb`'s own rationale).
    #[test]
    fn s8a_tx_control_class_splits_boundary_from_savepoint() {
        use TxControlClass::{Boundary, Savepoint};
        let cases: &[(&str, Option<TxControlClass>)] = &[
            ("BEGIN", Some(Boundary)),
            ("begin;", Some(Boundary)),
            ("START TRANSACTION READ ONLY", Some(Boundary)),
            ("COMMIT", Some(Boundary)),
            ("END", Some(Boundary)),
            ("ABORT", Some(Boundary)),
            ("ROLLBACK", Some(Boundary)),
            ("ROLLBACK;", Some(Boundary)),
            ("ROLLBACK WORK", Some(Boundary)),
            ("PREPARE TRANSACTION 'x'", Some(Boundary)),
            ("SAVEPOINT DOCTRINE_1", Some(Savepoint)),
            ("savepoint doctrine_1", Some(Savepoint)),
            ("RELEASE SAVEPOINT DOCTRINE_1", Some(Savepoint)),
            ("RELEASE DOCTRINE_1", Some(Savepoint)),
            ("ROLLBACK TO SAVEPOINT DOCTRINE_1", Some(Savepoint)),
            ("ROLLBACK TO DOCTRINE_1", Some(Savepoint)),
            // Comment/whitespace tolerance is inherited from `skip_leading_noise` and must survive.
            ("/* x */ ROLLBACK TO SAVEPOINT s", Some(Savepoint)),
            ("-- c\nBEGIN", Some(Boundary)),
            // Not tx control at all.
            ("SELECT 1", None),
            ("INSERT INTO t VALUES (1)", None),
            ("UPDATE savepoints SET x = 1", None),
            // COMPOUND statements: the classifier sees the LEADING verb ONLY, so a boundary verb in
            // a later position is invisible to it. Pinned here as the CURRENT, DELIBERATE behaviour
            // (hazard 64), not as an aspiration — this is what stops §22.2 (o) from claiming a
            // guarantee the guard does not provide. The pin AUTHORITY (`apply_tx_status`) is what
            // keeps a compound statement honest; this guard is not, and never was, a parser.
            ("SELECT 1; COMMIT", None),
            ("SAVEPOINT s2; START TRANSACTION", Some(Savepoint)),
            ("BEGIN; SELECT 1", Some(Boundary)),
        ];
        for (sql, want) in cases {
            assert_eq!(tx_control_class(sql), *want, "tx_control_class({sql:?})");
        }
    }

    /// The boolean façade stays EXACTLY as strict as the pre-M1-S8a `is_bare_tx_control` was.
    ///
    /// **Asserted against the pre-S8a expected values, NOT against `tx_control_class(..).is_some()`**
    /// — the façade is now *defined* as that expression, so comparing the two would be a tautology
    /// (`assert_eq!(f(x), f(x))`) and could not fail for any classifier change. This table is the
    /// pre-change behaviour written out, so dropping a verb from either class goes RED here.
    #[test]
    fn s8a_is_bare_tx_control_is_unchanged_from_pre_s8a() {
        let cases: &[(&str, bool)] = &[
            ("BEGIN", true),
            ("COMMIT", true),
            ("END", true),
            ("ABORT", true),
            ("ROLLBACK", true),
            ("START TRANSACTION", true),
            ("PREPARE TRANSACTION 'x'", true),
            ("SAVEPOINT s", true),
            ("RELEASE s", true),
            ("RELEASE SAVEPOINT s", true),
            ("ROLLBACK TO s", true),
            ("ROLLBACK TO SAVEPOINT s", true),
            ("SELECT 1", false),
            ("UPDATE savepoints SET x = 1", false),
        ];
        for (sql, want) in cases {
            assert_eq!(is_bare_tx_control(sql), *want, "is_bare_tx_control({sql:?})");
        }
    }
```

- [ ] **Step 2: Run and watch it fail**

```
cargo test -p ferro-pool pin::tests::s8a_
```
Expected: FAIL — `TxControlClass` / `tx_control_class` do not exist.

- [ ] **Step 3: Implement the classifier**

`engine/crates/ferro-pool/src/pin.rs` — replace `SINGLE_WORD_TX_CONTROL`/`TWO_WORD_TX_CONTROL` (`:86-100`) and `is_bare_tx_control` (`:155-170`):

```rust
/// What KIND of transaction-control statement a SQL string leads with.
///
/// The split exists because the two classes have different safety properties. A **boundary** verb
/// changes whether a transaction is OPEN — the pin AUTHORITY — so running one through a guarded
/// entry would let a client open a transaction the pool believes is not open, on a connection that
/// then returns to the pool for the next tenant (charter rule 6), with no `tx_id`, no actor, no
/// deadline and no rollback-on-session-death. A **savepoint** verb changes nothing about
/// transaction status — real Postgres's `ReadyForQuery` byte does not flip on any of them, which is
/// exactly what [`leading_tx_verb`] already models as "preserve" — and every savepoint dies with
/// its enclosing transaction, which the tx actor owns. So savepoints may pass through *inside a
/// transaction*, and Doctrine's nested-transaction emulation (`SAVEPOINT DOCTRINE_1` /
/// `RELEASE SAVEPOINT …` / `ROLLBACK TO SAVEPOINT …`, all plain `exec()` SQL) works.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxControlClass {
    /// Opens or closes a transaction block. ALWAYS refused on the guarded entries.
    Boundary,
    /// Manages a savepoint WITHIN a transaction. Allowed iff the checkout already has one open.
    Savepoint,
}

/// Boundary verbs that stand alone.
const BOUNDARY_SINGLE: [&str; 4] = ["BEGIN", "COMMIT", "END", "ABORT"];
/// Boundary verbs spelled as two words.
const BOUNDARY_PAIR: [(&str, &str); 2] = [("START", "TRANSACTION"), ("PREPARE", "TRANSACTION")];
/// Savepoint verbs that stand alone (`SAVEPOINT n`, `RELEASE [SAVEPOINT] n`).
const SAVEPOINT_SINGLE: [&str; 2] = ["SAVEPOINT", "RELEASE"];

/// Classify `sql`'s leading keyword(s), comment/whitespace tolerant (same scan as before).
///
/// `ROLLBACK` is the ONLY verb in both classes and the only one whose SECOND word decides:
/// `ROLLBACK TO [SAVEPOINT] n` is a savepoint operation, everything else spelled `ROLLBACK …`
/// (bare, `;`-terminated, `WORK`, `TRANSACTION`) ends the transaction.
pub(crate) fn tx_control_class(sql: &str) -> Option<TxControlClass> {
    let sql = skip_leading_noise(sql);
    let words = leading_words(sql, 2);
    let first = words.first()?.as_str();
    let second = words.get(1).map(String::as_str);

    if first == "ROLLBACK" {
        return Some(if second == Some("TO") {
            TxControlClass::Savepoint
        } else {
            TxControlClass::Boundary
        });
    }
    if BOUNDARY_SINGLE.contains(&first) {
        return Some(TxControlClass::Boundary);
    }
    if SAVEPOINT_SINGLE.contains(&first) {
        return Some(TxControlClass::Savepoint);
    }
    if let Some(second) = second
        && BOUNDARY_PAIR.iter().any(|(a, b)| *a == first && *b == second)
    {
        return Some(TxControlClass::Boundary);
    }
    None
}

/// True if `sql` leads with ANY transaction-control verb. The boolean façade the pre-M1-S8a guards
/// used, now DERIVED from [`tx_control_class`] so the two cannot drift.
pub(crate) fn is_bare_tx_control(sql: &str) -> bool {
    tx_control_class(sql).is_some()
}
```

- [ ] **Step 4: Route all THREE guarded entries through ONE conditional guard**

In `engine/crates/ferro-pool/src/pool.rs`, add to `impl<B: PoolBackend> Checkout<B>`:

```rust
    /// The tx-control guard shared by [`Checkout::exec`], [`Checkout::query`] and
    /// [`Checkout::query_stream`] — ONE implementation, so a future entry point cannot be added with
    /// a subtly different rule (before M1-S8a the rule was copy-pasted three times).
    ///
    /// Exhaustive over [`pin::TxControlClass`] on purpose: a new class breaks the build here rather
    /// than silently falling into "allowed".
    fn refuse_tx_control(&self, sql: &str, entry: &str) -> Result<(), PoolError> {
        match pin::tx_control_class(sql) {
            None => Ok(()),
            // Inside a transaction a savepoint operation is an ordinary statement: it cannot change
            // transaction status (PG's RFQ byte does not flip on it) and every savepoint dies with
            // the transaction the tx actor owns. This is what lets Doctrine's nested-transaction
            // emulation — `SAVEPOINT DOCTRINE_1`, plain `exec()` SQL — work.
            Some(pin::TxControlClass::Savepoint) if self.tx_open => Ok(()),
            // OUTSIDE one it is refused — and NOT merely for symmetry. MySQL silently IGNORES
            // `SAVEPOINT` under autocommit (no transaction is started, the savepoint has no effect),
            // so delegating to the server would hand the caller a rollback point that does not
            // exist. A loud refusal is the only honest answer.
            Some(pin::TxControlClass::Savepoint) => Err(PoolError::Unsupported(format!(
                "savepoint statement outside a transaction not allowed via {entry}(): {sql:?} \
                 (open a transaction first; MySQL silently ignores a savepoint under autocommit)"
            ))),
            Some(pin::TxControlClass::Boundary) => Err(PoolError::Unsupported(format!(
                "bare transaction-control statement not allowed via {entry}(): {sql:?} \
                 (use the TX service instead)"
            ))),
        }
    }
```

and replace each of the three inline guards:

```rust
    pub async fn exec(&mut self, sql: &str) -> Result<u64, PoolError> {
        self.refuse_tx_control(sql, "exec")?;
        // ... unchanged ...
    }

    pub async fn query(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, PoolError> {
        self.refuse_tx_control(sql, "query")?;
        // ... unchanged ...
    }

    pub async fn query_stream(&mut self, sql: &str, params: &[Value])
        -> Result<RowStreamHandle<'_, B>, PoolError>
    {
        self.refuse_tx_control(sql, "query_stream")?;
        // ... unchanged ...
    }
```

- [ ] **Step 5: Extend the pool-level guard test to cover all three entries × both classes × both tx states**

In `engine/crates/ferro-pool/tests/query_guard.rs`, using the `FakeBackend`:

```rust
/// The guard matrix, driven over ALL THREE guarded entries so a fix applied to one and not the
/// others is RED. Autocommit: everything tx-control-shaped is refused. In a transaction: boundary
/// verbs are still refused, savepoint verbs pass through.
#[tokio::test]
async fn s8a_tx_control_guard_matrix_across_every_guarded_entry() {
    let boundary = ["BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION", "END", "ABORT"];
    let savepoint = ["SAVEPOINT s1", "RELEASE SAVEPOINT s1", "ROLLBACK TO SAVEPOINT s1"];

    // --- autocommit checkout: BOTH classes refused, on every entry.
    for sql in boundary.iter().chain(savepoint.iter()) {
        let pool = fake_pool();
        let mut co = pool.checkout().await.unwrap();
        assert!(matches!(co.exec(sql).await, Err(PoolError::Unsupported(_))), "exec {sql:?}");
        assert!(matches!(co.query(sql, &[]).await, Err(PoolError::Unsupported(_))), "query {sql:?}");
        assert!(
            matches!(co.query_stream(sql, &[]).await.map(|_| ()), Err(PoolError::Unsupported(_))),
            "query_stream {sql:?}"
        );
    }

    // --- in a transaction: boundary still refused, savepoint allowed.
    for sql in boundary {
        let pool = fake_pool();
        let mut co = pool.checkout().await.unwrap();
        co.begin_tx_with(TxId(1), "BEGIN").await.unwrap();
        assert!(
            matches!(co.exec(sql).await, Err(PoolError::Unsupported(_))),
            "a boundary verb stays refused inside a tx: {sql:?}"
        );
    }
    for sql in savepoint {
        let pool = fake_pool();
        let mut co = pool.checkout().await.unwrap();
        co.begin_tx_with(TxId(1), "BEGIN").await.unwrap();
        co.exec(sql).await.unwrap_or_else(|e| panic!("savepoint passthrough inside a tx: {sql:?}: {e}"));
        assert!(co.tx_open(), "a savepoint operation must NOT close the transaction");
    }
}
```

This needs one new accessor on `Checkout`, next to `pin_state()` (`ferro-pool/src/pool.rs:513`). It must be a **plain `pub fn`, not `#[cfg(test)]`**: `query_guard.rs` is an INTEGRATION test (a separate crate), where `#[cfg(test)]` items of `ferro-pool` are not visible.

```rust
    /// Whether this checkout currently has a transaction open, as the pin engine understands it
    /// (the RFQ authority, or the manual set in `begin_tx_with`). Read by the tx-control guard and
    /// asserted by the pool-level guard tests.
    pub fn tx_open(&self) -> bool {
        self.tx_open
    }
```

The `FakeBackend`'s `leading_tx_verb` model already keeps `TxStatus::InTx` across a savepoint (`pin.rs:196-198`), so this assertion is a real check of the model, not a tautology.

- [ ] **Step 6: Write the live gate on BOTH engine families**

Append to `engine/crates/ferrod/tests/tx_it.rs` (PG) and `engine/crates/ferrod/tests/mysql_it.rs` (MySQL + MariaDB).

**No `harness()` exists (hazard 63).** The PG version below uses `tx_it.rs`'s REAL local helpers — `begin(client, rid, pool, isolation, readonly)` (`:77`), `exec_in_tx(client, rid, tx_id, sql, params, fetch, readonly)` (`:128`), `commit(client, rid, tx_id)` (`:161`) — plus `common::{exec_server, req, exec_ok, exec_err, pg_url, assert_session_alive}`. (`tx_it.rs` also has a local `exec_server_with_deadlines` at `:49` for its deadline tests; this test wants the plain `common::exec_server`, so add it to the `use common::{…}` list — `exec_err` too, which the file does not currently import.) The `mysql_it.rs` copy is the same body with `int` → `INT`, wrapped in `for (label, url) in mysql_targets()`, `begin(&mut c, rid, "default")` (that file's two-argument form), and each assertion message prefixed with the engine label so a failure names which engine broke.

```rust
/// Doctrine's nested-transaction emulation, verbatim: it emits `SAVEPOINT DOCTRINE_1` /
/// `ROLLBACK TO SAVEPOINT DOCTRINE_1` / `RELEASE SAVEPOINT DOCTRINE_1` as PLAIN SQL through
/// `exec()`, never a driver API. The read-back is what proves the savepoint actually took — an
/// accepted statement that did nothing would pass a "no error" assertion.
#[tokio::test]
async fn savepoint_sql_passes_through_inside_a_transaction() {
    let Some(url) = pg_url() else { return }; // prints `skip: FERRO_TEST_PG_URL unset`
    let server = exec_server(url);
    let mut c = server.connect().await;
    c.hello(0).await;

    exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_sp")).await;
    exec_ok(&mut c, 2, &ddl("CREATE TABLE s8a_sp (v int)")).await;

    let tx = begin(&mut c, 3, "default", None, false).await;
    for (rid, sql) in [
        (4, "INSERT INTO s8a_sp (v) VALUES (1)"),
        (5, "SAVEPOINT DOCTRINE_1"),
        (6, "INSERT INTO s8a_sp (v) VALUES (2)"),
        (7, "ROLLBACK TO SAVEPOINT DOCTRINE_1"),
        (8, "INSERT INTO s8a_sp (v) VALUES (3)"),
        (9, "RELEASE SAVEPOINT DOCTRINE_1"),
    ] {
        match exec_in_tx(&mut c, rid, tx, sql, Vec::new(), FETCH_NONE, false).await {
            Outcome::Ok(_) => {}
            other => panic!("{sql:?} must pass through inside a transaction, got {other:?}"),
        }
    }
    match commit(&mut c, 10, tx).await {
        Outcome::Ok(_) => {}
        other => panic!("COMMIT: {other:?}"),
    }

    let rows = exec_ok(&mut c, 11, &req("SELECT v FROM s8a_sp ORDER BY v")).await;
    let got: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::I64(n) => *n,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![1, 3], "the savepoint must have rolled 2 back and kept 1 and 3");
}

/// Outside a transaction it stays refused — deliberately, because MySQL would silently ignore a
/// bare `SAVEPOINT` under autocommit (hazard 35 as refined).
#[tokio::test]
async fn savepoint_sql_outside_a_transaction_is_refused() {
    let Some(url) = pg_url() else { return };
    let server = exec_server(url);
    let mut c = server.connect().await;
    c.hello(0).await;

    let e = exec_err(&mut c, 1, &ddl("SAVEPOINT DOCTRINE_1")).await;
    assert_eq!(e.code, errc::UNSUPPORTED);
    assert!(e.message.contains("outside a transaction"), "{}", e.message);

    // ...and a boundary verb is refused whether or not a transaction is open.
    let e2 = exec_err(&mut c, 2, &ddl("COMMIT")).await;
    assert!(e2.message.contains("use the TX service"), "{}", e2.message);

    // The session survives both refusals — exactly one END each (charter rule 4).
    assert_session_alive(&mut c, 0xC0FFEE).await;
}
```

`ddl(sql)` is the same two-line local helper Task 6 introduces (`req(sql)` with `readonly = false`,
`fetch = FETCH_NONE`); `FETCH_NONE` comes from the `pub const`s Task 1 promoted.

- [ ] **Step 7: Run, then MUTATION-PROVE**

```
cargo test -p ferro-pool pin query_guard
FERRO_TEST_PG_URL=… FERRO_TEST_MYSQL_URL=… FERRO_TEST_MARIADB_URL=… \
  cargo test --workspace savepoint_sql -- --nocapture
```
Mutate:

1. Make `tx_control_class` return `Boundary` for `ROLLBACK TO` → the classifier test and the live read-back both go RED.
2. Drop the `if self.tx_open` condition (allow savepoints everywhere) → `savepoint_sql_outside_a_transaction_is_refused` goes RED.
3. Apply `refuse_tx_control` to `exec` only → the guard matrix goes RED for `query`/`query_stream`.
4. Remove `"RELEASE"` from `SAVEPOINT_SINGLE` → `s8a_is_bare_tx_control_is_unchanged_from_pre_s8a` goes RED on the two `RELEASE` rows. **Record that this mutation would have stayed GREEN** under the v1 form of that test (`is_bare_tx_control(sql) == tx_control_class(sql).is_some()`), because both sides change together — the tautology this replaced.
5. Change the compound row `("SELECT 1; COMMIT", None)` in the classifier table to `Some(Boundary)` → RED. This pins the SCOPE claim in §22.2 (o): the entry documents what the guard does NOT do, and the table is the evidence for it. If a later slice decides to parse compound statements, this row is the one that must change first, deliberately.

Restore each.

- [ ] **Step 8: Spec truth + commit**

`ferro-spec-v0.2.md` §7 gains the two-class rule, and §22.2 gains:

```markdown
  **(o) Savepoint SQL passes through INSIDE a transaction; a LEADING boundary verb never does (M1-S8a).** The guarded `Checkout` entries (`exec`/`query`/`query_stream`) used to refuse every transaction-control verb uniformly, which made Doctrine's nested transactions impossible: `Doctrine\DBAL\Connection` implements them by executing the literal strings `SAVEPOINT DOCTRINE_<n>` / `RELEASE SAVEPOINT …` / `ROLLBACK TO SAVEPOINT …` as PLAIN SQL through `exec()`, not through a driver API. The guard is now two-class, and shared by all three entries through one `refuse_tx_control` implementation. **Boundary verbs** (`BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ABORT`, a bare `ROLLBACK`, `PREPARE TRANSACTION`) stay refused on every entry, in or out of a transaction: they change whether a transaction is OPEN — the pin AUTHORITY — so one slipping through would leave a transaction the pool believes is not open on a connection that then goes back to the pool for the next tenant, with no `tx_id`, no actor, no deadline and no rollback-on-session-death. **Savepoint verbs** (`SAVEPOINT n`, `RELEASE [SAVEPOINT] n`, `ROLLBACK TO [SAVEPOINT] n`) are allowed **iff the checkout already has a transaction open**, because they cannot change transaction status (real Postgres's `ReadyForQuery` byte does not flip on any of them — the engine's own `leading_tx_verb` model has classified them "preserve" since M1-S1) and because every savepoint is destroyed by the enclosing transaction's commit or rollback, which the tx actor owns on every exit path. **Outside a transaction they are still refused, and NOT for symmetry:** MySQL silently IGNORES a bare `SAVEPOINT` under autocommit — no transaction is started and the savepoint has no effect — so delegating to the server would hand the caller a rollback point that does not exist. (`ROLLBACK TO`/`RELEASE` under autocommit are already loud, `ERROR 1305`; the rule covers all three because a rule that is loud for one verb and delegated for two is unreasonable-about, and PG is silent for none of them.) `ROLLBACK` is the only verb in both classes; its SECOND word decides. **SCOPE OF THE GUARD, stated precisely because the obvious stronger claim is false:** it classifies the statement's **LEADING** verb only. A COMPOUND statement is not parsed and is not refused — measured on MySQL, where `CLIENT_MULTI_STATEMENTS` is negotiated, `SELECT 1; COMMIT` succeeds and leaves `tx_status = Idle`, and `SAVEPOINT s; START TRANSACTION` succeeds and leaves `InTx`. This is pre-existing behaviour and it is **not a leak**: `apply_tx_status` reads the real post-statement status off the protocol signal after every statement, so the pin engine still sees the open transaction and still pins/taints the connection — the guard is a diagnostic front door, the protocol signal is the authority. **Known interaction, documented not fixed:** the engine's own `SavepointStack` (the `SERVICE_TX` `SAVEPOINT`/`RELEASE`/`ROLLBACK_TO` API, which composes engine-owned `sp_N` names) does not observe passthrough savepoints, so MIXING the two forms inside one transaction can leave that stack disagreeing with the server. The DBAL tier uses passthrough exclusively. Proof: `ferrod`'s `tx_it.rs` / `mysql_it.rs::savepoint_sql_passes_through_inside_a_transaction` (a read-back showing the rolled-back row absent) on PG, MySQL 8.4 and MariaDB 11.8, plus `ferro-pool`'s `pin.rs` classifier table, which pins the compound cases explicitly.
```

```bash
./ci/local-gate.sh --live
git add engine ferro-spec-v0.2.md
git commit -m "feat(m1-s8a): savepoint SQL passes through inside a transaction

DBAL emits SAVEPOINT DOCTRINE_1 / RELEASE / ROLLBACK TO as plain exec() SQL, which
Ferro refused as bare transaction control. The guard is now two-class and shared
by all three guarded entries: boundary verbs stay refused unconditionally (they
change the pin authority), savepoint verbs pass through iff a transaction is open
(they cannot change transaction status and die with the tx). Outside a
transaction they are still refused because MySQL silently ignores them under
autocommit.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 8: Dialect-aware isolation `BEGIN` — the batched `SET TRANSACTION …; START TRANSACTION …` form

**Files:**
- Modify: `engine/crates/ferrod/src/tx/actor.rs:37-60` (`compose_begin_sql`), `:588-619` (the pinned table test)
- Modify: `engine/crates/ferrod/src/services/sql.rs:1231-1237` (the caller inside `begin_on_pool`)
- Create: `engine/crates/ferro-backend-mysql/tests/begin_dialect_it.rs`
- Modify: `engine/crates/ferrod/tests/mysql_it.rs:58-62` — the local `begin` gains `isolation`/`readonly` (it hard-codes them today, so it cannot express this task's cases at all), `tx_req` gains `readonly`/`fetch`, and a `rollback` helper is added. Its two existing `begin` call sites pass `None, false` explicitly. **This is the only harness work in this task** (hazard 63): there is no `harness()` to extend.
- Modify: `ferro-spec-v0.2.md:562` + §22.2
- Test: `engine/crates/ferrod/src/tx/actor.rs` unit; `engine/crates/ferrod/tests/mysql_it.rs`, `engine/crates/ferrod/tests/tx_it.rs`

**Interfaces:**
- Consumes: nothing from Tasks 1–7. (`Pool::backend()` is `pub` at `ferro-pool/src/pool.rs:205` and `PoolBackend::dialect()` is a synchronous constant at `backend.rs:165`, so the dialect is already reachable inside `begin_on_pool`.)
- Produces: `ferrod::tx::actor::compose_begin_sql(dialect: ferro_pool::backend::Dialect, isolation: Option<u8>, readonly: bool) -> Result<String, String>`.

**The measured facts this task is built on** (MySQL 8.4.11 + MariaDB 11.8.8, both engines identical):

| statement | result |
|---|---|
| `BEGIN` | OK |
| `BEGIN READ ONLY` | **ERROR 1064 (42000)** |
| `BEGIN ISOLATION LEVEL SERIALIZABLE` | **ERROR 1064** |
| `START TRANSACTION ISOLATION LEVEL SERIALIZABLE` | **ERROR 1064** |
| `START TRANSACTION READ ONLY` | OK |
| a STANDALONE `SET TRANSACTION …` | OK, but **TAINTS** (`state_changed=true`, no trackers ⇒ `is_mutation()`) |
| `SET TRANSACTION …; START TRANSACTION …` in ONE `query_drop` | OK, `in_trans=true`, tracker `TransactionState("T_______")`, **no taint** |

So today **every** MySQL `BEGIN` with `isolation != None` **or** `readonly == true` fails at BEGIN with a syntax error, and the naive two-statement fix would taint every such connection into a full `COM_RESET_CONNECTION` at the next recycle.

- [ ] **Step 1: Extend the pinned table test (it is the spec of this function)**

Replace `compose_begin_sql_table` at `engine/crates/ferrod/src/tx/actor.rs:588-619`:

```rust
    /// The full (dialect × isolation × readonly) matrix, verbatim. The 8 PostgreSQL strings are
    /// UNCHANGED from M1-S6 — this task must not move them.
    #[test]
    fn compose_begin_sql_table() {
        use ferro_pool::backend::Dialect;
        let iso_rc = Some(Isolation::ReadCommitted as u8);
        let iso_rr = Some(Isolation::RepeatableRead as u8);
        let iso_ser = Some(Isolation::Serializable as u8);

        // --- PostgreSQL: unchanged.
        let pg: &[(Option<u8>, bool, &str)] = &[
            (None, false, "BEGIN"),
            (None, true, "BEGIN READ ONLY"),
            (iso_rc, false, "BEGIN ISOLATION LEVEL READ COMMITTED"),
            (iso_rc, true, "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY"),
            (iso_rr, false, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
            (iso_rr, true, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"),
            (iso_ser, false, "BEGIN ISOLATION LEVEL SERIALIZABLE"),
            (iso_ser, true, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY"),
        ];
        for (iso, ro, want) in pg {
            assert_eq!(
                compose_begin_sql(Dialect::Postgres, *iso, *ro).unwrap(),
                *want,
                "pg({iso:?}, {ro})"
            );
        }

        // --- MySQL/MariaDB: `BEGIN READ ONLY` and `BEGIN ISOLATION LEVEL …` are ERROR 1064 on BOTH
        // engines (measured), and so is `START TRANSACTION ISOLATION LEVEL …`. The isolation forms
        // are therefore a `SET TRANSACTION …;` prefix in the SAME statement string — ONE
        // `simple_query`, which is all `begin_tx_with` issues, and the only form that does not taint.
        let my: &[(Option<u8>, bool, &str)] = &[
            (None, false, "START TRANSACTION"),
            (None, true, "START TRANSACTION READ ONLY"),
            (iso_rc, false, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION"),
            (iso_rc, true, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION READ ONLY"),
            (iso_rr, false, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION"),
            (iso_rr, true, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION READ ONLY"),
            (iso_ser, false, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION"),
            (iso_ser, true, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION READ ONLY"),
        ];
        for (iso, ro, want) in my {
            assert_eq!(
                compose_begin_sql(Dialect::MySql, *iso, *ro).unwrap(),
                *want,
                "mysql({iso:?}, {ro})"
            );
        }

        // --- SQLite: no backend exists yet. A bare BEGIN is composable; anything else is a LOUD
        // refusal rather than a silently-PG-shaped string a future backend would choke on.
        assert_eq!(compose_begin_sql(Dialect::Sqlite, None, false).unwrap(), "BEGIN");
        assert!(compose_begin_sql(Dialect::Sqlite, None, true).is_err());
        assert!(compose_begin_sql(Dialect::Sqlite, iso_ser, false).is_err());

        // An unknown isolation byte is a client error on EVERY dialect, never coerced to a default.
        for d in [Dialect::Postgres, Dialect::MySql, Dialect::Sqlite] {
            assert!(compose_begin_sql(d, Some(99), false).is_err(), "{d:?}");
        }
    }
```

- [ ] **Step 2: Run and watch it fail**

```
cargo test -p ferrod compose_begin_sql_table
```
Expected: FAIL to compile — `compose_begin_sql` takes two arguments.

- [ ] **Step 3: Implement the dialect-aware composer**

`engine/crates/ferrod/src/tx/actor.rs:45-60`:

```rust
/// Compose the engine's transaction-opening statement for `dialect` from the request's `isolation`
/// (a `u8` off the wire) and `readonly` flag. Pure, and unit-tested per (dialect × isolation ×
/// readonly) cell. An unknown isolation byte is a client error (`Err`, mapped by the handler to
/// `Protocol`), never coerced to a default.
///
/// **The result is ONE statement string, always.** `Checkout::begin_tx_with` issues exactly one
/// `simple_query` and wraps it in the whole pin/RFQ/tracker/Rule-A sequence, so a second call would
/// re-run that sequence — the MySQL isolation forms are therefore a `CLIENT_MULTI_STATEMENTS` batch
/// (already negotiated by the vendored fork), not two calls.
///
/// **Why MySQL cannot use the PG spelling** (measured on MySQL 8.4.11 AND MariaDB 11.8.8):
/// `BEGIN READ ONLY`, `BEGIN ISOLATION LEVEL …` and `START TRANSACTION ISOLATION LEVEL …` are all
/// `ERROR 1064 (42000)`. The only working forms are `START TRANSACTION [READ ONLY]` and a
/// `SET TRANSACTION …;` prefix, whose modifier applies to the NEXT transaction only — so it must
/// immediately precede the `START TRANSACTION`.
///
/// **Why the BATCH and not two statements** (also measured): a STANDALONE `SET TRANSACTION …`
/// returns an OK packet with `SERVER_SESSION_STATE_CHANGED` and NO trackers, which
/// `tracker::is_mutation` reads as a real session mutation — so it would taint EVERY
/// isolation/readonly transaction into a full `COM_RESET_CONNECTION` at the next recycle. Batched,
/// `query_drop` drains both result sets and the FINAL OK packet carries a `TransactionState`
/// tracker, which gates the bare-flag path off: no taint, `tx_status` reads `InTx`, and
/// `SERVER_STATUS_IN_TRANS_READONLY` confirms the read-only mode took.
///
/// The batch consequently MASKS the intermediate statement's own trackers (only the final OK packet
/// is read). That is acceptable here and only here: the engine composes this string itself. A USER
/// batch still goes through `Checkout::exec`, which is unchanged.
pub fn compose_begin_sql(
    dialect: Dialect,
    isolation: Option<u8>,
    readonly: bool,
) -> Result<String, String> {
    let level = match isolation {
        None => None,
        Some(iso) => Some(match Isolation::try_from(iso).map_err(|e| e.to_string())? {
            Isolation::ReadCommitted => "READ COMMITTED",
            Isolation::RepeatableRead => "REPEATABLE READ",
            Isolation::Serializable => "SERIALIZABLE",
        }),
    };

    // Exhaustive on purpose (hazard 42): a new dialect must break the build here rather than
    // silently inherit PG syntax.
    match dialect {
        Dialect::Postgres => {
            let mut sql = String::from("BEGIN");
            if let Some(level) = level {
                sql.push_str(" ISOLATION LEVEL ");
                sql.push_str(level);
            }
            if readonly {
                sql.push_str(" READ ONLY");
            }
            Ok(sql)
        }
        Dialect::MySql => {
            let start = if readonly {
                "START TRANSACTION READ ONLY"
            } else {
                "START TRANSACTION"
            };
            Ok(match level {
                None => start.to_string(),
                Some(level) => format!("SET TRANSACTION ISOLATION LEVEL {level}; {start}"),
            })
        }
        Dialect::Sqlite => {
            if level.is_some() || readonly {
                Err(
                    "isolation/readonly BEGIN is not supported on the sqlite dialect (no SQLite \
                     backend exists yet; this arm exists so one cannot silently inherit PG syntax)"
                        .to_string(),
                )
            } else {
                Ok("BEGIN".to_string())
            }
        }
    }
}
```

- [ ] **Step 4: Pass the dialect at the single call site**

`engine/crates/ferrod/src/services/sql.rs:1231-1237`, inside `begin_on_pool<B: PoolBackend>`:

```rust
    let begin_sql = match actor::compose_begin_sql(
        pool.backend().dialect(),
        req.isolation,
        req.readonly,
    ) {
        Ok(s) => s,
        Err(msg) => {
            responder.end_error(protocol(msg));
            return;
        }
    };
```

- [ ] **Step 5: Write the taint proof — the guard that can actually fail**

`engine/crates/ferro-backend-mysql/tests/begin_dialect_it.rs`:

```rust
//! The measured standalone-vs-batched `SET TRANSACTION` difference, pinned against BOTH live
//! engines. This is the load-bearing reason `compose_begin_sql` emits a batch: a standalone
//! `SET TRANSACTION` sets `SERVER_SESSION_STATE_CHANGED` with NO trackers, which the §7.1 rule reads
//! as a real session mutation — so every isolation/readonly transaction would taint into a full
//! `COM_RESET_CONNECTION` at the next recycle.

use ferro_pool::backend::{PoolBackend, TxStatus};
use ferro_backend_mysql::MysqlBackend;

/// `take_session_mutated` is the ASSIST taint signal, read-and-cleared per statement. Asserting on
/// it directly (rather than on a `Checkout`'s pin cause, which the RFQ authority would mask) is what
/// makes this test falsifiable.
///
/// Prints `skip: <VAR> unset` per unconfigured engine — which is what the live lane's
/// `ci/assert-no-skips.sh` looks for, so a live run with a backend missing fails rather than
/// reporting a green no-op. No `ran` bookkeeping: the skip line IS the signal, and an unused local
/// would only draw a clippy warning.
async fn each_engine<F, Fut>(f: F)
where
    F: Fn(String, &'static str) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    for (var, label) in [("FERRO_TEST_MYSQL_URL", "mysql"), ("FERRO_TEST_MARIADB_URL", "mariadb")] {
        match std::env::var(var) {
            Ok(url) => f(url, label).await,
            Err(_) => println!("skip: {var} unset"),
        }
    }
}

#[tokio::test]
async fn a_standalone_set_transaction_taints_but_the_batched_form_does_not() {
    each_engine(|url, label| async move {
        let backend = MysqlBackend::new(url);
        let mut conn = backend.connect().await.expect("connect");

        // Baseline: a plain SELECT taints nothing.
        backend.simple_query(&mut conn, "SELECT 1").await.unwrap();
        assert!(!backend.take_session_mutated(&mut conn), "[{label}] baseline must be clean");

        // (a) STANDALONE — the form a naive two-statement implementation would emit.
        backend
            .simple_query(&mut conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .await
            .unwrap();
        assert!(
            backend.take_session_mutated(&mut conn),
            "[{label}] a standalone SET TRANSACTION MUST taint — this is why the batch exists"
        );
        backend.simple_query(&mut conn, "ROLLBACK").await.ok();

        // (b) BATCHED — what `compose_begin_sql(Dialect::MySql, Some(Serializable), true)` emits.
        backend
            .simple_query(
                &mut conn,
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION READ ONLY",
            )
            .await
            .unwrap();
        assert!(
            !backend.take_session_mutated(&mut conn),
            "[{label}] the batched form must NOT taint (the final OK packet carries a \
             TransactionState tracker, which gates the bare-flag path off)"
        );
        assert_eq!(
            backend.tx_status(&conn),
            TxStatus::InTx,
            "[{label}] and it must actually open the transaction"
        );

        // ...and READ ONLY genuinely took.
        let e = backend
            .simple_query(&mut conn, "CREATE TEMPORARY TABLE s8a_ro (i INT)")
            .await
            .err();
        // MySQL/MariaDB report 1792 / SQLSTATE 25006 for a write in a read-only transaction.
        assert!(e.is_some(), "[{label}] a write inside a READ ONLY transaction must fail");
        backend.simple_query(&mut conn, "ROLLBACK").await.ok();
    })
    .await;
}
```

- [ ] **Step 6: Grow `mysql_it.rs`'s `begin` helper — the ONE piece of harness this task needs**

`mysql_it.rs`'s local `begin` (`:58-62`) hard-codes `isolation: None, readonly: false`, which is precisely the shape this task exists to change, so it cannot express the new cases. Widen it to the signature `tx_it.rs` already uses (`tx_it.rs:77-83`), and update the file's two existing call sites to pass `None, false` explicitly:

```rust
/// `service=TX, method=BEGIN` — assert the one-END terminal shape and decode the `BeginResponse`.
///
/// `isolation`/`readonly` were hard-coded to `None`/`false` before M1-S8a, because the PG-flavoured
/// `BEGIN READ ONLY` / `BEGIN ISOLATION LEVEL …` forms are ERROR 1064 on MySQL and MariaDB and there
/// was nothing else to send. `compose_begin_sql` is dialect-aware now, so they are real parameters —
/// the same signature `tx_it.rs::begin` has always had.
async fn begin(
    client: &mut TestClient,
    rid: u32,
    pool: &str,
    isolation: Option<u8>,
    readonly: bool,
) -> u64 {
    // ... body unchanged except that `BeginRequest` takes `isolation` and `readonly` ...
}

/// A tx-scoped `ExecRequest`, with the fetch mode and readonly flag the caller needs.
fn tx_req(tx_id: u64, sql: &str, readonly: bool, fetch: u8) -> ExecRequest { /* ... */ }
```

Also add a `rollback(client, rid, tx_id)` alongside the existing `commit` (the same `TxControl` body with `method_tx::ROLLBACK`), which this file does not yet have. Nothing else in `mysql_it.rs` changes.

- [ ] **Step 7: Write the end-to-end live gate through `ferrod`**

Append to `engine/crates/ferrod/tests/mysql_it.rs`. **Two things must be proven and they need different assertions** — read the following before writing any of it.

**READ ONLY** is directly observable: a write inside the transaction is refused with SQLSTATE `25006` (errno 1792) on both engines. That is the v1 assertion and it is correct; keep it.

**ISOLATION is NOT observable through `@@transaction_isolation`, and the v1 assertion was false on two counts (probe 1, blocker B3).** `SET TRANSACTION ISOLATION LEVEL …` without `SESSION`/`GLOBAL` applies to the **next transaction only** and does not change the session variable; and MySQL renders the value with a hyphen (`REPEATABLE-READ`), so even the string was wrong. **Do not "fix" it by emitting `SET SESSION TRANSACTION ISOLATION LEVEL …`** — that persists the level on the pooled connection past `COMMIT`, so the next tenant inherits it: a cross-tenant connection-state leak, charter rule 6, the one class this whole slice must never open. The batched next-transaction-only form is correct *because* it does not persist.

The isolation level therefore has to be proven **behaviourally, by a lock conflict**. Under `SERIALIZABLE` InnoDB implicitly converts a plain `SELECT` inside a transaction into a locking read, so a concurrent `UPDATE` of the row blocks; under the default `REPEATABLE READ` the same `SELECT` is a non-locking consistent read and the `UPDATE` goes straight through. Running the same scenario twice — once with `Isolation::Serializable`, once with `isolation: None` — makes the two outcomes the discriminator, and the control run is what stops the test passing for an unrelated reason (a lock left over from setup, say).

The concurrent `UPDATE` is bounded with the request's own `timeout_ms` (the S4 per-request CANCEL path), so a blocked write terminates as a `57014` cancel instead of hanging the suite on InnoDB's 50-second `innodb_lock_wait_timeout`.

```rust
/// Before M1-S8a, `BEGIN ISOLATION LEVEL …` / `BEGIN READ ONLY` were ERROR 1064 on both engines, so
/// EVERY isolation/readonly BEGIN failed. Nothing pinned that (every MySQL tx test used
/// `isolation: None`), so this is a pure addition.
///
/// READ ONLY is asserted directly (SQLSTATE 25006 on a write). ISOLATION cannot be: a
/// next-transaction-only `SET TRANSACTION` is deliberately NOT reflected in `@@transaction_isolation`
/// — and the SESSION form that WOULD be reflected is forbidden here, because it persists onto the
/// pooled connection for the next tenant. So isolation is proven by a LOCK CONFLICT, with a
/// `isolation: None` control run that must NOT conflict.
#[tokio::test]
async fn mysql_begin_honours_isolation_and_readonly() {
    for (label, url) in mysql_targets() {
        let server = exec_server(url);
        let mut c = server.connect().await;
        c.hello(0).await;

        // ---- (a) READ ONLY is enforced.
        exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_ro")).await;
        exec_ok(&mut c, 2, &ddl("CREATE TABLE s8a_ro (id INT PRIMARY KEY, v INT)")).await;
        exec_ok(&mut c, 3, &ddl("INSERT INTO s8a_ro VALUES (1, 1)")).await;

        let tx = begin(&mut c, 4, "default", Some(Isolation::Serializable as u8), true).await;
        let e = match exec_in_tx(&mut c, 5, tx, "INSERT INTO s8a_ro VALUES (2, 2)", Vec::new(), FETCH_NONE, false).await {
            Outcome::Error(ep) => ep,
            other => panic!("[{label}] a write in a READ ONLY tx must be refused, got {other:?}"),
        };
        assert_eq!(
            e.sqlstate.as_deref(),
            Some("25006"),
            "[{label}] READ ONLY must be enforced (errno 1792 / SQLSTATE 25006)"
        );
        rollback(&mut c, 6, tx).await;

        // ---- (b) SERIALIZABLE is enforced: a read inside the tx LOCKS the row.
        let tx = begin(&mut c, 7, "default", Some(Isolation::Serializable as u8), false).await;
        match exec_in_tx(&mut c, 8, tx, "SELECT v FROM s8a_ro WHERE id = 1", Vec::new(), FETCH_ROWS, true).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] the in-tx read must succeed: {other:?}"),
        }

        // A SECOND session, so the UPDATE genuinely contends rather than sharing the pinned conn.
        let mut other = server.connect().await;
        other.hello(0).await;
        let mut upd = req("UPDATE s8a_ro SET v = 99 WHERE id = 1");
        upd.readonly = false;
        upd.fetch = FETCH_NONE;
        upd.timeout_ms = Some(1_500); // bounded, so a blocked write ends as 57014, never hangs
        let blocked = exec_err(&mut other, 9, &upd).await;
        assert_eq!(
            blocked.sqlstate.as_deref(),
            Some("57014"),
            "[{label}] under SERIALIZABLE the in-tx read must LOCK the row, so a concurrent UPDATE \
             blocks and the request deadline cancels it — got {blocked:?}"
        );
        rollback(&mut c, 10, tx).await;

        // ---- (c) THE CONTROL. Same scenario with isolation: None (REPEATABLE READ) — the read is a
        // non-locking consistent read, so the UPDATE goes straight through. Without this run, (b)
        // would pass for any reason the UPDATE happened to be slow.
        let tx = begin(&mut c, 11, "default", None, false).await;
        match exec_in_tx(&mut c, 12, tx, "SELECT v FROM s8a_ro WHERE id = 1", Vec::new(), FETCH_ROWS, true).await {
            Outcome::Ok(_) => {}
            other => panic!("[{label}] the in-tx read must succeed: {other:?}"),
        }
        let mut upd2 = req("UPDATE s8a_ro SET v = 42 WHERE id = 1");
        upd2.readonly = false;
        upd2.fetch = FETCH_NONE;
        upd2.timeout_ms = Some(1_500);
        match exec(&mut other, 13, &upd2).await {
            Outcome::Ok(_) => {}
            other_out => panic!(
                "[{label}] under the DEFAULT isolation the concurrent UPDATE must NOT block — if \
                 this fails, (b) proves nothing: {other_out:?}"
            ),
        }
        rollback(&mut c, 14, tx).await;
    }
}
```

and to `engine/crates/ferrod/tests/tx_it.rs`, so the dialect split is proven NOT to have moved PG. PG **does** reflect the level in `transaction_isolation` (`BEGIN ISOLATION LEVEL …` sets it for the current transaction, unlike MySQL's next-transaction-only prefix), so the direct assertion is correct there and is kept:

```rust
/// PG is untouched by the dialect split — the composed strings are byte-identical to M1-S6's. This
/// mirrors the existing live isolation assertion at `tx_it.rs:402-440`, but with `readonly` on, so
/// the `BEGIN ISOLATION LEVEL … READ ONLY` cell is exercised end to end and not only in the table
/// test.
///
/// The direct `current_setting('transaction_isolation')` read is valid HERE and not on MySQL:
/// PG's `BEGIN ISOLATION LEVEL …` sets the CURRENT transaction's level and reports it, while
/// MySQL's `SET TRANSACTION …` prefix applies to the NEXT transaction and is invisible in
/// `@@transaction_isolation` (see `mysql_it.rs`'s lock-conflict proof).
#[tokio::test]
async fn pg_begin_isolation_and_readonly_are_unchanged() {
    let Some(url) = pg_url() else { return };
    let server = exec_server(url);
    let mut c = server.connect().await;
    c.hello(0).await;

    exec_ok(&mut c, 1, &ddl("DROP TABLE IF EXISTS s8a_pg_ro")).await;
    exec_ok(&mut c, 2, &ddl("CREATE TABLE s8a_pg_ro (v int)")).await;

    let tx = begin(&mut c, 3, "default", Some(Isolation::Serializable as u8), true).await;

    let iso = exec_ok(
        &mut c,
        4,
        &tx_req(tx, "SELECT current_setting('transaction_isolation')", Vec::new(), FETCH_ROWS, true),
    )
    .await;
    assert_eq!(iso.rows[0][0], Value::Text("serializable".to_string()));

    let e = match exec_in_tx(&mut c, 5, tx, "INSERT INTO s8a_pg_ro (v) VALUES (1)", Vec::new(), FETCH_NONE, false).await {
        Outcome::Error(ep) => ep,
        other => panic!("a write in a READ ONLY tx must be refused, got {other:?}"),
    };
    assert_eq!(
        e.sqlstate.as_deref(),
        Some("25006"),
        "PG enforces READ ONLY with the same SQLSTATE"
    );

    rollback(&mut c, 6, tx).await;
}
```

- [ ] **Step 8: Run, then MUTATION-PROVE**

```
cargo test -p ferrod compose_begin_sql_table
FERRO_TEST_MYSQL_URL=… FERRO_TEST_MARIADB_URL=… \
  cargo test -p ferro-backend-mysql --test begin_dialect_it -- --nocapture
FERRO_TEST_PG_URL=… FERRO_TEST_MYSQL_URL=… FERRO_TEST_MARIADB_URL=… \
  cargo test -p ferrod begin -- --nocapture
```
Mutate:

1. Emit the STANDALONE prefix only (`"SET TRANSACTION ISOLATION LEVEL …"`, no `START TRANSACTION`) → the `ferrod` live test goes RED (no transaction opens) **and** `begin_dialect_it`'s (b) branch goes RED (the standalone form taints). Restore. (The "two `simple_query` calls" variant is not mutable without changing `begin_tx_with` — which is exactly why the composer returns ONE string.)
2. Make the MySQL arm emit the PG string → `compose_begin_sql_table` **and** the live test go RED with `ERROR 1064`.
3. Replace the `Dialect` match's arms with a `_ =>` PG fallback → the SQLite assertions go RED.
4. **Prove the lock-conflict assertion is really about isolation** (the guard replacing the false `@@transaction_isolation` check): make the MySQL isolation arm emit `START TRANSACTION` with **no** `SET TRANSACTION` prefix — i.e. silently ignore the requested level, which is exactly what a plausible "simplification" would do → block (b) goes RED (the concurrent UPDATE now succeeds, so the expected `57014` never arrives) while block (a) READ ONLY and the control (c) both stay green. This is the mutation the v1 assertion could not have caught: `@@transaction_isolation` reads `REPEATABLE-READ` either way.
5. **Prove the control run is load-bearing:** delete block (c) and re-apply mutation 4 → (b) still goes RED, so (c) looks redundant; now instead make the UPDATE contend for an unrelated reason (drop `timeout_ms` to `Some(1)`) → (b) passes for the WRONG reason and only (c) catches it. Record it, restore both.

Restore each.

- [ ] **Step 9: Spec truth + commit**

Amend `ferro-spec-v0.2.md:562` (which currently records the MySQL isolation-BEGIN failure as an S8 carry) and drop the standing comments at `ferrod/tests/mysql_it.rs:52-55`, `mysql_chaos_it.rs:359` and `:649` that say isolation-BEGIN is deferred. Add to §22.2:

```markdown
  **(p) MySQL/MariaDB isolation/readonly `BEGIN` is a SINGLE-STATEMENT BATCH, and that is load-bearing (M1-S8a).** `compose_begin_sql` is now dialect-aware. On MySQL 8.4 and MariaDB 11.8 `BEGIN READ ONLY`, `BEGIN ISOLATION LEVEL …` and `START TRANSACTION ISOLATION LEVEL …` are ALL `ERROR 1064 (42000)` (measured), so the composed form is `START TRANSACTION [READ ONLY]`, prefixed for the isolation cases with `SET TRANSACTION ISOLATION LEVEL …;` **in the same statement string**. Two reasons it is one string and not two calls: `Checkout::begin_tx_with` issues exactly ONE `simple_query` and wraps it in the whole pin/RFQ/tracker/Rule-A sequence (a second call would re-run all of it); and — the measured one — a **STANDALONE** `SET TRANSACTION …` returns an OK packet with `SERVER_SESSION_STATE_CHANGED` and **no trackers**, which `tracker::is_mutation` reads as a genuine session mutation, so every isolation/readonly transaction would taint into a full `COM_RESET_CONNECTION` at the next recycle. **BATCHED** (over the already-negotiated `CLIENT_MULTI_STATEMENTS`), `query_drop` drains both result sets and the FINAL OK packet carries a `TransactionState` tracker, which gates the bare-flag path off: no taint, `tx_status` reads `InTx`, and `SERVER_STATUS_IN_TRANS_READONLY` confirms read-only took. The batch consequently MASKS the intermediate statement's own trackers — acceptable **only** because the engine composes this string itself; a user batch still goes through the unchanged `Checkout::exec`. It is also one of the three concrete holes any future tracker-clean hygiene skip (§7.2, R2) must reconcile. **The `SESSION` form is FORBIDDEN, and the prohibition is load-bearing:** `SET SESSION TRANSACTION ISOLATION LEVEL …` would persist the level on the pooled connection past `COMMIT`, so the next tenant would inherit it — a cross-tenant connection-state leak (charter rule 6). The next-transaction-only spelling is correct *because* it does not persist, and the observable consequence is that **the level is NOT visible in `@@transaction_isolation`** (which keeps reporting the session default, rendered by MySQL with a hyphen: `REPEATABLE-READ`). Anyone verifying this behaviour must therefore prove it by a **lock conflict** — under `SERIALIZABLE` InnoDB converts a plain in-transaction `SELECT` into a locking read, so a concurrent `UPDATE` of that row blocks, while under the default `REPEATABLE READ` it does not — never by reading the variable back. That trap is real: the naive variable assertion is false, and its natural "repair" is the leak. The PostgreSQL strings are byte-identical to M1-S6's (all 8 still pinned verbatim), and `Dialect::Sqlite` is an explicit LOUD refusal rather than a silent PG fallback. **PHP-reachable NAMED isolation constants are explicitly NOT in S8a**: they would need a new `[isolation]` section in the `/proto` registry (touching `registry.rs`'s `MethodsToml` and `Registry` **and** `build.rs`'s own `deny_unknown_fields` `Registry`), and Doctrine does not need them — `Connection::setTransactionIsolation()` emits `SET SESSION TRANSACTION ISOLATION LEVEL …` as plain SQL, not a driver flag. Proof: `ferro-backend-mysql`'s `begin_dialect_it.rs` (the standalone-taints / batched-does-not pair) and `ferrod`'s `mysql_it.rs::mysql_begin_honours_isolation_and_readonly`, on both engines.
```

```bash
./ci/local-gate.sh --live
git add engine ferro-spec-v0.2.md
git commit -m "feat(m1-s8a): dialect-aware isolation BEGIN — the batched SET TRANSACTION form

compose_begin_sql was dialect-blind PG syntax, so every MySQL BEGIN with an
isolation level or READ ONLY was ERROR 1064 on both MySQL 8.4 and MariaDB 11.8.
It now composes ONE statement per dialect; the MySQL isolation forms are a
CLIENT_MULTI_STATEMENTS batch because a STANDALONE SET TRANSACTION taints the
connection (measured) while the batched form does not — the final OK packet
carries a TransactionState tracker, which gates the bare-flag taint path off.

The 8 PostgreSQL strings are byte-identical; Dialect::Sqlite is a loud refusal.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 9: Imperative `begin` / `commit` / `rollBack` on the PHP client

**Files:**
- Modify: `php/client/src/Client/Connection.php` (new state + trio + statement delegation), `:236-305` (`stream`), `:326-437` (`transaction`, guard only)
- Modify: `php/client/src/Client/TxHandle.php` (`run` exposed as `runForConnection`; a `stream()`-parity note; `lastInsertId()` from Task 2)
- Create: `php/client/src/Client/Error/InvalidTransactionStateException.php` — the LEAF class every misuse guard in this task asserts on (hazard 68: `FerroException` is the ROOT and passes for anything)
- Create: `php/client/tests/Live/ImperativeTransactionLiveTest.php`, `php/client/tests/Client/ConnectionImperativeTxTest.php`
- Modify: `php/client/tests/Support/FakeSession.php` (the queueing helpers the offline tests need)
- Modify: `php/client/tests/Client/ConnectionTransactionTest.php` (the existing closure-form unit tests must stay green)

**Interfaces:**
- Consumes: `TxHandle` (unchanged public surface), `Connection::lastInsertId()` (Task 2), the savepoint passthrough (Task 7 — the imperative form is how DBAL reaches it).
- Produces: `Connection::begin(bool $readonly = false): void`, `Connection::commit(): void`, `Connection::rollBack(): void`, `Connection::inTransaction(): bool`.

**Design, and the three things it must not break.**

1. **DBAL owns retry (charter rule 3).** `Connection::transaction()`'s §19.1 loop must NOT be reused: DBAL's `Connection::beginTransaction()` / `commit()` / `rollBack()` are three unrelated calls with the caller's code in between, and DBAL's own `transactional()` is built on top of them. The driver will run with `RetryPolicy::none()`.
2. **A mid-transaction reconnect would silently void the `tx_id`.** `Connection::sendExec`'s autocommit path transparently reconnects and re-issues a Retryable read; a tx-scoped statement must not. `TxHandle::run` (`TxHandle.php:155-170`) is already the bare send-and-classify with no reconnect — so the imperative trio holds a `TxHandle` and delegates to it verbatim rather than growing a second path.
3. **The closure form and its §19.1 re-run policy stay exactly as they are.** `transaction()` gains one guard (refuse while an imperative transaction is open) and nothing else.

**Isolation is deliberately absent** from `begin()` — see hazard 46 and Task 8's §22.2 (p) entry. Doctrine emits `SET SESSION TRANSACTION ISOLATION LEVEL …` as plain SQL.

- [ ] **Step 1: Write the failing live test**

`php/client/tests/Live/ImperativeTransactionLiveTest.php`:

```php
<?php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Error\InvalidTransactionStateException;
use Ferro\Client\RetryPolicy;

final class ImperativeTransactionLiveTest extends LiveTestCase
{
    public function testCommitPersistsAndRollbackDiscards(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8a_imp');
        $c->exec('CREATE TABLE s8a_imp (v int)');

        $c->begin();
        $this->assertTrue($c->inTransaction());
        $c->exec('INSERT INTO s8a_imp (v) VALUES (1)');
        $c->commit();
        $this->assertFalse($c->inTransaction());

        $c->begin();
        $c->exec('INSERT INTO s8a_imp (v) VALUES (2)');
        $c->rollBack();

        $this->assertSame([['v' => 1]], $c->query('SELECT v FROM s8a_imp ORDER BY v'));
    }

    /**
     * THE property that makes the trio correct: while a transaction is open, every statement issued
     * through the SAME Connection must carry its tx_id. If it silently ran autocommit, the insert
     * below would survive the rollback — which is exactly the failure this asserts against.
     */
    public function testStatementsInsideAnImperativeTransactionAreScopedToIt(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8a_imp_scope');
        $c->exec('CREATE TABLE s8a_imp_scope (v int)');

        $c->begin();
        $c->exec('INSERT INTO s8a_imp_scope (v) VALUES (7)');
        // Visible INSIDE the transaction...
        $this->assertSame([['v' => 7]], $c->query('SELECT v FROM s8a_imp_scope'));
        $c->rollBack();
        // ...and gone after it.
        $this->assertSame([], $c->query('SELECT v FROM s8a_imp_scope'));
    }

    /** Doctrine's nested-transaction emulation, reached exactly the way DBAL reaches it. */
    public function testDoctrineStyleSavepointSqlWorksThroughTheImperativeApi(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8a_imp_sp');
        $c->exec('CREATE TABLE s8a_imp_sp (v int)');

        $c->begin();
        $c->exec('INSERT INTO s8a_imp_sp (v) VALUES (1)');
        $c->exec('SAVEPOINT DOCTRINE_1');
        $c->exec('INSERT INTO s8a_imp_sp (v) VALUES (2)');
        $c->exec('ROLLBACK TO SAVEPOINT DOCTRINE_1');
        $c->exec('RELEASE SAVEPOINT DOCTRINE_1');
        $c->commit();

        $this->assertSame([['v' => 1]], $c->query('SELECT v FROM s8a_imp_sp ORDER BY v'));
    }

    public function testMisuseIsLoud(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        // The LEAF class, not FerroException: the root passes for any Ferro error, including one
        // thrown by this test's own setup (hazard 68).
        $this->expectException(InvalidTransactionStateException::class);
        $c->commit(); // no open transaction
    }

    public function testTheClosureFormStillWorksAndRefusesToNest(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8a_imp_mix');
        $c->exec('CREATE TABLE s8a_imp_mix (v int)');

        $c->transaction(static function ($tx): void {
            $tx->exec('INSERT INTO s8a_imp_mix (v) VALUES (1)');
        });
        $this->assertSame([['v' => 1]], $c->query('SELECT v FROM s8a_imp_mix'));

        $c->begin();
        try {
            $c->transaction(static fn ($tx) => null);
            $this->fail('the closure form must refuse to nest inside an imperative transaction');
        } catch (InvalidTransactionStateException) {
            // expected — the leaf class, so this cannot pass because the closure form failed for
            // some unrelated reason.
        } finally {
            $c->rollBack();
        }
    }
}
```

- [ ] **Step 2: Run and watch it fail**

```
(cd php/client && ./vendor/bin/phpunit tests/Live/ImperativeTransactionLiveTest.php)
```
Expected: FAIL — `Call to undefined method Ferro\Client\Connection::begin()`.

- [ ] **Step 3: Add the state and the trio**

`php/client/src/Client/Connection.php`:

```php
    /**
     * The handle for an IMPERATIVE transaction opened by {@see begin}, or null.
     *
     * Non-null makes every statement method on this Connection route through it, so a statement
     * issued between `begin()` and `commit()`/`rollBack()` carries the transaction's `tx_id`. It is
     * a real {@see TxHandle} — the SAME object the closure form uses — precisely so the imperative
     * path inherits `TxHandle::run`'s bare send-and-classify semantics: NO transparent reconnect,
     * NO re-issue (charter rule 3). Reconnecting mid-transaction would void the `tx_id` silently.
     */
    private ?TxHandle $tx = null;

    /** Whether an imperative transaction opened by {@see begin} is currently open. */
    public function inTransaction(): bool
    {
        return $this->tx !== null;
    }

    /**
     * Open a transaction IMPERATIVELY and leave it open until {@see commit} or {@see rollBack}.
     *
     * This is the shape a Doctrine DBAL driver needs: DBAL's `Connection::beginTransaction()`,
     * `commit()` and `rollBack()` are three unrelated calls with the caller's code in between, it
     * owns its own nesting counter (implemented with `SAVEPOINT` SQL), and its `transactional()`
     * helper is built ON TOP of the trio — DBAL never hands a closure to a driver.
     *
     * **Retry is the CALLER's.** Unlike {@see transaction}, nothing here re-runs anything: there is
     * no closure to re-run, and re-issuing an individual in-transaction statement would be
     * meaningless (the transaction it belonged to is already dead). A driver should construct its
     * Connection with {@see RetryPolicy::none()} so the autocommit read-retry does not double up
     * with the caller's own policy.
     *
     * Nesting is not supported — call `SAVEPOINT` SQL instead, which passes through inside an open
     * transaction. Attempting to nest, or to {@see transaction} while one is open, throws.
     *
     * Isolation is deliberately NOT a parameter: named isolation constants would mean hand-written
     * protocol numbers on the PHP side (charter rule 2), and Doctrine sets isolation with a
     * `SET SESSION TRANSACTION ISOLATION LEVEL …` statement, not a driver flag.
     */
    public function begin(bool $readonly = false): void
    {
        if ($this->tx !== null) {
            throw new InvalidTransactionStateException(
                'a transaction is already open on this connection; Ferro does not nest transactions '
                    . '(use SAVEPOINT SQL, which passes through inside an open transaction)',
            );
        }
        $session = $this->session();
        $payload = BeginRequest::encode(
            ['pool' => $this->pool, 'isolation' => null, 'readonly' => $readonly],
            $this->encodePacker,
        );
        try {
            $outcome = $session->sendRequest(C::SERVICE_TX, C::METHOD_TX_BEGIN, $payload);
        } catch (ConnectionLostException | TransportException $e) {
            // A LOST BEGIN must be handed to the caller as a FATE, not as a raw transport error.
            // "DBAL owns retry" only means anything if DBAL is told what it is allowed to retry —
            // and a lost BEGIN opened nothing, so it is Retryable. The closure form already routes
            // this through the same classifier (`Connection::transaction`'s BEGIN arm); the
            // imperative form must not be the one path that leaks an untyped TransportException.
            //
            // NOTE the deliberate difference from the closure form: no reconnect and no re-issue
            // happen here (charter rule 3). The typed exception is the whole answer.
            throw $this->fate->classifyLoss(
                OpKind::TxBegin,
                true,
                'BEGIN lost: ' . $e->getMessage(),
                $e instanceof ConnectionLostException ? $e->errorPayload() : null,
                $this->reconnect?->lastEpochChanged() ?? false,
            );
        } catch (CodecException $e) {
            throw new ProtocolException('failed to decode BEGIN terminal: ' . $e->getMessage(), 0, $e);
        }
        if (!$outcome->isOk()) {
            // A REJECTED BEGIN opened nothing either, so nothing is left dangling. The taxonomy
            // exception propagates verbatim and the CALLER decides whether to retry.
            throw ErrorMapper::fromOutcome($outcome);
        }
        $this->tx = new TxHandle(
            $session,
            $this->codec,
            $this->pool,
            $this->decodeTxId($outcome),
            $this->encodePacker,
        );
    }

    /**
     * COMMIT the transaction opened by {@see begin}.
     *
     * A lost COMMIT is the §19.3 Indeterminate carve-out and propagates as
     * {@see IndeterminateException} — it is NEVER retried, here or anywhere. The handle is cleared
     * BEFORE the exception escapes so a failed commit cannot leave this Connection wedged in a
     * transaction that no longer exists engine-side.
     */
    public function commit(): void
    {
        $tx = $this->requireTx('commit');
        $this->tx = null;
        try {
            $tx->commit();
        } catch (ConnectionLostException | TransportException $e) {
            throw $this->fate->classifyLoss(
                OpKind::TxCommit,
                false,
                'COMMIT lost: ' . $e->getMessage(),
                null,
                $this->reconnect?->lastEpochChanged() ?? false,
            );
        }
    }

    /**
     * ROLLBACK the transaction opened by {@see begin}. The handle is cleared either way.
     *
     * **A lost ROLLBACK does not throw.** This is a deliberate asymmetry with {@see commit}, and it
     * exists because of how the caller uses it: `Doctrine\DBAL\Connection::transactional()` — and
     * essentially every hand-written `try { … } catch { $conn->rollBack(); throw; }` — calls this
     * from a `catch`/`finally` block, where the caller is ALREADY carrying the error that matters.
     * A raw throw from here would replace that error with a transport failure and the real cause
     * would never be seen.
     *
     * It is also harmless, which is what makes it correct rather than merely convenient: a rollback
     * whose response was lost has the same OUTCOME as one that succeeded. The transaction is dead
     * either way — the engine rolls back and tombstones the `tx_id` on session death, on deadline
     * and on drop (§19.3, `OpKind::TxRollback` classifies Retryable precisely because "a lost
     * rollback is not a lost write"). There is nothing for the caller to decide, so there is nothing
     * to report.
     *
     * A SERVER-side rejection (a well-formed `Outcome::Error`, e.g. an unknown `tx_id`) is a
     * different thing and still throws: that is the engine telling us our state is wrong, not a link
     * failure, and swallowing it would hide a real bug.
     */
    public function rollBack(): void
    {
        $tx = $this->requireTx('rollBack');
        $this->tx = null;
        try {
            $tx->rollback();
        } catch (ConnectionLostException | TransportException) {
            // Intentionally swallowed — see the docblock. The transaction is dead either way, and
            // this is almost always called from a `finally` that is carrying the real error.
        }
    }

    private function requireTx(string $method): TxHandle
    {
        return $this->tx ?? throw new InvalidTransactionStateException(
            $method . '() with no open transaction (call begin() first)',
        );
    }
```

**One new leaf exception class**, `php/client/src/Client/Error/InvalidTransactionStateException.php`, extending `FerroException`:

```php
/**
 * A transaction-lifecycle API was called in a state that cannot support it: `commit()`/`rollBack()`
 * with nothing open, a nested `begin()`, or `transaction()` while an imperative transaction is open.
 *
 * A distinct LEAF class, not a bare {@see FerroException}, because these are the only misuses this
 * client can detect purely from its own state — and because a test asserting on the root of the
 * exception tree passes for ANY Ferro error, including one thrown by the test's own setup SQL. Every
 * misuse test in this slice names this class.
 *
 * Never a taxonomy error: no request was sent, so there is no `ErrorPayload` and no fate.
 */
final class InvalidTransactionStateException extends FerroException {}
```

The two other misuse throws in this task — the nested-`begin()` guard and `transaction()`'s
open-imperative-transaction guard — use the same class.

- [ ] **Step 4: Route the statement methods through the open transaction**

Every statement entry on `Connection` gains one delegation line. `exec`, `query`, `queryOne`, `scalar` and `rows` all funnel through `dispatchAutocommit`, so the delegation is applied to each public method (not inside `dispatchAutocommit`, whose reconnect loop is the thing being bypassed):

```php
    public function exec(string $sql, array $params = [], bool $readonly = false): int
    {
        if ($this->tx !== null) {
            $res = $this->tx->runForConnection($sql, $params, $readonly, ExecCodec::FETCH_NONE);
            $this->lastInsertId = $res['last_insert_id'];
            return $res['affected'];
        }
        // ... unchanged autocommit path, which also records $this->lastInsertId ...
    }
```

`TxHandle::run` is `private`; expose it as `public function runForConnection(string $sql, array $params, bool $readonly, int $fetch): array` (the same body, same return shape) with a docblock saying it exists so `Connection`'s imperative path reuses the tx semantics verbatim instead of duplicating them.

`Connection::stream()` (`:236`) must carry the id too — the engine supports tx-scoped streaming (`ferrod/src/services/sql.rs:249-281`) and `ExecCodec::encode` already takes `?int $txId`:

```php
        // Inside an imperative transaction the stream MUST carry the tx_id: an autocommit stream
        // would run OUTSIDE the open transaction and see none of its uncommitted writes — a silent
        // wrong answer. The engine routes a tx-scoped streamed fetch to the owning actor.
        $payload = $this->codec->encode(
            $this->pool,
            $sql,
            $params,
            true,
            ExecCodec::FETCH_STREAM,
            $this->tx?->txId(),
        );
```

- [ ] **Step 5: Guard the closure form**

At the top of `Connection::transaction()` (`:326`):

```php
        if ($this->tx !== null) {
            throw new InvalidTransactionStateException(
                'transaction() cannot be called while an imperative transaction is open '
                    . '(commit() or rollBack() first); Ferro does not nest transactions',
            );
        }
```

The rest of `transaction()` — including the §19.1 re-run policy and the lost-COMMIT carve-out — is untouched.

- [ ] **Step 6: Add the offline unit tests (FakeSession, no database)**

`php/client/tests/Client/ConnectionImperativeTxTest.php`, over the existing `FakeSession` fixture
(`php/client/tests/Support/FakeSession.php`), which records every `sendRequest(service, method, payload)`:

```php
<?php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\InvalidTransactionStateException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\ExecCodec;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

final class ConnectionImperativeTxTest extends TestCase
{
    /**
     * Decode a recorded `ExecRequest` payload back to its field map.
     *
     * `ExecRequest` has **no `decode()`** — it exposes `encode()` and `mapFromWire()`
     * (`ExecRequest.php:18,42`), so the payload is unpacked first and then mapped. `PurePacker` is
     * the right packer here for two reasons: it is what `PackerFactory::forEncode()` returns, and
     * `ExtPacker::unpack` consumes the WHOLE buffer regardless of the offset it is handed.
     *
     * @return array<string,mixed>
     */
    private static function decodeExec(string $payload): array
    {
        $off = 0;
        $w = (new PurePacker())->unpack($payload, $off);
        return ExecRequest::mapFromWire((array) $w);
    }

    public function testBeginOpensATransactionAndScopesTheNextStatementToIt(): void
    {
        $session = FakeSession::withTxBegin(txId: 41)->thenExecOk();
        $c = new Connection(session: $session);

        $this->assertFalse($c->inTransaction());
        $c->begin();
        $this->assertTrue($c->inTransaction());

        $c->exec('INSERT INTO t VALUES (1)');

        // THE assertion: decode the recorded ExecRequest and read its tx_id back off the wire.
        // Asserting on the encoded payload — not on a getter — is what makes this falsifiable if the
        // delegation in Connection::exec() is ever dropped.
        $sent = $session->lastRequest();
        $this->assertSame(C::SERVICE_SQL, $sent['service']);
        $req = self::decodeExec($sent['payload']);
        $this->assertSame(41, $req['tx_id'], 'a statement inside begin() must carry the tx_id');
    }

    public function testAnAutocommitStatementCarriesANullTxId(): void
    {
        $session = FakeSession::withExecOk();
        $c = new Connection(session: $session);
        $c->exec('INSERT INTO t VALUES (1)');
        $req = self::decodeExec($session->lastRequest()['payload']);
        $this->assertNull($req['tx_id'], 'outside a transaction the tx_id stays null');
    }

    public function testStreamInsideATransactionCarriesTheTxId(): void
    {
        $session = FakeSession::withTxBegin(txId: 42)->thenStreamEnd();
        $c = new Connection(session: $session);
        $c->begin();
        foreach ($c->stream('SELECT 1') as $_) {
            // drain
        }
        $req = self::decodeExec($session->lastRequest()['payload']);
        $this->assertSame(42, $req['tx_id'], 'a stream inside a transaction must be tx-scoped');
        // The fetch modes live on the CODEC (`ExecCodec::FETCH_STREAM`), not on the generated
        // protocol constants — there is no `C::FETCH_*` (hazard 61).
        $this->assertSame(ExecCodec::FETCH_STREAM, $req['fetch']);
    }

    public function testCommitClearsTheHandleEvenWhenTheSessionThrows(): void
    {
        $session = FakeSession::withTxBegin(txId: 43)->thenThrowOnCommit();
        $c = new Connection(session: $session);
        $c->begin();
        try {
            $c->commit();
            $this->fail('a lost COMMIT must surface');
        } catch (IndeterminateException) {
            // §19.3's one transactional Indeterminate — the LEAF class, not FerroException:
            // asserting the root would also pass if commit() threw an unrelated protocol error.
        }
        $this->assertFalse(
            $c->inTransaction(),
            'a failed COMMIT must not leave the connection wedged in a transaction that is gone',
        );
    }

    /**
     * A LOST BEGIN reaches the caller as a FATE, not a raw transport error.
     *
     * "DBAL owns retry" is only meaningful if DBAL is told what may be retried. A lost BEGIN opened
     * nothing, so it is Retryable — and it must arrive as `RetryableException`, exactly as the
     * closure form's BEGIN arm already classifies it. This is the one assertion that would go RED if
     * `begin()` let a `TransportException` escape untyped.
     */
    public function testALostBeginIsClassifiedAsRetryableAndLeavesNoOpenTransaction(): void
    {
        $c = new Connection(session: FakeSession::thatThrowsTransportOnBegin());
        try {
            $c->begin();
            $this->fail('a lost BEGIN must surface');
        } catch (RetryableException) {
            // expected: nothing was opened, so re-running is safe — the caller's call.
        }
        $this->assertFalse($c->inTransaction(), 'a failed begin() opens nothing');
    }

    /**
     * A LOST ROLLBACK does NOT throw — DBAL calls rollBack() from a catch/finally that is already
     * carrying the real error, and the transaction is dead either way.
     */
    public function testALostRollbackIsSwallowedSoItCannotMaskTheCallersError(): void
    {
        $session = FakeSession::withTxBegin(txId: 45)->thenThrowOnRollback();
        $c = new Connection(session: $session);
        $c->begin();

        $c->rollBack(); // must not throw

        $this->assertFalse($c->inTransaction(), 'the handle is cleared either way');
    }

    /** @return iterable<string, array{0: \Closure(Connection): void}> */
    public static function misuses(): iterable
    {
        yield 'commit with no transaction' => [static fn (Connection $c) => $c->commit()];
        yield 'rollBack with no transaction' => [static fn (Connection $c) => $c->rollBack()];
        yield 'nested begin' => [static function (Connection $c): void { $c->begin(); $c->begin(); }];
        yield 'closure form while open' => [
            static function (Connection $c): void { $c->begin(); $c->transaction(static fn () => null); },
        ];
    }

    /**
     * Misuse throws the dedicated LEAF class.
     *
     * Deliberately NOT `FerroException`, which is the ROOT of the whole tree (hazard 68): every
     * taxonomy error, every protocol error and every transport error extends it, so
     * `expectException(FerroException::class)` passes when the misuse is not detected at all and
     * something else fails instead.
     *
     * @param \Closure(Connection): void $misuse
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('misuses')]
    public function testMisuseThrowsInvalidTransactionState(\Closure $misuse): void
    {
        $c = new Connection(session: FakeSession::withTxBegin(txId: 44));
        $this->expectException(InvalidTransactionStateException::class);
        $misuse($c);
    }
}
```

`FakeSession`'s named constructors (`withTxBegin`, `withExecOk`, `thenExecOk`, `thenStreamEnd`,
`thenThrowOnCommit`, `thenThrowOnRollback`, `thatThrowsTransportOnBegin`, `lastRequest`) are thin
additions to the existing fixture — it already records requests and replays canned `Outcome`s; add
only the queueing helpers these seven tests need, in the fixture's existing style. The two
throwing ones raise `TransportException`, which is what `Transport` raises on a lost link, so the
classification path under test is the real one.

- [ ] **Step 7: Run, then MUTATION-PROVE**

```
(cd php/client && ./vendor/bin/phpunit tests/Client tests/Live/ImperativeTransactionLiveTest.php)
(cd php/client && ./vendor/bin/phpstan analyse src --level 9)
```
Mutate:

1. Delete the delegation from `exec()` so it runs autocommit → `testStatementsInsideAnImperativeTransactionAreScopedToIt` goes RED (the row survives the rollback) **and** the offline `tx_id` assertion goes RED.
2. Move `$this->tx = null;` in `commit()` to AFTER `$tx->commit()` → `testCommitClearsTheHandleEvenWhenTheSessionThrows` goes RED (the connection is left wedged in a transaction that no longer exists).
3. Remove the `transaction()` guard → the nesting test goes RED.
4. Let `begin()`'s `ConnectionLostException | TransportException` arm through unclassified (delete the `catch` so the raw exception escapes) → `testALostBeginIsClassifiedAsRetryableAndLeavesNoOpenTransaction` goes RED. Without this arm DBAL is handed a transport error with no fate and cannot decide anything, which defeats the purpose of the whole task.
5. Make `rollBack()` rethrow (delete its swallowing `catch`) → `testALostRollbackIsSwallowedSoItCannotMaskTheCallersError` goes RED. This is the mutation that pins the `finally`-safety property.
6. Change `InvalidTransactionStateException` back to `FerroException` at any one throw site → `testMisuseThrowsInvalidTransactionState` goes RED for that row. Then, to see WHY the leaf class matters, revert the test to `expectException(FerroException::class)` and ALSO delete the guard entirely → the test **passes** on the resulting `TypeError`/`Error` only if that error is a `FerroException`, and more usefully: point the misuse at a connection whose setup throws and watch the root-class assertion swallow it. Record it; restore.

Restore each.

- [ ] **Step 8: Commit**

```bash
./ci/local-gate.sh --live
git add php/client
git commit -m "feat(m1-s8a): imperative begin/commit/rollBack on the PHP client

DBAL's Connection calls the driver's transaction trio imperatively and owns its
own nesting counter; it never hands a closure to a driver. The trio holds a real
TxHandle, so every statement between begin() and commit() carries the tx_id and
inherits TxHandle's bare send-and-classify semantics — no transparent reconnect,
no re-issue (charter rule 3; a mid-tx reconnect would void the tx_id silently).
stream() carries the tx_id too, or it would run outside the open transaction and
miss its uncommitted writes.

The closure form and its §19.1 re-run policy are unchanged except for one guard
refusing to nest.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 10: `Ferro\Bytes` — make `TAG_BYTES` reachable from PHP (`ParameterType::BINARY` / `LARGE_OBJECT`)

**Files:**
- Create: `php/client/src/Bytes.php`
- Modify: `php/client/src/Client/ExecCodec.php:307-332` (`bindOne`)
- Modify: `php/client/src/Protocol/Msgpack/ExtPacker.php:47` (`packBin`)
- Create: `php/client/tests/Unit/BytesBindTest.php`, `php/client/tests/Live/BytesLiveTest.php`
- Modify: `ferro-spec-v0.2.md:216` + §22.2 (k)(4)

**Interfaces:**
- Consumes: `LiveTestCase::requireMysqlPool()` (Task 2).
- Produces: `Ferro\Bytes` with `public readonly string $value`, `Bytes::fromStream(mixed $stream): self`.

**Why an explicit marker and not `is_string` sniffing.** Every PHP string binds `TAG_TEXT` today, and that must not change: a bare string's *contents* are never inspected to choose a tag (the same rule that keeps `'infinity'` in a `varchar` column from being retagged as a temporal — SPEC §22.2 (j)). So `BYTES` needs a value object, exactly as `Decimal`/`Date`/`Uuid` do. SPEC §22.2 (k)(4) already specifies this shape.

**The latent packer bug this task exposes (hazard 49).** `ExtPacker::packBin` is `\msgpack_pack($s)`, which emits msgpack **`str`**, while Rust's `Value::decode` BYTES arm uses `read_bin` — **marker-strict** for `0xc4/0xc5/0xc6`. It is harmless today only because nothing produces a `TAG_BYTES` param and `PackerFactory::forEncode()` always returns `PurePacker`. This task creates the first call path, so the fix ships with it — the same fix `packUint` already carries (delegate to the pure limb encoder).

- [ ] **Step 1: Write the failing unit tests**

`php/client/tests/Unit/BytesBindTest.php`:

```php
<?php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Bytes;
use Ferro\Client\ExecCodec;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\{ExtPacker, PurePacker};
use PHPUnit\Framework\TestCase;

final class BytesBindTest extends TestCase
{
    public function testBytesBindsToTagBytesWhileABareStringStaysText(): void
    {
        $codec = self::codec();
        $this->assertSame(C::TAG_BYTES, $codec->bindOne(new Bytes("\x00\x01\xff"))['tag']);
        $this->assertSame("\x00\x01\xff", $codec->bindOne(new Bytes("\x00\x01\xff"))['data']);
        // A bare string's CONTENTS are never sniffed — it stays TEXT even when it is not valid UTF-8
        // shaped, because retagging by content is the silent miscast §9.1 forbids.
        $this->assertSame(C::TAG_TEXT, $codec->bindOne('plain')['tag']);
    }

    public function testBytesEncodesAsAMsgpackBinFrameNotAStr(): void
    {
        $p = new PurePacker();
        $wire = \Ferro\Protocol\Value::bytes("\x00\x01\xff")->encode($p);
        // fixarray(2), pfix tag, then the BIN marker 0xc4 — never a str marker (0xa0-0xbf/0xd9…).
        $this->assertSame('92', bin2hex(substr($wire, 0, 1)));
        $this->assertSame('c4', bin2hex(substr($wire, 2, 1)), 'BYTES must ride the msgpack bin family');
    }

    /**
     * `ExtPacker::packBin` was `\msgpack_pack($s)`, which emits msgpack **str** — and the engine's
     * BYTES decoder is marker-strict for bin. Latent while nothing bound BYTES; this test is the
     * first thing that would have caught it.
     */
    public function testExtPackerPackBinIsByteIdenticalToThePurePacker(): void
    {
        if (!\extension_loaded('msgpack')) {
            $this->markTestSkipped('ext-msgpack absent');
        }
        foreach (['', "\x00", "\x00\x01\xff", str_repeat("\xfe", 300), str_repeat("\x01", 70000)] as $s) {
            $this->assertSame(
                bin2hex((new PurePacker())->packBin($s)),
                bin2hex((new ExtPacker())->packBin($s)),
                'ExtPacker::packBin must emit a real msgpack bin, byte-for-byte',
            );
        }
    }

    public function testFromStreamMaterialisesTheWholeStream(): void
    {
        $h = fopen('php://memory', 'r+');
        fwrite($h, "\x00\xff");
        rewind($h);
        $this->assertSame("\x00\xff", Bytes::fromStream($h)->value);
    }

    /**
     * **The rule that had no guard at all in v1 (probe 2, weak guard 4).** `bindOne` must have NO
     * implicit `is_resource` arm: reading a stream into memory is a decision with a memory cost, and
     * it is the CALLER's to make — explicitly, via {@see Bytes::fromStream}. v1's "mutation 3"
     * observed that adding such an arm broke nothing and concluded the arm should stay absent, which
     * left the rule enforced by a comment. This asserts it.
     *
     * A raw resource is therefore rejected by the DEFAULT arm, and the refusal must name `Bytes` so
     * the message tells the caller what to do instead.
     */
    public function testARawResourceIsRefusedAndTheMessagePointsAtBytesFromStream(): void
    {
        $codec = self::codec();
        $h = fopen('php://memory', 'r+');
        fwrite($h, "\x00\xff");
        rewind($h);

        try {
            $codec->bindOne($h);
            $this->fail('a raw stream resource must NOT bind implicitly');
        } catch (\Ferro\Client\Error\FerroException $e) {
            $this->assertStringContainsString(
                'Bytes',
                $e->getMessage(),
                'the refusal must name Ferro\\Bytes as the explicit route: ' . $e->getMessage(),
            );
        }
    }
}
```

- [ ] **Step 2: Run and watch it fail**

```
(cd php/client && ./vendor/bin/phpunit tests/Unit/BytesBindTest.php)
```
Expected: FAIL — `Ferro\Bytes` does not exist; `ExtPacker::packBin` emits a `str` marker.

- [ ] **Step 3: Add the value object**

`php/client/src/Bytes.php`:

```php
<?php // /php/client/src/Bytes.php
declare(strict_types=1);
namespace Ferro;

/**
 * An explicit BINARY bind marker (SPEC §9, §22.2 (k)(4)).
 *
 * Every bare PHP string binds `TAG_TEXT`, and that is deliberate: a string's CONTENTS are never
 * inspected to pick a tag (the same rule that stops `'infinity'` in a `varchar` column from being
 * retagged as a temporal). So a byte string needs a marker, exactly as `Ferro\Decimal` and
 * `Ferro\Uuid` do — otherwise `TAG_BYTES` is unreachable from PHP and Doctrine's
 * `ParameterType::BINARY` / `ParameterType::LARGE_OBJECT` cannot bind at all.
 *
 * Without it a non-UTF-8 string does not merely mis-bind, it fails at the CODEC: `TAG_TEXT` rides
 * the msgpack `str` family and the engine's reader ends in `String::from_utf8`, so the request is
 * rejected as `malformed messagepack payload: invalid utf8` — a generic protocol fault rather than
 * a diagnosable bind error.
 *
 * READS are asymmetric and that is intentional: a `bytea`/`VARBINARY` column hydrates to a plain
 * PHP string (a binary-safe type), so a round trip is `Bytes` out, `string` back.
 */
final class Bytes
{
    public function __construct(public readonly string $value)
    {
    }

    /**
     * Materialise a stream into a `Bytes`. Doctrine's `BlobType::convertToPHPValue` hands the driver
     * a PHP **resource** for `LARGE_OBJECT`; the client deliberately has no implicit `is_resource`
     * bind arm — deciding to read a stream into memory is the CALLER's, made explicitly here.
     *
     * @param mixed $stream an open, readable stream resource
     */
    public static function fromStream(mixed $stream): self
    {
        if (!\is_resource($stream)) {
            throw new \InvalidArgumentException(
                'Ferro\Bytes::fromStream expects an open stream resource, got ' . \get_debug_type($stream),
            );
        }
        $data = \stream_get_contents($stream);
        if ($data === false) {
            throw new \RuntimeException('Ferro\Bytes::fromStream could not read the stream');
        }
        return new self($data);
    }
}
```

- [ ] **Step 4: Add the bind arm and fix `ExtPacker::packBin`**

`php/client/src/Client/ExecCodec.php` — one arm among the §9 value objects (order is irrelevant here; `Bytes` is an object, so it cannot collide with `is_string`):

```php
            // The explicit BINARY marker (SPEC §22.2 (k)(4)). `TAG_BYTES` rides the msgpack `bin`
            // family, so a non-UTF-8 payload survives — unlike `TAG_TEXT`, whose `str` family is
            // rejected by the engine's reader as `invalid utf8` before the bind pre-flight.
            $v instanceof Bytes    => ['tag' => C::TAG_BYTES,   'data' => $v->value],
```

and update the `default` arm's message to list `Bytes` among the accepted value objects.

`php/client/src/Protocol/Msgpack/ExtPacker.php:47`:

```php
    /**
     * Delegated to the pure limb encoder — deliberately NOT `\msgpack_pack()`.
     *
     * `\msgpack_pack()` on a PHP string emits msgpack **`str`**, while the wire contract for
     * `TAG_BYTES` is the **`bin`** family and the engine's decoder is marker-strict (`read_bin`
     * accepts only `0xc4`/`0xc5`/`0xc6`). The extension has no way to express the distinction: PHP
     * has one string type. This was latent while nothing bound `TAG_BYTES` and
     * `PackerFactory::forEncode()` returned `PurePacker` regardless; `Ferro\Bytes` creates the first
     * call path. Same shape and same reason as {@see packUint}.
     */
    public function packBin(string $s): string { return $this->pure->packBin($s); }
```

- [ ] **Step 5: Write the live round trip on both engine families**

`php/client/tests/Live/BytesLiveTest.php`:

```php
<?php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Bytes;
use Ferro\Client\Error\FerroException;

final class BytesLiveTest extends LiveTestCase
{
    private const BLOB = "\x00\x01\xfe\xff\x7f\x80";

    public function testPostgresByteaRoundTrip(): void
    {
        $c = $this->connectConnection();
        $c->exec('DROP TABLE IF EXISTS s8a_bytes');
        $c->exec('CREATE TABLE s8a_bytes (b bytea)');
        $c->exec('INSERT INTO s8a_bytes (b) VALUES (?)', [new Bytes(self::BLOB)]);

        $rows = $c->query('SELECT b FROM s8a_bytes');
        $this->assertSame(bin2hex(self::BLOB), bin2hex($rows[0]['b']));
    }

    public function testMysqlVarbinaryRoundTrip(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->connectConnection(null, $pool);
        $c->exec('DROP TABLE IF EXISTS s8a_bytes');
        $c->exec('CREATE TABLE s8a_bytes (b VARBINARY(64))');
        $c->exec('INSERT INTO s8a_bytes (b) VALUES (?)', [new Bytes(self::BLOB)]);

        $rows = $c->query('SELECT b FROM s8a_bytes');
        $this->assertSame(bin2hex(self::BLOB), bin2hex($rows[0]['b']));
    }

    /**
     * The pre-M1-S8a failure, pinned so it cannot silently come back: a BARE non-UTF-8 string is
     * still rejected — and by the CODEC, not the bind pre-flight — because a string's contents are
     * never sniffed for a tag.
     */
    public function testABareNonUtf8StringIsStillRefused(): void
    {
        $c = $this->connectConnection();
        $c->exec('DROP TABLE IF EXISTS s8a_bytes_bare');
        $c->exec('CREATE TABLE s8a_bytes_bare (b bytea)');

        // NOT `expectException(FerroException::class)`: that is the ROOT of the tree (hazard 68), so
        // it would also pass if one of the two setup statements above threw — i.e. the test would be
        // green having never reached the assertion's subject. Assert on the MESSAGE instead, which
        // pins the actual mechanism: `TAG_TEXT` rides the msgpack `str` family and the engine's
        // reader ends in `String::from_utf8`, so the failure is a CODEC fault, not a bind error.
        try {
            $c->exec('INSERT INTO s8a_bytes_bare (b) VALUES (?)', [self::BLOB]);
            $this->fail('a bare non-UTF-8 string must not bind');
        } catch (FerroException $e) {
            $this->assertStringContainsStringIgnoringCase(
                'utf8',
                $e->getMessage(),
                'the refusal must be the UTF-8 codec fault, not some other error: ' . $e->getMessage(),
            );
        }
    }

    public function testLargeObjectStreamBindsThroughFromStream(): void
    {
        $c = $this->connectConnection();
        $c->exec('DROP TABLE IF EXISTS s8a_blob');
        $c->exec('CREATE TABLE s8a_blob (b bytea)');
        $h = fopen('php://memory', 'r+');
        fwrite($h, self::BLOB);
        rewind($h);
        $c->exec('INSERT INTO s8a_blob (b) VALUES (?)', [Bytes::fromStream($h)]);

        $rows = $c->query('SELECT b FROM s8a_blob');
        $this->assertSame(bin2hex(self::BLOB), bin2hex($rows[0]['b']));
    }
}
```

- [ ] **Step 6: Run, then MUTATION-PROVE**

```
(cd php/client && ./vendor/bin/phpunit tests/Unit/BytesBindTest.php tests/Live/BytesLiveTest.php)
(cd php/client && ./vendor/bin/phpstan analyse src --level 9)
```
Mutate:

1. Change the `Bytes` arm to `C::TAG_TEXT` → both live round trips go RED with `invalid utf8`.
2. Revert `ExtPacker::packBin` to `\msgpack_pack($s)` → `testExtPackerPackBinIsByteIdenticalToThePurePacker` goes RED (with ext-msgpack loaded, as CI has).
3. **Add an `is_resource($v)` arm to `bindOne` that materialises the stream** → `testARawResourceIsRefusedAndTheMessagePointsAtBytesFromStream` goes RED. This is the correction of the v1 mutation, which added the arm, observed that "nothing breaks", and concluded the arm should stay absent — leaving the rule with **no guard at all** (probe 2, weak guard 4). The rule now has one. Restore.
4. Drop `Bytes` from the `default` arm's message while keeping the refusal → the same test goes RED on the message assertion. A refusal that does not name the route out is a dead end for the caller.
5. Replace the message assertion in `testABareNonUtf8StringIsStillRefused` with `expectException(FerroException::class)` and then break the test's OWN setup (`CREATE TABLE s8a_bytes_bare (b nosuchtype)`) → the root-class form passes anyway; the message form goes RED. Record it; restore.

- [ ] **Step 7: Spec truth + commit**

`ferro-spec-v0.2.md:216` currently says `BYTES` is "read-only from PHP" — amend it. §22.2 (k)(4) is marked closed, recording the `ExtPacker::packBin` fix and the deliberate `Bytes`-out/`string`-back asymmetry.

```bash
./ci/local-gate.sh --live
git add php/client ferro-spec-v0.2.md
git commit -m "feat(m1-s8a): Ferro\\Bytes makes TAG_BYTES reachable from PHP

Every bare PHP string binds TAG_TEXT and must keep doing so — a string's contents
are never sniffed for a tag — so BINARY needs an explicit marker, as
Decimal/Date/Uuid do. Without it a non-UTF-8 param failed at the CODEC
(str + from_utf8) as a generic 'malformed request', not a bind error.

Also fixes a latent packer bug the new path exposes: ExtPacker::packBin was
\\msgpack_pack(), which emits msgpack str, while the engine's BYTES decoder is
marker-strict for bin. Same fix and same reason as packUint.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 11: `HELLO_ACK` pool metadata — the wire change, with a deliberate version-skew story

**Files:**
- Modify: `proto/methods.toml:1` (`protocol_version = 2`), `proto/registry.lock.json` (regenerated), `php/client/src/Protocol/Generated/Constants.php` (regenerated)
- Modify: `engine/crates/ferro-proto/src/messages.rs:41` (`HelloAck`) + a new `PoolInfo`
- Modify: `engine/crates/ferro-proto/src/bin/gen_vectors.rs:255-270`; **all** of `proto/vectors/*.json` **and `proto/vectors/negative/*.bin`** (regenerated — the four negative fixtures carry the live `PROTOCOL_VERSION` in their frame header, so `reserved_flag.bin` must stay structurally valid after the bump and `bad_version.bin` must still carry a version that is wrong; hazard 60)
- Modify: `engine/crates/ferro-proto/tests/messages.rs:16-25`, `tests/golden_vectors.rs:160`, `tests/header.rs` (the new skew assertion)
- Modify: `engine/crates/ferrod/src/session/handshake.rs:48-56`; `engine/crates/ferrod/src/session/mod.rs:342-346` (the ack is built HERE, from `config.pools`)
- Modify: **`engine/crates/ferro-e2e/src/client.rs:31,:103`** (`Handshake.pools` is `Vec<String>` filled from `ack.pools` — omitting this is a HARD COMPILE BREAK and `cargo test --workspace` is a DoD gate, hazard 60) and `engine/crates/ferro-e2e/src/main.rs:111` (it prints them)
- Create: `php/client/src/Protocol/PoolInfo.php`; Modify: `php/client/src/Protocol/HelloAck.php`, `Message.php:25-31`, `php/client/src/Client/Session.php:104-106,:228`
- Modify: `php/client/tests/Client/SessionHandshakeTest.php:62,90`; `proto/PROTOCOL.md` §1 + §4
- Create: `php/client/tests/Unit/PoolInfoTest.php`
- **NOT modified by this task: `engine/crates/ferrod/src/pools.rs`.** See the note under Step 6 — the session has no access to the `PoolRegistry`, and this task does not need it.

**Interfaces:**
- Consumes: nothing from Tasks 1–10.
- Produces: Rust `ferro_proto::messages::PoolInfo { name: String, kind: String, server_version: Option<String> }` and `HelloAck { engine_version: u32, boot_epoch: u64, features: u32, pools: Vec<PoolInfo>, type_registry_hash: String }`. PHP `Ferro\Protocol\PoolInfo` (`public readonly string $name`, `$kind`, `?string $serverVersion`), `HelloAck::$pools: list<PoolInfo>`, `Session::poolInfo(): list<PoolInfo>` and the retained `Session::pools(): list<string>`.
- **Scope boundary:** `server_version` is emitted as `None` by this task. Task 12 fills it. Splitting here keeps the wire change's blast radius (all vectors, both codecs, twelve files) separate from the pool-probing behaviour change.

**THE SKEW STORY — read this before touching anything.** Adding a field to `HelloAck` does **NOT** move `TYPE_REGISTRY_HASH`: the hash is FNV-1a over the raw bytes of `registry.lock.json` (`ferro-proto/build.rs:106-114`), which carries protocol version / magic / flags / services / methods / error codes / type tags — **no message shapes**. So a new engine and an old client would handshake "successfully" (`handshake.rs:31-36` compares only the hash) and then the client would throw `CodecException('HelloAck arity != 5')` deep inside `HelloAck::decode` (`HelloAck.php:40`) — an ugly late failure instead of the clean session-fatal `errc::UNSUPPORTED` the design intends.

**Therefore this task bumps `protocol_version` 1 → 2.** That is the only lever that exists, and it is the right one: `PROTOCOL_VERSION` is **byte 1 of every frame header** (`header.rs:19`) and `Header::decode` rejects a mismatch with `CodecError::BadVersion` (`header.rs:42-47`; PHP mirror `Header.php:26`), so a skewed pair now fails at the **first byte of the first frame**, in both directions, before any payload is parsed. It also changes `registry.lock.json`, moving `TYPE_REGISTRY_HASH` as a second, independent tripwire. **The cost is that every committed vector's `frame_hex` changes** — accepted, mechanical, and fully byte-locked by the existing conformance tests. Vectors are **regenerated**, never hand-edited, and that includes the four `proto/vectors/negative/*.bin` fixtures (hazard 60).

**Be honest in the docs about WHAT KIND of failure this is.** The version byte is checked in `Header::decode` on both sides, before any payload — so the skew is caught deterministically and early. But it surfaces as a **codec/transport failure** (`CodecError::BadVersion` → a decode error on the client, a malformed-frame session-fatal on the engine), **never as a typed handshake rejection** like the `errc::UNSUPPORTED` a `type_registry_hash` mismatch produces. That is a real difference in operator experience — the log line says "bad frame version", not "engine and client disagree" — and `proto/PROTOCOL.md` §1 must say so, so nobody later reports the missing typed error as a bug. The alternative (carrying a version inside `HELLO` and rejecting it typed) is a strictly larger change and is not in this slice.

- [ ] **Step 1: Write the failing Rust round-trip test**

`engine/crates/ferro-proto/tests/messages.rs`:

```rust
/// `HelloAck.pools` carries STRUCTURED metadata, not bare names. Arity of `HelloAck` itself is
/// unchanged (5) — it is the ELEMENT shape that grew, which is why the version bump (not arity) is
/// what makes a skewed pair fail fast.
#[test]
fn hello_ack_carries_structured_pool_metadata() {
    let ack = HelloAck {
        engine_version: 1,
        boot_epoch: 7,
        features: 0,
        pools: vec![
            PoolInfo {
                name: "main".into(),
                kind: "postgres".into(),
                server_version: Some("PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1)".into()),
            },
            PoolInfo {
                name: "reporting".into(),
                kind: "mysql".into(),
                server_version: None,
            },
        ],
        type_registry_hash: "deadbeef".into(),
    };
    let back = HelloAck::decode(&ack.encode()).expect("round trip");
    assert_eq!(back, ack);
    assert_eq!(back.pools[0].kind, "postgres");
    assert_eq!(back.pools[1].server_version, None);
}
```

- [ ] **Step 2: Run and watch it fail**

```
cargo test -p ferro-proto hello_ack_carries_structured_pool_metadata
```
Expected: FAIL to compile — `PoolInfo` does not exist and `pools` is `Vec<String>`.

- [ ] **Step 3: Change the Rust message shape**

`engine/crates/ferro-proto/src/messages.rs:41`:

```rust
/// One pool's advertised metadata (M1-S8a). A positional fixarray of 3, nested inside
/// `HelloAck.pools`.
///
/// `kind` is the backend FAMILY (`"postgres"` / `"mysql"`), which the engine has known since
/// `PoolRegistry::build` (from the DSN scheme) but never put on the wire. `server_version` is the
/// backend's own `version()` string, **verbatim and unnormalised** — parsing it into a platform
/// decision is a client-tier concern (a Doctrine driver needs `mariadb` to appear in the string for
/// the MariaDB branch, and PG's leading word stripped), and normalising it here would bake one
/// ecosystem's conventions into the protocol.
///
/// `server_version` is `nil` when the engine has not learned it — a pool whose backend was
/// unreachable at handshake time. The handshake never depends on backend availability.
///
/// Still NEVER exposed: the DSN (§12 server secret).
msg!(PoolInfo { name: String, kind: String, server_version: Option<String> });

msg!(HelloAck { engine_version: u32, boot_epoch: u64, features: u32, pools: Vec<PoolInfo>, type_registry_hash: String });
```

- [ ] **Step 4: Bump the protocol version and regenerate the registry**

`proto/methods.toml:1`:

```toml
protocol_version    = 2   # M1-S8a: HELLO_ACK pools became structured metadata. Bumped so a skewed
                          # engine/client pair fails at the FIRST BYTE of the FIRST FRAME
                          # (Header::decode -> BadVersion) instead of deep inside HelloAck::decode —
                          # the lock file carries no message shapes, so nothing else would move.
```

```bash
cargo run -p ferro-proto --bin gen_registry    # or the documented regen entry point
php proto/tools/gen-php.php
cargo test -p ferro-proto registry_sync        # the regen-zero-diff guard
```

- [ ] **Step 5: Regenerate every vector and review the diff deliberately**

`gen_vectors.rs:255-270` — the `hello_ack` case must use a **non-empty** `pools` covering both `Some` and `None`, or the nested shape is locked by nothing (`pools: vec![]` locks no element bytes at all):

```rust
    let ack = HelloAck {
        engine_version: 1,
        boot_epoch: 0xFFFF_FFFF_FFFF_FFF0,
        features: 0,
        // NON-EMPTY on purpose: an empty list byte-locks no element shape. Both the Some and the
        // None arm of `server_version` are present so the nested fixarray is fully pinned.
        pools: vec![
            PoolInfo {
                name: "main".into(),
                kind: "postgres".into(),
                server_version: Some("PostgreSQL 17.10".into()),
            },
            PoolInfo {
                name: "reporting".into(),
                kind: "mysql".into(),
                server_version: None,
            },
        ],
        type_registry_hash: "deadbeef".into(),
    };
    write_case(
        "hello_ack",
        0,
        service::CORE,
        method_core::HELLO_ACK,
        1,
        ack.encode(),
        serde_json::json!({ "engine_version":1, "boot_epoch":"18446744073709551600",
                            "features":0,
                            "pools":[
                              {"name":"main","kind":"postgres","server_version":"PostgreSQL 17.10"},
                              {"name":"reporting","kind":"mysql","server_version":null}
                            ],
                            "type_registry_hash":"deadbeef" }),
    );
```

```bash
cargo run -p ferro-proto --bin gen_vectors
git diff --stat proto/vectors
git diff --stat proto/vectors/negative
```
Expected: **every** vector's `frame_hex` changes in its second byte (`f701…` → `f702…`) and `hello_ack.json`'s payload changes wholesale. Confirm byte 2 is the only difference in the others — a diff touching a payload elsewhere means something other than the version moved and must be investigated before continuing.

**The four `proto/vectors/negative/*.bin` fixtures move too and must be checked BY HAND, because their whole purpose is to be wrong in exactly one way** (hazard 60):

- `reserved_flag.bin` — must stay structurally valid *except* for the reserved flag. If its version byte is left at `1`, it now fails on the VERSION and the reserved-flag rejection it exists to prove is never reached: the test passes for the wrong reason.
- `bad_version.bin` — must still carry a version that is **wrong**, i.e. no longer `2`. If `gen_vectors` writes `PROTOCOL_VERSION` into it, it becomes a VALID frame and the test inverts.
- `bad_magic.bin`, `oversize_len.bin` — must keep failing on magic/length respectively, which means their version byte must be the *current* one so nothing else fires first.

After regenerating, run the negative-vector conformance tests on both sides and confirm each still fails for its OWN reason (the assertions name the expected `CodecError`/`CodecException` variant, so a wrong-reason pass shows up as a variant mismatch, not as a silent green).

- [ ] **Step 6: Produce the metadata engine-side (kind only, `server_version: None`) — with NO `PoolRegistry`**

**The v1 instruction said "its caller in `session/mod.rs` passes `registry.pool_info()`". That is not possible (probe 2, blocker B4):** `Session::run_with_handler` has no `PoolRegistry` and no way to reach one. The ack is built from `config.pools` (`session/mod.rs:342-346`); the registry is constructed in `ferrod/src/main.rs:35` and `ferro-e2e/src/main.rs:63` and reaches the session **only sealed inside the opaque `HandlerFactory` closure**; `serve()` does not take it (`serve.rs:51-58`).

**And it does not need one.** `PoolSpec` already carries `name` **and** `kind` (`config.rs:114-127`; `kind` comes from `config::infer_pool_kind`, which reads the DSN scheme at config-parse time). Everything this task advertises is already in `Config`. Threading the registry is Task 12's problem — it is the version probe that genuinely needs a live pool — and keeping it out of Task 11 is what lets the wire change land as a self-contained commit.

So the mapping lives next to the handshake, as a pure function of the config:

`engine/crates/ferrod/src/session/handshake.rs` — `hello_ack_frame` takes `Vec<PoolInfo>` instead of `Vec<String>`, and a small helper builds them:

```rust
/// The `HELLO_ACK` metadata for every configured pool, derived purely from `Config` (PROTOCOL.md §4).
///
/// No `PoolRegistry` is involved and none is needed: `PoolSpec.kind` is already the backend FAMILY,
/// inferred from the DSN SCHEME by `config::infer_pool_kind` at config-parse time. That also keeps
/// the handshake independent of whether any pool has ever been dialled — `ferrod` boots with
/// unreachable backends today and must keep doing so.
///
/// `server_version` is `None` here; M1-S8a Task 12 fills it (and is where the registry finally has
/// to be threaded in). Only the NAME and the family are exposed — never the DSN (§12 server secret).
pub fn pool_info_from_config(config: &Config) -> Vec<PoolInfo> {
    let mut out: Vec<PoolInfo> = config
        .pools
        .iter()
        .map(|spec| PoolInfo {
            name: spec.name.clone(),
            kind: match spec.kind {
                PoolKind::Postgres => "postgres".to_string(),
                PoolKind::Mysql => "mysql".to_string(),
            },
            server_version: None,
        })
        .collect();
    // Deterministic order. `config.pools` is a Vec so it is already stable, but sorting makes the
    // contract explicit and survives a future map-backed representation — a handshake that reports
    // pools in a different order per connection is needlessly untestable.
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}
```

and `session/mod.rs:342-346` becomes:

```rust
        let ack = handshake::hello_ack_frame(
            first.header.request_id,
            epoch,
            handshake::pool_info_from_config(&config),
        );
```

The `match spec.kind` is exhaustive over `PoolKind` on purpose: a third backend family must break the build here rather than silently advertise the wrong string to a driver that picks a platform from it.

**And the consumer that breaks (hazard 60):** `engine/crates/ferro-e2e/src/client.rs` declares `pub pools: Vec<String>` (`:31`) and fills it from `ack.pools` (`:103`), with `main.rs:111` printing it. Change the field to `Vec<PoolInfo>` and let the print show the whole triple — that is the demo's job, and it is now more informative. This is not optional tidying: without it `cargo test --workspace`, a DoD gate on every task, does not compile.

- [ ] **Step 7: Move the PHP codec in the same change set (charter rule 2)**

`php/client/src/Protocol/PoolInfo.php`:

```php
<?php // /php/client/src/Protocol/PoolInfo.php
declare(strict_types=1);
namespace Ferro\Protocol;

/**
 * One pool's metadata from `CORE/HELLO_ACK`. Mirrors the Rust `messages::PoolInfo` BYTES: a
 * positional fixarray of 3 — [name:str, kind:str, server_version:str|nil].
 *
 * `kind` is the backend family (`"postgres"` / `"mysql"`). `serverVersion` is the backend's own
 * `version()` output VERBATIM — normalising it (stripping PG's leading word, extracting a
 * major.minor.patch) is the consuming tier's job, not the protocol's. It is `null` when the engine
 * has not learned it, e.g. a pool whose backend was unreachable: the handshake never depends on
 * backend availability.
 */
final class PoolInfo
{
    public function __construct(
        public readonly string $name,
        public readonly string $kind,
        public readonly ?string $serverVersion,
    ) {}

    /** Decode one already-unpacked `[name, kind, server_version]` triple. */
    public static function fromWire(mixed $w): self
    {
        if (!is_array($w) || count($w) !== 3) {
            throw new CodecException('PoolInfo: expected a 3-element array');
        }
        $v = array_values($w);
        $version = $v[2];
        if ($version !== null && !is_string($version)) {
            throw new CodecException('PoolInfo: server_version is not str|nil');
        }
        return new self(SqlValueCodec::toStr($v[0]), SqlValueCodec::toStr($v[1]), $version);
    }

    /** @return array{0:string,1:string,2:string|null} the positional wire shape. */
    public function toWire(): array
    {
        return [$this->name, $this->kind, $this->serverVersion];
    }
}
```

`HelloAck.php` — the `pools` loop at `:49` becomes `PoolInfo::fromWire`, the constructor's `@param list<string> $pools` becomes `list<PoolInfo>`, and the `count($w) !== 5` check **stays** (arity is unchanged; the skew tripwire is the version byte, not the arity).

`Message.php:25-31`'s `'hello_ack'` encoder must emit the nested triples (it is in `CLIENT_ENCODED_MESSAGES`, so the byte lock runs it).

`Session.php` — keep the existing accessor and add the richer one:

```php
    /** @return list<string> the pool NAMES, for `ExecRequest.pool`. Unchanged surface. */
    public function pools(): array
    {
        return array_map(static fn (PoolInfo $p): string => $p->name, $this->poolInfo);
    }

    /** @return list<PoolInfo> the full advertised metadata (name + backend family + server version). */
    public function poolInfo(): array
    {
        return $this->poolInfo;
    }
```

`SessionHandshakeTest.php:62,90` build `hello_ack` payloads by hand — update both to the nested shape (a stale one now throws, which is the guard).

- [ ] **Step 8: Add the PHP unit test + run the whole conformance suite**

`php/client/tests/Unit/PoolInfoTest.php` covers `fromWire`/`toWire` round trips, the `null` version, and the two malformed shapes (wrong arity, non-string version).

```bash
cargo test -p ferro-proto
(cd php/client && ./vendor/bin/phpunit tests/Conformance tests/Client/SessionHandshakeTest.php tests/Unit/PoolInfoTest.php)
```

- [ ] **Step 9: MUTATION-PROVE the skew tripwire**

1. Revert `proto/methods.toml`'s `protocol_version` to `1`, regenerate ONLY the registry (not the vectors) → `cargo test -p ferro-proto registry_sync` and the golden-vector byte lock both go RED. Restore.
2. With the bump in place, hand-craft a frame whose byte 1 is `0x01` and feed it to `Header::decode` in a unit test → `CodecError::BadVersion`. Add that assertion permanently:

```rust
    /// The M1-S8a skew tripwire: a frame from an OLDER-protocol peer is rejected at the FIRST BYTE
    /// PAIR, before any payload is parsed — which is what makes the `HelloAck` shape change safe.
    ///
    /// `expected` is read from `consts::PROTOCOL_VERSION`, never written as a literal: a
    /// hand-written protocol constant is a defect wherever it appears, tests included (charter rule
    /// 2, hazard 69). `got` is derived the same way, so this test keeps working — and keeps meaning
    /// the same thing — through the next bump.
    #[test]
    fn a_frame_from_the_previous_protocol_version_is_rejected_by_the_header() {
        let stale = consts::PROTOCOL_VERSION - 1;
        let mut buf =
            Header { flags: 0, service: 1, method: 1, request_id: 1, payload_len: 0 }.encode();
        buf[1] = stale;
        match Header::decode(&buf) {
            Err(CodecError::BadVersion { expected, got }) => {
                assert_eq!(expected, consts::PROTOCOL_VERSION);
                assert_eq!(got, stale);
            }
            other => panic!("a stale protocol version must be rejected by the header, got {other:?}"),
        }
    }
```
3. Leave `pools: vec![]` in `gen_vectors.rs` and re-run: the vector regenerates but locks no element bytes — confirm that a deliberately wrong `PoolInfo` field ORDER in `Message.php` then goes UNDETECTED, and that with the non-empty fixture it goes RED. **Record this**: it is the proof that Step 5's non-empty fixture was load-bearing.

- [ ] **Step 10: Document + commit**

`proto/PROTOCOL.md` §4's `HELLO_ACK` table row 4 becomes `array<[str, str, str|nil]>` with the field meanings, §1 records `protocol_version = 2` and why it moved, and the §7 vector index notes the `hello_ack` reshape.

```bash
./ci/local-gate.sh --live
git add proto engine php
git commit -m "feat(m1-s8a): HELLO_ACK advertises per-pool metadata; protocol_version 1 -> 2

HelloAck.pools becomes a list of [name, kind, server_version] triples so a client
can learn the backend FAMILY at handshake instead of probing with a
dialect-specific query. kind is free (PoolSpec.kind, from the DSN scheme);
server_version rides as nil here and is filled by the next task.

The version bump is the deliberate skew story: the lock file carries no message
shapes, so nothing would otherwise move TYPE_REGISTRY_HASH and a skewed pair
would handshake 'successfully' and then throw inside HelloAck::decode. Byte 1 of
every frame header is the protocol version, so a skewed pair now fails at the
first byte of the first frame in both directions. Every vector is regenerated.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 12: Learn each pool's server version — once, lazily, and never on the boot path

**Files:**
- Modify: `engine/crates/ferrod/src/pools.rs` (per-pool version cache + the concurrent, bounded probe + a probe counter)
- **Thread `Arc<PoolRegistry>` to the session — this is where hazard 59's plumbing actually happens, and it is a compile cascade across seven files:**
  - `engine/crates/ferrod/src/session/mod.rs` — `Session::run_with_handler` gains a `pool_registry: Arc<PoolRegistry>` parameter; `Session::run` (`:218-222`) builds its own with `PoolRegistry::build(&config)` (its `Config` carries no pools, so this dials nothing — the same shape as the throwaway `TxRegistry` it already mints)
  - `engine/crates/ferrod/src/serve.rs:51-58` — `serve` gains the same parameter and clones it per accepted connection
  - `engine/crates/ferrod/src/main.rs:35,56` — `PoolRegistry::build` already returns `Arc<Self>`, so pass `registry.clone()` to `sql::make_handler` and `registry` to `serve`
  - `engine/crates/ferro-e2e/src/main.rs:63,75` — identical two-line change
  - `engine/crates/ferrod/tests/common/mod.rs` — `TestServer::spawn_with_factory` (`:141`) and `spawn_with_factory_and_config` (`:177`) gain an `Arc<PoolRegistry>` parameter (their callers already build one); `spawn` (`:68`), `spawn_with_handler*` (`:93,:99`), `spawn_one_session*` (`:234,:241`) and `spawn_serve*` (`:269,:280`) build an empty one from their own `Config` — they have no pools and none of their tests read `HelloAck.pools`. `exec_server` (`:554`) and `stream_server` (`:587`) pass their existing registry to both `make_handler` and the spawner.
  - `engine/crates/ferrod/tests/tx_it.rs:72` — the one direct `spawn_with_factory` caller outside `common`
- Modify: `engine/crates/ferrod/src/session/handshake.rs` (`hello_ack_frame`'s caller now awaits the metadata; `pool_info_from_config` from Task 11 becomes the FALLBACK used only by `Session::run`'s pool-less path)
- Create: `engine/crates/ferrod/tests/hello_meta_it.rs`
- Create: `php/client/tests/Live/PoolMetadataLiveTest.php`
- Modify: `php/client/tests/Live/LiveTestCase.php` (`connect()` must `hello()` — see Step 5)
- Modify: `ferro-spec-v0.2.md:230,:585` + §22.2 (i)

**Interfaces:**
- Consumes: `ferro_proto::messages::PoolInfo` and `handshake::hello_ack_frame(request_id, epoch, Vec<PoolInfo>)` (Task 11).
- Produces: `PoolRegistry::pool_info(&self) -> Vec<PoolInfo>` — **async**, bounded as a whole, filling `server_version`; and `PoolRegistry::probes_issued(&self) -> u64`, a monotonic counter that makes the caching claim observable.

**The constraints that decide the design.**

1. **`ferrod` boots with unreachable DBs today** and must keep doing so — `Pool::new` is lazy (`ferrod/src/pools.rs:138-142`), there is no warmup or `min_size` (`ferro-pool/src/health.rs:17`), and `PoolRegistry::build` dials nothing. So the version can **not** be learned at build time.
2. **A driver needs it deterministically at connect time**, so "whenever some request happens to check out a connection" is not good enough either.
3. **The probe SITS ON THE HANDSHAKE CRITICAL PATH, inside the client's own I/O deadline (probe 2, blocker B7).** `Ferro::connect` defaults `$ioTimeout = 5.0` s (`Ferro.php:42`) and applies it to the `HELLO_ACK` read (`Transport.php:74-77`). The v1 design probed pools **serially** at 2 s each, so **three unreachable pools = 6 s > 5 s** and `Ferro::connect` fails outright — destroying the exact property this task claims to preserve. Worse, v1 deliberately did not cache failures, so **every** handshake paid it for as long as a backend was down; and `OnceCell::get_or_try_init` **serialises** concurrent initialisers, so an FPM reconnect storm after a `boot_epoch` change (§19.1) would queue one probe at a time.
4. **A learned version must not be immortal.** A `OnceCell` sealed on success holds for the daemon's life, so a rolling backend upgrade leaves `ferrod` advertising a stale version that a DBAL driver uses to pick a **platform** — the wrong grammar for the server it is talking to, silently.

**The resolution.** Learn it lazily, at the first handshake that asks; probe **all pools CONCURRENTLY**; bound the **whole** `pool_info()` call, not each pool separately; cache a success with a **TTL** rather than forever; and cache a *failure* briefly (a negative backoff) so a down backend costs one probe per backoff window instead of one per handshake. If a pool's version is not known, the field is `nil` and the handshake still succeeds — the handshake never depends on backend availability, in any state.

`SELECT version()` works verbatim on PostgreSQL, MySQL and MariaDB (function names are case-insensitive in the MySQL family), so no per-backend method is needed: it goes through the ordinary `Checkout::query`, which means the assist lexer, the RFQ read and the taint bookkeeping all run exactly as they do for any user SELECT. (Measured: it returns `Value::Text` on all three and does **not** taint.)

- [ ] **Step 1: Write the failing live test, including the unreachable-pool case**

`engine/crates/ferrod/tests/hello_meta_it.rs`. **The one new harness piece this task needs (hazard 63):** `common::exec_server` builds a ONE-pool registry, so add its N-pool sibling next to it, in the same style — no `ferrod` process is spawned by any of these helpers, they build the registry in-process and run the real `Session::run_with_handler`, which is exactly what `exec_server` already does:

```rust
/// A live `ferrod` session server over a registry of N named pools. The N-pool sibling of
/// [`exec_server`], for the M1-S8a `HELLO_ACK` metadata gates: `kind` is inferred per pool from its
/// own DSN scheme, so one call can mix a Postgres and a MySQL pool.
///
/// Returns the `TestServer` AND the `Arc<PoolRegistry>`, because the metadata tests assert on
/// `registry.probes_issued()` — the counter that makes "learned once" observable rather than merely
/// plausible.
pub fn pools_server(pools: &[(&str, &str)]) -> (TestServer, Arc<PoolRegistry>) {
    let config = Config {
        pools: pools
            .iter()
            .map(|(name, dsn)| PoolSpec {
                name: (*name).to_string(),
                dsn: (*dsn).to_string(),
                kind: ferrod::config::infer_pool_kind(dsn),
                pin_functions: Vec::new(),
                pin_on_unknown: true,
            })
            .collect(),
        ..Config::default()
    };
    let registry = PoolRegistry::build(&config);
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry.clone(),
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    let server = TestServer::spawn_with_factory_and_config(
        BootEpoch(1),
        config,
        registry.clone(),
        tx_registry,
        factory,
    );
    (server, registry)
}
```

The tests themselves use `TestServer::connect()` + `TestClient::hello(rid)`, which already returns `HelloResult { request_id, ack }`:

```rust
/// The metadata a DBAL driver reads at connect time, from a real multi-pool `ferrod`.
#[tokio::test]
async fn hello_ack_reports_each_pools_kind_and_server_version() {
    let (Some(pg), Some(my)) = (pg_url(), mysql_url()) else { return }; // each prints its own `skip:`
    let (server, registry) = pools_server(&[("pgpool", &pg), ("mypool", &my)]);

    let ack = server.connect().await.hello(0).await.ack;

    let pgi = ack.pools.iter().find(|p| p.name == "pgpool").expect("pgpool advertised");
    assert_eq!(pgi.kind, "postgres");
    let v = pgi.server_version.as_deref().expect("PG version must be learned");
    assert!(v.starts_with("PostgreSQL "), "PG reports its version() verbatim, got {v:?}");

    let myi = ack.pools.iter().find(|p| p.name == "mypool").expect("mypool advertised");
    assert_eq!(myi.kind, "mysql");
    let v = myi.server_version.as_deref().expect("MySQL version must be learned");
    assert!(
        v.chars().next().is_some_and(|c| c.is_ascii_digit()),
        "MySQL's version() starts with a digit, got {v:?}"
    );

    // ---- Learned ONCE. THIS is the caching assertion, and it is deliberately NOT
    // `assert_eq!(ack2.pools, ack.pools)`: that proves STABILITY, not caching — a registry that
    // re-probed on every handshake would pass it (probe 2, weak guard 2). The probe COUNTER is the
    // observable, so a lost cache goes RED.
    let after_first = registry.probes_issued();
    assert_eq!(after_first, 2, "one probe per pool on the first handshake");

    let ack2 = server.connect().await.hello(0).await.ack;
    assert_eq!(ack2.pools, ack.pools, "the metadata must be stable across handshakes");
    assert_eq!(
        registry.probes_issued(),
        after_first,
        "a second handshake inside the TTL must issue NO new probe — the cache is real, not just \
         a stable answer"
    );
}

/// MariaDB's `version()` must CONTAIN "mariadb" (case-insensitively) — that substring is how a
/// Doctrine driver selects the MariaDB platform branch, so the engine passing the string through
/// VERBATIM (rather than normalising it) is load-bearing.
#[tokio::test]
async fn hello_ack_mariadb_version_is_distinguishable_from_mysql() {
    let Some(url) = mariadb_url() else { return }; // prints `skip: FERRO_TEST_MARIADB_URL unset`
    let (server, _registry) = pools_server(&[("maria", &url)]);
    let ack = server.connect().await.hello(0).await.ack;
    let v = ack.pools[0].server_version.clone().expect("version");
    assert!(v.to_ascii_lowercase().contains("mariadb"), "got {v:?}");
}

/// THE safety property: the handshake NEVER depends on backend availability, and it never blows the
/// client's I/O deadline. `ferrod` handshakes with unreachable pools; their version is `nil`.
///
/// The THREE dead pools are the point (probe 2, blocker B7): `Ferro::connect`'s default `ioTimeout`
/// is 5 s and covers the HELLO_ACK read, so a per-pool bound with serial probing would take 3× the
/// per-pool timeout and fail the connect. The whole call is bounded and the pools are probed
/// concurrently, so this handshake completes in roughly ONE budget regardless of how many are down —
/// which is what the elapsed-time assertion pins.
#[tokio::test]
async fn unreachable_pools_still_handshake_with_a_null_version_and_do_not_blow_the_io_deadline() {
    let Some(pg) = pg_url() else { return };
    // Port 1 is reserved and refuses instantly on loopback; 10.255.255.1 black-holes, so the two
    // dead pools cover BOTH failure shapes — fast refusal and hang-until-budget.
    let refused = "postgres://ferro:ferro@127.0.0.1:1/ferro";
    let blackhole_a = "postgres://ferro:ferro@10.255.255.1:5432/ferro";
    let blackhole_b = "postgres://ferro:ferro@10.255.255.2:5432/ferro";
    let (server, _registry) = pools_server(&[
        ("live", &pg),
        ("dead1", refused),
        ("dead2", blackhole_a),
        ("dead3", blackhole_b),
    ]);

    let started = std::time::Instant::now();
    let ack = server.connect().await.hello(0).await.ack;
    let elapsed = started.elapsed();

    let live = ack.pools.iter().find(|p| p.name == "live").unwrap();
    assert!(live.server_version.is_some(), "a reachable pool still reports its version");
    for name in ["dead1", "dead2", "dead3"] {
        let d = ack.pools.iter().find(|p| p.name == name).unwrap();
        assert_eq!(d.kind, "postgres", "the KIND is known from the DSN scheme regardless");
        assert_eq!(d.server_version, None, "an unreachable pool reports nil, it does not fail");
    }
    assert!(
        elapsed < std::time::Duration::from_secs(4),
        "THREE unreachable pools must cost roughly ONE probe budget, not three: the client's \
         default ioTimeout is 5s and covers this read. Took {elapsed:?}"
    );
}

/// A failed probe is remembered for a SHORT window, not forever — so a backend that comes back is
/// picked up without a daemon restart, and a backend that stays down does not cost every handshake
/// a full probe budget.
#[tokio::test]
async fn a_failed_probe_is_retried_after_the_backoff_and_never_sealed() {
    let Some(pg) = pg_url() else { return };
    let (server, registry) = pools_server(&[("dead", "postgres://ferro:ferro@127.0.0.1:1/ferro")]);
    let _ = pg; // the reachable DSN is only needed to gate the live lane

    let a = server.connect().await.hello(0).await.ack;
    assert_eq!(a.pools[0].server_version, None);
    let after_first = registry.probes_issued();
    assert_eq!(after_first, 1);

    // Inside the backoff: NO new probe (this is what stops a down backend costing every handshake).
    let _ = server.connect().await.hello(0).await;
    assert_eq!(
        registry.probes_issued(),
        after_first,
        "a failure inside the backoff window must not re-probe"
    );

    // After it: a new probe IS issued. `VERSION_RETRY_BACKOFF` is 5s; sleep just past it.
    tokio::time::sleep(std::time::Duration::from_millis(5_200)).await;
    let _ = server.connect().await.hello(0).await;
    assert!(
        registry.probes_issued() > after_first,
        "a failure must NEVER be sealed — after the backoff the probe must run again"
    );
}
```

- [ ] **Step 2: Run and watch it fail**

```
FERRO_TEST_PG_URL=… FERRO_TEST_MYSQL_URL=… cargo test -p ferrod --test hello_meta_it -- --nocapture
```
Expected: FAIL — `server_version` is `None` for every pool (Task 11 emits `None` unconditionally).

- [ ] **Step 3: Add the per-pool cache and the probe**

`engine/crates/ferrod/src/pools.rs`:

```rust
/// The budget for the WHOLE `pool_info()` call — every pool's probe together, not each one.
///
/// **This bound is the one that matters (probe 2, blocker B7).** The probe runs inside the client's
/// own I/O deadline: `Ferro::connect` defaults `ioTimeout` to 5 s and applies it to the HELLO_ACK
/// read, so a PER-POOL bound of 2 s with SERIAL probing means three unreachable pools take 6 s and
/// `Ferro::connect` fails — the exact property this whole design exists to preserve. Bounded here,
/// N unreachable pools cost the same as one.
const VERSION_PROBE_BUDGET: Duration = Duration::from_millis(1_500);

/// How long a learned version is trusted before it is re-probed.
///
/// Not `OnceCell`-forever: a rolling backend upgrade would otherwise leave `ferrod` advertising a
/// version from before the restart for the daemon's entire life, and a DBAL driver picks a PLATFORM
/// (i.e. which SQL grammar it emits) from that string. Ten minutes is far longer than a handshake
/// storm and far shorter than an upgrade window.
const VERSION_TTL: Duration = Duration::from_secs(600);

/// How long a FAILED probe is remembered before trying again.
///
/// v1 cached nothing on failure, which sounds safe and is not: while a backend is down, EVERY
/// handshake pays a full probe timeout. A short negative cache makes a down backend cost one probe
/// per window instead of one per connection, while still recovering within seconds of it coming back.
const VERSION_RETRY_BACKOFF: Duration = Duration::from_secs(5);

/// What is currently known about one pool's server version.
#[derive(Clone)]
enum VersionState {
    /// Never probed, or the last attempt's backoff has expired.
    Unknown,
    /// Learned at `at`; trusted until `at + VERSION_TTL`.
    Known { version: String, at: Instant },
    /// Failed at `at`; not retried until `at + VERSION_RETRY_BACKOFF`.
    Failed { at: Instant },
}

/// One resolved pool plus the metadata `HELLO_ACK` advertises for it.
struct PoolEntry {
    pool: AnyPool,
    kind: &'static str,
    /// A plain `Mutex<VersionState>` rather than a `OnceCell`, for three reasons the OnceCell could
    /// not give us: it can EXPIRE (a rolling upgrade must not pin a stale version for the daemon's
    /// life), it can remember a FAILURE for a backoff window, and it never serialises callers behind
    /// an in-flight initialiser — `OnceCell::get_or_try_init` queues concurrent initialisers, which
    /// under an FPM reconnect storm after a `boot_epoch` change turns N handshakes into N sequential
    /// probes. The lock is held only to read/write the small state, never across the probe await.
    version: std::sync::Mutex<VersionState>,
}
```

`PoolRegistry` itself gains the counter beside `by_name` (`pools.rs:48-50`), and `build` initialises
it to zero:

```rust
pub struct PoolRegistry {
    by_name: HashMap<String, PoolEntry>,
    /// Monotonic count of version probes ISSUED. See [`PoolRegistry::probes_issued`] — it exists so
    /// the "learned once" claim can be asserted rather than assumed.
    probes_issued: std::sync::atomic::AtomicU64,
}
```

```rust
impl PoolRegistry {
    /// The advertised metadata for every pool, learning any not-yet-known server version.
    ///
    /// **Why here and not at build time:** `ferrod` boots with unreachable backends today (pools are
    /// LAZY — `Pool::new` dials nothing and there is no warmup), and that is a property worth
    /// keeping. **Why not at first checkout:** a session may handshake before any pool has ever been
    /// used, and a driver needs the value deterministically at connect time.
    ///
    /// **Bounded as a WHOLE and probed CONCURRENTLY**, because this call sits on the handshake
    /// critical path inside the client's `ioTimeout` (5 s by default). Any pool that has not
    /// answered when the budget expires simply reports `None` for this handshake; nothing fails.
    ///
    /// A probe failure (unreachable backend, timeout, an unexpected row shape) yields `None` for
    /// that pool and is remembered only for `VERSION_RETRY_BACKOFF`. The handshake itself never
    /// fails because of it, in any state.
    pub async fn pool_info(&self) -> Vec<PoolInfo> {
        // Probe every pool CONCURRENTLY under ONE budget. `join_all` inside a single `timeout` is
        // the whole point: N unreachable pools cost what one does.
        let probes = self.by_name.iter().map(|(name, entry)| async move {
            (name.clone(), entry.kind, entry.version(&self.probes_issued).await)
        });
        let results = match timeout(VERSION_PROBE_BUDGET, futures::future::join_all(probes)).await {
            Ok(r) => r,
            Err(_elapsed) => {
                // The budget expired. Report what is already CACHED (free, no I/O) and `None` for
                // the rest; the next handshake tries again.
                tracing::debug!("ferrod: server-version probe budget expired; advertising cached values only");
                self.by_name
                    .iter()
                    .map(|(name, entry)| (name.clone(), entry.kind, entry.cached_version()))
                    .collect()
            }
        };

        let mut out: Vec<PoolInfo> = results
            .into_iter()
            .map(|(name, kind, server_version)| PoolInfo {
                name,
                kind: kind.to_string(),
                server_version,
            })
            .collect();
        // Deterministic order: `by_name` is a HashMap, and a handshake that reports pools in a
        // different order on every connection is needlessly untestable.
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// How many version probes this registry has ISSUED since boot.
    ///
    /// Exists purely so the caching claim is OBSERVABLE. "The second handshake reports the same
    /// string" proves stability, not caching — a design that re-probed every time would pass it
    /// (probe 2, weak guard 2). This counter is what the live test asserts on.
    pub fn probes_issued(&self) -> u64 {
        self.probes_issued.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl PoolEntry {
    /// The cached version, WITHOUT probing — used when the budget has already expired.
    fn cached_version(&self) -> Option<String> {
        match &*self.version.lock().expect("version state mutex") {
            VersionState::Known { version, at } if at.elapsed() < VERSION_TTL => Some(version.clone()),
            _ => None,
        }
    }

    /// The version, probing if nothing usable is cached.
    async fn version(&self, probes: &std::sync::atomic::AtomicU64) -> Option<String> {
        // 1. Decide under the lock, then DROP it — never hold a std Mutex across an await.
        {
            match &*self.version.lock().expect("version state mutex") {
                VersionState::Known { version, at } if at.elapsed() < VERSION_TTL => {
                    return Some(version.clone());
                }
                VersionState::Failed { at } if at.elapsed() < VERSION_RETRY_BACKOFF => {
                    return None;
                }
                _ => {}
            }
        }

        // 2. Probe. Concurrent callers may both probe here — deliberately: a duplicate `SELECT
        // version()` is far cheaper than serialising every handshake behind one initialiser.
        probes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let probed = probe_version(&self.pool).await;

        // 3. Record.
        let mut state = self.version.lock().expect("version state mutex");
        *state = match &probed {
            Some(v) => VersionState::Known { version: v.clone(), at: Instant::now() },
            None => {
                tracing::debug!(kind = %self.kind, "ferrod: server-version probe failed");
                VersionState::Failed { at: Instant::now() }
            }
        };
        probed
    }
}

/// `SELECT version()` works VERBATIM on PostgreSQL, MySQL and MariaDB (function names are
/// case-insensitive in the MySQL family), so no per-backend method is needed. It goes through the
/// ordinary guarded `Checkout::query`, which means the assist lexer, the RFQ read and the taint
/// bookkeeping all run exactly as they do for a user SELECT — nothing special-cased.
///
/// The string is returned RAW: normalising it (stripping PG's leading word, extracting a
/// major.minor.patch) would bake one ecosystem's platform-selection conventions into the engine.
/// A Doctrine driver needs `mariadb` to survive in the string; that only works if nothing rewrites it.
async fn probe_version(pool: &AnyPool) -> Option<String> {
    match pool {
        AnyPool::Pg(p) => probe_version_on(p).await,
        AnyPool::Mysql(p) => probe_version_on(p).await,
    }
}

async fn probe_version_on<B: PoolBackend>(pool: &Pool<B>) -> Option<String> {
    let mut co = pool.checkout().await.ok()?;
    let res = co.query("SELECT version()", &[]).await.ok()?;
    match res.rows.first()?.first()? {
        Value::Text(s) => Some(s.clone()),
        other => {
            tracing::debug!(?other, "ferrod: version() returned an unexpected cell shape");
            None
        }
    }
}
```

- [ ] **Step 4: Thread the registry to the session and make the handshake await it**

**CHOICE MADE:** `hello_ack_frame` stays **synchronous** and keeps taking `Vec<PoolInfo>`; the caller awaits `registry.pool_info()` and hands the result in. The alternative (pass `&PoolRegistry` and await inside) would drag pool internals into `handshake.rs` and make the only pure, unit-testable function in the handshake path async for no gain.

`engine/crates/ferrod/src/session/mod.rs:342-346` — the single call site becomes:

```rust
        // Learned lazily per pool, CONCURRENTLY, bounded as a whole by VERSION_PROBE_BUDGET, and
        // never fatal: a pool whose backend is unreachable (or slow) advertises
        // `server_version: nil` and the handshake still completes. The budget is deliberately well
        // under the client's default 5 s `ioTimeout`, which covers the HELLO_ACK read.
        let pools = pool_registry.pool_info().await;
        let ack = handshake::hello_ack_frame(first.header.request_id, epoch, pools);
```

**The parameter has to come from somewhere, and that is the plumbing hazard 59 describes.** `Session::run_with_handler` gains `pool_registry: Arc<PoolRegistry>`, which cascades to exactly the files listed above. Two of them are worth spelling out:

```rust
// engine/crates/ferrod/src/session/mod.rs — the pool-less convenience entry.
    pub async fn run(stream: UnixStream, config: Config, epoch: BootEpoch) {
        let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
        // Its own registry, exactly as it already mints its own throwaway TxRegistry. `Config`s used
        // on this path carry no pools, so this builds an empty registry and dials nothing — and if
        // one ever does carry pools, `Pool::new` is lazy, so it still dials nothing until asked.
        let pool_registry = PoolRegistry::build(&config);
        let factory: HandlerFactory = Arc::new(|_session_id| default_handler_fn());
        Self::run_with_handler(stream, config, epoch, pool_registry, tx_registry, factory).await;
    }
```

```rust
// engine/crates/ferrod/src/serve.rs — one more clone per accepted connection.
pub async fn serve(
    listener: UnixListener,
    config: Config,
    epoch: BootEpoch,
    drain: Drain,
    pool_registry: Arc<PoolRegistry>,
    tx_registry: Arc<TxRegistry>,
    factory: HandlerFactory,
) {
    // ... inside the accept arm, beside `session_tx_registry`:
    let session_pool_registry = pool_registry.clone();
```

`main.rs` and `ferro-e2e/src/main.rs` need no ownership change at all: `PoolRegistry::build` already returns `Arc<Self>` (`pools.rs:57,70`), so it is `registry.clone()` into `make_handler` and `registry` into `serve`.

- [ ] **Step 5: Add the PHP live assertion — and make the harness HANDSHAKE first**

**`LiveTestCase::connect()` does not perform the handshake** (`LiveTestCase.php:87-90` returns `new Session(Transport::connectUnix(...))` and nothing calls `hello()`; the only `->hello()` in the file is inside `waitUntilReady`'s probe loop at `:177`). `Session::pools()` has no "did you handshake?" guard — it just returns its `private array $pools = []` — so a test that reads pool metadata off a fresh `connect()` gets `[]` and **fails for the wrong reason** (probe 2, major). Fix the harness rather than the test:

```php
    /**
     * Connect a fresh {@see Session} to this test's running ferrod over its UDS socket, HANDSHAKEN.
     *
     * The handshake is not optional bookkeeping: `HELLO_ACK` is where the session learns
     * `boot_epoch`, the advertised pools and (since M1-S8a) each pool's kind and server version. A
     * `Session` that never handshook reports an EMPTY pool list, so a test reading metadata off one
     * fails for a reason that has nothing to do with what it is testing.
     */
    protected function connect(): Session
    {
        $session = new Session(Transport::connectUnix($this->socketPath, 2.0, 5.0));
        $session->hello();
        return $session;
    }
```

Check the existing `tests/Live` callers of `connect()` after this change: any that relied on an
un-handshaken session (a test asserting the handshake itself, for instance) must switch to an
explicit `new Session(...)` so the change does not silently alter what they exercise.

`php/client/tests/Live/PoolMetadataLiveTest.php`:

```php
<?php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Session;

final class PoolMetadataLiveTest extends LiveTestCase
{
    public function testHandshakeAdvertisesEachPoolsKindAndVersion(): void
    {
        $this->requireMysqlPool();
        // `connect()` handshakes (see LiveTestCase) — without that, `poolInfo()` is [] and every
        // assertion below fails for a reason unrelated to what is being tested.
        $session = $this->connect();
        $this->assertInstanceOf(Session::class, $session);
        $this->assertNotSame([], $session->poolInfo(), 'the session must have handshaken');

        $byName = [];
        foreach ($session->poolInfo() as $p) {
            $byName[$p->name] = $p;
        }
        $this->assertArrayHasKey('default', $byName);
        $this->assertArrayHasKey(self::MYSQL_POOL, $byName);

        $this->assertSame('postgres', $byName['default']->kind);
        $this->assertStringStartsWith('PostgreSQL ', (string) $byName['default']->serverVersion);

        $this->assertSame('mysql', $byName[self::MYSQL_POOL]->kind);
        $this->assertNotNull($byName[self::MYSQL_POOL]->serverVersion);

        // The name-only accessor is unchanged for existing callers.
        $this->assertEqualsCanonicalizing(['default', self::MYSQL_POOL], $session->pools());
    }
}
```

- [ ] **Step 6: Run, then MUTATION-PROVE**

```
FERRO_TEST_PG_URL=… FERRO_TEST_MYSQL_URL=… FERRO_TEST_MARIADB_URL=… \
  cargo test -p ferrod --test hello_meta_it -- --nocapture
(cd php/client && ./vendor/bin/phpunit tests/Live/PoolMetadataLiveTest.php)
```
Mutate — **all of these are REQUIRED, none is optional** (probe 2, weak guard 3: v1's mutation 1 said "if that scenario is impractical, instead…", which is how a guard quietly ships unproven):

1. **Seal the failure** — change `VersionState::Failed` to be permanent (never expire the backoff) → `a_failed_probe_is_retried_after_the_backoff_and_never_sealed` goes RED on its final assertion. This is the `OnceCell::get_or_try_init`-forever behaviour v1 prescribed, and this test is what makes it detectable.
2. **Seal the success** — remove the `VERSION_TTL` check so a `Known` state never expires → add nothing; instead run the same test with `VERSION_TTL` temporarily set to `Duration::from_millis(1)` and confirm a second handshake DOES re-probe (`probes_issued` grows). Restore the constant. This is the rolling-upgrade property: without a TTL, `ferrod` advertises a pre-restart version for its entire life and a driver picks a platform from it.
3. **Break the caching** — probe unconditionally (delete the cache read in `PoolEntry::version`) → `hello_ack_reports_each_pools_kind_and_server_version` goes RED on `probes_issued`. **Record that `assert_eq!(ack2.pools, ack.pools)` stays GREEN** under this mutation: it is the demonstration that the v1 "pays no round trip" assertion proved stability, not caching.
4. **Remove the whole-call bound** — delete the `timeout(VERSION_PROBE_BUDGET, …)` wrapper → `unreachable_pools_still_handshake_with_a_null_version_and_do_not_blow_the_io_deadline` goes RED (or hangs) on the black-holed pools. Restore.
5. **Serialise the probes** — replace `join_all` with a sequential `for` loop while keeping the whole-call bound → the same test goes RED on the elapsed-time assertion, because three dead pools now consume the budget one after another and the live pool never gets probed. This is the concrete shape of blocker B7 and the reason concurrency is not a nicety here.
6. **Normalise the version string** (strip `"PostgreSQL "`) → the PG `starts_with` goes RED while the MariaDB substring test still passes — which is exactly why the raw pass-through is asserted on both.
7. **Remove `hello()` from `LiveTestCase::connect()`** → `testHandshakeAdvertisesEachPoolsKindAndVersion` goes RED on the `assertNotSame([], …)` guard with a message that says *why*, instead of on a confusing missing-array-key further down.

Restore each.

- [ ] **Step 7: Spec truth + commit**

Amend `ferro-spec-v0.2.md:230` and `:585` (both say `HelloAck` is `[engine_version, boot_epoch, features, pools, type_registry_hash]` with names only, and that pool metadata is an S8 carry) and §22.2 (i)'s note that `naive_datetime_zone: server` "lands with that same S8 metadata" — record that the metadata now ships **kind + server version**, that the session **timezone** is still not on the wire, and that `naive_datetime_zone: server` therefore remains unimplemented.

Add a §22.2 entry `(q)` recording the three properties an operator or a future slice must not undo, because each looks like a simplification and is not: the probe is bounded **as a whole** and runs **concurrently** because it sits inside the client's `ioTimeout` (a per-pool bound with serial probing fails `Ferro::connect` at three unreachable pools); a learned version **expires** (a `OnceCell` sealed for the daemon's life makes a rolling backend upgrade advertise a stale version that a DBAL driver converts into a **platform** choice); and a failure is cached **briefly** rather than not at all (uncached, every handshake pays a full probe budget while a backend is down). Record that a probe counter (`PoolRegistry::probes_issued`) exists solely so those claims are testable, and that the handshake **never** fails because of a probe, in any state.

```bash
./ci/local-gate.sh --live
git add engine php ferro-spec-v0.2.md
git commit -m "feat(m1-s8a): learn each pool's server version lazily, concurrently, and with a TTL

ferrod knew no backend's version and pools are lazy, so it could not be learned
at build time without making boot depend on DB reachability — a property worth
keeping — and 'at first checkout' is too late for a driver that must pick a
platform at connect time. So it is learned at the first handshake that asks.

Three properties are load-bearing and none is incidental. (1) The probe sits on
the handshake critical path INSIDE the client's own ioTimeout (5s by default,
covering the HELLO_ACK read), so all pools are probed CONCURRENTLY under ONE
budget: N unreachable pools cost what one does, where a per-pool bound with
serial probing would have failed Ferro::connect outright. (2) A success expires
after a TTL rather than sealing for the daemon's life — a rolling backend upgrade
must not leave ferrod advertising a pre-restart version that a driver turns into
a platform choice. (3) A failure is remembered only for a short backoff, so a
down backend costs one probe per window instead of one per handshake, and a
backend that comes back is picked up without a restart. A probe counter makes
all three assertable rather than plausible.

This is also where Arc<PoolRegistry> is finally threaded through serve() and
Session::run_with_handler — the session had no access to it before.

SELECT version() works verbatim on all three engines, so the probe goes through
the ordinary guarded Checkout::query with no per-backend method. The string is
advertised RAW: normalising it would bake one ecosystem's platform-selection
rules into the engine, and a Doctrine driver needs 'mariadb' to survive in it.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Slice close-out

After Task 12, before declaring M1-S8a done:

- [ ] `./ci/local-gate.sh --live` green from a clean tree, with all three backends up.
- [ ] `git log --oneline` shows twelve commits, each independently green.
- [ ] Re-read `ferro-spec-v0.2.md` §7, §9, §9.1, §14 and §22.2 end to end and confirm every sentence is still true — this slice touched all five. In particular: §22.2 entries `(g)`, `(h)`, `(k)(2)`, `(k)(3)` and `(k)(4)` must all be marked closed, and `(n)`, `(o)`, `(p)`, `(q)` must exist.
- [ ] Confirm every guard this slice added went RED under at least one recorded mutation. The mutation observations that must appear in the commit or PR body, because each one records a guard that WOULD NOT have failed: Task 4's `every_variant()` length assertion staying green over a deleted variant; Task 5's lockstep proof staying green over a half-applied domain unwrap when the fixture lacks `dom_text`/`dom_bool`/`dom_bytea`; Task 6's live suite staying green over a broken standalone `MYSQL_TYPE_ENUM` arm; Task 12's `assert_eq!(ack2.pools, ack.pools)` staying green over a registry that re-probes every handshake.
- [ ] Confirm the S8b carry list is what remains: the `ferro/doctrine-dbal-driver` package itself (`Driver`/`Connection`/`Statement`/`Result` + the `ExceptionConverter` + `IndeterminateWriteException` + the driver-side temporal conversion, since DBAL's stock format strings reject canonical `datetimetz` for **every** value and fractional `time`).
- [ ] Confirm what S8a deliberately did **not** close, and that each has a §22.2 line: MySQL `query_stream` (n); the tracker-clean hygiene `None`-skip (R2); PHP-reachable named isolation constants (p); `naive_datetime_zone: server` (i); and the `DISCARD ALL` typeinfo-cache defect (m), which S8b's schema-manager traffic will meet.
