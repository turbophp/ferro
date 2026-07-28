# Ferro — A Rust Database Access Engine for PHP

**Status:** Draft v0.2 — implementation handoff · 2026-07-23
**Audience:** implementing agent (Claude Code) and maintainer review
**Scope:** engine daemon (`ferrod`), wire protocol, PHP clients (native, Doctrine DBAL, Eloquent), `ferro` CLI, packaging
**Supersedes:** v0.1. Deltas listed in §22.

---

## 0. How to read this document

- **MUST / SHOULD / MAY** are normative (RFC 2119 sense).
- §21 is the **decision log**. Decisions there are settled — do not re-litigate them in code review or refactors. Open items carry an interim default so nothing blocks implementation.
- Build in milestone order (§17). M0 has an explicit task order (§17.1); start there.
- The machine-readable protocol registry in `/proto` (§20.2) is the single source of truth for method ids, flags, error codes, and type tags. Both the Rust and PHP codecs generate constants from it. Any protocol change MUST update the registry, the golden vectors, and both codecs in the same change set.
- Where this spec is silent, prefer: correctness over throughput, explicit configuration over inference, and the smallest surface that turns the acceptance suites green.

## 1. Problem statement

PHP-FPM is shared-nothing: every worker owns its own database connections, prepared-statement caches, and TLS sessions. 200 workers means 200 upstream connections and zero cross-worker reuse. PDO, designed in 2004, compounds this: synchronous only, stringly typed, connection-oriented, and inconsistent across drivers.

Ferro is **not a database**. It is a per-host access engine: one Rust daemon that owns all upstream connections, pools them in transaction mode, multiplexes many PHP workers over a local socket, and exposes a modern typed API — while remaining a drop-in replacement for Doctrine DBAL and Laravel Eloquent so existing applications adopt it by changing configuration only.

**Prior art and positioning.** PgBouncer proves host-level transaction pooling and shares the session-state detection limits that §7.4 confronts explicitly. Prisma ran a Rust query engine out-of-process and later retreated in-process for operational reasons — but Prisma's engine served a single Node client and had no cross-process pooling upside; Ferro's entire point is N workers sharing one daemon, which is exactly the case where out-of-process pays. sqlx supplies the model for §11's compile-time checked queries. ProxySQL/RDS Proxy demonstrate the ops value of a protocol-aware middle tier; Ferro differs by owning **both** ends of the wire (client library and daemon), which is what makes tx-id pinning, typed hydration, and the manifest handshake possible.

## 2. Goals

1. **G1 — Pooling:** N workers share M real connections (M ≪ N) with transaction-mode pooling, correct pinning, and hygiene on release.
2. **G2 — Drop-in:** Doctrine DBAL driver and Laravel/Eloquent connector requiring only config changes. Acceptance: upstream test suites pass (§14, §15).
3. **G3 — Typed:** One canonical type system across backends; native API hydrates readonly PHP DTOs.
4. **G4 — Concurrency:** Request multiplexing; PHP Fibers enable parallel queries from one request. Sync fallback works everywhere.
5. **G5 — Checked SQL:** Build-time verification of queries against a shadow schema + DTO/stub codegen (sqlx-style).
6. **G6 — Observability & security:** Tracing, pool metrics, slow log; credentials isolated in the engine; optional manifest-only query mode.
7. **G7 — Predictable failure:** Engine restarts and backend flaps surface as a small, typed error set with defined client recovery (§9.2, §19). No silent ambiguity about the fate of a write, ever.

## 3. Non-goals

- Not a storage engine, not a SQL rewriter, not a cache of query results.
- No ORM in Rust: identity maps, lazy proxies, object graphs, PHP-closure middleware stay in PHP. The engine's job ends where live object semantics begin.
- No read/write inference from SQL text for replica routing in v1 — routing is explicit (§7.6).
- No transparent engine-side retry of user statements — the engine reports outcomes truthfully; retry is client policy (§19.3).
- Oracle deferred (blocking OCI wrapper on a `spawn_blocking` pool, post-v1). Windows native is TCP-only best-effort (§21 D4).

## 4. Architecture overview

```
┌────────────── host ──────────────────────────────────────────┐
│  PHP-FPM workers (N)                                          │
│   ├─ ferro/client        (native typed API, Fibers-aware)     │
│   ├─ ferro/doctrine-dbal (Driver impl → client)               │
│   └─ ferro/laravel       (Illuminate Connection → client)     │
│              │  UDS /run/ferro/{build-hash}.sock              │
│              ▼                                                │
│  ferrod (Rust, tokio)                                         │
│   ├─ session layer: handshake (epoch), auth, multiplexing,    │
│   │                 flow control, PING/PONG                   │
│   ├─ SQL service: param bind, result framing                  │
│   ├─ TX service: pin management, savepoints, retry hints      │
│   ├─ pin engine: protocol signals + assist lexer (§7.2)       │
│   ├─ pools: pg | mysql | sqlite | mssql  (+ replicas)         │
│   ├─ codegen manifest store (query_id → plan)                 │
│   └─ admin/metrics: OTLP, Prometheus, slow log                │
└──────────────────────────────────────────────────────────────┘
```

One `ferrod` per host (or per pod). Clients hold one UDS connection per worker; all requests are multiplexed over it. Transactions pin engine-side to a `tx_id`, not to the client socket, so a Fiber-suspended request keeps its transaction across frames.

## 5. Transport and framing

**Transport:** Unix domain socket, path versioned by build: `/run/ferro/{schema_hash}.sock`. Deploys with changed contracts spawn a fresh daemon instance; the old one drains and exits on idle TTL (§18). TCP loopback fallback (`FERRO_ADDR`) for containers without shared sockets and for Windows dev (D4).

**Frame header** — 16 bytes, little-endian, followed by a MessagePack payload:

| field | type | notes |
|---|---|---|
| magic | u8 | `0xF7` |
| version | u8 | protocol major |
| flags | u16 | `STREAM`, `END`, `CANCEL`, `OOB_FD`, `COMPRESSED` (reserved, D11) |
| service | u16 | `01` core · `02` sql · `03` tx · `04` stream · `05` admin |
| method | u16 | per-service method id — registry in `/proto/methods.toml` |
| request_id | u32 | client-assigned, multiplexing key (§5.2) |
| payload_len | u32 | MessagePack body length |

**Handshake:** client `HELLO {client_version, type_registry_hash, manifest_hash?, pid, features}` → server `HELLO_ACK {engine_version, boot_epoch, features, pools[], type_registry_hash}`.

- `boot_epoch` (u64): unique per daemon start. Clients cache it; a changed epoch on reconnect voids all session assumptions (§19).
- A type-registry mismatch is a **hard error** (forces regen/redeploy) — this is the versioning story.
- Feature bits (engine): `MEMFD`, `LISTEN_STREAMS`, `MANIFEST`. Feature bits (client): `MEMFD_RX` (able to receive fds, §5.1), `FIBERS` (informational). Unknown bits MUST be ignored.

### 5.1 Large payloads (memfd) — honest version

Results or params above a threshold (default 1 MiB) MAY move out-of-band: the engine writes into a **sealed** `memfd` (`F_SEAL_SHRINK|GROW|WRITE|SEAL`), passes the fd via `SCM_RIGHTS`, and the frame carries `{fd_index, len, encoding}` with flag `OOB_FD`.

What this buys, precisely: **engine-side** zero-copy and strictly bounded engine memory. On the PHP side there is **one copy**: the client receives the fd via `ext-sockets` (`socket_import_stream` on the UDS stream + `socket_recvmsg` with `SCM_RIGHTS`) and reads it through `php://fd/N` — PHP userland cannot mmap. True client-side zero-copy arrives only with the optional native accelerator (D12). Requirements and gating:

- The path is Linux-only and REQUIRES the client to have advertised `MEMFD_RX` in HELLO (which the client only sets when `ext-sockets` is loaded). Otherwise the engine MUST fall back to inline credit-based streaming. No behavioral difference is observable other than throughput.
- Row streaming (§10) remains the default for cursors; memfd is for bulk single-shot payloads (COPY, large JSON/bytea).

### 5.2 Session and stream rules (normative)

- **request_id:** client-assigned u32. An id is *in-flight* from the first frame the client sends with it until the client has received a frame carrying `END` for it. Ids MUST NOT be reused while in-flight; clients SHOULD allocate monotonically and wrap. The engine MUST reject a reused in-flight id with a `Protocol` error frame on that id and MUST NOT disturb the original request.
- **Ordering:** frames for a given request_id are delivered in order; interleaving across ids is arbitrary.
- **Termination invariant:** every request is terminated by exactly one frame carrying `END` (success payload, error payload, or cancelled payload). All client state machines key off this.
- **CANCEL:** client MAY send `CANCEL {request_id}` for any in-flight id; it is advisory and idempotent. The engine propagates a backend cancel (PG cancel key, MySQL `KILL QUERY`, MSSQL attention signal) best-effort. If the request already completed, CANCEL is a no-op. The client MUST tolerate ordinary result frames arriving after it sent CANCEL, up to the terminal END — which reports either `Cancelled` or the raced-to-completion result. See §9.2 for the write-fate rule.
- **Flow control:** server→client streams are credit-based **per request**: default window 64 frames / 4 MiB, replenished via `WINDOW_UPDATE {request_id, frames, bytes}`. Additionally a per-session aggregate cap (default 16 MiB) bounds total buffered engine output; the engine never buffers unbounded results.
- **Liveness:** core `PING`/`PONG`. Clients SHOULD ping after 30 s idle when awaiting nothing; a missed PONG deadline (2× interval) marks the connection dead and enters the reconnect loop (§19.2). `GOODBYE` announces graceful client close so the engine can distinguish drain from death.

## 6. Core SQL service

`EXEC` request:

```
{ pool, sql | query_id, params: [TypedValue], options: {
    timeout_ms?, readonly?, fetch: rows|stream|none,
    isolation?  (only meaningful inside tx),
    trace?: {traceparent} } }
```

`EXEC` response:

```
{ cols: [{name, type}], rows: [[TypedValue]] | stream_ref,
  affected: u64, last_insert_id?: TypedValue,
  warnings: [], stats: {queue_us, exec_us, rows, bytes} }
```

`queue_us` vs `exec_us` is deliberately first-class: pool wait time is the KPI that tells operators to grow `max_size` (§16).

`TX` service: `BEGIN {pool, isolation?, readonly?} → tx_id`, then `EXEC {tx_id, …}`, `SAVEPOINT`, `RELEASE`, `ROLLBACK_TO`, `COMMIT`, `ROLLBACK`. Engine-side deadlines: `idle_in_tx_timeout` (default 10 s) and `max_tx_duration` (default 60 s) roll back and release the pin, returning `Retryable{TxDeadline}`. Interaction with engine restart and write-fate rules: §19.

## 7. Pooling model

Per-pool mode: **transaction** (default), **session** (always pinned; the escape hatch), **statement** (rare, autocommit-only).

### 7.1 Pin architecture: protocol signals first, lexer assist second

Pin decisions are made **post-execution from backend protocol signals** wherever the backend can report state, with a lightweight statement lexer covering only what is text-visible but protocol-invisible. This inverts v0.1, which over-trusted the lexer.

**PostgreSQL.** The `ReadyForQuery` status byte (`I`/`T`/`E`) is authoritative for transaction state — pin/unpin for tx purposes keys off it, not off lexing `BEGIN`. `ParameterStatus` covers only `GUC_REPORT` parameters (`search_path` is *not* among them) and is treated as an assist signal, never as authority.

**MySQL / MariaDB.** Connection setup MUST enable the session trackers: `session_track_system_variables='*'`, `session_track_state_change=ON`, `session_track_transaction_info=CHARACTERISTICS`, with `CLIENT_SESSION_TRACK` negotiated. Pin decisions then come from OK-packet tracker payloads, which report session mutations at the server — by design this covers mutations performed *inside stored programs*. (Verification test required in M1: assert tracker fires for `SET SESSION` executed within a procedure. If it does not, MySQL falls back to the conservative rules below.) A connection whose trackers reported no mutation and which never pinned is **known clean**.

**Assist lexer** (keyword classifier, not a parser) covers, and pins on: `LISTEN`/`UNLISTEN`; advisory-lock function calls in top-level SQL (`pg_advisory_lock` family without `_xact`); raw client `PREPARE`/`EXECUTE`/`DEALLOCATE` (engine-managed prepares are pool-safe and namespaced, §7.5); temp-table DDL; non-local `SET` as a backstop (PG: anything not `SET LOCAL`; MySQL: `SET SESSION` — normally the tracker catches these first); SQLite `ATTACH`/state-changing `PRAGMA`. Unknown/unclassifiable statements pin conservatively (`pin_on_unknown = true` default).

**Escape hatch:** per-pool `pin_functions = ["app_acquire_lock", …]` — statements whose text references a listed function pin for the session. This is the supported path for applications whose own server-side functions mutate session state (§7.4).

### 7.2 Hygiene: at checkout, pipelined, conditional

Hygiene runs at **checkout**, pipelined ahead of the first user statement — the round trip hides behind the user's own query instead of being paid at release. Rules:

- A connection that was **pinned** during its last lease is tainted: it gets the full reset (PG `DISCARD ALL`; MySQL `COM_RESET_CONNECTION`; MSSQL reset-on-next-RPC flag, see D1). Note both are destructive to prepared statements — accepted for tainted connections.
- A **never-pinned** PG connection gets the targeted profile: `RESET ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES;` — this preserves the engine's namespaced prepared statements. (v0.1's targeted list leaked advisory locks and temp tables; this one does not. `DISCARD ALL` remains the superset.)
- A **known-clean** MySQL connection (tracker-verified) skips hygiene entirely. This is what makes the ≥ 95 % statement-cache target (§16) compatible with `COM_RESET_CONNECTION` deallocating prepares: reset only fires when something actually happened.
- SQLite: N/A (engine-owned, §7.7).

### 7.3 Statement cache

Per backend connection, LRU keyed by normalized SQL fingerprint, shared across all PHP workers by construction. PG uses named statements under an engine namespace (`__ferro_{fp}`); MySQL `COM_STMT_PREPARE`. Cache size, hit rate, and evictions exported (§13).

### 7.4 The transaction-mode contract (documented limitation)

Session state mutated **inside server-side functions/procedures** is undetectable on PostgreSQL: `set_config(…, is_local => false)`, `SET`/advisory locks executed within PL/pgSQL, never appear on the wire, and `ParameterStatus` cannot report them. No lexer fixes this; PgBouncer has the same hole. Therefore, normatively:

- Transaction mode **defines** function-side session mutation as unsupported, except for functions listed in `pin_functions`.
- Applications that need it use **session mode** for that pool.
- MySQL with trackers detects most of this class (pending the M1 verification above) and is documented as the stronger backend for transaction mode.
- User documentation MUST state this contract prominently, in the pooling chapter, with the `pin_functions` and session-mode escape hatches beside it.

### 7.5 Replicas & routing

Pools may declare `replicas = [{dsn, weight}]`; `readonly: true` on EXEC (or a read-only pool alias) routes to replicas with health/lag gating (`max_replica_lag_ms`). Ferro does **not** guess read vs write from SQL in v1 — routing is explicit from the client. Doctrine/Laravel already model read/write connections and map cleanly onto this.

### 7.6 SQLite

The engine owns the file. WAL mode, one writer serialized engine-side, many readers. This removes PHP's worst SQLite failure mode (`SQLITE_BUSY` storms under FPM) and makes SQLite a legitimate small-service backend. Online backup API exposed via admin service for snapshots.

### 7.7 Health & lifecycle

Liveness pings on idle, `max_lifetime` recycling, exponential backoff on backend outage, per-backend circuit breaker (half-open probes). `SIGTERM` → drain: refuse new checkouts, let pins finish up to `drain_deadline`, then hard-close. `SIGHUP` → config reload with pool diffing. Note the positive framing for docs: the breaker plus engine-side reconnection *shields* workers from backend flaps — a herd of FPM workers no longer stampedes a recovering database.

## 8. Backend matrix

| Backend | Crate | Notes |
|---|---|---|
| PostgreSQL | tokio-postgres | pipelining, LISTEN/NOTIFY streams, COPY via memfd, cancel keys, ReadyForQuery-driven pinning |
| MySQL / MariaDB | mysql_async | session trackers mandatory at setup (§7.1), OK-packet `last_insert_id`, conditional `COM_RESET_CONNECTION` |
| SQLite | rusqlite (+ engine writer queue) | WAL, serialized writer, online backup API |
| SQL Server | tiberius | **session mode by default** until reset-on-RPC semantics verified (D1) |
| Oracle | post-v1 | blocking OCI on `spawn_blocking` pool |

MongoDB/Redis/ClickHouse are explicitly out of the SQL service; they can later ship as sibling services on the same framing without touching this spec.

## 9. Canonical type system

One tagged value encoding on the wire; per-backend adapters map to native formats. PHP visible types below are what the native API hydrates; DBAL/Eloquent tiers apply their own ecosystem conventions on top (§14–15).

| Canonical | PG | MySQL | SQLite | MSSQL | PHP (native API) |
|---|---|---|---|---|---|
| BOOL | bool | tinyint(1)* | int | bit | `bool` |
| I64 | int2/4/8 | *int signed | integer | int/bigint | `int` |
| U64 | — | bigint unsigned | — | — | `int` or `Ferro\U64` if > PHP_INT_MAX |
| F64 | float4/8 | float/double | real | float | `float` |
| DECIMAL | numeric | decimal | text | decimal | `Ferro\Decimal` (string-backed) |
| TEXT | text/varchar | *text/varchar | text | nvarchar | `string` |
| BYTES | bytea | *blob | blob | varbinary | `string` (binary) |
| DATE / TIME | date/time | date/time | text | date/time | `Ferro\Date`, `Ferro\Time` |
| TIMESTAMP | timestamp | datetime | text | datetime2 | `DateTimeImmutable` (naive, policy §9.1) |
| TIMESTAMPTZ | timestamptz | timestamp | — | datetimeoffset | `DateTimeImmutable` (UTC instant) |
| UUID | uuid | binary(16)/char(36) | text | uniqueidentifier | `Ferro\Uuid` |
| JSON | json/jsonb | json | text | nvarchar | lazy `Ferro\Json` (decode on access) |
| ARRAY<T> | native | — | — | — | `array` |
| INTERVAL / INET / VECTOR | native | — | — | — | dedicated value objects |

*\* configurable legacy mappings (e.g. `tinyint(1)→bool` off for people who use it as a real int).*

### 9.1 Policies over guesses

Cross-driver ambiguity is resolved by explicit, pool-level policy, not silent driver behavior: `decimal: object|string`, `naive_datetime_zone: utc|server|error`, `u64_overflow: object|string|error`, `uuid: object|string`. Defaults are the safe object forms. This is where Ferro is deliberately *better* than PDO, whose per-driver casting inconsistencies are a chronic source of production bugs.

### 9.2 Normalized error taxonomy — three branches

Every backend and engine error maps to one tree, carrying the raw SQLSTATE/errno alongside:

```
Retryable      { ConnectionLost{epoch_changed: bool}, PoolTimeout, TxDeadline,
                 Deadlock, SerializationFailure, ReplicaUnavailable }
Indeterminate  { WriteUnconfirmed{cause: link_lost|timeout|engine_restart} }
NonRetryable   { Syntax, Constraint{Unique, ForeignKey, NotNull, Check},
                 Auth, QueryTimeout, Cancelled, Protocol, Unsupported }
```

**`Indeterminate` is the branch v0.1 was missing.** It is raised when a side-effecting statement was dispatched and its fate is unknown: the backend link (or the engine itself, §19) died between dispatch and response. Rules:

- Reads that die mid-flight are `Retryable{ConnectionLost}` — a read has no fate to be unsure about.
- A cancelled or timed-out **autocommit write** whose non-execution the engine could not confirm surfaces as `Indeterminate{cause: timeout}`, not `Cancelled`/`QueryTimeout`. Inside a transaction, timeout/cancel is safe: the engine rolls the tx back, and the client sees `Retryable`.
- Clients MUST NOT auto-retry `Indeterminate` unless the query's manifest entry declares `idempotent: true` (§11). Raw-SQL writes are never auto-retried.
- The engine never retries user statements transparently in any branch — it reports; the client's policy layer decides (§19.3).

DBAL's ExceptionConverter and Laravel's `QueryException` map from this one tree (§14–15), giving identical exception semantics across all backends. `Indeterminate` maps to a Ferro-specific exception class in both tiers (subclassing each ecosystem's driver-exception base) since neither ecosystem models the concept.

## 10. Native PHP client (`ferro/client`)

Pure PHP, zero required extensions; **PHP ≥ 8.2** (readonly classes). Optional accelerators, runtime-detected: `ext-msgpack` (codec hot path, `composer suggest`), `ext-sockets` (enables the `MEMFD_RX` feature, §5.1). Connection-less by design: you submit work, you never hold a connection.

```php
$db = Ferro::pool('main');

// typed hydration into readonly DTOs
$user  = $db->queryOne(User::class, 'select id, email, created_at from users where id = ?', [$id]);
$rows  = $db->query(OrderRow::class, 'select … where status = ?', [Status::Open]);

// scalars / assoc escape hatches
$count = $db->scalar('select count(*) from orders');
$assoc = $db->rows('select …');                 // array<array<string,mixed>>

// transactions are closures; scope is explicit, retry is policy
$db->transaction(function (Ferro\Tx $tx) {
    $tx->exec('update accounts set balance = balance - ? where id = ?', [$amt, $a]);
    $tx->exec('update accounts set balance = balance + ? where id = ?', [$amt, $b]);
}, Retry::onSerializationFailure(times: 3, backoff: Backoff::jitter(10, 200)));

// streaming: constant memory, credit-driven
foreach ($db->stream(Event::class, 'select … order by id') as $event) { … }

// notifications as iterators (PG)
foreach ($db->listen('jobs') as $note) { … }
```

**Hydration contract:** DTOs are `final readonly` classes with promoted constructor properties; column→property matching by name (snake→camel mapping configurable), types validated against the canonical registry at first use and memoized. No reflection per row on the fast path.

**Resilience:** the client implements the reconnect loop, epoch tracking, and retry policy of §19. The transaction closure above is the recovery surface: `Retryable` inside a closure re-runs the whole closure on a fresh connection/epoch when the policy allows.

### 10.1 Concurrency

`queryAsync()` returns a `Ferro\Future`. Under plain FPM, `await` blocks on the socket — correct everywhere, zero setup. If a Fiber scheduler is active (`Ferro\Loop::run(...)`, or Revolt/AMPHP detected), awaiting suspends the Fiber and the client multiplexes all in-flight requests over the single UDS connection:

```php
[$profile, $orders, $flags] = Ferro\await([
    $db->queryOneAsync(Profile::class, …),
    $db->queryAsync(OrderRow::class, …),
    $flagsDb->queryAsync(Flag::class, …),
]);
```

Fan-out of k queries costs ≈ max(query) + boundary overhead, not the sum. Transactions are Fiber-safe because pins follow `tx_id`, not the socket. **Octane/Swoole caveat:** Swoole coroutines and Fibers don't compose; under Octane the client runs in sync-per-worker mode (documented).

## 11. Checked SQL & codegen (`ferro` CLI)

sqlx's trick, ported: verify every query against a real schema at build time, and generate the types. The CLI is a Rust binary in the engine workspace (D10), sharing the proto and backend crates.

- `ferro schema sync` — applies the project's migrations to a disposable shadow database (or loads a committed schema snapshot).
- `ferro check` — collects queries from: `.sql` files with front-matter and `#[FerroQuery('…')]` attributes (v1); inline `Ferro::sql('…')` literals via a PHPStan extension (phase 2, D3). Each is `PREPARE`d against the shadow schema; syntax errors, unknown columns, and param-count/type mismatches fail CI.
- `ferro gen` — emits `gen/`: readonly DTO classes with native property types, PHPStan stubs, and `manifest.json`.

**Manifest schema:** `query_id → {normalized_sql, param_types, result_shape, pool, readonly: bool, idempotent: bool}`. `idempotent` defaults to `false`; it is declared in the query's front-matter/attribute and is the sole license for auto-retrying an `Indeterminate` write (§9.2, §19.3) — a natural-key upsert can declare it, a balance increment cannot.

The manifest hash participates in the handshake (§5). **Manifest-only mode** (`pools.<name>.manifest_only = true`): the engine executes only registered `query_id`s and rejects raw SQL — the injection surface collapses to zero for that pool, and compromised PHP cannot exfiltrate beyond the declared query set. Ships as a hardening option, off by default.

## 12. Security model

- UDS with `SO_PEERCRED` allow-list (uid/gid) + optional bearer token for TCP fallback.
- **Credential isolation:** DB DSNs/passwords live only in `ferrod`'s config/secret store. PHP never sees them; an FPM compromise leaks no database credentials, and with manifest-only mode cannot even issue arbitrary queries. Consequence for ecosystem tooling that shells out with credentials: §14–15 and D8.
- TLS to upstreams (rustls), per-pool CA/cert config, `sslmode` equivalents.
- memfd payloads are sealed (§5.1) and fds pass only over peer-cred-verified sockets; at-rest encryption of these buffers is out of scope for v1 (D6).
- Per-pool guards: `readonly = true` pools set backend read-only session state and reject classified writes; `max_result_bytes`, `default_timeout_ms` are pool policy.

## 13. Observability

- OTLP traces: one span per EXEC (`query_id`/fingerprint, pool, backend conn id, rows, bytes, `queue_us`/`exec_us` split), linked to the caller's `traceparent` passed in EXEC options.
- Prometheus: pool size/idle/pinned, checkout p50/p99, queue depth, pin duration histogram, **pin-cause counters** (labels: `tx, set, listen, lock, prepare, temp, unknown, pin_function`), **hygiene counters** (`skipped_clean, targeted, full`), statement-cache hit rate, replica lag, error-taxonomy counters including `indeterminate_total`, engine `boot_epoch` gauge and restart counter.
- Slow log: normalized fingerprints, redacted params by default (`log_params = never|on_error|always`).
- `ferro top`: live TUI over the admin service (current pins, longest transactions, hot fingerprints).

## 14. Doctrine tier (`ferro/doctrine-dbal-driver`)

**Goal:** existing Doctrine DBAL / ORM applications switch by configuration only. The ORM's identity map, lazy proxies, and hydrators are untouched (rows cross the wire as arrays at this tier; typed-DTO hydration is a native-API feature). The wins here are pooling, multiplexing, shared statement cache, and normalized errors.

**Surface.** Implements the DBAL driver SPI, **DBAL `^4.0` first** (D2; `^3.8` bridge is an M2 deliverable):

- `Ferro\DBAL\Driver` → `connect()` returns a `Ferro\DBAL\Connection` bound to a pool session; `getDatabasePlatform()` selects the platform from `HELLO_ACK` pool metadata + server version; `getExceptionConverter()` maps the §9.2 tree to DBAL exceptions (`UniqueConstraintViolationException`, `DeadlockException`, `ConnectionLost`, …) uniformly across backends, plus `Ferro\DBAL\IndeterminateWriteException` for the third branch.
- `Connection`: `prepare()`, `query()`, `exec()`, `lastInsertId()`, `beginTransaction()/commit()/rollBack()` → TX service frames (savepoints via DBAL's normal savepoint path); `quote()` client-side per platform (D5; discouraged, present for compat); `getServerVersion()` from handshake; `getNativeConnection()` returns the `Ferro\Client\Session` — **documented break** for code expecting a `PDO` instance.
- `Statement`/`Result`: `bindValue()` with DBAL `ParameterType`→canonical mapping; `Result::fetch*()` families backed by row frames; `rowCount()` from `affected`; streaming used automatically when the consumer iterates (`iterateAssociative()` et al. never buffer).

**Configuration:**

```php
// Doctrine / Symfony
'connections' => ['default' => [
    'driverClass' => Ferro\DBAL\Driver::class,
    'ferro' => ['pool' => 'main', 'read_pool' => 'main_ro'],
]],
```

**Semantics & compat notes:**

- `lastInsertId()`: MySQL from the OK packet of the last EXEC on the session; PG via `RETURNING` (DBAL/ORM already prefer sequences/`RETURNING`); sequence-name argument supported for PG.
- DBAL middlewares (logging, schema managers, migrations) operate above the driver SPI and work unchanged; `doctrine/migrations` runs its DDL through the same session (DDL pins per §7).
- Known incompatibilities to document: bundles calling `getNativeConnection()` expecting PDO; `pdo_pgsql`-specific COPY hacks (replaced by a first-class `Ferro\Pg\Copy` API); persistent-connection tuning advice (obsolete under Ferro); **anything shelling out to `pg_dump`/`mysqldump` with application config credentials — the credentials no longer exist in PHP** (D8: ops provisions separate dump credentials; no engine passthrough in v1). The incompat list is a first-class doc page with per-package workarounds, budgeted in M2.
- **Acceptance:** DBAL 4 functional test suite green on PG + MySQL + SQLite; ORM functional suite green on PG + MySQL. Runner scripts live in `/testkit` (§20.3).

## 15. Eloquent tier (`ferro/laravel`) — drop-in

**Goal:** `config/database.php` changes `driver` and nothing else. Models, query builder, migrations, seeds, `DB::` facade, pagination, `chunk`/`lazy`, upserts, transactions with `attempts`, and events all behave identically.

```php
'connections' => [
    'mysql' => [
        'driver' => 'ferro-mysql',      // was 'mysql'
        'pool'   => 'main',
        'read'   => ['pool' => 'main_ro'],   // optional, maps to Laravel's read/write split
        // host/username/password removed — credentials live in ferrod (§12)
    ],
],
```

**Mechanism.** A service provider registers `ferro-{mysql,pgsql,sqlite,sqlsrv}` via `Illuminate\Database\Connection::resolverFor()`. Each resolver builds `Ferro{MySql,Postgres,SQLite,SqlServer}Connection extends` the corresponding Illuminate connection class — inheriting the stock Grammar and Processor, which only build SQL strings and post-process results and therefore need no changes. The subclass overrides the execution layer:

- `select()`, `selectResultSets()`, `cursor()` → EXEC frames; `cursor()` maps to the credit-based stream (§5.2) surfaced as `LazyCollection` — `Model::lazy()`/`chunkById()` get constant-memory behavior for free.
- `statement()`, `affectingStatement()`, `unprepared()` → EXEC with `fetch: none`, `affected` returned.
- `beginTransaction()/commit()/rollBack()` and savepoint compilation → TX service; `DB::transaction($fn, attempts: 3)` maps `attempts` to the engine's retryable classes (deadlock/serialization) so retries actually re-run the closure on a fresh tx.
- `getPdo()/getReadPdo()` return a **`FerroPdoShim`** — a thin PDO-interface adapter implementing only the residual surface the framework and common packages touch (`quote`, `lastInsertId`, `inTransaction`, `exec`, `getAttribute(SERVER_VERSION)`). It exists so `ManagesTransactions` internals and ecosystem packages keep working; `getRawPdo()` throws with a pointer to this section.
- `lastInsertId` semantics identical to §14; `Schema` builder and migrations run through `statement()` unchanged.
- **Known incompatibilities to document:** `php artisan schema:dump` and `php artisan db` (both spawn native clients with config credentials, which no longer exist — D8), `spatie/laravel-backup` and similar dump-based packages, packages reaching for `PDO::pgsqlCopyFromArray` (Ferro-native `Ferro\Pg\Copy` equivalent documented). Same incompat doc page as §14.

**Acceptance:** the `illuminate/database` integration test suite green on MySQL, PG, SQLite via Ferro connections; a Laravel demo app (auth scaffolding + queues + Horizon DB metrics) runs with only the config diff above. Octane supported in sync mode (§10.1 caveat).

## 16. Performance targets (v1 exit criteria)

- Boundary overhead (client call → engine → trivial `SELECT 1` → response): **p50 < 60 µs, p99 < 200 µs** on loopback UDS, vs local PDO baseline.
- Equal-throughput upstream connection count reduced **≥ 5×** on the reference workload (200 FPM workers, pgbench-like mix).
- 10-query fan-out latency ≤ max(single query) + 2 ms under Fibers.
- Streaming a 1 GB result: client RSS bounded by window (≤ 16 MiB), engine RSS bounded by per-stream buffers.
- Statement-cache hit rate > 95 % steady-state on the reference app (made achievable by conditional hygiene, §7.2).

All targets are measured against a **recorded reference environment** — `bench/README` pins CPU model, kernel, PHP version, and JIT status; numbers are meaningless otherwise.

### 16.1 Boundary budget and the M0 decision gate

Component budget for the trivial-call path, pure-PHP client:

| component | est. µs |
|---|---|
| 2× UDS syscalls (write/read) | 2–4 |
| MessagePack encode+decode, pure PHP | 5–15 (JIT-dependent) |
| frame header pack/unpack | ~0.5 |
| engine hop (decode, pool checkout hit, dispatch) | 5–10 |
| backend `SELECT 1` round trip | 20–40 |
| hydration (memoized plan) | ~1/row |

p50 < 60 µs is reachable pure-PHP. **p99 is where pure PHP dies first** — GC pauses, JIT-less hosts, wide rows. Mitigations in order: runtime-detect `ext-msgpack` (`composer suggest`, transparent swap); header codec is a single `pack`/`unpack` either way. **Decision gate (D12): the M0 bench harness measures p99 on the reference environment with the pure-PHP codec. A miss pulls the `ext-php-rs` accelerator (frame codec + hydration, same wire contract) forward into M1.** M0 does not exit without this measurement recorded.

## 17. Delivery milestones

- **M0** — `/proto` registry + golden vectors; frame codec (Rust) + fuzz target; `ferrod` core (session layer, HELLO/epoch, PING/PONG); PG pool (hand-rolled, D9); EXEC/TX happy paths; sync PHP client with codec autodetect and reconnect-loop skeleton; bench harness + PDO baseline. **Exit gate: D12 measurement recorded.**
- **M1** — pin engine as specified (§7.1–7.2: protocol signals, trackers, assist lexer, conditional hygiene) + MySQL backend + tracker verification test; full error taxonomy incl. `Indeterminate`; Doctrine driver; DBAL 4 suite green. Accelerator work lands here iff D12 gate failed.
- **M2** — Eloquent tier + PDO shim; Illuminate suite green; observability (OTLP/Prometheus/slow log); SQLite engine-owned mode; DBAL `^3.8` bridge; incompat doc page (§14–15).
- **M3** — Fibers multiplexing; `ferro check`/`gen` with `idempotent` manifest; manifest handshake; memfd path behind `MEMFD_RX`; COPY API.
- **M4** — MSSQL (mode per D1 outcome); manifest-only hardening mode; replica routing + lag gating; `ferro top`.
- **M5** — LISTEN/NOTIFY streams; Octane guidance; packaging (deb/rpm/container sidecar; systemd templated + socket-activated units per §18).

### 17.1 M0 task order

1. `/proto` registry files (`methods.toml`, `errors.toml`, `types.toml`) + constant generation: Rust via `build.rs` in `ferro-proto`, PHP via a small generator script committed alongside.
2. Frame codec in `ferro-proto` + golden vectors in `/proto/vectors/` + `cargo-fuzz` target on the decoder.
3. `ferrod` skeleton: UDS listener, per-session task, HELLO/HELLO_ACK with `boot_epoch`, PING/PONG, GOODBYE, protocol-error frames.
4. Hand-rolled PG pool on `tokio-postgres`: checkout/release, `max_lifetime`, liveness ping. Pin state machine stubbed (everything pins on `BEGIN` via ReadyForQuery only — full engine is M1).
5. SQL service EXEC: param bind, row framing, `stats`, per-request credit windows.
6. TX service: BEGIN/COMMIT/ROLLBACK/savepoints, `tx_id` pinning, deadline timers.
7. PHP client (sync): socket + codec (pure PHP, `ext-msgpack` autodetect), `query/queryOne/rows/scalar`, transaction closure, error mapping v0, reconnect loop with epoch check.
8. Bench harness: trivial-call latency distribution (Ferro vs local PDO), fan-out placeholder, results committed under `bench/results/` with environment manifest.
9. CI: `cargo test/clippy/fmt` (warnings denied), fuzz smoke job, PHPUnit + PHPStan, docker-compose PG service.

## 18. Deployment & lifecycle

Single static binary. **systemd (primary):** templated units — `ferrod@.service` + `ferrod@.socket`, instance name = schema hash, socket unit binds `/run/ferro/%i.sock`. Socket activation means **systemd holds the listener fd across daemon restarts**: connections arriving mid-restart queue in the backlog instead of getting `ECONNREFUSED`, which is what makes §19's reconnect story quiet in practice. Deploys: start `ferrod@{new_hash}.socket`, atomically switch a `current` symlink for tooling, old instance drains via idle TTL and exits; old and new coexist during rollout. (v0.1's single static unit could not express this — hash-versioned paths require the template form.)

Also: pod sidecar sharing an `emptyDir` socket volume. Config `ferro.toml` (pools, DSNs via env/secret refs, policies); `SIGHUP` reload, `SIGTERM` drain. Client discovers the socket by schema-hash path, then `FERRO_ADDR` fallback. Kubernetes readiness = admin `/healthz` (all pools connectable, or degraded-but-serving flag).

## 19. Engine restart & client resilience

A `ferrod` crash or deploy is a *correlated* failure — every worker at once — unlike shared-nothing PDO where connection death is uncorrelated. v0.1 hand-waved this; it is now first-class. The design goal: an engine restart is a **blip for reads and declared-idempotent writes, a clean typed error for everything else, and never a silent unknown**.

### 19.1 Epochs

`boot_epoch` arrives in HELLO_ACK and is cached by the client. On reconnect, a changed epoch means: all engine-side state (tx pins, streams, prepared handles) is gone. Type-registry hash is re-verified as always; mismatch remains a hard error.

### 19.2 Client reconnect loop

On dead socket (EOF, write failure, missed PONG deadline): reconnect with jittered exponential backoff (base 10 ms, cap 1 s, deadline default 5 s → then `Retryable{ConnectionLost}` surfaces to the caller as pool-unavailable). With socket activation (§18), restarts rarely refuse — connects block in backlog until the new instance accepts.

### 19.3 Fate rules on connection loss

For each request in flight when the link died:

- **Read** (`readonly: true`, or manifest `readonly`): `Retryable{ConnectionLost, epoch_changed}`. Auto-retried by the client when `retry_reads = true` (default), transparently, on the new epoch.
- **Autocommit write:** `Indeterminate{WriteUnconfirmed, cause: link_lost}`. Auto-retried **only** when the manifest entry declares `idempotent: true`; otherwise surfaced. Raw-SQL writes always surface.
- **In-transaction request:** the whole transaction is dead (the engine rolled it back or died with it). Closure-API users: the closure re-runs on the new epoch under the caller's retry policy — this is why the closure is the primary tx API. Explicit BEGIN/COMMIT users receive `Retryable{ConnectionLost, epoch_changed: true}` and must restart the transaction themselves.
- A `COMMIT` frame sent with no response is the one transactional `Indeterminate`: the commit may or may not have applied. Surfaced as `Indeterminate{cause: link_lost}`; never auto-retried.

Engine-side mirror: when a *backend* link dies, the engine applies the same classification (dispatched-without-response → `Indeterminate`; not-yet-dispatched → `Retryable`) and MUST NOT transparently re-dispatch (§3).

### 19.4 What the engine gives back

The same daemon that concentrates risk also absorbs it: circuit breakers, engine-side backend reconnection, and drain semantics mean a flapping database is met by one polite reconnector instead of 200 stampeding workers, and workers keep getting fast typed `Retryable` errors instead of hanging on TCP timeouts. Documentation should present this chapter as a trade made deliberately, with the mechanics above as the price paid.

## 20. Repository layout, conventions, testing

### 20.1 Layout

```
/engine                     Cargo workspace
  crates/ferro-proto        wire types, codec, generated consts, vector tests
  crates/ferro-classify     assist lexer (§7.1)
  crates/ferro-pool         generic pool + pin state machine
  crates/ferro-backend-pg   | -mysql | -sqlite | -mssql
  crates/ferrod             daemon binary (session layer, services, admin)
  crates/ferro-cli          `ferro` CLI (schema sync / check / gen)
/php
  client/                   ferro/client            (PSR-4 `Ferro\`)
  doctrine-dbal/            ferro/doctrine-dbal-driver
  laravel/                  ferro/laravel
/proto                      methods.toml, errors.toml, types.toml, vectors/
/bench                      harness + committed results with env manifests
/testkit                    docker-compose (pg, mysql, mssql), fixtures,
                            upstream-suite runners (DBAL, illuminate/database)
```

### 20.2 Conventions

Rust: latest stable pinned in `rust-toolchain.toml`, edition 2024, tokio multi-thread, `thiserror` in libs / `anyhow` at binary edges, `tracing` throughout, clippy warnings denied in CI, `cargo-fuzz` on every codec/lexer. PHP: ≥ 8.2, PSR-12, PHPStan level 9 on `client/`, PHPUnit 10. The `/proto` registry is the only place protocol numbers exist; hand-written constants in either language are a review reject.

### 20.3 Testing strategy

- **Conformance:** golden frame vectors in `/proto/vectors/`; the PHP codec MUST byte-match them (both codec paths: pure and `ext-msgpack`).
- **Engine:** unit tests per crate; integration tests against dockerized backends via `/testkit`; pin-engine tests assert pin cause labels for each trigger class, incl. the M1 MySQL tracker-in-procedure verification (§7.1).
- **Chaos:** a harness that SIGKILLs `ferrod` mid-workload and asserts the §19.3 fate matrix — every in-flight request must land in exactly the specified branch. This is the acceptance test for G7.
- **Upstream suites:** `/testkit` runners execute the DBAL and `illuminate/database` suites against Ferro connections; green is the M1/M2 acceptance bar, run in CI nightly.
- **Bench:** harness runs in CI as informational; the D12 gate at M0 exit is a human sign-off on recorded numbers.

## 21. Decision log & open items

| # | Decision | Rationale / revisit trigger |
|---|---|---|
| D1 | MSSQL pools default to **session mode** until tiberius reset-on-RPC semantics are verified (M4 spike). | Correctness over pooling wins; flip default when verified. |
| D2 | DBAL **`^4.0` first** (M1 acceptance); `^3.8` bridge in M2. | Cuts a third of the SPI shims out of the critical path; Symfony 6 adoption pressure decides how hard M2 pushes. |
| D3 | Query extraction v1 = `.sql` files + attributes; PHPStan inline-literal extraction = phase 2. | Deterministic sources first; static analysis is a quality-of-life add. |
| D4 | Windows dev = TCP loopback only; memfd disabled; named pipes out of scope v1. | Not a production target. |
| D5 | `quote()` implemented client-side with per-platform tables; no engine round trip. | A network hop inside a discouraged compat API is absurd; tables are small and testable. |
| D6 | No at-rest encryption of memfd buffers in v1. | Sealed anonymous memory + `SO_PEERCRED`-gated fd passing is the boundary; revisit if multi-tenant hosts become a target. |
| D7 | "Ferro" remains a placeholder. **Maintainer task before M1:** crates.io / Packagist / trademark check. | Human-only item. |
| D8 | No credential passthrough from engine to PHP tooling (`schema:dump`, `artisan db`, dump-based backup packages). Documented incompat + ops-provisioned dump credentials. | Credential isolation is a headline guarantee; punching a hole for convenience inverts it. Revisit as a scoped admin "credential lease" only with real demand. |
| D9 | The pool is hand-rolled, not `deadpool`/`bb8`. | The pin state machine, conditional hygiene, and checkout-pipelining *are* the product; generic pools fight all three. |
| D10 | `ferro` CLI is a Rust binary in the workspace. | Shares proto + backend crates; shadow-DB PREPARE needs real drivers. |
| D11 | `COMPRESSED` flag reserved; no implementation before post-M3 bench evidence. | UDS loopback rarely wants zstd; keep the bit, skip the work. |
| D12 | Codec strategy is empirical: pure PHP + `ext-msgpack` autodetect at M0; `ext-php-rs` accelerator pulled into M1 iff the M0 p99 gate fails. | §16.1. |

**Open items (maintainer):** license selection; reference-hardware sign-off for §16; naming (D7); security review scheduling before any public beta.

## 22. Changelog v0.1 → v0.2

- Pin detection inverted: backend protocol signals (PG ReadyForQuery, MySQL session trackers) are primary; lexer demoted to assist role for protocol-invisible statements (§7.1).
- Hygiene moved to checkout (pipelined, RTT hidden) and made conditional; targeted PG profile fixed to release advisory locks and temp state; MySQL reset skipped on tracker-clean connections, reconciling reset semantics with the 95 % cache target (§7.2).
- Transaction-mode contract stated: function-side session mutation unsupported on PG, `pin_functions` + session mode as escape hatches (§7.4).
- Error taxonomy gained the `Indeterminate` branch; write-fate rules defined for cancel/timeout/link-loss (§9.2).
- New §19: boot epochs, client reconnect loop, fate matrix, closure-as-recovery-surface, no transparent engine retries; chaos test mandated (§20.3).
- Manifest schema gained `readonly` + `idempotent`; idempotence is the sole auto-retry license for indeterminate writes (§11).
- §5.1 memfd corrected: one client-side copy via `ext-sockets`/`php://fd`, gated on `MEMFD_RX`; zero-copy is engine-side until the native accelerator exists.
- §5.2 added: normative request_id, CANCEL/END race, per-request + per-session credit, PING/GOODBYE.
- §16.1 added: component-level latency budget, p99 risk analysis, D12 decision gate.
- §18 rewritten: templated `ferrod@` units + socket activation holding the listener fd; old/new coexistence during rollout.
- Doctrine/Eloquent incompat lists extended with credential-dependent tooling (D8); DBAL 4-first (D2).
- Min PHP raised to 8.2 (readonly classes). All v0.1 §19 open questions resolved or defaulted in §21.

### 22.1 M0 implementation deviations (thin-slice scope)

These record where the M0 build (milestone §17.1) intentionally implements a *subset* of the target design above; each is a scope/milestone note, not a change to the target spec. See `docs/superpowers/specs/2026-07-23-ferro-m0-execution-design.md` §4 for the decision context.

- **§20.1 workspace** — single repo-root Cargo workspace (`engine/crates/*` + `bench`) instead of an `/engine`-rooted one, so the charter commands run verbatim (B-1).
- **§7.1 / §17.1(4) pin stub (M-2, CLOSED in M1-S1)** — the M0 pin stub was driven from the TX-service lifecycle, not from the PG ReadyForQuery I/T/E byte (stock `tokio-postgres` exposes none). RFQ-byte access was raised as a §21 open item for the M1 pin engine (M-2). **Resolved in M1-S1** (decision M1-D2, `docs/superpowers/specs/2026-07-28-ferro-m1-execution-design.md`): a vendored `tokio-postgres` fork (`vendor/tokio-postgres`, root `Cargo.toml` `[patch.crates-io]`) exposes the real ReadyForQuery status byte via `Client::transaction_status()`, wired through `PoolBackend::tx_status` into `Checkout::apply_tx_status` (`ferro-pool`) as the pool's pin AUTHORITY — the engine's own manual pin bookkeeping (`begin_tx_with`/`commit_tx`/`rollback_tx`'s hand-set fields, the S6 actor's teardown `set_tainted(true)`) is retained as defense-in-depth, not the authority. The upstream PR is drafted, not yet filed (`/UPSTREAM_PR.md`, pending maintainer sign-off/human authorization); the fork and this note are dropped if/when it merges.
- **Charter rule 6 / §6 placeholders** — an engine-side mechanical `?`→`$n` scanner is accepted as parameter-syntax normalization (not SQL rewriting); a bare jsonb `?` must be written `??` or the client emits native `$n` (M-1).
- **§20.2 tooling** — PHPUnit 11 (not 10); the M0 client advertises `features=0` (B-2).
- **§7.2 hygiene** — M0 does minimal hygiene (release-time `ROLLBACK` guard only, applied at the *next* checkout since `Drop` is sync); full conditional/pipelined hygiene is M1. The D12 `SELECT 1` path is unaffected (R8).
- **§5.2 / §6 EXEC framing (D-S5-1)** — M0's EXEC **buffers** each result into the single `Outcome::Ok` terminal frame; the windowed streaming DATA-channel producer (per-request credit wakeup, per-session cap accounting/release, cross-channel terminal ordering, HEAD/DATA framing, `fetch:stream`) is **deferred to post-M0**. Building it before the D12 bench (§16.1) demands it is speculative optimization (charter rule 5). A result whose encoded terminal body exceeds `MAX_FRAME_PAYLOAD` → `NonRetryable{Unsupported}`. The Indeterminate write-fate guarantee (§19.3) is upheld in M0: a non-readonly conn-loss surfaces `WriteUnconfirmed{Indeterminate}`, never an auto-retry.
- **§5 HELLO_ACK pool discovery (S7 Task 2, CLOSED)** — `HelloAck.pools` now advertises the configured pool NAMES (`config.pools[].name`), so a client discovers referenceable pools from the handshake (PROTOCOL.md §4). The S5 stub hardcoded `pools: vec![]`; `hello_ack_frame` gained a `pool_names` param populated at the single `session/mod.rs` call site. Only names cross the wire — never DSNs (§12 server secret). Proven end-to-end by the PHP live handshake test.
- **§7 TX-service known limitation (S6 orphan-BEGIN race)** — if a session's reader loop ends (EOF/GOODBYE/fatal) while a `BEGIN` handler is between `pool.checkout()` and `tx_registry.register()`, the actor can be registered *after* `abort_session` snapshotted the registry, orphaning it under a dead `SessionId`. It is **self-healing and client-invisible**: the actor reaps itself via its `idle_in_tx` deadline (≤ default 10 s → rollback + release the pooled conn + release the semaphore permit + tombstone), the tombstone is `TOMBSTONE_CAP`-bounded and owner-scoped to an unreachable, never-reused id, and there is no double/zero-`END`, no cross-session reachability, and no permit leak beyond `idle_in_tx`. The airtight fix (have `handle_begin` re-check the request `CancellationToken` immediately before `register`, rolling back + dropping the fresh conn if the session is draining) is **deferred to S7-era hardening** — it changes no wire contract and nothing downstream depends on it.
- **§16.1 / §17.1(8) D12 measurement recorded (S8, M0 exit gate MET — provisional)** — the D12 boundary-latency measurement is committed under `bench/results/` with a complete environment manifest, and `ci/check-d12-recorded.sh` exits 0. The recorded run (`bench/README.md`) is tagged **`provisional: true, reference: false`**: it was measured on **WSL2** with a **host-process release `ferrod` + host PHP against the Dockerized testkit Postgres** (the S8 plan's directed topology). On this environment the §16.1 boundary targets (p50 < 60 µs, p99 < 200 µs) are **not met** — the absolute numbers are dominated by the WSL2 + Docker-proxy Postgres round trip, which costs ~678 µs p50 even for the raw PDO baseline; Ferro's *added* overhead vs PDO in the identical environment is ~163–170 µs p50 / ~109–124 µs p99. Per §16.1/§20.3 D12 is a **human sign-off on recorded numbers, not a CI threshold**: the two outstanding sign-off items are (1) a **bare-metal / host-network reference re-run** (the fully-containerized topology of the execution-design is part of that scope), and (2) the **`ext-php-rs` accelerator** decision (the pure-PHP `PurePacker` is what M0 measures; `ext-msgpack` is absent here and its swap is unbuilt). Neither is an M0 blocker — the exit condition is the *recording*, which is met.
