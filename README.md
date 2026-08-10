# Ferro

**A per-host database access engine for PHP.**

One Rust daemon (`ferrod`) owns every upstream database connection on the host, pools them in transaction mode, and multiplexes all of your PHP-FPM workers over a local Unix socket — while staying a **drop-in** replacement for Doctrine DBAL and Laravel Eloquent, adopted by changing configuration only.

> **Status: pre-release, under active development.** The M0 vertical (wire protocol, daemon, PG pool, SQL + TX services, sync PHP client, benchmark harness) is complete and works end-to-end against real Postgres. M1 is in progress: the pin engine, conditional connection hygiene, the full error taxonomy + write-fate matrix (with live chaos suites), credit-based result streaming, the MySQL/MariaDB backend, and canonical type coverage across PG 17 / MySQL 8.4 / MariaDB 11.8 have landed; the Doctrine DBAL driver is in progress. Nothing here is production-ready yet; the full design lives in [ferro-spec-v0.2.md](ferro-spec-v0.2.md). The name "Ferro" is a placeholder.

---

## The problem

PHP-FPM is shared-nothing: every worker owns its own database connections, prepared-statement caches, and TLS sessions. **200 workers means 200 upstream connections and zero cross-worker reuse.** PDO — designed in 2004 — compounds this: synchronous only, stringly typed, connection-oriented, and inconsistent across drivers.

The existing fixes each solve a slice and leave the rest:

| Approach | What it fixes | What it can't fix |
|---|---|---|
| `PDO::ATTR_PERSISTENT` | reconnect cost | still one connection *per worker*; no multiplexing, no hygiene, leaks session state between requests |
| External poolers (PgBouncer, ProxySQL, RDS Proxy) | connection count | they are **transparent wire proxies**: transaction boundaries are visible on the wire, but session mutation often isn't. PgBouncer's transaction mode documents session features (non-`LOCAL` `SET`, advisory locks, temp tables, `LISTEN`, prepared statements) as unsupported and moves on; ProxySQL and RDS Proxy detect what they can and fall back to pinning the connection — silently losing the multiplexing you deployed them for. Mutations none of them can see (inside server-side functions) still leak |
| In-process pools (Swoole/Octane, amphp) | pooling within one process | requires rewriting your app to be worker-safe/async; connections still scale with worker count across the host |

And underneath all of them sits the failure nobody models: **the write whose fate is unknown.** A timeout or connection loss lands *after* an `UPDATE` was dispatched but *before* the response arrived. Did it apply? Every layer in today's stack either retries it blindly (possible double-apply), or reports a generic error and leaves you guessing. For a balance transfer, both answers are wrong.

Finally: every one of those 200 workers holds the database password in its config. A single FPM compromise leaks your credentials.

## What Ferro is

Ferro is **not** a database, a SQL rewriter, a result cache, or an ORM in Rust. It occupies a category between the connection pooler and the data-access layer:

**a host-local access engine that owns both ends of the wire.**

That co-design — engine daemon *and* client library from the same protocol registry — is what makes it structurally different from every transparent proxy:

- Transactions are **declared** by the client protocol and pin engine-side to a **`tx_id`, not to a client socket** — the one thing no wire-transparent proxy can do. It's what will make the Fibers tier (M3) safe: a suspended request keeps its transaction across frames while one worker connection multiplexes many in-flight requests.
- Pin and session state are driven by **backend protocol signals** as the authority — PostgreSQL's `ReadyForQuery` status byte and MySQL's OK-packet session trackers; a keyword lexer only assists with what's text-visible but protocol-invisible, and pins conservatively on what it can't classify.
- Errors map to **one normalized taxonomy across all backends** — with a first-class `Indeterminate` branch for the unknown-fate write. The engine never transparently retries a user statement; it classifies and reports, and retry is client policy. Auto-retry of an indeterminate write is licensed only by a build-time manifest (M3) declaring the query `idempotent: true`.
- Database credentials live **only in the daemon.** PHP never sees a DSN or password; the planned **manifest-only mode** (M4) goes further — a pool that executes only build-time-registered `query_id`s rejects raw SQL entirely, collapsing that pool's injection surface to zero (SPEC §11, §12).

```
┌────────────── host ──────────────────────────────────────────┐
│  PHP-FPM workers (N)                                          │
│   ├─ ferro/client        (native typed API, Fibers-aware)     │
│   ├─ ferro/doctrine-dbal (DBAL Driver impl → client)          │
│   └─ ferro/laravel       (Illuminate Connection → client)     │
│              │  UDS /run/ferro/{schema_hash}.sock             │
│              ▼                                                │
│  ferrod (Rust, tokio)                                         │
│   ├─ session layer: handshake (epoch), auth, multiplexing     │
│   ├─ SQL service: param bind, result framing                  │
│   ├─ TX service: tx_id pinning, savepoints, deadlines         │
│   ├─ pin engine: protocol signals + assist lexer              │
│   ├─ pools: pg | mysql | sqlite | mssql  (+ replicas)         │
│   ├─ manifest store: query_id → checked plan                  │
│   └─ admin/metrics: OTLP, Prometheus, slow log                │
└──────────────────────────────────────────────────────────────┘
```

*(Target architecture, SPEC §4 — the roadmap below tracks which boxes exist today.)*

## What it improves, concretely

**Connection economics.** N workers share M real connections with M ≪ N. The v1 target is a **≥ 5× reduction** in upstream connections at equal throughput on a 200-worker reference workload (SPEC §16). Postgres connections are expensive server-side; this is the difference between tuning `max_connections` upward forever and not thinking about it.

**Correct transaction-mode pooling.** Transparent poolers either document session-state breakage as an accepted caveat or pin conservatively and quietly give the multiplexing back. Ferro's pin engine detects session mutation from backend protocol signals first and a conservative lexer second, pins the connection only when it must, and applies **conditional hygiene** at checkout: a tainted connection gets the full reset (`DISCARD ALL`), a clean recycled one gets a targeted profile that closes every session-leak class — advisory locks, `LISTEN` registrations, temp objects, `WITH HOLD` cursors, session authorization — while preserving the engine's future namespaced prepared statements (the shared statement cache is a later slice). What genuinely can't be detected (session state mutated *inside* a server-side function on PG) is a **documented contract** with explicit escape hatches (`pin_functions`, per-pool session mode), not a silent footgun (SPEC §7.4).

**Predictable failure.** Every backend and engine error maps to one tree: `Retryable`, `Indeterminate`, `NonRetryable` — carrying the raw SQLSTATE/errno alongside (SPEC §9.2). An engine restart is detected by a `boot_epoch` change in the reconnect handshake, which voids all engine-side state assumptions; in-flight work lands in exactly one branch per a defined fate matrix (SPEC §19). The design goal: a restart is *a blip for reads and declared-idempotent writes, a clean typed error for everything else, and never a silent unknown.* This isn't aspirational — the fate matrix is implemented, and a live chaos suite kills connections and cancels/times-out statements against real Postgres to prove every in-flight request lands in exactly the specified branch, and that a write is applied at most once.

**A shield, not just a concentrator.** Concentrating connections in one daemon also concentrates recovery: the design puts per-backend circuit breakers and engine-side reconnection in front of the workers, so a flapping database is met by one polite reconnector instead of 200 stampeding workers — and workers get fast, typed `Retryable` errors instead of hanging on TCP timeouts (SPEC §7.7, §19.4). Packaged deploys (M5) will use systemd socket activation, so the listener fd survives daemon restarts and connections queue instead of getting `ECONNREFUSED` (SPEC §18).

**Types you can trust.** One canonical type system across backends — all 14 registry types read and bound end-to-end on PG 17, MySQL 8.4, and MariaDB 11.8. Cross-driver ambiguities — decimals, unsigned 64-bit, naive datetimes, UUIDs — resolve by explicit client-side *policy*, not per-driver guessing (SPEC §9.1): the four policies are shipped, decoding into `Ferro\` value objects, and non-representable values fail loudly instead of being coerced. The native API hydrates `readonly` PHP DTOs with a memoized plan (no per-row reflection).

**Concurrency without a rewrite.** The native client is designed to be Fibers-aware (M3): under a Fiber scheduler, awaiting will suspend the Fiber while the client multiplexes all in-flight queries over its single socket — a fan-out of k queries targets ≈ max(query), not the sum (SPEC §10.1, §16). Under plain FPM the same code blocks synchronously and works everywhere; transactions stay Fiber-safe by construction because pins follow the `tx_id`, not the socket.

**Operator visibility.** Every EXEC reports `queue_us` vs `exec_us` — pool-wait time versus execution time, the split that tells you whether to grow the pool or fix the query. The Prometheus surface (M2) adds pin causes, hygiene actions, statement-cache hit rate, and an `indeterminate_total` counter you want to be zero (SPEC §13).

**SQLite, done right (M2).** The engine will own the file: WAL mode, one serialized writer, many readers — removing PHP's worst SQLite failure mode, `SQLITE_BUSY` storms under FPM (SPEC §7.6).

## Drop-in adoption

> The Doctrine driver (M1) is under active development — not yet released; the Eloquent package (M2) does not exist yet. The config below shows what adoption will look like (SPEC §14, §15).

The drop-in tiers change the **execution layer only**. Doctrine's platforms and Laravel's Grammar/Processor stay completely stock — Ferro never generates or rewrites SQL.

**Doctrine DBAL / Symfony** (SPEC §14):

```php
'connections' => ['default' => [
    'driverClass' => Ferro\DBAL\Driver::class,
    'ferro' => ['pool' => 'main', 'read_pool' => 'main_ro'],
]],
```

**Laravel / Eloquent** (SPEC §15):

```php
'connections' => [
    'mysql' => [
        'driver' => 'ferro-mysql',           // was 'mysql'
        'pool'   => 'main',
        'read'   => ['pool' => 'main_ro'],   // optional read/write split
        // host/username/password removed — credentials live in ferrod
    ],
],
```

Acceptance for both tiers is the **upstream test suites** — DBAL 4's functional suite and `illuminate/database`'s integration suite — running green over Ferro connections.

The native client is where the new capabilities live. This is the **target API** (SPEC §10) — today's shipped surface is `Ferro::connect()` plus `query`/`queryOne`/`rows`/`scalar`/`exec`, typed DTO hydration into value objects, the lazy `stream()` generator, the transaction closure with a retry policy, and imperative `begin`/`commit`/`rollBack`; `Ferro::pool()` and the async/Fibers surface come later:

```php
$db = Ferro::pool('main');

// typed hydration into readonly DTOs
$user = $db->queryOne(User::class, 'select id, email, created_at from users where id = ?', [$id]);

// transactions are closures; scope is explicit, retry is policy
$db->transaction(function (Ferro\Tx $tx) {
    $tx->exec('update accounts set balance = balance - ? where id = ?', [$amt, $a]);
    $tx->exec('update accounts set balance = balance + ? where id = ?', [$amt, $b]);
}, Retry::onSerializationFailure(times: 3, backoff: Backoff::jitter(10, 200)));

// streaming: constant memory, credit-driven
foreach ($db->stream(Event::class, 'select … order by id') as $event) { … }
```

## What Ferro is deliberately not

Scope discipline is a design feature (SPEC §3):

- **Not a storage engine, not a SQL rewriter, not a result cache.**
- **No ORM in Rust** — identity maps, lazy proxies, object graphs stay in PHP. The engine's job ends where live object semantics begin.
- **No read/write inference from SQL text** — replica routing is explicit from the client.
- **No transparent engine-side retries, ever** — the engine reports outcomes truthfully; retry is client policy. This is load-bearing for the `Indeterminate` guarantee.

## Status & roadmap

| Milestone | Scope | State |
|---|---|---|
| **M0** | `/proto` registry + golden vectors, frame codec + fuzzing, `ferrod` core (sessions, epochs), hand-rolled PG pool, EXEC/TX happy paths, sync PHP client, bench harness vs PDO baseline | ✅ complete — provisional D12 boundary measurement recorded in [bench/results/](bench/results/) (WSL2 environment; the §16.1 latency targets await a bare-metal reference re-run — see [bench/README.md](bench/README.md)) |
| **M1** | Pin engine (protocol signals + assist lexer + conditional hygiene), full error taxonomy incl. `Indeterminate` + write-fate matrix, result streaming (deferred from M0), MySQL backend, canonical type coverage, Doctrine driver | 🔨 in progress — pinning, assist lexer, conditional hygiene, error taxonomy + write-fate matrix (live chaos suites on PG and MySQL/MariaDB), credit-based result streaming (PG; MySQL streaming deferred, SPEC §22.2), MySQL/MariaDB backend, and 14-type canonical coverage landed; Doctrine driver in progress |
| M2 | Eloquent tier + PDO shim, observability (OTLP/Prometheus/slow log), SQLite engine-owned mode | planned |
| M3 | Fibers multiplexing, `ferro check`/`gen` (build-time checked SQL, sqlx-style), manifest handshake, memfd large payloads, COPY API | planned |
| M4 | MSSQL, manifest-only hardening mode, replica routing + lag gating, `ferro top` TUI | planned |
| M5 | LISTEN/NOTIFY streams, Octane guidance, packaging (deb/rpm/container sidecar, systemd socket-activated units) | planned |

Everything in this README describes the **contract being built** — [the spec](ferro-spec-v0.2.md) is normative, §21 is the binding decision log, and §22 records honestly where the implementation currently deviates. Features listed under planned milestones do not exist yet.

## Repository layout

```
/engine/crates     engine crates (repo-root Cargo workspace: engine/crates/* + bench;
                   Rust edition 2024, tokio)
  ferro-proto               wire types, codec, generated consts, vector tests
  ferro-classify            assist lexer
  ferro-pool                hand-rolled pool + pin state machine
  ferro-backend-pg          PostgreSQL backend
  ferro-backend-mysql       MySQL/MariaDB backend
  ferro-e2e                 live end-to-end tests against the testkit backends
  ferrod                    daemon binary (session layer, services, admin)
/php/client        ferro/client — pure PHP ≥ 8.2, zero required extensions
/php/doctrine-dbal ferro/doctrine-dbal-driver — in progress
/proto             methods.toml, errors.toml, types.toml, golden vectors
/bench             harness (ferro-bench) + committed results with environment manifests
/testkit           docker-compose backends, fixtures, suite runners
/vendor            vendored tokio-postgres + mysql_async forks (protocol-signal
                   access; upstream PRs drafted — UPSTREAM_PR.md,
                   UPSTREAM_PR_MYSQL_ASYNC.md)
```

`/proto` is the single source of truth for method ids, flags, error codes, and type tags. Both codecs generate their constants from it; a hand-written protocol constant in either language is a defect.

## Development

```bash
docker compose -f testkit/docker-compose.yml up -d      # backends
cargo test --workspace                                   # engine tests
(cd php/client && composer install && composer test)     # client tests
cargo run -p ferro-bench -- --baseline pdo               # bench harness
```

Gates: `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace`, PHPUnit, PHPStan level 9 on `php/client`.

## License

[MIT](LICENSE)
