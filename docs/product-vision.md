# Ferro — Product Vision & Family Doctrine

**Status:** non-normative · 2026-08-10
**Authority:** none over implementation. `ferro-spec-v0.2.md` is the contract, `CLAUDE.md` is the working agreement, and SPEC §21 is the binding decision log. This document records **product strategy** — what gets built after v1, in what order, and what was deliberately refused — so those decisions don't have to be re-derived (or worse, re-litigated) later. Where this doc and the spec disagree, the spec wins and this doc is stale.

---

## 1. Thesis

Ferro occupies a category between the connection pooler and the data-access layer: **a host-local access engine that owns both ends of the wire** — the daemon and the client library, generated from one protocol registry.

That co-design is the structural moat. Every transparent wire proxy (PgBouncer, ProxySQL, RDS Proxy) has only bytes to work with: transaction boundaries are visible on the wire, but session mutation often isn't, and a transaction can never be decoupled from the client socket that started it. Ferro's client *declares* transaction scope as a `tx_id` independent of any socket, and backend protocol signals remain the authority for pin state. No proxy can retrofit this; it requires owning the client.

The second moat is the correctness contract: the `Indeterminate` branch — a side-effecting operation whose fate is unknown is *reported as unknown*, never silently retried, never guessed at. This discipline generalizes beyond SQL (see §4), and no incumbent in any adjacent slot models it.

The distribution strategy is **drop-in adoption**: config-only integration through narrow, specified seams (DBAL Driver SPI, Illuminate `resolverFor()`, and later Guzzle's `HandlerStack`, Laravel Queue drivers, Symfony Messenger transports). Adoption must always be gradual (one connection at a time) and reversible (one config line back).

### 1.1 The north star — a multi-request lifecycle for PHP

The long-term destination that explains the family's shape: **open the doors for PHP to state and resources that live across requests, the way long-lived-process languages have always had.** "Multi-request lifecycle" decomposes into four layers; the strategy claims layers 2–4 and names layer 1 honestly as someone else's door:

| Layer | What lives across requests | Who opens the door |
|---|---|---|
| 1. Code | booted app, warm object graph | worker mode (FrankenPHP/Octane) — the channel, never Ferro (§9) |
| 2. Resources | connections, tx pins, subscriptions, consumer groups | the engine family (§4) |
| 3. Coordination | locks, counters, presence, signals | Ferro State (§4.8) |
| 4. Live objects | *shared, mutable PHP objects* | the actor door — specified in §4.9 (north star, not roadmap) |

## 2. Chassis vs. engines

Everything Ferro ships is one of two things:

- **The chassis** — service-agnostic infrastructure specified as the chassis contract (SPEC §5, §5.1, §5.2, §13): session layer, HELLO/epoch handshake, the exactly-one-END invariant, request multiplexing, UDS + `SO_PEERCRED` security, and the `/proto` registry + codegen are built (M0), and credit-based streaming landed in M1-S5; OTLP/Prometheus export (M2) and memfd large payloads (M3) land per SPEC §17.
- **Engines (services)** — domain services multiplexed over the chassis, each a `service` id in the frame header (SPEC §5). The SQL + TX services are the first engine; every future family member is another service id on the same wire, consumed by the same one-connection-per-worker client.

```
ferrod — one daemon per host, one chassis
├─ 02/03  Database access engine   pooling, pinning, tx_id, fate matrix   ← in progress (M1)
├─ 04     Streams                  LISTEN/NOTIFY → event stream engine    ← M5, then grows
├─ 05     Admin service            versioned contract for tooling/console
├─ 06     HTTP transport engine    outbound HTTP: pools, TLS, fate        ← NEXT IN LINE (post-v1)
├─ 07     Job transport engine     queues: leases, acks, tx enqueue       ← after HTTP
└─ (0x)   Request dispatch         RoadRunner-style, optional             ← v3-era back pocket
```

Service ids above `05` are illustrative — the `/proto` registry assigns them when an engine actually ships. The rule that matters: **nothing SQL-specific creeps into the chassis**, so each next engine costs a service definition, not a platform.

## 3. The admission test

A candidate engine joins the family only if **all five** hold. This test was applied to every candidate in this doc; it is the reason the graveyard (§8) exists.

- **A. Shared-nothing really hurts** — N PHP workers duplicating expensive or stateful resources (connections, TLS sessions, leases) with zero cross-worker reuse.
- **B. The slot is empty** — no incumbent already doing host-level, PHP-native multiplexing for this resource. Adjacent occupants (e.g., Envoy for mesh traffic) must be structurally unable to take the slot, not merely absent from it.
- **C. A drop-in seam exists** — a specified interface where the execution layer swaps under stock application code, config-only. No seam → no engine (it becomes a native-API-only product with a different, smaller market).
- **D. The correctness moat transfers** — the fate-classification discipline must buy something real in the new domain, not just come along for the ride.
- **E. The engine/PHP boundary stays clean** — ferrod never executes PHP, never owns application semantics, and **never owns storage**. Ferro fronts backends; it is not a database, not a broker, not a filesystem.

Corollary — **the latency-inversion rule**: never put the daemon hop where the hop costs as much as the operation it fronts, *unless the engine provides state or semantics FPM structurally cannot hold* (a lease, a consumer-group membership, a transaction-composed write, a blocking read). The boundary overhead is noise against a 1–10 ms SQL query or a WAN API call; it is 50–100 % against a ~100 µs cache `GET` (order-of-magnitude figures — the measured boundary cost is the provisional D12 number in `bench/results/`, bare-metal re-run pending) — and a plain cache `GET` gains nothing from the daemon, which is why the hop is wrong there (§8, Redis row) but right for a same-cost `XADD` that rides a transaction or a lease (§4.3). This rule also killed hosted Ferro (§7), independently of the five points above.

## 4. The family, in order

### 4.1 Database access engine — the wedge (now)

The current milestone plan (SPEC §17). Everything else in this document waits behind it: the family is earned one member at a time, and nobody adopts a family — they adopt the database engine and *discover* the family. v1 stays undiluted.

**Committed feature goals — the configuration layer (P12).** Recorded as engine goals (demand-sequenced, post-v1; each becomes a SPEC amendment per the charter's definition-of-done when built):

- **Per-tenant pools (parameterized pools).** The multi-tenant SaaS unlock. Tenancy-per-database Laravel apps (stancl/tenancy et al.) today open per-tenant connections per request and hold *every tenant's credentials in PHP*. The Ferro-native shape: a pool **template** plus a tenant→DSN mapping held daemon-side; pools instantiated lazily on first checkout, reaped on idle TTL; hygiene, pinning, and the fate matrix per tenant for free. Headline: *"thousands of tenant databases behind one daemon — PHP never sees a tenant credential."* Pitch-deck eligible (§10): it threatens nothing Laravel sells and lands on their SaaS heartland.
- **Dynamic tenant loading.** The tenant catalog never requires a daemon restart. SPEC §7.7/§18 already anchor `SIGHUP` config reload with pool diffing; this extends to runtime registration — an admin-service control surface and/or a catalog resolved through a fronted backend — with hard bounds from day one (max dynamic pools, per-tenant connection caps, per-tenant checkout quotas) so a tenant flood cannot exhaust the host. A tenant is a *namespace*, not just a DSN: metrics, slow-log attribution, and fate events carry the tenant label (within §5's closed-vocabulary redaction rules).
- **Managed credential store support.** SPEC §18 already anchors DSNs as env/secret refs; the goal is first-class secret-manager resolvers (Vault, AWS Secrets Manager, Azure Key Vault, Kubernetes Secrets) with lease and rotation awareness — culminating in **zero-drop credential rotation**: new connections established on new credentials while old-credential connections drain on release, no restart, no dropped checkout (rides SPEC §7.7's recycling machinery). This extends SPEC §12's headline guarantee one level: credentials absent not only from PHP, but from static config files too.

### 4.2 HTTP transport engine — next in line

**Decided 2026-08-10 (P3): the HTTP transport engine ("Ferro HTTP") is the designated second engine**, ahead of the queue engine. Rationale against the test:

- **A.** Per-worker curl handles: 200 workers calling Stripe/OpenAI = 200 cold TLS handshakes, no HTTP/2 connection sharing, per-worker DNS caches. In the API-economy era this is the second-largest shared-nothing tax after the database.
- **B.** The slot is structurally empty. Transparent egress proxies face the HTTPS dilemma: tunnel `CONNECT` (no pooling, no visibility) or terminate TLS mid-path (MITM cert distribution nobody in PHP-land runs). Ferro dodges it the same way it dodged PgBouncer's transaction-to-socket coupling: PHP hands over *the request*, not a TCP stream, so ferrod originates TLS itself. "Owns both ends" pays a second time. (Note the analogy's limit: SPEC §7.4's session-mutation detection hole is *shared* with PgBouncer, not dodged — the spec says so explicitly.) Service-mesh adoption among PHP shops is approximately nil.
- **C.** The best seam in the family: Guzzle's swappable handler (`HandlerStack`) keeps the entire Guzzle API and middleware ecosystem intact above a ferro handler; PSR-18 and Symfony HttpClient are clean interfaces; Laravel's `Http` facade rides Guzzle.
- **D.** A timed-out non-idempotent `POST` is `Indeterminate{WriteUnconfirmed}` in an HTTP costume. Taxonomy mapping: `Retryable` (connect failure, 503 + Retry-After), `Indeterminate` (request dispatched, no response, non-idempotent method), `NonRetryable` (4xx). The auto-retry license maps onto RFC-idempotent methods and the `Idempotency-Key` header (IETF draft, Stripe-popularized) — the HTTP ecosystem independently invented the manifest `idempotent: true` flag.
- **E.** Engine owns transport, pools, TLS, retries, circuit breakers. PHP owns request construction, middlewares, cookies, auth flows, response semantics.

Two capabilities new to the family, not inherited:

1. **Structural SSRF elimination + API-credential isolation.** Named upstreams declared in `ferro.toml`, with their API keys attached daemon-side. A compromised PHP process can neither mint requests to arbitrary hosts nor read the Stripe key — it never had either capability. The manifest-only philosophy, second act.
2. **Streaming for the LLM era.** SSE/chunked responses ride the chassis's credit-based streams; bulk downloads ride memfd. Concurrent Fiber fan-out can await a DB query and two API calls over one UDS socket — inexpressible in any current PHP stack.

Scope bound for v1 of this engine: request/response + streaming bodies + named upstreams + fate taxonomy + per-upstream pools/breakers + per-upstream rate limits. Redirect-following, cookie jars, multipart conveniences, HTTP/3 stay in PHP or come later. Bound it ruthlessly; Guzzle middlewares carry the long tail.

**Launch positioning: the AI gateway (P9).** The loudest demand wave in PHP-land is FPM workers calling LLM providers, and it is a *profile* of this engine, not a new one: SSE streaming through the credit path, provider API keys in daemon custody, failover across named upstreams — plus **host-level rate limiting with zero extra infrastructure**: the daemon sees every worker's outbound calls, so per-upstream limits coordinate across all workers on the host automatically (today this requires a Redis-backed limiter and app-side discipline; provider limits are per-key/org, so host-level enforcement approximates them — honestly better, not structurally unique). The seam: `openai-php/client` and its peers accept any PSR-18 client at construction — the streaming path additionally uses the SDKs' stream-handler hook, still config-at-construction, SDKs otherwise unchanged. Remote AI gateways (Cloudflare AI Gateway, Azure APIM) also do key custody and base-URL drop-in — what they *cannot* do is the local hop: no WAN round trip on every call, and a client that authenticates by `SO_PEERCRED` instead of holding any gateway token at all. This demand profile *strengthens* P3: the ecosystem's #1 contender is a use-case of the engine already designated next.

**Also absorbed here:** gRPC client support (real enterprise pain — per-worker channels, the pecl install dance — but gRPC rides HTTP/2: an extension of this engine, never a peer).

**Trigger to start:** database engine at v1 (DBAL + Eloquent suites green), real production adopters, and the demand signal audible ("our API latency / our SSRF audit / our API-key sprawl / our OpenAI bill"). What would reopen the ordering: overwhelming unprompted queue pain from the first production users — the tiebreaker between HTTP and queues was always which pain users voice first, and evidence beats this doc.

### 4.3 Job transport engine — after HTTP

The queue engine ("Ferro Queue"). Same test, previously passed:

- Fronts **existing backends** — Postgres `SKIP LOCKED` queues first, Redis Streams second — reusing the pools and pin machinery ferrod already owns. It is not a broker and never owns a durable log (rule E; a queue engine with its own storage would be the first ferrod component that can *lose data*, a new failure domain refused on principle).
- Engine owns transport: leases, acks, retries, backoff, delayed delivery. PHP remains the worker — ferrod feeds jobs, never executes them.
- **The killer composition:** transactional enqueue. A job enqueued inside a transaction rides the same `tx_id` as the business write — job and data commit or roll back atomically. The outbox pattern made native; structurally impossible for Redis-backed queue stacks (the dual-write problem is why outbox exists). This is the first place two engines compose on one chassis, and it is the strongest argument for the chassis.
- Seams: Laravel Queue driver SPI, Symfony Messenger transport SPI. Acceptance bar mirrors the spec's G2 drop-in goal (upstream suites green over a Ferro connection): Horizon-equivalent workloads green through a ferro queue connection.
- Fate semantics: a lease that dies mid-job is a classified event; at-least-once vs at-most-once is a declared policy, not a Redis-timeout accident.
- **Scheduler duties fold in here (P9):** recurring/cron-style jobs and fleet-singleton execution (Laravel's `onOneServer`, currently a cache-lock convention) are queue-engine *features* — the daemon already holds the lease machinery and the backend locks. No separate scheduler engine, ever.

### 4.4 Streams — grows out of M5

LISTEN/NOTIFY streams ship in the M5 milestone, carried on the stream service id (`04` in the §2 tree) but delivered as a database-engine feature. "Streams" becomes a first-class *product* as subscription sources accumulate: Redis pub/sub (impossible under FPM today — a subscription held by ferrod, surfaced as a PHP iterator), Kafka consumer streams (§4.7), and — if sibling data services ever land (§4.6) — change streams. All ride the chassis credit flow.

### 4.5 Request dispatch — back pocket, explicitly optional

A RoadRunner-style worker-dispatch service (ferrod accepts HTTP, dispatches to supervised long-lived PHP workers over the same UDS). Architecturally elegant — but it fails test **B** (php-fpm, FrankenPHP, RoadRunner, Unit: the most crowded slot in PHP infrastructure, one incumbent foundation-backed) and strains **E**. Runtime-neutrality is Ferro's distribution advantage; FrankenPHP is a channel, not a rival. Revisit only as a v3-era *optional* convenience sold into installed infrastructure — and never via libphp embedding, in any scenario.

### 4.6 Sibling data services (MongoDB et al.) — expansion, not acquisition

SPEC §8 reserves the slot ("sibling services on the same framing"). MongoDB passes A (Atlas connection caps + per-worker libmongoc pools), B (no external pooler exists in that ecosystem), and E — and fails **C**: no DBAL-like SPI exists, so adoption is native-API-only. Verdict: justified only by existing Ferro customers who also run Mongo and want one daemon, one credential store, one console per host. Never a first act.

### 4.7 Kafka — the event-transport growth path (not "engine #4")

**Decided 2026-08-10 (P8): Kafka enters through the existing family, not as a new headline engine** — a transport backend for Ferro Queue (produce/consume via the Symfony Messenger and php-enqueue seams) plus consumer streams in Ferro Streams. It earns its own service id only if consumer-group semantics outgrow the queue model.

It passes the test unusually well:

- **A (extreme, workaround-validated):** Kafka producers are designed long-lived — batching, in-flight buffers, metadata caching. A per-request FPM producer is a documented anti-pattern (connection + metadata fetch per request, blocking `flush()` at request end, buffered messages lost with the dying worker), and consumer groups — persistent membership, heartbeats, rebalancing — are structurally impossible under FPM. The PHP ecosystem already routes around this with REST proxies and supervised CLI daemons: demand validated by workaround.
- **B:** Confluent REST Proxy / Karapace are centralized HTTP services with JSON overhead and no PHP-native seam — the transparent-proxy shape again. The host-local, co-designed-client shape is empty.
- **D (best transfer in the family):** Kafka's idempotent producer (sequence-numbered, broker-deduped) is a *protocol-licensed retry* — the same gift as MongoDB's `txnNumber`: safe auto-retry blessed by the server's own contract, no guessing. Scope honestly: the license lives and dies with the producer session (PID) — if ferrod itself dies, the license dies with it and the fate stays `Indeterminate` unless `transactional.id` is in play. Consumer offsets are lease/ack semantics.
- **E, with one explicit contract:** ferrod never ACKs a produce to PHP before the broker ACKs it — or before it is durably staged in a Postgres outbox table. In-flight buffering yes; durable ownership never.
- **C is the weak leg:** the seams are Symfony Messenger's transport SPI (Kafka transports are third-party userland, not core) and php-enqueue (low-maintenance these days); Laravel has no native Kafka surface. Part of the market therefore requires the native client API — the weakest C in the family.

Two headline features when it comes:

1. **Transactional produce** — the outbox pattern native: a produce inside a DB transaction rides the shared `tx_id`, staged atomically with the business write, shipped to Kafka post-commit by the engine. The dual-write problem is *the* correctness failure of event-driven PHP; two engines on one chassis dissolve it. (This is §4.3's transactional-enqueue composition, extended to event transport.)
2. **Rebalance-storm immunity** — ferrod holds consumer-group membership across PHP worker restarts, so deploys stop convulsing the consumer group.

**Trigger:** after HTTP and Queue prove the pattern; demand signal from event-driven shops.

### 4.8 Prospects watchlist

Candidates that passed enough of the §3 test to watch, but not enough to sequence. Watchlist status means: no design work, no spec lines, revisit when a trigger fires.

**Realtime/WebSocket push ("Ferro Realtime").** The biggest *sustained* framework demand — proven by Laravel building Reverb first-party after years of Pusher-bill and Soketi-ops complaints. FPM can't hold a socket, so **A** passes outright; the seam is excellent (**C**: Laravel's Broadcasting driver is config-only; Echo stays untouched client-side); and a Rust connection-holder fed events over UDS would beat a long-running-PHP process on every operational axis (memory ceilings, connection density). Two honest blockers keep it off the roadmap: **B is contested by a first-party incumbent** — the hardest kind — and, structurally, **Realtime inverts the family's direction**: every shipped engine fronts *backends* (outbound); this fronts *clients* (inbound), which is halfway to the request-dispatch back pocket (§4.5) and carries the same rule-E erosion risk. If it ever advances: fan-out plumbing only — presence, auth, and channel logic stay in PHP. **SSE is a transport of this same entry, not a separate prospect** — the easier half technically (HTTP-native, proxy-friendly, `Last-Event-ID` resumption built into the protocol), but with an even harder incumbent: Mercure (protocol-spec'd SSE hub, first-party Symfony integration, bundled inside FrankenPHP, same author — the channel-into-rival problem squared). Consuming SSE from upstreams is unrelated and already core to §4.2. The only differentiated angle if this entry ever advances: **Streams → SSE end-to-end** — Postgres `NOTIFY` (later Kafka consumer streams) already terminates in ferrod (§4.4); fanning it out to browsers gives database-to-browser live UI with no broker and no publisher glue, fed by engines the daemon already runs — a composition Mercure structurally requires a publisher to imitate. *Trigger: Reverb's operational ceilings become a documented ecosystem complaint at scale, or a production Ferro adopter asks for it unprompted.*

**Object-storage transport ("Ferro Storage").** Universal reach (every Laravel app touches the `Storage` facade), another config-only seam (**C**: Flysystem adapter SPI), plus S3/GCS connection+TLS pooling, multipart orchestration, and object-store credentials joining daemon custody. SPEC §5.1's large-payload machinery points in this direction but doesn't cover it yet: memfd is spec'd for bulk single-shot payloads, engine→client only, with one client-side copy (SPEC §5.1, §22) — an upload path and true zero-copy would be new spec work, budgeted when this advances, not inherited for free. Why it's a prospect and not a plan: presigned URLs already offload the heaviest path (client↔S3 direct), so the residual pain is real but not burning. If built, it is a **thin service riding Ferro HTTP's pools**, not a peer engine. *Trigger: Ferro HTTP shipped, and adopters moving large payloads through PHP ask for it.*

**Ferro State — the coordination service (P11).** Host-local, in-memory state over the UDS, holding only state that is **born in memory** — locks, semaphores, counters, presence, single-flight signals — never state *derived from a datastore* (derived = cache = refused, Ground rule 6; APCu/Relay/Redis own that path). It survives the two standing rules deliberately: the latency-inversion corollary **via its own exception clause** — these primitives are literally the clause's examples (a lease, a blocking read: state and semantics FPM structurally cannot hold); a plain KV op would *not* be licensed, which is exactly why the API is primitive-shaped (that it also replaces a Redis network round trip with a local UDS hop is a bonus, not the license). And rule E, because the contract is **epoch-scoped** — ephemeral by protocol, like APCu across a reload; nothing durable is promised, so nothing can be "lost." The slot is the unheld middle: richer than APCu (per-FPM-master shared memory, invisible to CLI queue workers, dumb KV), closer and liveness-aware where Redis is TTL-guesswork. The unique offer is the *combination* — host-spanning (FPM + CLI + Octane) *and* liveness-released *and* µs-local; each property alone has an incumbent, all three together do not. The moat is liveness: the daemon knows the instant a session dies, so **locks and leases release on holder death by session-death detection, not TTL expiry** — the advisory-lock experience for application locks, ending Redis TTL roulette (the entire Redlock controversy is downstream of Redis not knowing if you're alive). Cache-creep defense is structural: primitive-shaped API (`lock()`, `increment()`, `semaphore()`), tiny typed values, per-namespace quotas — no generic byte-bag, and deliberately **no** Laravel Cache-store driver; the seams are `Cache::lock()` and `RateLimiter` (both config-swappable). Blast-radius rule: caps and quotas are day-one design, not hardening — this memory shares a process with every DB connection on the host.

**Persistence doctrine — the durability ladder (P11).** Settled doctrine, filed here because it governs Ferro State. *The state class chooses the rung; the rung never chooses the state.*

- **D0 — process memory.** Locks and presence, forever: recovery of a dead holder's lock is a bug, not a feature.
- **D1 — survives crash and deploy, not reboot.** systemd **FDSTORE**: PID 1 holds a state memfd across daemon crashes and returns it on restart — zero disk, no recovery code (the same mechanism family as SPEC §18's socket activation). Plus epoch-to-epoch **state handoff** during graceful rollout: the draining instance transfers a snapshot to its successor, and the epoch contract stays intact because the new epoch receives state *explicitly*.
- **D1.5 — bounded-loss snapshots.** Atomic-rename snapshot files with an explicitly accepted loss window (HAProxy's `server-state-file` precedent). Licensed because the state class accepts the window for its own data; §5's "no stats file on disk" stands untouched — stats are derived history, and history belongs to Prometheus.
- **D2 — survives reboot.** No current occupant: nothing born-in-memory needs it once locks are excluded on principle. If a state class ever *proves* it does, the answer is a reserved internal database on SPEC §7.6's engine-owned SQLite machinery (WAL mode, serialized writer, online backup — already on the roadmap; zero new storage engines) — honestly re-accepting the two prices the ladder otherwise avoids (state surviving restarts weakens the clean-slate epoch story; SQLite fsyncs from ferrod's process), which is exactly why D2 stays empty until proven necessary.
- **D3 — survives machine loss.** *Is* the fronted backends, full stop. Any state an application would grieve was never host-state at all.
- **A hand-rolled WAL inside ferrod is refused** (P11): the refusal is about *building durability machinery* for state that doesn't need it — database gravity (fsync discipline, torn writes, compaction, then replication) and worse-Postgres one hop from Postgres — the same adjudication as P4, and it doesn't change for having a different name. If durability is ever truly required, it is *reused* (SPEC §7.6) or *fronted* (D3), never built. *Graduation path:* the primitives ship first as internal machinery (HTTP-engine rate limits and breaker state, queue leases), proving semantics with no public API to regret; a public service id comes after, via the `Cache::lock()`/`RateLimiter` seams. *Trigger: engines' internal use is proven and an adopter asks for host locks/limits unprompted.*

**Explicitly absorbed into existing family members:**
- *Scheduler/cron* → Ferro Queue features (§4.3, P9) — no engine, ever.
- *gRPC clients* → Ferro HTTP extension (§4.2, P9) — no engine, ever.
- *Coordination primitives* (rate limiters, semaphores, host-local locks) → the Ferro State entry above (P11, superseding P9's absorbed-as-features disposition for coordination): they ship first as engine internals and may graduate to the State *service*; cross-host coordination still needs a shared backend and stays behind fronted stores.
- *Full-text search access* (Elasticsearch/Meilisearch/Typesense) → HTTP upstreams under Ferro HTTP; Scout et al. keep working above the seam.
- *Mail/notification transport* → queued mail is already the ecosystem answer; rides Ferro Queue.

### 4.9 The actor door — north star, not roadmap (P11)

The specification of §1.1's layer 4. **It cannot be built the obvious way:** a shared heap of live PHP objects is physically impossible — workers are separate interpreters; object graphs don't cross process boundaries. Every system that delivered "state across requests" without shared memory used the same model instead (Erlang/OTP, Orleans, Cloudflare Durable Objects): **the requests move to the state** — one actor owns the object, everyone else sends messages, single-threaded per key, so no locks and no races by construction.

**The PHP-native actor door**, if it is ever opened: PHP owns the actors — worker-mode processes holding *live* objects (real models, real closures), handling messages one at a time. ferrod owns the substrate — the directory (key → activation), the exactly-one-activation-per-key invariant (the same class of guarantee as exactly-one-END), mailboxes over the chassis with credit backpressure, liveness via session-death detection, and **passivation**: idle actors serialize state to a fronted backend and evaporate; the address stays valid forever, activation is on-demand (Orleans' virtual-actor insight). What it would unlock: every use case PHP currently outsources to Redis-plus-races — live carts, collaborative documents, game sessions, per-user LLM conversation state, per-entity sagas — becomes a keyed PHP class with methods.

**Rules already settled for that door:**
- **Protocol, not product.** The substrate is spec'd language-neutral (an activation contract any runtime could implement — same discipline as the chassis), but positioning and the worker-lifecycle commitment stay PHP-native. The language-agnostic version of this product exists and is crowded (Dapr actors, Temporal, Restate's virtual objects); the *empty* slot is PHP-native — and the moat is structural: Dapr invokes actors as HTTP callbacks into stateless workers with **state rehydrated per call** — not live objects. Keeping a PHP object genuinely alive requires owning PHP worker lifecycle, the commitment no generic substrate will make.
- It **inherits §4.5's app-server adjacency caution** — routing messages to specific workers is specialized dispatch. The differentiator that licenses it where generic dispatch was refused: addressed stateful workers exist nowhere in PHP-land (B empty), unlike generic request dispatch (B crowded).
- **ferrod never hosts execution** — the "host the actors yourself for language-neutrality" move (Golem's WASM answer; Rivet's V8-isolate variant) is the runtime graveyard (§8) in a new coat. The daemon routes to runtimes; it never becomes one.

**Status: north star, not roadmap.** v3+ horizon, zero near-term milestones committed. The incremental path is the existing plan itself: engines (layer 2) → Ferro State (layer 3, whose liveness primitives are the actor substrate's organs) → single-flight (a degenerate actor) → directory and mailboxes. Each step ships standalone value; the last one is the door.

## 5. Ferro Dashboard — the fleet console

The Horizon lesson, correctly read: Grafana is a better dashboard, so don't build a dashboard — build a **domain-aware, actionable console**. It knows what a pin is, what an `Indeterminate` incident means, what a lease is; and it has buttons (drain a daemon, kill a pin, retry a failed job, flip a pool to session mode).

Architecture doctrine:

- **Stateless.** Two read paths, zero write paths: admin-service fan-out for *now* (discovery via k8s API / static list / DNS), Prometheus query API for *history*. Kill the console pod, lose nothing.
- **ferrod exposes stats, never stores them.** Consistent with SPEC §13, which only exports (OTLP traces, Prometheus metrics, slow log) and provides no storage. The specifics beyond §13 are *this doc's doctrine*, to be spec'd when the M2 observability slice lands: slow log as structured JSON to stdout/journald; bounded in-memory ring buffers (recent slow queries, recent fate events) for `ferro top` and the console's live view; no stats file on disk, ever — stats are storage too (rule E).
- **Redaction contract:** no DSNs or credentials anywhere; normalized fingerprints only, never raw SQL, in metrics/spans; params redacted by default (`log_params = never|on_error|always`); closed label vocabularies (no free-text labels → no PII drift, no cardinality explosions); admin/metrics bind loopback/UDS by default, mTLS/bearer + read-vs-operate roles when crossing hosts.
- One tab per **shipped** engine: Fleet (epochs, version/schema skew during rollout), Database (pools, checkout p99, pins-by-cause, `Indeterminate` incidents, hot fingerprints), then HTTP (upstreams, breaker states, per-API p99), then Queues (the Horizon-parity tab). Tabs are earned by shipping, not announced.
- Evolution path: admin service (M2+, versioned public contract) → `ferro top` TUI (M4) → fleet web console (post-v1).
- **The commercial seam lives here.** Engines and clients stay OSS — that's the wedge and the trust. Fleet-scale visibility and operations is what ops teams demonstrably pay for (the open-core seam Grafana and Redpanda settled on). Decision deferred; the doctrine is only *don't give it away by accident*.

## 6. Naming

| Product | Descriptor | Notes |
|---|---|---|
| Ferro Database | **database access engine** | the spec's own term; "query engine" is banned in public copy (implies planning/execution à la Trino — exactly what §3 forswears) |
| Ferro HTTP | **HTTP transport engine** | "transport" = what it owns; colloquial "HTTP engine" fine |
| Ferro Queue | **job transport engine** | "transport" is Symfony Messenger's own word for the layer; "management" is banned in the engine's name — managing is the Dashboard's job |
| Ferro Streams | event stream engine | later |
| Ferro Dashboard | fleet console | where "management" actually lives |

"Ferro" itself remains a placeholder pending the D7 trademark/naming check (SPEC §21).

## 7. Client tiers and deployment reach (post-v1)

- **Language expansion, in order:** Python (structurally identical pain — gunicorn/uWSGI process model, `CONN_MAX_AGE` ≈ persistent PDO; seams: SQLAlchemy dialect + Django ENGINE; PyO3 native codec), then Ruby (ActiveRecord adapter; Rails-on-PgBouncer pain is documented and chronic). Explicit non-customers: Go, Java, .NET, Elixir, long-lived Node — their in-process pools are fine, and the daemon buys them nothing.
- **Underserved niches the daemon uniquely fits:** short-lived processes (artisan/cron/pipeline steps get warm connections for free) and multi-language hosts sharing one pool budget and one console.
- **Deployment shapes:** host daemon (systemd socket activation), Kubernetes DaemonSet (hostPath socket), pod sidecar (emptyDir), ECS/Fargate sidecar, DB-adjacent TCP inside a VPC (`FERRO_ADDR`).
- **Never a hosted service.** A managed cloud offering inverts the performance story (the UDS is load-bearing; WAN hops multiply exactly what tx_id pinning optimizes), enters Hyperdrive/RDS-Proxy/Accelerate territory with half the playbook forsworn (no caching, no geography), and buys a control-plane/multi-tenancy/billing company nobody here is building. Distinguish two things: a **managed/hosted offering is refused outright** (§8, P6); a **self-hosted remote transport** — ferrod on a DB-adjacent host inside the customer's network, the existing `FERRO_ADDR` shape — is in scope and already licensed by SPEC §5. If that remote shape is ever marketed, the pitch is the fate taxonomy (the one Ferro asset that gains value over a network hop), and only after the taxonomy is proven in production.
- **Onboarding doctrine (every tier, every engine):** install daemon → declare pools/upstreams (credentials move daemon-side) → activate (socket unit) → composer require → one-line config diff → `ferro doctor` (names the exact failure: peer-cred allow-list, socket perms, registry-hash skew, pool reachability). Gradual by construction, reversible in one line. Target for the doctor command: the M2 era — it is *not* currently in SPEC §17's M2 scope, so committing to it means amending §17 (with a §22 note) per the charter's definition of done; the argument for pulling it forward is that the DBAL/Eloquent suite runs will hit every first-run failure before any user does.

## 8. The graveyard — deliberate refusals

Recorded so the reasoning survives; each was examined and refused, not overlooked.

| Idea | Verdict | Why |
|---|---|---|
| Hosted/cloud Ferro | refused | see §7 — inverts the perf story, occupied slot, control-plane company |
| PHP application server (replace php-fpm / FrankenPHP's Go layer) | refused | fails B (four incumbents, one foundation-backed) and E (an app server *is* the thing that runs PHP); forfeits runtime-neutrality, converts the best adoption channel into a rival |
| PHP runtime in Rust (drop-in) | refused | HHVM is the existence proof at maximal funding; the extension C-ABI + stdlib quirk surface is the moat, and PHP 8's JIT shrank the prize; real PHP time is I/O-bound — which is Ferro's slot |
| AOT-compiled PHP ("compiled build instead of JIT") | refused | HPHPc lost to HHVM's JIT *inside Facebook* — runtime type feedback beats static compilation of a dynamic language; full-PHP AOT must ship an interpreter anyway; dev/prod parity breaks; survivors either compile a subset (kPHP, Peachpie) or abandoned PHP for a JIT-run dialect (Hack) — either way, drop-in is surrendered |
| Result caching, SQL rewriting, engine-side retries, read/write inference, ORM-in-Rust | refused | charter Ground rule 6 / SPEC §3 — restated here only because every adjacent product does at least one of them, and their absence *is* the trust story |
| Redis access engine | refused | fails A (Redis connections are cheap; phpredis persistence already amortizes them) and the latency-inversion rule (§3 corollary: the daemon hop ≈ the cost of a cache `GET`); B contested by Relay, whose in-process shared-memory architecture is *correct* for the cache path. The valuable slivers are already family backends: Redis Streams under Ferro Queue (§4.3), pub/sub under Ferro Streams (§4.4). Stampede coalescing on hot keys refused as caching-adjacent (Ground rule 6) |
| Metrics/log storage, stateful aggregators | refused | most occupied slot in ops; §13 exports to the customer's plane; the console stays stateless |
| Owning durable storage for user/domain data in any engine (broker logs, stats files) | refused | rule E; ferrod fronts backends — a component that can lose data is a new failure domain refused on principle. Born-in-memory coordination state is governed separately by §4.8's durability ladder (P11): D1.5's bounded-loss snapshots and the reserved D2 rung on SPEC §7.6's SQLite machinery are the licensed exceptions — a hand-rolled WAL never is |

## 9. Adjacent tracks (not family members)

Same thesis — "PHP is a modern platform when the parts around the interpreter are built like it's 2026" — different chassis, different market:

- **Rust type checker for PHP** (the ruff→ty playbook, act two): PHPStan-compatible semantics at watch-mode speed, LSP fallout. The one adjacent idea whose **window can close** — Python's precedent is loud, and Mago has the parser groundwork. Worth a warm design doc; not worth diluting v1.
- **Erased generics via the checker** — TypeScript's move: syntax enforced at build time, invisible at runtime, no Zend RFC required. Rides the checker.
- **`ext-php-rs` maturation** — conditional: if the D12 gate pulls the accelerator into scope (SPEC §21 D12 — pending the bare-metal re-run sign-off), Ferro becomes a dogfooding contributor to Rust-as-PHP-extension-language.
- **FrankenPHP guidance** — Ferro's second-best adoption channel (classic mode = FPM story verbatim; worker mode = hygiene fixes its footguns; no Swoole conflict → the M3 Fibers tier stays fully available, unlike Octane-on-Swoole). When M5's runtime guidance is written (currently scoped as "Octane guidance" in SPEC §17 — widen it then), it should include a *tested* FrankenPHP configuration example: the ZTS-thread + Fiber + UDS-client interaction gets a testkit run before any positive claim ships in docs. Treat as channel, never rival.

## 10. Go-to-market — the Laravel pitch

The path into the Laravel ecosystem is documented precedent, not speculation: Octane shipped **first-party support for RoadRunner** — a third-party daemon, written in Go, by an outside company — because it made Laravel apps measurably better and had community traction first. FrankenPHP repeated the pattern. The door for "external daemon, first-party integration" exists; the doctrine below is how to walk through it without wasting the one first impression.

**The pitch is one product and one sentence.** *"Your Eloquent app, one config line, 5× fewer database connections, and no write ever fails silently — reversible in one line."* Database access engine only.

**The deck rule (load-bearing).** Laravel is a funded company with first-party products, and this doc contains three future collisions: Ferro Queue ↔ Horizon, Ferro Dashboard ↔ Pulse, Realtime ↔ Reverb. Lead with any of those and Ferro is a competitor asking for distribution. Ferro Database threatens nothing they sell and improves everything they host — the one purely complementary family member. The family vision stays in this repo, out of the deck.

**Two prerequisites, both non-negotiable:**

1. **The `illuminate/database` integration suite green through a Ferro connection** (the M2 acceptance bar) before any contact. The core team's first questions are predictable — `chunk()`/`lazy()`, transactions with `attempts:`, Telescope visibility — and "the entire upstream suite passes, here's CI" ends them. Pitching at M1 wastes the shot.
2. **A credible maintenance story.** RoadRunner had Spiral behind it. A daemon in the app's critical path with a bus factor of one is the objection that kills the deal politely. The open-core shape (§5) must be real enough to say out loud — company, sponsorship, or partner — before the pitch.

**Channels, in order:**

1. **Bottom-up (the RoadRunner path).** Ship `ferro/laravel` publicly: benchmark write-up vs `PDO::ATTR_PERSISTENT` on a realistic FPM fleet, chaos demo as video, Laravel News + ecosystem podcasts. Traction *is* the pitch — Octane integrated RoadRunner because people were already using it.
2. **Laracon.** The culturally native launchpad for Laravel infrastructure. Demo choreography: a live connection-count graph collapsing 200→8 as workers migrate with one config line; `kill -9` ferrod mid-transaction → one typed exception, epoch reconnect, app carries on; flip the config line back to exit. Fifteen minutes, three screenshot moments.
3. **Direct — two different doors.** Technical: Taylor/core, only after traction. Commercial: the **Laravel Cloud / Forge platform team** — Laravel Cloud runs serverless Postgres under FPM-shaped workloads and lives the §1 problem at platform scale with a margin attached; host-level pooling with correct hygiene = more tenant density per Postgres compute unit. That's a COGS pitch, not a DX pitch, and it can fund the work. Forge angle: "one checkbox provisions ferrod."

**Objections to hold answers for:** another-daemon-to-babysit (static binary, socket activation, Forge/Cloud provisions it invisibly; FrankenPHP normalized binaries-next-to-PHP) · debugging-through-a-layer (it's the inverse: `queue_us`/`exec_us`, `ferro top`, typed taxonomy) · Octane (sync under Swoole per SPEC §10.1; expected-full under FrankenPHP *once the §9 testkit run verifies it* — hold only the verified claim at pitch time) · local dev (ferrod runs on macOS/Herd-land; Windows via TCP fallback, D4) · session-state gotchas (SPEC §7.4 named upfront, `pin_functions`, session-mode escape hatch) · who-maintains-this (prerequisite 2).

**The ask is a ladder, never a leap:** traction noticed → ecosystem blessing (Laravel News) → first-party driver integration à la Octane-RoadRunner → Cloud/Forge infrastructure partnership. Never open above rung one; RoadRunner earned each rung in order.

**Pre-pitch assets:** the benchmark write-up; the chaos video; and the **compat report** — the top-50 Laravel packages run against a Ferro connection, published as a matrix, with the D8 breakages (`schema:dump`, dump-based backups) documented with workarounds. Nothing disarms an ecosystem gatekeeper like having found your own incompatibilities first.

## 11. Product decision log

| # | Date | Decision | Reopens if |
|---|---|---|---|
| P1 | 2026-08-10 | Category: host-local access engine owning both ends of the wire; wedge-then-expand; family earned one engine at a time | — |
| P2 | 2026-08-10 | Admission test (§3) governs all family candidates | — |
| P3 | 2026-08-10 | **HTTP transport engine is next in line** after database-engine v1; queue engine follows | first production users' unprompted pain is queues, not HTTP |
| P4 | 2026-08-10 | Queue engine fronts existing backends (PG `SKIP LOCKED`, Redis Streams); never owns a durable log; transactional enqueue is its headline | — |
| P5 | 2026-08-10 | Dashboard: stateless, domain-aware, actionable; ferrod exposes stats, never stores; commercial seam lives at the fleet console, decided later | — |
| P6 | 2026-08-10 | Refusals in §8 stand; none is revisited without new structural facts | facts change (e.g., an incumbent vacates a slot) |
| P7 | 2026-08-10 | Naming per §6; "query engine" and "queue management engine" banned in public copy | D7 naming outcome |
| P8 | 2026-08-10 | Kafka enters via existing engines (Queue transport backend + Streams consumer streams); produce is never ACKed to PHP before broker ACK or durable outbox staging. Redis-as-access-engine refused (§8); Redis remains a family *backend* | consumer-group semantics outgrow the queue model → Kafka gets its own service id |
| P9 | 2026-08-10 | Demand scan: AI gateway is Ferro HTTP's launch positioning (a profile, not an engine — reinforces P3); scheduler duties fold into Ferro Queue; gRPC folds into Ferro HTTP; Realtime and Storage go to the §4.8 watchlist with named triggers; search/mail absorbed as features (coordination's absorbed-as-features disposition superseded same-day by P11 — see §4.8 Ferro State) | a watchlist trigger fires (§4.8), or Reverb-scale realtime demand arrives from a production adopter |
| P10 | 2026-08-10 | Laravel go-to-market per §10: pitch Database only (deck rule — Queue/Dashboard/Realtime stay out); no contact before the Illuminate suite is green AND the maintenance story is credible; channels bottom-up → Laracon → direct, with Laravel Cloud/Forge as the commercial door; the ask climbs the ladder one rung at a time | Laravel Inc. ships or announces a competing first-party pooling layer, or a partnership opportunity arrives before M2 (then the prerequisite gate is re-weighed, not skipped) |
| P11 | 2026-08-10 | North star recorded (§1.1; actor door specified in §4.9): a multi-request lifecycle for PHP in four layers; the actor door is PHP-native, protocol-not-product, v3+ with zero near-term commitments. Ferro State enters the §4.8 watchlist with the born-in-memory contract, liveness moat, durability ladder (D1 via systemd FDSTORE + epoch handoff), and internal-first graduation; a WAL inside ferrod is refused (P4's adjudication, restated) | State's internal-machinery phase disproves the liveness semantics, or a fronted backend closes the liveness gap (e.g., a Redis primitive that releases locks on client death) |
| P12 | 2026-08-10 | Configuration-layer feature goals committed for the database engine (§4.1): per-tenant pools (template + daemon-side tenant→DSN map, lazy/reaped), dynamic tenant loading (no-restart catalog, hard bounds, tenant-as-namespace), managed credential stores (Vault/ASM/AKV/k8s resolvers, lease-aware, zero-drop rotation). Demand-sequenced, post-v1; SPEC amended per charter when each is built | a v1 adopter needs tenant pools sooner — pull-forward is allowed, dilution of v1 scope is not |
