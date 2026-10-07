# SPEC §24 — Ferro Queue: the job transport engine

**Status:** normative, adopted 2026-10-06 (M7-G0, SPEC §22.2 (cn)). This file is §24 of
`ferro-spec-v0.2.md`, which carries a stub pointing here; it has the same authority as the rest of
the spec, and the charter's definition of done applies to it unchanged. Its decisions are recorded in
SPEC §21 as **D21** (retry licences per service, one entry shared with §23; this section's licences
are §24.6) and **D22** (the §3 / charter-rule-6 scope exception for the engine's closed statement
set). The choices the draft left open were decided under the owner's full-freedom grant as applied 2026-10-06 (ledger, "Owner directives and grants"),
and are listed in §24.17. **D22 was RATIFIED by the owner on 2026-10-06, with two amendments**
(SPEC §22.2 (de)). Because it amends binding scope text (§3, charter rule 6), no G-slice code
(G1 onward) could start before ratification; **G1 may now start.** The amendments are applied in
place below: (a) a store's table defaults to `ferro_jobs`, so mixed mode becomes an explicit
opt-in (§24.3); (b) a store declares a `KIND`, `sql` being the only v1 kind, and the token is opaque
bytes on the wire, 1 to 1 024 bytes (§24.3, §24.4, §24.5). **`job_id` is opaque bytes too** (SPEC
**D24**, owner decision 2026-10-06, settling open item O-G1), so every QUEUE shape is kind-neutral
before G1 freezes it (§24.3, §24.4).

**Allocations.** This section allocates service id `QUEUE = 7`, the error codes `LeaseLost = 0x300F`
and `PoolMismatch = 0x3010` (both NonRetryable), and the registry constant
`queue_wait_grace_ms = 1000`, after §23's (`HTTP = 6`, `0x1007`, `0x1008`, `0x300D`, `0x300E`).
The service id and both codes are **reserved in `/proto` as comments only** (beside `[services]` in
`methods.toml`, and in the reserved-codes block of `errors.toml`), with no keys, so no generated
constant and no registry-hash change follows, and no other change may take these numbers. The keys,
the constant, golden vectors and both codecs land with slice G1, the engine's first code slice
(§24.14). Ferro Queue is not behind a cargo feature and
takes no engine feature bit.
*[Amended M7-G1a (SPEC §22.2 (df)): **keyed.** `QUEUE = 7`, `[methods.queue]` (`ENQUEUE = 1` … `CLEAR = 7`),
`LeaseLost = 0x300F`, `PoolMismatch = 0x3010` and `queue_wait_grace_ms = 1000` are real `/proto` keys with
golden vectors and both codecs, and the reservation comments are gone. G1a also keyed what the shapes
needed: `InvalidHandle = 0x3011` (NonRetryable; §24.3 prerequisite (c)), the `[ack_outcome]` table
(`ACKED = 1`, `GONE = 2`, so neither codec hand-writes ACK's byte), and the three shape bounds both
decoders enforce, `queue_handle_max_bytes = 1024`, `queue_enqueue_max_jobs = 1000` and
`queue_reserve_max_queues = 16` (a bound a RECEIVER enforces becomes a `/proto` key, M6-F2's rule). The
messages are `/proto/PROTOCOL.md` **§14**, not §13: M3-D4's COPY took §13 first. The registry hash moved;
no `protocol_version` bump.]*

**Read first:** §24.16 (how each conflict with existing text was resolved), §24.17 (the decisions
and the premises not yet measured) and §24.15 (what v1 does not build). Review-finding ids such as
"F13" and "R1" are provenance; the appendix lists them.

## 24. Ferro Queue — the job transport engine (M7, D16)

Ferro Queue is the second engine on the chassis (product-vision §2). It is a `/proto` service whose
verbs move **jobs** between PHP producers and PHP workers. Jobs are rows in a table in the
application's own database, reached through a pool `ferrod` already owns.

- The engine owns the **transport**: leases, fenced acknowledgement, release with a client-chosen
  delay, waiting without polling from PHP, and composition with a business transaction.
- PHP owns the **job**: what the payload means, which class runs it, tries and backoff policy,
  chains, batches, middleware, failure storage, and running it.

v1's drop-in seam is Laravel's queue (§24.11). The Symfony Messenger transport is post-v1 (§24.15,
§24.17 Q4).

### 24.1 Admission (product-vision §3), in writing, against the current tree

**A — shared-nothing really hurts. Holds; the size is UNMEASURED until G3.** Ferro already carries
Laravel's `database` worker (§15 demo, §22.2 (ch)). An idle worker sleeps `--sleep` s (default 3),
then polls with `DatabaseQueue::pop()`: a BEGIN, `SELECT … FOR UPDATE SKIP LOCKED`, UPDATE and COMMIT
transaction, pinned under Ferro for its whole duration.

- At the default, 200 idle workers run about 67 pinned transactions/s that find nothing. A job waits
  up to 3 s before anyone sees it.
- Lowering `--sleep` buys latency with load, linearly.
- A worker that dies holding a job strands it for `retry_after` (default 90 s).
- Messenger's PostgreSQL path can wait on LISTEN (UNVERIFIED: the Doctrine transport's
  `use_notify`). **Through Ferro that path gets nothing.** Under transaction-mode pooling the assist
  lexer taints a LISTEN (§7.1), hygiene runs `UNLISTEN *` (§7.2), and no notification reaches PHP
  before M5's LISTEN streams. How Messenger degrades (presumably to its polling timeout) is a
  premise of the post-v1 Messenger transport (§24.15).

**B — the slot is empty. Holds, and the incumbent is Ferro's own drop-in path.** The stock `database`
driver already runs through `ferro-*`, transactional enqueue included. Ferro Queue earns its place
only by what an in-process driver **cannot** do:

1. **Wait on the host** without holding a connection or polling per worker (§24.8).
2. **Fence acknowledgement**, removing a hazard that exists in stock today, read from source:
   - `deleteReserved()` deletes `where('id', $id)` without checking that the caller still holds the
     job;
   - `deleteAndRelease()` re-inserts even when the row is gone, which makes a third copy;
   - so a worker past its `retry_after` deletes a job a second worker is running;
   - and its `fail()` writes a `failed_jobs` row for that job (`Job::fail()`'s `finally`).
   Ferro removes the last two only together with the `FerroJob::fail()` override (§24.11). Fencing
   covers the job's ROW. It does not cover side effects Laravel performs before the final ACK, such
   as chain dispatch and batch bookkeeping (§24.7).
3. **Host-level metrics, spans and fate classification** for jobs (§24.9).

B does not rest on fast release of a dead worker's job: liveness release is not in v1 (§24.15).

Horizon's Redis queues and external brokers are not host-local and cannot ride a SQL transaction.
No PHP-native host-level job transport exists. PgBouncer sees statements, not jobs.

**C — a drop-in seam exists. Holds for Laravel, which is v1's drop-in tier.**

- **Laravel:** `QueueManager::addConnector`. Adoption is one driver value, `driver = 'ferro'`, plus the shipped `ferro_jobs`
  migration or `TABLE=jobs` on the store (§24.3). The auto-discovered `FerroServiceProvider`
  registers the driver (§22.2 (ch)).
- **Symfony (post-v1):** `TransportFactoryInterface` with a `ferro://` DSN, plus a bundle line
  (UNVERIFIED that nothing else is needed). The Messenger transport is demoted out of v1 (§24.17 Q4).

**D — the correctness moat transfers. Holds; it is the strongest leg.**

- A lost autocommit ENQUEUE or RESERVE is `Indeterminate`, with a licensed re-send where one exists
  (§24.6).
- A late acknowledgement from a holder whose lease was taken over is refused with a known fate
  (`LeaseLost`) instead of being silently applied.
- PDO models none of this.

**E — the engine/PHP boundary stays clean. Holds by construction (§24.2).**

**Latency inversion (§3 corollary). Passes.**

- ENQUEUE is an INSERT and ACK is a DELETE.
- A waiting RESERVE is a blocking read holding a lease.
- No verb is a sub-100 µs cache-shaped operation.

### 24.2 Model and invariants (normative)

- **I1 — Rows are the whole truth.** A job's state is its row: present or absent, `queue`, `payload`,
  `attempts`, `available_at`, `reserved_at` and `created_at`.
  - Everything the engine holds in memory is epoch-scoped (§19.1) and is an optimisation: wait sets
    and wake hints.
  - After a `ferrod` restart at any instant, the rows alone determine every job's state, its next
    delivery and the validity of every token.
- **I2 — The engine never runs, parses or rewrites a job.** It never decodes the payload or reads a
  job's class, tries, timeout or backoff (charter rule 6). It refuses a payload only on size and on
  NUL (§24.4), never on content.
- **I3 — Engine-authored SQL only, against operator-declared tables.**
  - The client names a STORE (operator-declared) and a queue (a bound value). It never names a table,
    a column or SQL. The engine composes its statements per dialect.
  - A single engine DML statement runs through the **guarded** `Checkout::query`/`exec`, so the
    boundary guard, the classifier and the pin authority (§7.1) see it exactly as they see a user
    statement.
  - An **engine-owned transaction** (a multi-statement verb outside a client transaction) opens with
    `begin_tx_with(compose_begin_sql(dialect, READ COMMITTED, false))`. It closes with
    `commit_tx`/`rollback_tx`, the TX service's own pin hooks, on one checkout.
  - Inside a client transaction, every statement goes through the TX actor (§24.5).
  - No user statement is ever read, changed or inferred from. The scope exception this needs is
    D22 (C8).
- **I4 — No engine retries (charter rule 3).** The engine never re-executes a statement after it
  failed or its fate is unconfirmed, and ends each request with one classified terminal. A parked
  RESERVE's repeated sweeps are polls after an empty success, not retries (§24.8). **Redelivery after a lease expires is a property of stored state:** a
  row past its deadline satisfies the predicate, and the next RESERVE (a new request) selects it.
  There is no reaper and no timer that moves rows. In v1 the engine initiates exactly two kinds of
  write. Neither re-executes anything, and neither is retried if it fails:
  1. **Unreserve of an undelivered reservation** (§24.8). A job the engine leased for a request whose
     terminal was never handed to a live session's writer is restored to exactly its pre-reservation
     state. No client ever held its token, so no fate is unknown to anyone.
  2. **Dedup-key purge** of expired rows in the operator-created dedup table (§24.6).

  This list is closed, and D22 covers only what it names. A third kind, **liveness release** (freeing
  a delivered job early because its worker's session ended badly), is **not in v1** (§24.15, §24.17
  Q2). It is not an analogue of `abort_session`, which can only make an effect *not* happen: it can
  enable a **second execution** of a job whose first may still be running, on an inference the
  engine cannot verify ("the session ended badly, so the worker is dead"). Adding it needs its own
  §21 decision and an amendment of this invariant.
- **I5 — Exactly one END per request (charter rule 4),** including a parked RESERVE.

### 24.3 Storage

**Stores.** A store is one table in one pool plus its lease and wait policy. It is declared in the
engine, and PHP names it.

```
FERRO_QUEUE_STORES=jobs
FERRO_QUEUE_JOBS_KIND=sql                # the default and the only v1 kind; anything else is refused
FERRO_QUEUE_JOBS_POOL=main               # required (sql kind)
FERRO_QUEUE_JOBS_TABLE=ferro_jobs        # the default; [schema.]identifier, validated at load;
                                         # `jobs` = stock's table, i.e. mixed mode (below)
FERRO_QUEUE_JOBS_LEASE_S=90              # = Laravel's retry_after default; minimum 2
FERRO_QUEUE_JOBS_POLL_MS=1000            # coalesced poll interval (§24.8)
FERRO_QUEUE_JOBS_MAX_WAIT_MS=30000       # ceiling on a RESERVE's wait
FERRO_QUEUE_JOBS_WAKER_STMT_TIMEOUT_MS=5000
FERRO_QUEUE_JOBS_MAX_PAYLOAD_BYTES=4194304
FERRO_QUEUE_JOBS_LABELLED_QUEUES=default,emails
FERRO_QUEUE_JOBS_DEDUP_TABLE=            # unset = dedup keys refused
FERRO_QUEUE_JOBS_DEDUP_TTL_S=86400
FERRO_QUEUE_JOBS_DEDUP_PURGE_MS=60000    # independent of the depth sampler
FERRO_QUEUE_JOBS_DEPTH_SAMPLE_MS=30000   # 0 = off
```

**Config-load refusals.** Each is fatal for that store only, and is logged by store name, never with
a DSN:

- a `KIND` other than `sql`. Store kinds exist so that Redis Streams or SQS can be added after v1
  without a shape change (D22 amendment (b)). Each such kind needs its own §21 decision, and v1 builds
  none, so any other value is refused as unsupported in v1, never ignored. That way no operator
  believes a broker-backed store is running;
- an unknown pool;
- a pool whose family is unsupported, or below the version gate;
- a table identifier that is not `[A-Za-z_][A-Za-z0-9_]*`, optionally schema-qualified;
- `LEASE_S < 2`;
- `MAX_PAYLOAD_BYTES` above `max_frame_payload` minus the reply envelope;
- any `LIVENESS_RELEASE` key: liveness release is not in v1 (§24.15), and a key that names it is
  refused rather than ignored, so no operator believes it is on.

When read-only pools exist (§7.6, M4), a store on one is refused too. Today none exist, so the rule
waits.

*[Amended M7-G1a (SPEC §22.2 (df)): **built, in `ferro-queue`'s `config`, with these decisions.** The
loading discipline is `ferro-http`'s (§22.2 (cw)): a blank value reads as unset; an unknown key under a
declared store disables that store (a typo such as `…_LEASE=30` must not leave the lease at 90 s);
keys are matched exactly, so `jobs` and `jobs_high` both resolve; a `FERRO_QUEUE_*` variable no store
owns is logged at `warn`; a store name is 1–64 bytes of `[A-Za-z0-9._-]`. Every key that STARTS with
`LIVENESS_RELEASE` is refused. G1a adds refusals of values with no meaning: a zero `POLL_MS`
(it would spin), `WAKER_STMT_TIMEOUT_MS`, `MAX_PAYLOAD_BYTES`, `DEDUP_TTL_S` or `DEDUP_PURGE_MS`, and a
`LABELLED_QUEUES` entry that is not a queue name. **An identifier part is at most 63 bytes**, because
PostgreSQL silently truncates a longer one to `NAMEDATALEN - 1`, so the store would address another
table; and **the identifier is used verbatim and quoted** (`"schema"."table"`, `` `t` ``), as Laravel's
schema builder quotes the tables it creates, so `TABLE=Jobs` means the case-sensitive `"Jobs"`. The
"reply envelope" for `MAX_PAYLOAD_BYTES` is `sql_reserve_envelope()`, 1 111 bytes, an itemised upper
bound on a one-job RESERVE terminal minus its payload, pinned against the real encoder.]*

**Version gate.** PostgreSQL ≥ 12 (the `MATERIALIZED` CTE, §24.4), MySQL ≥ 8.0.1 and MariaDB ≥ 10.6
(`SKIP LOCKED`). SQLite is not supported in v1 (§24.15). The gate is checked at first use against the
pool's existing version probe (§22.2 (u)). *[Built M7-G1a: `PoolRegistry::server_version` reads the
same cache `HELLO_ACK` advertises, starting the probe if none is running. A version that does not parse
FAILS CLOSED. A pool whose version is unknown (an unreachable backend, a failed probe inside its
back-off) refuses the verb as `ConnectionLost` (Retryable): nothing was sent. MariaDB is told from MySQL
by the substring `mariadb`, as the Doctrine tier does. Only PostgreSQL stores pass the gate live in G1a;
MySQL-family stores are refused before it until G6. *Review round:* the wait for an in-flight probe is
bounded by the REQUEST's deadline and CANCEL as well as by the probe's own 1.5 s budget (the store's
verdict lock is held across it); an unknown version is logged once per probe back-off window (5 s) per
store, not once per request; and a gate REFUSAL is reused only for the probe's TTL (600 s), the lifetime
of the version it was decided on, so a backend upgraded under a running `ferrod` is noticed without a
restart.]* **Cost:** on PostgreSQL this is stricter than Laravel's own
9.5 gate. PG 9.5–11 are out of support; PG 11 reached end of life in 2023. Accepted (§24.17 Q8).

**The layout is Laravel's stock `jobs` table, unchanged, and it is v1's only layout** (decided,
§24.17 Q1). **Its default name is `ferro_jobs`** (owner decision 2026-10-06, D22 amendment (a)).
`jobs.stub` defines:

- `id` bigIncrements;
- `queue` string, indexed;
- `payload` longText;
- `attempts` unsignedTinyInteger (`tinyint unsigned` on MySQL, `smallint` on PG);
- `reserved_at` unsignedInteger, nullable;
- `available_at` and `created_at` unsignedInteger.

All times are Unix seconds.

Reasons for the stock layout, by weight:

1. **Gradual, reversible adoption, when the operator asks for it.** With the table set to `jobs`,
   switching between `database` and `ferro` with jobs in flight works, and stock workers can drain
   the same table, with conditions (mixed mode, below).
2. **D18 parity is reachable at all.** Upstream asserts `DB::table('jobs')->count()`, so the D18
   engine column sets the table to `jobs` (§24.12).
3. **No new layout.** Under the default name an adopter creates one table of a layout Laravel already
   defines. Under `jobs` the adopter creates nothing.
4. **The fence needs no new column.**

**Why the default name is `ferro_jobs`, not `jobs`** (D22 amendment (a)):

- **No silent sharing.** With `jobs` as the default, a `ferro` store and a stock `database` queue on
  the same database would share one table without anyone choosing it. Stock workers would then take
  and delete Ferro's jobs with PHP-clock leases and unfenced deletes. That is mixed mode, which is
  supported only under a condition (`retry_after == LEASE_S`) nobody would have checked.
- **Not `_ferro_jobs`.** A leading underscore is unusual in Laravel schemas, and some tools treat
  such a name as hidden.
- **Cost:** zero-configuration gradual adoption is lost. Moving from `database` to `ferro` takes one
  migration (`ferro_jobs`, which `ferro/laravel` ships) or one setting (`TABLE=jobs`, which is mixed
  mode). Jobs already queued in `jobs` are not seen by a store on `ferro_jobs`, so the operator either
  drains `jobs` with stock workers first or sets `TABLE=jobs`.

**Cost, stated:**

- Leases and delays have one-second resolution.
- The attempts ceiling is 255 on MySQL and 32 767 on PG.
- The lease is per store.
- The time columns overflow in **2038 on PostgreSQL** (signed `integer`) and in **2106 on
  MySQL/MariaDB** (`INT UNSIGNED`). This is Laravel's defect, inherited.
- A Ferro-native layout is post-v1 and would be a second layout, not a replacement.

**The fencing token is `(id, attempts, created_at)`.**

- **Minting.** Every reservation increments `attempts` under the row lock. `created_at` is fixed for
  the row's life.
- **Validity.** A token is valid iff a row exists with that `id`, `attempts` and `created_at`.
  - It therefore survives lease expiry until someone else reserves the job, so a late ACK is honoured
    when nobody else took the job.
  - Unreserve does not invalidate a delivered token: it touches only tokens nobody holds.
- **On the wire** the token is **opaque bytes, 1 to 1 024 of them** (msgpack `bin`). It was an opaque
  `u64` until D22 amendment (b), and was changed before G1 froze the shapes. The client never
  interprets a token: it receives it in a `RESERVE` reply and sends it back unchanged.
  - **Minting is the store kind's.** The `sql` kind mints **8 bytes**, packing `created_at` (the low
    32 bits of the stored value) above `attempts` (16 bits), as the `u64` did. The byte layout is
    internal to the `sql` kind and is fixed at G1 by its builders' tests. A future layout, or a future
    kind (a Redis stream entry id, an SQS receipt handle of about 1 KB), mints differently without a
    wire change.
  - **A token the store cannot decode** (for the `sql` kind, any length other than 8) is refused
    before any statement. G1 decides and pins its terminal, under prerequisite (c) below. *[Decided
    M7-G1a: `InvalidHandle` (`0x3011`, NonRetryable). The `sql` decoder also refuses an 8-byte token whose
    top two bytes are not zero: no engine mints one, so it is a client defect, and refusing it makes
    the token canonical too. A token of 0 or more than 1 024 bytes is refused by both CODECS
    (`Protocol` on the engine), before the store is consulted.]* `LeaseLost` would be literally true, since
    the token names no current reservation, but only a client defect produces such a token, and the
    Laravel tier treats an autocommit `LeaseLost` as done (§24.11), which would hide the defect. A
    token over 1 024 bytes is an out-of-bounds field, refused like any other, and G1 pins where.

**The job id on the wire is opaque bytes too** (SPEC D24, owner decision 2026-10-06). `job_id`
(and `RELEASE`'s `new_job_id`) is **1 to 1 024 bytes** (msgpack `bin`) in all six wire positions
(§24.4), because a Redis stream entry id (two 64-bit integers) or an SQS message id (a UUID string)
does not fit an `i64`. The client never interprets it.

- **The `sql` kind encodes the row's `bigint` `id` internally**, and decodes it back before any
  statement. Its byte encoding is fixed at G1 by the builders' tests. *[Decided M7-G1a: **canonical
  decimal text**, exactly `i64::to_string()`'s ASCII bytes (1 to 20 of them; a `-` only for a negative
  id). The decoder accepts a byte string iff re-encoding the value it parses gives the same bytes, so
  `007`, `+7`, `-0` and ` 7` are `InvalidHandle`. It lets G5 hand Laravel stock's digits and keeps a
  `job_id` safe in JSON and logs. **Cost:** up to 20 bytes where a fixed binary form is 8.]* A decimal-text encoding would
  let the Laravel tier hand Laravel the same digits stock does (§24.11), and G1 weighs that when it
  chooses the encoding.
- **The fence is unchanged.** It is still `(id, attempts, created_at)` over the row's columns
  (above), and the engine decodes `job_id` and the token into those columns. A `job_id` the store
  cannot decode is refused before any statement, under the same rule as an undecodable token, and G1
  pins its terminal.
- **Ordering:** rows are still served best-effort FIFO by `id` inside the database (§24.7), but the
  wire carries **no numeric order**. A client may not compare or sort job ids, and must not infer
  enqueue order from them.
- **Cost:** a few bytes per job on the wire, and no numeric ordering for the client.

**G1 prerequisites for the opaque fields (normative; review round of (de)).** *[All three met at
M7-G1a (SPEC §22.2 (df)): (a) the decimal-text encoding above, with `ferro-queue`'s
`a_replayed_id_is_byte_identical`; (b) `/proto/vectors/refusal/`, 24 vectors, refused by both codecs
for their own reason; (c) `InvalidHandle`.]*

- **(a) A canonical `sql` `job_id` encoding with a strict decode.** Each `bigint` id has exactly one
  encoding. The decoder refuses every non-canonical form (for a decimal-text encoding: leading zeros,
  a sign, surrounding spaces). Otherwise two byte strings would name one row. A dedup replay
  (§24.6) returns a `job_id` byte-identical to the first send's, and G1 pins this with a test.
- **(b) Refusal vectors at both bounds.** Each opaque field gets a 0-byte refusal vector beside the
  1 025-byte one, refused by both codecs.
- **(c) The terminal for an undecodable token or `job_id` is neither `Protocol` nor `LeaseLost`.** It
  is not `Protocol`, because the frame is well-formed: that code means a wire fault. It is not
  `LeaseLost`, because that would hide a client defect behind "did nothing" (§24.11). G1 may allocate
  a new NonRetryable code for it, after `0x3010`. If it does, the code lands in `/proto` with
  vectors and both codecs, and G1 records its choice either way.
- **Precondition, stated rather than enforced: an `(id, created_at)` pair is never reissued while a
  token for it is outstanding.**
  - `TRUNCATE … RESTART IDENTITY`, MySQL `TRUNCATE`, sequence resets, restores and async-replica
    promotion can reissue ids. `created_at` makes a collision need the same id within the same
    second.
  - The residual is documented: do not truncate, restore or promote under live workers. CLEAR
    (§24.4) is a DELETE and resets nothing.
- **Epochs.** Tokens live in rows, so they survive a `boot_epoch` change. This is a deliberate
  exception to §19.1 (C3).

**The clock is the database's.** Each verb computes `now` **once**, as integer seconds:

- PostgreSQL: `floor(extract(epoch from statement_timestamp()))::bigint`, in the verb's first
  statement. That is statement time, not the wall-clock `clock_timestamp()`.
- MySQL/MariaDB: `UNIX_TIMESTAMP()`, which is statement-start time truncated to seconds.

A multi-statement verb binds the first statement's `now` into the rest.
*[Amended at the M7-G1b review (SPEC §22.2 (dg), F3): **on PostgreSQL the only multi-statement
autocommit verb is a RESERVE naming several queues, and it is not one unit.** Its queues are tried in
order, one statement each, and the FIRST that reserves anything answers alone (§24.4) — so each queue's
statement is its own unit with its own `now`, and every job in a reply was reserved by ONE statement
under ONE `now` (one `reserved_at`, one `lease_deadline`; pinned live). A later queue's statement reads
a later clock, which can only make more jobs available and is never early. No `now` is bound across
them, because an empty statement would have to report its `now`, restructuring the statement this
section's rescan premise was measured on. The binding rule stands for MySQL's multi-statement
transaction (G6), where one unit really is several statements.]*

Rounding rules (normative):

- **Availability:** a job is available iff `reserved_at IS NULL AND available_at <= now`.
- **Delay:** `available_at = now` when `delay_s = 0`, else `available_at = now + 1 + delay_s`. That is
  `ceil` of the real instant for any instant with a fractional second. A delay `d` therefore matures
  in [d, d+1) s after the INSERT (plus wake latency) and is **never early**. **Cost:** stock Laravel
  matures in (d−1, d], so Ferro is up to 1 s later.
- **Lease expiry:** a reservation is expired iff `reserved_at < now - lease_s`. The effective lease is
  therefore in (L, L+1] s and **never shorter than declared**. Stock's `<=` gives (L−1, L].
- *[Amended M7-G1b (SPEC §22.2 (dg)): **a `delay_s` whose `available_at` would overflow the time
  column is refused before sending** (carried from the G1a review; measured: PostgreSQL otherwise
  answers `22003` AFTER the statement was sent). The check cannot read the database's `now` before
  anything is sent, so it reads the ENGINE's wall clock and keeps a one-day margin
  (`DELAY_CLOCK_MARGIN_S`): `delay_s` is refused (`Unsupported`, "nothing was sent") iff
  `engine_now + 1 + delay_s + 86 400` exceeds the column's ceiling (`integer` on PostgreSQL). ENQUEUE
  checks every job, RELEASE its one; `delay_s = 0` is never refused. **Residual, stated:** a database
  clock more than a day ahead of `ferrod`'s lets a delay within a day of the ceiling through, and
  PostgreSQL refuses it with `22003` — a known non-execution (the INSERT is atomic), never a wrong or
  partial write. **Cost:** a delay landing within a day of 2038-01-19 is refused although it would
  have fit.]*
- **`lease_deadline`** in replies is `reserved_at + lease_s + 1`, the first DB second at which another
  RESERVE may take the job.

**Mixed mode** means stock `DatabaseQueue` workers and Ferro workers on one table. **It is an
explicit opt-in:** the store's `TABLE` is set to the stock connections' table, normally `jobs`,
because the default `ferro_jobs` is never a stock table (D22 amendment (a)). It is supported and
tested (chaos row 10) only when the stock connections' `retry_after` equals `LEASE_S`.

- Stock workers stamp and compare with the **PHP** clock, so a stock worker may take a Ferro-held job
  up to (PHP-to-DB skew + 1 s) early.
- "No worse than stock" is true for skew only under that equality.
- Stock RELEASE inserts a new `id`, as Ferro's does, so FIFO position after a release matches.

**Who creates the table: the application's migrations, never the engine.**

- Engine DDL would make `ferrod` a schema owner, needing DDL privileges and racing migrations.
- Laravel apps already have a stock `jobs` table, but the default store table is `ferro_jobs`
  (D22 amendment (a)). `ferro/laravel` therefore publishes a **`ferro_jobs` migration**: stock
  `jobs.stub`'s layout, unchanged, under the Ferro name. It also publishes one optional migration
  for the dedup table. An app that sets `TABLE=jobs` (mixed mode) needs neither table migration.
- Everyone else gets the DDL from `ferro queue schema --store <name> --dialect <pg|mysql>`, for the
  store's configured table (`ferro_jobs` unless `TABLE` says otherwise). The CLI prints it and never
  runs it.

**Shape verification.** At a store's first use in each `boot_epoch` the engine reads
`information_schema.columns` once and checks names and types. `ferrod` has no configuration reload
in v1 (§23.6.1, §18), so a migration that changes the table's shape is re-verified at the next
restart; until then a verb that fails against the changed table is classified like any statement.

- On a mismatch, every verb on that store answers `Unsupported`, naming the column.
  *[Amended M7-G1a (SPEC §22.2 (df)): built for PostgreSQL (MySQL's statement lands with G6). The
  accepted types are the stock layout's exactly — `id bigint`, `queue character varying` or `text`,
  `payload text`, `attempts smallint`, `reserved_at`/`available_at`/`created_at integer` — because the
  `sql` token carries `created_at` in 32 bits and `attempts` in 16, so a wider column would make tokens
  collide; `reserved_at` must be nullable; extra columns are allowed. **An absent table is cached only
  briefly** (`ABSENT_RECHECK`, 2 s): the ordinary deploy order is "start `ferrod`, then migrate", so a
  table created after boot is served within two seconds, while a missing table costs one catalog read
  per store per two seconds rather than a checkout per request. A table that exists with the wrong shape
  IS cached until the next restart, as written above. *Review round (M1):* the table is resolved the way
  the verbs' statements will resolve it — `to_regclass($1)` on the QUOTED identifier, so an unqualified
  `TABLE` follows the pool's whole `search_path` (the first version looked in `current_schema()` only and
  called a table in the path's second schema absent) — and the columns are read from `pg_attribute` by
  that oid, with `format_type()` names. The relation must be a table: `relkind` `r` or `p` (partitioned)
  passes; a view, materialized view or foreign table with exactly the right columns is refused, naming
  its kind, because the verbs are written for a table: a view need not be updatable or lockable, and a
  foreign table's locking belongs to the remote server. Every returned column is
  cast to `text`, so the decode does not depend on catalog domain types.]*
  *[Amended M7-G2 review round (SPEC §22.2 (di), F5): **verification now FIXES the relation**. The
  schema it resolved (through the POOL's `search_path`) is cached with the verdict, and every verb's
  statement names `"<schema>"."<table>"`, autocommit and tx-scoped alike. So "the way the verbs'
  statements will resolve it" holds by construction, and no session's `search_path` (an application's
  `SET LOCAL` inside its transaction) can steer a verb to a relation that was never verified. The
  pool's `search_path` is read once per store per process, like the shape (no reload in v1).]*
- The engine also resolves the identity default (PG `pg_get_serial_sequence`) for diagnostics only.
- It never repairs the table.
- *[Amended M7-G1b (SPEC §22.2 (dg)): **`id` must be UNIQUE** (carried from the G1a review): a fence
  `WHERE id = $1 AND attempts = $2 AND created_at = $3` over duplicate ids would match several rows, so
  one ACK could delete two jobs. The same catalog statement now also reports whether some index on the
  relation is unique, VALID (a failed `CREATE UNIQUE INDEX CONCURRENTLY` leaves an invalid one),
  IMMEDIATE (a `DEFERRABLE` constraint admits duplicates until commit), not partial, not on an
  expression, and has exactly one key column, `id` (`INCLUDE` columns allowed). Without one the store is
  refused (`Unsupported`, "column id is not unique"), cached like any wrong shape. Measured on
  PostgreSQL 16 for each of those cases and for a partitioned table's primary key (which passes).
  **An ordinary table with INHERITANCE children is refused too** (review F1): a unique index does not
  reach a child created `INHERITS (jobs)`, every statement (no `ONLY`) reaches the child's rows, and
  measured, one ACK deleted the same `(id, attempts, created_at)` from parent and child, then reported
  `Indeterminate`. A PARTITIONED table stays accepted: PostgreSQL requires a unique index on it to
  include every partition-key column, so a unique index on `id` alone means it is partitioned by `id`
  and uniqueness holds across its partitions (measured: a duplicate across hash sub-partitions is
  refused, `PRIMARY KEY (id)` on a table partitioned by `queue` is refused, an index built `ON ONLY`
  the parent is invalid, and neither a partitioned table nor a partition can be an inheritance
  parent). Residual, as for every check here: a child added AFTER verification is not seen until the
  next `boot_epoch`; a fence matching two rows is then refused as an unreadable result
  (`Indeterminate`), never reported as one job.]*

**Indexes.** Laravel's `(queue)` index is the minimum. Whether a composite or partial index pays is a
G3 bench question (charter rule 5). The engine never creates an index.

**Failed jobs are not the engine's. There is no FAIL verb.**

- Laravel's failer (and, post-v1, Messenger's `failure_transport`) stays above the seam.
- Native users get an atomic dead-letter move by composition: ACK plus ENQUEUE in one transaction
  (§24.5).

### 24.4 `/proto` shape

- **Service.** `QUEUE = 7`, after HTTP's `6`, allocated by this section and entered in `/proto` at
  G1. §5's service row carries it.
- **New codes.** Both are NonRetryable known fates, allocated after §23's set and entered in
  `/proto` at G1:
  - **`LeaseLost` (`0x300F`).** A fenced verb's token names no current reservation. The verb did
    nothing.
  - **`PoolMismatch` (`0x3010`).** A tx-scoped verb named a store whose pool is not the
    transaction's. It is refused before any statement, so the transaction is untouched.
  - An unknown store reuses `Unsupported`, as an unknown pool does.
- **New registry constant.** `queue_wait_grace_ms = 1000` (top level of `methods.toml`, generated
  into both codecs). It is the engine's bound past `wait_ms` on a parked RESERVE's terminal (§24.8).
- **No `protocol_version` bump.** A new service and new messages change no existing shape, and the
  registry hash refuses a skewed pair (the C3-7b precedent). **Every QUEUE shape is frozen at G1.**
  Any later shape change bumps the version (PROTOCOL.md §8). The `liveness` field is therefore in
  RESERVE from G1 although v1 builds no consumer for it: a v1 engine refuses `liveness: true` with
  `Unsupported` before any statement, and every v1 client sends `false`. A post-v1 liveness release
  (§24.15) then needs no version bump.

Shapes are positional msgpack arrays with strict arity. Field order lands with the golden vectors at
G1 (PROTOCOL.md §14 — *[amended M7-G1a: §13 went to M3-D4's COPY]*; §12 is HTTP's, §23). `common` is `[tx_id|nil, timeout_ms|nil, traceparent|nil]`, and `traceparent` is
parsed as on EXEC (§22.2 (cd)). Every success terminal carries `stats {queue_us, exec_us}`.
`token` and `job_id` (with `new_job_id`) are opaque bytes, 1 to 1 024 of them, everywhere they
appear (§24.3; D22 amendment (b) for the token, D24 for the id). The golden vectors carry each at its
`sql`-kind size and at 1 024 bytes, and both codecs refuse 0 and 1 025 (§24.3's G1 prerequisites). The PHP side holds each as an
opaque string and must send it back as `bin`. Encoding it as `str` would change the wire type and
could fail the codec's UTF-8 check.

| method | request | success terminal | `tx_id` |
|---|---|---|---|
| `ENQUEUE = 1` | `[store, jobs: [[queue, payload: str, delay_s: u32]] (1..=1000), dedup_key: str\|nil, common]` | `[job_id: bin (1..=1024)\|nil, inserted: u32, deduplicated: bool, stats]`. `job_id` is non-nil iff exactly one job | yes |
| `RESERVE = 2` | `[store, queues: [str] (1..=16, priority order), max_jobs: u16, wait_ms: u32, liveness: bool, common]` | `[jobs: [[job_id: bin (1..=1024), token: bin (1..=1024), attempts, queue, payload, created_at, lease_deadline]], stats]`, possibly empty | **refused** (`Unsupported`) |
| `ACK = 3` | `[store, job_id: bin (1..=1024), token: bin (1..=1024), common]` | `[outcome: u8 (1 acked, 2 gone), stats]`. `gone` is never returned in a transaction | yes |
| `RELEASE = 4` | `[store, job_id, token, delay_s, common]` | `[new_job_id: bin (1..=1024)\|nil, stats]`. `nil` = `gone` (autocommit only) | yes |
| `EXTEND = 5` | `[store, job_id, token, common]` | `[lease_deadline, stats]` | yes |
| `SIZE = 6` | `[store, queue, common]` | `[pending, delayed, reserved, oldest_pending_at: i64\|nil, stats]` | yes |
| `CLEAR = 7` | `[store, queue, common]` | `[deleted: u64, stats]` | yes |

*[Frozen M7-G1a (SPEC §22.2 (df); layouts in `/proto/PROTOCOL.md` §14). Where the table above names no
type, G1a fixed one: `attempts` is `u32`; `created_at` and `lease_deadline` are `i64` (Unix seconds, signed
because PostgreSQL's `integer` is); `pending`, `delayed`, `reserved`, `deleted` and both `stats` fields are
`u64` bounded below 2^63, so PHP reads native ints; SIZE's `oldest_pending_at` is `i64 | nil` (the
smallest pending `available_at`, Laravel 12's `creationTimeOfOldestPendingJob()` — added in the G1a review
round after checking `illuminate/queue` v12.69.3); `common.tx_id` is a `u64` decoded exactly as
`ExecRequest.tx_id` is (any `u64`; PHP cannot send one ≥ 2^63);
`common.traceparent` decodes lossily, as on EXEC. ACK and EXTEND share one request shape
(`[store, job_id, token, common]`), and SIZE and CLEAR another (`[store, queue, common]`). The bounds in
the table (`bin (1..=1024)`, `(1..=1000)`, `(1..=16)`) are enforced by BOTH decoders, so breaking one is
`Protocol`; an ACK `outcome` outside `[ack_outcome]` is refused on decode too. **A queue name is 1 to 255
characters without U+0000** — the stock `string('queue')` width, in characters as both families count
it; the bound is what makes the RESERVE reply envelope finite — and is refused `Unsupported` before any
checkout, as the "queue name" refusal below says. Until the slices that build them land, a RESERVE with
`wait_ms > 0` (G3), a dedup-keyed ENQUEUE (G4), a tx-scoped verb (G2) and any verb on a MySQL-family store
(G6) are refused `Unsupported` before any checkout, never silently served without the behaviour asked for.]*
*[Amended M7-G2 (SPEC §22.2 (di)): tx-scoped ENQUEUE, ACK, RELEASE, EXTEND, SIZE and CLEAR are SERVED
(§24.5); a tx-scoped RESERVE stays refused `Unsupported`, for good, and is refused before its `tx_id` is
even resolved.]*

**Verb semantics.**

- **ENQUEUE.** Inserts `attempts = 0`, `reserved_at = NULL`, `created_at = now` and `available_at`
  per §24.3. It is one multi-row INSERT, atomic on every dialect.
  - **Refused before any statement:**
    - a payload that is not valid UTF-8;
    - a payload containing **U+0000**, on every dialect: PG `text` refuses it with 22021, and refusing
      everywhere keeps a table portable across mixed families;
    - a payload over `MAX_PAYLOAD_BYTES`;
    - a `dedup_key` on a batch of more than one job.
  - **Ids.** A batch returns `inserted` only. MySQL has no `INSERT … RETURNING`, and `first_id + i`
    is wrong under `auto_increment_increment > 1` and unguaranteed under `innodb_autoinc_lock_mode = 2`.
    Laravel's `push`/`later` need one id, and `bulk()` returns `bool` (`DatabaseQueue.php:147-161`),
    so nothing in the seams needs batch ids.
- **RESERVE.** It serves one queue at a time in the given order. On PostgreSQL it is one autocommit
  statement:

  ```sql
  WITH n AS MATERIALIZED (SELECT floor(extract(epoch FROM statement_timestamp()))::bigint AS s),
  c AS MATERIALIZED (
    SELECT t.id FROM t, n
    WHERE t.queue = $q AND t.attempts < $ceiling AND octet_length(t.payload) <= $max
      AND ((t.reserved_at IS NULL AND t.available_at <= n.s) OR t.reserved_at < n.s - $lease)
    ORDER BY t.id LIMIT $k FOR UPDATE OF t SKIP LOCKED)
  UPDATE t SET reserved_at = n.s, attempts = t.attempts + 1
  FROM c, n WHERE t.id = c.id
  RETURNING t.id, t.attempts, t.created_at, t.queue, t.payload, n.s + $lease + 1
  ```

  - **Rescan premise.** The `MATERIALIZED` locking CTE is evaluated once, so the statement never
    locks or updates more than `$k` rows. G1 must **assert** this under concurrency (affected ≤
    `$k`, no row returned twice across concurrent reservers), not assume it.
  - **MySQL/MariaDB** run an engine-owned READ COMMITTED transaction on one checkout:
    `SELECT id, UNIX_TIMESTAMP() … ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED`, then an UPDATE of
    those ids binding that `now`, then a SELECT of the reserved rows, then COMMIT.
  - **Frame clamp.** `max_jobs` is clamped so the reply fits one frame:
    `floor((max_frame_payload − envelope) / (MAX_PAYLOAD_BYTES + per-job overhead))`, where the
    per-job overhead counts the store kind's largest `job_id` **plus** its largest token. For the
    `sql` kind that is the maximum length of G1's `job_id` encoding plus 8; for any kind it is never
    more than 2 048 (1 024 + 1 024, D22 (b) and D24). **Cost:** at the
    4 MiB default that is 3, so batch RESERVE is nearly inert. An operator who wants batches lowers
    `MAX_PAYLOAD_BYTES`. Laravel always asks for 1.
  - **Over-size rows.** Rows over `MAX_PAYLOAD_BYTES` (from stock producers) are never reserved and
    are counted as stuck (§24.9).
  - **Waiting.** A non-zero `wait_ms` parks the request (§24.8).
  - *[Amended M7-G1b (SPEC §22.2 (dg)): **one queue answers.** The queues are tried in the given
    order, one statement each on one checkout, and the FIRST queue that yields any job answers the
    request; later queues are not tried. Every job in a reply therefore comes from one queue, and a
    failure while serving queue *i* is that one statement's fate, because the queues before it
    reserved nothing. **Cost:** a `max_jobs = 5` request may receive two jobs from `high` while
    `default` has more; the client asks again (Laravel asks for one). **The clamp is applied before
    the statement, as its `LIMIT`,** and is itemised exactly (`checks::reserve_limit`: a fixed part of
    27 bytes and 1 084 per job at worst, the `sql` kind's 20-byte `job_id` plus 8-byte token
    included), pinned against the real encoder as TIGHT — the clamped count of maximum-size jobs fits
    one frame and one more does not. **The statement is §24.4's exactly**, with `$ceiling` =
    `smallint`'s 32 767 (`attempts < 32 767`, so the reservation's `+ 1` fits; a row AT the ceiling is
    never reserved again); its `RETURNING` rows are sorted by id before they are delivered (the
    statement promises no order), and more rows than the `LIMIT` is refused as an unreadable result
    rather than delivered.]*
- **ACK.** `DELETE … WHERE id = ? AND attempts = ? AND created_at = ?`.
  - **Autocommit:** affected = 1 is `acked`. Affected = 0 runs an unlocked probe of the id:
    - absent → `gone`, which is success-equivalent;
    - present with a different token → `LeaseLost`;
    - present with the same token (a concurrent delete committed after the statement snapshot) →
      `gone`.
    On PG the probe is in the same statement (a data-modifying CTE). On MySQL it is a second
    autocommit statement. `gone` and `LeaseLost` are both "did nothing", so a race that misreports
    one as the other affects only a counter.
  - **In a transaction:** affected = 0 is **always `LeaseLost`** (R1). The probe is not run.
- **RELEASE.** Re-inserts the job under a **new** `id` with `attempts` kept, `reserved_at = NULL`,
  `created_at = now` (as stock's `pushToDatabase`), and `available_at` per §24.3. It deletes the old
  row, fenced.
  - **PostgreSQL:** one statement,
    `WITH d AS (DELETE … fenced … RETURNING queue, payload, attempts) INSERT … SELECT … FROM d
    RETURNING id`.
  - **MySQL/MariaDB:** `SELECT … FOR UPDATE` (fenced; none → stop), then `INSERT … SELECT` from the
    locked row, then a fenced `DELETE` (must affect 1). Outside a client transaction this runs in an
    engine-owned READ COMMITTED transaction. Inside one it runs as one actor command (§24.5).
  - **No match:** `nil` (`gone`) in autocommit and `LeaseLost` in a transaction.
  - A new id sends a hot-failing job to the back of the queue, as stock does.
  - *[Amended M7-G1b (SPEC §22.2 (dg)): **"no match" runs ACK's probe.** This section says a RELEASE
    with no match answers `nil` in autocommit, and §24.6's table says a stale token's autocommit RELEASE
    answers `LeaseLost`; both hold, because the PostgreSQL statement probes the old id exactly as ACK's
    does: absent, or present with the SAME token (a concurrent delete) → `nil`; present with another
    token → `LeaseLost`. One statement: the fenced DELETE feeds the INSERT, and the probe reads the
    statement's snapshot.]*
- **EXTEND.** `UPDATE … SET reserved_at = now`, fenced. It renews by one full `lease_s`. It also
  retakes a job whose lease expired when nobody else took it, because the holder is evidently
  alive. No match → `LeaseLost`.
- **SIZE** counts by state. Laravel's `size()` returns the sum.
  *[Amended M7-G1b (SPEC §22.2 (dg)): the states are Laravel 12's exactly (illuminate/queue v12.69.3,
  read from source): `pending` = `reserved_at IS NULL AND available_at <= now`, `delayed` =
  `reserved_at IS NULL AND available_at > now`, `reserved` = `reserved_at IS NOT NULL` — **an expired
  lease still counts as reserved**, as stock counts it, although RESERVE would take it — and
  `oldest_pending_at` = the smallest pending `available_at`. One statement, one row even for an empty
  queue. CLEAR deletes every row of the queue, reserved ones included, as stock's `clear()` does.]*
- **CLEAR** deletes the queue's rows. It is data-plane and has no admin gate.
- **Isolation (normative).**
  - Engine-owned transactions are READ COMMITTED, composed and pinned as above.
  - PostgreSQL single-statement autocommit verbs inherit the role's or database's default isolation.
    That is the operator's setting, and this is documented. Under SERIALIZABLE, contention surfaces
    as 40001 instead of a skipped row. A 40001 is a known non-execution (Retryable), so correctness
    holds and only throughput changes.
  - Tx-scoped verbs run at the application's isolation (§24.5).
- **Routing.** Queue verbs always run on the store's primary pool. They are never routed to a replica
  (§7.6, M4).

**One END per verb, and the one CANCEL/delivery rule (normative).**

- Each verb is one request ending in one terminal.
- Refusals (unknown store, payload, queue name, a tx-scoped RESERVE, `PoolMismatch`) are declared
  before any checkout.
- For a RESERVE, **a job is DELIVERED iff the terminal carrying it is handed to a live session's
  writer.** In every other case the engine **unreserves** it (§24.8): the session has begun teardown,
  the waiter's wait deadline plus grace passed first, or the request was cancelled by session
  teardown.
- A client `CANCEL` on a live session follows §5.2. The terminal is `Cancelled` if no reservation
  statement has completed for the request. Otherwise it is the raced result, and the client must
  accept the jobs it carries.
- No path both unreserves a job and delivers it. G3 pins this with a mutation-tested race test.
- *[Amended M7-G1b (SPEC §22.2 (dg)): **until G3 builds unreserve, an autocommit RESERVE whose terminal
  is raced by session teardown leaves its reservation to expire** — a stock-equivalent phantom attempt
  of the kind §24.7 lists — rather than restoring it. G1b's RESERVE never waits, so the window is one
  statement long. A CANCEL observed before a RESERVE statement is SENT (while the verb waits for its
  connection, or between two queues) is classified by the shared fate matrix as an unsent write:
  `Retryable{ConnectionLost}`, nothing reserved (the connection-wait case is pinned live; the
  case of a CANCEL that has arrived before a later statement is sent — between two queues — is pinned
  by a unit test on a fake backend, which a review showed was possible where this text first said it
  was not); the `Cancelled` terminal above is the parked case, G3's.]*

**No push or STREAM delivery in v1.** The seam pulls. Streaming would lease jobs ahead of
consumption, and the leases would run down in a client buffer.

### 24.5 Transactional composition — the headline

**Rule.** ENQUEUE, ACK, RELEASE, EXTEND, SIZE and CLEAR accept a `tx_id`. With one, the verb runs on
the transaction's pinned connection through the TX actor, and its outcome is the transaction's
outcome. In-transaction ENQUEUE plus a business write is the outbox pattern, made structural.

**Only a `sql` store in the transaction's pool** (D22 amendment (b)). Transactional composition
exists because the store's rows live in the same database as the business write, on the same
pinned connection. So it is available only for a store of kind `sql` whose pool is the
transaction's pool (step 2 below refuses any other pool with `PoolMismatch`). A future non-SQL kind
(Redis Streams, SQS) has no transaction to join: it refuses every tx-scoped verb before any
statement, as a tx-scoped RESERVE is refused today (`Unsupported`). That holds unless a later §21
decision adds an outbox, which would stage the job in the transaction's database and forward it
after COMMIT. v1 builds only the `sql` kind, so in v1 this rule refuses nothing new.

**Mechanism.**

1. **Resolve the transaction** with the SQL service's `resolve_active(tx_registry, tx_id,
   session_id)`. It is made `pub(crate)` (R2), and the owner check is unchanged. A missing or
   foreign `tx_id` is `TxNotFound`, and a tombstoned one is `TxDeadline`, as on EXEC.
2. **Check the pool.** Compare the store's pool with `TxHandle.pool`. A mismatch is `PoolMismatch`,
   before anything is sent. Cross-pool transactional enqueue is impossible by construction.
3. **Send one actor command per verb:** `TxCommand::Queue { steps, timeout_ms, cancel, after_commit,
   reply }`.
   - `steps` is the verb's whole engine-authored statement list, built by the pure `ferro-queue`
     builders. Each step carries a stop condition, such as "the previous step matched no row".
   - The actor runs the steps **back to back, with no other command of the same transaction
     interleaved**. That makes a MySQL RELEASE's SELECT/INSERT/DELETE (or a dedup ENQUEUE's
     statements) atomic with respect to the transaction's other requests, including a `ROLLBACK_TO`
     a Fiber sends meanwhile (F17).
   - On PostgreSQL every verb except dedup ENQUEUE is a single step.
   - Everything that holds for `TxCommand::Exec` holds unchanged: serialisation behind earlier
     commands, `timeout_ms` and CANCEL rolling the transaction back and tombstoning it (`TxDeadline`,
     Retryable), session-death abort, deadlines, and `fate::classify_fate` with `in_tx: true`.
4. **`after_commit`** carries the verb's wake hint (§24.8). The actor keeps it only if every step
   succeeded. It applies it only on a successful COMMIT and drops it on rollback, abort or deadline.
   - A `ROLLBACK_TO` that undoes a step leaves at worst a stale hint, which costs one empty poll.
   - Hints are never correctness.

This is **two additions** to the actor: the `Queue` command and the after-commit list.

*[Amended M7-G2 (SPEC §22.2 (di)): **built on PostgreSQL**, with these choices the text above left
open or the implementation forced:*

- ***Order of the pre-send steps.*** *The per-request refusals of §24.4 (a tx-scoped RESERVE first, for
  good), then step 1 (`resolve_active`, unchanged), then step 2 (`PoolMismatch`), then the store's
  first-use verification (§24.3), then step 3. `PoolMismatch` therefore precedes any checkout,
  verification's included — pinned live against a store whose pool nobody listens on.*
- ***First-use verification runs on a SEPARATE checkout***, never inside the application's
  transaction: a failed catalog statement there would abort it, and at REPEATABLE READ its snapshot is
  the application's. It happens once per store per process. On a pool whose every connection is pinned
  it waits within the request's deadline and answers `PoolTimeout` (nothing sent, the transaction
  untouched). ***And every verb names the relation verification RESOLVED, schema-qualified***
  *(review F5).* A separate checkout resolves an unqualified `TABLE` through the POOL's
  `search_path`, while the transaction's connection carries the APPLICATION's session state. The first
  version named the configured bare `TABLE` in its statements, so an application's
  `SET LOCAL search_path` inside its transaction steered an in-transaction ENQUEUE into another
  schema's `ferro_jobs`, a table the engine never verified and no worker serves (reproduced live).
  Every statement, autocommit included, now names `"<resolved schema>"."<table>"`, so no session's
  `search_path` can steer a verb off the verified relation. The catalog's schema name is quoted with
  its quote characters doubled. Pinned live: two schemas, the in-transaction verb lands in the
  verified one, and the autocommit control does too.
- ***The deadline, precisely.*** *The request's one `timeout_ms` is consumed first by the store's
  first-use verification. What is LEFT of it when the command is sent to the actor is the verb's
  bound. That bound's timer starts when the actor STARTS the verb, exactly as `Exec`'s per-statement
  timer starts when the actor runs the statement. So time the command spends queued behind the
  transaction's earlier commands is NOT counted, and the request's deadline is overrun by exactly
  that queueing time (`Exec`-identical). The deadline covers verification plus the verb, not the
  queue between them. Pinned live (review F2, mutation mA): a verb behind a ~500 ms verification wait
  with `timeout_ms = 1200` answers at the request's 1 200 ms (measured 1.204–1.208 s over three runs),
  not ~1 700 ms. If no time is left after verification → `PoolTimeout`, unsent, the transaction
  untouched. The transaction's own `max_tx` bounds a verb MID-statement too (pinned live with
  `max_tx` = 800 ms, mutation mS).*
- ***A CANCEL before dispatch leaves the transaction intact (review F4).*** *A CANCEL that arrives
  while the verb waits behind the transaction's earlier commands is checked BEFORE anything is
  dispatched, and is answered `Cancelled` (NonRetryable): nothing sent, the transaction untouched. This
  is the no-time-left rule's reasoning applied to a cancel. A CANCEL that arrives after dispatch rolls
  the transaction back (`TxDeadline`). Both sides are pinned live. **Asymmetry, stated:** a tx-scoped
  EXEC still dispatches and then rolls back on such a CANCEL. Changing EXEC is out of this slice's
  scope; it is a ledger follow-up.*
- ***Steps.*** *Every PostgreSQL verb is ONE step, so G2 builds no per-step stop condition: the actor
  runs the steps back to back, stops at the first failing step, and a stop condition such as "the
  previous step matched no row" lands with the first verb that has more than one step (G4's dedup
  ENQUEUE, G6's MySQL RELEASE). The in-transaction ACK and RELEASE are their own builders
  (`ack_in_tx`, `release_in_tx`): the fence without the probe (R1).*
- ***The actor decodes.*** *The verb's result is decoded INSIDE the actor, before it serves the
  transaction's next command, because two outcomes act on the transaction and nothing may slip in
  between: an applied verb's wake hint is kept for COMMIT, and a WRITE whose effect cannot be reported
  rolls the transaction back (SPEC D25, §24.6 amendment).*
- ***The hint comes from the result, not the request.*** *A RELEASE request carries no queue — the
  queue is in the row — so the in-transaction RELEASE returns the new row's queue, and the hint is
  produced by the decode: one per distinct queue of an ENQUEUE (§24.8 trigger 1 names "an ENQUEUE",
  any delay), one for a RELEASE only at `delay_s = 0`, none for ACK, EXTEND, SIZE, CLEAR, a verb that
  failed, or a `LeaseLost`. A `ROLLBACK_TO` leaves a stale hint (pinned live); ROLLBACK, a failed or
  lost COMMIT, abort and deadline drop them. ***So does a COMMIT of an ABORTED block*** *(review
  F1). PostgreSQL answers it with the tag `ROLLBACK` and NO error, so the first version fired the
  hints of jobs that never committed. The tag is not exposed (the backend's `simple_query` discards
  it), so the actor reads the pin authority, the RFQ status byte, BEFORE sending the COMMIT, and drops
  the hints when it is `E`. A block recovered with `ROLLBACK TO` reads `T` and fires (both pinned
  live). The CLIENT is told such a COMMIT succeeded, as `pdo_pgsql` tells it. That predates G2 and is
  not changed here.*
- ***The hint has no consumer until G3.*** *Firing it increments a counter
  (`QueueStores::wake_hints`); G3 routes it to the store's waker (§24.8). Autocommit verbs fire no hint
  yet — the autocommit hints are G3's.*
- *`queue_us` is 0 on a tx-scoped verb, as on a tx-scoped EXEC: a pinned connection is never queued
  for.]*

**What is atomic and what is not:**

- **In-transaction ENQUEUE plus a business write:** both or neither commit. A lost COMMIT is the one
  transactional `Indeterminate`, covering both together.
- **In-transaction ACK plus a business write (transactional ack):** the job's DB effects commit at
  most once, provided three things hold:
  1. the worker does all its DB work in that transaction;
  2. it sends the ACK inside it before COMMIT;
  3. it **rolls back on `LeaseLost`**.

  The ACK's DELETE takes the row lock, and competing RESERVEs `SKIP LOCKED`, so nobody can take the
  job while the ACK is pending.
  - A lost lease answers `LeaseLost`, including a row someone else already removed (R1).
  - At REPEATABLE READ or above on PG, a lost race can surface as **40001 (Retryable)** instead of
    `LeaseLost`. Either way the transaction must not commit.
  - Effects outside the database are not covered.
- **The Laravel driver gets the in-transaction path only where stock had it.** It passes a
  `tx_id` iff its DB connection is in a transaction (§24.11).
  - `after_commit` and `ShouldQueueAfterCommit` keep their stock meaning: the push is deferred and
    runs autocommit.
- **Batches (corrected).** `PendingBatch::dispatch()` stores the batch **outside** any transaction
  (`PendingBatch.php:343-357`). Only `incrementTotalJobs` plus `bulk()` share
  `repository->transaction()` (`Batch.php:202-210`).
  - When that repository's connection and the store share a session and a pool, the job count and
    the enqueue are atomic.
  - Batch **creation** is not atomic with the enqueue. Its failure path is stock's catch, then
    `delete()`.
- **RESERVE in a transaction is refused.** A wait would hold the pin, and a rollback would void a
  delivered token.

### 24.6 Fate table (§9.2, §19.3)

**`OpContext` per verb (normative).**

- `readonly = true` for SIZE only. Every other verb is `readonly = false`, RESERVE included: it
  writes `attempts` and a lease.
- `sent` is as on EXEC.
- `in_tx` is true for a tx-scoped verb, and for the steps of an engine-owned transaction before its
  COMMIT is sent.
- **Engine-owned transactions:**
  - a failure before COMMIT is sent is a known non-execution. The engine rolls back, or the backend
    discards the transaction with the link;
  - a cancel or timeout there answers `Cancelled` or `QueryTimeout`, and a lost link answers
    `Retryable{ConnectionLost}`;
  - a COMMIT that was sent and whose reply was lost is `Indeterminate{WriteUnconfirmed}`.
- The §19.3 `57014` override applies unchanged.

| situation | terminal / outcome | what the caller may do |
|---|---|---|
| any verb, frame never fully written (§19.3, C1e-3) | `Retryable{ConnectionLost}` | retry; nothing ran |
| refused before sending (unknown store, `PoolMismatch`, payload, NUL, dedup on a batch, tx-scoped RESERVE) | NonRetryable code | fix the call; an open transaction is unaffected |
| ENQUEUE autocommit, sent, then link lost / timeout / unconfirmed cancel | `Indeterminate{WriteUnconfirmed}` | surface it. **With a `dedup_key`, re-send is licensed** |
| ENQUEUE in a transaction | a lost or timed-out statement → `TxDeadline`/`ConnectionLost` (the transaction is dead); otherwise the transaction's fate; a lost COMMIT → `Indeterminate` covering the job **and** the business write | closure re-run (§10) |
| RESERVE sent, reply lost (client link, engine↔backend link, unconfirmed cancel) | `Indeterminate{WriteUnconfirmed}`, which is §9.2 unchanged: a lease may have been taken | **re-send is licensed** (C1/C7): a re-send is a new reservation, and a lost one reappears after `lease_deadline` with its attempt counted (a stock-equivalent phantom, §24.7) |
| RESERVE wait expired | `Ok{jobs: []}` | not an error |
| RESERVE CANCEL on a live session | `Cancelled`, or the raced jobs (§24.4) | accept any jobs that arrive |
| ACK / RELEASE / EXTEND autocommit, sent, link lost | `Indeterminate{WriteUnconfirmed}` | **re-send with the same token is licensed**: `acked`/`gone` → done; `LeaseLost` → another holder's |
| ACK / RELEASE / EXTEND, stale token, autocommit | `LeaseLost`: it did nothing | the job is or will be redelivered. Laravel tier: report it and treat the job as done (§24.11) |
| any fenced verb in a transaction, token not current (absent row included) | `LeaseLost`; the transaction stays open, since no SQL error occurred | **roll back.** Laravel tier: throws (§24.11) |
| lease expires under a running worker | redelivered to the next RESERVE with `attempts + 1`; the slow worker's fenced verb → `LeaseLost` | at-least-once: idempotent handlers, or transactional ack |
| worker session ends, gracefully or not, holding a delivered job | nothing is released; the job is redelivered when its lease expires, as with stock Laravel (liveness release is not in v1, §24.15) | — |
| `ferrod` dies or restarts | in-memory state lost; leases expire by their stored deadlines; tokens stay valid | in-flight requests per the rows above |
| SIZE lost | `Retryable` (a read) | retry |
| CLEAR lost | `Indeterminate`, **not** licensed: a re-send would delete jobs enqueued after the first CLEAR | surface it |

*[Amended M7-G1b (SPEC §22.2 (dg)): **built for every autocommit verb on PostgreSQL**, through the
shared `fate::classify_fate` with the `OpContext` above (`readonly` for SIZE only; `sent` honest per
statement; `in_tx: false`), and pinned live: a write verb sent and then timed out or CANCELled is
`Indeterminate{WriteUnconfirmed}` (the §19.3 `57014` override) with a read-back proving it applied
nothing and was not re-sent; SIZE is `Cancelled`; a verb that never got its connection is `PoolTimeout`
or `Retryable{ConnectionLost}`, never `Indeterminate`. **One row the table did not have:** a statement
that RAN but whose rows do not have the shape its builder produces (only a table altered after
verification, or a defect, does that) is `Indeterminate{WriteUnconfirmed}` for a write — its effect is
real and cannot be reported, so no known-fate code may claim it did nothing — and `Unsupported` for
SIZE.]*

*[Amended M7-G2 (SPEC §22.2 (di)): **the tx-scoped rows are built on PostgreSQL and pinned live**,
through the same `fate::classify_fate` with `in_tx: true` (`readonly` still SIZE only):*

- *`PoolMismatch` and a tx-scoped RESERVE (`Unsupported`) are refused before anything is sent and leave
  the transaction usable. An unknown or another session's `tx_id` is `TxNotFound`, and the owner's
  tombstoned one is `TxDeadline`, as on EXEC.*
- *A verb sent and then timed out (`timeout_ms`), CANCELled, past the transaction's `max_tx`, or
  answered `57014` by the backend itself (an application's `SET LOCAL statement_timeout`) rolls the
  transaction back and tombstones it: `TxDeadline{Retryable}`. A backend link lost under it is
  `Retryable{ConnectionLost}`. In both cases the transaction is dead, neither the job nor the business
  write exists, and nothing was re-sent (read back).*
- *A CANCEL observed BEFORE the verb is dispatched (it waited behind an earlier command) is
  `Cancelled` (NonRetryable): nothing sent, the transaction intact (review F4).*
- *A plain statement error (for example `25006` in a READ ONLY transaction) is its known fate, and
  the transaction stays registered for the client's ROLLBACK, as for a failed EXEC.*
- *A lost COMMIT is `Indeterminate{WriteUnconfirmed}`, covering the job and the business write
  together (both or neither, read back). That COMMIT fires no wake hint.*
- *An unmatched fence is `LeaseLost`, an absent row included (R1); the transaction stays open.*
- ***One row this table did not have, decided as SPEC D25: an in-transaction WRITE whose effect the
  engine cannot report.*** *There are two shapes. In the first, the statement ran but its rows do not
  have the builder's shape. In the second, it failed after it was sent with an error that is neither
  the backend's SQL answer nor a lost link — for example, a result the driver could not decode after
  the statement executed. Autocommit answers `Indeterminate` (above). Inside a transaction the engine
  ROLLS THE TRANSACTION BACK and tombstones it, so the effect can never commit, and the verb's terminal
  is **NonRetryable** (`Unsupported`). Its message says retrying cannot help until the store's table is
  fixed and `ferrod` restarts: the cause is a table changed after the once-per-process verification, so
  every retry would fail identically while redoing the business work. A later COMMIT on that `tx_id`
  answers as a tombstone does (`TxDeadline`). An unreadable SIZE is `Unsupported` and leaves the
  transaction open. Only a table altered after verification, or a defect, reaches this row. It is
  pinned live (a BEFORE INSERT trigger added after verification, with the autocommit control
  `Indeterminate`) and on a fake backend (a post-send decode failure).]*

**The new retry licences (D21).** All three are client-policy licences in §9.2's sense, and the
engine never re-sends. Each is a protocol-defined property of a verb whose statements the engine
itself composes, or (the dedup key) a caller's declaration; none is inferred from user SQL.

1. **Fenced verbs (ACK, RELEASE, EXTEND), autocommit only.** The fence admits at most one effective
   application per token, and a re-send's answer resolves the first send's fate. In a transaction
   there is nothing to re-send, because a lost statement kills the transaction.
2. **Dedup-keyed single-job ENQUEUE.** `(dedup_key) → job_id` is recorded for `DEDUP_TTL_S` in the
   same atomic unit as the INSERT. A re-send within the TTL answers `deduplicated: true` with the
   original `job_id` and inserts nothing. Keys are never logged, labelled or exported.
   - **Statement sequence (PostgreSQL; a G4 spike premise, UNVERIFIED).** It runs in one
     engine-owned READ COMMITTED transaction, or as one actor command inside a client transaction:
     1. `INSERT INTO d (dedup_key, job_id, expires_at) VALUES (?, NULL, now + ttl) ON CONFLICT
        (dedup_key) DO UPDATE SET job_id = NULL, expires_at = EXCLUDED.expires_at WHERE
        d.expires_at <= now RETURNING 1`. This claims a fresh key or an expired one. A concurrent
        uncommitted claim blocks it until that claim resolves.
     2. If a row was returned: INSERT the job, then `UPDATE d SET job_id = ?`.
     3. Otherwise: `SELECT job_id FROM d WHERE dedup_key = ?`, which RC sees as committed.
     - Inside an application transaction at REPEATABLE READ or above, step 1 against an invisible
       committed key raises **40001** (Retryable), never a wrong `job_id`.
     - G4 must reproduce all three paths under concurrency before building. MySQL's sequence is a
       G6 premise.
   - **Purge** runs every `DEDUP_PURGE_MS` in bounded batches, independent of the depth sampler.
   - Without a dedup table configured, a `dedup_key` is refused.
3. **RESERVE.** Its re-send is a new request whose worst case is that a lost lease expires.

### 24.7 Delivery semantics

- **At-least-once is the only v1 policy.** `at_most_once` is cut (§24.15). A job is delivered until a
  fenced ACK or RELEASE lands. Duplicates arise only from:
  - lease expiry under a still-running worker;
  - a lost ACK that is never re-sent;
  - mixed-mode stock workers.

  Each is counted where the engine can see it (§24.9).
- **`attempts` counts deliveries, as stock does (no engine-made phantoms).** Laravel's `--tries`
  (default 1) reads `attempts`, and a job over the limit fails without running. So an attempt must
  mean "a worker was handed this job".
  - The engine guarantees this for every reservation it makes on a request's behalf: an undelivered
    reservation is unreserved (§24.8).
  - The residual phantoms are the stock-equivalent classes:
    - a reservation committed whose reply is then lost on the engine↔backend link;
    - a terminal handed to the writer of a session that dies before the bytes reach the worker;
    - a failed unreserve (counted).
  - Stock has the same class: a pop whose COMMIT reply is lost, or a worker that dies after its pop.
  - **Rejected alternatives:**
    - incrementing at first ACK/EXTEND would let two reservations share a token, which breaks the
      fence, and Laravel reads `attempts()` before running;
    - decrementing on a *delivered* job would reuse a token someone holds.
- **Ordering.** Best-effort FIFO by `id` within a queue. There is no guarantee. Job ids are opaque on
  the wire (D24), so a client can neither observe nor rely on this order through them.
- **Priorities.** Laravel's model: queue order (`--queue=high,default`), up to 16 queues per RESERVE.
- **Leases.** `lease_s` per store, renewed only by EXTEND. **No automatic renewal while a session
  lives:** a hung worker would hold its job forever. A job's `timeout` must stay below `lease_s`, as
  Laravel already requires.
- **Poison jobs.** There is no engine tries limit (I2). The only engine rule is the column ceiling: a
  row at `attempts = ceiling` is never reserved again, and it is counted by
  `ferro_queue_stuck_jobs{reason="attempts"}`.
- **Delayed jobs.** Relative to the DB clock and never early (§24.3). A sub-second delay is rounded
  up to whole seconds (post-v1, Messenger's millisecond `DelayStamp` would be too).
- **Batches.** `job_batches` stays on the batching DB connection (§24.5). Ferro Queue never touches
  it.
- **Side effects before the final ACK are not fenced (R3), and this is a cost.**
  `CallQueuedHandler::call` dispatches a chain's next job and records batch success **before**
  `delete()` (`CallQueuedHandler.php:69-82`). A stale holder therefore still dispatches a duplicate
  next-in-chain job and records batch progress twice. Its ACK then answers `LeaseLost`. Stock has the
  same duplicate and additionally deletes the live holder's row. Removing it would need the tier to
  change Laravel's handler ordering, which is outside the seam.

### 24.8 Waiting without holding connections

**Normative cost bound.** A parked RESERVE holds a wait-set entry and a timer, never a connection, a
pin or a permit. Parked waiters on one store cost at most one checkout per poll per
`(store, queue)`, plus one per wake.

**The waker.**

- **Per queue.** One waker task per store owns the wait sets (FIFO per `(store, queue)`) and the poll
  schedule. **Single flight is per `(store, queue)`, not per store.** Different queues' statements run
  concurrently, bounded by the pool.
- **Register, then sweep.** A RESERVE registers in the wait set of each of its queues *before* its
  first sweep. A non-empty sweep deregisters it and answers. This closes the lost-wakeup window
  between sweep and registration.
- **Serving.** To serve a queue, the waker runs ONE reserve statement with `LIMIT k`. k is the sum of
  the clamped `max_jobs` of the head waiters, up to the number of waiters. It hands the jobs out in
  FIFO order.
- **Statement deadline.** Every waker statement carries `WAKER_STMT_TIMEOUT_MS`, so one slow queue
  never stalls another.
  - A statement that times out or is cancelled is classified like any statement: a PG autocommit
    reserve is `Indeterminate` under the `57014` override.
  - The waiters it was serving get that terminal and may re-send (licensed). Leases it may have taken
    surface as stock-equivalent phantoms, counted by `ferro_queue_reserve_unconfirmed_total`.
- **Wait bound (normative).** A parked RESERVE's terminal is handed to its writer no later than
  `wait_ms + queue_wait_grace_ms` after receipt.
  - The waker starts no statement for a waiter past its `wait_ms`, and such a waiter gets
    `Ok{jobs: []}`.
  - If a statement started for a waiter has not returned by the grace bound, the waiter is answered
    `Ok{jobs: []}` and the statement's late jobs for it are unreserved.
- **Unreserve (normative).** A job reserved for a request whose terminal is not handed to a live
  session's writer is restored with ONE fenced statement:
  `UPDATE … SET reserved_at = NULL, attempts = attempts − 1 WHERE id = ? AND attempts = ? AND
  created_at = ? AND reserved_at = ?`.
  - The cases are: the session has begun teardown at hand-off; the grace bound passed; the waiter
    was cancelled by session teardown.
  - **Safety argument.** No client ever saw the token. Any later reservation of the row changes
  `attempts` and `reserved_at`, so the fence makes a delayed unreserve a no-op rather than an
  attack on someone else's lease.
  - The job returns to its original position (same `id`).
  - A failed unreserve is not retried: the job waits for `lease_deadline`. It is counted
    (`ferro_queue_unreserve_failed_total`).

**Triggers, fastest first.**

1. **Local wake hints:** an autocommit ENQUEUE, a RELEASE with `delay_s = 0`, and an unreserve on
   this engine. The in-transaction versions fire at COMMIT (§24.5).
2. **The coalesced poll,** every `POLL_MS` (default 1000) for each `(store, queue)` with waiters. It
   covers other hosts, stock producers, delays coming due and expired leases. The default equals the
   layout's one-second resolution, so delays need no timer.

**Fleet cost.** Idle polling is (queues with waiters) / `POLL_MS` per host, against stock's
(workers) / `--sleep`. G3 measures this.

**A failed poll ends its waiters' requests and is not retried (I4).** The served waiters get the
classified terminal, the others on that queue get `Ok{jobs: []}`, and all of them re-ask. An empty
poll is not a failure.

**The client side of a wait (normative).** A RESERVE must never park longer than the client's
transport tolerates. Which bound applies depends on whether the client carries M3's per-request
deadlines (ledger D1c, specified in §23.11.0), and D1c lands in M3, before any Queue slice:

- **With D1c (the client every Queue slice is expected to build against).** A request's client
  deadline is its own bound plus a margin, and the socket read timeout is a PING-probed liveness
  bound that a slow request no longer trips. A RESERVE's client deadline is
  `wait_ms + queue_wait_grace_ms + margin`, so a parked RESERVE cannot poison the session, and
  `wait_ms` is bounded only by the engine's `MAX_WAIT_MS` and the caller's `block_for`. The
  `ioTimeout` clamp below does not apply.
- **Without D1c (the client as it stood when this section was adopted).** The transport has one
  read timeout (`ioTimeout`; 5 s native, 30 s in the Laravel tier), and a read timeout poisons the
  session (C1e-3). The clamp below is then mandatory.

G4 builds against whichever client is on `main`, states which rule it implements, and chaos row 15
tests that rule. The clamp, for a client without D1c:

- The client sends `wait_ms ≤ ioTimeout − queue_wait_grace_ms − 1000 ms`, clamping silently.
  - With an `ioTimeout` under 3 s it sends `wait_ms = 0`.
  - The native default (5 s) therefore waits at most 3 s, and the Laravel default (30 s) at most
    28 s.
  - A `block_for` above that is clamped. This is documented, not refused.
- The engine also clamps to `MAX_WAIT_MS`.
- **Measured premise (R4).** On PHP 8.4.19 a signal delivered during a blocked stream read restarts
  the read's timeout. With a 3 s timeout and SIGTERM at 1 s, the read returned at 4.00 s, timed out,
  and the async handler ran afterwards. A signal can therefore only *lengthen* the transport's
  tolerance, never make the clamp unsafe. G3/G5 reproduce this on the real `Transport`.
- **SIGTERM while parked (the common path at every deploy).** `Worker::listenForSignals` only sets
  `shouldQuit` (`Worker.php:723-731`), and the parked read is not interrupted (R4). The RESERVE
  completes within `wait_ms + grace`.
  - If it carries a job, `Worker::daemon` runs that job before `stopIfNecessary`
    (`Worker.php:164-199`). That is stock behaviour, identical to `RedisQueue`'s `block_for`.
  - **Cost and operator rule:** shutdown latency is at most `block_for + 1 s + the longest job`.
    supervisord's `stopwaitsecs` must exceed that.
  - A worker SIGKILLed while parked ends its session non-gracefully. A reservation not yet handed off
    is unreserved. One already handed off is a delivered job, released only by lease expiry.

**Liveness release: not in v1** (decided, §24.17 Q2; listed in §24.15). A delivered job whose worker
dies waits for its lease to expire, (L, L+1] s, exactly as with stock Laravel, so nothing regresses
against the incumbent. The draft that preceded this section specified an opt-in release on
non-graceful session end; it was dropped from v1 rather than shipped half-trusted, because it is the
one engine write that can enable a second execution of a job whose first may still be running (I4).
The review established what any future design must satisfy, recorded here so it is not re-derived:

- it must be OFF by default and opt-in per store, and apply only to leases whose RESERVE carried
  `liveness: true` (the field already in the frozen G1 shape, §24.4);
- it must fire only on a **non-graceful** session end (EOF without GOODBYE, or a transport error),
  never on GOODBYE and never at drain, because in the Laravel tier a session closes under a live
  worker on every `DB::reconnect()`, `purge()` or C1e-3 poisoned-session replacement (review F4);
- the flag must be set only on a **dedicated queue session** that the DB tier never reconnects;
- the release must run strictly **after** `tx_registry.abort_session(..).await`, or it blocks behind
  its own teardown's row lock (review F7);
- its lease index must be transaction-aware: a tx-scoped verb's removal applies only on COMMIT
  (review F14);
- it needs its own §21 decision, because D22 covers only the writes I4 lists.

**Drain (§7.7).** On `ferrod` SIGTERM, parked RESERVEs get `Ok{jobs: []}` and **nothing** is
released. This needs the drain signal §23.6.1 plumbs into the sessions and the waker; today sessions
are never told the daemon is draining. Workers finish their fenced verbs on the successor, because tokens live in rows.

**PostgreSQL LISTEN/NOTIFY is not the v1 wake mechanism.** It needs an app-installed trigger
(no engine DDL) and a dedicated listening connection per pool (M5 machinery, built after M7). It is a
post-v1 option, triggered by measured cross-host latency.

### 24.9 Observability (§13, product-vision §5)

**Redaction carries over.**

- Payloads, dedup keys and job class names are never logged, labelled or exported.
- Queue names are labels only when the operator lists them in `LABELLED_QUEUES`. Every other queue
  is `queue="_other"`.
- `store` is a closed, escaped vocabulary like `pool` (§22.2 (bs)).

**Counters.**

- `ferro_queue_ops_total{store,queue,op,outcome}`: `op` is one of the seven verbs; `outcome` is one
  of `ok|empty|lease_lost|gone|deduplicated|error`. Errors are also counted by the taxonomy counters
  at the terminal builder (§22.2 (bu)).
- `ferro_queue_enqueued_total{store,queue,mode="autocommit|tx"}`. The `tx` mode counts statements,
  not committed jobs, and its help text says so.
- `ferro_queue_redeliveries_total{store,queue,cause="lease_expired|released"}`.
- `ferro_queue_unreserved_total{store,cause="teardown|deadline"}` and
  `ferro_queue_unreserve_failed_total{store}`.
- `ferro_queue_reserve_unconfirmed_total{store}`: waker statements whose fate was `Indeterminate`.
- `ferro_queue_polls_total{store,trigger="hint|interval"}`.

**Histograms.** Enqueue and ack duration, `wait_duration_seconds` (time parked), and
`pickup_latency_seconds` (reservation time minus `available_at`, the queue's SLO number).

**Gauges.**

- `ferro_queue_waiters{store}`.
- Sampled every `DEPTH_SAMPLE_MS` (0 = off) by one grouped COUNT per store:
  - `ferro_queue_depth{store,queue,state="pending|delayed|reserved"}`;
  - `ferro_queue_oldest_pending_age_seconds`;
  - `ferro_queue_stuck_jobs{store,reason="attempts|oversize"}`.
- This is sampling, not storage.

**Spans.** One OTLP span per QUEUE request: op, store, labelled queue, job count, a reservation's
`attempts`, `ferro.in_tx` and `error.type`. It ends with the one terminal (§22.2 (ce)). An ENQUEUE's
`traceparent` is not stored in the job (I2).

**Slow log.** Queue verbs above the threshold, with `op`, store, labelled queue, count and
`queue_us`/`exec_us`. A parked wait never counts as slow.

### 24.10 Security (§12)

- **Admission is unchanged.** Any admitted peer may use any store, as it may use any pool. Per-store
  allow-lists are post-v1.
- **No new path or identifier surface.** The client cannot name a table (I3).
- **Corrected:** a store is a capability boundary **only on a manifest-only pool** (§11). On any other
  pool an admitted peer can already EXEC raw SQL against the same table, so the store adds no
  boundary there.
- **Manifest-only pools accept QUEUE verbs.** They are a closed, engine-authored operation set (C4).
  **Cost, stated:** a store declared on a manifest-only pool widens that pool's surface by all seven
  verbs, none of which the manifest declares — RESERVE reads any job payload, which routinely carries
  personal data or serialized models, and CLEAR deletes a whole queue. Declaring the store there is
  the operator's choice; the engine states it rather than prevents it. Manifest-only mode itself is
  M4 (ledger E2), after M7, so until M4 no pool is manifest-only.
- **No admin verbs.** CLEAR is data-plane. D15 is untouched.

### 24.11 PHP side

**Native API (`ferro/client`, dependency-free, charter rule 7).**

```php
$q = $db->queue('jobs');                                  // a store on this Connection's session
$q->enqueue('emails', $json, delaySeconds: 0, dedupKey: null);  // rides $db's open tx automatically
$q->enqueueMany('emails', [$a, $b]);                      // returns a count; no ids, no dedup key
$w = Ferro::connect($socket)->queueWorker('jobs');        // a dedicated session (§24.8)
$jobs = $w->reserve(['high', 'default'], max: 1, waitMs: 3000);  // bounded per §24.8
$w->ack($r);  $w->release($r, delaySeconds: 30);  $w->extend($r);
$db->transaction(fn ($tx) => /* DB work */ $q->ack($r));  // transactional ack on $db's session
$q->transactionalAck($r, fn ($tx) => …);                  // rolls back on LeaseLost
```

- **Ownership.** A `Queue` is bound to a `Connection`, so a `tx_id` can only come from its own
  session. `queueWorker()` returns a handle bound to a dedicated session: it refuses tx-scoped verbs
  and, like every v1 client, sends `liveness: false` (§24.4).
- **Retries.** The policy layer re-sends autocommit fenced verbs, dedup-keyed ENQUEUE and RESERVE on
  `Indeterminate`. It never re-sends a plain ENQUEUE or CLEAR.
- **Abandoned RESERVE futures (async, M3-D1).** Jobs carried by a terminal the caller abandons are
  RELEASEd with delay 0, and the attempt stays counted. **Cost:** an async caller should not abandon a
  RESERVE it may need under `--tries=1`-like policies. There is no client-side unreserve verb. Its
  re-send would not be fence-safe, because a same-second re-reservation can carry the same `(token,
  reserved_at)`.
- **Errors.** `LeaseLostException` and `PoolMismatchException` sit inside the `FerroException`
  contract.

**Laravel driver (`ferro/laravel`): one driver value, plus the shipped `ferro_jobs` migration or `TABLE=jobs` on the store (§24.3).**

```php
'connections' => ['database' => [
    'driver' => 'ferro',          // was 'database'
    'connection' => 'pgsql',      // a ferro-* DB connection (producers, transactional verbs)
    'store' => 'jobs',
    'queue' => 'default',
    'block_for' => 5,             // seconds, clamped per §24.8; null = no wait
    'after_commit' => false,
]],
```

- **`FerroQueue extends Illuminate\Queue\Queue implements Queue, ClearableQueue`.**
  - **Producers** (`push`/`later`/`pushRaw`/`bulk`) use the DB connection's session, through
    `getPdo()->ferro()` (§22.2 (bw)), with `tx_id` iff that connection is in a transaction.
  - `push` returns the job id. `bulk` returns `true`, as stock does. *[D24: the id is opaque bytes
    on the wire, where stock returns an integer. What `push()` and `FerroJob::getJobId()` hand Laravel
    is G5's to decide and pin: the raw bytes, or a printable form. Binary strings are unsafe in JSON
    and logs, and the choice depends on G1's `sql` encoding (§24.3). Either way it is a drop-in
    difference for code that treats a queued job's id as a number.]*
  - **`pop()`** reserves on a **dedicated, lazily dialled queue session** with the DB connection's
    socket options.
    - It never shares a session with the DB tier, so `DB::reconnect()`, `purge()` and C1e-3
      replacements cannot tear down a parked long-poll or race it (F4), and a long-poll never holds
      up the DB session.
    - The session costs one UDS connection per worker and no backend connection.
    - It sends `max_jobs = 1` and waits only when `$index == 0`, which is `RedisQueue`'s `block_for`
      rule.
    - On `Indeterminate` it re-sends once (licensed). If that fails too, it returns `null`: the job,
      if leased, reappears after its lease.
  - `size()` is SIZE summed, and `clear()` is CLEAR.
  - Retry-after is the store's `LEASE_S`. A `retry_after` key in the config is refused at connect.
  - **The table is the engine's, not the config's.** The store's `TABLE` is declared in `ferrod`
    (default `ferro_jobs`, §24.3). `ferro/laravel` ships the `ferro_jobs` migration (stock
    `jobs.stub`'s layout under the Ferro name). An app that keeps its stock `jobs` table sets
    `TABLE=jobs` on the store instead, which is mixed mode (§24.3). **A trap the default creates,
    for G5 to close:** stock `config/queue.php` carries `'table' => env('DB_QUEUE_TABLE', 'jobs')`,
    so a config edited from `database` would still say `jobs` while the store uses `ferro_jobs`. The
    tier cannot honour the key, because the table is not on the wire, and must not ignore it
    silently. G5 decides between refusing it at connect (the `retry_after` precedent above) and
    another explicit rule, and pins the choice with a test.
- **`FerroJob extends Illuminate\Queue\Jobs\Job`.** `attempts()` comes from the reservation.
  - **`delete()`** sends ACK.
    - **Inside a transaction** on the DB connection it is an in-transaction ACK, and **`LeaseLost`
      throws `LeaseLostException`** (F13), so the application's transaction cannot commit for a job
      it does not hold.
    - **In autocommit** it uses the queue session, and `LeaseLost` is reported through the exception
      handler, counted, and treated as done.
  - **`release($delay)`** follows the same rule with RELEASE.
  - **`fail($e)` is overridden (F1).** It keeps stock's order up to the delete, including the
    timeout `rollBack(toLevel: 0)` of the `queue.failed.database` connection. Then:
    1. it ACKs **autocommit on the idle queue session**, never in a transaction, because a failure is
       not part of the job's business transaction and the call may run inside the SIGALRM handler;
    2. on `acked`/`gone` it continues as stock: `failed($e)`, then `JobFailed` in a `finally`, then
       the failer writes `failed_jobs`;
    3. on **`LeaseLost`** it marks itself deleted and reports, and **neither calls `failed()` nor
       dispatches `JobFailed`**. The job is another holder's, and that holder's outcome decides
       whether it fails;
    4. if the ACK is still `Indeterminate` after the licensed re-send, it rethrows **without**
       `JobFailed`: the job may be redelivered, and its failure will be decided again then.

    **Cost (accepted, §24.17 Q7):** a stale holder's `failed()` callback does not run. Stock runs it, and also writes
    a `failed_jobs` row for a job someone else may complete.
- **Horizon.** Horizon is Redis-only (§22.2 (ch)), so nothing changes there. `queue:monitor`
  (through `size()`), Pulse (through events), `queue:failed`, `queue:retry` and `queue:flush` work
  unchanged.

**Symfony Messenger: not in v1** (decided, §24.17 Q4). v1's drop-in tier is Laravel, so the
Messenger transport is post-v1, with its own acceptance bar to be set when it is built (§24.12). The
sketch the draft carried is kept as a starting point, **not normative and wholly UNVERIFIED** (there
was no copy of `symfony/messenger` to check it against):

- `FerroTransport implements TransportInterface, MessageCountAwareInterface`, plus
  `KeepaliveReceiverInterface` where available, packaged as `ferro/messenger-transport`.
- `send()` → ENQUEUE, with `tx_id` iff the DSN's DBAL connection, whose native connection is a Ferro
  `Connection`, is in a transaction.
- `get()` → RESERVE on a dedicated session; `ack()` → ACK; `reject()` → ACK, because failure routing
  is a separate send; `keepalive()` → EXTEND; `getMessageCount()` → SIZE.
- `setup()` and `auto_setup` never create tables. They verify the table and throw with the CLI's DDL
  in the message.
- Retries stay non-atomic: Messenger sends a new message and then acks the old one.

### 24.12 Acceptance bar (D18: parity with a stock-driver control)

**Laravel (`laravel/framework` v11.51.0).**

- **Provenance, stated.** Upstream's own `database`-driver queue CI runs on **SQLite only**
  (`queues.yml:76-83`: `DB_CONNECTION: sqlite`, `QUEUE_CONNECTION: database`). Upstream has never run
  `tests/Integration/Queue` with the database driver on PostgreSQL or MySQL. Ferro's control column
  on those families is therefore new ground, and a control failure there is upstream's, triaged
  before the Ferro columns count. v1 has no SQLite store, so the one configuration upstream proves is
  unreachable for the engine column.
- **Connection name.** `QueueTestCase` keys its skips on the queue **connection name**, and three
  files hard-code `queue.default = 'database'`. The Ferro column keeps the name `database` and changes
  only `driver`, which makes §22.2 (am)'s driver-name artifact unreachable by construction (verified).

Columns per family (PostgreSQL first, then MySQL and MariaDB at G6):

1. **control:** stock `database` driver over stock `pdo_*`;
2. **DB tier:** stock `database` driver over `ferro-*`;
3. **engine:** the `ferro` driver over `ferro-*`, with the store's **`TABLE=jobs` set explicitly**.
   Upstream asserts `DB::table('jobs')`, and the default `ferro_jobs` (D22 amendment (a)) would
   leave those assertions counting an empty table. The column therefore uses the opt-in `jobs` table,
   with no stock workers on it. The §15 demo's engine column picks either table in G5 and states which.

Scope:

- `tests/Integration/Queue/*`, minus the Redis, Dynamo and SQS files (the same exclusions in every
  column);
- plus `tests/Integration/Database/Queue/{QueueTransactionTest, BatchableTransactionTest}`, which
  SIGKILL a `remote()` worker and need the ferrod socket passed to the child;
- **`tests/Queue/QueueDatabaseQueueIntegrationTest` is dropped from every column.** It builds its own
  Capsule connection hard-coded to `sqlite` `:memory:` (`:36-41`), so it would run with zero Ferro
  contact: the silent-SQLite trap §22.2 records for DBAL and testbench.

Triage categories: a test-clock artifact (PHP fake time, §24.3), the documented D8/§7.4 classes, or a
documented incompatibility. Anything else is a defect.

**Horizon-equivalent workloads (product-vision §4.3's bar), named.** Horizon itself is Redis-only.
The stand-in is the database-queue workload a Horizon deployment runs:

- the §15 demo's `testDatabaseQueueRunsAndFailsJobs` and `testQueueBatches`, given an engine column in
  the per-PR `laravel-demo` gate;
- Horizon's two `BatchRepository` reads, already exercised there;
- `queue:monitor` through `size()`;
- `queue:failed`/`queue:retry`/`queue:flush`.

Product-vision §4.3's wording is corrected to name this stand-in, in the same change (C2).

**Ferro-only feature proofs** (not D18 columns, each with a stock control row that documents stock
behaviour):

- transactional enqueue on commit and on rollback;
- stale-holder `LeaseLost` with the second holder's row intact;
- stale-holder fail path with 0 `failed_jobs` rows.

**Messenger: no v1 bar.** The transport is post-v1 (§24.17 Q4), so M7's acceptance bar is the
Laravel columns above. When the transport is built, its slice first inventories `symfony/messenger`
and `symfony/doctrine-messenger` for transport-agnostic tests; the fallback is a Ferro-written parity
suite against BOTH the Ferro transport and the stock Doctrine transport as control, and the bar is
set then, under D18.

### 24.13 Chaos plan (§20.3 extended)

Every scenario runs against real Dockerized PostgreSQL (and MySQL/MariaDB at G6). Each ends with a
**read-back invariant over the rows**. The global invariant: **no committed job is ever lost**, and
every duplicate and every phantom attempt is attributable to a counted or documented cause.

1. **SIGKILL `ferrod` mid-ENQUEUE (autocommit).** `Indeterminate`; 0 or 1 rows, never 2. With a
   `dedup_key`, the licensed re-send yields exactly 1.
2. **SIGKILL in a transaction holding an ENQUEUE, before COMMIT.** `Retryable`, and neither row
   exists. Killed during COMMIT: `Indeterminate`, and **both or neither**, over many iterations.
   *[Built M7-G2 (SPEC §22.2 (di)), `ferrod`'s `queue_g2_it`. The engine-side half: the backend link
   killed under an in-transaction ENQUEUE answers `Retryable{ConnectionLost}`, neither row exists. The
   link killed inside the COMMIT (a deferred trigger holds it open) answers `Indeterminate`, both or
   neither. A REAL `ferrod` process is SIGKILLed before COMMIT (neither row, read back only after the
   server ended the transaction) and during COMMIT, 12 iterations per run: half provably inside it,
   half at 0–30 ms after it was sent. Every iteration was both or neither. Measured over three runs
   (neither/both): 0/12, 1/11 and 0/12 — so in two runs of three only one outcome occurred, and the test
   does not assert that both occur.
   The client's own `Retryable`/`Indeterminate` classification of a SIGKILLed engine is the existing
   §19.3 client rule, re-proven through the queue API at G4.]*
3. **SIGKILL `ferrod` while a RESERVE is parked, and while one is mid-statement.** Nothing is lost; a
   leased job reappears after `lease_deadline`.
4. **SIGKILL `ferrod` mid-ACK.** `Indeterminate`; the licensed re-send answers `acked`, `gone` or
   `LeaseLost`; the job runs at most twice.
5. **SIGKILL the worker mid-job, engine up.** Redelivered after (L, L+1] s, matching the stock
   control (liveness release is not in v1).
6. **A stuck worker past its lease.** Redelivered; the stale ACK answers `LeaseLost`; the read-back
   proves the second holder's row survived. Control: stock deletes it.
7. **Transactional ack under induced lease loss.** The worker rolls back; business rows per job ≤ 1
   over N iterations, including the absent-row case (R1).
   *[Built M7-G2 (SPEC §22.2 (di)), 40 iterations plus a control, each a different shape:*
   - *the stale and the new holder race concurrently;*
   - *the stale holder arrives after the new holder committed (the absent row, R1; its autocommit
     control answers `gone`);*
   - *the new holder rolls back;*
   - *REPEATABLE READ with a snapshot older than the re-reservation, which answers `40001`
     (Retryable).*

   *The stale holder never commits, and no job has more than one business row. The control: a
   lease that expired with nobody re-reserving is honoured.]*
8. **Backend connection killed mid-ENQUEUE, mid-RESERVE and mid-ACK.** The engine-side mirror of rows
   1, 3 and 4.
9. **`ferrod` SIGTERM drain with leases outstanding.** Nothing is released; ACKs succeed on the
   successor.
10. **Mixed mode with `retry_after == LEASE_S`** (the store's `TABLE=jobs`, the explicit opt-in, §24.3). No loss; Ferro's fencing holds; duplicates are
    attributed to stock workers.
11. **Parked-waiter cost.** 200 parked RESERVEs hold zero pool connections, asserted on the pin and
    idle gauges.
12. **No engine-made phantoms (F5).** Under `--tries=1`, race thousands of RESERVE waits against
    cancels, `wait_ms` expiries and waiter SIGKILLs. Assert that **no job reaches `failed_jobs`
    without its handler having run**, except jobs counted by
    `reserve_unconfirmed`/`unreserve_failed`. Plus the CANCEL race (F16): no job is ever both
    delivered and available.
13. **Stale-holder fail path (F1).** The job times out (SIGALRM) or throws after its lease was taken.
    0 `failed_jobs` rows for it, `failed()` not called, and the second holder's run unaffected.
    Control: stock writes the row.
14. **A job calls `DB::reconnect()` / `DB::purge()` mid-run, and a C1e-3 poisoned-session replacement
    is induced (F4).** The job runs exactly once and its ACK lands: no redelivery, no lost parked
    RESERVE on the worker's dedicated queue session, and no poisoned session.
15. **`wait_ms` at the client's bound (F10).** RESERVEs with `wait_ms` at the bound §24.8 gives the
    client G4 builds against (the D1c deadline, or the `ioTimeout` clamp), and jobs
    arriving at `wait_ms ± 50 ms`. Zero read timeouts and zero poisoned sessions; every delivered job
    is acked by its receiver.
16. **SIGTERM to a parked Laravel worker (F10).** The worker exits within `block_for + 1 s` (plus a
    job if one arrived), runs any job delivered in that window, and leaves no lease behind.
17. **In-transaction MySQL RELEASE racing a `ROLLBACK_TO` from another Fiber on the same `tx_id`
    (F17).** Never two live rows for one job.
18. **Id reuse (F20).** `TRUNCATE … RESTART IDENTITY` under a stale holder: the stale ACK cannot
    delete the new row unless both `id` and `created_at` collide. This documents the precondition.
19. **Isolation footprint (F11, G6 bench).** ENQUEUE and ACK blocking under concurrent RESERVE at RC
    (engine-composed) against a forced RR. Proves the composed level is what runs.

### 24.14 Slice plan

*[Amended M7-G1a (SPEC §22.2 (df)): G1 is **split** into G1a and G1b — `/proto` plus everything a verb
needs before its first statement, then the verbs themselves — because the whole of it would not have been
one reviewable slice (the HTTP precedent: `/proto` alone was F2). `/proto` is complete and frozen in G1a.]*

| slice | delivers | proves |
|---|---|---|
| **G0** *(DONE, §22.2 (cn))* | this section; `QUEUE = 7`, `LeaseLost`, `PoolMismatch` and `queue_wait_grace_ms` allocated in the spec (their `/proto` entries land at G1); §21 D21/D22; the §24.16 amendments | review attacked §24.5–§24.8 before any code |
| **G1a** *(BUILT M7-G1a, SPEC §22.2 (df); DONE when merged)* | `/proto`: `[services] QUEUE = 7`, `[methods.queue]`, `LeaseLost`/`PoolMismatch` and the new `InvalidHandle` (`0x3011`), `queue_wait_grace_ms`, the `[ack_outcome]` table and three shape bounds; PROTOCOL.md §1 and a new **§14**; golden vectors (every handle position at its `sql` size and at 1 024 bytes) and refusal vectors (0 and 1 025 bytes for every handle position; 0 and max + 1 jobs and queues) in both codecs; **all shapes frozen**; §24.3's G1 prerequisites (the canonical decimal `job_id`, the 8-byte token, both with strict decodes; `InvalidHandle`); store config with every refusal; the version gate and shape verification at first use (PostgreSQL; cached per process, an absent table excepted); `ferrod` routing and a QUEUE handler that makes every pre-checkout refusal and the first-use verification, then answers `Unsupported` for every verb | an undecodable handle is `InvalidHandle` before any statement; a wrong shape names its column; a view is refused and a partitioned table passes; an unqualified table follows `search_path`; the gate and the verdict caching (including the absent-table TTL, counted) are wired; mutation-proven |
| **G1b** *(BUILT M7-G1b, SPEC §22.2 (dg); DONE when merged)* | ENQUEUE (single/batch) / RESERVE (no wait) / ACK / RELEASE / EXTEND / SIZE / CLEAR autocommit on PG; the widened fence; the clock and rounding rules; the statement builders in `ferro-queue`. *Carried from the G1a review:* shape verification must also require `id` to be UNIQUE (the primary key or a unique index) — a fence `WHERE id = $1 AND …` over duplicate ids would match several rows; and `now + 1 + delay_s` must be pre-checked against PG `integer` before send, since a `delay_s` near `u32::MAX` otherwise overflows `available_at` as a post-send `22003` (refuse it `Unsupported`, nothing sent); SIZE fills `oldest_pending_at` | stale token → `LeaseLost`; late-but-uncontended ACK honoured; RELEASE to the back; **affected ≤ LIMIT under concurrent reservers** (F12a); never-early delays and lease ≥ L, at second boundaries; NUL refused; mutation-proven |
| **G2** *(BUILT M7-G2, SPEC §22.2 (di); DONE when merged)* | tx path: `resolve_active` made `pub(crate)`, `PoolMismatch`, `TxCommand::Queue` + `after_commit`, in-tx `LeaseLost` semantics (R1), refused tx-scoped RESERVE | atomicity both ways; mismatch leaves the transaction usable; chaos rows 2 and 7 |
| **G3** | the waker (per queue, `LIMIT k`, statement deadlines, register-then-sweep), long-poll, the wait bound, **unreserve**, wake hints, coalesced polls, drain; queue metrics and spans | cost bound (row 11); one END under every CANCEL/deadline race and the deliver-xor-unreserve rule (row 12); idle-polling bench vs stock (A's number); R4 reproduced on the real transport |
| **G4** | native PHP API, `queueWorker()`, wait clamp, client fate and licensed re-sends; dedup table and purge **after** the dedup spike reproduces §24.6's three paths | chaos rows 1, 3–6, 8, 9 and 15 through the client |
| **G5** | Laravel driver: `FerroQueue` (dedicated reserve session), `FerroJob` (`delete`/`release` rules, the `fail()` override); the `ferro_jobs` migration and the rule for a stock `'table'` key (§24.11); pin what `push()`/`FerroJob::getJobId()` return for an opaque `job_id` (D24); demo engine column; the three-column upstream run on PG | D18 on PostgreSQL; chaos rows 13, 14 and 16; the SIGALRM reentrancy premise |
| **G6** | MySQL/MariaDB stores (engine-owned RC transactions, actor-command steps, MySQL dedup spike) and their D18 columns | D18 and chaos on MySQL 8.4 and MariaDB 11.8; rows 17 and 19 |

**Removed from the v1 plan, kept as post-v1 labels** (§24.15): **G7**, the Symfony Messenger
transport and its parity suite (§24.17 Q4); **G8**, liveness release (§24.17 Q2). Neither touches
G1–G6: the `liveness` field is already in the frozen G1 shape, and a v1 engine refuses
`liveness: true` and any `LIVENESS_RELEASE` key.

### 24.15 Not in v1

- **The `at_most_once` delivery policy (cut, F3; confirmed, §24.17 Q5).** RELEASE carries no payload,
  so re-enqueueing after a deleting RESERVE has nothing to insert. An undeliverable at-most-once job
  would be lost silently. A future design needs a payload-carrying RELEASE and a declared, counted
  loss.
- **Liveness release (former slice G8; §24.17 Q2).** A delivered job whose worker dies waits for its
  lease, as with stock Laravel. The constraints a future design must meet are recorded in §24.8; it
  needs its own §21 decision, because it would be an engine write that can enable a second execution.
- **The Symfony Messenger transport and its parity suite (former slice G7; §24.17 Q4).** v1's drop-in
  tier is Laravel. The transport sketch in §24.11 is non-normative and UNVERIFIED.
- **Backends.** Redis Streams and Kafka (P8), SQS, Beanstalk, and a **SQLite store**: under D13 every
  RESERVE takes the writer lock, and SQLite apps keep the stock driver through `ferro-sqlite`.
  **Prepared, not built** (D22 amendment (b)): the store `KIND` key and the opaque-bytes token let
  Redis Streams or SQS be added later without a shape change (`job_id` is opaque bytes as well,
  SPEC D24). Each such kind needs its own §21 decision, because D22 covers SQL stores only, and
  none offers transactional composition without an outbox (§24.5).
- **The scheduler and `onOneServer`** (P9).
- **Job-level features:** unique jobs (`ShouldBeUnique` stays Laravel's), numeric priorities, FIFO
  groups, payload encryption or inspection, a FAIL verb, rate limiting, engine tries limits, and
  fencing of pre-ACK side effects (R3).
- **Delivery and visibility:** push/streamed delivery, LISTEN/NOTIFY wakes, an admin READ verb
  (E4/`ferro top`), Horizon support, and a client-side unreserve verb.
- **Access control:** per-store uid allow-lists. Any admitted peer may use any store, as it may use
  any pool (§24.10; the same recorded gap as §23.18 Q4).
- **Engine-side work:** a Ferro-native layout, per-reservation leases, any engine DDL, automatic lease
  renewal, and dedup on batch ENQUEUE.

### 24.16 Conflicts with existing text, and how each was resolved

Every item is resolved in this change. The amendments are applied in `ferro-spec-v0.2.md`,
`docs/product-vision.md` and the ledger as *[Amended …]* notes; the `/proto` and `PROTOCOL.md` ones
land with slice G1.

**Numbering.** §23 holds D19 and D20. The retry licences are **D21**, one §21 entry shared with §23
(its wording is in §23.17), and the scope exception is **D22**.

- **C1 — §11, §9.2 and §19.3 called manifest `idempotent: true` "the sole license"** for
  auto-retrying an `Indeterminate` write. §24.6 adds protocol-defined licences. **Resolved by D21:**
  the manifest flag is the sole licence for a SQL statement, and each other service's licences are
  defined in its own section. §11 is amended to say so.
- **C2 — product-vision §4.3** said the engine owns "retries, backoff", and its bar said
  "Horizon-equivalent workloads green". The engine owns redelivery as stored state and release with a
  client-chosen delay, and never re-runs anything itself (I4). §4.3 is corrected in this change,
  including naming the §24.12 stand-in for "Horizon-equivalent", the at-least-once-only policy and
  the post-v1 Messenger seam. §17's amendment note says that the M7 bullet's "retries" ("… leases,
  acks, retries, …") means redelivery as stored state (§24.2 I4).
- **C3 — §19.1, "a changed epoch voids all engine-side state."** Tokens are stored state and survive.
  §19.1 gains one sentence pointing to §24.3.
- **C4 — §11, manifest-only mode "rejects raw SQL."** QUEUE verbs are engine-authored. §11 gains one
  clause (§24.10).
- **C5 — engine-initiated writes.** The first draft sanctioned liveness release by analogy with
  rollback; the review withdrew the analogy (F7). **Resolved by dropping liveness release from v1**
  (§24.17 Q2). v1 has exactly two engine-initiated writes (I4): **unreserve**, which restores a row
  nobody's token names and so cannot enable a second execution, and **dedup purge**, which deletes
  expired keys in a Ferro-declared table. Neither is a statement retry, so charter rule 3 holds in
  letter and spirit.
- **C6 — housekeeping:** §5's service list carries `07 queue`; §20.1 gains `crates/ferro-queue`
  (store model, per-dialect statement builders including actor step lists, shape checks,
  fence-result decoding; no I/O) and, in `ferrod`, `services/queue.rs` and `queue/waker.rs`. No
  `php/messenger` package is planned for v1. §17's M7 bullet is amended to drop Messenger from v1.
- **C7 — §9.2 (the RESERVE classification; F15).** The first draft classified a lost RESERVE
  `Retryable`, which contradicts §9.2's definition: RESERVE is a dispatched side-effecting statement.
  It is **`Indeterminate`, so §9.2's tree and its definition stand unchanged.** What changes is the
  licence sentence, amended per D21. The adopted §9.2 sentence reads, verbatim: "clients MUST NOT
  auto-retry `Indeterminate` unless the query's manifest entry declares `idempotent: true` (§11), or,
  for a Ferro Queue verb, §24.6 licenses the re-send (an autocommit fenced verb, a dedup-keyed
  `ENQUEUE`, a `RESERVE`). Ferro HTTP licenses through classification instead, and nothing licenses
  re-sending an `Indeterminate` HTTP request: a request declared idempotent, by its caller or by the
  operator for its upstream, is never `Indeterminate` — a link-level failure after sending is
  `Retryable`, and a timeout or cancel follows the read rule above (§23.7.1, §23.7.2); the method
  alone licenses nothing. Raw-SQL writes are never auto-retried." §19.3 gains a pointer. The client's `retry_reads` does **not**
  apply to RESERVE: its re-send runs under the queue licence, not as a read.
- **C8 — §3 and charter rule 6 (F18).** §3 says "not a storage engine, not a SQL rewriter". Charter
  rule 6 says "no SQL rewriting … no read/write inference". The precedents the first draft cited
  (`compose_begin_sql`, `sp_N`, hygiene, the version probe, `VACUUM INTO`) are transaction or session
  control, a read-only probe, or an admin snapshot. **None writes rows in an application table.**
  Ferro Queue does three new things:
  - it writes application rows on its own authority (unreserve);
  - it defines an application table's semantics (fencing, the dedup table);
  - it reads application data shapes (`information_schema`).

  **Resolved by D22**, decided under the owner's full-freedom grant (§24.17 Q6). Because it amends
  binding scope text, no G-slice code (G1 onward) could start before the owner ratified it. **The
  owner ratified it on 2026-10-06, with two amendments** (the `ferro_jobs` default table and store
  kinds with an opaque-bytes token, SPEC §21 D22, §22.2 (de)), so G1 may start. §3 gains an amendment
  note to this effect:

  > "**Exception (D22):** the job transport engine (§24) composes a closed set of statements against
  > an operator-declared table whose layout the application owns, and may write its rows on its own
  > authority only as §24.2 I4 enumerates. That is a transport over application-owned storage, not
  > storage owned by the engine; no user statement is read, rewritten or inferred from."

  Charter rule 6 reads with the sentence "Engine-authored statements of the §24 queue verbs are not
  SQL rewriting, and the `ferro` queue driver is not a drop-in database tier under the rule's 'change
  execution, never SQL generation': it is a new driver name whose transport replaces
  `DatabaseQueue`'s builder-generated SQL by design, while the drop-in database tiers keep stock SQL
  generation." D22 records it. `CLAUDE.md`'s copy of the charter was not edited in this change
  and gains the sentence when that file is next updated. At ratification (§22.2 (de)) the edit was
  prepared, and it was applied on the owner's direct approval the same day.

  Stated honestly: this widens what the engine may do to application data. The cost is that a defect
  in an engine statement builder can now damage application rows. The mitigations are the shape
  verification, the fence on every engine write, and mutation-proven builders in a no-I/O crate.

### 24.17 Decisions taken at adoption, and the premises still owed

The G0 draft left eight choices for confirmation. Each was **decided under the owner's full-freedom grant as applied 2026-10-06 (ledger, "Owner directives and grants")**; the labels Q1–Q8 are kept because the text above cites them. Q6 (D22) was decided only provisionally, and the owner ratified it on 2026-10-06 with two amendments, one of which refines Q1 (SPEC §22.2 (de)).

- **Q1 — layout: Laravel's stock `jobs` table is v1's only layout** (§24.3), with its costs stated
  there: one-second resolution, a per-store lease, a 255-attempt ceiling on MySQL (32 767 on PG),
  time columns that overflow in 2038 on PG and 2106 on MySQL/MariaDB, and the `(id, created_at)`
  reuse precondition. *[Refined at D22's ratification (owner, 2026-10-06): the layout is unchanged,
  but its default NAME is `ferro_jobs`, so sharing a table with stock workers (mixed mode) is an
  explicit opt-in, `TABLE=jobs` (§24.3).]*
- **Q2 — liveness release: dropped from v1.** It is the one engine write that can enable a second
  execution, and v1 does not need it: without it a dead worker's job waits for its lease, exactly as
  with stock Laravel. Listed in §24.15; its constraints are kept in §24.8. The `liveness` field stays
  in the frozen RESERVE shape and a v1 engine refuses `true`.
- **Q3 — the retry licences: signed as the single merged D21** (§23.17 holds its wording). This
  section's licences — autocommit fenced verbs re-sent with the same token, a dedup-keyed single-job
  ENQUEUE, and RESERVE — are cited under it (§24.6).
- **Q4 — Symfony Messenger: demoted out of v1.** v1's drop-in tier is Laravel, so the transport and
  its parity suite (former G7) are post-v1, and M7 has no Messenger bar. **Cost:** a Symfony
  application gets the native API only in v1, and D16's and product-vision §4.3's naming of the
  Messenger seam is amended accordingly.
- **Q5 — `at_most_once`: cut, confirmed** (§24.15).
- **Q6 — the §3 / charter-rule-6 scope exception: decided as D22, RATIFIED by the owner on
  2026-10-06 with two amendments.** v1 as redefined by D16 includes a queue engine, which cannot
  exist without it (C8). Because it amends binding scope text, the grant could decide it only
  provisionally, and no G-slice code (G1 onward) could start before ratification. The owner ratified
  it with (a) the `ferro_jobs` default table and (b) store kinds with an opaque-bytes token (SPEC §21
  D22, §22.2 (de)). G1 may start.
- **Q7 — the `FerroJob::fail()` divergence: accepted.** On `LeaseLost` the stale holder skips
  `failed()` and `JobFailed`. That is safer than stock (no `failed_jobs` row for a job someone else
  runs), at the stated cost that a `failed()` callback with side effects (a notification) no longer
  fires on that holder.
- **Q8 — the PostgreSQL ≥ 12 gate: accepted.** It is stricter than Laravel's 9.5, and PG 11 is end of
  life. No pre-12 RESERVE form is built.

**Premises not yet measured, each owned by the slice named.** None may be relied on before its slice
measures it; a false one changes the plan, not the evidence.

- ~~PG `MATERIALIZED` locking CTE: affected ≤ LIMIT and no double return under concurrency (G1).~~
  **ASSERTED at G1b** (SPEC §22.2 (dg)): 16 sessions with a measured peak of 16 RESERVEs in flight,
  no reply over `k`, every job delivered once, `sum(attempts)` equal to the job count; re-confirmed by
  the review's chaos probe (40 sessions plus stock workers, three runs). Measured beside it: dropping
  `MATERIALIZED` changes nothing on PostgreSQL 16 (a `FOR UPDATE` CTE is never inlined), while the
  `WHERE id IN (… LIMIT k FOR UPDATE SKIP LOCKED)` form over-reserves even in one session.
- Dedup statement sequence on PG (G4) and its MySQL counterpart (G6).
- R4 on the real `Transport`, and the SIGTERM-while-parked exit bound (G3/G5).
- Sync and async client reentrancy when Laravel's SIGALRM handler runs `FerroJob::fail()` while a
  request is in flight on the DB session. ACKing on the idle queue session narrows this; G5 measures
  it.
- Whether PHP fake time breaks any upstream queue test in the Ferro column (G5).
- The index plan for deep queues (G3).
- MariaDB 10.6's `SKIP LOCKED` inside the engine transaction, and the RR-versus-RC footprint (G6).
- Which client rule G4 builds against (§24.8: the D1c deadline, or the `ioTimeout` clamp), and that
  chaos row 15 holds for it (G4).

Post-v1 only, owed by nothing in v1: Symfony Messenger's interfaces, `KeepaliveReceiverInterface`,
the Doctrine transport's `use_notify` and how it degrades with no notifications through Ferro, and
bundle-only registration.

**Measured while this section was drafted (scratchpad, to be reproduced in-tree at G3/G5):** R4.
PHP 8.4.19, `fread` on a blocking socketpair with a 3 s `stream_set_timeout`,
`pcntl_async_signals(true)`, SIGTERM at 1 s. The read returned after 4.00 s with `timed_out = true`,
and the SIGTERM handler had run.

---

### Appendix: review history

This section was drafted as M7-G0, attacked by an adversarial review before adoption, and revised
once. The review journalled **21 findings** (5 HIGH, 12 MEDIUM, 3 LOW-MEDIUM, 1 LOW); each was
re-verified at its cited line before the revision relied on it. Re-verification found four more
(R1–R4). The ids are the "F*n*" and "R*n*" references in the text above.

| # | Sev. | Finding | Disposition |
|---|---|---|---|
| F1 | HIGH | Swallowing `LeaseLost` in `delete()` cannot stop the `failed_jobs` row (`JobFailed` fires in `Job::fail()`'s `finally`) | `FerroJob::fail()` overridden; chaos row 13 (§24.11) |
| F2 | MEDIUM | Error codes collided with the HTTP draft | Codes allocated after §23's: `0x300F`, `0x3010`; decisions D21/D22 (§24.4) |
| F3 | HIGH | `at_most_once` RELEASE unimplementable; jobs lost silently | `at_most_once` cut (§24.15) |
| F4 | HIGH | Liveness release keyed to a session the app closes at will (`DB::reconnect()`, `purge()`, C1e-3) | Made opt-in and non-graceful-only in the revision, then dropped from v1 at adoption (§24.17 Q2) |
| F5 | HIGH | Engine-made phantom attempts fail never-run jobs under `--tries=1` | Undelivered reservations are unreserved (§24.8); residual stock-equivalent phantoms counted (chaos row 12) |
| F6 | MEDIUM | MySQL engine transactions cannot use the guarded `Checkout` entries | I3 restated: guarded entries for single DML, pin hooks for engine-owned transactions (§24.2) |
| F7 | MEDIUM | The rollback analogy for liveness release points the wrong safety direction | Analogy withdrawn; ordering after `abort_session` recorded (§24.2, §24.8) |
| F8 | MEDIUM | `QueueDatabaseQueueIntegrationTest` never touches Ferro; upstream's database CI is SQLite | Dropped from every column; provenance stated (§24.12) |
| F9 | MEDIUM | Clock and rounding claims wrong or unbounded | DB statement time, `ceil` delays, strict lease, 2 s floor, mixed-mode condition (§24.3) |
| F10 | HIGH | Long-poll RESERVE collides with the client read timeout | Wait bound plus client clamp; D1c interaction stated (§24.8) |
| F11 | MEDIUM | Isolation of engine statements unspecified | Engine-owned transactions READ COMMITTED (§24.4) |
| F12 | MEDIUM | RESERVE shape and batch premises must be spiked | `MATERIALIZED` CTE asserted at G1; batch ENQUEUE returns a count; dedup sequence a G4 premise (§24.4, §24.6) |
| F13 | MEDIUM | The tier's `LeaseLost` swallow broke transactional ack | In-transaction `LeaseLost` throws (§24.11) |
| F14 | LOW-MEDIUM | Lease index not transaction-aware; lost wake-ups | Register-then-sweep; after-commit hints (§24.5, §24.8) |
| F15 | MEDIUM | A lost RESERVE as `Retryable` contradicted §9.2 | `Indeterminate`, with a D21 licence (§24.6, C7) |
| F16 | MEDIUM | CANCEL of a RESERVE both "delivered" and "released" | One rule: delivered iff handed to a live writer, else unreserved (§24.4) |
| F17 | MEDIUM | MySQL multi-statement verbs not atomic within a transaction | One actor command per verb (§24.5) |
| F18 | MEDIUM | No precedent for engine-authored DML on an application table | D22 (C8) |
| F19 | LOW-MEDIUM | Six factual errors (batches, readonly pools, LISTEN, capability boundary, U+0000, Horizon stand-in) | Each corrected in place |
| F20 | LOW-MEDIUM | Ids are reused; tokens are long-lived | Fence widened to `(id, attempts, created_at)`; precondition stated (§24.3) |
| F21 | LOW | One flight per store serialises every waiter | Single flight per `(store, queue)`, `LIMIT k`, per-statement deadline (§24.8) |
| R1 | — | Inside a transaction an absent row must answer `LeaseLost`, not `gone` | §24.4 |
| R2 | — | `resolve_active` is private | G2 makes it `pub(crate)` (§24.5) |
| R3 | — | Laravel dispatches chain and batch side effects before the final `delete()` | Stated cost (§24.7) |
| R4 | — | A signal during a blocked PHP stream read restarts its timeout | Measured; reproduced in-tree at G3/G5 (§24.8, §24.17) |
