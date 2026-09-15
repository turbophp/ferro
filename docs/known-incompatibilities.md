# Ferro drop-in: known incompatibilities

Ferro is a drop-in by CONFIGURATION for two framework tiers:

- **`ferro/doctrine-dbal-driver`** — Doctrine DBAL 4, via `driverClass` + `driverOptions`, with
  Grammar/Processor, the DBAL platforms and the stock schema managers untouched (SPEC §14).
- **`ferro/laravel`** — Illuminate's `Connection` execution layer, via one driver name in the
  connection config, with the stock Grammar, Processor and Schema builder untouched (SPEC §15).

These are the places where a real application can still notice the difference. Almost every one is a
deliberate consequence of the engine's model — a per-host daemon that pools upstream connections in
**transaction mode** and holds the only database credentials — rather than a defect waiting to be
fixed quietly; the few that are defects say so in those words.

**How to read this page.** Every entry carries a citation: a SPEC §22.2 changelog letter, a recorded
acceptance-suite run under `docs/dbal-suite/` or `docs/laravel-suite/`, a named test, or a follow-up
document. That is a contract, not a style: `ci/check-incompatibilities-doc.sh` runs in CI and fails
the build when a citation stops resolving, when a follow-up loses its status, or when this page cites
a follow-up that has since been RESOLVED.

The gate exists because this page had already rotted in the way that matters most. It told readers
that a `bigint` at or above 2^32 could not be read — a **fixed** defect, with a live test proving the
whole int64 range — and that a dead backend could wedge a query for two minutes, which had been
bounded a milestone earlier. A stale "this is broken" is worse than a stale README: people believe it
and build workarounds. So an entry whose defect is fixed is **rewritten in place and kept**, marked
FIXED with the milestone that closed it, rather than quietly deleted.

Sections are shared unless their heading names a package. Where a behaviour differs by tier, both
answers are given.

---

## Errors

### A cancelled or timed-out `SELECT` is reported as an **indeterminate write**

This is the first entry because it is the one that looks alarming and must not be "fixed" by the
obvious workaround.

A statement that PostgreSQL cancels server-side — including one killed by an operator's
`statement_timeout`, a normal production setting — surfaces through this driver as
`Ferro\DBAL\IndeterminateWriteException`: *"your write may or may not have landed"*, for a statement
that wrote nothing.

**Why.** The DBAL 4 SPI carries **no read/write signal**. `executeQuery('INSERT … RETURNING id')` is
indistinguishable from a `SELECT` at the driver boundary, and charter rule 6 forbids inferring the
answer from SQL text. So the driver declares every statement a **write**. The engine's §19.3 fate
matrix reads that flag in two places, and the second is the `57014` (query cancelled) override: with
no transaction open, a cancelled statement is *cancelled/non-retryable* when the client declared
`readonly` and *write-unconfirmed/**indeterminate*** when it did not.

That is the **safe** direction — a lost write is never reported as "provably did not apply" — and
this is its cost. It is a new failure *shape*, not a lost one: the same statement through the native
`Ferro\Client` API, which declares `readonly` for reads, gets a clean "statement cancelled or timed
out".

- **Do NOT add a blanket retry on `IndeterminateWriteException`.** That is exactly the
  at-most-once violation the branch exists to prevent, and it is why
  `IndeterminateWriteException` deliberately does **not** implement
  `Doctrine\DBAL\Exception\RetryableException`.
- **If a connection genuinely only reads, say so:** `'driverOptions' => ['readonly' => true]`
  restores the clean cancellation answer for that connection. Explicit configuration, never
  inference.
- **Inside a transaction the question does not arise.** A cancelled statement rolls the transaction
  back, the `tx_id` is tombstoned, and the fate is *known*: `Retryable`.

Pinned by `ExceptionMappingLiveTest::testACancelledSelectIsIndeterminateOnAWriteConnectionAndNotOnAReadonlyOne`,
which asserts BOTH cells, so the cost is falsifiable and cannot later be silently "fixed" by guessing
read-vs-write from SQL.

### A cancelled statement arrives with a **NULL SQLSTATE** and `getCode() === 0`

An application that branches on `$e->getSQLState()` sees `null` where PDO would give `57014`. The
engine's §19.3 `57014` override rebuilds the error payload and drops both the SQLSTATE and the vendor
errno. The fate is carried by the exception **class** (`Ferro\DBAL\IndeterminateWriteException` vs a
bare `DriverException`) and by the `/proto` code on the chained client exception. Every *other*
error keeps its SQLSTATE, and on the MySQL family its vendor errno as well — which is what the stock
`API\MySQL\ExceptionConverter` keys on.

### A lost connection is **not** `Doctrine\DBAL\Exception\ConnectionLost`

Where DBAL has one bucket, Ferro has two, because the difference is the whole point of the engine:

| what happened | Ferro | why |
|---|---|---|
| the statement's fate is **known** (never transmitted, a declared read, or an in-tx statement whose transaction is now dead) | `Ferro\DBAL\RetryableDriverException` (implements `RetryableException`) | retry is safe |
| the statement's fate is **unknown** | `Ferro\DBAL\IndeterminateWriteException` (deliberately NOT retryable) | retry could apply it twice |

Neither extends `ConnectionLost`, on purpose: frameworks treat `ConnectionLost` as
reconnect-and-retry. Measured at the acceptance gate — upstream's own
`Doctrine\DBAL\Tests\Functional\TransactionTest::testCommitFailure` and friends kill the backend
session and expect `ConnectionLost`; under Ferro they get the refined
answer instead. **If your application catches `ConnectionLost`, catch `Doctrine\DBAL\Exception\DriverException`
instead and branch on the two Ferro classes.**

### A type-policy refusal arrives as a plain `DriverException`, with no fate

A value the client's §9.1 policy refuses (`Ferro\Client\Error\TypePolicyException`) carries **no fate
branch at all** — it is a client-side policy refusal, not a database error. It does reach the
exception converter (anything that is not a `Doctrine\DBAL\Driver\Exception` would escape DBAL's
conversion entirely), and it comes out a plain `Doctrine\DBAL\Exception\DriverException`: never an
indeterminate write, never upgraded to retryable. The driver's own refusals
(`Ferro\DBAL\Exception\NonRepresentableValue`, `UnsupportedStatement`, `ServerVersionUnavailable`,
`BackendFamilyUnknown`) take the same branch-less path. A malformed payload remains a
`ProtocolException`.

---

## Connection object (Doctrine DBAL)

- **`getNativeConnection()` returns a `Ferro\Client\Connection`, not a `PDO`.** Anything calling
  `pg_escape_string($native, …)`, `$native->real_escape_string()` or a `PDO::` method will fatal.
- **`getNativeConnection()` does not escape the driver's type boundary.** It hands back the very
  connection the driver built, which carries `Ferro\DBAL\Value\DbalValuePolicy` — so a column the
  driver refuses is refused there too. To read a `24:00:00`, a zero-in date, an `infinity` or a
  sub-second `timestamptz`, open a connection of your OWN through `Ferro\Ferro::connect()` (default
  `M1ValuePolicy`, or `RawStringValuePolicy` for the raw canonical text). The refusal is a
  **driver-tier policy**, not an engine limitation.
- **No database credentials exist in PHP.** The DSN lives in the engine (SPEC §12 / D8). The DBAL
  `user`, `password`, `host`, `dbname` and `charset` parameters are therefore inert — measured at the
  acceptance gate, where upstream's `testInvalidUserName` / `testInvalidPassword` / `testInvalidHost`
  cannot fail and `testInheritCharsetFromPrimary` reports the engine's `utf8mb4` rather than the
  requested `latin1`. Tooling that shells out to `pg_dump`/`mysqldump` with the application's config
  cannot work; ops provisions separate dump credentials.
- **A pool whose BACKEND is unreachable fails at `getDatabasePlatform()`, not at connect.**
  Connecting succeeds because the Ferro handshake never depends on backend availability; the platform
  needs the server version, which does. The failure is a loud
  `Ferro\DBAL\Exception\ServerVersionUnavailable` naming the pool — never a silently-defaulted
  platform, because a wrong platform is a wrong SQL dialect. Pin `'serverVersion' => '17.10'` in the
  DBAL params if you want a zero-round-trip answer.
- **A backend that is DOWN fails within `checkout_timeout`, not the OS connect timeout.** This page
  used to record the opposite — an unbounded dial that could wedge a first query for the ~127 s the
  OS takes to give up. `Pool::checkout` has bounded `backend.connect()` with `checkout_timeout`
  since M1 Phase B, and both backends bound their out-of-band cancel side-connection dial at a fixed
  2 s (SPEC §22.2 (aj)). TCP keepalive on an already-established connection was then closed as
  WONTFIX-as-code (§22.2 (au)): it is a pool-DSN parameter on both families, PostgreSQL defaults it
  ON at 7200 s and MySQL defaults it OFF, and hardcoding either would remove an operator knob.

---

## Identity and keys (Doctrine DBAL, and the ORM)

- **`lastInsertId()` throws on PostgreSQL, always.** PG's protocol carries no such field and Ferro
  refuses to emulate it with `SELECT lastval()`, because on a transaction-mode pool the follow-up
  runs on a **different connection** and returns a silently wrong key. Use `INSERT … RETURNING id`.
  The thrown class is the SPI's own `Doctrine\DBAL\Driver\Exception\NoIdentityValue`, wrapped by DBAL
  into a `DriverException` as usual.
- **`lastInsertId()` has no sequence-name argument.** DBAL 4 removed the overload; this is upstream,
  not Ferro.
- **`lastInsertId()` is cleared by a failed statement** — a deliberate divergence from PDO. Read it
  immediately after the successful INSERT.
- **Doctrine ORM + PostgreSQL + the default IDENTITY strategy cannot insert.**
  `Doctrine\ORM\Id\IdentityGenerator::generateId()` is `(int) $conn->lastInsertId()`, and DBAL 4
  defaults PostgreSQL to `GENERATOR_TYPE_IDENTITY`. Configure the **SEQUENCE** strategy for the
  PostgreSQL platform through the ORM's `Configuration::setIdentityGenerationPreferences()`, keyed on
  `Doctrine\DBAL\Platforms\PostgreSQLPlatform::class`. (The exact constant for the strategy is ORM's
  own and is not restated here: `doctrine/orm` is not a dependency of this repository, so nothing in
  this file has been verified against it. The mechanism, and the reason it is needed, are what this
  entry is asserting.)

  **Drop-in is config-only for DBAL, and is explicitly NOT config-only for ORM on PostgreSQL.** The
  engine's pooling model is not bent to fit an ORM default; the honest one-line configuration is.
- **ORM multi-table DELETE/UPDATE on class-table inheritance needs an explicit transaction.**
  `MultiTableDeleteExecutor` issues `CREATE TEMPORARY TABLE`, `INSERT`, `DELETE` and `DROP` as four
  separate statements with no transaction; on a transaction-mode pool statements 2-4 land on
  different connections. Wrap the query in `$conn->transactional(…)`. (Read from `doctrine/orm 3`
  during M1-S8b research; like the entry above, it has **not** been re-verified at the acceptance
  gate, because the ORM suite is not run — see `docs/dbal-suite/2026-08-11-results.md`.)

---

## Transactions and session state

- **`setTransactionIsolation()` requires the wrapper.** Configure
  `'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class`. Without it the raw
  `SET SESSION TRANSACTION ISOLATION LEVEL …` / `SET SESSION CHARACTERISTICS AS …` statement is
  **REFUSED, loudly**, with a message naming this one-line fix. That refusal is the kind treatment:
  left alone, the statement lands on an arbitrary pooled connection, reports SUCCESS, is wiped by
  hygiene before the next `BEGIN`, and your application silently gets the pool default while
  `getTransactionIsolation()` keeps reporting the level it asked for. With the wrapper, the level is
  captured as a typed enum above the SQL layer and rides `BEGIN` on the next transaction.
- **`READ UNCOMMITTED` is upgraded to `READ COMMITTED`** — never weakened. PostgreSQL treats them as
  the same level; on MySQL this is a genuine, documented **tightening**.
- **`setAutoCommit(false)` must be configured before the first connect**, on
  `Doctrine\DBAL\Configuration`. Calling `Connection::setAutoCommit(false)` on an already-connected
  connection opens nothing (DBAL's `beginTransaction()` lives in `connect()`, which returns early)
  and the next `commit()` raises `NoActiveTransaction`. That is upstream behaviour, measured.
- **`setAutoCommit(false)` pins a backend connection for the whole request**, and re-pins immediately
  after every commit — measured with `pg_current_xact_id()` identical across statements. It works;
  it just turns Ferro's central win off.
- **Savepoints work normally.** DBAL nests transactions client-side and emits ordinary savepoint SQL;
  those statements ride the same pinned `tx_id` as the transaction that opened them.
- **A `PRAGMA`, `SET` or any other session setting applied at connect-time does not stick.** It lands
  on whichever pooled connection served that one call. Doctrine's opt-in
  `AbstractSQLiteDriver\Middleware\EnableForeignKeys` is the clearest case: it runs
  `PRAGMA foreign_keys=ON` when the driver connects, and through Ferro that is a no-op. **Foreign
  keys are enforced anyway** — the engine sets and verifies the pragma on every SQLite connection it
  dials, so the guarantee comes from the pool rather than from your middleware. Settings the engine
  does not apply at dial cannot be made to stick from PHP; ask for them on the pool.
- **Several statements in one `executeStatement()` are refused, on every backend.** Ferro prepares
  every statement, so `"CREATE TABLE a (…); CREATE TABLE b (…)"` in a single call fails —
  PostgreSQL with `42601` *"cannot insert multiple commands into a prepared statement"*, SQLite with
  *"Multiple statements provided"*. Send them one at a time.

---

## Values

The driver's type boundary is a **conversion step the driver owns**, not SQL rewriting. It exists
because Doctrine's stock type layer is, measured on 4.4.4, a silently-corrupting calendar parser.

- **A value Doctrine would parse INCORRECTLY is refused, not converted.** Measured, with **no
  exception raised** by stock DBAL: `date '2026-00-05'` → `DateTime(2025-12-05)`;
  `datetime '0000-00-00 00:00:00'` → `DateTime(-0001-11-30)`; PostgreSQL's legal `time '24:00:00'` →
  `00:00:00`. Through this driver each of those raises
  `Ferro\DBAL\Exception\NonRepresentableValue` instead. **We refuse what PDO corrupts.**
  The full refused set: PG `time '24:00:00'`, PG `date`/`timestamp` `infinity`/`-infinity`, MySQL
  zero and zero-in dates, MySQL negative `TIME` intervals, sub-second `TIME`, and sub-second
  `TIMESTAMPTZ` (refused rather than truncated — silent precision loss is the same defect class).
  Read those columns through your own `Ferro\Client\Connection`, or cast them in SQL.
- **`datetimetz` is re-rendered per platform.** `DateTimeTzType` has no fallback and accepts only
  `Y-m-d H:i:sO` on PostgreSQL and `Y-m-d H:i:s` on the MySQL family, so no canonical RFC3339 form
  parses anywhere. A whole-second `TIMESTAMPTZ` is re-rendered into the platform's own format; a
  sub-second one is refused.
- **An integer parameter above `PHP_INT_MAX` is refused client-side**, not silently saturated
  (a PHP `(int)` cast saturates rather than wrapping). Bind it as a string against a `numeric`
  column, or keep it in `bigint` range.
- **FIXED at M1-S9 — a `bigint` at or above 2^32 reads.** This page used to say it did not, and it
  said so for a month after the fix shipped, which is why this page now has a gate (see *How to read
  this page*). The defect was real: `SELECT 4294967296::bigint` raised
  `ProtocolException: value tag 2: expected a int payload, got string` on every backend and every
  value policy, taking every `bigint` PK past 4.29e9 and every epoch-milliseconds column with it.
  The whole int64 range reads as of M1-S9 (m1-s8c), pinned live by `I64RangeLiveTest`, which
  round-trips each boundary through a real `int8`/`BIGINT` **column** rather than only a literal.
  Kept rather than deleted because the entry was public long enough to be believed.
- **A `LARGE_OBJECT` bind is materialised in memory** and is bounded by the 16 MiB maximum frame
  payload. A chunked bind would be a protocol change.
- **`BINARY` / `LARGE_OBJECT` are the only route to binary.** Every bare PHP string binds as text;
  the driver wraps those two `ParameterType`s in `Ferro\Bytes` for you.
- **MySQL/MariaDB sessions run at `time_zone = '+00:00'`.** `NOW()`, `CURDATE()` and `CURTIME()`
  return UTC on every Ferro MySQL connection. Doctrine and Laravel make the same choice; pooling
  determinism requires it.

---

## Files and paths

- **A statement may only name a file inside the pool's allowed directory (SQLite).** SQLite is the
  one family where an ordinary statement can make the engine open a file the client named —
  `VACUUM INTO '<path>'`, and `ATTACH DATABASE '<path>'` inside a transaction. The write happens as
  the daemon's user, not PHP-FPM's, which is a confused deputy: SPEC §12/D8 keeps the database path
  in the engine precisely so PHP never learns it. **SPEC D14** confines it. The allowed directory
  defaults to the database file's own directory, so `VACUUM INTO 'snapshot.db'` beside the database
  works with no configuration; an operator widens it per pool with `FERRO_POOL_<NAME>_ALLOW_DIR`.
  Outside it, the statement is refused with SQLite's own `SQLITE_AUTH` (errno 23) and **no file is
  created**.

  The enforcement point is SQLite's own authorizer, not a list of verbs: `VACUUM INTO` attaches its
  destination, so one guard covers both spellings and any future one. Nothing is parsed or rewritten
  — SQLite states which file it is about to open and the engine answers, so charter rule 6 never
  arises. Plain `VACUUM`, which stock Laravel's `dropAllTables()` ends in, opens no file and is
  untouched. SPEC §21 D14, §22.2 (bp).
- **On PostgreSQL and MySQL the question does not arise**, because neither dialect lets a statement
  name a path the engine would write — server-side `COPY TO '<file>'` and `SELECT … INTO OUTFILE`
  are the backend's own privileged operations, executed by the *database server* under its own
  privilege checks, not by `ferrod`.

---

## Schema, migrations and introspection

- **FIXED at M1-S9 — the stock PostgreSQL schema manager works.** This page used to say it did not:
  DBAL's `PostgreSQLSchemaManager::selectIndexColumns()` selects `pg_index.indkey`, an `int2vector`,
  which the PG read path had no mapping for, and it took `introspectTable()`, `listTableIndexes()`,
  schema diffing and `doctrine/migrations` down with it (50 of the 78 non-passing PostgreSQL tests
  at the S8b gate). `int2vector`/`oidvector` are admitted as TEXT as of M1-S9 (SPEC §22.2 (ae)) and
  the re-measurement recovered exactly those 50
  ([`docs/dbal-suite/2026-09-09-a5-results.md`](dbal-suite/2026-09-09-a5-results.md)). Kept here
  rather than deleted because the entry was public long enough to be believed.
- **FIXED at M1-S9 — the two stock-Doctrine bind shapes that were refused on PostgreSQL.** A bound
  interval in the platform's date-arithmetic SQL (`? || ' SECOND'`, where PostgreSQL infers `text`
  for an `INTEGER` bind) and a boolean written through `Connection::insert()` without a `$types`
  entry (Doctrine's `BooleanType` hands the driver `int(1)`) both work now: the PG bind widened
  `I64` into `text` and into `bool`, the latter value-gated to 0/1 (SPEC §22.2 (af)). The
  16 tests this blocked came back in the same re-measurement.
- **The application user has no `CREATE DATABASE` privilege** in the testkit, deliberately. Anything
  that provisions databases needs its own credentials.
- **On SQLite, a non-simple `ALTER TABLE` must run inside a transaction.** `SQLitePlatform` rebuilds
  a table through `CREATE TEMPORARY TABLE __temp__x AS SELECT …` plus four more statements, and
  `AbstractSchemaManager::alterTable()` runs those as SEPARATE calls with no transaction around
  them. A TEMP table is session state, and on a transaction-mode pool it does not survive to the
  next request — so statement 2 fails with `no such table: __temp__x`. Wrap it:

  ```php
  $conn->transactional(static fn ($c) => $c->createSchemaManager()->alterTable($diff));
  ```

  and it works, because a transaction pins the connection. SQLite has transactional DDL, so the
  wrap costs nothing. This is not a SQLite quirk — **PostgreSQL loses an autocommit TEMP table
  exactly the same way** (`42P01` on the next request); SQLite is simply the first family whose
  stock Doctrine schema manager depends on it. 24 of the 28 non-passing SQLite tests at the C3-6a
  gate are this one cause
  ([`docs/dbal-suite/2026-09-15-c3-6a-sqlite-results.md`](dbal-suite/2026-09-15-c3-6a-sqlite-results.md)).

---

## Performance and shape (Doctrine DBAL)

- **`iterateAssociative()` streams — on every family and on both the parameterless and the
  parameterised path.** This entry has been narrowed twice as its two stated causes were removed.
  The parameterised path was said to buffer "by necessity, because a streamed terminal carries no
  `affected`"; that was measured FALSE (§22.2 (ag)) and it now streams (§22.2 (ah)). MySQL/MariaDB
  were said to buffer because engine-side streaming was deferred; that deferral is now closed
  (§22.2 (n)) and they stream too. Nothing here buffers for iteration any more.
- **Abandoning an iteration cancels the stream — for the canonical idiom.**
  `foreach ($conn->iterateAssociative($sql) as $row) { break; }` cancels: the driver `Result` is
  destroyed by refcount and frees itself. `$it = $conn->iterateAssociative($sql); foreach ($it as $row) { break; }`
  does **not**: `$it` keeps it alive, and the remainder is transferred on the next statement
  (measured: 99 975 of 100 000 rows). A live reference is indistinguishable from a caller who may
  still fetch, so this is a PHP refcount fact, not a design choice. **Iterate the call directly, or
  `unset()` the iterator.**
- **A statement issued *while* an iteration is open drains the remainder into memory first** — the
  session is strictly single-in-flight. The canonical
  `foreach (iterate…) { executeStatement(…) }` idiom therefore keeps working, at the cost of
  buffering what is left, which is what PDO does unconditionally.
  `Ferro\DBAL\Connection::settledRowCount()` reports how many rows that has cost on this connection:
  `0` for pure iteration and for a properly abandoned one, non-zero only for interleaving.
- **On a streamed read, a statement's ERROR surfaces mid-iteration, not from `executeQuery()`.** The
  open reads only the column header. The fate classification is unchanged; the vantage point is.
- **`rowCount()` on a PostgreSQL streamed `SELECT` is DRAIN-THEN-ANSWER** (M1-S9 B1b, §22.2 (ah);
  this REPLACES the old "streamed route reports 0" divergence, whose stated cause — "a stream
  terminal carries no `affected`" — was measured false, §22.2 (ag)). On PostgreSQL BOTH routes now
  stream and both answer what `pdo_pgsql` answers: the command-tag row count. The cost is honest
  and only paid when asked: calling `rowCount()` on a still-open streamed result finishes the read
  (buffering the remaining rows, which stay fetchable); pure iteration still never buffers.
  **MySQL/MariaDB report `0` for a `SELECT` — still, and now for a different reason.** They used to
  report `0` because they buffered and a buffered MySQL SELECT has no affected count; since B2c they
  stream, and the post-drain OK packet a MySQL SELECT ends with also reports `0` (SPEC §22.2 (n)'s
  second measured fact). The observable answer is unchanged; only the mechanism behind it is. A result `free()`d before its terminal keeps
  `0` — a command tag that was never read has no honest value.
  `rowCount()` after an `INSERT`/`UPDATE`/`DELETE` is always correct on both families.
- **`free()` keeps `rowCount()`** while emptying rows and columns. Upstream is split on this
  (SQLite3 keeps its count, PgSQL answers `0`); ours is a choice.
- **`Ferro\Pg\Copy`** — the first-class replacement for `pdo_pgsql` COPY hacks named in SPEC §14 —
  does not exist yet. Deferred.

---

## Laravel / Eloquent (`ferro/laravel`)

Everything above applies to this tier too, because it sits on the same engine and the same client.
What follows is what is different about reaching it through Illuminate. The numbers come from
upstream `laravel/framework` v11.51.0's own integration tests, run against a real server through one
`ferrod`, each column reproduced twice and each against a CONTROL running the identical tests through
upstream's own PDO driver:
[`docs/laravel-suite/2026-09-10-c2-results.md`](laravel-suite/2026-09-10-c2-results.md) and
[`docs/laravel-suite/2026-09-15-c3-6b-sqlite-results.md`](laravel-suite/2026-09-15-c3-6b-sqlite-results.md).

### Getting in

- **Two driver names are registered: `ferro-pgsql` and `ferro-sqlite`.** `FerroConnections::register()`
  wires them through Illuminate's own `Connection::resolverFor()` map, and adoption is the one-word
  `driver` change in the connection config that SPEC §15 asks for. **MySQL/MariaDB is not
  registered at all** — the engine has supported it since M1-S6, but this tier has no
  `FerroMySqlConnection`, so §15's acceptance bar, which names MySQL, is **not met**. Said here
  rather than left to be discovered.
- **The driver NAME is part of your application's behaviour, and it is the single largest source of
  difference.** Illuminate resolves connections by name, and upstream's own tests — plus plenty of
  third-party packages — branch on `$connection->getDriverName()`. Measured on PostgreSQL over 633
  driver-agnostic cases: under the opt-in stock-name alias Ferro is **indistinguishable from
  `pdo_pgsql`** (605/607 on both, identical ordered failure sets, and the two remaining reproduce
  through stock PDO, so they are upstream's own). Under `ferro-pgsql` it is 570/579, and every one of
  those nine differences was positively identified in upstream source as code branching on
  `$this->driver`: an `expectException` never armed, a `match` picking the wrong expected type, a
  `markTestSkipped` that never fires. SPEC §22.2 (am), (ar).
- **The alias is opt-in because it hijacks every connection of that name.** `register(['pgsql' =>
  'ferro-pgsql'])` makes name-branching code take its PostgreSQL path — and also captures any
  connection in the application that was meant to dial PostgreSQL directly. An application with a
  mixed setup should rename those connections' driver instead.
- **On SQLite the alias is NOT the same escape hatch, and that asymmetry is structural.** Upstream's
  own tests open throwaway SQLite connections regardless of the family under test, none of them
  carrying a `ferro_socket`, so registering the alias costs **42 extra errors** for the two
  name-gated skips it buys. SPEC §22.2 (bn).

### The PDO shim

- **`getPdo()` returns a `Ferro\Laravel\FerroPdoShim`, not a `PDO`.** It implements what Illuminate
  actually calls; anything else fails loudly rather than silently returning something plausible. The
  shim is the seam transactions run through, which is why `ManagesTransactions` — Laravel's
  transaction counter, savepoint naming, events and `attempts:` retry loop — is inherited unchanged
  rather than reimplemented.
- **`DB::escape()`, `toRawSql()` and `->dd()` work on PostgreSQL and are REFUSED on SQLite.**
  `quote()` is client-side with **no engine round trip** (SPEC §21 D5), so it is gated on the per-pool
  `literals_are_standard` bit that `HELLO_ACK` advertises — and today only the PostgreSQL backend
  advertises it. On a SQLite pool the value is `null`, which is **fail-closed**: it is never read as
  false (that would claim backslashes are escapes) and never as true. The refusal names the pool.
  SPEC §22.2 (as), (at).
- **`lastInsertId()` is sticky on the connection, exactly as PDO's is** — and that is a deliberate
  divergence from the client underneath it. The client's value is per-STATEMENT and cleared on the
  way in to every request, which is right for a pooled engine: a statement can land on another
  backend connection and a carried-over key would be silently wrong. PDO's belongs to the handle.
  `Processor::processInsertGetId()` looks like it reads the id immediately, but `Connection::insert()`
  fires `QueryExecuted` first and any listener that runs a query clears the client's value in
  between — measured as **182 of 225 errors on one line** before the shim remembered it. So the
  connection remembers each non-null key from its own writes and never clears it, which is what
  `pdo_sqlite` returns in every case, including after a rollback. SPEC §22.2 (bn).

### Values

- **Rows arrive as driver-native strings, not as the SPEC §9 value objects.** The tier hands
  Illuminate what PDO hands it, because Illuminate's own helpers index into the result —
  `Builder::pluck($col, $key)` breaks outright on a value object. This is the same `RawStringValuePolicy`
  hand-off the Doctrine tier takes.
- **One deliberate divergence from PDO is kept, because it is safer.** A `TIMESTAMPTZ` arrives as
  canonical RFC3339, which Illuminate's `Date::parse` fallback reads as UTC. PDO's `+00` form is
  silently reinterpreted in the application timezone. SPEC §22.2 (al).
- **A `bigint` at or above 2^32 reads** — see *Values* above; the defect that page-entry records was
  fixed at M1-S9 and affected this tier too.

### Errors and transactions

- **`FerroQueryException` puts SQLSTATE in `getCode()`** — PDO's convention, and the *opposite* of
  the sibling Doctrine tier's errno convention. It is not cosmetic: with the sibling's convention a
  real PostgreSQL `40001` propagates out of `DB::transaction(attempts: 3)` instead of being retried,
  which is mutation-proven live.
- **`select()` is fate-declared a WRITE**, so the cancelled-`SELECT` entry at the top of this page
  applies here too. It is not a conservative guess that could be tightened: `PostgresProcessor::processInsertGetId`
  runs `insert … returning id` through `selectFromWriteConnection()`, so treating `select()` as a
  read would mis-declare the commonest Eloquent write on PostgreSQL.

### Schema and migrations

- **`search_path` belongs in TWO places on PostgreSQL** — on the `ferrod` pool DSN, because the
  session is pooled and a `SET search_path` from PHP would not survive to the next statement; and in
  the Laravel connection config as well, because `PostgresBuilder::getSchemas()` reads it from the
  config array and never asks the server. SPEC §22.2 (aq).
- **`Schema::dropAllTables()` on SQLite goes through `Ferro\Laravel\Schema\FerroSQLiteBuilder`.**
  Upstream takes a statement branch only for an in-memory database; for a file one it calls
  `refreshDatabaseFile()`, which is `file_put_contents($connection->getDatabaseName(), '')` — and
  under Ferro that name is the config **label**, not a path (SPEC §12/D8). Left alone it creates and
  truncates a junk file named after your connection while every table survives; that is not
  hypothetical, it produced two such files in this repository's root during a mutation run. The
  override takes the statement branch unconditionally, runs the four **stock-grammar** statements in
  one transaction (`PRAGMA writable_schema` is connection-scoped) with `vacuum` after the commit, and
  makes `refreshDatabaseFile()` throw. No SQL is generated here. SPEC §22.2 (bn).
- **A SQLite `ALTER TABLE` that drops or changes a column referenced by a foreign key is refused, and
  there is no execution-layer remedy.** `SQLiteGrammar::compileAlter()` emits six statements and
  `Blueprint::build()` runs them as six checkouts, so its leading `PRAGMA foreign_keys = OFF` never
  reaches the `drop table` and the child row refuses it (errno 787). Three remedies were ruled out by
  measurement, not by argument: as Illuminate runs it → refused; all six inside **one** transaction →
  still refused, because that pragma is a no-op inside a transaction; transaction plus the
  transaction-legal `PRAGMA defer_foreign_keys` → still refused. Inventing a fourth would mean the
  tier substituting SQL the stock grammar did not emit. Drop the constraint, alter, re-add — or use
  PostgreSQL. SPEC §22.2 (bn).
- **`selectResultSets()` returns one result set.** `ExecOk` carries exactly one `cols`+`rows`, and
  carrying N of them is a breaking wire change; it is deferred on the ground that no tier can reach a
  second result set today (DBAL 4's driver `Result` has no `nextRowset`, and the only Illuminate
  consumer would be a MySQL tier that does not exist). The MySQL `CALL` blind spot that used to sit
  underneath it — a prepared `CALL` declaring zero result columns, so rows came back cell-less — is
  **fixed**: `cols` falls back to the executed result set's metadata. SPEC §22.2 (av), (aw).

### Not established

§15's acceptance bar names PostgreSQL, MySQL and SQLite. **MySQL is not run**, because this tier does
not register it; SQLite and PostgreSQL are, with controls. The Eloquent ORM's own test suite is not
run on any family.

---

## Not supported, and where it went

- **SQLite: supported since M2/C3.** A pool is spelled `sqlite:///path/to/file.db` on `ferrod` —
  a bare filesystem path is deliberately NOT inferred as SQLite, and an in-memory database is
  refused outright (two `:memory:` connections are two different databases, so a pool would hand
  different tenants different data). Every connection runs in WAL mode with foreign keys enforced,
  both verified at dial rather than requested. Two things to know before adopting it: the
  `ALTER TABLE` entry under *Schema, migrations and introspection*, and that **an ISO date in a
  SQLite column reads back as a string**, not a `Ferro\Date` — SQLite has no date-time storage
  class, and `pdo_sqlite` behaves the same way, so Doctrine's own types handle it unchanged.
- **Named parameters at the driver:** positional `?` only. DBAL expands named parameters above the
  driver for `executeQuery()`/`executeStatement()`, so this is only visible if you call
  `prepare()->bindValue(':name', …)` yourself — exactly as capable as the stock mysqli driver.
- **`read_pool` as a config key:** it does not exist. Charter rule 6 forbids inferring read-vs-write,
  so the charter-compliant shape is a **second, explicitly configured connection** carrying
  `'driverOptions' => ['readonly' => true]`.
