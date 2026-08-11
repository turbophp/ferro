# Ferro drop-in: known incompatibilities

Ferro is a drop-in for **Doctrine DBAL 4** by CONFIGURATION: `driverClass` + `driverOptions`, with
Grammar/Processor, the DBAL platforms and the stock schema managers untouched. These are the places
where a real application can still notice the difference. Each one is a deliberate consequence of the
engine's model — a per-host daemon that pools upstream connections in **transaction mode** and holds
the only database credentials — not a defect waiting to be fixed quietly. Every entry below was
MEASURED during M1-S8b; the acceptance numbers behind them are in
[`docs/dbal-suite/2026-08-11-results.md`](dbal-suite/2026-08-11-results.md).

SPEC §14 budgets the full per-package catalogue for M2. This is the page it grows from.

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
reconnect-and-retry. Measured at the acceptance gate — upstream's `TransactionTest::testCommitFailure`
and friends kill the backend session and expect `ConnectionLost`; under Ferro they get the refined
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

## Connection object

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
- **The first query against a backend that is DOWN can block for the OS connect timeout**
  (~127 s measured) rather than failing fast. Tracked in
  `docs/followups/2026-08-10-unbounded-backend-dial.md`.

---

## Identity and keys

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
- **A `bigint` at or above 2^32 currently cannot be READ.** `SELECT 4294967296::bigint` raises
  `ProtocolException: value tag 2: expected a int payload, got string` — on every backend, on every
  value policy, with or without `ext-msgpack`. This is a **defect**, not a policy: it affects any
  `bigint` PK past 4.29e9 and every epoch-milliseconds column. Tracked in
  `docs/followups/2026-08-11-i64-above-2e32-unreadable-in-php-client.md`.
- **A `LARGE_OBJECT` bind is materialised in memory** and is bounded by the 16 MiB maximum frame
  payload. A chunked bind would be a protocol change.
- **`BINARY` / `LARGE_OBJECT` are the only route to binary.** Every bare PHP string binds as text;
  the driver wraps those two `ParameterType`s in `Ferro\Bytes` for you.
- **MySQL/MariaDB sessions run at `time_zone = '+00:00'`.** `NOW()`, `CURDATE()` and `CURTIME()`
  return UTC on every Ferro MySQL connection. Doctrine and Laravel make the same choice; pooling
  determinism requires it.

---

## Schema, migrations and introspection

- **The stock PostgreSQL schema manager does not work yet.** DBAL's
  `PostgreSQLSchemaManager::selectIndexColumns()` selects `pg_index.indkey`, an `int2vector`, and the
  engine's PG read path has no mapping for that type — so `introspectTable()`, `listTableIndexes()`,
  schema diffing and `doctrine/migrations` all fail on PostgreSQL with a loud
  *"unsupported type for column \"indkey\""*. It is a single missing catalog type amplified across
  every introspection path (50 of the 78 non-passing PostgreSQL tests at the acceptance gate).
  The MySQL family is unaffected. Tracked in
  `docs/followups/2026-08-11-pg-int2vector-blocks-the-schema-manager.md`.
- **Two stock-Doctrine bind shapes are refused on PostgreSQL** and work on MySQL:
  a bound interval in the platform's date-arithmetic SQL (`? || ' SECOND'` makes PostgreSQL infer
  `text` for an `INTEGER` bind), and a boolean written through `Connection::insert()` **without** a
  `$types` entry (Doctrine's `BooleanType` hands the driver `int(1)`, and PostgreSQL's `bool` slot
  refuses an integer). The workaround for the second is to declare the type:
  `$conn->insert('t', ['flag' => true], ['flag' => Types::BOOLEAN])`. Tracked in
  `docs/followups/2026-08-11-pg-bind-matrix-narrower-than-libpq.md`.
- **The application user has no `CREATE DATABASE` privilege** in the testkit, deliberately. Anything
  that provisions databases needs its own credentials.

---

## Performance and shape

- **`iterateAssociative()` streams on PostgreSQL for parameterless queries and buffers otherwise**,
  and **always buffers on MySQL/MariaDB**, where engine-side row streaming is still deferred. The
  parameterised path buffers by necessity: a streamed request's terminal carries no `affected`
  field, so streaming it would make every parameterised write return `0` from
  `executeStatement()`.
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
- **`rowCount()` after a `SELECT` diverges by family AND by route.** PostgreSQL through the
  parameterised (buffered) route reports the row count; PostgreSQL through the zero-parameter
  (streamed) route reports `0`, because a stream terminal carries no `affected`; MySQL reports `0`
  either way. It is not normalised, because normalising means counting rows — the exact conflation
  §14 warns about — and DBAL itself documents `rowCount()`-on-a-`SELECT` as driver-specific.
  `rowCount()` after an `INSERT`/`UPDATE`/`DELETE` is always correct.
- **`free()` keeps `rowCount()`** while emptying rows and columns. Upstream is split on this
  (SQLite3 keeps its count, PgSQL answers `0`); ours is a choice.
- **`Ferro\Pg\Copy`** — the first-class replacement for `pdo_pgsql` COPY hacks named in SPEC §14 —
  does not exist yet. Deferred.

---

## Not supported, and where it went

- **SQLite:** there is no SQLite backend. `AnyPool` is `{ Pg | Mysql }`.
- **Named parameters at the driver:** positional `?` only. DBAL expands named parameters above the
  driver for `executeQuery()`/`executeStatement()`, so this is only visible if you call
  `prepare()->bindValue(':name', …)` yourself — exactly as capable as the stock mysqli driver.
- **`read_pool` as a config key:** it does not exist. Charter rule 6 forbids inferring read-vs-write,
  so the charter-compliant shape is a **second, explicitly configured connection** carrying
  `'driverOptions' => ['readonly' => true]`.
