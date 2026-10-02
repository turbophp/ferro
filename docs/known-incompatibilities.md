# Ferro drop-in: known incompatibilities

Ferro is a drop-in by CONFIGURATION for two framework tiers:

- **`ferro/doctrine-dbal-driver`** — Doctrine DBAL 4, and DBAL 3.8+ through a second
  `driverClass`, via `driverClass` + `driverOptions`, with Grammar/Processor, the DBAL platforms and
  the stock schema managers untouched (SPEC §14).
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
- **On DBAL 3 the `driverClass` is `Ferro\DBAL\Dbal3\Driver`, and naming the other major's class
  is a fatal error.** DBAL 3 connects to learn the server version before choosing a platform only for
  a driver implementing `VersionAwarePlatformDriver`, which DBAL 4 deleted, and the two majors
  declare `quote()`, `lastInsertId()` and the transaction methods with signatures one class cannot
  both satisfy — PHP rejects that when the class is declared, which happens as soon as
  `DriverManager::getConnection()` loads it, before any connection opens. The isolation
  `wrapperClass` is per major too (`Ferro\DBAL\Dbal3\FerroConnection`). Behaviour is otherwise the
  same on both: one connection core, one binder, one value policy and one exception converter serve
  both, and the package's live suite runs unchanged against each (SPEC §22.2 (by);
  `Dbal3DriverTest::testItIsVersionAwareSoDbal3ConnectsBeforeChoosingAPlatform`).
- **Under Symfony's DoctrineBundle, set `charset` on the connection (any value — it is inert for
  Ferro) and do not set `dbname_suffix`.** DoctrineBundle 2's `ConnectionFactory` asks the driver for
  a platform BEFORE anything connects whenever `charset` is unset or `dbname_suffix` is set, only to
  default the charset — version-less on DBAL 3, with `serverVersion ?? ''` on DBAL 4. Before a
  connection the driver does not know even the backend family, so both majors refuse rather than
  guess a SQL dialect, and the message names this fix. Ferro has no client-side database name, so
  there is nothing for `dbname_suffix` (the `when@test` recipe default) to suffix. On a MySQL-family
  pool, setting `charset` also skips DoctrineBundle's default table collation
  (`utf8mb4_unicode_ci`); set `default_table_options` yourself to keep the DDL a `pdo_mysql` app
  would emit (SPEC §22.2 (by)).
- **No database credentials exist in PHP.** The DSN lives in the engine (SPEC §12 / D8). The DBAL
  `user`, `password`, `host`, `dbname` and `charset` parameters are therefore inert — measured at the
  acceptance gate, where upstream's `testInvalidUserName` / `testInvalidPassword` / `testInvalidHost`
  cannot fail and `testInheritCharsetFromPrimary` reports the engine's `utf8mb4` rather than the
  requested `latin1`. Tooling that shells out to `pg_dump`/`mysqldump` with the application's config
  cannot work; ops provisions separate dump credentials.
- **DBAL 3's `SqliteSchemaManager::createDatabase($path)` creates no file.** That method (deprecated
  upstream) opens a second connection with `path` set to the argument; a Ferro connection ignores
  `path`, because the database is the engine pool's (SPEC §12 / D8, and D14 confines every file the
  engine opens), so it reaches the pool's existing database and returns without error. Measured
  through upstream's `Doctrine\DBAL\Tests\Functional\Schema\SqliteSchemaManagerTest::testCreateAndDropDatabase`, which asserts the file
  exists. Provision a database as a pool instead. On DBAL 4 the same call fails loudly rather than
  silently: `SQLitePlatform::getCreateDatabaseSQL()` throws `NotSupported`
  (`docs/dbal-suite/2026-10-02-c5b-dbal3-results.md`).
- **A pool whose BACKEND is unreachable fails at `getDatabasePlatform()`, not at connect.**
  Connecting succeeds because the Ferro handshake never depends on backend availability; the platform
  needs the server version, which does. The failure is a loud
  `Ferro\DBAL\Exception\ServerVersionUnavailable` naming the pool — never a silently-defaulted
  platform, because a wrong platform is a wrong SQL dialect. Pin `'serverVersion' => 'PostgreSQL 17.10'` in the
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

- **`lastInsertId()` on PostgreSQL answers inside a transaction and throws outside one.** PG's
  protocol carries no such field. INSIDE a transaction the connection is pinned, so the driver runs
  `SELECT lastval()` there — exactly what `pdo_pgsql` runs — and hygiene's `DISCARD SEQUENCES` at
  checkout means it can only see this tenant's `nextval()` (SPEC §22.2 (ci);
  `LastInsertIdLiveTest::testPostgresNeverAnswersWithThePreviousTenantsSequenceValue`). It has
  `pdo_pgsql`'s failure mode too: with no `nextval()` yet in the transaction PostgreSQL raises
  `55000`, which aborts the transaction. **One difference from PDO:** the transaction's session starts
  with EMPTY sequence state (the same hygiene that makes the answer safe), so an INSERT made BEFORE
  `beginTransaction()` is invisible inside it — `pdo_pgsql` answers that key, Ferro raises `55000`
  and the transaction is aborted. Do the INSERT inside the transaction. OUTSIDE a transaction it throws, because the autocommit
  statement's connection has gone back to the pool and a follow-up would run on a **different
  connection** and return a silently wrong key — use `INSERT … RETURNING id` or a transaction. The
  thrown class is the SPI's own `Doctrine\DBAL\Driver\Exception\NoIdentityValue`, wrapped by DBAL into
  a `DriverException` as usual.
- **`lastInsertId()` has no sequence-name argument.** DBAL 4 removed the overload; this is upstream,
  not Ferro. On **DBAL 3**, which still has it, the name is used on PostgreSQL inside a transaction
  only — `currval(name)`, as `pdo_pgsql` runs it — and ignored on MySQL and SQLite, as PDO ignores it
  (SPEC §22.2 (ci)).
- **On DBAL 3, `lastInsertId()` throws where DBAL 3's SPI allows `false`.** `false` is a silently
  WRONG key in Doctrine ORM 2: its `IdentityGenerator` casts the answer with `(int)`, so `false`
  becomes the primary key `0`. The throw is a `Doctrine\DBAL\Driver\Exception` (DBAL 3 has no
  `NoIdentityValue`), which DBAL 3's wrapper converts to a `DriverException` as usual. DBAL 3's own
  upstream test of the `false` return is skipped on all three families Ferro serves, since each
  supports identity columns (SPEC §22.2 (by);
  `Dbal3ConnectionTest::testNoKeyThrowsADriverExceptionNeverFalse`).
- **`lastInsertId()` is cleared by a failed statement** — a deliberate divergence from PDO. Read it
  immediately after the successful INSERT.
- **FIXED in M2 — Doctrine ORM + PostgreSQL + the default IDENTITY strategy inserts.** This entry
  used to say it could not, and that ORM adoption on PostgreSQL needed the SEQUENCE strategy.
  `IdentityGenerator::generateId()` is `(int) $conn->lastInsertId()`, DBAL 4 makes ORM 3 map `AUTO` to
  `IDENTITY` on PostgreSQL, and the unit of work always inserts inside a transaction — where
  `lastInsertId()` now answers (the first entry). Measured through upstream doctrine/orm 3.7.3's own
  functional suite under its stock configuration: **1571 of 1597** executed tests pass against a
  `pdo_pgsql` control at 1597/1597, up from 397 (`docs/orm-suite/2026-10-02-local-results.md`). What
  remains is code that calls `lastInsertId()` itself OUTSIDE a transaction (4 of those tests). **ORM
  identity generation on PostgreSQL is config-only** (a database-defaulted sub-second `timestamptz`
  column is still refused — see *Values*).
- **ORM multi-table DELETE/UPDATE on class-table inheritance needs an explicit transaction.**
  `MultiTableDeleteExecutor` issues `CREATE TEMPORARY TABLE`, `INSERT`, `DELETE` and `DROP` as four
  separate statements with no transaction; on a transaction-mode pool statements 2-4 land on
  different connections. Wrap the query in `$conn->transactional(…)`. MEASURED by doctrine/orm
  3.7.3's functional suite on PostgreSQL and MariaDB: 6 tests
  (`Doctrine\Tests\ORM\Functional\AdvancedDqlQueryTest`,
  `Doctrine\Tests\ORM\Functional\ClassTableInheritanceTest`, `Doctrine\Tests\ORM\Functional\Ticket\DDC2090Test`) fail with the TEMP table missing, and pass through the
  stock driver (`docs/orm-suite/2026-10-02-local-results.md`).

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
- **On DBAL 3, nested transactions use savepoints only if the application asks.** DBAL 3.8–3.10
  default `nestTransactionsWithSavepoints` to false for every driver, so a nested
  `beginTransaction()` emits no `SAVEPOINT` and an inner `rollBack()` only marks the whole
  transaction rollback-only. Call `$conn->setNestTransactionsWithSavepoints(true)`; DBAL 4 always
  nests with savepoints. Upstream behaviour, not Ferro's — listed because it decides whether the
  savepoint guarantees on this page apply (SPEC §22.2 (by);
  `TransactionLiveTest::testDbalNestedTransactionsUseSavepointsOnThePinnedTransaction`).
- **Through Doctrine's `transactional()`, an indeterminate COMMIT is MASKED under "There is no active
  transaction."** — on both majors (DBAL 3 from 3.9.4). `commit()` resets the nesting level even
  when it fails, so `transactional()`'s own rollback throws `ConnectionException` and PHP chains the
  real `Ferro\DBAL\IndeterminateWriteException` beneath it as `getPrevious()`. Nothing retryable
  reaches the top, so a retry loop does not replay the transaction; but `catch
  (IndeterminateWriteException)` around `transactional()` does not match — walk the chain, or use
  `beginTransaction()`/`commit()`, whose `commit()` throws the class itself. An upstream defect no
  driver can reach (the wrapper throws first); pinned by a tripwire on each major, with the one-line
  upstream remedy in [`docs/followups/2026-10-02-transactional-masks-a-failed-commit.md`](followups/2026-10-02-transactional-masks-a-failed-commit.md)
  (`TransactionalCommitFailureTest::testTransactionalMasksAnIndeterminateCommitUnderNoActiveTransaction`).
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

- **On DBAL 3, `quote()` refuses a `BINARY` or `LARGE_OBJECT` value.** Both stock PostgreSQL drivers
  escape it as `bytea` there, so quoting it as TEXT would store different bytes — measured, `"\\x41"`
  stored one byte where `pdo_pgsql` stores four. A binary literal's syntax depends on the backend and
  the column type, so the driver points at the bound parameter, which carries the bytes intact. DBAL
  4's `quote()` takes no type, so the case does not arise there (SPEC §22.2 (by);
  `Dbal3ConnectionTest::testQuoteRefusesBinaryTypes`).

- **A value Doctrine would parse INCORRECTLY is refused, not converted.** Measured, with **no
  exception raised** by stock DBAL: `date '2026-00-05'` → `DateTime(2025-12-05)`;
  `datetime '0000-00-00 00:00:00'` → `DateTime(-0001-11-30)`; PostgreSQL's legal `time '24:00:00'` →
  `00:00:00`. Through this driver each of those raises
  `Ferro\DBAL\Exception\NonRepresentableValue` instead. **We refuse what PDO corrupts.**
  The full refused set: PG `time '24:00:00'`, PG `date`/`timestamp` `infinity`/`-infinity`, MySQL
  zero and zero-in dates, MySQL negative `TIME` intervals, sub-second `TIME`, and sub-second
  `TIMESTAMPTZ` (refused rather than truncated — silent precision loss is the same defect class).
  Read those columns through your own `Ferro\Client\Connection`, or cast them in SQL. **The refusal
  is at FETCH, before any Doctrine type runs**, so on PostgreSQL it covers a bare `SELECT now()` or
  DQL `CURRENT_TIMESTAMP()` read through the driver too — measured: 16 of doctrine/orm 3.7.3's
  functional tests (`Doctrine\Tests\ORM\Functional\QueryDqlFunctionTest`'s `DATE_ADD`/`DATE_SUB` cases) fail this way, where
  `pdo_pgsql` returns the fractional string.
- **`datetimetz` is re-rendered per platform.** `DateTimeTzType` has no fallback and accepts only
  `Y-m-d H:i:sO` on PostgreSQL and `Y-m-d H:i:s` on the MySQL family, so no canonical RFC3339 form
  parses anywhere. A whole-second `TIMESTAMPTZ` is re-rendered into the platform's own format; a
  sub-second one is refused.
- **A PHP float bound into a PostgreSQL `numeric` stores the float's exact shortest form, not PDO's
  14-digit rendering.** `pdo_pgsql` sends a float under `PARAM_STR` as PHP's `(string)` cast, which
  rounds to 14 significant digits first; Ferro sends the shortest decimal that round-trips to the same
  float. They store different values for a float that needs more digits (`0.1 + 0.2`: Ferro
  `0.30000000000000004`, PDO `0.3`) and at a rounding boundary of the column's scale
  (`0.00499999999999999` into `numeric(10,2)`: Ferro `0.00` — the correct rounding of that float — PDO
  `0.01`, a double rounding through `0.005`). Deliberate: Ferro stores what the application holds. Bind
  a decimal as a STRING to control the digits exactly (SPEC §22.2 (ci)).
- **On MySQL/MariaDB, a NULL-typed select-list column is refused before execution** — `SELECT NULL`,
  and, on MariaDB, a bare parameter in the select list (`SELECT ? AS p`), which MariaDB declares the
  same way when the statement is prepared. Admitting the type from that metadata would let the
  statement run and then fail to read its value — a write applied and reported as a failure — so it
  is refused with the statement never sent (SPEC §22.2 (ci)). On MySQL 8.4 a bare parameter is
  declared as a string instead and is ANSWERED: its cells are read with the executed result's
  metadata. Cast it (`CAST(NULL AS CHAR)`,
  `CAST(? AS SIGNED)`) to give the column a type.
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
  return UTC on every Ferro MySQL connection; pooling determinism requires one fixed zone. Neither
  framework pins it by default — Laravel does only when the connection config sets `timezone`, a key
  this engine never sees (see the Laravel section) — so **a `TIMESTAMP` written through a non-UTC
  session reads back in UTC after adoption.** The stored INSTANT is unchanged (MySQL keeps
  `TIMESTAMP` in UTC and renders it in the session's zone); its rendering is not, and code that
  reads the naive string as a wall clock — Eloquent does — sees it shifted (measured: written as
  `13:14:15` in a `+02:00` session, read as `11:14:15`). `DATETIME` has no zone and is unaffected.
  Check the server's `time_zone`, and the application's own, before adopting. (Corrected in M2-C1f:
  this entry used to say "Doctrine and Laravel make the same choice", which neither does.)

---

## Files and paths

- **A statement may only name a file inside the pool's allowed directory (SQLite).** SQLite is the
  one family where an ordinary statement can make the engine open a file the client named —
  `VACUUM INTO '<path>'`, and `ATTACH DATABASE '<path>'` inside a transaction. The write happens as
  the daemon's user, not PHP-FPM's, which is a confused deputy: SPEC §12/D8 keeps the database path
  in the engine precisely so PHP never learns it. **SPEC D14** confines it. The allowed directory
  defaults to the database file's own directory, so a snapshot beside the database works with no
  configuration — spelled as an ABSOLUTE path: a relative name resolves against the daemon's working
  directory, not the database's, and is refused unless that directory is inside the allowed one. An
  operator widens it per pool with `FERRO_POOL_<NAME>_ALLOW_DIR`. Outside it, the statement is refused
  with SQLite's own `SQLITE_AUTH` (errno 23) and **no file is created**.

  Three spellings are refused **wherever they point**, because the guard cannot check them
  (SPEC §22.2 (cf)): an `ATTACH` whose target is a bound parameter or an expression (`ATTACH ?`,
  `ATTACH 'a' || 'b'` — SQLite passes the guard no filename for those, so write the target as a
  literal; `VACUUM INTO ?` is unaffected), a `file:` URI (SQLite decodes its percent-escapes after the
  guard has looked), and a target that is itself a symlink. Taking a snapshot does not need any of
  them: the admin service's `BACKUP` verb (SPEC §7.6, OPERATE under D15) writes one by plain file
  name into the allowed directory and finalises it atomically.

  The enforcement point is SQLite's own authorizer, not a list of verbs: `VACUUM INTO` attaches its
  destination, so one guard covers both spellings and any future one. Nothing is parsed or rewritten
  — SQLite states which file it is about to open and the engine answers, so charter rule 6 never
  arises. Plain `VACUUM`, which stock Laravel's `dropAllTables()` ends in, opens no file and is
  untouched. SPEC §21 D14, §22.2 (bp), (cf).
- **On PostgreSQL and MySQL the question does not arise**, because neither dialect lets a statement
  name a path the engine would write — server-side `COPY TO '<file>'` and `SELECT … INTO OUTFILE`
  are the backend's own privileged operations, executed by the *database server* under its own
  privilege checks, not by `ferrod`.

---

## Schema, migrations and introspection

- **DBAL 3 on PostgreSQL: set the `user` parameter to the backend role, or a schema named after the
  role makes DBAL 3 pick the WRONG current schema.** DBAL 3's PostgreSQL schema manager works out
  the current schema by substituting the connection's `user` PARAMETER for `"$user"` in
  `SHOW search_path`, and a Ferro connection has no credentials, so no `user`. When the role owns a
  schema of its own name — PostgreSQL's documented secure-schema-usage pattern — DBAL 3 then takes
  the next schema in the path as current: the role's tables come back schema-qualified, `public`'s
  come back bare, and the comparator plans a CREATE of the bare name plus a DROP of the qualified one
  (measured: a destructive migration). Setting DBAL's `user` parameter to the backend role fixes it
  and is inert for Ferro's own connection; so does keeping `"$user"` out of the pool's `search_path`.
  DBAL 4 asks the server (`SELECT current_schema()`) and is not affected. Pinned in all three cells
  by `SearchPathLiveTest::testTheCurrentSchemaFollowsTheUserParamOnDbal3AndTheServerOnDbal4`
  (SPEC §22.2 (bz)).
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
[`docs/laravel-suite/2026-09-10-c2-results.md`](laravel-suite/2026-09-10-c2-results.md),
[`docs/laravel-suite/2026-09-15-c3-6b-sqlite-results.md`](laravel-suite/2026-09-15-c3-6b-sqlite-results.md) and
[`docs/laravel-suite/2026-10-02-c1f-mysql-results.md`](laravel-suite/2026-10-02-c1f-mysql-results.md).

### Getting in

- **Four driver names are registered: `ferro-pgsql`, `ferro-sqlite`, `ferro-mysql` and
  `ferro-mariadb`.** One per Laravel family: Laravel 11 resolves MariaDB through its OWN `mariadb`
  driver (`MariaDbConnection`, `MariaDbGrammar`, `MariaDbBuilder`), so a MariaDB application's
  one-word change is `mariadb` → `ferro-mariadb`. `ferro-mysql` against a MariaDB server is what
  stock `mysql` against MariaDB is — the MySQL grammar, which Laravel 10 applications still run —
  and it differs: `castAsJson()` compiles to `cast(? as json)`, a syntax error on MariaDB, and
  `threadCount()` answers `null` (measured through stock `pdo_mysql` too, so it is the grammar's,
  not Ferro's). `FerroConnections::register()` wires them through Illuminate's own
  `Connection::resolverFor()` map, and adoption is the one-word `driver` change in the connection
  config that SPEC §15 asks for. SPEC §22.2 (cb).
- **`ferro_socket` is the only socket key read.** A stock MySQL config carries `unix_socket`
  (Laravel's `DB_SOCKET`) naming mysqld's OWN socket; it is ignored like `host`, `username` and
  `password`, because it describes a server the application no longer dials. (Before the C1f review
  it was read as the ferrod socket, so a leftover `DB_SOCKET` dialled the database server and failed
  with a wire-magic error naming neither key.)
- **On MySQL/MariaDB, the `database` key must name the pool's database.** On PostgreSQL and SQLite it
  is a label — the pool's DSN chooses the database — but `MySqlBuilder` passes it into every
  `information_schema` query (`table_schema = ?`) behind `hasTable()`, `getTables()`,
  `getColumns()`, `dropAllTables()` and `migrate`, so a label that differs from the pool's database
  makes the schema builder look at the wrong schema: `hasTable()` false for a table that exists,
  `migrate` re-creating its own repository table, `migrate:fresh` dropping nothing — or, if the label
  names ANOTHER schema, listing that schema's tables and dropping same-named ones in the real
  database. The tier compares it with `select database()` the first time the schema builder is
  asked for and refuses a mismatch, naming both. SPEC §22.2 (cb).
- **Laravel's MySQL session keys are ignored: `strict`, `modes`, `isolation_level`, `timezone`,
  `charset` and `collation`.** Stock `MySqlConnector::configureConnection()` turns them into
  `SET SESSION sql_mode`, `SET SESSION TRANSACTION ISOLATION LEVEL`, `SET time_zone` and
  `SET NAMES … COLLATE …` on every connect. Under Ferro the pool owns the session, the connector
  never runs, and a pooled session is reset between tenants, so there is nowhere for a per-connection
  setting to live: the SERVER's defaults apply instead (and `time_zone` is pinned to UTC, above).
  Measured on MariaDB 10.11 with Laravel's shipped `'strict' => true`: stock gets
  `ONLY_FULL_GROUP_BY`/`NO_ZERO_DATE` in `sql_mode`, `utf8mb4_unicode_ci` and the configured
  isolation; Ferro gets the server's `sql_mode` without them, the server's default `utf8mb4`
  collation and `REPEATABLE READ`. Either direction can bite: an app relying on strict mode loses
  it on a lenient server, and a `'strict' => false` app gets `1055` errors on a strict one. Put
  these in the server's global configuration.
- **The driver NAME is part of your application's behaviour, and it is the single largest source of
  difference.** Illuminate resolves connections by name, and upstream's own tests — plus plenty of
  third-party packages — branch on `$connection->getDriverName()`. Measured on PostgreSQL over 633
  driver-agnostic cases: under the opt-in stock-name alias Ferro is **indistinguishable from
  `pdo_pgsql`** (605/607 on both, identical ordered failure sets, and the two remaining reproduce
  through stock PDO, so they are upstream's own). Under `ferro-pgsql` it is 570/579, and every one of
  those nine differences was positively identified in upstream source as code branching on
  `$this->driver`: an `expectException` never armed, a `match` picking the wrong expected type, a
  `markTestSkipped` that never fires. SPEC §22.2 (am), (ar). **The same artifact appears on the MySQL
  family**, where `ferro-mysql` SKIPS 8 tests the control runs — `#[RequiresDatabase]` resolves from
  the driver name (4 name `['mysql', 'mariadb']`, 3 data sets `['sqlite', 'mysql', 'mariadb']`, 1
  `['mysql', 'mariadb', 'pgsql']`; `ferro-mariadb` adds one `'mariadb'`-only test, 9) — and fails one
  `match ($this->driver)`. **Under the alias the MySQL family is indistinguishable from `pdo_mysql`
  on all 633 driver-agnostic cases** — MySQL 8.4 587/587 and MariaDB 11.8 588/588 on both sides,
  identical skip sets, no failures. (C1f recorded two errors per alias column, Laravel's
  `castAsJson()` reaching a `quote()` that refused; C1g closed it.) SPEC §22.2 (cb), (cc).
- **The alias is opt-in because it hijacks every connection of that name.** `register(['pgsql' =>
  'ferro-pgsql'])` makes name-branching code take its PostgreSQL path — and also captures any
  connection in the application that was meant to dial PostgreSQL directly. An application with a
  mixed setup should rename those connections' driver instead.
- **A `read`/`write` split is not implemented, although SPEC §15's own config example shows one.**
  Every statement runs on the connection's one pool. With a `read` key in the config, Illuminate
  builds the read side with its STOCK connector, so `getReadPdo()` throws `Unsupported driver
  [ferro-pgsql]` (measured) — while queries keep working, because this tier never routes through it.
  Explicit replica routing is SPEC §7.5 (M4). SPEC §22.2 (bw).
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
- **`DB::escape()`, `toRawSql()`, `->dd()`, `castAsJson()` and `DB::pretend()` with string
  bindings work on every family** — each of them reaches `quote()` (`pretend()` through
  `Grammar::substituteBindingsIntoRawSql()`, to log the statement it did not run). **FIXED in M2-C1g
  for MySQL/MariaDB and SQLite**, where all five used to be refused. `quote()` is client-side with no
  engine round trip (SPEC §21 D5). **The literal it builds is NOT always `pdo_*`'s bytes, on
  purpose.** A PDO driver escapes by the LIVE escape mode of the connection it is about to send on;
  this shim has no connection, and the only mode it could read is the pool's advertised
  `literals_are_standard` — one probed session's value, cached. C1g's adversarial review showed that
  value describing a DIFFERENT session three ways on MySQL (an application changing its own
  `sql_mode` inside a transaction, an operator changing the server's global mode, and an
  `init_connect` that applies to a fresh dial but not after `COM_RESET_CONNECTION`), and a literal
  built from it broke out each time. So the shim uses forms that mean the same bytes in EVERY mode:
  a string with no backslash — the only mode-dependent character — is quoted by doubling `'`
  (byte-identical to `pdo_pgsql` and `pdo_sqlite`); a string with a backslash becomes `E'…'` on
  PostgreSQL and `_utf8mb4 X'<hex>'` on MySQL/MariaDB. Measured: every case reads back exactly
  under both escape modes on MySQL/MariaDB and both `standard_conforming_strings` settings on
  PostgreSQL, and a GBK connection charset (reachable through `init_connect`, where `pdo_mysql`
  itself breaks out) cannot break the hex form. **What you will see:** on MySQL
  `DB::escape("O'Brien")` is `'O''Brien'` where `pdo_mysql` returns `'O\'Brien'` (the same string
  to the server — upstream's `MySql/EscapeTest::testEscapeString` compares the bytes and records
  it), and a value containing a backslash renders as `E'…'` or hex in `toRawSql()` output. It
  refuses only when the pool's backend FAMILY is unknown. One more difference, reachable only by
  calling `getPdo()->quote()` directly (Illuminate rejects it first): invalid UTF-8 containing a
  backslash fails LOUDLY on MySQL (`1300`, under any `sql_mode`) where `pdo_mysql` would build a
  literal. SPEC §22.2 (as), (at), (cc).
- **`DB::getPdo()->query()` and `prepare()` are refused.** There is no PDO underneath and a
  `PDOStatement` cannot be built without one, so code that reaches past Illuminate to the raw handle
  must use `DB::select()`/`DB::statement()` instead. Measured by upstream's
  `MySql/DatabaseMySqlConnectionTest::testLastInsertIdIsPreserved`, which runs
  `DB::getPdo()->query('SELECT 1')` inside a `QueryExecuted` listener — the property that test
  exists for (the key survives a query in between) holds; only the call it makes to prove it is
  refused. SPEC §22.2 (cc).
- **`lastInsertId()` is sticky on the HANDLE, exactly as PDO's is** — and that is a deliberate
  divergence from the client underneath it. The client's value is per-STATEMENT and cleared on the
  way in to every request, which is right for a pooled engine: a statement can land on another
  backend connection and a carried-over key would be silently wrong. PDO's belongs to the handle.
  `Processor::processInsertGetId()` looks like it reads the id immediately, but `Connection::insert()`
  fires `QueryExecuted` first and any listener that runs a query clears the client's value in
  between — measured as **182 of 225 errors on one line** before the shim remembered it. So the
  handle remembers each non-null key from its own writes and never clears it, which is what
  `pdo_sqlite` returns in every case, including after a rollback; a reconnect starts a fresh handle
  with no key, as PDO's does. One divergence: where `pdo_sqlite` answers `"0"` for a handle that has
  generated nothing, this tier THROWS, since `"0"` is indistinguishable from a key. SPEC §22.2 (bn),
  (bw).
- **On MySQL/MariaDB, `lastInsertId` is the STATEMENT's key, as `pdo_mysql`'s is** — the opposite
  of the sticky handle above, because the two PDO drivers differ: `pdo_mysql` answers the statement
  just executed and `"0"` after one that generated none, a `SELECT`, an `UPDATE` or a rollback
  (measured). Both readers follow it: stock `MySqlConnection::insert()` stores the key on the
  connection inside the statement's own run, before `QueryExecuted`, and `FerroMySqlConnection::insert()`
  does exactly that with the engine's per-statement key — so `insertGetId()` on a table without an
  auto-increment answers `0`, as on PDO; and `DB::getPdo()->lastInsertId()` answers the last
  statement's key or `"0"`. (The C1f review caught that second reader still returning the SQLite
  handle's sticky key on a MySQL pool — a STALE key after a keyless insert, a `SELECT` or an
  `INSERT IGNORE`; a wrong key is worse than none, §22.2 (m).) SPEC §22.2 (cb).

### Values

- **Rows arrive as driver-native strings, not as the SPEC §9 value objects.** The tier hands
  Illuminate what PDO hands it, because Illuminate's own helpers index into the result —
  `Builder::pluck($col, $key)` breaks outright on a value object. This is the same `RawStringValuePolicy`
  hand-off the Doctrine tier takes.
- **One deliberate divergence from PDO is kept on PostgreSQL, because it is safer there.** A
  `TIMESTAMPTZ` arrives as canonical RFC3339, which Illuminate's `Date::parse` fallback reads as UTC.
  PDO's `+00` form is silently reinterpreted in the application timezone. SPEC §22.2 (al).
- **On MySQL the same tag arrives as `pdo_mysql` returns it — to the second**: a `TIMESTAMP` column
  — what `$table->timestamps()` creates — reads back as the naive UTC wall clock
  (`2017-11-12 13:14:15`), every Ferro MySQL session being pinned to `+00:00`. Eloquent WRITES naive
  strings, which the server reads in that session, so a naive read is what makes the round trip
  stable; RFC3339 here would make Illuminate read a UTC instant where it wrote a wall clock — a silent
  shift for any app not running in UTC. SPEC §22.2 (cb). **A FRACTIONAL column renders differently**:
  the canonical wire text carries no fraction when it is zero and exactly six digits otherwise, while
  `pdo_mysql` pads to the column's own precision — `TIMESTAMP(3)` holding `.25` is `.250` there and
  `.250000` here, and a whole second in `TIMESTAMP(6)` is `.000000` there and nothing here. Same
  instant, and Illuminate's date casts parse both; a consumer comparing the raw string of such a
  column (`pluck()` keys, `getRawOriginal()`) sees the difference. The column's precision is not on
  the wire for any tag. Pinned live by
  `MySqlConnectionLiveTest::testAFractionalTimestampRendersTheCanonicalFractionNotTheColumnsPrecision`.
- **A `bigint` at or above 2^32 reads** — see *Values* above; the defect that page-entry records was
  fixed at M1-S9 and affected this tier too.

### Errors and transactions

- **On MySQL a duplicate key is recognised by errno 1062, not by `pdo_mysql`'s wording.** Stock
  `MySqlConnection::isUniqueConstraintError()` matches the message text `Integrity constraint
  violation: 1062`, which a Ferro error does not carry (it carries the server's own message, with the
  errno and SQLSTATE as fields), so before C1f `createOrFirst()` re-threw the duplicate it exists to
  catch — 9 framework-suite errors. `FerroMySqlConnection` decides it from the errno, the same
  "by type, not by wording" rule this tier applies to lost connections and concurrency errors.
  **Application code that matches on that exact phrase in the message will not find it**; code that
  catches `UniqueConstraintViolationException`, as Laravel's own does, works. SPEC §22.2 (cb).
- **`FerroQueryException` puts SQLSTATE in `getCode()`** — PDO's convention, and the *opposite* of
  the sibling Doctrine tier's errno convention. It is not cosmetic: with the sibling's convention a
  real PostgreSQL `40001` propagates out of `DB::transaction(attempts: 3)` instead of being retried,
  which is mutation-proven live.
- **`select()` is fate-declared a WRITE**, so the cancelled-`SELECT` entry at the top of this page
  applies here too. It is not a conservative guess that could be tightened: `PostgresProcessor::processInsertGetId`
  runs `insert … returning id` through `selectFromWriteConnection()`, so treating `select()` as a
  read would mis-declare the commonest Eloquent write on PostgreSQL.

- **Illuminate's lost-connection RETRY is decided by type, not by message.** Stock Illuminate
  reconnects and re-runs a statement whose error text looks like a lost connection. Through Ferro it
  does so for a failed DIAL (nothing was sent) and for a connection loss the engine itself classified
  as known-fate, and **never** for a write whose fate is unknown — whatever the text says. Since
  `select()` is declared a write (above), a SELECT lost mid-flight is reported rather than silently
  retried, which PDO would do. The same type rule stops `DB::transaction(attempts:)` re-running a
  transaction whose COMMIT reply was lost. SPEC §22.2 (bw).
- **FIXED — a long-lived connection now recovers by itself after a `ferrod` restart.** Until M2-C1e-3
  every statement on such a connection failed as an indeterminate write until the application called
  `DB::reconnect()` or the worker restarted — which mattered for Octane and queue workers — because
  the client classified a request that failed while still being WRITTEN ("write failed after 0 of 47
  bytes") as fate-unknown. A frame that never fully left the client cannot have executed, so it is
  now `Retryable`, and Illuminate's lost-connection retry reconnects and re-runs the statement once —
  including when the restart lands in the middle of a `cursor()`. A statement whose reply was lost
  AFTER it was sent still surfaces, as it must, and that is the one case recovery is not immediate:
  over TCP, or while an engine is draining, the kernel can still accept the first write after a
  restart, so that statement surfaces as an indeterminate write and the NEXT one recovers. The
  native client and the Doctrine tier recover too: a session a failure closed is replaced before the
  next request. SPEC §22.2 (bw), (bx).

### Schema and migrations

- **On MySQL/MariaDB, `Schema::withoutForeignKeyConstraints()` and `dropAllTables()` run on ONE
  pinned connection, and a bare `Schema::disableForeignKeyConstraints()` outside a transaction is
  REFUSED.** `SET FOREIGN_KEY_CHECKS=0` is session state, and each statement is its own checkout, so
  stock's sequence — the `SET`, then the `DROP`s — ran the `DROP`s on a connection where checks were
  back on: on MariaDB `migrate:fresh`, `db:wipe` and `RefreshDatabase` failed with `1451` on any
  schema where a parent table sorts before its child (`MySqlBuilder` drops in name order; MySQL 8.4
  accepts that single multi-table `DROP`), and on both servers the seeder idiom
  `disableForeignKeyConstraints(); Parent::truncate();` failed, as did dropping a referenced parent
  alone. Found by the C1f review; the
  framework suite never saw it because its own FK schemas sort child-first. `Ferro\Laravel\Schema\FerroMySqlBuilder`
  (and `FerroMariaDbBuilder`) now run both inside one transaction — DDL's implicit commit happens on
  the PINNED connection, after the `SET` — and refuse the bare call rather than silently doing
  nothing, since its only effect would be on a connection the next statement does not reach. Inside
  `DB::transaction()` it works, for the same reason. **Cost:** `withoutForeignKeyConstraints(fn)`
  runs `fn` in a transaction, so a non-DDL statement inside it is rolled back if `fn` throws, which
  stock does not do. Use `withoutForeignKeyConstraints()` or wrap the block in `DB::transaction()`.
  SPEC §22.2 (cb).
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

**§15's acceptance bar is NOT met.** It asks for the `illuminate/database` integration suite
GREEN on MySQL, PostgreSQL and SQLite, plus a Laravel demo app. What exists is a curated 89-file
subset run on all three families against controls: green on none of them through the `ferro-*` name
(the driver-name artifact above); under the alias, indistinguishable from the control on
PostgreSQL (whose own two failures reproduce through `pdo_pgsql`) and on the MySQL family (since
C1g). Upstream's family-SPECIFIC directories run under the alias only, short of their controls by
the deliberate `''` rendering and the refused `getPdo()->query()` above. The demo app EXISTS
(`testkit/laravel-demo/`, SPEC §22.2 (ch)) and is a per-PR CI gate, Ferro and stock control alike, on
every family — but it is seven tests of one application's database surface (sessions, password
resets, the `database` queue and batches, the `database` cache), not the suite. Its first run found
the `database` queue worker processing nothing on any Ferro family (`PDO::ATTR_DRIVER_NAME`, fixed).
The Eloquent ORM's own test suite is not run on any family. The recorded MySQL-family numbers are in the
C1f and C1g results docs.

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
