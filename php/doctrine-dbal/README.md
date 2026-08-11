# ferro/doctrine-dbal-driver

A **Doctrine DBAL 4** driver whose execution layer talks to `ferrod` — the Ferro engine — through
`ferro/client`. An existing Doctrine or Symfony application switches by **configuration only**:
Grammar/Processor, the DBAL platform classes and the stock schema managers stay untouched.

Requires PHP ≥ 8.2 and `doctrine/dbal ^4.0`. Backends: **PostgreSQL** and **MySQL/MariaDB**.
There is no SQLite backend.

> Read [`docs/known-incompatibilities.md`](../../docs/known-incompatibilities.md) before you adopt
> this. It is short, every entry is measured, and two of them will change how you write code: a
> cancelled `SELECT` is reported as an indeterminate write, and on PostgreSQL a parameterless
> `executeQuery()` streams — so DML sent through it reports `rowCount() === 0` and, if you discard
> the result, swallows its error. Use `executeStatement()` for anything that writes.

## Install

```bash
composer require ferro/doctrine-dbal-driver
```

## Configure

```php
use Doctrine\DBAL\DriverManager;

$conn = DriverManager::getConnection([
    'driverClass'   => Ferro\DBAL\Driver::class,

    // The engine socket. `driverOptions.socket` is an equivalent spelling.
    'unix_socket'   => '/run/ferro/app.sock',
    // …or the TCP fallback, when the daemon is not local:
    // 'host' => '127.0.0.1', 'port' => 7777,

    'driverOptions' => [
        'pool'            => 'main',   // the engine pool name; default 'default'
        'readonly'        => false,    // see "Read-only connections" below
        'connect_timeout' => 2.0,      // seconds
        'io_timeout'      => 5.0,      // seconds
    ],

    // Optional, and recommended if you already know it — see "Platform selection".
    // 'serverVersion' => '17.10',

    // REQUIRED — not optional. Without it, an indeterminate write inside transactional() is
    // reported to your application as "There is no active transaction." See "transactional()"
    // below. It is also what makes setTransactionIsolation() work.
    'wrapperClass'  => Ferro\DBAL\Wrapper\FerroConnection::class,
]);
```

**There are no database credentials here, and that is the point.** The DSN lives in the engine's
configuration (SPEC §12 / decision D8), so the DBAL `user`, `password`, `dbname` and `charset`
parameters are inert. Ops configures the pool once, per host; the application only names it.
(`host`/`port` are read, but they address the ferrod **daemon** — the TCP fallback when no socket is
configured — never a database server. A configured socket takes precedence over a host, silently.)

**Unrecognised configuration is refused, not ignored.** A top-level `ferro` key, `read_pool` at any
level, and any unknown key inside `driverOptions` raise an `InvalidArgumentException` naming the
replacement. Earlier drafts of this project's docs showed `'ferro' => ['pool' => 'main']`, which
parsed to the pool named `default` — a different DSN, possibly a different database — without a
word.

Symfony, in `config/packages/doctrine.yaml`:

```yaml
doctrine:
    dbal:
        driver_class: Ferro\DBAL\Driver
        wrapper_class: Ferro\DBAL\Wrapper\FerroConnection
        options:                       # DoctrineBundle's `options` IS DBAL's `driverOptions`
            socket: /run/ferro/app.sock
            pool: main
```

`driverOptions.socket` is spelled out here rather than the top-level `unix_socket` because
`options` maps straight onto `driverOptions`, so this shape works whatever a given DoctrineBundle
release accepts at the top level. Both spellings are read by the driver.

### Platform selection

The driver learns the pool's **kind** from the engine handshake and the **version string** from the
handshake's pool metadata, falling back to a single `SELECT version()` if the engine has not resolved
one yet. If the version is still unknown when the platform is needed, it throws
`Ferro\DBAL\Exception\ServerVersionUnavailable` naming the pool — **never a default platform**,
because a wrong platform is a silently wrong SQL dialect rather than a clean error. Set DBAL's own
`'serverVersion'` parameter to skip the round trip entirely.

The version string is normalised for **PostgreSQL only**. On the MySQL family it is passed through
verbatim, deliberately: MariaDB is detected by the substring `MariaDB` in the version, so
`'11.8.8-MariaDB-ubu2404'` selects `MariaDB110700Platform` while a "helpfully" normalised `'11.8.8'`
would select `MySQL84Platform` — a different dialect.

### Read-only connections

The DBAL 4 SPI carries no read/write signal: `executeQuery('INSERT … RETURNING id')` is
indistinguishable from a `SELECT` at the driver boundary, and Ferro never infers one from SQL text.
So the driver declares every statement a **write** for the engine's §19.3 fate matrix. That is the
safe direction, and it has a cost: a `SELECT` cancelled server-side or killed by `statement_timeout`
is reported as `Ferro\DBAL\IndeterminateWriteException`.

If a connection genuinely only reads, declare it:

```php
'driverOptions' => ['pool' => 'replica', 'readonly' => true],
```

That is also the charter-compliant shape of a read/write split: a **second, explicitly configured
connection**, never an inference from the statement. There is no `read_pool` option, and the driver
refuses that key by name rather than ignoring it.

**Three consequences, all measured, none obvious.**

1. **It refuses ONE autocommit write route, not all of them.** `executeStatement($sql)` with no
   parameters — DBAL's write API, and a fact about the caller rather than the SQL, so refusing it
   does not violate charter rule 6 — is refused before it is sent. A *parameterised*
   `executeStatement($sql, [$p])`, either `executeQuery()` form, and `prepare()->executeStatement()`
   all share a code path with parameterised **reads**, so they are not refused and the write lands.
   Measured on PostgreSQL: 1 of 5 routes refused, 4 write.
2. **It can no longer make a failed write look retryable.** This is the half that matters. A lost
   autocommit statement on a readonly connection used to report
   `Ferro\DBAL\RetryableDriverException` — the marker framework retry loops key on — for a write
   that may well have landed. The driver now re-mints that to `IndeterminateWriteException`, so the
   declaration cannot invite a replay. In-transaction losses, pool timeouts and deadlocks keep their
   true verdicts.
3. **It IS enforced inside an explicit transaction, by changing the `BEGIN`.** The engine composes
   `BEGIN … READ ONLY` (PostgreSQL) / `START TRANSACTION READ ONLY` (MySQL family), so a write
   inside `transactional()` on such a connection fails with SQLSTATE `25006` — both families.

Because of (1), `readonly` is still a declaration you must be able to **defend**: a connection
carrying it must not be able to reach a write, including a rarely-taken audit `INSERT` in an error
path. What protects at-most-once when one slips through is (2), not the refusal.

All three are in [`docs/known-incompatibilities.md`](../../docs/known-incompatibilities.md) with the
measurements.

### Isolation levels

`Doctrine\DBAL\Connection::setTransactionIsolation()` emits `SET SESSION TRANSACTION ISOLATION LEVEL …`,
which on a transaction-mode pool lands on an arbitrary pooled connection, reports success and is
wiped by hygiene before the next `BEGIN`. This driver **refuses that statement, loudly**, rather than
letting it silently do nothing.

Add the wrapper and the API works properly — the level is captured as a typed value above the SQL
layer and rides `BEGIN`:

```php
'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class,
```

`READ UNCOMMITTED` is upgraded to `READ COMMITTED` — a genuine tightening on MySQL, never a
weakening.

## Exceptions

Everything the driver raises is a `Doctrine\DBAL\Driver\Exception`, so DBAL's normal conversion
applies and the stock per-family converters (SQLSTATE on PostgreSQL, vendor errno on MySQL) still do
their job. Two Ferro classes are added:

| class | meaning | retryable? |
|---|---|---|
| `Ferro\DBAL\IndeterminateWriteException` | the statement's fate is genuinely **unknown** | **no**, and it must never become so |
| `Ferro\DBAL\RetryableDriverException` | the fate is known and retrying is safe | yes (`Doctrine\DBAL\Exception\RetryableException`) |

The engine never transparently retries a user statement; retry is your policy. Do not add a blanket
retry on `IndeterminateWriteException` — that is the at-most-once violation the class exists to
prevent.

### `transactional()` — why the wrapper is required

`Doctrine\DBAL\Connection::transactional()` exempts five specific exception classes from the
rollback it performs after a failed commit; everything else triggers a `rollBack()` at nesting level
0, which throws `Doctrine\DBAL\Exception\NoActiveTransaction` **and replaces the exception in
flight**. So on the stock connection class:

```php
$conn->transactional(fn ($c) => $c->executeStatement('INSERT …'));
// link dies during COMMIT
//   → Doctrine\DBAL\Exception\NoActiveTransaction: "There is no active transaction."
//   the real fate survives only as getPrevious(); catch (IndeterminateWriteException) never fires
```

`Ferro\DBAL\Wrapper\FerroConnection` restores the driver's verdict, so the same code raises
`Ferro\DBAL\IndeterminateWriteException` as it should. Re-parenting the exception onto DBAL's exempt
list was not an option: `ConnectionLost` — the only honest fit, and what `pdo_pgsql` reports for this
event — is `final`, `DeadlockException` carries the retryable marker, and the remaining three each
assert a fate we do not know.

If your application must configure a **different** `wrapperClass` (for example
`Doctrine\DBAL\Connections\PrimaryReadReplicaConnection`), compose the trait instead:

```php
final class MyConnection extends PrimaryReadReplicaConnection
{
    use Ferro\DBAL\Wrapper\IndeterminateSafeTransactional;
}
```

## Streaming

`iterateAssociative()` and its siblings stream row-by-row on **PostgreSQL** for parameterless
queries and buffer otherwise; on MySQL/MariaDB they buffer, because engine-side row streaming there
is still deferred. Interleaving a statement into an open iteration works (the remainder is drained
first); abandoning the canonical `foreach ($conn->iterateAssociative($sql) as $row) { … break; }`
cancels the stream. A **bound** iterator does not — `unset()` it or iterate the call directly.

**The streamed route is the parameterless route, not the read route**, and it cannot be otherwise:
`Doctrine\DBAL\Connection::executeQuery($sql)` with no parameters reaches the driver's `query()`
whatever the verb, and charter rule 6 forbids inferring read-vs-write from SQL text. Two
consequences on PostgreSQL, both measured:

```php
$conn->executeQuery('UPDATE t SET x = 1')->rowCount();   // 0 — the stream terminal has no `affected`
$conn->executeQuery('INSERT INTO t VALUES (1)');         // duplicate key: NO exception, ever
$conn->executeStatement('UPDATE t SET x = 1');           // 10 — correct, on both families
```

Use `executeStatement()` for statements that write. The known-incompatibilities page has the full
route x family x verb table and the measurements.

## Native access

`getNativeConnection()` returns the `Ferro\Client\Connection` the driver is built on — not a `PDO`.
It carries the driver's type policy, so a value the driver refuses is refused there too; open your
own client connection if you need the raw canonical text.

## Development

```bash
composer install
./vendor/bin/phpunit                       # offline; tests/Live skip without the env below
./vendor/bin/phpstan analyse src --level 9

FERRO_TEST_PG_URL="postgres://ferro:ferro@127.0.0.1:55432/ferro" \
FERRO_TEST_MYSQL_URL="mysql://ferro:ferro@127.0.0.1:33060/ferro" \
FERRO_TEST_MARIADB_URL="mysql://ferro:ferro@127.0.0.1:33061/ferro" \
FERRO_FERROD_BIN=../../target/debug/ferrod \
  ./vendor/bin/phpunit tests/Live --fail-on-skipped
```

The upstream Doctrine DBAL functional subset runs through `testkit/dbal-suite.sh`; the recorded
numbers and their triage are in [`docs/dbal-suite/2026-08-11-results.md`](../../docs/dbal-suite/2026-08-11-results.md).

## Known gaps

- On PostgreSQL, a **parameterless `executeQuery()` streams**, so DML sent through it reports
  `rowCount() === 0` and — if the result is discarded — swallows its error. `executeStatement()` is
  correct on every route and family.
- **`lastInsertId()` throws on PostgreSQL** by design; use `INSERT … RETURNING id`, and configure
  Doctrine ORM's SEQUENCE identity strategy there (drop-in is config-only for DBAL, and explicitly
  **not** config-only for ORM on PostgreSQL).
- A PostgreSQL type outside Ferro's 14 canonical tags (arrays, ranges, `inet`, `interval`, `timetz`,
  PostGIS, `hstore`, enums…) reads as **PostgreSQL's own text**, exactly as `pdo_pgsql` hands it
  back — so an array does not arrive as a PHP array.
- **MySQL/MariaDB do not stream**; `iterate*()` buffers there.
- The first query against a backend that is **down** can block for the OS connect timeout rather than
  failing fast.

All of them, with measurements and follow-up links, are on the known-incompatibilities page.
