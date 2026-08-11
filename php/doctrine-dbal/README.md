# ferro/doctrine-dbal-driver

A **Doctrine DBAL 4** driver whose execution layer talks to `ferrod` — the Ferro engine — through
`ferro/client`. An existing Doctrine or Symfony application switches by **configuration only**:
Grammar/Processor, the DBAL platform classes and the stock schema managers stay untouched.

Requires PHP ≥ 8.2 and `doctrine/dbal ^4.0`. Backends: **PostgreSQL** and **MySQL/MariaDB**.
There is no SQLite backend.

> Read [`docs/known-incompatibilities.md`](../../docs/known-incompatibilities.md) before you adopt
> this. It is short, every entry is measured, and two of them (a cancelled `SELECT` reported as an
> indeterminate write; the PostgreSQL schema manager) will change how you plan the migration.

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

    // Required ONLY if you call setTransactionIsolation() — see "Isolation levels".
    // 'wrapperClass'  => Ferro\DBAL\Wrapper\FerroConnection::class,
]);
```

**There are no database credentials here, and that is the point.** The DSN lives in the engine's
configuration (SPEC §12 / decision D8), so the DBAL `user`, `password`, `host`, `dbname` and
`charset` parameters are inert. Ops configures the pool once, per host; the application only names
it.

Symfony, in `config/packages/doctrine.yaml`:

```yaml
doctrine:
    dbal:
        driver_class: Ferro\DBAL\Driver
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
connection**, never an inference from the statement.

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

## Streaming

`iterateAssociative()` and its siblings stream row-by-row on **PostgreSQL** for parameterless queries
and buffer otherwise; on MySQL/MariaDB they buffer, because engine-side row streaming there is still
deferred. Interleaving a statement into an open iteration works (the remainder is drained first);
abandoning the canonical `foreach ($conn->iterateAssociative($sql) as $row) { … break; }` cancels the
stream. A **bound** iterator does not — `unset()` it or iterate the call directly. See the
known-incompatibilities page for the measurement.

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

- The stock **PostgreSQL schema manager** (and therefore `doctrine/migrations` on PG) does not work
  yet: one unsupported catalog type blocks index introspection.
- A **`bigint` at or above 2^32 cannot be read** through `ferro/client` today.
- **`lastInsertId()` throws on PostgreSQL** by design; use `INSERT … RETURNING id`.
- The first query against a backend that is **down** can block for the OS connect timeout rather than
  failing fast.

All four, with measurements and follow-up links, are on the known-incompatibilities page.
