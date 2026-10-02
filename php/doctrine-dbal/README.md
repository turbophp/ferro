# ferro/doctrine-dbal-driver

A **Doctrine DBAL** driver whose execution layer talks to `ferrod` — the Ferro engine — through
`ferro/client`. An existing Doctrine or Symfony application switches by **configuration only**:
Grammar/Processor, the DBAL platform classes and the stock schema managers stay untouched.

Requires PHP ≥ 8.2 and `doctrine/dbal ^4.0` — or **`^3.8`**, through a second `driverClass` (see
[DBAL 3](#dbal-3)). Backends: **PostgreSQL**, **MySQL/MariaDB** and **SQLite**.

> Read [`docs/known-incompatibilities.md`](../../docs/known-incompatibilities.md) before you adopt
> this. It is short, every entry is measured, and two of them (a cancelled `SELECT` reported as an
> indeterminate write; a non-simple `ALTER TABLE` on SQLite needing a transaction around it) will
> change how you plan the migration.

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

    // Optional, PostgreSQL only — see "Platform selection". Write the BANNER form: a bare '17.10'
    // (or any MySQL/SQLite version) names no backend family and is refused.
    // 'serverVersion' => 'PostgreSQL 17.10',

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
        charset: utf8mb4               # REQUIRED under DoctrineBundle — inert for Ferro, see below
        options:                       # DoctrineBundle's `options` IS DBAL's `driverOptions`
            socket: /run/ferro/app.sock
            pool: main
```

**Set `charset`, and do not set `dbname_suffix`.** Without a `charset` (or with a `dbname_suffix`,
which the `when@test` recipe adds), DoctrineBundle 2's `ConnectionFactory` asks the driver for a
platform before anything has connected, only to choose a default charset. Before a connection the
driver does not know even the backend family, so it refuses rather than guess a SQL dialect, and the
message names this fix. The value is inert: the connection charset belongs to the engine's pool.
Ferro has no client-side database name, so there is nothing for `dbname_suffix` to suffix. On a
MySQL-family pool, setting `charset` also skips DoctrineBundle's default table collation, so add
`default_table_options: { charset: utf8mb4, collation: utf8mb4_unicode_ci }` to keep the DDL a
`pdo_mysql` application would emit.

`driverOptions.socket` is spelled out here rather than the top-level `unix_socket` because
`options` maps straight onto `driverOptions`, so this shape works whatever a given DoctrineBundle
release accepts at the top level. Both spellings are read by the driver.

### DBAL 3

On `doctrine/dbal` 3.8 or later, configure the DBAL 3 classes instead — everything else on this page
applies unchanged, because both majors share one connection core, binder, value policy and exception
converter:

```php
'driverClass'  => Ferro\DBAL\Dbal3\Driver::class,
// only if you call setTransactionIsolation():
'wrapperClass' => Ferro\DBAL\Dbal3\FerroConnection::class,
```

```yaml
doctrine:
    dbal:
        driver_class: Ferro\DBAL\Dbal3\Driver
```

**Why a second class rather than one that adapts.** DBAL 3 connects to learn the server version
before choosing a platform only for a driver implementing `VersionAwarePlatformDriver`, an interface
DBAL 4 deleted; and the two majors declare `quote()`, `lastInsertId()` and the transaction methods
with signatures one class cannot both satisfy. PHP checks that when the class is declared, so naming
the wrong major's class fails as soon as `DriverManager::getConnection()` loads it, before any
connection (`Declaration of … must be compatible with …`, or `Interface
"Doctrine\DBAL\VersionAwarePlatformDriver" not found`), rather than misbehaving. Upgrading DBAL 3 → 4
means changing these two lines.

Behaviour that is DBAL 3's own:

- **Nested transactions use savepoints only if you ask:** call
  `$conn->setNestTransactionsWithSavepoints(true)`. DBAL 3 defaults it to off for every driver, so a
  nested `beginTransaction()` emits no `SAVEPOINT` and an inner `rollBack()` only marks the whole
  transaction rollback-only. DBAL 4 always nests with savepoints.
- **`lastInsertId()` throws when there is no key; it never returns `false`.** DBAL 3's SPI docblock
  allows `false`, but Doctrine ORM 2's `IdentityGenerator` casts it with `(int)`, so `false` would
  become the primary key `0`. A sequence-name argument is accepted and not used on any family (on
  PostgreSQL a follow-up `currval()` outside a transaction would run on another pooled connection;
  inside one it would be correct, and is deferred to the ORM work).
- **BEGIN/COMMIT/ROLLBACK failures arrive as the same Doctrine exception classes as on DBAL 4** — a
  `40001` at COMMIT is a `DeadlockException` (`RetryableException`), an unconfirmed COMMIT an
  `IndeterminateWriteException`. DBAL 3's own wrapper never converts these, so the driver does.
- `bindParam()` (by reference, read at `execute()`) and `execute($params)` (PDO-style, every value
  bound as `STRING`, replacing earlier bindings) work, deprecated upstream as they are.
- `quote($value, $type)` accepts the scalar DBAL 3's wrapper hands it and quotes it as a string
  literal — except `BINARY`/`LARGE_OBJECT`, which it **refuses**: quoting those as text would store
  different bytes on PostgreSQL. Bind them instead.

Tested against the locked 3.10.x by the package's full live suite (the same tests DBAL 4 runs, with
the MySQL-family ones in CI) and against the `^3.8` floor, 3.8.0, by static analysis and the DBAL 3
unit tests — see Development.

### Platform selection

The driver learns the pool's **kind** from the engine handshake and the **version string** from the
handshake's pool metadata, falling back to a single `SELECT version()` if the engine has not resolved
one yet. If the version is still unknown when the platform is needed, it throws
`Ferro\DBAL\Exception\ServerVersionUnavailable` naming the pool — **never a default platform**,
because a wrong platform is a silently wrong SQL dialect rather than a clean error. On PostgreSQL you
can set DBAL's own `'serverVersion'` parameter to skip the round trip entirely, written as the banner
(`'PostgreSQL 17.10'`): that parameter short-circuits the handshake, so it must name the family
itself, and a bare MySQL or SQLite version cannot.

The version string is normalised for **PostgreSQL only**. On the MySQL family it is passed through
verbatim, deliberately: MariaDB is detected by the substring `MariaDB` in the version, so
`'11.8.8-MariaDB-ubu2404'` selects `MariaDB110700Platform` while a "helpfully" normalised `'11.8.8'`
would select `MySQL84Platform` — a different dialect.

`getServerVersion()` answers the same normalised string (since M2-C5b): on PostgreSQL
`'17.10 (Debian …) on x86_64-…'`, without the leading product name, which is the shape `pdo_pgsql`
reports. The raw banner `'PostgreSQL 17.10 (…)'` is something `version_compare()` reads as OLDER than
every version, so a `version_compare($conn->getServerVersion(), '12.0', '<')` gate — upstream
doctrine/dbal's own suite has one — answered wrong.

### Read-only connections

Neither DBAL SPI carries a read/write signal: `executeQuery('INSERT … RETURNING id')` is
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

`iterateAssociative()` and its siblings stream row-by-row on **every family**, for parameterless and
prepared statements alike; `exec()` alone stays buffered, since its contract is the affected count.
Interleaving a statement into an open iteration works (the remainder is drained first); abandoning
the canonical `foreach ($conn->iterateAssociative($sql) as $row) { … break; }` cancels the stream. A
**bound** iterator does not — `unset()` it or iterate the call directly. See the
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

The **DBAL 3 lane** is the same package resolved against `doctrine/dbal` 3.x into a second vendor
tree, so both majors are installed side by side:

```bash
COMPOSER=composer.dbal3.json composer install          # → vendor-dbal3/
./vendor-dbal3/bin/phpunit -c phpunit.dbal3.xml        # tests/Dbal3 + the SHARED tests/Live
COMPOSER=composer.dbal3.json ./vendor-dbal3/bin/phpstan analyse -c phpstan.dbal3.neon
```

`COMPOSER=` is load-bearing for PHPStan: it builds its class locator from the composer file in the
working directory, so without it the DBAL 3 analysis silently reflects DBAL 4's types. CI also
downgrades the lane to 3.8.0 and re-runs the analysis and `tests/Dbal3` there.

The upstream Doctrine DBAL functional subset runs through `testkit/dbal-suite.sh`; the recorded
numbers and their triage are in [`docs/dbal-suite/2026-09-09-a5-results.md`](../../docs/dbal-suite/2026-09-09-a5-results.md)
(and the first recording, before M1-S9's fixes, in [`2026-08-11-results.md`](../../docs/dbal-suite/2026-08-11-results.md)).

## Known gaps

- **`lastInsertId()` throws on PostgreSQL** by design; use `INSERT … RETURNING id`, and the ORM's
  SEQUENCE identity strategy on PostgreSQL.
- A **cancelled or timed-out `SELECT`** is reported as an indeterminate write unless the connection
  is declared `readonly` (see "Read-only connections").
- Through Doctrine's **`transactional()`**, an indeterminate COMMIT reaches you as
  `ConnectionException: There is no active transaction.` with the `IndeterminateWriteException`
  chained beneath it — an upstream DBAL behaviour no driver can reach, on both majors.

This list used to carry three more items — the PostgreSQL schema manager, `bigint` values at or above
2^32, and an unbounded first dial against a down backend. All three were fixed in M1 and are recorded
as such, with their measurements, on the known-incompatibilities page, which is the authoritative
list.
