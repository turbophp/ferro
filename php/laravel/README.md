# ferro/laravel

Ferro connections for Laravel / Illuminate — the §15 Eloquent tier.

> **Status (M2):** PostgreSQL (`ferro-pgsql`), SQLite (`ferro-sqlite`) and MySQL / MariaDB
> (`ferro-mysql`, since C1f) are registered. Reads, writes, transactions (including
> `DB::transaction(attempts: …)`), streaming `cursor()` / `LazyCollection`, reconnection and the
> PDO surface Illuminate itself touches all run through the engine. Each family is measured with
> upstream `laravel/framework`'s own integration suite, against a control column that runs the same
> tests through the stock PDO driver (`docs/laravel-suite/`). What does not behave like PDO is listed
> in `docs/known-incompatibilities.md`.

## Configuration

```php
'connections' => [
    'mysql' => [
        'driver'       => 'ferro-mysql',        // was 'mysql'   (also: ferro-pgsql, ferro-sqlite)
        'ferro_socket' => '/run/ferro/app.sock',
        'pool'         => 'main',
        'database'     => 'app',                // REQUIRED by Illuminate; a LABEL here, not a selector
        // host/username/password may stay; they are IGNORED. Credentials live in ferrod (§12).
    ],
],
```

Register the resolvers once, from a service provider's `register()` or an application bootstrap:

```php
Ferro\Laravel\FerroConnections::register();
```

`ferro_host` / `ferro_port` select the TCP fallback when there is no socket. `host` is deliberately
**not** read: it describes the upstream database, which the application no longer dials.

`FerroConnections::register(['mysql' => 'ferro-mysql'])` also registers Ferro under the stock driver
NAME. That is opt-in, because the alias replaces every connection in the application whose `driver`
is that name — but some code branches on the name (upstream's own tests do, through
`#[RequiresDatabase('mysql')]` and `$this->driver`), and only the alias makes such code take the
same branch it takes on PDO.

## What this tier changes, and what it does not

Only the **execution layer**. The stock Grammar, Processor and Schema builder are inherited
untouched — they build SQL strings and post-process arrays, and charter rule 6 says the drop-in
tiers change execution, never SQL generation. Each family's connection extends the stock one
(`PostgresConnection`, `SQLiteConnection`, `MySqlConnection`) and shares one execution trait.

## Things worth knowing

**Every statement is fate-declared a write.** `select()` looks like a read and usually is, but it
genuinely carries writes: `PostgresProcessor::processInsertGetId` runs `insert … returning id`
through `selectFromWriteConnection()`. Declaring reads would mis-declare the fate of the commonest
Eloquent write on PostgreSQL. The cost is the §22.2 (ac) trade: a plain `SELECT` killed by a
server-side `statement_timeout` surfaces as an indeterminate write. Same trade the Doctrine tier
makes, for the same reason.

**`lastInsertId()` follows each family's PDO driver.** `pdo_sqlite` keeps the last key on the
handle, so the SQLite connection does too; `pdo_mysql` answers the statement just executed, `"0"`
when it generated none, so the MySQL connection does that. PostgreSQL uses `insert … returning`.

**On MySQL a `TIMESTAMP` reads back as `pdo_mysql` returns it**: the naive UTC wall clock
(`2017-11-12 13:14:15`) — every Ferro MySQL session is pinned to `+00:00` — rather than the
canonical RFC3339 form the PostgreSQL tier keeps for `timestamptz`. Eloquent writes naive strings,
so this is what makes a write → read round trip byte-stable (SPEC §22.2 (cb)).

**`selectResultSets()` is not supported.** The wire carries one result set per statement, and no
tier can reach a second one today. (The MySQL `CALL` defect that used to sit underneath it is fixed:
§22.2 (av), (aw).)

**`DB::escape()` / `toRawSql()` need a pool that advertises `literals_are_standard`.** PostgreSQL
pools do; SQLite and MySQL pools do not yet, and the shim refuses rather than guesses the escaping
rule (§21 D5, §22.2 (at)).
