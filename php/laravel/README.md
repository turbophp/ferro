# ferro/laravel

Ferro connections for Laravel / Illuminate — the §15 Eloquent tier.

> **Status (M2):** PostgreSQL (`ferro-pgsql`), SQLite (`ferro-sqlite`), MySQL (`ferro-mysql`) and
> MariaDB (`ferro-mariadb`, Laravel 11's own MariaDB family) are registered. Reads, writes, transactions (including
> `DB::transaction(attempts: …)`), streaming `cursor()` / `LazyCollection`, reconnection and the
> PDO surface Illuminate itself touches all run through the engine. Each family is measured with
> upstream `laravel/framework`'s own integration suite, against a control column that runs the same
> tests through the stock PDO driver (`docs/laravel-suite/`). What does not behave like PDO is listed
> in `docs/known-incompatibilities.md`.

## Configuration

```php
'connections' => [
    'mysql' => [
        'driver'       => 'ferro-mysql',        // was 'mysql'   (also: ferro-mariadb, ferro-pgsql, ferro-sqlite)
        'ferro_socket' => '/run/ferro/app.sock',
        'pool'         => 'main',
        'database'     => 'app',                // REQUIRED by Illuminate — see below
        // host/username/password/unix_socket may stay; they are IGNORED. Credentials live in ferrod (§12).
    ],
],
```

That is the whole change in a Laravel application: the package ships `Ferro\Laravel\FerroServiceProvider`
under `extra.laravel.providers`, so package discovery registers the `ferro-*` drivers. An
application that disables discovery, or a plain Illuminate application without the framework,
registers them once itself:

```php
Ferro\Laravel\FerroConnections::register();
```

`ferro_host` / `ferro_port` select the TCP fallback when there is no socket. `host` and
`unix_socket` are deliberately **not** read: they describe the upstream database, which the
application no longer dials.

**`database` is a label on PostgreSQL and SQLite and a SELECTOR on MySQL / MariaDB.** On the first
two the pool's DSN chooses the database and the key only names it for Laravel. On the MySQL family
`MySqlBuilder` binds it into every `information_schema` query, so it must be the pool's own
database; the connection checks that the first time the schema builder is used and refuses a
mismatch rather than introspecting the wrong schema.

**Laravel's MySQL session keys are ignored** — `strict`, `modes`, `isolation_level`, `timezone`,
`charset`, `collation`. Stock Laravel issues them on connect; here the pool owns (and resets) the
session, so the server's own defaults apply, and every session runs at `time_zone = '+00:00'`. Put
them in the server's configuration. Details and measurements: `docs/known-incompatibilities.md`.

`FerroConnections::register(['mysql' => 'ferro-mysql'])` also registers Ferro under the stock driver
NAME. That is opt-in, because the alias replaces every connection in the application whose `driver`
is that name — but some code branches on the name (upstream's own tests do, through
`#[RequiresDatabase('mysql')]` and `$this->driver`), and only the alias makes such code take the
same branch it takes on PDO.

## Outbound HTTP: the `Http` facade (Ferro HTTP, SPEC §23.11.6)

With `ferro/guzzle` installed, an `http.ferro` block routes Laravel's `Http` facade through Ferro HTTP
— the per-host engine's pooled, credential-holding HTTP transport — with nothing else changed:

```php
// config/http.php
return ['ferro' => [
    'socket'    => '/run/ferro/app.sock',
    'upstreams' => ['https://api.openai.com' => 'openai'],   // origin => the upstream ferrod declares
]];
```

The provider rebinds the `Illuminate\Http\Client\Factory` singleton to
`Ferro\Laravel\Http\FerroHttpFactory`, which hands every pending request `Ferro\Guzzle\FerroHandler`.
Laravel's own stub, recorder and before-sending handlers stay ABOVE it, so `Http::fake()`,
`Http::assertSent()` and the request events behave as stock, and a faked request never reaches the
engine. `Http::pool()` goes through Ferro too.

**Retries — the recommended recipe:**

```php
Http::retry(3, 100, when: Ferro\Laravel\Http\Retry::when())->post($url, $body);
```

It retries only what is safe to send again and never a request whose fate is unknown. A bare
`Http::retry(3)` is Laravel's own policy and re-sends a POST whose connection died after sending,
exactly as it does under curl; retrying is the client's policy, so Ferro leaves that choice to you.

`http.ferro` makes Ferro the facade's DEFAULT transport; it is not an egress control. A request-level
handler (`withOptions(['handler' => …])`, `globalOptions`, `setHandler`, `setClient`) routes that
request around Ferro and its SSRF confinement. Differences from curl:
`docs/known-incompatibilities.md` (*Ferro HTTP*).

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
when it generated none, so the MySQL / MariaDB connection does that — `insertGetId()` and
`DB::getPdo()->lastInsertId()` alike. PostgreSQL uses `insert … returning`.

**On MySQL a `TIMESTAMP` reads back as `pdo_mysql` returns it, to the second**: the naive UTC wall
clock (`2017-11-12 13:14:15`) — every Ferro MySQL session is pinned to `+00:00` — rather than the
canonical RFC3339 form the PostgreSQL tier keeps for `timestamptz`. Eloquent writes naive strings,
so this is what keeps a write → read round trip stable (SPEC §22.2 (cb)). A fractional column
renders the canonical fraction (none when zero, otherwise six digits), not the column's precision as
`pdo_mysql` does. A `TIMESTAMP` written through a non-UTC session before adoption reads back in UTC.

**Foreign-key checks on MySQL / MariaDB are pinned to one connection.** `SET FOREIGN_KEY_CHECKS=0` is
session state and every statement is its own checkout, so the stock sequence would turn checks off
on a connection the next statement never reaches. `Schema::withoutForeignKeyConstraints()` and
`dropAllTables()` (`migrate:fresh`, `db:wipe`, `RefreshDatabase`) run inside one transaction instead,
and a bare `Schema::disableForeignKeyConstraints()` outside a transaction is refused — use
`withoutForeignKeyConstraints()` or wrap the block in `DB::transaction()`.

**`selectResultSets()` is not supported.** The wire carries one result set per statement, and no
tier can reach a second one today. (The MySQL `CALL` defect that used to sit underneath it is fixed:
§22.2 (av), (aw).)

**`DB::escape()`, `toRawSql()`, `castAsJson()` and `DB::pretend()` with string bindings work on every
family — but the literal is not always PDO's bytes, on purpose.** A PDO driver escapes by the live
escape mode of its connection; this tier has no connection to read, and the pool-level mode the
engine advertises can describe a different session (SPEC §22.2 (cc)). So it uses forms that mean the
same bytes in every mode: a string without a backslash is quoted by doubling `'` (on MySQL that is
`'O''Brien'` where `pdo_mysql` writes `'O\'Brien'` — the same string to the server), and a string
with a backslash becomes `E'…'` on PostgreSQL and `_utf8mb4 X'<hex>'` on MySQL / MariaDB.

**`DB::getPdo()->query()` and `prepare()` are refused** — there is no PDO underneath. Use
`DB::select()` / `DB::statement()`.
