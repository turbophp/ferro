# ferro/laravel

Ferro connections for Laravel / Illuminate — the §15 Eloquent tier.

> **Status: C1b, the first slice. `select()` only.** Writes, transactions, `cursor()` and the PDO
> shim are later slices (see `docs/dev-loop/PHASE-C-SCOPE.md`). Every other execution method still
> falls through to Illuminate's stock PDO path, which has no PDO here and fails loudly rather than
> appearing to work.

## Configuration

```php
'connections' => [
    'pgsql' => [
        'driver'       => 'ferro-pgsql',        // was 'pgsql'
        'ferro_socket' => '/run/ferro/app.sock',
        'pool'         => 'main',
        // host/username/password may stay; they are IGNORED. Credentials live in ferrod (§12).
    ],
],
```

Register the resolvers once, from a service provider's `register()` or an application bootstrap:

```php
Ferro\Laravel\FerroConnections::register();
```

`ferro_host` / `ferro_port` select the TCP fallback when there is no socket. Note `host` is
deliberately **not** read: it describes the upstream database, which the application no longer
dials.

## What this tier changes, and what it does not

Only the **execution layer**. The stock Grammar, Processor and Schema builder are inherited
untouched — they build SQL strings and post-process arrays, and charter rule 6 says the drop-in
tiers change execution, never SQL generation.

## Two things worth knowing

**Every statement is fate-declared a write.** `select()` looks like a read and usually is, but it
genuinely carries writes: `PostgresProcessor::processInsertGetId` runs `insert … returning id`
through `selectFromWriteConnection()`, which is `select($query, $bindings, false)`. Declaring reads
would mis-declare the fate of the commonest Eloquent write on PostgreSQL. The cost is the §22.2 (ac)
trade: a plain `SELECT` killed by a server-side `statement_timeout` surfaces as an indeterminate
write. Same trade the Doctrine tier makes, for the same reason.

**`selectResultSets()` is not supported**, and the wire is not why. A MySQL `CALL` returns no usable
rows today — a prepared `CALL` declares zero result columns even when the procedure emits a result
set — so multi-result-set support would ship a feature that still returns nothing. See
`docs/dev-loop/PHASE-C-SCOPE.md` C1a.
