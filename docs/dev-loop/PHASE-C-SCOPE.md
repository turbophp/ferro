# Phase C (M2 — the Eloquent milestone): slice plan

**Status: SCOPE ONLY. No Phase C code has been written.** This document exists so C1 is sliced
before it is started, the way B2 was — that split (fork edit → reclaim seam → bound → parkable conn
→ stream → driver flip) is why six risky changes landed without a rollback, each proven in CI before
the next built on it.

Every claim below is marked **[verified]** (checked against the code this iteration) or
**[UNVERIFIED]** (a premise the owning slice must check FIRST). Phase B produced four items whose
stated premise did not survive contact; the marking is there so that cost is not paid again.

## The shape of the milestone

§15's mechanism is the same shape as §14's and the same charter rule 6 constraint applies: register
`ferro-{mysql,pgsql,…}` via `Illuminate\Database\Connection::resolverFor()`, subclass the matching
Illuminate connection class, and override **the execution layer only** — stock Grammar, stock
Processor, stock Schema builder.

**S8a/S8b is the precedent to copy, including its split.** S8b could not start until S8a had built
the seven engine/client carries it needed. The same question decides C1's first slice: *what does
the Illuminate tier need that `ferro/client` does not have yet?*

## Carries: what the Eloquent tier needs from below

| Need (§15) | Status | Notes |
|---|---|---|
| `select()`, `statement()`, `affectingStatement()`, `unprepared()` | **[verified]** present | `fetchRaw` / `fetch:none` + `affected`, all built for S8b. |
| `cursor()` → `LazyCollection` | **[verified]** present | `streamRaw()` streams on BOTH families since Phase B item B2; the pool-kind gate is gone. This was the largest C1 dependency and it is already paid. |
| `beginTransaction`/`commit`/`rollBack` + savepoints | **[verified]** present | Imperative trio (S8a) + savepoint SQL passthrough (§22.2 (r)). |
| `lastInsertId` | **[verified]** present | On the wire since S8a; on stream terminals since B2c. |
| `getAttribute(SERVER_VERSION)` for the PDO shim | **[verified]** present | `poolInfo()` carries `server_version` (S8a, `HELLO_ACK` v2). |
| **`selectResultSets()` — MULTIPLE result sets** | **GAP [verified], but NOT the binding constraint** | `ExecOk` carries exactly ONE `cols` + ONE `rows`, so several result sets have no wire representation; §15 lists the method without noting this. **The deeper blocker, also [verified]:** a MySQL `CALL` returns no usable rows today — a prepared `CALL` declares zero result columns even when the procedure emits a result set, so the streamed path discards the rows and the buffered path yields N cell-less rows. Fixing the wire without fixing that would ship a feature that still returns nothing. See C1a. |
| `DB::transaction($fn, attempts: 3)` retry mapping | **[verified] — works, but ONLY if the tier's exception follows PDO's code convention, NOT DBAL's** | See "The `attempts:` requirement" below. Getting it backwards silently disables retry for PostgreSQL serialization failures. |
| `read`/`write` split → a second pool | **[verified] — no engine work needed** | `PoolSpec` (name, dsn, kind, pin_functions, pin_on_unknown) has NO replica or read-role concept; pools are just named DSNs, and `read-replica` appears in the tree only as an example pool NAME in a config test. That is GOOD news: Laravel's `'read' => ['pool' => 'main_ro']` is satisfied entirely client-side by selecting a different pool name per query. **The work is inheriting Illuminate's stickiness rules, not building replication:** the base `Connection` already decides read-vs-write per query (a write makes subsequent reads sticky; reads inside a transaction go to the write connection) and expresses that by picking `getPdo()` vs `getReadPdo()`. Our execution layer does not use PDO, so the subclass must read WHICH role the base class selected and map it to a pool name — inheriting the semantics rather than re-deriving them. |
| `FerroPdoShim` (`quote`, `lastInsertId`, `inTransaction`, `exec`, `getAttribute`) | **[decided] — do NOT implement `quote()` speculatively** | `lastInsertId`/`inTransaction`/`exec`/`getAttribute` are all backed by things that already exist. `quote()` is different in kind: implementing it means owning dialect-specific SQL string escaping, which is security-critical code written for no known caller. §15 lists it because ecosystem packages touch it, but WHICH packages and HOW is unknown. The slice that finds a real caller decides; until then it refuses, with a message naming the alternative (parameter binding). This follows the house stance set at S7 — *we refuse what PDO corrupts* — and charter rule 6, and it is reversible in the safe direction: a refusal can become an implementation, an unnoticed escaping bug cannot be un-shipped. |

## The `attempts:` requirement (verified against the INSTALLED `illuminate/database` v11.51.0)

**Container feasibility, checked first:** `illuminate/database ^11.0` installs cleanly here
(v11.51.0, via `env -u GITHUB_TOKEN COMPOSER_AUTH='{}' composer install`), so C1b can be built and
tested locally rather than only in CI. The three structural facts below were read out of that
installed tree, not fetched from a branch — `Connection::resolverFor($driver, Closure)` exists at
`Connection.php:1683` with a `static::$resolvers` map, `QueryException extends PDOException`
(`QueryException.php:9`), and it does `$this->code = $previous->getCode()` (line 48).

`DB::transaction($fn, attempts: 3)` retries iff `causedByConcurrencyError($e)` is true. That helper
matches on exactly two things:

1. `$e instanceof PDOException` **and** `($e->getCode() === 40001 || $e->getCode() === '40001')` —
   verbatim from the installed source, so EITHER the int or the string form satisfies it; or
2. the exception MESSAGE containing one of a fixed list of substrings — among them
   `"Deadlock found when trying to get lock"`, `"deadlock detected"` and
   `"Lock wait timeout exceeded; try restarting transaction"`.

Two consequences, and the second is the one that would have shipped silently.

**Criterion 2 already works, by faithfulness rather than by design.** Ferro preserves the raw server
message verbatim on every fate arm, so a MySQL deadlock, a PG `deadlock detected` (PostgreSQL's own
deadlock wording, and it IS in the list) and a MySQL 1205 lock-wait timeout all match those
substrings as-is. Nothing is needed for those — **which is exactly what makes the gap below easy to
miss**: the common cases pass without anyone implementing anything.

**Criterion 1 requires the tier's exception to put SQLSTATE in `getCode()` — the OPPOSITE of what the
DBAL driver does.** `Illuminate\Database\QueryException extends PDOException` and its constructor
does `$this->code = $previous->getCode()`, i.e. it inherits the code from OUR driver exception. PDO's
convention is that `getCode()` IS the SQLSTATE; DBAL's convention (and `Ferro\DBAL\Exception\DriverException`'s)
is that `getCode()` is the vendor ERRNO, with SQLSTATE in `getSQLState()`. **If the Eloquent tier
copies the DBAL tier's convention, criterion 1 never fires**, and a PostgreSQL serialization failure —
SQLSTATE `40001`, whose message `could not serialize access due to concurrent update` matches NONE of
the substrings in criterion 2 — is **never retried**, even though Ferro classified it `Retryable`
correctly. `attempts: 3` would appear to work (deadlocks retry via criterion 2) while silently not
working for the one case SERIALIZABLE workloads depend on.

So: **the Eloquent tier's driver exception MUST follow PDO's convention, and a live guard must assert
that a PG serialization failure actually re-runs the closure** — not merely that it classifies
Retryable. This is the same shape as §22.2 (ac)'s lesson: a correct classification is worthless if
the tier above cannot read it.

## Proposed slices

- **C1a — the `selectResultSets()` wire gap. DECIDED while scoping: documented incompatibility for
  M2, because the wire is not its real blocker.** The primary consumer of `selectResultSets()` is a
  stored procedure, and **a MySQL `CALL` cannot return usable rows today at all** — a prepared `CALL`
  reports ZERO result columns even when the procedure emits a result set at run time. Verified in
  code (`ferro-backend-mysql/src/stream.rs`), and the two paths round the same blind spot off
  differently: the STREAMED path takes the no-rows arm and discards the rows, while the BUFFERED path
  maps every row through the empty prepared-column list and yields **N rows with no cells**. So
  carrying multiple result sets on the wire would be building on sand: the feature would still return
  nothing usable on the backend that motivates it.
  **The dependency order is therefore CALL-blind-spot FIRST, multi-result-set SECOND**, and both are
  engine work, not tier work. Recorded so the tier is not blocked on a `/proto` slice that would not
  have helped. The blind-spot fix is its own investigation (likely: read column metadata from the
  EXECUTE response rather than the PREPARE response, or route `CALL` over the text protocol) and is
  not scheduled here.
- **C1b — package skeleton + service provider + one connection class, `select()` only.** The
  smallest thing that can execute a real query through a real Illuminate connection. Its exit gate
  is the S8b lesson: a HARD CONTACT ASSERTION (`getNativeConnection() instanceof …` + a round-tripped
  `SELECT 1`) before a single suite test runs. Upstream's `TestUtil` silently fell back to SQLite
  and reported a green 105-test run with zero Ferro contact; that must not be re-learned.
- **C1c — writes + transactions. DONE.** `statement()`/`affectingStatement()`/`unprepared()` over
  `fetch:none`; transactions over a minimal `FerroPdoShim`. The exit gate landed as specified and the
  requirement was **mutation-proven live**: swapping `FerroQueryException` to the sibling Doctrine
  tier's errno convention makes a real PG `40001` propagate OUT of `transaction(attempts: 3)` instead
  of retrying — `attempts:` silently inert, exactly as predicted.
  **One design change fell out of building it, and it moves a later slice earlier:** the PDO shim is
  NOT merely a compatibility layer for ecosystem packages, as §15 frames it. `ManagesTransactions` —
  which owns the transaction counter, savepoint naming through the stock grammar, the connection
  events and the `attempts:` retry loop — is written entirely against
  `getPdo()->beginTransaction()/commit()/rollBack()/inTransaction()/exec()`. Supplying those five
  methods inherits all of that unchanged; the alternative was copying the trait's body and keeping it
  in step with Laravel forever. It is possible because `Connection::getPdo()` has NO return type, so
  the shim is duck-typed and need not extend `\PDO`. **C1e is therefore already half-built**, and what
  remains of it is the question of which further PDO methods any real package needs — with `quote()`
  still refused (see the table above).
- **C1d — `cursor()`/`LazyCollection`. DONE**, and it was small as predicted: `streamRaw()` already
  streamed on both families since B2, so this was tier wiring rather than engine work. `cursor()` is
  itself a Generator (stock Illuminate's is too), so the body stays lazy; a `finally` calls
  `RawStream::close()` for the `CANCEL`+drain on abandonment.
  **The abandonment guard asserts the NEXT query, not the abandoned one** — a missing cancel does not
  damage the query you stopped, it damages the one after. Mutation-proven, and the failure mode is
  worse than a wrong answer: deleting the `finally` HANGS the session (the next request waits behind
  ~50 000 unread frames) rather than returning wrong data. That is the shape of bug that reads as CI
  infrastructure flakiness and gets re-run instead of fixed — worth knowing before it happens in
  anger.
- **C1e — the PDO shim**, scoped by what C1b–C1d actually turn out to need, not by §15's list
  up front.
- **C2 — the `illuminate/database` suite.** Modelled on `testkit/dbal-suite.sh`, but **materially
  heavier than that sibling**, for reasons measured against `laravel/framework` v11.x and
  `orchestra/testbench-core` v9.0.0 source (both cloned and read; no guessing). See
  "What C2 actually requires" below. It is **at least two slices**, and the runner is the first.

## What C2 actually requires (measured against upstream source, not assumed)

**1. The full framework, not a bare Capsule — this is the size driver.** Every test in
`tests/Integration/Database/` extends `Illuminate\Tests\Integration\Database\DatabaseTestCase`,
which extends `Orchestra\Testbench\TestCase`, which boots a real `Illuminate\Foundation\Application`
and console `Kernel`. `DatabaseTestCase` also uses `DatabaseMigrations`, whose `refreshTestDatabase()`
runs `$this->artisan('migrate:fresh', …)`. So the suite needs **`orchestra/testbench-core` as a new
dependency**, a booted application, and a working Artisan console — none of which the DBAL suite
needed. That is the honest reason C2 is bigger than its sibling.

**2. The silent-SQLite trap is REAL here too, and its exact shape matters.** In
`Orchestra\Testbench\Bootstrap\LoadConfiguration::bootstrap()`:

```php
if (\is_null($config->get('database.connections.testing'))) {
    $config->set('database.connections.testing', [
        'driver' => 'sqlite', 'database' => ':memory:', …
    ]);
}
if ($config->get('database.default') === 'sqlite' && ! file_exists(…)) {
    $config->set('database.default', 'testing');
}
```

Upstream's `phpunit.xml.dist` sets `DB_CONNECTION=testing`, and the skeleton `config/database.php`
defines no `testing` connection — so testbench **silently injects SQLite in-memory, unconditionally,
with no warning**. Exactly the failure that made the DBAL suite report `OK (105 tests)` against
SQLite with zero engine contact.

**But it is avoidable BY CONSTRUCTION, and that is worth knowing precisely:** the injection keys on
the connection NAME `testing` (and on `sqlite` with a missing file). A custom name that is not
configured does **not** hit this path — `DatabaseManager::configuration()` throws
`InvalidArgumentException: Database connection [x] not configured.` instead, which is loud. **So the
rule for C2's harness is: never reuse the name `testing`.** The contact assertion stays regardless —
belt and braces, and per FB-7 it must be an unguessable probe, not a constant.

**3. Testbench will not wire a custom driver's config for us.** Its
`SyncDatabaseEnvironmentVariables` bootstrapper maps only `MYSQL_*`, `MARIADB_*`, `POSTGRES_*` and
`MSSQL_*` onto `database.connections.{driver}.*`. A `ferro-pgsql` connection gets no env syncing, so
the harness must supply the connection config directly.

**4. `migrate:fresh` — VERIFIED, and it produced C1e's first non-speculative requirement.**
`dropAllTables()` (what `migrate:fresh` runs) works through the tier, including across a foreign
key, so **C2 is not blocked**. But `hasColumn()`/`getColumnListing()` were: stock
`PostgresGrammar::compileColumns()` does
`version_compare($this->connection?->getServerVersion(), '12.0', '<')` to decide whether its
introspection SQL selects `a.attgenerated`, and `Connection::getServerVersion()` is
`getPdo()->getAttribute(PDO::ATTR_SERVER_VERSION)` — which the shim refused.

**This is the C2-before-C1e reordering paying off exactly as argued.** C1e was deferred because
nothing had demanded a PDO method and building to §15's list would have been speculative; the
measurement then named the one method real framework code needs, and it is backed by `poolInfo()`'s
`server_version`, already on the wire since S8a. A missing version is LOUD rather than defaulted —
`version_compare(null, '12.0', '<')` is true, so a silent default would emit pre-12 introspection SQL
against a modern PostgreSQL. Every other attribute still refuses by name; the roster grows only as
real code is measured needing it.

**5. Scale and isolation.** 131 `.php` files under `tests/Integration/Database/` (~656 `test*`
methods). There is **one flat testsuite** in the root `phpunit.xml.dist` covering `./tests`; upstream
isolates the database tests by pointing phpunit at the directory and overriding env vars
(`.github/workflows/databases.yml` runs `vendor/bin/phpunit tests/Integration/Database` with
`DB_CONNECTION: pgsql`). So C2 can select by path + a generated config, the same way the DBAL runner
does with its allowlist.

## The bar, stated honestly up front

§15's acceptance is "the `illuminate/database` integration test suite green on MySQL, PG, SQLite via
Ferro connections", plus a demo app. **SQLite has no backend (C3), so the SQLite column is
unreachable until C3 lands** — exactly the situation §14's bar was in, and the honest recording rule
from the S8b close applies: report the columns that ran, name the one that could not, and do not
restate the bar as if it were met. Whether C3 should therefore come BEFORE C2 is an open sequencing
question, not a settled one.

---

## What the first C2 run actually measured (2026-09-10)

Numbers and full triage: `docs/laravel-suite/2026-09-10-c2-results.md`. Three things that change how
the rest of Phase C should be planned:

**1. The runner is NOT CI-only.** The scope note above recorded "Docker's daemon is not running here,
so the runner is CI-only exactly like its sibling". That held for the container-side reset and not
for anything else: `FERRO_LARAVEL_SVC=psql` runs the *identical* `reset-pg.sql` through a local
`psql`, so a box with PostgreSQL and no Docker daemon produces recordable numbers. The reset is what
makes a number reproducible; where the client binary lives is not. Every subsequent measurement slice
gets a same-session feedback loop it was assumed not to have.

**2. The `driver` NAME is a measurement variable, so C2 records two columns.** Illuminate resolves by
driver name, and six of upstream's own `QueryBuilderTest` cases assert nothing at all unless
`$this->driver` is `pgsql` or `sqlsrv`. `FerroConnections::register()` now takes an opt-in alias, and
the suite runs under both names. This is the same class of problem as the DBAL suite's `TestUtil`
(which honoured only `driver`) — but the failure mode is the opposite and gentler: there, the wrong
name meant a silently green run against SQLite; here it means silently *skipped* assertions. Both are
"the harness decided something the numbers then hide".

**3. The premise that most needed checking was our own tests' coverage, not upstream's shape.** Every
structural premise about testbench held. What did not hold was the implicit assumption that
`php/laravel`'s own passing suite meant the tier worked: the framework's suite found two defects in
its first two runs, and both were invisible to the tier's tests *by construction* — the binding one
because unit tests bind scalars and Eloquent binds `Carbon`, the decode one because the tier's live
tests asserted on columns whose canonical tag is a plain scalar. **Carry this into C1e and C3: a
tier's own green suite is evidence about the tier's tests, not about the tier.**
