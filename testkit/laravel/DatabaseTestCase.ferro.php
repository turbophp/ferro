<?php // testkit/laravel/DatabaseTestCase.ferro.php — copied OVER the upstream base class.
//
// Upstream's `tests/Integration/Database/DatabaseTestCase.php` verbatim, plus the Ferro wiring. It
// is patched rather than subclassed because every test in that tree extends THIS class by name, and
// the DBAL suite established the pattern: copy over, then VERIFY the patch applied — a silently
// failed patch is exactly how a suite goes green against the wrong engine.

namespace Illuminate\Tests\Integration\Database;

use Ferro\Laravel\FerroConnections;
use Illuminate\Foundation\Testing\DatabaseMigrations;
use Orchestra\Testbench\TestCase;

abstract class DatabaseTestCase extends TestCase
{
    use DatabaseMigrations;

    /**
     * The current database driver.
     *
     * @return string
     */
    protected $driver;

    protected function setUp(): void
    {
        $this->beforeApplicationDestroyed(function () {
            foreach (array_keys($this->app['db']->getConnections()) as $name) {
                $this->app['db']->purge($name);
            }
        });

        parent::setUp();
    }

    protected function defineEnvironment($app)
    {
        // ---------------------------------------------------------------------------------------
        // FERRO WIRING. Everything below the marker is upstream's body, unchanged.
        //
        // THE CONNECTION IS NEVER NAMED `testing`, and that is not cosmetic. Testbench's
        // `LoadConfiguration::bootstrap()` injects `database.connections.testing` as sqlite
        // `:memory:` UNCONDITIONALLY when it is not configured, and upstream's phpunit.xml.dist sets
        // DB_CONNECTION=testing — which is precisely how the sibling Doctrine suite once reported
        // `OK (105 tests, 211 assertions)` against SQLite with zero engine contact. An unconfigured
        // CUSTOM name throws `Database connection [x] not configured` instead, loudly. Using a name
        // testbench has no opinion about is what makes the trap unreachable by construction.
        // ---------------------------------------------------------------------------------------
        // FERRO_LARAVEL_DRIVER selects WHICH configuration is being measured, and both are real
        // product configurations, not harness knobs:
        //   ferro-pgsql (default) — the one-word-config-change adoption of §15.
        //   pgsql                 — the same engine registered under the stock driver NAME, via
        //                           `FerroConnections::register()`'s opt-in alias.
        //   stock-pgsql           — THE CONTROL: upstream's own `pdo_pgsql`, no Ferro anywhere.
        // The name is observable to the tests themselves: six of upstream's own QueryBuilderTest
        // cases gate their assertion on `in_array($this->driver, ['pgsql', 'sqlsrv'])` and assert
        // nothing at all under any other name. Recording both columns is what keeps that visible
        // instead of letting it hide inside a single number.
        //
        // **THE CONTROL COLUMN IS WHAT MAKES A NON-PASS TRIAGEABLE AT ALL**, and it earned its
        // place before it was built: two `SchemaBuilderSchemaNameTest` cases looked like a Ferro
        // schema gap, and running the identical test through stock `pdo_pgsql` against the SAME
        // PostgreSQL produced the IDENTICAL result line (28 tests, 146 assertions, 2 errors, 2
        // skipped) — so they are upstream's own on this framework/server pair, not ours. At 633
        // tests that question comes up constantly, and answering it by hand each time is how a
        // triage silently degrades into a guess.
        //
        // It is deliberately NOT the default and cannot be reached by accident: the value is
        // explicit, and `bootstrap.php`'s contact assertion INVERTS for it — under `stock-pgsql` it
        // REFUSES to run if the connection turns out to be a Ferro one. A control that quietly ran
        // through Ferro, or a Ferro column that quietly ran through PDO, would each be worse than
        // no control at all. Note this mode is the ONE place PHP holds database credentials, which
        // §12/D8 otherwise forbids — inherent to being PDO, and a measurement-only path.
        $driver = getenv('FERRO_LARAVEL_DRIVER') ?: 'ferro-pgsql';
        if (!in_array($driver, ['ferro-pgsql', 'pgsql', 'stock-pgsql'], true)) {
            throw new \RuntimeException(sprintf(
                'FERRO_LARAVEL_DRIVER="%s" is not one of: ferro-pgsql, pgsql, stock-pgsql.',
                $driver,
            ));
        }

        if ($driver === 'stock-pgsql') {
            $app['config']->set('database.connections.pgsql', self::controlConfigForBootstrap());
            $app['config']->set('database.default', 'pgsql');
            $connection = $app['config']->get('database.default');
            $this->driver = $app['config']->get("database.connections.$connection.driver");
            return;
        }

        FerroConnections::register(['pgsql' => 'ferro-pgsql']);

        $sock = getenv('FERRO_LARAVEL_SOCK');
        if ($sock === false || $sock === '') {
            // Loud, not defaulted: a missing socket must not degrade into some other connection.
            throw new \RuntimeException('FERRO_LARAVEL_SOCK is unset; refusing to guess a connection.');
        }

        // THE CONNECTION IS NAMED AFTER ITS DRIVER, and there is exactly ONE of it.
        //
        // The obvious alternative — a fixed name like `ferro` — was tried and MEASURED wrong:
        // upstream tests reach for `database.connections.{$this->driver}` by that key.
        // `SchemaBuilderSchemaNameTest::defineEnvironment` clones it into two further connections
        // AND sets `search_path` on it, and Laravel's `PostgresBuilder::getSchemas()` reads
        // `search_path` from the CONFIG ARRAY (never from the server session), so a default
        // connection under a different key silently kept `['public']`: `migrate:fresh` then never
        // dropped `my_schema.table`, and all 22 of that file's cases failed on the NEXT case with
        // `relation "table" already exists`. Two copies of the config diverge the moment a test
        // mutates one; one connection, named the way the tests look it up, cannot.
        //
        // It is still never named `testing`, which is what matters for the SQLite trap: testbench's
        // `LoadConfiguration::bootstrap()` injects THAT name as in-memory sqlite when unconfigured.
        // Both values here (`ferro-pgsql`, `pgsql`) are fully specified, so nothing is implied — and
        // under the alias this deliberately OVERWRITES testbench's stock `pgsql` entry (which points
        // at a real PostgreSQL) rather than leaving a second, non-Ferro route into the suite.
        $app['config']->set('database.connections.' . $driver, [
            'driver' => $driver,
            // REQUIRED by Illuminate even though Ferro does not dial it. `ConnectionFactory::
            // createSingleConnection()` reads `$config['database']` with NO default, BEFORE any
            // resolver runs, so a connection array without it dies on `Undefined array key
            // "database"`. Its value is Laravel's own label — `getDatabaseName()`, QueryException
            // messages, the migration repository — never a selector: the pool's DSN chooses the
            // upstream database. (`prefix` IS defaulted by `parseConfig()`, so it is optional.)
            'database' => getenv('FERRO_LARAVEL_DB') ?: 'laravel_tests',
            'ferro_socket' => $sock,
            'pool' => getenv('FERRO_LARAVEL_POOL') ?: 'default',
        ]);
        $app['config']->set('database.default', $driver);

        // --- upstream body, verbatim -----------------------------------------------------------
        $connection = $app['config']->get('database.default');

        $this->driver = $app['config']->get("database.connections.$connection.driver");
    }

    /**
     * The CONTROL connection: stock `pdo_pgsql` straight at the same PostgreSQL the pool dials,
     * built from the SAME `FERRO_LARAVEL_DSN` so the two columns cannot drift onto different
     * servers or databases — which would make every difference between them uninterpretable.
     *
     * `search_path` is read out of the DSN's libpq `options=-csearch_path=…` and passed as Laravel's
     * own config key, because that is how the two ends of this contract differ and the difference is
     * the point: a stock app sets `search_path` in `config/database.php` and the PDO connector
     * issues `SET search_path`, while a Ferro app sets it on the ferrod pool DSN (the session is
     * pooled, so it belongs to the pool). Laravel's `PostgresBuilder::getSchemas()` reads the CONFIG
     * key on BOTH — it never asks the server — so a Ferro app that uses a non-default schema needs
     * it in both places.
     *
     * Public and awkwardly named on purpose: `bootstrap.php`'s inverted control assertion builds the
     * same connection BEFORE any test runs, and it must dial exactly what the tests will.
     *
     * @return array<string,mixed>
     */
    public static function controlConfigForBootstrap(): array
    {
        $dsn = getenv('FERRO_LARAVEL_DSN');
        if ($dsn === false || $dsn === '') {
            throw new \RuntimeException('FERRO_LARAVEL_DSN is unset; the control column has nothing to dial.');
        }
        $u = parse_url($dsn);
        if (!is_array($u) || !isset($u['host'])) {
            throw new \RuntimeException('FERRO_LARAVEL_DSN is not a parseable URL.');
        }
        parse_str($u['query'] ?? '', $q);
        $searchPath = 'public';
        $options = $q['options'] ?? null;
        if (is_string($options) && preg_match('/-csearch_path=([^\s]+)/', $options, $m) === 1) {
            $searchPath = $m[1];
        }

        return [
            'driver' => 'pgsql',
            'host' => $u['host'],
            'port' => $u['port'] ?? 5432,
            'database' => ltrim($u['path'] ?? '', '/') ?: 'laravel_tests',
            'username' => isset($u['user']) ? rawurldecode($u['user']) : '',
            'password' => isset($u['pass']) ? rawurldecode($u['pass']) : '',
            'charset' => 'utf8',
            'prefix' => '',
            'search_path' => $searchPath,
            'sslmode' => 'prefer',
        ];
    }
}
