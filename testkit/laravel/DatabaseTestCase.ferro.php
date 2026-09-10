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
        // The name is observable to the tests themselves: six of upstream's own QueryBuilderTest
        // cases gate their assertion on `in_array($this->driver, ['pgsql', 'sqlsrv'])` and assert
        // nothing at all under any other name. Recording both columns is what keeps that visible
        // instead of letting it hide inside a single number.
        $driver = getenv('FERRO_LARAVEL_DRIVER') ?: 'ferro-pgsql';
        if (!in_array($driver, ['ferro-pgsql', 'pgsql'], true)) {
            throw new \RuntimeException(sprintf('FERRO_LARAVEL_DRIVER="%s" is not one of: ferro-pgsql, pgsql.', $driver));
        }
        FerroConnections::register(['pgsql' => 'ferro-pgsql']);

        $sock = getenv('FERRO_LARAVEL_SOCK');
        if ($sock === false || $sock === '') {
            // Loud, not defaulted: a missing socket must not degrade into some other connection.
            throw new \RuntimeException('FERRO_LARAVEL_SOCK is unset; refusing to guess a connection.');
        }

        $app['config']->set('database.connections.ferro', [
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
        $app['config']->set('database.default', 'ferro');

        // --- upstream body, verbatim -----------------------------------------------------------
        $connection = $app['config']->get('database.default');

        $this->driver = $app['config']->get("database.connections.$connection.driver");
    }
}
