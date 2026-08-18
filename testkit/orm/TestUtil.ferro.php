<?php // testkit/orm/TestUtil.ferro.php  ->  copied over <orm>/tests/Tests/TestUtil.php by testkit/orm-suite.sh

declare(strict_types=1);

namespace Doctrine\Tests;

use Doctrine\Common\EventSubscriber;
use Doctrine\DBAL\Configuration as DbalConfiguration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\AbstractSQLiteDriver\Middleware\EnableForeignKeys;
use Doctrine\DBAL\DriverManager;
use Doctrine\ORM\Configuration;
use InvalidArgumentException;

use function assert;
use function class_exists;
use function explode;
use function getenv;
use function in_array;
use function json_decode;

use const JSON_THROW_ON_ERROR;
use const PHP_VERSION_ID;

/**
 * FERRO ORM HARNESS TESTUTIL (M1-S9) — replaces upstream tests/Tests/TestUtil.php. Changed vs
 * 3.6.8: getTestConnectionParameters() honours db_driverClass (Ferro) with a db_driver stock
 * branch (the recorded comparator); initializeDatabase() is a no-op (the container-side reset in
 * testkit/orm-suite.sh owns idempotence — PHP holds no credentials, SPEC §12/D8);
 * configureProxies() applies the documented D-S8b-5 SEQUENCE identity preference when
 * FERRO_ORM_PG_SEQUENCE=1. Everything else is upstream verbatim.
 *
 * THREE further notes, each measured rather than assumed:
 *
 *  1. **Why the replacement exists at all.** Upstream's `mapConnectionParameters()` (3.6.8:197-246)
 *     maps only `driver`; `driverClass` is not in its key list and is silently DISCARDED. Set
 *     `db_driverClass` alone and `getTestConnectionParameters()` THROWS; set both and `db_driver`
 *     WINS — the suite then measures stock PDO under a Ferro-labeled banner. This file inverts that
 *     precedence (driverClass FIRST) and refuses to guess when neither is present.
 *
 *  2. **`getConnection()` is kept verbatim EXCEPT for one `?? null`.** Its SQLite check reads
 *     `$connectionParameters['driver']` with no isset, and the ferro branch has no `driver` key —
 *     which would emit an "Undefined array key" E_WARNING on every one of the suite's thousands of
 *     getConnection() calls (TestInit sets `error_reporting(E_ALL)`, and PHPUnit 11 records
 *     warnings per test, so the result line itself would move). The guard is NAMED here because
 *     "everything else verbatim" otherwise forbids it.
 *
 *  3. **The SEQUENCE block is FIRST in `configureProxies()`, and that placement is load-bearing.**
 *     Upstream's method OPENS with
 *     `if (PHP_VERSION_ID >= 80400 && $enableNativeLazyObjects) { …; return; }` — an early return
 *     taken on this harness's own environment (PHP 8.4.18, native lazy objects defaulted on). An
 *     APPENDED preference would be dead code, and the measured cost is 1229 errors / ~35% of the
 *     suite (research-orm step 6) surfacing with the exact D-S8b-5 error text — a broken harness
 *     that reads as a genuine finding about PostgreSQL identity strategy. testkit/orm/bootstrap.php
 *     asserts the preference actually landed on a real Configuration before any test runs.
 */
class TestUtil
{
    /** @var bool Whether the database schema is initialized. */
    private static bool $initialized = false;

    /**
     * Gets a <b>real</b> database connection using the following parameters
     * of the $GLOBALS array:
     *
     * 'db_driverClass' : The Ferro driver class (this harness's own branch), OR
     * 'db_driver' : The name of the Doctrine DBAL database driver to use (stock comparator).
     * 'db_user' : The username to use for connecting.
     * 'db_password' : The password to use for connecting.
     * 'db_host' : The hostname of the database to connect to.
     * 'db_dbname' : The name of the database to connect to.
     * 'db_port' : The port of the database to connect to.
     * 'db_unix_socket' : The ferrod socket (Ferro branch).
     *
     * These variables of the $GLOBALS array are filled by PHPUnit based on an XML configuration file.
     *
     * IMPORTANT:
     * 1) Each invocation of this method returns a NEW database connection.
     * 2) The database is reset container-side by testkit/orm-suite.sh, not here.
     */
    public static function getConnection(DbalConfiguration|null $config = null): DbalExtensions\Connection
    {
        if (! self::$initialized) {
            self::initializeDatabase();
            self::$initialized = true;
        }

        $connectionParameters = self::getTestConnectionParameters();

        // FERRO: `?? null` added — see class docblock note 2. Everything else in this method is
        // upstream 3.6.8 verbatim.
        if (in_array($connectionParameters['driver'] ?? null, ['pdo_sqlite', 'sqlite3'], true) && class_exists(EnableForeignKeys::class)) {
            if ($config === null) {
                $config = new DbalConfiguration();
            }

            $config->setMiddlewares([...$config->getMiddlewares(), new EnableForeignKeys()]);
        }

        $connection = DriverManager::getConnection($connectionParameters, $config);
        assert($connection instanceof DbalExtensions\Connection);

        self::addDbEventSubscribers($connection);

        return $connection;
    }

    public static function getPrivilegedConnection(): DbalExtensions\Connection
    {
        $connection = DriverManager::getConnection(self::getPrivilegedConnectionParameters());
        assert($connection instanceof DbalExtensions\Connection);

        return $connection;
    }

    public static function configureProxies(Configuration $configuration): void
    {
        // ------------------------------------------------------------------------------------
        // FERRO, AND IT MUST BE THE FIRST STATEMENT IN THIS METHOD (class docblock note 3): the
        // upstream body below RETURNS EARLY on PHP >= 8.4 with native lazy objects, which is this
        // harness's environment.
        //
        // D-S8b-5, the documented ORM-on-PostgreSQL adoption path (SPEC §14): IDENTITY is ORM's
        // DBAL-4 default for PG, IdentityGenerator::generateId() is `(int) $conn->lastInsertId()`,
        // and PG reports no generated key through Ferro BY DESIGN. Upstream's own deprecation text
        // (ClassMetadataFactory) recommends exactly this preference. This is WITHIN-BAR: testing
        // the product as documented, not rigging the harness. Measured cost of omitting it: 1229
        // errors, ~35% of the suite — research-orm step 6.
        // ------------------------------------------------------------------------------------
        if (getenv('FERRO_ORM_PG_SEQUENCE') === '1') {
            $configuration->setIdentityGenerationPreferences([
                \Doctrine\DBAL\Platforms\PostgreSQLPlatform::class
                    => \Doctrine\ORM\Mapping\ClassMetadata::GENERATOR_TYPE_SEQUENCE,
            ]);
        }

        $enableNativeLazyObjects = getenv('ENABLE_NATIVE_LAZY_OBJECTS');

        if ($enableNativeLazyObjects === false) {
            // If the environment variable is not set, we default to true.
            // This is OK because environment variables are always strings, and
            // we are comparing it to a boolean.
            $enableNativeLazyObjects = true;
        }

        if (PHP_VERSION_ID >= 80400 && $enableNativeLazyObjects) {
            $configuration->enableNativeLazyObjects(true);

            return;
        }

        $configuration->setProxyDir(__DIR__ . '/Proxies');
        $configuration->setProxyNamespace('Doctrine\Tests\Proxies');
    }

    /**
     * Upstream drops and re-creates the test database here through a PRIVILEGED connection. Ferro
     * structurally cannot: the DSN lives in the ENGINE and PHP holds no credentials at all
     * (SPEC §12 / D8), so there is nothing a client-side privileged connection could mean — and
     * dropping a database a live pool holds connections to is refused anyway.
     *
     * **This method is also the suite's ONLY reset**, so a no-op here is sound only because
     * testkit/orm-suite.sh performs a fail-closed container-side reset BEFORE launching ferrod.
     * Do not remove one without the other: measured, an unreset ORM database turns 48 non-passing
     * into 227 with a triage that blames the driver (research-orm step 6b). A recorded number MUST
     * come from a run that printed the runner's `[ferro-orm] reset: …` line.
     */
    private static function initializeDatabase(): void
    {
    }

    private static function addDbEventSubscribers(Connection $conn): void
    {
        if (! isset($GLOBALS['db_event_subscribers'])) {
            return;
        }

        $evm = $conn->getEventManager();
        /** @var class-string<EventSubscriber> $subscriberClass */
        foreach (explode(',', $GLOBALS['db_event_subscribers']) as $subscriberClass) {
            $subscriberInstance = new $subscriberClass();
            $evm->addEventSubscriber($subscriberInstance);
        }
    }

    /**
     * Upstream this is a connection with credentials that can drop and create databases. Ferro has
     * no such thing and cannot (SPEC §12 / D8), so "privileged" here means exactly one thing: a
     * SECOND, independent connection with the same parameters. ORM 3.6.8 has ZERO external call
     * sites for `getPrivilegedConnection()` (grep over the clone's tests/), so this is a surface
     * kept for API compatibility, not a behaviour any test depends on.
     *
     * @return array<string, mixed>
     */
    private static function getPrivilegedConnectionParameters(): array
    {
        return self::getTestConnectionParameters();
    }

    /** @return array<string, mixed> */
    private static function getTestConnectionParameters(): array
    {
        if (isset($GLOBALS['db_driverClass'])) {
            // The Ferro branch, checked FIRST — the inverse of upstream's precedence, so a stray
            // db_driver can never silently win and run stock PDO under a Ferro banner. Upstream's
            // mapConnectionParameters() maps ONLY 'driver'; a db_driverClass was DISCARDED
            // (research-orm step 1), so the array is built directly instead.
            //
            // 'wrapperClass' keeps upstream's hardcoded slot (:243): the suite's own QueryLog
            // wrapper, which OrmFunctionalTestCase's query-count assertions and getConnection()'s
            // own assert() both require. Ferro's REQUIRED wrapper (§22.2 (ah)) reaches the same
            // single slot by INHERITANCE — testkit/orm-suite.sh re-parents that class onto
            // FerroConnection, and testkit/orm/bootstrap.php asserts the re-parenting applied.
            return [
                'driverClass'   => $GLOBALS['db_driverClass'],
                'wrapperClass'  => DbalExtensions\Connection::class,
                'unix_socket'   => $GLOBALS['db_unix_socket'],
                'dbname'        => $GLOBALS['db_dbname'] ?? 'doctrine_orm_tests',
                'driverOptions' => json_decode(
                    (string) ($GLOBALS['db_driver_options'] ?? '{}'),
                    true,
                    512,
                    JSON_THROW_ON_ERROR,
                ),
            ];
        }

        if (isset($GLOBALS['db_driver'])) {
            // The STOCK comparator branch (pdo_pgsql / pdo_mysql) — the baseline any Ferro number
            // is judged against. Same wrapper slot, same reset discipline.
            return [
                'driver'       => $GLOBALS['db_driver'],
                'wrapperClass' => DbalExtensions\Connection::class,
                'host'         => $GLOBALS['db_host'],
                'port'         => (int) $GLOBALS['db_port'],
                'user'         => $GLOBALS['db_user'],
                'password'     => $GLOBALS['db_password'],
                'dbname'       => $GLOBALS['db_dbname'],
            ];
        }

        throw new InvalidArgumentException(
            'neither db_driverClass (Ferro) nor db_driver (stock comparator) is set — this '
            . 'harness refuses to guess a driver; testkit/orm-suite.sh sets exactly one',
        );
    }
}
