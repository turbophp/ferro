<?php // testkit/orm/TestUtil.ferro.php — copied OVER doctrine/orm's tests/Tests/TestUtil.php by testkit/orm-suite.sh

declare(strict_types=1);

namespace Doctrine\Tests;

use Doctrine\Common\EventSubscriber;
use Doctrine\DBAL\Configuration as DbalConfiguration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\ORM\Configuration;
use RuntimeException;

use function assert;
use function explode;
use function getenv;
use function is_string;
use function json_decode;

use const JSON_THROW_ON_ERROR;
use const PHP_VERSION_ID;

/**
 * Ferro replacement for doctrine/orm 3.7.3 tests/Tests/TestUtil.php (M2, SPEC §22.2 (ci)).
 *
 * Upstream's maps `db_driver` and never `driverClass` (verified in 3.7.3's
 * `mapConnectionParameters()`), so a Ferro driver cannot be selected through it at all; and it
 * DROPs/CREATEs the database through a privileged connection, which a Ferro connection has no
 * credentials for. Differences from upstream:
 *  - honours db_driverClass (Ferro column) OR db_driver (control), refuses both / neither;
 *  - db_driver_options is a JSON object -> driverOptions;
 *  - initializeDatabase() (privileged DROP/CREATE DATABASE) is gone: the runner resets out of band
 *    (Ferro's PHP holds no credentials, SPEC §12/D8). The control uses the SAME out-of-band reset so
 *    the two columns differ in the driver only.
 *  - getPrivilegedConnection() = a second independent connection with the same params.
 * Surface kept: getConnection(?config), getPrivilegedConnection(), configureProxies().
 */
class TestUtil
{
    public static function getConnection(DbalConfiguration|null $config = null): DbalExtensions\Connection
    {
        $connection = DriverManager::getConnection(self::params(), $config);
        assert($connection instanceof DbalExtensions\Connection);
        self::addDbEventSubscribers($connection);

        return $connection;
    }

    public static function getPrivilegedConnection(): DbalExtensions\Connection
    {
        $connection = DriverManager::getConnection(self::params());
        assert($connection instanceof DbalExtensions\Connection);

        return $connection;
    }

    public static function configureProxies(Configuration $configuration): void
    {
        $enableNativeLazyObjects = getenv('ENABLE_NATIVE_LAZY_OBJECTS');
        if ($enableNativeLazyObjects === false) {
            $enableNativeLazyObjects = true;
        }

        if (PHP_VERSION_ID >= 80400 && $enableNativeLazyObjects) {
            $configuration->enableNativeLazyObjects(true);

            return;
        }

        $configuration->setProxyDir(__DIR__ . '/Proxies');
        $configuration->setProxyNamespace('Doctrine\Tests\Proxies');
    }

    /** @return array<string,mixed> */
    public static function params(): array
    {
        $params = [];
        foreach (['driver', 'driverClass', 'host', 'port', 'user', 'password', 'dbname', 'unix_socket', 'path'] as $key) {
            if (isset($GLOBALS['db_' . $key]) && $GLOBALS['db_' . $key] !== '') {
                $params[$key] = $GLOBALS['db_' . $key];
            }
        }
        if (isset($params['port'])) {
            $params['port'] = (int) $params['port'];
        }
        if (isset($GLOBALS['db_driver_options']) && is_string($GLOBALS['db_driver_options'])) {
            $params['driverOptions'] = json_decode($GLOBALS['db_driver_options'], true, 512, JSON_THROW_ON_ERROR);
        }
        if (isset($params['driver']) && isset($params['driverClass'])) {
            throw new RuntimeException('ferro orm-suite TestUtil: both db_driver and db_driverClass set; refusing.');
        }
        if (! isset($params['driver']) && ! isset($params['driverClass'])) {
            throw new RuntimeException('ferro orm-suite TestUtil: neither db_driverClass nor db_driver set; refusing.');
        }
        $params['wrapperClass'] = DbalExtensions\Connection::class;

        return $params;
    }

    private static function addDbEventSubscribers(Connection $conn): void
    {
        if (! isset($GLOBALS['db_event_subscribers'])) {
            return;
        }

        $evm = $conn->getEventManager();
        /** @var class-string<EventSubscriber> $subscriberClass */
        foreach (explode(',', $GLOBALS['db_event_subscribers']) as $subscriberClass) {
            $evm->addEventSubscriber(new $subscriberClass());
        }
    }
}
