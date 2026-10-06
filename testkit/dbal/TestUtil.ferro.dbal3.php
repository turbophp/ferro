<?php // testkit/dbal/TestUtil.ferro.dbal3.php  ->  copied over <dbal 3.x>/tests/TestUtil.php by testkit/dbal-suite.sh

declare(strict_types=1);

namespace Doctrine\DBAL\Tests;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\DefaultSchemaManagerFactory;
use RuntimeException;

use function array_keys;
use function array_map;
use function array_values;
use function implode;
use function is_string;
use function json_decode;

use const JSON_THROW_ON_ERROR;

/**
 * Ferro's replacement for doctrine/dbal **3.10.6**'s own `tests/TestUtil` — the DBAL 3 twin of
 * `TestUtil.ferro.php` (M2-C5b). Every decision is that file's, for the same measured reasons; read
 * its docblock. In short:
 *
 *  - `getConnectionParams()` honours `db_driverClass` and THROWS when neither it nor `db_driver` is
 *    set. Upstream 3.10.6 falls back to in-memory `pdo_sqlite` when `db_driver` is unset
 *    (`hasRequiredConnectionParams()` reads ONLY `db_driver`), so the suite would pass against the
 *    wrong engine with nothing skipped — the trap this file exists to close.
 *  - `initializeDatabase()` is a no-op: upstream drops and recreates the database through a
 *    privileged connection, which Ferro cannot serve (PHP holds no credentials, SPEC §12 / D8). The
 *    RUNNER's container-side (or file-delete) reset is where idempotence lives.
 *  - `isDriverOneOf()` answers the column's PDO driver name — the control's real one, and for the
 *    Ferro column the PDO driver of the family the pool serves — so both columns take upstream's
 *    vendor gates exactly as a stock `pdo_*` run would (E9, SPEC §22.2 (da); it answered FALSE for
 *    every name until then, which made every positive vendor gate skip in BOTH columns).
 *
 * **Where 3.10.6's surface differs from 4.4.4's, this file follows 3.10.6** — measured from the
 * pinned clone, not assumed: `generateResultSetQuery(array $rows, AbstractPlatform $platform)` takes
 * TWO parameters and derives the column names from each row's keys (4.4.4 takes the names
 * separately), and quotes them with `quoteIdentifier()` (4.4.4: `quoteSingleIdentifier()`). Its body
 * is transcribed byte-for-byte below. Upstream's `createConfiguration($driver)` also installs
 * `EnableForeignKeys` for `pdo_sqlite`; it is NOT installed here, exactly as the DBAL 4 file does not,
 * so the SQLite control column is comparable line by line with C3-6a's DBAL 4 one (Ferro's SQLite
 * pool enforces foreign keys itself; a session pragma from a middleware could not, §22.2 (bl)).
 */
class TestUtil
{
    /** Upstream returns a NEW connection on every call and so do we. */
    public static function getConnection(): Connection
    {
        self::initializeDatabase();

        return DriverManager::getConnection(self::getConnectionParams(), self::createConfiguration());
    }

    /** @return array<string,mixed> */
    public static function getConnectionParams(): array
    {
        $params = [];

        foreach (['driver', 'driverClass', 'path', 'host', 'port', 'user', 'password', 'dbname', 'unix_socket', 'wrapperClass'] as $key) {
            if (isset($GLOBALS['db_' . $key]) && $GLOBALS['db_' . $key] !== '') {
                $params[$key] = $GLOBALS['db_' . $key];
            }
        }

        if (isset($params['port'])) {
            $params['port'] = (int) $params['port'];
        }

        if (isset($GLOBALS['db_driver_options']) && is_string($GLOBALS['db_driver_options'])) {
            /** @var array<string,mixed> $decoded */
            $decoded                 = json_decode($GLOBALS['db_driver_options'], true, 512, JSON_THROW_ON_ERROR);
            $params['driverOptions'] = $decoded;
        }

        if (isset($GLOBALS['db_serverVersion']) && $GLOBALS['db_serverVersion'] !== '') {
            $params['serverVersion'] = $GLOBALS['db_serverVersion'];
        }

        if (isset($params['driver']) && isset($params['driverClass'])) {
            throw new RuntimeException(
                'Ferro TestUtil: both db_driver and db_driverClass are set. One run is either the '
                . 'Ferro column or the control, never both — DriverManager would silently prefer '
                . 'driverClass and the "control" would be measuring Ferro.',
            );
        }

        if (! isset($params['driverClass']) && ! isset($params['driver'])) {
            throw new RuntimeException(
                'Ferro TestUtil: neither db_driverClass nor db_driver is set. This runner exists '
                . 'precisely because the upstream TestUtil would silently fall back to in-memory '
                . 'SQLite here.',
            );
        }

        return $params;
    }

    /**
     * Pre-provisioned and RESET by `testkit/dbal-suite.sh`; see the class docblock. A no-op here is
     * only sound because that reset exists — do not remove one without the other.
     */
    private static function initializeDatabase(): void
    {
    }

    private static function createConfiguration(): Configuration
    {
        $configuration = new Configuration();
        $configuration->setSchemaManagerFactory(new DefaultSchemaManagerFactory());

        return $configuration;
    }

    /**
     * Upstream: a connection with credentials that can drop and create databases. Ferro has no such
     * thing; here it is a SECOND, independent connection to the same pool — see the DBAL 4 file.
     */
    public static function getPrivilegedConnection(): Connection
    {
        return DriverManager::getConnection(self::getConnectionParams(), self::createConfiguration());
    }

    public static function isDriverOneOf(string ...$names): bool
    {
        // The control names its real driver (`db_driver`); the Ferro column names the PDO driver of
        // the family its pool serves (`db_vendor_driver`, from the runner). Neither is a guess this
        // file may make, so a run with neither refuses rather than silently taking every "other" branch.
        $driver = $GLOBALS['db_driver'] ?? $GLOBALS['db_vendor_driver'] ?? null;
        if (! is_string($driver) || $driver === '') {
            throw new RuntimeException('isDriverOneOf(): neither db_driver (the control) nor db_vendor_driver (the Ferro column) is set');
        }

        return in_array($driver, $names, true);
    }

    /**
     * Generates a query that will return the given rows without the need to create a temporary table.
     *
     * COPIED BYTE-FOR-BYTE from the pinned 3.10.6 clone's `tests/TestUtil.php`. Do not rewrite it.
     *
     * @param array<int,array<string,mixed>> $rows
     */
    public static function generateResultSetQuery(array $rows, AbstractPlatform $platform): string
    {
        return implode(' UNION ALL ', array_map(static function (array $row) use ($platform): string {
            return $platform->getDummySelectSQL(
                implode(', ', array_map(static function (string $column, $value) use ($platform): string {
                    if (is_string($value)) {
                        $value = $platform->quoteStringLiteral($value);
                    }

                    return $value . ' ' . $platform->quoteIdentifier($column);
                }, array_keys($row), array_values($row))),
            );
        }, $rows));
    }
}
