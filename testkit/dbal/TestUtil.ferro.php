<?php // testkit/dbal/TestUtil.ferro.php  ->  copied over <dbal>/tests/TestUtil.php by testkit/dbal-suite.sh

declare(strict_types=1);

namespace Doctrine\DBAL\Tests;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\DefaultSchemaManagerFactory;
use RuntimeException;

use function array_map;
use function implode;
use function is_string;
use function json_decode;

use const JSON_THROW_ON_ERROR;

/**
 * Ferro's replacement for doctrine/dbal 4.4.4's own `tests/TestUtil`.
 *
 * TWO upstream behaviours make the stock file unusable against a third-party driver, and BOTH are
 * measured rather than assumed:
 *
 *  1. `getConnectionParams()` returns the mapped `$GLOBALS['db_*']` params ONLY when
 *     `$params['driver']` is set, and otherwise returns `['driver' => 'pdo_sqlite', 'memory' => true]`
 *     (upstream 4.4.4 lines 82-98). `db_driverClass` is DISCARDED. Since `pdo_sqlite` is loaded almost
 *     everywhere, the entire functional suite then runs GREEN against in-memory SQLite with zero Ferro
 *     contact — and nothing skips, so no skip-detector catches it. This file honours `db_driverClass`
 *     and THROWS when it is absent, so the fallback can never happen silently.
 *
 *  2. `initializeDatabase()` opens a PRIVILEGED connection with `dbname` unset and runs
 *     `dropDatabase()` + `createDatabase()` on every run. Ferro cannot serve that: the DSN lives in
 *     the ENGINE and PHP has no credentials at all (SPEC §12 / D8), there is nothing for a
 *     client-side `dbname` to mean, and dropping a database a live pool holds connections to is
 *     refused anyway.
 *
 *     **That method is also the functional suite's ONLY reset**, so removing it without replacing it
 *     makes the suite non-idempotent: measured against a KNOWN-GOOD driver, the same command gave
 *     `Errors 23` and then `Errors 33` on consecutive runs, while upstream's version gave `0/0`
 *     before and after. The replacement is `testkit/dbal-suite.sh`'s container-side reset, which
 *     needs no PHP credentials — the same shape the `testkit/mysql-init.sql` grant already uses. The
 *     method below stays a no-op, and the RUNNER is where idempotence now lives; a recorded number
 *     MUST come from a run that performed the reset (the runner prints a `[ferro] reset: …` line).
 *
 * `isDriverOneOf()` answers FALSE for every name, and that is a deliberate decision with 60 call
 * sites behind it: claiming `pdo_pgsql`/`pdo_mysql` would opt Ferro into PDO-specific expectations
 * and into whole vendor sub-trees written against those extensions. Answering nothing means every
 * vendor-gated test takes its "other" branch, which is the honest description of what Ferro is.
 *
 * The public surface below is the MEASURED one, read out of the pinned 4.4.4 clone rather than
 * guessed: `isDriverOneOf` (:237), `getPrivilegedConnection` (:232), `isPdoStringifyFetchesEnabled`
 * (:245), `generateResultSetQuery` (:257), `getConnectionParams` (:82), `getConnection` (:64) are
 * PUBLIC; `initializeDatabase` (:100), `getPrivilegedConnectionParameters` (:176),
 * `getTestConnectionParameters` (:189), `mapConnectionParameters` (:199) and `createConfiguration`
 * (:155) are PRIVATE and called by nothing outside the class.
 *
 * TWO deliberate departures from the plan's draft of this file, both forced by reading the real
 * upstream source:
 *   - `getConnection()` does NOT cache. Upstream does not either, and
 *     `tests/Functional/WriteTest.php:213 testLastInsertIdNewConnection` calls it precisely to obtain
 *     a SECOND, fresh connection; caching would hand it the shared one and the test would assert
 *     against the wrong object.
 *   - `generateResultSetQuery()` takes THREE parameters (`$columnNames, $rows, $platform`), not the
 *     two the plan's comment named. The body below is transcribed BYTE-FOR-BYTE from
 *     `tests/TestUtil.php:257-270` of the pinned clone: it is platform-SQL GENERATION, not a policy
 *     answer, and a reconstruction from the plan's signature would have been wrong in both arity and
 *     behaviour.
 */
class TestUtil
{
    /**
     * Upstream returns a NEW connection on every call and so do we — see the class docblock.
     */
    public static function getConnection(): Connection
    {
        self::initializeDatabase();

        return DriverManager::getConnection(self::getConnectionParams(), self::createConfiguration());
    }

    /** @return array<string,mixed> */
    public static function getConnectionParams(): array
    {
        $params = [];

        foreach (['driverClass', 'host', 'port', 'user', 'password', 'dbname', 'unix_socket', 'wrapperClass'] as $key) {
            if (isset($GLOBALS['db_' . $key]) && $GLOBALS['db_' . $key] !== '') {
                $params[$key] = $GLOBALS['db_' . $key];
            }
        }

        if (isset($params['port'])) {
            $params['port'] = (int) $params['port'];
        }

        if (isset($GLOBALS['db_driver_options']) && is_string($GLOBALS['db_driver_options'])) {
            /** @var array<string,mixed> $decoded */
            $decoded                = json_decode($GLOBALS['db_driver_options'], true, 512, JSON_THROW_ON_ERROR);
            $params['driverOptions'] = $decoded;
        }

        if (isset($GLOBALS['db_serverVersion']) && $GLOBALS['db_serverVersion'] !== '') {
            $params['serverVersion'] = $GLOBALS['db_serverVersion'];
        }

        if (! isset($params['driverClass'])) {
            throw new RuntimeException(
                'Ferro TestUtil: db_driverClass is not set. This runner exists precisely because the '
                . 'upstream TestUtil would silently fall back to in-memory SQLite here.',
            );
        }

        return $params;
    }

    /**
     * Pre-provisioned and RESET by `testkit/dbal-suite.sh`, container-side; see the class docblock.
     * A no-op here is only sound because that reset exists — do not remove one without the other.
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

    public static function isDriverOneOf(string ...$names): bool
    {
        return false;
    }

    /**
     * Upstream this is a connection with credentials that can drop and create databases. **Ferro has
     * no such thing, and cannot**: the DSN lives in the engine and PHP holds no credentials at all
     * (SPEC §12 / D8). So "privileged" here means exactly one thing — a SECOND, independent
     * connection to the same pool — which is what the allowlisted call site actually needs
     * (`tests/Functional/TransactionTest.php` uses it to observe an in-progress transaction from
     * outside it). A test that genuinely needs DDL privileges Ferro's user does not have will fail
     * loudly on that DDL, which is the correct outcome and is triaged as category (c).
     *
     * Note it is deliberately NOT the shared connection: sharing would make the cross-connection
     * observation it exists for meaningless.
     */
    public static function getPrivilegedConnection(): Connection
    {
        return DriverManager::getConnection(self::getConnectionParams(), self::createConfiguration());
    }

    /**
     * Whether PDO is configured to stringify fetched values. Ferro is not PDO and has no such mode:
     * every column arrives typed from the `/proto` tag registry.
     */
    public static function isPdoStringifyFetchesEnabled(): bool
    {
        return false;
    }

    /**
     * Generates a query that will return the given rows without the need to create a temporary table.
     *
     * COPIED BYTE-FOR-BYTE from the pinned clone, `tests/TestUtil.php:250-270`. Do not rewrite it.
     *
     * @param list<string>      $columnNames The names of the result columns. Must be non-empty.
     * @param list<list<mixed>> $rows        The rows of the result. Each row must have the same number of columns
     *                                       as the number of column names.
     */
    public static function generateResultSetQuery(array $columnNames, array $rows, AbstractPlatform $platform): string
    {
        return implode(' UNION ALL ', array_map(static function (array $row) use ($columnNames, $platform): string {
            return $platform->getDummySelectSQL(
                implode(', ', array_map(static function (string $column, $value) use ($platform): string {
                    if (is_string($value)) {
                        $value = $platform->quoteStringLiteral($value);
                    }

                    return $value . ' ' . $platform->quoteSingleIdentifier($column);
                }, $columnNames, $row)),
            );
        }, $rows));
    }
}
