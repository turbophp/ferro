<?php // /php/doctrine-dbal/src/PlatformVersion.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\AbstractMySQLDriver;
use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;
use Doctrine\DBAL\Driver\AbstractSQLiteDriver;
use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Ferro\DBAL\Exception\BackendFamilyUnknown;

/**
 * Turns (backend family, raw `version()` string) into a STOCK Doctrine platform.
 *
 * **We choose the STRING; Doctrine chooses the PLATFORM.** The version ladders
 * (`>= 8.4 MySQL84Platform`, `>= 11.7 MariaDB110700Platform`, `>= 12.0 PostgreSQL120Platform`, …)
 * live in DBAL's own abstract drivers and move between DBAL releases; restating them here would be
 * a second source of truth that silently rots. So this class delegates to
 * `AbstractPostgreSQLDriver::getDatabasePlatform()` / `AbstractMySQLDriver::getDatabasePlatform()`
 * through a platform-only anonymous subclass, and its whole job is the ONE transform those two
 * cannot do for themselves.
 *
 * **That transform is asymmetric, and getting it uniform is the measured way to ship a wrong SQL
 * dialect.** `ferrod` caches the backend's own `version()` output VERBATIM (`pools.rs`'s
 * `VERSION_SQL`, and `PoolInfo`'s own docblock says normalising it is the consuming tier's job):
 *
 *  - **PostgreSQL** answers `PostgreSQL 17.10 (Debian …) on x86_64-…`, and the stock parser is
 *    ANCHORED (`/^(?P<major>\d+)…/`). Measured: that string throws `InvalidPlatformVersion` on
 *    EVERY connection. Stripping the leading product name is mandatory.
 *  - **MySQL/MariaDB** answer `8.4.11` and `11.8.8-MariaDB-ubu2404`, and MariaDB is detected ONLY by
 *    `stripos($version, 'mariadb') !== false`. Measured: normalising `11.8.8-MariaDB-ubu2404` down
 *    to `11.8.8` selects `MySQL84Platform` — a MariaDB server driven with MySQL's grammar, silently.
 *    So the MySQL-family string passes through BYTE-IDENTICAL.
 *  - **SQLite** answers a bare `3.53.2` (`sqlite_version()`), and there is exactly one SQLite
 *    platform whose selection ignores the version altogether. Nothing to transform.
 *
 * Charter rule 6 is intact: no platform is subclassed, no SQL is generated here. We select.
 */
final class PlatformVersion
{
    /** The `PoolInfo.kind` wire values (`PoolKind::wire_name()` in `ferrod`). Never nil. */
    public const KIND_POSTGRES = 'postgres';
    public const KIND_MYSQL = 'mysql';
    public const KIND_SQLITE = 'sqlite';

    /**
     * Strip PostgreSQL's leading product name and NOTHING else; leave every other family verbatim.
     *
     * Minimal by design: `'17.10 (Debian 17.10-1.pgdg13+1)'` is measured to parse fine, so there is
     * no reason to extract a bare `major.minor` and every reason not to (each extra rule is another
     * chance to discard a suffix that turns out to be load-bearing, which is exactly what the
     * MariaDB case is).
     */
    public static function normalise(string $kind, string $raw): string
    {
        if ($kind !== self::KIND_POSTGRES) {
            return $raw;
        }
        return preg_replace('/^\s*PostgreSQL\s+/i', '', $raw) ?? $raw;
    }

    /** @throws BackendFamilyUnknown */
    public static function platformFor(string $kind, string $rawVersion): AbstractPlatform
    {
        $provider = new FixedVersion(self::normalise($kind, $rawVersion));
        return match ($kind) {
            self::KIND_POSTGRES => self::postgres()->getDatabasePlatform($provider),
            self::KIND_MYSQL => self::mysql()->getDatabasePlatform($provider),
            self::KIND_SQLITE => self::sqlite()->getDatabasePlatform($provider),
            default => throw BackendFamilyUnknown::forKind($kind),
        };
    }

    /**
     * Derive the family from a version string alone — the ONLY option on the
     * platform-before-connect path (`Doctrine\DBAL\Connection::getDatabasePlatform()` builds a
     * static provider from `$params['serverVersion']` and never asks the driver connection).
     * Returns null when the string names no family; the caller must then FAIL, never guess.
     *
     * **Two of the three families are unidentifiable from a version string alone, and SQLite is not
     * a new case.** MySQL answers a bare `8.4.11` and SQLite a bare `3.53.2`; neither names itself,
     * and no pattern separates them that is not a guess (`3.53.2` and `8.4.11` have the same shape).
     * So both take the null arm and the caller fails loudly. The practical rule is the same one that
     * has always applied to a MySQL pool: **do not set `serverVersion` in the connection params** —
     * it short-circuits the handshake that is the only authoritative source of the family
     * (`PoolInfo.kind`). Only PostgreSQL, whose banner names the product, survives that
     * short-circuit.
     */
    public static function familyFromVersion(string $version): ?string
    {
        if (stripos($version, 'postgres') !== false) {
            return self::KIND_POSTGRES;
        }
        if (stripos($version, 'mariadb') !== false || stripos($version, 'mysql') !== false) {
            return self::KIND_MYSQL;
        }
        return null;
    }

    private static function postgres(): AbstractPostgreSQLDriver
    {
        return new class extends AbstractPostgreSQLDriver {
            /** @param array<string,mixed> $params */
            public function connect(#[\SensitiveParameter] array $params): DriverConnection
            {
                throw new \LogicException('platform-only delegate: this driver never connects');
            }
        };
    }

    /**
     * SQLite has exactly one platform and `AbstractSQLiteDriver::getDatabasePlatform()` IGNORES the
     * version provider entirely (measured on 4.4.4: the body is `return new SQLitePlatform();`).
     * The delegate is kept anyway rather than constructing `SQLitePlatform` here, for the same
     * reason as the other two: if a future DBAL grows a version ladder for SQLite, this inherits it
     * instead of silently continuing to return the base platform.
     */
    private static function sqlite(): AbstractSQLiteDriver
    {
        return new class extends AbstractSQLiteDriver {
            /** @param array<string,mixed> $params */
            public function connect(#[\SensitiveParameter] array $params): DriverConnection
            {
                throw new \LogicException('platform-only delegate: this driver never connects');
            }
        };
    }

    private static function mysql(): AbstractMySQLDriver
    {
        return new class extends AbstractMySQLDriver {
            /** @param array<string,mixed> $params */
            public function connect(#[\SensitiveParameter] array $params): DriverConnection
            {
                throw new \LogicException('platform-only delegate: this driver never connects');
            }
        };
    }
}
