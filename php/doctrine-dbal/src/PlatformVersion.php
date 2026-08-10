<?php // /php/doctrine-dbal/src/PlatformVersion.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\AbstractMySQLDriver;
use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;
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
 *
 * Charter rule 6 is intact: no platform is subclassed, no SQL is generated here. We select.
 */
final class PlatformVersion
{
    /** The `PoolInfo.kind` wire values (`PoolKind::wire_name()` in `ferrod`). Never nil. */
    public const KIND_POSTGRES = 'postgres';
    public const KIND_MYSQL = 'mysql';

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
            default => throw BackendFamilyUnknown::forKind($kind),
        };
    }

    /**
     * Derive the family from a version string alone — the ONLY option on the
     * platform-before-connect path (`Doctrine\DBAL\Connection::getDatabasePlatform()` builds a
     * static provider from `$params['serverVersion']` and never asks the driver connection).
     * Returns null when the string names no family; the caller must then FAIL, never guess.
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
