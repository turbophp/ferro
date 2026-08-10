<?php // /php/doctrine-dbal/tests/Unit/PlatformVersionTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Platforms\MariaDB110700Platform;
use Doctrine\DBAL\Platforms\MySQL84Platform;
use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Ferro\DBAL\Exception\BackendFamilyUnknown;
use Ferro\DBAL\PlatformVersion;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 5 — the platform fork, with the two OPPOSITE requirements that make a single uniform
 * "version normaliser" ship a wrong SQL dialect.
 *
 * MEASURED against the live containers and fed through the STOCK abstract drivers:
 *   pg      `PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on x86_64-…`  -> THROWS InvalidPlatformVersion
 *   pg      `17.10 (Debian 17.10-1.pgdg13+1)`                          -> PostgreSQL120Platform
 *   mysql   `8.4.11`                                                   -> MySQL84Platform
 *   mariadb `11.8.8-MariaDB-ubu2404`                                   -> MariaDB110700Platform
 *   mariadb `11.8.8`   (i.e. the suffix stripped)                      -> MySQL84Platform  *** WRONG ***
 *
 * So normalisation is MANDATORY on the PG path (the stock regex is anchored at `^` and our cached
 * string starts with the literal word "PostgreSQL") and FORBIDDEN on the MySQL path (MariaDB is
 * detected ONLY by `stripos($version, 'mariadb')`). The literal strings below are the ones ferrod
 * actually caches — `SELECT version()`, verbatim and unnormalised.
 */
final class PlatformVersionTest extends TestCase
{
    private const PG_LIVE = 'PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on x86_64-pc-linux-gnu, '
        . 'compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit';
    private const MYSQL_LIVE = '8.4.11';
    private const MARIADB_LIVE = '11.8.8-MariaDB-ubu2404';

    public function testTheLivePostgresStringSelectsThePostgresPlatform(): void
    {
        self::assertInstanceOf(
            PostgreSQL120Platform::class,
            PlatformVersion::platformFor(PlatformVersion::KIND_POSTGRES, self::PG_LIVE),
        );
    }

    public function testTheLiveMysqlAndMariadbStringsSelectDIFFERENTPlatforms(): void
    {
        self::assertInstanceOf(
            MySQL84Platform::class,
            PlatformVersion::platformFor(PlatformVersion::KIND_MYSQL, self::MYSQL_LIVE),
        );
        $maria = PlatformVersion::platformFor(PlatformVersion::KIND_MYSQL, self::MARIADB_LIVE);
        self::assertInstanceOf(MariaDB110700Platform::class, $maria);
        self::assertNotInstanceOf(
            MySQL84Platform::class,
            $maria,
            'stripping "-MariaDB" would silently select the MySQL dialect for a MariaDB server',
        );
    }

    /** The normaliser must touch the PG string and leave the MySQL-family string BYTE-IDENTICAL. */
    public function testNormalisationIsPostgresOnly(): void
    {
        self::assertStringStartsWith(
            '17.10',
            PlatformVersion::normalise(PlatformVersion::KIND_POSTGRES, self::PG_LIVE),
        );
        self::assertSame(
            self::MARIADB_LIVE,
            PlatformVersion::normalise(PlatformVersion::KIND_MYSQL, self::MARIADB_LIVE),
            'the MySQL-family string is load-bearing and must pass through verbatim',
        );
        self::assertSame(
            self::MYSQL_LIVE,
            PlatformVersion::normalise(PlatformVersion::KIND_MYSQL, self::MYSQL_LIVE),
        );
    }

    /** An unknown family is a LOUD failure, never a default platform (SPEC §14). */
    public function testAnUnknownFamilyThrows(): void
    {
        $this->expectException(BackendFamilyUnknown::class);
        PlatformVersion::platformFor('sqlite', '3.45');
    }

    /**
     * ADDED (beyond the plan) — the platform-before-connect fork, which `Driver::getDatabasePlatform()`
     * reaches whenever `$params['serverVersion']` short-circuits the connection entirely (hazard 15).
     * Without this, `familyFromVersion()` is only ever exercised through the live smoke's PG leg, so
     * a body that answered `KIND_POSTGRES` for everything would be invisible offline — and would
     * emit PostgreSQL grammar at a MariaDB server.
     */
    public function testTheFamilyIsDerivedFromAVersionStringOnlyWhenTheStringNamesOne(): void
    {
        self::assertSame(PlatformVersion::KIND_POSTGRES, PlatformVersion::familyFromVersion(self::PG_LIVE));
        self::assertSame(PlatformVersion::KIND_MYSQL, PlatformVersion::familyFromVersion(self::MARIADB_LIVE));
        self::assertSame(PlatformVersion::KIND_MYSQL, PlatformVersion::familyFromVersion('8.4.11-MySQL'));
        // The mirror: a bare number names NO family, and guessing one is the failure mode §14 bans.
        // `8.4.11` is a real MySQL answer AND a plausible PostgreSQL 8.4 answer — which is exactly
        // why it must be null rather than either family.
        self::assertNull(PlatformVersion::familyFromVersion(self::MYSQL_LIVE));
        self::assertNull(PlatformVersion::familyFromVersion('3.45'));
    }
}
