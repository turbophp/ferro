<?php // /php/doctrine-dbal/tests/Dbal3/Dbal3DriverTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Dbal3;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Platforms\MariaDb1060Platform;
use Doctrine\DBAL\Platforms\MySQL80Platform;
use Doctrine\DBAL\Platforms\PostgreSQL100Platform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Doctrine\DBAL\Schema\PostgreSQLSchemaManager;
use Doctrine\DBAL\Schema\SqliteSchemaManager;
use Doctrine\DBAL\VersionAwarePlatformDriver;
use Ferro\DBAL\Dbal3\Driver;
use Ferro\DBAL\Exception\BackendFamilyUnknown;
use Ferro\DBAL\PlatformVersion;
use PHPUnit\Framework\TestCase;

/**
 * M2-C5 — the DBAL 3 `driverClass`'s platform half, which is the part DBAL 3 asks for differently
 * (`createDatabasePlatformForVersion(string)` on a `VersionAwarePlatformDriver`, rather than DBAL 4's
 * `getDatabasePlatform(ServerVersionProvider)`). The live version strings are the same ones the
 * DBAL 4 lane's `PlatformVersionTest` pins, so the two majors are held to the same inputs.
 *
 * **The expected classes are the OLDEST ones that still prove the version reached DBAL's ladder**,
 * because this file runs against the `^3.8` FLOOR (3.8.0, in CI) as well as the locked 3.10.x, and
 * the newest platform differs between them (3.10 adds a PostgreSQL 12 and a MySQL 8.4 platform that
 * 3.8.0 does not have). Each is still a real discriminator: the version-LESS answer — what a driver
 * that lost the version would get — is the family's base (`PostgreSQL94Platform`, `MySQLPlatform`),
 * which is an instance of none of them.
 */
final class Dbal3DriverTest extends TestCase
{
    private const PG_LIVE = 'PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on x86_64-pc-linux-gnu, '
        . 'compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit';
    private const MYSQL_LIVE = '8.4.11';
    private const MARIADB_LIVE = '11.8.8-MariaDB-ubu2404';
    private const SQLITE_LIVE = '3.53.2';

    /**
     * The interface IS the reason for the class: without it DBAL 3 never connects before choosing a
     * platform. A refactor that dropped it would still compile and still pass every per-method test
     * below — DBAL 3 would simply stop calling them.
     */
    public function testItIsVersionAwareSoDbal3ConnectsBeforeChoosingAPlatform(): void
    {
        self::assertInstanceOf(VersionAwarePlatformDriver::class, new Driver());
    }

    public function testTheLivePostgresBannerSelectsTheModernPostgresPlatform(): void
    {
        // The stock DBAL 3 parser is anchored on a leading digit, exactly as DBAL 4's is: the raw
        // banner would throw InvalidPlatformVersion without the shared normalisation.
        self::assertInstanceOf(PostgreSQL100Platform::class, Driver::platformFor(PlatformVersion::KIND_POSTGRES, self::PG_LIVE));
    }

    public function testMysqlAndMariadbSelectDifferentPlatforms(): void
    {
        self::assertInstanceOf(MySQL80Platform::class, Driver::platformFor(PlatformVersion::KIND_MYSQL, self::MYSQL_LIVE));
        $maria = Driver::platformFor(PlatformVersion::KIND_MYSQL, self::MARIADB_LIVE);
        self::assertInstanceOf(MariaDb1060Platform::class, $maria);
        self::assertNotInstanceOf(MySQL80Platform::class, $maria,
            'the MariaDB suffix must survive to DBAL 3\'s detector too, or MariaDB gets MySQL\'s grammar');
    }

    /** SQLite's DBAL 3 driver is not version-aware; the delegate is asked the version-less way. */
    public function testSqliteSelectsTheSqlitePlatform(): void
    {
        self::assertInstanceOf(SqlitePlatform::class, Driver::platformFor(PlatformVersion::KIND_SQLITE, self::SQLITE_LIVE));
    }

    /**
     * The platform-before-connect path (`serverVersion` in the params): only PostgreSQL's banner
     * names its family, so a bare MySQL version cannot be served and must fail rather than guess.
     */
    public function testBeforeConnectOnlyASelfNamingVersionIsServed(): void
    {
        self::assertInstanceOf(PostgreSQL100Platform::class, (new Driver())->createDatabasePlatformForVersion(self::PG_LIVE));
        $this->expectException(BackendFamilyUnknown::class);
        (new Driver())->createDatabasePlatformForVersion(self::MYSQL_LIVE);
    }

    /**
     * Reached only by a direct call (DBAL 3 always has a version for a version-aware driver). The
     * stock drivers answer their family's OLDEST platform here; Ferro refuses rather than choose a
     * dialect blind.
     */
    public function testTheVersionlessPlatformIsRefused(): void
    {
        $this->expectException(\Doctrine\DBAL\Driver\Exception::class);
        $this->expectExceptionMessage('needs the server version');
        (new Driver())->getDatabasePlatform();
    }

    /**
     * DBAL 3's DEFAULT schema-manager factory (`LegacySchemaManagerFactory`) builds every schema
     * manager through the driver, so this is on the hot path of every migration. It must be the
     * STOCK manager for the platform Doctrine chose.
     */
    public function testTheSchemaManagerIsTheStockOneForThePlatform(): void
    {
        $driver = new Driver();
        $conn = new DbalConnection([], $driver);
        self::assertInstanceOf(PostgreSQLSchemaManager::class, $driver->getSchemaManager($conn, new PostgreSQL100Platform()));
        self::assertInstanceOf(SqliteSchemaManager::class, $driver->getSchemaManager($conn, new SqlitePlatform()));
    }

    /**
     * The handshake's FAMILY wins over the version string (M2-C5 review F10: only a MySQL-gated live
     * test killed a mutation that ignored it). A bare `8.4.11` names no family, so without the
     * handshake this throws `BackendFamilyUnknown` — which is what the mutation produces.
     */
    public function testTheHandshakeFamilyDecidesWhenTheDriverHasConnected(): void
    {
        $driver = new Driver();
        $kind = new \ReflectionProperty(\Ferro\DBAL\AbstractDriver::class, 'kind');
        $kind->setValue($driver, PlatformVersion::KIND_MYSQL);
        self::assertInstanceOf(MySQL80Platform::class, $driver->createDatabasePlatformForVersion(self::MYSQL_LIVE));
        $kind->setValue($driver, PlatformVersion::KIND_SQLITE);
        self::assertInstanceOf(SqlitePlatform::class, $driver->createDatabasePlatformForVersion(self::SQLITE_LIVE));
    }

    /**
     * `TemporalFormat`'s per-family `TIMESTAMPTZ` rendering is DBAL 3's too: it must match DBAL 3's
     * own platforms' `getDateTimeTzFormatString()`, or DBAL 3's `DateTimeTzType` would fail to parse
     * what the driver hands it.
     */
    public function testTheTemporalFormatMatchesDbal3sPlatforms(): void
    {
        self::assertSame((new PostgreSQL100Platform())->getDateTimeTzFormatString(), \Ferro\DBAL\Value\TemporalFormat::forKind(PlatformVersion::KIND_POSTGRES)->dateTimeTz);
        self::assertSame((new MySQL80Platform())->getDateTimeTzFormatString(), \Ferro\DBAL\Value\TemporalFormat::forKind(PlatformVersion::KIND_MYSQL)->dateTimeTz);
        self::assertSame((new MariaDb1060Platform())->getDateTimeTzFormatString(), \Ferro\DBAL\Value\TemporalFormat::forKind(PlatformVersion::KIND_MYSQL)->dateTimeTz);
        self::assertSame((new SqlitePlatform())->getDateTimeTzFormatString(), \Ferro\DBAL\Value\TemporalFormat::forKind(PlatformVersion::KIND_SQLITE)->dateTimeTz);
    }
}
