<?php // /php/doctrine-dbal/src/Dbal3/Driver.php
declare(strict_types=1);
namespace Ferro\DBAL\Dbal3;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\VersionAwarePlatformDriver;
use Ferro\DBAL\AbstractDriver;
use Ferro\DBAL\Exception\BackendFamilyUnknown;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\PlatformVersion;

/**
 * The `ferro/doctrine-dbal-driver` entry point for **DBAL 3** (`^3.8`, SPEC §21 D2's M2 bridge):
 *
 * ```php
 * 'connections' => ['default' => [
 *     'driverClass'   => Ferro\DBAL\Dbal3\Driver::class,
 *     'unix_socket'   => '/run/ferro/app.sock',
 *     'driverOptions' => ['pool' => 'main'],
 * ]],
 * ```
 *
 * The same engine, client, connection core, binder, value policy and exception converter as the
 * DBAL 4 {@see \Ferro\DBAL\Driver}; only the SPI surface differs (SPEC §22.2 (by)).
 *
 * **Why it implements `VersionAwarePlatformDriver`, and why that forces a second class.** DBAL 3's
 * `Connection::detectDatabasePlatform()` connects to learn the server version ONLY for a driver
 * implementing that interface; for any other driver it calls `getDatabasePlatform()` with no
 * version and no connection, which for Ferro would mean choosing a platform — a SQL dialect —
 * without knowing even the family. DBAL 4 deleted the interface, and PHP rejects a class that
 * names a missing interface when the class is declared, so the two majors cannot share a
 * `driverClass`.
 */
final class Driver extends AbstractDriver implements VersionAwarePlatformDriver
{
    /** @param array<string,mixed> $params */
    public function connect(#[\SensitiveParameter] array $params): Connection
    {
        [$ferro, $o, $kind] = $this->open($params);
        return new Connection($ferro, $o->pool, $kind, $o->readonly);
    }

    /**
     * DBAL 3 asks a version-aware driver for a platform through
     * {@see createDatabasePlatformForVersion}, and its own `Connection` reaches THIS method only when
     * it could not learn a version — which for this driver cannot happen, because
     * `Connection::getServerVersion()` either resolves one or throws (the §14 nil-version decision).
     *
     * **It is nevertheless CALLED in the commonest DBAL 3 deployment:** DoctrineBundle 2's
     * `ConnectionFactory` asks the driver for a version-less platform, before anything has
     * connected, to choose a default `charset` (and, for the MySQL family, a default table
     * collation) whenever the connection configures no `charset` — and always when it configures
     * `dbname_suffix`. Before a connect the driver does not know even the FAMILY, so any answer here
     * would be a guess of the dialect; it REFUSES, where the stock drivers answer their family's
     * oldest platform, and the message names the configuration that avoids the call: a `charset`
     * (inert for Ferro, whose connection charset belongs to the engine's pool) and no
     * `dbname_suffix` (Ferro has no client-side database name to suffix). The DBAL 4 driver's
     * `BackendFamilyUnknown` gives DoctrineBundle's DBAL 4 call the same advice (M2-C5 review F11).
     *
     * **After a connect the refusal is a POLICY, not a necessity** (M2-C5b review F3): the family is
     * known then, and the stock answer — the family's OLDEST platform — would be easy to give. It is
     * still refused, because it is not the platform the connection itself uses (that one is chosen
     * from the server version), and handing a caller a second, older dialect for the same database
     * is the silently-wrong-dialect class this driver exists not to produce. Upstream's
     * `testDispatchEventWhenDatabasePlatformIsExplicitlyPassed` makes exactly this call and is
     * recorded as refused by design (SPEC §22.2 (bz)).
     */
    public function getDatabasePlatform(): AbstractPlatform
    {
        throw DriverException::local(
            'Ferro: a DBAL 3 platform needs the server version, and this call carries none — before a '
            . 'connection opens the driver does not know even the backend family, and after one the '
            . 'version-less answer would be an older dialect than the connection uses, so it will not '
            . 'guess a SQL dialect. If this is Symfony\'s DoctrineBundle (its ConnectionFactory asks for a '
            . 'platform to choose a default charset), set `charset` on the connection — it is inert '
            . 'for Ferro — and remove `dbname_suffix`. Otherwise let Doctrine choose the platform: '
            . 'Connection::getDatabasePlatform() connects and asks the engine.',
        );
    }

    /** @param string $version */
    public function createDatabasePlatformForVersion($version): AbstractPlatform
    {
        // The family the handshake told us, when we have one. Otherwise this is the
        // platform-before-connect path (`$params['serverVersion']` short-circuits the connection
        // entirely), where the version string is the only signal there is.
        $kind = $this->kind() ?? PlatformVersion::familyFromVersion($version);
        if ($kind === null) {
            throw BackendFamilyUnknown::beforeConnect($version);
        }
        return self::platformFor($kind, $version);
    }

    /**
     * (family, raw `version()` string) → a STOCK DBAL 3 platform, chosen by DBAL 3's own abstract
     * driver for that family — the same delegation as DBAL 4's, asked DBAL 3's way.
     *
     * SQLite's DBAL 3 driver is not version-aware (there is one SQLite platform), so it is asked for
     * its platform the version-less way; that is the stock driver's own answer, not a fallback.
     *
     * @throws BackendFamilyUnknown
     */
    public static function platformFor(string $kind, string $rawVersion): AbstractPlatform
    {
        $delegate = PlatformVersion::delegateFor($kind);
        if ($delegate instanceof VersionAwarePlatformDriver) {
            return $delegate->createDatabasePlatformForVersion(PlatformVersion::normalise($kind, $rawVersion));
        }
        return $delegate->getDatabasePlatform();
    }

    /**
     * Required by DBAL 3's `Driver` interface, and on its default path: DBAL 3's
     * `LegacySchemaManagerFactory` builds every schema manager through it. Answered by the PLATFORM
     * — `AbstractPlatform::createSchemaManager()`, which is exactly what DBAL 3's
     * `DefaultSchemaManagerFactory` does — so the schema manager is the stock one for whichever
     * platform Doctrine chose, and this driver needs no family of its own to pick it (charter
     * rule 6: the schema managers stay stock).
     *
     * @return AbstractSchemaManager<AbstractPlatform>
     */
    public function getSchemaManager(DbalConnection $conn, AbstractPlatform $platform): AbstractSchemaManager
    {
        return $platform->createSchemaManager($conn);
    }
}
