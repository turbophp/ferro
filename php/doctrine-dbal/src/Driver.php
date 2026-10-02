<?php // /php/doctrine-dbal/src/Driver.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\ServerVersionProvider;
use Ferro\DBAL\Exception\BackendFamilyUnknown;

/**
 * The `ferro/doctrine-dbal-driver` entry point for **DBAL 4**. Configure it with `driverClass`:
 *
 * ```php
 * 'connections' => ['default' => [
 *     'driverClass'   => Ferro\DBAL\Driver::class,
 *     'unix_socket'   => '/run/ferro/app.sock',
 *     'driverOptions' => ['pool' => 'main'],
 * ]],
 * ```
 *
 * On **DBAL 3** configure {@see \Ferro\DBAL\Dbal3\Driver} instead. One class cannot serve both:
 * DBAL 3 only connects before choosing a platform for a driver implementing
 * `VersionAwarePlatformDriver`, an interface DBAL 4 deleted, and PHP rejects a class naming an
 * interface that does not exist (SPEC §14, §22.2 (by)).
 *
 * `DriverManager::createDriver()` does `return new $driverClass();`, so this class MUST have a
 * no-argument constructor and everything arrives through `$params`.
 */
final class Driver extends AbstractDriver implements DriverInterface
{
    /** @param array<string,mixed> $params */
    public function connect(#[\SensitiveParameter] array $params): Connection
    {
        [$ferro, $o, $kind] = $this->open($params);
        return new Connection($ferro, $o->pool, $kind, $o->readonly);
    }

    public function getDatabasePlatform(ServerVersionProvider $versionProvider): AbstractPlatform
    {
        $version = $versionProvider->getServerVersion();
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
     * (family, raw `version()` string) → a STOCK DBAL 4 platform, chosen by DBAL's own abstract
     * driver for that family ({@see PlatformVersion::delegateFor}) after the one transform it cannot
     * do itself ({@see PlatformVersion::normalise}).
     *
     * @throws BackendFamilyUnknown
     */
    public static function platformFor(string $kind, string $rawVersion): AbstractPlatform
    {
        return PlatformVersion::delegateFor($kind)
            ->getDatabasePlatform(new FixedVersion(PlatformVersion::normalise($kind, $rawVersion)));
    }
}
