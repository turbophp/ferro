<?php // /php/doctrine-dbal/src/Driver.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Driver\API\PostgreSQL\ExceptionConverter as PostgreSQLExceptionConverter;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\ServerVersionProvider;
use Ferro\Client\RetryPolicy;
use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\DBAL\Exception\BackendFamilyUnknown;
use Ferro\DBAL\Exception\DriverException;
use Ferro\Ferro;

/**
 * The `ferro/doctrine-dbal-driver` entry point. Configure it with `driverClass`:
 *
 * ```php
 * 'connections' => ['default' => [
 *     'driverClass'   => Ferro\DBAL\Driver::class,
 *     'unix_socket'   => '/run/ferro/app.sock',
 *     'driverOptions' => ['pool' => 'main'],
 * ]],
 * ```
 *
 * `DriverManager::createDriver()` does `return new $driverClass();`, so this class MUST have a
 * no-argument constructor and everything arrives through `$params`.
 */
final class Driver implements DriverInterface
{
    /** The backend family of the LAST pool this driver connected to, or null before any connect. */
    private ?string $kind = null;

    /** @param array<string,mixed> $params */
    public function connect(#[\SensitiveParameter] array $params): Connection
    {
        $o = DriverOptions::fromParams($params);
        // RetryPolicy::none() is deliberate and is what `Ferro\Client\Connection::begin()`'s own
        // docblock tells a driver to use: DBAL (or the application above it) owns the retry
        // decision, and the client's autocommit read-retry must not double up with it.
        // RawStringValuePolicy hands up the canonical wire text verbatim — the driver-native shape
        // a DBAL type layer expects. Task 9 replaces it with the DBAL-specific policy.
        $ferro = $o->socketPath !== null
            ? Ferro::connect($o->socketPath, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, new RawStringValuePolicy())
            : Ferro::connectTcp((string) $o->host, $o->port, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, new RawStringValuePolicy());

        $info = $ferro->poolInfo();
        if ($info === null) {
            throw DriverException::local(sprintf(
                'Ferro: the engine does not advertise a pool named "%s". Configured pools come from '
                . 'ferrod\'s FERRO_POOLS; check `driverOptions.pool`.',
                $o->pool,
            ));
        }
        $this->kind = $info->kind;
        return new Connection($ferro, $o->pool, $info->kind, $o->readonly);
    }

    public function getDatabasePlatform(ServerVersionProvider $versionProvider): AbstractPlatform
    {
        $version = $versionProvider->getServerVersion();
        // The family the handshake told us, when we have one. Otherwise this is the
        // platform-before-connect path (`$params['serverVersion']` short-circuits the connection
        // entirely), where the version string is the only signal there is.
        $kind = $this->kind ?? PlatformVersion::familyFromVersion($version);
        if ($kind === null) {
            throw BackendFamilyUnknown::beforeConnect($version);
        }
        return PlatformVersion::platformFor($kind, $version);
    }

    public function getExceptionConverter(): ExceptionConverterInterface
    {
        // Task 11 replaces this with Ferro\DBAL\ExceptionConverter, which intercepts the §9.2
        // Indeterminate branch and then delegates to the STOCK per-family converter.
        return new PostgreSQLExceptionConverter();
    }

    /** The backend family learned at the last {@see connect}, or null. */
    public function kind(): ?string
    {
        return $this->kind;
    }
}
