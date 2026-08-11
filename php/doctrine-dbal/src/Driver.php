<?php // /php/doctrine-dbal/src/Driver.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\ServerVersionProvider;
use Ferro\Client\Error\FerroException;
use Ferro\Client\RetryPolicy;
use Ferro\DBAL\Exception\BackendFamilyUnknown;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\Value\DbalValuePolicy;
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

    /**
     * **EVERY failure below leaves as a `Doctrine\DBAL\Driver\Exception`**, which is what the two
     * `catch` arms are for and is not tidiness. `Doctrine\DBAL\Connection::connect()` catches exactly
     * `Driver\Exception` (`vendor/doctrine/dbal/src/Connection.php:222`) and converts it; anything
     * else escapes DBAL's conversion entirely and reaches the application past every
     * `catch (Doctrine\DBAL\Exception)` it, its framework bundle, its ORM, its Messenger transport,
     * its health check and `doctrine:migrations` have. The whole-branch review MEASURED the escape:
     * with no ferrod on the socket, `Ferro\Client\Error\TransportException` —
     * `instanceof Doctrine\DBAL\Driver\Exception: NO`. A drop-in replacement whose failure mode is
     * uncatchable is not drop-in during exactly the window that matters (SPEC §18 socket activation,
     * §19.1 boot_epoch storms — a restart is an EXPECTED event, not an exceptional one).
     *
     * The two arms carry different fates, deliberately:
     *  * `FerroException` → {@see DriverException::connectFailed}, which classifies a transport
     *    failure `Retryable` (nothing can have been applied on a session that never opened) and a
     *    handshake rejection as fatal-and-branchless.
     *  * `\InvalidArgumentException` → a LOCAL driver exception. `DriverOptions` throws it for a
     *    malformed `driverOptions` value, and a configuration mistake is deterministic: it carries no
     *    §9.2 branch precisely so no retry helper picks it up. It is converted HERE rather than
     *    inside `DriverOptions` so that class stays a pure typed read of `$params` — the DBAL
     *    contract applies at the boundary, which is this method.
     *
     * @param array<string,mixed> $params
     */
    public function connect(#[\SensitiveParameter] array $params): Connection
    {
        try {
            $o = DriverOptions::fromParams($params);
        } catch (\InvalidArgumentException $e) {
            throw DriverException::local($e->getMessage(), $e);
        }

        // RetryPolicy::none() is deliberate and is what `Ferro\Client\Connection::begin()`'s own
        // docblock tells a driver to use: DBAL (or the application above it) owns the retry
        // decision, and the client's autocommit read-retry must not double up with it.
        // The value policy is the driver's TYPE BOUNDARY: canonical wire text for the tags DBAL
        // parses correctly, a per-family re-render for TIMESTAMPTZ (which it cannot parse at all),
        // and a loud refusal for the values it would parse into something ELSE.
        $policy = new DbalValuePolicy();
        try {
            $ferro = $o->socketPath !== null
                ? Ferro::connect($o->socketPath, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, $policy)
                : Ferro::connectTcp((string) $o->host, $o->port, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, $policy);
        } catch (FerroException $e) {
            // The transport TARGET, never `$params`: §12 keeps every credential in the engine, and
            // the socket path (or host:port) plus the pool name is exactly what an operator needs.
            throw DriverException::connectFailed($e, sprintf(
                'Ferro: could not connect to the engine at %s (pool "%s")',
                $o->socketPath ?? ((string) $o->host . ':' . $o->port),
                $o->pool,
            ));
        }

        $info = $ferro->poolInfo();
        if ($info === null) {
            throw DriverException::local(sprintf(
                'Ferro: the engine does not advertise a pool named "%s". Configured pools come from '
                . 'ferrod\'s FERRO_POOLS; check `driverOptions.pool`.',
                $o->pool,
            ));
        }
        // The family is only knowable AFTER the handshake, and the policy is a CONSTRUCTOR argument
        // of the connection — hence the two-step wiring. Nothing has decoded a cell yet: HELLO_ACK
        // carries no TypedValues, and no user statement can have run.
        $policy->bindBackend($info->kind);
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

    /**
     * The family is the one learned at the last {@see connect}. Before any connect there is nothing
     * to convert yet — Doctrine only asks for the converter when a driver exception has already
     * been raised, which requires a connection — so PostgreSQL's table is a harmless default here
     * and, unlike a PLATFORM, choosing it wrongly cannot change any SQL that is emitted.
     */
    public function getExceptionConverter(): ExceptionConverterInterface
    {
        return new ExceptionConverter($this->kind ?? PlatformVersion::KIND_POSTGRES);
    }

    /** The backend family learned at the last {@see connect}, or null. */
    public function kind(): ?string
    {
        return $this->kind;
    }
}
