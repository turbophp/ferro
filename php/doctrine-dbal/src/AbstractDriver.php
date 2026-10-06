<?php // /php/doctrine-dbal/src/AbstractDriver.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Ferro\Client\Connection as FerroClient;
use Ferro\Client\RetryPolicy;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\Value\DbalValuePolicy;
use Ferro\Ferro;

/**
 * Everything the two `driverClass`es share: opening a Ferro session, learning the pool's family
 * from the handshake, and the exception converter. {@see Driver} (DBAL 4) and
 * {@see \Ferro\DBAL\Dbal3\Driver} (DBAL 3) add only the platform methods, whose names and
 * signatures differ between majors (M2-C5, SPEC §22.2 (by)).
 */
abstract class AbstractDriver
{
    /** The backend family of the LAST pool this driver connected to, or null before any connect. */
    private ?string $kind = null;

    /**
     * Open the session and learn the family. Returns what each major's `connect()` needs to build
     * ITS connection class.
     *
     * @param array<string,mixed> $params
     * @return array{FerroClient, DriverOptions, string}
     */
    protected function open(#[\SensitiveParameter] array $params): array
    {
        $o = DriverOptions::fromParams($params);
        // RetryPolicy::none() is deliberate and is what `Ferro\Client\Connection::begin()`'s own
        // docblock tells a driver to use: DBAL (or the application above it) owns the retry
        // decision, and the client's autocommit read-retry must not double up with it.
        // The value policy is the driver's TYPE BOUNDARY: canonical wire text for the tags DBAL
        // parses correctly, a per-family re-render for TIMESTAMPTZ (which it cannot parse at all),
        // and a loud refusal for the values it would parse into something ELSE.
        $policy = new DbalValuePolicy();
        $ferro = $o->socketPath !== null
            ? Ferro::connect($o->socketPath, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, $policy, receiveFds: $o->receiveFds)
            : Ferro::connectTcp((string) $o->host, $o->port, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, $policy);

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
        return [$ferro, $o, $info->kind];
    }

    /**
     * The family is the one learned at the last {@see open}. Before any connect there is nothing
     * to convert yet — Doctrine only asks for the converter when a driver exception has already
     * been raised, which requires a connection — so PostgreSQL's table is a harmless default here
     * and, unlike a PLATFORM, choosing it wrongly cannot change any SQL that is emitted.
     */
    public function getExceptionConverter(): ExceptionConverterInterface
    {
        return new ExceptionConverter($this->kind ?? PlatformVersion::KIND_POSTGRES);
    }

    /** The backend family learned at the last connect, or null. */
    public function kind(): ?string
    {
        return $this->kind;
    }
}
