<?php // /php/doctrine-dbal/src/Exception/ServerVersionUnavailable.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;

/**
 * The engine could not tell us the backend's version, and Doctrine needs one to choose a PLATFORM —
 * i.e. to choose which SQL dialect every subsequent statement is written in.
 *
 * SPEC §14: on a `nil` `server_version` the driver must "fail loudly or defer resolution, never
 * silently fall back to a default platform". This is the end of the deferral: the handshake did not
 * advertise a version and asking the backend directly did not work either.
 */
final class ServerVersionUnavailable extends AbstractException
{
    public static function forPool(string $pool, string $kind, ?\Throwable $previous): self
    {
        return new self(
            sprintf(
                'Ferro: the server version for pool "%s" is unknown, so no Doctrine platform can be '
                . 'chosen. The backend FAMILY is known ("%s") — only the version within it is not, '
                . 'and on MySQL-family pools that is what distinguishes MariaDB from MySQL. This is '
                . 'a NORMAL, transient state and not necessarily a fault: the engine learns the version '
                . 'lazily and caches it with a TTL, so a cache expiry racing a re-probe, a probe '
                . 'failure inside its retry backoff, or a backend that is currently unreachable all '
                . 'produce it — retrying later may simply succeed. For a deterministic fix, set the '
                . '`serverVersion` connection parameter (e.g. \'serverVersion\' => \'17.10\' or '
                . '\'11.8.8-MariaDB\'), which Doctrine uses instead of asking the connection at all. '
                . 'No platform is guessed here, because a wrong platform is a wrong SQL dialect for '
                . 'every statement that follows.',
                $pool,
                $kind,
            ),
            null,
            0,
            $previous,
        );
    }
}
