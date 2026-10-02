<?php // /php/client/src/Client/BackupResult.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * The outcome of a successful {@see Connection::backup}: the published snapshot's size, and SPEC
 * §13's split of where the time went. Deliberately carries NO path — the engine never sends the
 * directory the snapshot lives in (SPEC §12/D8 keep the database's location out of PHP's reach); the
 * operator who configured `FERRO_POOL_<NAME>_ALLOW_DIR` knows it, the application does not need to.
 */
final class BackupResult
{
    public function __construct(
        /** The snapshot's size in bytes. */
        public readonly int $bytes,
        /** Microseconds waiting for a pooled connection. */
        public readonly int $queueUs,
        /** Microseconds the snapshot statement itself took. */
        public readonly int $execUs,
    ) {
    }
}
