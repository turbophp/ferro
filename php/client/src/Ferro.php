<?php // /php/client/src/Ferro.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Connection;
use Ferro\Client\Session;
use Ferro\Client\Transport;

/**
 * The M0 entry-point facade: open a transport, run the HELLO handshake, and hand back a
 * {@see Connection} bound to a pool. The config-only drop-in tiers (Doctrine/Eloquent, M1) build on
 * this — it is deliberately thin (no config object yet; that grows in later slices).
 */
final class Ferro
{
    /**
     * Connect to a `ferrod` over its Unix domain socket, complete the handshake, and return a
     * ready {@see Connection}.
     *
     * @param string $socketPath the UDS path (e.g. `/run/ferro/{schema_hash}.sock`).
     * @param string $pool the pool name to bind requests to (must be advertised in HELLO_ACK).
     */
    public static function connect(
        string $socketPath,
        string $pool = 'default',
        float $connectTimeout = 2.0,
        float $ioTimeout = 5.0,
    ): Connection {
        $session = new Session(Transport::connectUnix($socketPath, $connectTimeout, $ioTimeout));
        $session->hello();
        return new Connection($session, $pool);
    }

    /**
     * Connect over TCP (the `FERRO_ADDR` fallback) instead of a Unix socket.
     */
    public static function connectTcp(
        string $host,
        int $port,
        string $pool = 'default',
        float $connectTimeout = 2.0,
        float $ioTimeout = 5.0,
    ): Connection {
        $session = new Session(Transport::connectTcp($host, $port, $connectTimeout, $ioTimeout));
        $session->hello();
        return new Connection($session, $pool);
    }
}
