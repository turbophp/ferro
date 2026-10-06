<?php // /php/client/src/Client/DuplexTransportInterface.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * A transport that can keep READING while a write cannot progress (M6-F8 review round).
 *
 * The engine's writer can only drain into a client that reads. If the client blocks in a write
 * while the engine's session reader is itself waiting on that writer — it reserves a slot on the
 * writer's channel for every new request, and with many Ferro HTTP exchanges that channel can be
 * full of their frames — neither side moves again: measured, a 12 MiB `REQUEST` written behind 200
 * unconsumed responses blocked for the whole write timeout, closed the session, and turned every
 * request on it Indeterminate. So the session writes through this, and reads frames whenever the
 * socket will not take more bytes but has some to give.
 */
interface DuplexTransportInterface extends SelectableTransportInterface
{
    /**
     * Write all of `$bytes` (the {@see TransportInterface::writeAll} contract: on failure the bytes
     * were NOT all written). Whenever the socket cannot take more and has bytes to read, call
     * `$onReadable` — which reads at most one frame and must not write — then carry on. A write
     * that makes no progress for one read timeout fails, as before.
     *
     * @param \Closure(): void $onReadable
     */
    public function writeAllReading(string $bytes, \Closure $onReadable): void;
}
