<?php // /php/client/src/Client/MultiplexingSessionInterface.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Protocol\Outcome;

/**
 * A session that can carry several requests in flight at once (M3-D1, SPEC §10.1): write now
 * ({@see submit}), read the terminal later ({@see awaitTerminal}), in any order.
 *
 * {@see Connection}'s `…Async` methods use it when the live session implements it, and otherwise
 * run the call at once and return a settled {@see \Ferro\Future} — the same answer, just not
 * concurrent.
 */
interface MultiplexingSessionInterface extends SessionInterface
{
    /** Write a request frame; return its `request_id`. Unsent is {@see Error\TransportException::requestNotSent}. */
    public function submit(int $service, int $method, string $payload): int;

    /** Read until the terminal for `$requestId` arrives, keeping other ids' frames for their awaiters. */
    public function awaitTerminal(int $requestId): Outcome;
}
