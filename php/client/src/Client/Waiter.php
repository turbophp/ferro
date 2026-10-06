<?php // /php/client/src/Client/Waiter.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * What a suspended Fiber is waiting for (M3-D1b): one request's terminal on one session. A
 * scheduler resumes the Fiber once {@see ready} is true, which means awaiting it will not block.
 */
final class Waiter
{
    public function __construct(
        public readonly Session $session,
        public readonly int $requestId,
    ) {}

    public function ready(): bool
    {
        return $this->session->isReady($this->requestId);
    }
}
