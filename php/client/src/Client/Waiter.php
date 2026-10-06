<?php // /php/client/src/Client/Waiter.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * What a suspended Fiber is waiting for (M3-D1b): one request's terminal on one session, or, with
 * `$until`, any other condition that only frames read from that session can make true (an open
 * stream closing). A scheduler resumes the Fiber once {@see ready} is true and keeps reading the
 * session meanwhile.
 */
final class Waiter
{
    /** @param (\Closure(): bool)|null $until a condition to wait for instead of a terminal */
    public function __construct(
        public readonly Session $session,
        public readonly int $requestId,
        private readonly ?\Closure $until = null,
    ) {}

    public function ready(): bool
    {
        return $this->until !== null ? ($this->until)() : $this->session->isReady($this->requestId);
    }
}
