<?php // /php/client/src/Client/RequestIdAllocator.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * Allocates per-request `request_id`s: monotonic within a `u32` window, wrapping at 2^32, and
 * NEVER 0 (0 is reserved by the wire for session-context terminals — session-fatal / GOODBYE /
 * no-request-context, see `ferrod` `session::mod`). The starting value is injectable so the wrap
 * boundary is unit-testable without incrementing four billion times.
 */
final class RequestIdAllocator
{
    private const U32_MASK = 0xFFFFFFFF;

    public function __construct(private int $last = 0) {}

    public function next(): int
    {
        $this->last = ($this->last + 1) & self::U32_MASK;
        if ($this->last === 0) {
            // Wrapped past 2^32-1: skip the reserved 0.
            $this->last = 1;
        }
        return $this->last;
    }
}
