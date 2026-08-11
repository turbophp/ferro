<?php // /php/client/tests/Support/UnhydratableRowDto.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

/**
 * A DTO that CANNOT hydrate an `(int, int)` row: `\DateTimeImmutable` accepts no coercion from an
 * int under `strict_types`, so binding the constructor raises a `TypeError` which
 * {@see \Ferro\Client\ExecCodec::hydrateDto} converts to a `HydrationException`.
 *
 * Its purpose is to put a HYDRATION failure — a failure of the CALLER's code, with the wire
 * perfectly healthy — on the streamed path, so a test can prove that the in-transaction drain which
 * runs afterwards does not overwrite it with the stream's own terminal
 * ({@see \Ferro\Client\Connection::releaseStream}'s `$surfaceTerminalError`).
 */
final readonly class UnhydratableRowDto
{
    public function __construct(
        public \DateTimeImmutable $i,
        public int $q,
    ) {}
}
