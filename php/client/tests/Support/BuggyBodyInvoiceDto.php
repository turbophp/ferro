<?php // /php/client/tests/Support/BuggyBodyInvoiceDto.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Decimal;

/**
 * The SAME column shape as {@see InvoiceDto} (so the hydration plan resolves identically) with an
 * ordinary application bug in the constructor BODY: a private helper called with too few arguments,
 * which raises an `\ArgumentCountError` (a `\TypeError` subclass) naming `needsTwo()`, not the DTO's
 * own constructor.
 *
 * It exists to pin the SCOPE of `ExecCodec::hydrateDto`'s `\TypeError` wrap. This fault is not a
 * §9.1 type-policy boundary, so it must escape UNCHANGED rather than be re-labelled as a
 * `HydrationException` carrying advice ("type the DTO property to match, or decode with a §9.1
 * string policy") that cannot possibly fix it — advice that also buried the real bug in
 * `getPrevious()`.
 */
final readonly class BuggyBodyInvoiceDto
{
    public function __construct(
        public int $id,
        public Decimal $amount,
        public \DateTimeImmutable $at,
    ) {
        /** @phpstan-ignore-next-line the missing argument is the point of this fixture */
        self::needsTwo($id);
    }

    private static function needsTwo(int $a, int $b): int
    {
        return $a + $b;
    }
}
