<?php // /php/client/tests/Support/BuggyBodyTypeInvoiceDto.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Decimal;

/**
 * The second in-body fault shape, alongside {@see BuggyBodyInvoiceDto}: a private helper called with
 * the WRONG argument type, which raises a plain `\TypeError` naming `needsTwo()` rather than the
 * DTO's own constructor. Same reason for existing — it must escape `ExecCodec::hydrateDto`
 * UNCHANGED, because §9.1 type-policy advice cannot fix an application bug.
 */
final readonly class BuggyBodyTypeInvoiceDto
{
    public function __construct(
        public int $id,
        public Decimal $amount,
        public \DateTimeImmutable $at,
    ) {
        /** @phpstan-ignore-next-line the wrong argument type is the point of this fixture */
        self::needsTwo($id, 'not an int');
    }

    private static function needsTwo(int $a, int $b): int
    {
        return $a + $b;
    }
}
