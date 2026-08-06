<?php // /php/client/tests/Support/StringAmountInvoiceDto.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

/**
 * The same result shape as {@see InvoiceDto}, but with `amount` typed `string` — the DTO a PDO-era
 * application already has, where a `numeric` column arrived as a string.
 *
 * It still hydrates under the M1-S7 default policy, because every §9 value object is `Stringable`
 * AND a reflection call is evaluated in WEAK typing mode (`strict_types` does not propagate into
 * calls made by internal functions), so the `Ferro\Decimal` coerces to its canonical text. `at`
 * stays `\DateTimeImmutable`, which is deliberately NOT `Stringable` — see
 * {@see \Ferro\Tests\Unit\DtoHydrationTest}.
 */
final readonly class StringAmountInvoiceDto
{
    public function __construct(
        public int $id,
        public string $amount,
        public \DateTimeImmutable $at,
    ) {}
}
