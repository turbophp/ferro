<?php // /php/client/tests/Support/InvoiceDto.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Decimal;

/**
 * A `final readonly` DTO whose promoted parameters are typed with the SPEC §9 VALUE OBJECTS — the
 * M1-S7 shape a Doctrine/Eloquent-era application actually writes (`amount` is a `numeric` column,
 * `at` a `timestamptz`, `ref` a `uuid`). Through M1-S6 every hydrated cell was a scalar, so this
 * combination did not exist; it is the native-API DTO path hazard 35 is about.
 */
final readonly class InvoiceDto
{
    public function __construct(
        public int $id,
        public Decimal $amount,
        public \DateTimeImmutable $at,
    ) {}
}
