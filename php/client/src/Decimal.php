<?php // /php/client/src/Decimal.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Value\CanonicalText;

/**
 * SPEC §9 `DECIMAL` → an EXACT, string-backed decimal (`decimal: object`, the §9.1 default).
 *
 * **It is deliberately not a number.** The canonical payload is the backend's own full-precision
 * rendering with its DISPLAY SCALE PRESERVED (`/proto/PROTOCOL.md` §3.2), and this object carries
 * that text byte-for-byte: `1.10` and `1.1` are DISTINCT values and never normalize to each other,
 * because DBAL compares decimals as strings and a schema's declared scale is part of the value.
 * Routing through `float` would lose precision outright; routing through any fixed-width decimal
 * type would both lose the scale and be unable to hold PG `NUMERIC`'s 131 072 integral digits or
 * its `NaN`/`Infinity` renderings.
 *
 * `__toString()` returns the canonical wire text, so a read → write-back round trip is byte-stable
 * (the bind path re-emits {@see value} verbatim).
 *
 * Not a `readonly class`: the package targets PHP >= 8.2, where `readonly` is a property modifier.
 */
final class Decimal implements \Stringable
{
    public readonly string $value;

    /** @throws \Ferro\Client\Error\ProtocolException when `$value` is not a canonical DECIMAL. */
    public function __construct(string $value)
    {
        $this->value = CanonicalText::decimal($value);
    }

    public function __toString(): string
    {
        return $this->value;
    }

    /** False for the PG `NUMERIC` specials `NaN`, `Infinity` and `-Infinity` (all legal payloads). */
    public function isFinite(): bool
    {
        return !in_array($this->value, CanonicalText::DECIMAL_SPECIALS, true);
    }
}
