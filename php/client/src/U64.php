<?php // /php/client/src/U64.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Value\CanonicalText;

/**
 * SPEC §9 `U64` above `PHP_INT_MAX` → an exact, string-backed unsigned 64-bit integer
 * (`u64_overflow: object`, the §9.1 default).
 *
 * PHP's `int` is signed 64-bit, so the top half of the `uint64` range (`2^63` .. `2^64-1` — every
 * large MySQL `BIGINT UNSIGNED`) has no native representation: `(int)` SATURATES at `PHP_INT_MAX`,
 * silently turning distinct values into one. This object carries the decimal text instead, and
 * {@see toInt} refuses rather than truncates.
 *
 * A value that FITS a PHP `int` is decoded as a plain `int`, not as this object (SPEC §9's PHP
 * column: "`int` or `Ferro\U64` if > PHP_INT_MAX") — but the constructor still accepts one, so an
 * application can bind a `U64` uniformly.
 *
 * Not a `readonly class`: the package targets PHP >= 8.2, where `readonly` is a property modifier.
 */
final class U64 implements \Stringable
{
    /** The canonical decimal rendering: no sign, no leading zeros. */
    public readonly string $value;

    /**
     * @param int|string $value a non-negative `int`, or a decimal string in `0 .. 2^64-1`.
     * @throws \Ferro\Client\Error\ProtocolException when it is neither.
     */
    public function __construct(int|string $value)
    {
        $this->value = CanonicalText::u64($value);
    }

    public function __toString(): string
    {
        return $this->value;
    }

    /** Whether this value survives a PHP `int` (compared on the decimal text, never by casting). */
    public function fitsInt(): bool
    {
        return CanonicalText::fitsPhpInt($this->value);
    }

    /**
     * @throws \RangeException when the value exceeds `PHP_INT_MAX` — a silent truncation here would
     *   be the exact data corruption this object exists to prevent.
     */
    public function toInt(): int
    {
        if (!$this->fitsInt()) {
            throw new \RangeException(
                'Ferro\U64: ' . $this->value . ' exceeds PHP_INT_MAX and cannot be narrowed to an int '
                . 'without loss — use (string) or the `u64_overflow: string` policy (SPEC §9.1)',
            );
        }
        return (int) $this->value;
    }
}
