<?php // /php/client/src/Date.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Value\CanonicalText;

/**
 * SPEC §9 `DATE` → the canonical `YYYY-MM-DD` text (`/proto/PROTOCOL.md` §3.2).
 *
 * **Why a value object and not a `DateTimeImmutable`:** a date has no time and no zone, so wrapping
 * it in a zoned instant invites exactly the shift bugs §9.1 exists to prevent — and, decisively,
 * several LEGAL payloads are not calendar days at all: PG's `infinity`/`-infinity`, MySQL's zero
 * date `0000-00-00` and its zero-IN-date forms (`2026-00-05`, legal without `NO_ZERO_IN_DATE`).
 * Those are carried VERBATIM; {@see isSentinel} is how a caller asks before parsing. An impossible
 * day (`2026-13-01`, `2026-02-30`) is a malformed payload and throws.
 *
 * Not a `readonly class`: the package targets PHP >= 8.2, where `readonly` is a property modifier.
 */
final class Date implements \Stringable
{
    public readonly string $value;

    /** @throws \Ferro\Client\Error\ProtocolException when `$value` is not a canonical DATE. */
    public function __construct(string $value)
    {
        $this->value = CanonicalText::date($value);
    }

    public function __toString(): string
    {
        return $this->value;
    }

    /**
     * True when this value is NOT a calendar day: `infinity`, `-infinity`, or a date with a zero
     * year/month/day component. Feeding one to a date parser yields an exception or a nonsense
     * date — both silent-corruption classes — so branch on this first.
     */
    public function isSentinel(): bool
    {
        return CanonicalText::dateIsSentinel($this->value);
    }
}
