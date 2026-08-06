<?php // /php/client/src/Time.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Value\CanonicalText;

/**
 * SPEC §9 `TIME` → the canonical `[-]HH:MM:SS[.ffffff]` text (`/proto/PROTOCOL.md` §3.2).
 *
 * **Why a value object and not a `DateTimeImmutable`:** the canonical range does not fit one. PG's
 * `time '24:00:00'` is legal and would WRAP to `00:00:00` in any wall-clock type, and a MySQL
 * `TIME` is a signed interval spanning ±838 h (`-838:59:59` .. `838:59:59`). The text is carried
 * verbatim so both survive, and so a read → write-back round trip is byte-stable.
 *
 * Not a `readonly class`: the package targets PHP >= 8.2, where `readonly` is a property modifier.
 */
final class Time implements \Stringable
{
    public readonly string $value;

    /** @throws \Ferro\Client\Error\ProtocolException when `$value` is not a canonical TIME. */
    public function __construct(string $value)
    {
        $this->value = CanonicalText::time($value);
    }

    public function __toString(): string
    {
        return $this->value;
    }

    /** True for a negative MySQL `TIME` interval (a leading `-`); PG never produces one. */
    public function isNegative(): bool
    {
        return CanonicalText::timeIsNegative($this->value);
    }
}
