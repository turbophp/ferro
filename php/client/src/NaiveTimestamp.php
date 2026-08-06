<?php // /php/client/src/NaiveTimestamp.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Value\CanonicalText;

/**
 * SPEC §9 `TIMESTAMP` (naive — PG `timestamp`, MySQL `datetime`) → a `DateTimeImmutable` that
 * remembers it was NAIVE.
 *
 * **Why a subclass (F14, settled in M1-S7 Task 7).** §9's PHP column says a naive `TIMESTAMP`
 * hydrates to a `DateTimeImmutable`, and it must stay literally true: `instanceof
 * DateTimeImmutable` / `DateTimeInterface` holds, and every `format()`/`getTimestamp()`/`diff()`
 * call behaves identically. What the subclass adds is a DISCRIMINATOR for the bind path — without
 * it, a value read from a naive column and written straight back is indistinguishable from a UTC
 * instant, so it would bind as `TAG_TIMESTAMPTZ` and shift by the session offset. A brand-new
 * non-`DateTimeImmutable` value object was rejected: it would contradict §9 and break every
 * `instanceof DateTimeInterface` in application code.
 *
 * **Task 8a's `ExecCodec::bindOne` MUST match `NaiveTimestamp` BEFORE `DateTimeImmutable`** —
 * subclass first, or every naive value binds as an instant and the round trip stops being
 * byte-stable.
 *
 * Under `naive_datetime_zone: utc` (the §9.1 default) {@see fromCanonicalText} pins the object to an
 * explicit `UTC` `DateTimeZone`, NOT to `date_default_timezone_get()`: the wall-clock text is
 * identical either way (`format('Y-m-d H:i:s.u')` cannot tell them apart), but a locally-zoned
 * object shifts the moment anything converts it — which is precisely the class of silent bug §9.1
 * exists to eliminate. `naive_datetime_zone: server` is deferred to M1-S8 (nothing on the wire
 * carries the backend's session timezone yet).
 */
final class NaiveTimestamp extends \DateTimeImmutable
{
    /**
     * Build one from the canonical `YYYY-MM-DD HH:MM:SS[.ffffff]` wire text, pinned to UTC.
     *
     * The SENTINEL payloads (`infinity`, `-infinity`, `0000-00-00 00:00:00`, and MySQL's
     * zero-IN-date forms) are deliberately REJECTED here: they are not wall-clock values, and the
     * policy hands them back as canonical text rather than inventing a date.
     *
     * @throws \Ferro\Client\Error\ProtocolException when the text is not a constructible naive
     *   wall-clock value.
     */
    public static function fromCanonicalText(string $text): self
    {
        $canonical = CanonicalText::timestamp($text);
        if (!CanonicalText::timestampIsInstant($canonical)) {
            throw new \Ferro\Client\Error\ProtocolException(sprintf(
                'value tag %d: %s is a sentinel payload, not a wall-clock TIMESTAMP — it has no '
                . 'DateTimeImmutable form and is carried as canonical text (/proto/PROTOCOL.md §3.2)',
                \Ferro\Protocol\Generated\Constants::TAG_TIMESTAMP,
                $canonical,
            ));
        }
        // An explicit UTC zone, never the process default (see the class doc).
        return new self($canonical, new \DateTimeZone('UTC'));
    }

    /**
     * The canonical wire text this value re-binds as: no `.ffffff` group when the sub-second part is
     * zero, otherwise exactly six digits (`/proto/PROTOCOL.md` §3.2). Never a zone suffix.
     */
    public function toCanonicalText(): string
    {
        return $this->format('u') === '000000'
            ? $this->format('Y-m-d H:i:s')
            : $this->format('Y-m-d H:i:s.u');
    }
}
