<?php // /php/doctrine-dbal/src/Value/DbalValuePolicy.php
declare(strict_types=1);
namespace Ferro\DBAL\Value;

use Ferro\Client\Value\CanonicalText;
use Ferro\Client\Value\ValuePolicy;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\Exception\NonRepresentableValue;
use Ferro\Protocol\Generated\Constants as C;

/**
 * The Doctrine tier's decode policy: canonical wire text for everything DBAL parses correctly, a
 * per-family re-render for the one tag it cannot parse at all, and a LOUD REFUSAL for the values it
 * would parse into something else.
 *
 * **Why a policy and not a conversion inside `Result`.** `ValuePolicy::decode(int $tag, mixed $data)`
 * is handed the per-cell TYPE TAG, which is the only place that information exists on the client:
 * both `ExecCodec::decode()` and `Connection::stream()` deliberately drop the `ColMeta` tag from
 * their column lists, because the per-cell tag is the decode authority. So the driver gets tag
 * awareness for free, with no client API change.
 *
 * **Charter rule 6 is intact.** This is the driver's own conversion step —
 * {@see \Ferro\Client\Value\RawStringValuePolicy}'s docblock says in as many words that the S8 tier
 * "must supply its own conversion rather than lean on the stock type layer for those two tags". No
 * platform is subclassed, no SQL is generated, no result is cached.
 *
 * The three behaviours, each MEASURED against doctrine/dbal 4.4.4:
 *
 *  1. **`TIMESTAMPTZ` is re-rendered.** Canonical text is RFC3339 with a literal `Z`
 *     (`2026-08-05T13:45:07Z`); `DateTimeTzType` has NO fallback and accepts only
 *     `Y-m-d H:i:sO` on PostgreSQL and `Y-m-d H:i:s` on the MySQL family. Every canonical form
 *     throws on every platform, so without this the tag is simply unreadable through DBAL.
 *  2. **Sub-second `TIMESTAMPTZ` is REFUSED, not truncated.** No microsecond form parses on any
 *     platform, and truncating to whole seconds would be a silent precision loss.
 *  3. **Calendar-impossible values are REFUSED.** `date '2026-00-05'` → `2025-12-05`,
 *     `datetime '0000-00-00 00:00:00'` → `-0001-11-30`, `time '24:00:00'` → `00:00:00`. All three
 *     measured, all three with NO exception raised.
 *
 * Everything else is the canonical text verbatim, exactly as `RawStringValuePolicy` hands it up:
 * `DECIMAL` keeps its display scale and its `NaN`/`Infinity` payloads (DBAL's `DecimalType` is a
 * pass-through), `JSON` is the raw document, `UUID` the 36-char lowercase form, `DATE` `Y-m-d`,
 * `TIME` `H:i:s`, and a NAIVE `TIMESTAMP` keeps its microseconds because `DateTimeType` DOES have a
 * `new DateTime($value)` fallback (this last point contradicts a claim in
 * `RawStringValuePolicy`'s docblock, which Task 14 corrects).
 *
 * **The four temporal tags are VALIDATED against the canonical contract first, which
 * `RawStringValuePolicy` deliberately does not do.** That policy's stated reason for skipping the
 * canonical-form check is that "DBAL's own converters report anything else in their own
 * vocabulary" — and these measurements are exactly what falsifies it for a date parser:
 * `date '2026-13-45'` → `2027-02-14` and `'2026-02-30'` → `2026-03-02`, silently, no exception.
 * A non-canonical payload is a WIRE fault, so it surfaces as the client's own
 * {@see \Ferro\Client\Error\ProtocolException} (which {@see \Ferro\DBAL\Connection} wraps like any
 * other client error) rather than as a value refusal; what matters is that it can never be rolled
 * over into a plausible wrong date.
 */
final class DbalValuePolicy implements ValuePolicy
{
    private ?TemporalFormat $fmt = null;

    /**
     * Bind the backend family, ONCE, as soon as the handshake reveals it.
     *
     * The policy has to be constructed BEFORE the connection (it is a constructor argument of
     * `Ferro\Client\Connection`), and the family is only known AFTER the handshake — so the wiring
     * is necessarily two-step. It is a one-shot setter rather than a mutable property so that the
     * "which dialect am I decoding for" question can never change under a live connection, and
     * {@see decode} throws rather than guessing if a temporal cell somehow arrives first.
     */
    public function bindBackend(string $kind): void
    {
        if ($this->fmt !== null) {
            throw DriverException::local('Ferro: DbalValuePolicy::bindBackend() called twice.');
        }
        $this->fmt = TemporalFormat::forKind($kind);
    }

    public function decode(int $tag, mixed $data): mixed
    {
        return match ($tag) {
            C::TAG_NULL => CanonicalText::requireNull($data),
            C::TAG_BOOL => CanonicalText::requireBool($data),
            C::TAG_I64 => CanonicalText::requireInt($data),
            C::TAG_F64 => CanonicalText::requireFloat($data),
            C::TAG_TEXT => CanonicalText::requireString($data, $tag),
            C::TAG_BYTES => CanonicalText::requireBytes($data),
            C::TAG_U64 => CanonicalText::u64($data),
            C::TAG_DECIMAL, C::TAG_UUID, C::TAG_JSON => CanonicalText::requireString($data, $tag),
            C::TAG_DATE => $this->date(CanonicalText::requireString($data, $tag)),
            C::TAG_TIME => $this->time(CanonicalText::requireString($data, $tag)),
            C::TAG_TIMESTAMP => $this->timestamp(CanonicalText::requireString($data, $tag)),
            C::TAG_TIMESTAMPTZ => $this->timestampTz(CanonicalText::requireString($data, $tag)),
            default => throw CanonicalText::unsupportedTag($tag),
        };
    }

    private function date(string $t): string
    {
        CanonicalText::date($t);
        if (CanonicalText::dateIsSentinel($t)) {
            throw NonRepresentableValue::forTag(
                'DATE',
                $t,
                'it is a sentinel or a zero-in-date, and Doctrine\'s DateType would convert it '
                . 'without complaint to a DIFFERENT calendar date',
            );
        }
        return $t;
    }

    private function time(string $t): string
    {
        CanonicalText::time($t);
        if (CanonicalText::timeIsNegative($t)) {
            throw NonRepresentableValue::forTag('TIME', $t, 'Doctrine has no representation for a negative time');
        }
        if (str_contains($t, '.')) {
            throw NonRepresentableValue::forTag(
                'TIME',
                $t,
                'Doctrine\'s TimeType parses only `H:i:s` and has no fallback, so the fraction '
                . 'would have to be dropped',
            );
        }
        $colon = strpos($t, ':');
        if ($colon !== false && (int) substr($t, 0, $colon) > 23) {
            throw NonRepresentableValue::forTag(
                'TIME',
                $t,
                'it is a time-of-day beyond 24 hours (legal in PostgreSQL and in a MySQL TIME '
                . 'interval), which Doctrine\'s TimeType silently wraps to the next day',
            );
        }
        return $t;
    }

    private function timestamp(string $t): string
    {
        CanonicalText::timestamp($t);
        if (!CanonicalText::timestampIsInstant($t)) {
            throw NonRepresentableValue::forTag(
                'TIMESTAMP',
                $t,
                'it is a sentinel or a zero datetime, and Doctrine\'s DateTimeType would convert it '
                . 'without complaint to a DIFFERENT instant',
            );
        }
        return $t;
    }

    private function timestampTz(string $t): string
    {
        $fmt = $this->fmt ?? throw DriverException::local(
            'Ferro: a TIMESTAMPTZ cell arrived before the backend family was known; the driver '
            . 'binds it during connect(), so this indicates the policy was used outside the driver.',
        );
        CanonicalText::timestamptz($t);
        if (!CanonicalText::timestamptzIsInstant($t)) {
            throw NonRepresentableValue::forTag(
                'TIMESTAMPTZ',
                $t,
                'it is a sentinel or a zero timestamp, and Doctrine\'s DateTimeTzType would either '
                . 'reject it or convert it to a different instant',
            );
        }
        if (str_contains($t, '.')) {
            throw NonRepresentableValue::forTag(
                'TIMESTAMPTZ',
                $t,
                'Doctrine\'s DateTimeTzType parses only whole seconds on every platform and has no '
                . 'fallback, so the sub-second part could only be dropped',
            );
        }
        // `!` resets every unspecified field (here: microseconds) instead of inheriting them from
        // "now", so the rendered text depends on the payload alone. Invisible for the two shipped
        // format strings, load-bearing the moment either grows a `u`.
        $dt = \DateTimeImmutable::createFromFormat('!Y-m-d\TH:i:s\Z', $t, new \DateTimeZone('UTC'));
        if ($dt === false) {
            throw NonRepresentableValue::forTag('TIMESTAMPTZ', $t, 'it is not canonical RFC3339 UTC text');
        }
        return $dt->format($fmt->dateTimeTz);
    }
}
