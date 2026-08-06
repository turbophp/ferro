<?php // /php/client/src/Client/Value/M1ValuePolicy.php
declare(strict_types=1);
namespace Ferro\Client\Value;

use Ferro\Client\Error\TypePolicyException;
use Ferro\Date;
use Ferro\Decimal;
use Ferro\Json;
use Ferro\NaiveTimestamp;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Time;
use Ferro\U64;
use Ferro\Uuid;

/**
 * The M1-S7 {@see ValuePolicy}: all FOURTEEN implemented canonical tags → the SPEC §9 PHP types,
 * governed by the four SPEC §9.1 knobs in {@see TypePolicyOptions}.
 *
 * |  tag           | PHP (default `object` forms)                         | knob                  |
 * |----------------|------------------------------------------------------|-----------------------|
 * | NULL/BOOL/I64/F64/TEXT/BYTES | `null`/`bool`/`int`/`float`/`string`/`string` (binary) | — |
 * | `U64`          | `int`, or {@see U64} above `PHP_INT_MAX`              | `u64_overflow`        |
 * | `DECIMAL`      | {@see Decimal} (string-backed, exact)                | `decimal`             |
 * | `DATE`/`TIME`  | {@see Date} / {@see Time}                            | —                     |
 * | `TIMESTAMP`    | {@see NaiveTimestamp} (a `DateTimeImmutable`, UTC)   | `naive_datetime_zone` |
 * | `TIMESTAMPTZ`  | `\DateTimeImmutable` in UTC (an instant)             | —                     |
 * | `UUID`         | {@see Uuid}                                          | `uuid`                |
 * | `JSON`         | lazy {@see Json}                                     | —                     |
 *
 * **Every arm THROWS on a bad payload; nothing coerces (hazard 30).** The M0 idiom
 * ({@see M0ValuePolicy::toInt}/`toStr`) returns `0`/`''` for an unexpected payload, which here would
 * mean a malformed `DECIMAL` becoming `Decimal('')` and a truncated `TIMESTAMP` becoming epoch-zero
 * — the silent miscast §9.1 exists to prevent. The split is by CAUSE: a MALFORMED payload is a wire
 * fault ({@see \Ferro\Client\Error\ProtocolException}, raised by {@see CanonicalText}); an operator
 * POLICY refusal is a {@see TypePolicyException}, so the S8 Doctrine `ExceptionConverter` never
 * reports a configuration choice as a driver protocol failure.
 *
 * **Sentinels never reach a date parser.** `infinity` / `-infinity` / `0000-00-00 00:00:00`, and
 * MySQL's zero-IN-date forms, are legal payloads that are NOT calendar values: `DATE` carries them
 * inside {@see Date} ({@see Date::isSentinel}), while `TIMESTAMP`/`TIMESTAMPTZ` — whose PHP type is
 * an instant — hand back the canonical TEXT verbatim. That keeps a legal column readable (throwing
 * would make it unreadable with no escape hatch) without ever inventing a date.
 *
 * **A decode failure is NOT a §19.3 fate signal.** It surfaces client-side inside
 * {@see \Ferro\Client\ExecCodec::decodeRow} — after the statement already succeeded, and on the
 * streamed path after earlier rows were already yielded — so it sits in the `FerroException` tree,
 * deliberately outside the Retryable/Indeterminate/NonRetryable branches mapped from the wire.
 */
final class M1ValuePolicy implements ValuePolicy
{
    public function __construct(private readonly TypePolicyOptions $options = new TypePolicyOptions()) {}

    /** The §9.1 knobs this policy decodes with. */
    public function options(): TypePolicyOptions
    {
        return $this->options;
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
            C::TAG_U64 => $this->decodeU64($data),
            C::TAG_DECIMAL => $this->decodeDecimal($data),
            C::TAG_DATE => new Date(CanonicalText::requireString($data, $tag)),
            C::TAG_TIME => new Time(CanonicalText::requireString($data, $tag)),
            C::TAG_TIMESTAMP => $this->decodeTimestamp($data),
            C::TAG_TIMESTAMPTZ => $this->decodeTimestampTz($data),
            C::TAG_UUID => $this->decodeUuid($data),
            C::TAG_JSON => new Json(CanonicalText::requireString($data, $tag)),
            default => throw CanonicalText::unsupportedTag($tag),
        };
    }

    /**
     * `u64_overflow` — `object` (default): an `int` when it fits, {@see U64} above `PHP_INT_MAX`;
     * `string`: the decimal text for the WHOLE column, so its PHP type does not change with a row's
     * magnitude (what DBAL and a `string` schema column want); `error`: refuse the overflow only.
     *
     * BOTH wire forms are normalized first (hazard 28) — the value's msgpack marker follows its
     * MAGNITUDE, not its tag, so `5` arrives as `int` and `2^33` as a decimal string.
     */
    private function decodeU64(mixed $data): int|string|U64
    {
        $decimal = CanonicalText::u64($data);
        if ($this->options->u64Overflow === 'string') {
            return $decimal;
        }
        if (CanonicalText::fitsPhpInt($decimal)) {
            return (int) $decimal;
        }
        if ($this->options->refusesU64Overflow()) {
            throw new TypePolicyException(sprintf(
                'u64_overflow=error refuses a U64 above PHP_INT_MAX (%d digits) — it has no lossless '
                . 'PHP int form. Use u64_overflow=object (a Ferro\U64) or =string (SPEC §9.1).',
                strlen($decimal),
            ));
        }
        return new U64($decimal);
    }

    /** `decimal` — `object` (default): {@see Decimal}; `string`: the canonical text, still validated. */
    private function decodeDecimal(mixed $data): string|Decimal
    {
        $value = new Decimal(CanonicalText::requireString($data, C::TAG_DECIMAL));
        return $this->options->decimal === 'string' ? $value->value : $value;
    }

    /** `uuid` — `object` (default): {@see Uuid}; `string`: the canonical text, still validated. */
    private function decodeUuid(mixed $data): string|Uuid
    {
        $value = new Uuid(CanonicalText::requireString($data, C::TAG_UUID));
        return $this->options->uuid === 'string' ? $value->value : $value;
    }

    /**
     * `naive_datetime_zone` — `utc` (default): a {@see NaiveTimestamp} pinned to an explicit UTC
     * zone; `error`: refuse the tag outright.
     *
     * The refusal is checked BEFORE the value is interpreted (but after the payload family is
     * verified, so a genuine wire fault is still reported as one): the knob's purpose is migrating a
     * schema off naive columns, so every read of one must fail — uniformly, not only for rows that
     * happen to be constructible.
     */
    private function decodeTimestamp(mixed $data): string|NaiveTimestamp
    {
        $text = CanonicalText::requireString($data, C::TAG_TIMESTAMP);
        if ($this->options->refusesNaiveTimestamp(C::TAG_TIMESTAMP)) {
            throw new TypePolicyException(
                'naive_datetime_zone=error refuses a naive TIMESTAMP: the wire carries no zone for '
                . 'it, so any instant this client built would be a guess. Use naive_datetime_zone=utc '
                . '(the default) or read the column with a raw-string policy (SPEC §9.1).',
            );
        }
        $canonical = CanonicalText::timestamp($text);
        if (!CanonicalText::timestampIsInstant($canonical)) {
            return $canonical; // infinity / zero date / zero-in-date: verbatim, never parsed
        }
        return NaiveTimestamp::fromCanonicalText($canonical);
    }

    /**
     * `TIMESTAMPTZ` is an INSTANT and has no knob: the payload is already normalized to UTC by the
     * backend, so there is nothing to guess.
     *
     * The object is built with an explicit `UTC` {@see \DateTimeZone} rather than by handing the
     * `…Z` text to `new \DateTimeImmutable()`, which produces a timezone whose `getName()` is the
     * literal `'Z'` (type 2) — the same moment, but a different, surprising object that does not
     * compare or serialize like a UTC one.
     */
    private function decodeTimestampTz(mixed $data): string|\DateTimeImmutable
    {
        $canonical = CanonicalText::timestamptz(CanonicalText::requireString($data, C::TAG_TIMESTAMPTZ));
        if (!CanonicalText::timestamptzIsInstant($canonical)) {
            return $canonical; // infinity / a MySQL zero TIMESTAMP: verbatim, never parsed
        }
        $wallClock = str_replace('T', ' ', substr($canonical, 0, -1)); // drop the trailing 'Z'
        return new \DateTimeImmutable($wallClock, new \DateTimeZone('UTC'));
    }
}
