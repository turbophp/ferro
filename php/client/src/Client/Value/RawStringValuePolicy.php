<?php // /php/client/src/Client/Value/RawStringValuePolicy.php
declare(strict_types=1);
namespace Ferro\Client\Value;

use Ferro\Protocol\Generated\Constants as C;

/**
 * The **driver-native** {@see ValuePolicy}: every M1-S7 canonical tag comes back as the CANONICAL
 * WIRE TEXT verbatim (`/proto/PROTOCOL.md` §3.2), with the M0 scalars keeping their natural PHP
 * types. It is the identity policy for the new tag set, and it takes no §9.1 knobs — there is
 * nothing to decide when nothing is interpreted.
 *
 * **This is the M1-S8 Doctrine DBAL hand-off.** A DBAL driver is expected to behave like PDO: hand
 * up driver-native STRINGS and let the type layer convert. `DateTimeType::convertToPHPValue`,
 * `DecimalType`, `JsonType` and `GuidType` all parse a string themselves and would choke on (or
 * silently double-convert) a {@see \Ferro\NaiveTimestamp} or a {@see \Ferro\Decimal}. What this
 * policy provides is the VERBATIM canonical wire text of `/proto/PROTOCOL.md` §3.2 — nothing more:
 * a scale-preserving decimal, the raw JSON document, the lowercase hyphenated UUID, `Y-m-d H:i:s[.u]`
 * for `TIMESTAMP` and RFC3339 `Y-m-d\TH:i:s[.u]Z` for `TIMESTAMPTZ`.
 *
 * **S8 WARNING — DBAL's stock platform format strings do NOT match the canonical datetime text, and
 * the S8 tier must supply its own conversion rather than lean on the stock type layer for those two
 * tags.** `AbstractPlatform::getDateTimeFormatString()` is `Y-m-d H:i:s`, which `createFromFormat`
 * rejects the moment a `TIMESTAMP` carries its canonical `.250000` fraction; and
 * `PostgreSQLPlatform::getDateTimeTzFormatString()` is `Y-m-d H:i:sO`, which matches neither the `T`
 * separator, the literal `Z`, nor the microseconds of a canonical `TIMESTAMPTZ`. The remaining tags
 * (`DECIMAL`, `JSON`, `UUID`, `DATE`, `TIME`, and a whole-second `TIMESTAMP`) do land in the shape the
 * stock converters expect. Charter rule 6 is unaffected: this is the DRIVER's own conversion step,
 * not a change to Grammar/Processor or platform SQL generation. The native
 * `Ferro\Client\Connection` API keeps {@see M1ValuePolicy}.
 *
 * **Verbatim, but never coercing.** A payload that is not in the right msgpack family still throws
 * (hazard 30) — `SqlValueCodec::toStr`'s `''` fallback would hand DBAL an empty string for a
 * malformed cell. What this policy skips is the CANONICAL-FORM check (calendar validity, UUID
 * casing): the backend renderer already produced canonical text, and DBAL's own converters report
 * anything else in their own vocabulary, so a second parser here would only add a redundant failure
 * mode. `U64` is normalized to its decimal string because its wire form follows its magnitude
 * (hazard 28) — a column must not change PHP type between rows.
 */
final class RawStringValuePolicy implements ValuePolicy
{
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
            C::TAG_DECIMAL,
            C::TAG_DATE,
            C::TAG_TIME,
            C::TAG_TIMESTAMP,
            C::TAG_TIMESTAMPTZ,
            C::TAG_UUID,
            C::TAG_JSON => CanonicalText::requireString($data, $tag),
            default => throw CanonicalText::unsupportedTag($tag),
        };
    }
}
