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
 * silently double-convert) a {@see \Ferro\NaiveTimestamp} or a {@see \Ferro\Decimal}. Ferro's
 * canonical text is already exactly the shape those converters expect — `Y-m-d H:i:s[.u]`, a
 * scale-preserving decimal, the raw JSON document — so the DBAL tier wires this policy and inherits
 * a stock, unmodified type layer (charter rule 6: the drop-in tiers change execution, never
 * semantics). The native `Ferro\Client\Connection` API keeps {@see M1ValuePolicy}.
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
