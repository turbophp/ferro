<?php // /php/client/src/Protocol/SqlValueCodec.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Encodes/decodes a single canonical TypedValue between the golden-vector "message" JSON shape
 * {tag, data} and the wire `[tag, payload]` 2-array, byte-for-byte with the Rust `Value` codec
 * (value.rs). DECODED BYTES `data` is a list of byte ints so a non-UTF8 blob survives JSON and
 * re-encodes exactly; on ENCODE both that list and a raw byte string are accepted, because
 * `ExecCodec::bindOne` emits the string form for a {@see \Ferro\Bytes} param — see
 * {@see bytesPayload}. Also hosts the scalar-narrowing helpers the SQL message codecs share
 * (PHPStan level 9 requires every decoded `mixed` be narrowed before use).
 */
final class SqlValueCodec
{
    public static function encode(PackerInterface $p, mixed $vj): string
    {
        if (!is_array($vj)) { throw new CodecException('SqlValueCodec: value is not an array'); }
        $tag = self::toInt($vj['tag'] ?? -1);
        $data = $vj['data'] ?? null;
        $v = match ($tag) {
            C::TAG_NULL => Value::null(),
            C::TAG_BOOL => Value::bool((bool) $data),
            C::TAG_I64 => Value::i64(self::toInt($data)),
            C::TAG_F64 => Value::f64(self::toFloat($data)),
            C::TAG_TEXT => Value::text(self::toStr($data)),
            C::TAG_BYTES => Value::bytes(self::bytesPayload($data)),
            // M1-S7 canonical tags (/proto/PROTOCOL.md §3.2). Routed through the Value factories
            // so the payload guards live in exactly ONE place (Value::requireStr/requireUint) —
            // deliberately NOT through self::toStr/toInt, whose 0/''/0.0 fallbacks would turn a
            // malformed payload into a silent bad WRITE on the bind path (SPEC §9.1).
            C::TAG_U64 => Value::u64(self::uintPayload($data)),
            C::TAG_DECIMAL => Value::decimal(self::strPayload($data, $tag)),
            C::TAG_DATE => Value::date(self::strPayload($data, $tag)),
            C::TAG_TIME => Value::time(self::strPayload($data, $tag)),
            C::TAG_TIMESTAMP => Value::timestamp(self::strPayload($data, $tag)),
            C::TAG_TIMESTAMPTZ => Value::timestamptz(self::strPayload($data, $tag)),
            C::TAG_UUID => Value::uuid(self::strPayload($data, $tag)),
            C::TAG_JSON => Value::json(self::strPayload($data, $tag)),
            default => throw new CodecException("unsupported TypedValue tag {$tag}"),
        };
        return $v->encode($p);
    }

    /**
     * Narrow a `mixed` canonical-text payload to `string` WITHOUT coercing (PHPStan level 9 needs
     * the narrow; §9.1 needs the refusal). Mirrors `Value::requireStr` — kept here because the
     * typed factories take `string`, so the check must happen before the call, not inside it.
     * @throws CodecException
     */
    private static function strPayload(mixed $data, int $tag): string
    {
        if (!is_string($data)) {
            throw new CodecException("TypedValue tag {$tag}: expected a canonical-text string payload, got " . get_debug_type($data));
        }
        return $data;
    }

    /**
     * Narrow a `mixed` U64 payload to `int|string`. A value above PHP_INT_MAX arrives as a decimal
     * string (PurePacker's uint64 representation) and MUST stay one — `self::toInt` would saturate
     * u64::MAX to PHP_INT_MAX. Range/format validation itself lives in `Value::requireUint`.
     * @throws CodecException
     */
    private static function uintPayload(mixed $data): int|string
    {
        if (is_int($data) || is_string($data)) { return $data; }
        throw new CodecException('TypedValue tag ' . C::TAG_U64 . ': expected a non-negative int or decimal string, got ' . get_debug_type($data));
    }

    /**
     * Map an already-unpacked wire `[tag, payload]` pair to the {tag, data} JSON shape.
     * @return array{tag:int,data:mixed}
     */
    public static function fromWire(mixed $pair): array
    {
        if (!is_array($pair) || count($pair) !== 2) { throw new CodecException('bad wire TypedValue'); }
        $vals = array_values($pair);
        $tag = self::toInt($vals[0]);
        $data = $vals[1];
        if ($tag === C::TAG_BYTES) {
            // A wire `bin` decodes to a PHP string; represent it as a list of byte ints (the JSON
            // schema). Anything else is a wire fault and THROWS — the READ-side mirror of
            // {@see bytesPayload}'s refusal, closed in the M1-S8a review round.
            //
            // This arm used to read `self::intsFromBytes(is_string($data) ? $data : '')`, so a nil,
            // int, float, bool or array payload on a `bytea`/`BLOB` column silently decoded to an
            // EMPTY blob: the read lost the data with no signal, and a read -> write-back round trip
            // then PERSISTED the emptiness. S8a closed exactly this coercion on the ENCODE side
            // ("a silently-empty blob is exactly the silent corrupt WRITE §9.1 exists to prevent")
            // and left the READ side open; one direction of a pair is not a policy.
            //
            // Reachability on the happy path is low — a conformant engine always emits `bin`, and
            // TYPE_REGISTRY_HASH blocks a coverage-skewed peer — so this is defence in depth. It is
            // still the right shape: an empty blob and a codec fault must not look identical.
            if (!is_string($data)) {
                throw new CodecException(
                    'TypedValue tag ' . C::TAG_BYTES . ': expected a bin payload (a byte string), got '
                    . get_debug_type($data),
                );
            }
            $data = self::intsFromBytes($data);
        }
        // NOTE: `TAG_BYTES` is still the ONLY special case, and that is by design. Every M1-S7 tag
        // rides the msgpack `str` family (or, for TAG_U64, the uint family), so its decoded payload
        // is already the canonical value and passes straight through. A future `bin`-family tag
        // would need this same list<int> conversion PLUS a vector-JSON workaround (a `bin` payload
        // is indistinguishable from `str` after unpack and cannot round-trip through the golden
        // vectors' `message` field) — which is precisely why the wire contract is text-canonical.
        return ['tag' => $tag, 'data' => $data];
    }

    // --- scalar narrowing (decoded msgpack / decoded-JSON scalars are `mixed` under level 9) ---

    public static function toInt(mixed $v): int
    {
        return match (true) {
            is_int($v) => $v,
            is_float($v), is_string($v), is_bool($v) => (int) $v,
            default => 0,
        };
    }
    public static function toFloat(mixed $v): float
    {
        return match (true) {
            is_float($v) => $v,
            is_int($v), is_string($v), is_bool($v) => (float) $v,
            default => 0.0,
        };
    }
    public static function toStr(mixed $v): string
    {
        return match (true) {
            is_string($v) => $v,
            is_int($v), is_float($v) => (string) $v,
            is_bool($v) => $v ? '1' : '',
            default => '',
        };
    }
    public static function nullableInt(mixed $v): ?int
    {
        return $v === null ? null : self::toInt($v);
    }
    public static function nullableStr(mixed $v): ?string
    {
        return $v === null ? null : self::toStr($v);
    }
    /** @return list<mixed> */
    public static function listOf(mixed $v): array
    {
        return is_array($v) ? array_values($v) : [];
    }

    private static function bytesFromInts(mixed $data): string
    {
        $s = '';
        foreach (self::listOf($data) as $b) { $s .= chr(self::toInt($b) & 0xff); }
        return $s;
    }

    /**
     * A `TAG_BYTES` payload, from EITHER of its two legitimate producers.
     *
     * *  a raw **byte string** — what {@see \Ferro\Client\ExecCodec::bindOne} emits for a
     *    {@see \Ferro\Bytes} bind. A blob is bound as-is: converting it to a `list<int>` first would
     *    inflate a 1 MiB `LARGE_OBJECT` into a million-element PHP array, and the `Ferro\Bytes` path
     *    exists precisely to carry large binary parameters.
     * *  a **`list<int>`** of bytes — what {@see fromWire} produces and what the golden vectors'
     *    `message` JSON carries (JSON cannot hold arbitrary bytes), so a decoded wire cell or a
     *    committed vector re-encodes byte-identically.
     *
     * The two are unambiguous, so both are accepted; anything else THROWS. That refusal is the
     * point: until M1-S8a this arm was `bytesFromInts($data)` alone, whose `listOf()` returns `[]`
     * for a string — so a raw-string payload encoded as `c400`, an **EMPTY bin**. Nothing produced
     * one while `TAG_BYTES` was unreachable from PHP; `Ferro\Bytes` created the first producer, and
     * a silently-empty blob is exactly the silent corrupt WRITE §9.1 exists to prevent.
     *
     * @throws CodecException
     */
    private static function bytesPayload(mixed $data): string
    {
        if (is_string($data)) { return $data; }
        if (is_array($data)) { return self::bytesFromInts($data); }
        throw new CodecException(
            'TypedValue tag ' . C::TAG_BYTES . ': expected a byte string or a list<int> of bytes, got '
            . get_debug_type($data),
        );
    }
    /** @return list<int> */
    private static function intsFromBytes(string $s): array
    {
        if ($s === '') { return []; }
        $u = unpack('C*', $s);
        return $u === false ? [] : array_values($u);
    }
}
