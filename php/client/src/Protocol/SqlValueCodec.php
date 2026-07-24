<?php // /php/client/src/Protocol/SqlValueCodec.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Encodes/decodes a single canonical TypedValue between the golden-vector "message" JSON shape
 * {tag, data} and the wire `[tag, payload]` 2-array, byte-for-byte with the Rust `Value` codec
 * (value.rs). BYTES `data` is a list of byte ints so a non-UTF8 blob survives JSON and re-encodes
 * exactly. Also hosts the scalar-narrowing helpers the SQL message codecs share (PHPStan level 9
 * requires every decoded `mixed` be narrowed before use).
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
            C::TAG_BYTES => Value::bytes(self::bytesFromInts($data)),
            default => throw new CodecException("unsupported TypedValue tag {$tag} in M0"),
        };
        return $v->encode($p);
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
            // A wire `bin` decodes to a PHP string; represent it as a list of byte ints (the JSON schema).
            $data = self::intsFromBytes(is_string($data) ? $data : '');
        }
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
    /** @return list<int> */
    private static function intsFromBytes(string $s): array
    {
        if ($s === '') { return []; }
        $u = unpack('C*', $s);
        return $u === false ? [] : array_values($u);
    }
}
