<?php // /php/client/src/Protocol/Value.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerInterface;

final class Value
{
    private function __construct(public readonly int $tag, public readonly mixed $data) {}

    public static function null(): self { return new self(C::TAG_NULL, null); }
    public static function bool(bool $b): self { return new self(C::TAG_BOOL, $b); }
    public static function i64(int $n): self { return new self(C::TAG_I64, $n); }
    public static function f64(float $f): self { return new self(C::TAG_F64, $f); }
    public static function text(string $s): self { return new self(C::TAG_TEXT, $s); }
    public static function bytes(string $s): self { return new self(C::TAG_BYTES, $s); }

    // --- M1-S7 canonical tags (/proto/PROTOCOL.md §3.2). Every payload below is CANONICAL TEXT
    // riding the msgpack `str` family, except U64 which rides the uint family. The codec moves the
    // text verbatim and validates nothing beyond what the wire family requires — rendering is the
    // producer's job, where the source format is actually known.
    /** @param int|string $n a decimal string carries a value above PHP_INT_MAX losslessly. */
    public static function u64(int|string $n): self { return new self(C::TAG_U64, $n); }
    public static function decimal(string $s): self { return new self(C::TAG_DECIMAL, $s); }
    public static function date(string $s): self { return new self(C::TAG_DATE, $s); }
    public static function time(string $s): self { return new self(C::TAG_TIME, $s); }
    public static function timestamp(string $s): self { return new self(C::TAG_TIMESTAMP, $s); }
    public static function timestamptz(string $s): self { return new self(C::TAG_TIMESTAMPTZ, $s); }
    public static function uuid(string $s): self { return new self(C::TAG_UUID, $s); }
    public static function json(string $s): self { return new self(C::TAG_JSON, $s); }

    public function encode(PackerInterface $p): string
    {
        $payload = match ($this->tag) {
            C::TAG_NULL => $p->packNil(),
            C::TAG_BOOL => $p->packBool((bool) $this->data),
            C::TAG_I64 => $p->packInt(self::toInt($this->data)),
            C::TAG_F64 => $p->packFloat64(self::toFloat($this->data)),
            C::TAG_TEXT => $p->packStr(self::toStr($this->data)),
            C::TAG_BYTES => $p->packBin(self::toStr($this->data)),
            // `packUint`, NEVER `packInt`: packInt physically cannot emit a value above
            // PHP_INT_MAX, and this is the BIND path — a saturated u64 is a silent bad WRITE.
            C::TAG_U64 => $p->packUint(self::requireUint($this->data)),
            // The seven text-canonical tags. `requireStr`, NEVER `self::toStr` — toStr turns a
            // malformed payload into an empty string, which on the bind path is a silent
            // corrupt WRITE (the exact silent-miscast class SPEC §9.1 exists to prevent).
            C::TAG_DECIMAL,
            C::TAG_DATE,
            C::TAG_TIME,
            C::TAG_TIMESTAMP,
            C::TAG_TIMESTAMPTZ,
            C::TAG_UUID,
            C::TAG_JSON => $p->packStr(self::requireStr($this->data, $this->tag)),
            default => throw new CodecException('unsupported TypedValue tag ' . $this->tag),
        };
        return $p->packArrayLen(2) . $p->packInt($this->tag) . $payload;
    }

    /**
     * A canonical-text payload MUST already be a string — no coercion, ever.
     * @throws CodecException
     */
    private static function requireStr(mixed $v, int $tag): string
    {
        if (!is_string($v)) {
            throw new CodecException("TypedValue tag {$tag}: expected a canonical-text string payload, got " . get_debug_type($v));
        }
        return $v;
    }

    /**
     * A U64 payload is a non-negative PHP int (magnitude <= PHP_INT_MAX) or a decimal string
     * (the PurePacker representation of anything larger). Both are passed through to packUint
     * untouched: `(int)`-casting the string here would saturate u64::MAX to PHP_INT_MAX.
     * @throws CodecException
     */
    private static function requireUint(mixed $v): int|string
    {
        if (is_int($v) && $v >= 0) { return $v; }
        if (is_string($v) && preg_match('/^\d+$/', $v) === 1) { return $v; }
        throw new CodecException('TypedValue tag ' . C::TAG_U64 . ': expected a non-negative int or decimal string, got ' . get_debug_type($v));
    }

    public static function decode(PackerInterface $p, string $buf, int &$offset): self
    {
        $arr = $p->unpack($buf, $offset);
        if (!is_array($arr) || count($arr) !== 2) { throw new CodecException('bad TypedValue array'); }
        return new self(self::toInt($arr[0]), $arr[1]);
    }

    /** Narrows the (int|float|string|bool)-typed $data payload to int; only reached via factories that set an int. */
    private static function toInt(mixed $v): int
    {
        return match (true) {
            is_int($v) => $v,
            is_float($v), is_string($v), is_bool($v) => (int) $v,
            default => 0,
        };
    }
    /** Narrows $data to float; only reached via factories that set a float. */
    private static function toFloat(mixed $v): float
    {
        return match (true) {
            is_float($v) => $v,
            is_int($v), is_string($v), is_bool($v) => (float) $v,
            default => 0.0,
        };
    }
    /** Narrows $data to string; only reached via factories that set a string. */
    private static function toStr(mixed $v): string
    {
        return match (true) {
            is_string($v) => $v,
            is_int($v), is_float($v) => (string) $v,
            is_bool($v) => $v ? '1' : '',
            default => '',
        };
    }
}
