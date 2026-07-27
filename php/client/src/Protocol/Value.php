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

    public function encode(PackerInterface $p): string
    {
        $payload = match ($this->tag) {
            C::TAG_NULL => $p->packNil(),
            C::TAG_BOOL => $p->packBool((bool) $this->data),
            C::TAG_I64 => $p->packInt(self::toInt($this->data)),
            C::TAG_F64 => $p->packFloat64(self::toFloat($this->data)),
            C::TAG_TEXT => $p->packStr(self::toStr($this->data)),
            C::TAG_BYTES => $p->packBin(self::toStr($this->data)),
            default => throw new CodecException('unsupported TypedValue tag ' . $this->tag),
        };
        return $p->packArrayLen(2) . $p->packInt($this->tag) . $payload;
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
