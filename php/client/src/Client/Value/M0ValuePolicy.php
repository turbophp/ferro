<?php // /php/client/src/Client/Value/M0ValuePolicy.php
declare(strict_types=1);
namespace Ferro\Client\Value;

use Ferro\Client\Error\ProtocolException;
use Ferro\Protocol\Generated\Constants as C;

/**
 * The M0 {@see ValuePolicy}: the six canonical scalar tags → native PHP scalars.
 *
 *   NULL  → null
 *   BOOL  → bool
 *   I64   → int    (values > PHP_INT_MAX are post-M0 / unreachable in the M0 scalar set)
 *   F64   → float
 *   TEXT  → string
 *   BYTES → string (binary; the wire `list<int>` re-assembled into a byte string)
 *
 * Every reserved M1 tag (DECIMAL/DATE/TIME/TIMESTAMP/TIMESTAMPTZ/UUID/JSON/ARRAY/INTERVAL/INET/
 * VECTOR/U64) is `Unsupported` in M0 and raises a {@see ProtocolException} naming the tag — a loud,
 * diagnosable failure, never a silent miscast. The M1 policies implement these arms.
 */
final class M0ValuePolicy implements ValuePolicy
{
    public function decode(int $tag, mixed $data): mixed
    {
        return match ($tag) {
            C::TAG_NULL  => null,
            C::TAG_BOOL  => (bool) $data,
            C::TAG_I64   => self::toInt($data),
            C::TAG_F64   => self::toFloat($data),
            C::TAG_TEXT  => self::toStr($data),
            C::TAG_BYTES => self::bytesFromInts($data),
            default => throw new ProtocolException(sprintf(
                'value tag %d is not supported in M0 (DECIMAL/TIMESTAMP/UUID/JSON/... land in M1)',
                $tag,
            )),
        };
    }

    private static function toInt(mixed $v): int
    {
        return match (true) {
            is_int($v) => $v,
            is_float($v), is_string($v), is_bool($v) => (int) $v,
            default => 0,
        };
    }

    private static function toFloat(mixed $v): float
    {
        return match (true) {
            is_float($v) => $v,
            is_int($v), is_string($v), is_bool($v) => (float) $v,
            default => 0.0,
        };
    }

    private static function toStr(mixed $v): string
    {
        return match (true) {
            is_string($v) => $v,
            is_int($v), is_float($v) => (string) $v,
            is_bool($v) => $v ? '1' : '',
            default => '',
        };
    }

    /** Re-assemble a BYTES payload (a `list<int>` per {@see \Ferro\Protocol\SqlValueCodec::fromWire}). */
    private static function bytesFromInts(mixed $data): string
    {
        if (is_string($data)) { return $data; } // defensive: a raw binary string passes through
        if (!is_array($data)) { return ''; }
        $s = '';
        foreach ($data as $b) {
            $s .= chr(is_int($b) ? ($b & 0xff) : 0);
        }
        return $s;
    }
}
