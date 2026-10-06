<?php // /php/client/src/Protocol/OobRef.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for the payload of a frame carrying the `OOB_FD` flag (M3-D3, SPEC §5.1,
 * `/proto/PROTOCOL.md` §1.1): `[fd_index, len, encoding]`. The frame's real payload is in a SEALED
 * memfd the engine passed beside it with `SCM_RIGHTS`. Mirrors the Rust `messages::OobRef` BYTES and
 * is pinned by the `oob_ref` vector.
 *
 * Decoding is STRICT: the engine attaches exactly one fd per `OOB_FD` frame (`fd_index` 0), the only
 * encoding is `FRAME_PAYLOAD` (the memfd holds exactly what the frame would have carried inline), and
 * `len` is bounded by the same `MAX_FRAME_PAYLOAD` ceiling as an inline payload — anything else is a
 * malformed frame, never a value to adapt to.
 */
final class OobRef
{
    /** @param array<string,mixed> $m @return string the encoded fixarray(3) */
    public static function encode(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(3)
            . $p->packUint(SqlValueCodec::toInt($m['fd_index'] ?? 0))
            . $p->packUint(SqlValueCodec::toInt($m['len'] ?? 0))
            . $p->packUint(SqlValueCodec::toInt($m['encoding'] ?? C::OOB_ENCODING_FRAME_PAYLOAD));
    }

    /**
     * Decode a whole `OOB_FD` frame payload, refusing trailing bytes and every value this client
     * does not implement.
     *
     * @return array{fd_index:int,len:int,encoding:int}
     */
    public static function decode(string $payload, PackerInterface $p): array
    {
        $off = 0;
        $w = $p->unpack($payload, $off);
        if ($off !== strlen($payload)) {
            throw new CodecException('OobRef has trailing bytes');
        }
        if (!is_array($w)) {
            throw new CodecException('OobRef is not an array');
        }
        return self::mapFromWire($w);
    }

    /**
     * @param array<mixed> $w
     * @return array{fd_index:int,len:int,encoding:int}
     */
    public static function mapFromWire(array $w): array
    {
        $w = array_values($w);
        if (count($w) !== 3) {
            throw new CodecException('OobRef arity != 3');
        }
        [$fdIndex, $len, $encoding] = $w;
        if ($fdIndex !== 0) {
            throw new CodecException('OobRef fd_index must be 0 (one fd per OOB_FD frame), got ' . var_export($fdIndex, true));
        }
        if (!is_int($len) || $len < 0 || $len > C::MAX_FRAME_PAYLOAD) {
            throw new CodecException('OobRef len is not a payload length within MAX_FRAME_PAYLOAD');
        }
        if ($encoding !== C::OOB_ENCODING_FRAME_PAYLOAD) {
            throw new CodecException('OobRef encoding is not one this client implements: ' . var_export($encoding, true));
        }
        return ['fd_index' => $fdIndex, 'len' => $len, 'encoding' => $encoding];
    }
}
