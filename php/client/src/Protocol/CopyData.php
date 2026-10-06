<?php // /php/client/src/Protocol/CopyData.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * STREAM/`COPY_DATA` (M3-D4): one chunk of RAW COPY bytes, `[data: bin]`, in either direction.
 * STREAM/`COPY_DONE` is the empty fixarray {@see self::DONE}. Mirrors the Rust
 * `messages::copy::{CopyData, CopyDone}` bytes; pinned by the `copy_data`/`copy_done` vectors.
 *
 * Decoding is strict and does not go through a generic unpacker: a generic unpacker returns a `str`
 * and a `bin` as the same PHP string, and a chunk that arrived as text would be a codec defect worth
 * refusing rather than accepting. It also builds no intermediate array per chunk.
 */
final class CopyData
{
    /** The `COPY_DONE` body: an empty fixarray. */
    public const DONE = "\x90";

    /** The most bytes `CopyData` adds around a chunk: the fixarray marker and a `bin32` header. */
    public const OVERHEAD = 6;

    public static function encode(string $data, PackerInterface $p): string
    {
        return $p->packArrayLen(1) . $p->packBin($data);
    }

    /** The chunk inside an encoded `CopyData` payload. */
    public static function decode(string $payload): string
    {
        $n = strlen($payload);
        if ($n < 2 || $payload[0] !== "\x91") {
            throw new CodecException('CopyData is not a fixarray(1)');
        }
        $marker = ord($payload[1]);
        [$headerLen, $len] = match ($marker) {
            0xc4 => [3, $n >= 3 ? ord($payload[2]) : -1],
            0xc5 => [4, $n >= 4 ? self::be(substr($payload, 2, 2)) : -1],
            0xc6 => [6, $n >= 6 ? self::be(substr($payload, 2, 4)) : -1],
            default => throw new CodecException(sprintf('CopyData data is not a bin (marker 0x%02x)', $marker)),
        };
        if ($len < 0 || $n !== $headerLen + $len) {
            throw new CodecException(sprintf('CopyData declares %d bytes in a %d-byte payload', $len, $n));
        }
        return substr($payload, $headerLen);
    }

    private static function be(string $bytes): int
    {
        $v = 0;
        foreach (str_split($bytes) as $b) {
            $v = ($v << 8) | ord($b);
        }
        return $v;
    }
}
