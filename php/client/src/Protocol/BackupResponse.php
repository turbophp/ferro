<?php // /php/client/src/Protocol/BackupResponse.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for the ADMIN BACKUP success body — the terminal `Outcome::Ok` body. Mirrors the
 * Rust `messages::admin::BackupResponse` BYTES: a fixarray of 3 — `bytes` (u64, the snapshot's size),
 * `queue_us` and `exec_us` (u64, the SPEC §13 pool-wait / statement split). All three are native PHP
 * ints: a snapshot file cannot exceed `PHP_INT_MAX` bytes on any filesystem PHP runs on. Encodes the
 * body only; the caller wraps it in the Outcome envelope. Pinned by /proto/PROTOCOL.md §11 and the
 * `admin_backup_response` vector.
 */
final class BackupResponse
{
    /** @param array<string,mixed> $m @return string the encoded fixarray(3) body (no Outcome wrapper) */
    public static function encode(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(3)
            . $p->packUint(SqlValueCodec::toInt($m['bytes'] ?? 0))
            . $p->packUint(SqlValueCodec::toInt($m['queue_us'] ?? 0))
            . $p->packUint(SqlValueCodec::toInt($m['exec_us'] ?? 0));
    }

    /**
     * Map an already-unpacked 3-element wire array back to the "message" JSON shape.
     * @param array<int,mixed> $w
     * @return array{bytes:int,queue_us:int,exec_us:int}
     */
    public static function mapFromWire(array $w): array
    {
        $w = array_values($w);
        if (count($w) !== 3) { throw new CodecException('BackupResponse arity != 3'); }
        // STRICT, unlike the coercing `SqlValueCodec::toInt` the TX codecs use: a size or a duration
        // that is not a non-negative native int is a malformed reply, never a number to invent
        // (C3-7b-2 review F3 measured `[nil,nil,nil]` decoding as a 0-byte success). A u64 above
        // PHP_INT_MAX unpacks as a decimal string and is refused here too — no snapshot is that big.
        $out = [];
        foreach (['bytes', 'queue_us', 'exec_us'] as $i => $field) {
            $v = $w[$i];
            if (!is_int($v) || $v < 0) {
                throw new CodecException("BackupResponse {$field} is not a non-negative integer");
            }
            $out[$field] = $v;
        }
        return ['bytes' => $out['bytes'], 'queue_us' => $out['queue_us'], 'exec_us' => $out['exec_us']];
    }
}
