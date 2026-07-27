<?php // /php/client/src/Protocol/TxControl.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for the TX COMMIT / ROLLBACK request (service TX, methods COMMIT = 2 /
 * ROLLBACK = 3 — the frame-header method id selects which; the body is identical). Mirrors the Rust
 * `messages::tx::TxControl` BYTES: a fixarray of 1 field — `tx_id` (u64, bounded < 2^63, so a native
 * PHP int). Pinned by /proto/PROTOCOL.md §9.4 and the `tx_commit` vector.
 */
final class TxControl
{
    /** @param array<string,mixed> $m @return string the encoded fixarray(1) payload */
    public static function encode(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(1) . $p->packUint(SqlValueCodec::toInt($m['tx_id'] ?? 0));
    }

    /**
     * Map an already-unpacked 1-element wire array back to the "message" JSON shape.
     * @param array<int,mixed> $w
     * @return array{tx_id:int}
     */
    public static function mapFromWire(array $w): array
    {
        $w = array_values($w);
        if (count($w) !== 1) { throw new CodecException('TxControl arity != 1'); }
        return ['tx_id' => SqlValueCodec::toInt($w[0])];
    }
}
