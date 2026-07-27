<?php // /php/client/src/Protocol/BeginResponse.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for the TX BEGIN success body — the terminal `Outcome::Ok` body (§6). Mirrors the
 * Rust `messages::tx::BeginResponse` BYTES: a fixarray of 1 field — `tx_id` (u64). It composes inside
 * `Outcome::Ok` (status 0) exactly as `ExecOk` does because its encoding is one complete top-level
 * MessagePack value ({@see Outcome}). `tx_id` is a monotonic, never-reused counter contractually
 * bounded < 2^63 (PROTOCOL.md §2/§7), so it is a NATIVE PHP int — NOT the `boot_epoch`
 * decimal-string treatment. Encodes the BeginResponse body only; the caller wraps it in the Outcome
 * envelope. Pinned by /proto/PROTOCOL.md §9.3 and the `tx_begin_response` vector.
 */
final class BeginResponse
{
    /** @param array<string,mixed> $m @return string the encoded fixarray(1) BeginResponse body (no Outcome wrapper) */
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
        if (count($w) !== 1) { throw new CodecException('BeginResponse arity != 1'); }
        return ['tx_id' => SqlValueCodec::toInt($w[0])];
    }
}
