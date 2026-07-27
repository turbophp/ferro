<?php // /php/client/src/Protocol/SavepointRequest.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for the TX SAVEPOINT / RELEASE / ROLLBACK_TO request (service TX, methods
 * SAVEPOINT = 4 / RELEASE = 5 / ROLLBACK_TO = 6 — the frame-header method id selects which; the body
 * is identical). Mirrors the Rust `messages::tx::SavepointRequest` BYTES: a fixarray of 2 fields in
 * declaration order — `tx_id` (u64, bounded < 2^63, native PHP int) and `name` (`str | nil`; bare
 * `nil` ⇒ the engine names it, `sp_<n>` stack). Pinned by /proto/PROTOCOL.md §9.5 and the
 * `tx_savepoint` vector.
 */
final class SavepointRequest
{
    /** @param array<string,mixed> $m @return string the encoded fixarray(2) payload */
    public static function encode(array $m, PackerInterface $p): string
    {
        $name = $m['name'] ?? null;
        return $p->packArrayLen(2)
            . $p->packUint(SqlValueCodec::toInt($m['tx_id'] ?? 0))
            . ($name === null ? $p->packNil() : $p->packStr(SqlValueCodec::toStr($name)));
    }

    /**
     * Map an already-unpacked 2-element wire array back to the "message" JSON shape.
     * @param array<int,mixed> $w
     * @return array{tx_id:int,name:?string}
     */
    public static function mapFromWire(array $w): array
    {
        $w = array_values($w);
        if (count($w) !== 2) { throw new CodecException('SavepointRequest arity != 2'); }
        return [
            'tx_id' => SqlValueCodec::toInt($w[0]),
            'name' => SqlValueCodec::nullableStr($w[1]),
        ];
    }
}
