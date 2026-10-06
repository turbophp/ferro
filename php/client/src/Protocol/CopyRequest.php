<?php // /php/client/src/Protocol/CopyRequest.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for the body of SQL/`COPY_IN` and SQL/`COPY_OUT` (M3-D4). Mirrors the Rust
 * `messages::copy::CopyRequest` BYTES: a fixarray of 5 — `pool` (str), `sql` (str, the caller's COPY
 * statement, unmodified), `readonly` (bool, the §19.3 declaration), `timeout_ms` (`u32 | nil`),
 * `tx_id` (`u64 | nil`). Value-free, so this is the plain positional layout the TX messages use.
 * Pinned by /proto/PROTOCOL.md §13 and the `copy_in_request`/`copy_out_request` vectors.
 */
final class CopyRequest
{
    /**
     * @param array<string,mixed> $m
     * @return string the encoded fixarray(5) payload
     */
    public static function encode(array $m, PackerInterface $p): string
    {
        $timeout = $m['timeout_ms'] ?? null;
        $txId = $m['tx_id'] ?? null;
        return $p->packArrayLen(5)
            . $p->packStr(SqlValueCodec::toStr($m['pool'] ?? ''))
            . $p->packStr(SqlValueCodec::toStr($m['sql'] ?? ''))
            . $p->packBool((bool) ($m['readonly'] ?? false))
            . ($timeout === null ? $p->packNil() : $p->packUint(SqlValueCodec::toInt($timeout)))
            . ($txId === null ? $p->packNil() : $p->packUint(SqlValueCodec::toInt($txId)));
    }

    /**
     * Map an already-unpacked 5-element wire array back to the "message" JSON shape.
     * @param array<int,mixed> $w
     * @return array{pool:string,sql:string,readonly:bool,timeout_ms:?int,tx_id:?int}
     */
    public static function mapFromWire(array $w): array
    {
        $w = array_values($w);
        if (count($w) !== 5) { throw new CodecException('CopyRequest arity != 5'); }
        return [
            'pool' => SqlValueCodec::toStr($w[0]),
            'sql' => SqlValueCodec::toStr($w[1]),
            'readonly' => (bool) $w[2],
            'timeout_ms' => SqlValueCodec::nullableInt($w[3]),
            'tx_id' => SqlValueCodec::nullableInt($w[4]),
        ];
    }
}
