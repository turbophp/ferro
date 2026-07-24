<?php // /php/client/src/Protocol/ExecRequest.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Bespoke positional codec for the SQL EXEC request (service SQL, method EXEC = 1). Mirrors the Rust
 * `messages::sql::ExecRequest` BYTES (a fixarray of 7 fields in declaration order — pool, sql?,
 * query_id?, params, timeout_ms?, readonly, fetch — see /proto/PROTOCOL.md §8.1). Not msg!/rmp-serde
 * on the Rust side because it carries TypedValues; the two languages mirror bytes, not decode
 * structure, so PHP unpacks the whole body and walks the nested arrays. Encodes from / decodes to the
 * golden-vector "message" JSON shape; the S7 runtime binds real DTOs.
 */
final class ExecRequest
{
    /** @param array<string,mixed> $m @return string the encoded fixarray(7) payload */
    public static function encode(array $m, PackerInterface $p): string
    {
        $params = SqlValueCodec::listOf($m['params'] ?? null);
        $paramsBytes = $p->packArrayLen(count($params));
        foreach ($params as $vj) { $paramsBytes .= SqlValueCodec::encode($p, $vj); }

        $fields = [
            $p->packStr(SqlValueCodec::toStr($m['pool'] ?? '')),
            self::optStr($p, $m['sql'] ?? null),
            self::optStr($p, $m['query_id'] ?? null),
            $paramsBytes,
            self::optUint($p, $m['timeout_ms'] ?? null),
            $p->packBool((bool) ($m['readonly'] ?? false)),
            $p->packUint(SqlValueCodec::toInt($m['fetch'] ?? 0)),
        ];
        return $p->packArrayLen(7) . implode('', $fields);
    }

    /**
     * Map an already-unpacked 7-element wire array back to the "message" JSON shape.
     * @param array<int,mixed> $w
     * @return array<string,mixed>
     */
    public static function mapFromWire(array $w): array
    {
        $w = array_values($w);
        if (count($w) !== 7) { throw new CodecException('ExecRequest arity != 7'); }
        $params = SqlValueCodec::listOf($w[3]);
        return [
            'pool' => SqlValueCodec::toStr($w[0]),
            'sql' => SqlValueCodec::nullableStr($w[1]),
            'query_id' => SqlValueCodec::nullableStr($w[2]),
            'params' => array_map([SqlValueCodec::class, 'fromWire'], $params),
            'timeout_ms' => SqlValueCodec::nullableInt($w[4]),
            'readonly' => (bool) $w[5],
            'fetch' => SqlValueCodec::toInt($w[6]),
        ];
    }

    private static function optStr(PackerInterface $p, mixed $v): string
    {
        return $v === null ? $p->packNil() : $p->packStr(SqlValueCodec::toStr($v));
    }
    private static function optUint(PackerInterface $p, mixed $v): string
    {
        return $v === null ? $p->packNil() : $p->packUint(SqlValueCodec::toInt($v));
    }
}
