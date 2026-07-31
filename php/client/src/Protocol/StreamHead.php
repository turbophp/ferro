<?php // /php/client/src/Protocol/StreamHead.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Bespoke positional codec for the STREAM `HEAD` message (service `STREAM`, method `HEAD` = 1) —
 * the streaming-fetch counterpart of `ExecOk.cols`, sent once before any `DATA` frame. Mirrors the
 * Rust `messages::sql::StreamHead` BYTES: a fixarray of 1 field — `cols` (array of `[name, tag]`
 * `ColMeta`, the exact shape `ExecOk.cols` uses, so the client hydrator is shared between the
 * buffered and streamed paths — see /proto/PROTOCOL.md §10). NOT wrapped in the `Outcome` envelope:
 * `HEAD`/`DATA` are not terminal frames (that envelope is reserved for the `END` frame, §6). Mirrors
 * `ExecOk`'s own `encodeCols`/cols-decode helpers.
 */
final class StreamHead
{
    /** @param array<string,mixed> $m @return string the encoded fixarray(1) StreamHead payload */
    public static function encode(array $m, PackerInterface $p): string
    {
        $colsBytes = self::encodeCols($p, SqlValueCodec::listOf($m['cols'] ?? null));
        return $p->packArrayLen(1) . $colsBytes;
    }

    /**
     * Map an already-unpacked 1-element wire array back to the "message" JSON shape.
     * @param array<int,mixed> $w
     * @return array{cols:list<array{name:string,tag:int}>}
     */
    public static function mapFromWire(array $w): array
    {
        $w = array_values($w);
        if (count($w) !== 1) { throw new CodecException('StreamHead arity != 1'); }

        $cols = [];
        foreach (SqlValueCodec::listOf($w[0]) as $c) {
            if (!is_array($c) || count($c) !== 2) { throw new CodecException('bad ColMeta wire'); }
            $cc = array_values($c);
            $cols[] = ['name' => SqlValueCodec::toStr($cc[0]), 'tag' => SqlValueCodec::toInt($cc[1])];
        }

        return ['cols' => $cols];
    }

    /** @param list<mixed> $cols */
    private static function encodeCols(PackerInterface $p, array $cols): string
    {
        $out = $p->packArrayLen(count($cols));
        foreach ($cols as $c) {
            if (!is_array($c)) { throw new CodecException('bad col'); }
            $out .= $p->packArrayLen(2)
                . $p->packStr(SqlValueCodec::toStr($c['name'] ?? ''))
                . $p->packUint(SqlValueCodec::toInt($c['tag'] ?? 0));
        }
        return $out;
    }
}
