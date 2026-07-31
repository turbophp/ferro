<?php // /php/client/src/Protocol/StreamData.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Bespoke positional codec for the STREAM `DATA` message (service `STREAM`, method `DATA` = 2) — a
 * batch of result rows on the streaming-fetch DATA channel (carried in a frame with the `STREAM`
 * flag, `Constants::FLAG_STREAM`, set). Mirrors the Rust `messages::sql::StreamData` BYTES: a
 * fixarray of 1 field — `rows` (array of rows, each an array of TypedValue cells) — the SAME
 * `Value` `[tag, payload]` scalar codec `ExecOk.rows` uses (see /proto/PROTOCOL.md §10). NOT
 * wrapped in the `Outcome` envelope: `HEAD`/`DATA` are not terminal frames (that envelope is
 * reserved for the `END` frame, §6). Mirrors `ExecOk`'s own `encodeRows`/rows-decode helpers.
 */
final class StreamData
{
    /** @param array<string,mixed> $m @return string the encoded fixarray(1) StreamData payload */
    public static function encode(array $m, PackerInterface $p): string
    {
        $rowsBytes = self::encodeRows($p, SqlValueCodec::listOf($m['rows'] ?? null));
        return $p->packArrayLen(1) . $rowsBytes;
    }

    /**
     * Map an already-unpacked 1-element wire array back to the "message" JSON shape.
     * @param array<int,mixed> $w
     * @return array{rows:list<list<array{tag:int,data:mixed}>>}
     */
    public static function mapFromWire(array $w): array
    {
        $w = array_values($w);
        if (count($w) !== 1) { throw new CodecException('StreamData arity != 1'); }

        $rows = [];
        foreach (SqlValueCodec::listOf($w[0]) as $row) {
            $rows[] = array_map([SqlValueCodec::class, 'fromWire'], SqlValueCodec::listOf($row));
        }

        return ['rows' => $rows];
    }

    /** @param list<mixed> $rows */
    private static function encodeRows(PackerInterface $p, array $rows): string
    {
        $out = $p->packArrayLen(count($rows));
        foreach ($rows as $row) {
            $cells = SqlValueCodec::listOf($row);
            $out .= $p->packArrayLen(count($cells));
            foreach ($cells as $vj) { $out .= SqlValueCodec::encode($p, $vj); }
        }
        return $out;
    }
}
