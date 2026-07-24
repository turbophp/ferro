<?php // /php/client/src/Protocol/ExecOk.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Bespoke positional codec for the terminal EXEC success body (the `Outcome::Ok` body). Mirrors the
 * Rust `messages::sql::ExecOk` BYTES: a fixarray of 5 fields — cols (array of `[name, tag]`), rows
 * (array of rows, each an array of TypedValue cells), affected, last_insert_id (Option<Value>:
 * bare nil ⇒ absent), stats `[queue_us, exec_us, rows, bytes]` (see /proto/PROTOCOL.md §8.2). This
 * body composes inside `Outcome::Ok` (status 0) exactly as the Rust splices it. Encodes the ExecOk
 * body only; the caller wraps it in the Outcome envelope on the wire.
 */
final class ExecOk
{
    /** @param array<string,mixed> $m @return string the encoded fixarray(5) ExecOk body (no Outcome wrapper) */
    public static function encode(array $m, PackerInterface $p): string
    {
        $colsBytes = self::encodeCols($p, SqlValueCodec::listOf($m['cols'] ?? null));
        $rowsBytes = self::encodeRows($p, SqlValueCodec::listOf($m['rows'] ?? null));
        $affected = $p->packUint(SqlValueCodec::toInt($m['affected'] ?? 0));
        $last = ($m['last_insert_id'] ?? null) === null
            ? $p->packNil()
            : SqlValueCodec::encode($p, $m['last_insert_id']);
        $stats = self::encodeStats($p, $m['stats'] ?? null);

        return $p->packArrayLen(5) . $colsBytes . $rowsBytes . $affected . $last . $stats;
    }

    /**
     * Map an already-unpacked 5-element wire array back to the "message" JSON shape.
     * @param array<int,mixed> $w
     * @return array<string,mixed>
     */
    public static function mapFromWire(array $w): array
    {
        $w = array_values($w);
        if (count($w) !== 5) { throw new CodecException('ExecOk arity != 5'); }

        $cols = [];
        foreach (SqlValueCodec::listOf($w[0]) as $c) {
            if (!is_array($c) || count($c) !== 2) { throw new CodecException('bad ColMeta wire'); }
            $cc = array_values($c);
            $cols[] = ['name' => SqlValueCodec::toStr($cc[0]), 'tag' => SqlValueCodec::toInt($cc[1])];
        }

        $rows = [];
        foreach (SqlValueCodec::listOf($w[1]) as $row) {
            $rows[] = array_map([SqlValueCodec::class, 'fromWire'], SqlValueCodec::listOf($row));
        }

        $statsW = SqlValueCodec::listOf($w[4]);
        return [
            'cols' => $cols,
            'rows' => $rows,
            'affected' => SqlValueCodec::toInt($w[2]),
            'last_insert_id' => $w[3] === null ? null : SqlValueCodec::fromWire($w[3]),
            'stats' => [
                'queue_us' => SqlValueCodec::toInt($statsW[0] ?? 0),
                'exec_us' => SqlValueCodec::toInt($statsW[1] ?? 0),
                'rows' => SqlValueCodec::toInt($statsW[2] ?? 0),
                'bytes' => SqlValueCodec::toInt($statsW[3] ?? 0),
            ],
        ];
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

    private static function encodeStats(PackerInterface $p, mixed $stats): string
    {
        $s = is_array($stats) ? $stats : [];
        return $p->packArrayLen(4)
            . $p->packUint(SqlValueCodec::toInt($s['queue_us'] ?? 0))
            . $p->packUint(SqlValueCodec::toInt($s['exec_us'] ?? 0))
            . $p->packUint(SqlValueCodec::toInt($s['rows'] ?? 0))
            . $p->packUint(SqlValueCodec::toInt($s['bytes'] ?? 0));
    }
}
