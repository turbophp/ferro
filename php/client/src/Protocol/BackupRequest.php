<?php // /php/client/src/Protocol/BackupRequest.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for the ADMIN BACKUP request (service ADMIN, method BACKUP = 1; SPEC §7.6, D15).
 * Mirrors the Rust `messages::admin::BackupRequest` BYTES: a fixarray of 4 fields in declaration
 * order — `pool` (str), `file` (str: a plain FILE NAME the engine places in the pool's D14 allowed
 * directory — never a path), `replace` (bool), `timeout_ms` (`u32 | nil`). Value-free, so this is the
 * plain positional layout the TX messages use. Pinned by /proto/PROTOCOL.md §11 and the
 * `admin_backup_request` vector.
 */
final class BackupRequest
{
    /**
     * @param array<string,mixed> $m
     * @return string the encoded fixarray(4) payload
     */
    public static function encode(array $m, PackerInterface $p): string
    {
        $timeout = $m['timeout_ms'] ?? null;
        return $p->packArrayLen(4)
            . $p->packStr(SqlValueCodec::toStr($m['pool'] ?? ''))
            . $p->packStr(SqlValueCodec::toStr($m['file'] ?? ''))
            . $p->packBool((bool) ($m['replace'] ?? false))
            . ($timeout === null ? $p->packNil() : $p->packUint(SqlValueCodec::toInt($timeout)));
    }

    /**
     * Map an already-unpacked 4-element wire array back to the "message" JSON shape.
     * @param array<int,mixed> $w
     * @return array{pool:string,file:string,replace:bool,timeout_ms:?int}
     */
    public static function mapFromWire(array $w): array
    {
        $w = array_values($w);
        if (count($w) !== 4) { throw new CodecException('BackupRequest arity != 4'); }
        return [
            'pool' => SqlValueCodec::toStr($w[0]),
            'file' => SqlValueCodec::toStr($w[1]),
            'replace' => (bool) $w[2],
            'timeout_ms' => SqlValueCodec::nullableInt($w[3]),
        ];
    }
}
