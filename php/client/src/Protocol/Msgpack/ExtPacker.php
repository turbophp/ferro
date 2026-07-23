<?php // /php/client/src/Protocol/Msgpack/ExtPacker.php
declare(strict_types=1);
namespace Ferro\Protocol\Msgpack;
use Ferro\Protocol\CodecException;

/** ext-msgpack fast path. Only used where it byte-matches the canonical profile (conformance-gated). */
final class ExtPacker implements PackerInterface
{
    public function __construct() { if (!\extension_loaded('msgpack')) { throw new CodecException('ext-msgpack absent'); } }
    public function packNil(): string { return \msgpack_pack(null); }
    public function packBool(bool $b): string { return \msgpack_pack($b); }
    public function packInt(int $n): string { return \msgpack_pack($n); }
    public function packUint(int|string $n): string { return \msgpack_pack(is_string($n) ? (int) $n : $n); }
    public function packFloat64(float $f): string { return \msgpack_pack($f); }
    public function packStr(string $s): string { return \msgpack_pack($s); }
    public function packBin(string $s): string { return \msgpack_pack($s); }
    public function packArrayLen(int $n): string { throw new CodecException('ExtPacker packs whole values, not array headers'); }
    public function unpack(string $buf, int &$offset): mixed { $v = \msgpack_unpack($buf); $offset = strlen($buf); return $v; }
}
