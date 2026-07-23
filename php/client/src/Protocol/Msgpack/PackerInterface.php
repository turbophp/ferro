<?php // /php/client/src/Protocol/Msgpack/PackerInterface.php
declare(strict_types=1);
namespace Ferro\Protocol\Msgpack;

interface PackerInterface
{
    public function packNil(): string;
    public function packBool(bool $b): string;
    public function packInt(int $n): string;         // signed-smallest canonical
    public function packUint(int|string $n): string; // unsigned-smallest; string allows > PHP_INT_MAX
    public function packFloat64(float $f): string;
    public function packStr(string $s): string;
    public function packBin(string $s): string;
    public function packArrayLen(int $n): string;
    /** @return mixed decoded scalar/array; advances $offset */
    public function unpack(string $buf, int &$offset): mixed;
}
