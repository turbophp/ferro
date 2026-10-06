<?php // /php/client/tests/Support/HookedPacker.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * The encode packer, with a hook run once on the next pack call after it is armed — a point where
 * the session is between transport operations, as PHP's cycle collector can be (M6-F8 review
 * round 2: a destructor run between two of the deferred writes).
 */
final class HookedPacker implements PackerInterface
{
    /** @var (\Closure(): void)|null */
    public ?\Closure $onPack = null;

    private PackerInterface $inner;

    public function __construct()
    {
        $this->inner = PackerFactory::forEncode();
    }

    private function fire(): void
    {
        if ($this->onPack !== null) {
            $hook = $this->onPack;
            $this->onPack = null;
            $hook();
        }
    }

    public function packNil(): string { $this->fire(); return $this->inner->packNil(); }

    public function packBool(bool $b): string { $this->fire(); return $this->inner->packBool($b); }

    public function packInt(int $n): string { $this->fire(); return $this->inner->packInt($n); }

    public function packUint(int|string $n): string { $this->fire(); return $this->inner->packUint($n); }

    public function packFloat64(float $f): string { $this->fire(); return $this->inner->packFloat64($f); }

    public function packStr(string $s): string { $this->fire(); return $this->inner->packStr($s); }

    public function packBin(string $s): string { $this->fire(); return $this->inner->packBin($s); }

    public function packArrayLen(int $n): string { $this->fire(); return $this->inner->packArrayLen($n); }

    public function unpack(string $buf, int &$offset): mixed { return $this->inner->unpack($buf, $offset); }
}
