<?php // /php/client/tests/Unit/PurePackerTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;
use Ferro\Protocol\Msgpack\PurePacker;
use PHPUnit\Framework\TestCase;

final class PurePackerTest extends TestCase
{
    public function testCanonicalScalars(): void
    {
        $p = new PurePacker();
        $this->assertSame("\xc0", $p->packNil());
        $this->assertSame("\xc3", $p->packBool(true));
        $this->assertSame("\x01", $p->packInt(1));            // positive fixint
        $this->assertSame("\xcc\xc8", $p->packInt(200));      // uint8 (matches rmp write_sint)
        $this->assertSame("\xd1\xff\x38", $p->packInt(-200)); // int16 (negatives keep the signed marker)
        $this->assertSame("\xcb" . pack('E', 1.5), $p->packFloat64(1.5)); // 'E' = big-endian double
        $this->assertSame("\xa2hi", $p->packStr('hi'));      // fixstr
        $this->assertSame("\xc4\x03\x01\x02\x03", $p->packBin("\x01\x02\x03"));
        $this->assertSame("\x92", $p->packArrayLen(2));      // fixarray(2)
    }

    public function testUint64BeyondPhpIntDecodesToString(): void
    {
        $p = new PurePacker();
        $bytes = "\xcf\xff\xff\xff\xff\xff\xff\xff\xf0"; // uint64 0xFFFFFFFFFFFFFFF0
        $off = 0;
        $this->assertSame('18446744073709551600', $p->unpack($bytes, $off));
    }
}
