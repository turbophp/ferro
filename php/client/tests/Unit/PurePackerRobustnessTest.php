<?php // /php/client/tests/Unit/PurePackerRobustnessTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\Value;
use PHPUnit\Framework\TestCase;

/**
 * S1 hardening: every raw byte/substring read in PurePacker must be bounds-checked so a truncated
 * or lying-length frame throws CodecException — never an E_WARNING, a fabricated 0/"", or an
 * unbounded allocation/loop.
 */
final class PurePackerRobustnessTest extends TestCase
{
    public function testTruncatedEmptyBufferThrows(): void
    {
        $this->expectException(CodecException::class);
        $o = 0;
        (new PurePacker())->unpack('', $o);
    }

    public function testTruncatedArrayBodyThrows(): void
    {
        // 0x92 = fixarray(2), but only one more byte follows: truncated before the second element.
        $this->expectException(CodecException::class);
        $o = 0;
        (new PurePacker())->unpack("\x92\x02", $o);
    }

    public function testTruncatedFixstrBodyThrows(): void
    {
        // 0xa5 = fixstr(5), but only 2 payload bytes follow.
        $this->expectException(CodecException::class);
        $o = 0;
        (new PurePacker())->unpack("\xa5ab", $o);
    }

    public function testLyingArrayLengthThrowsFast(): void
    {
        // array32 claiming ~4e9 elements with zero backing bytes must throw immediately via the
        // upfront bound check in unpackArray() — not attempt to allocate/loop billions of times.
        $this->expectException(CodecException::class);
        $o = 0;
        (new PurePacker())->unpack("\xdd\xff\xff\xff\xff", $o);
    }

    public function testNegativeIntDecodeRoundtrip(): void
    {
        // First test of the PHP negative-int DECODE path: 0xd0/0xd1/0xd2/0xd3 + sign extension.
        $p = new PurePacker();
        foreach ([-1, -200, -40000, -3000000000] as $v) {
            $bytes = $p->packInt($v);
            $o = 0;
            $this->assertSame($v, $p->unpack($bytes, $o), "roundtrip for {$v}");
            $this->assertSame(strlen($bytes), $o, "fully consumed encoding of {$v}");
        }
    }

    public function testValueDecodeRoundtrip(): void
    {
        // Value::decode() had zero test coverage before this.
        $p = new PurePacker();
        $expected = Value::i64(-200);
        $bytes = $expected->encode($p);
        $o = 0;
        $decoded = Value::decode($p, $bytes, $o);
        $this->assertSame($expected->tag, $decoded->tag);
        $this->assertSame($expected->data, $decoded->data);
    }

    public function testPackUintSmallStringNarrows(): void
    {
        $p = new PurePacker();
        $this->assertSame("\x07", $p->packUint('7'));
        $this->assertSame("\xcd\x01\x2c", $p->packUint('300'));
    }

    public function testPackUintOverflowThrows(): void
    {
        $this->expectException(CodecException::class);
        (new PurePacker())->packUint('18446744073709551616'); // 2^64, one past u64 max
    }
}
