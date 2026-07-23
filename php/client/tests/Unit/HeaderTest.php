<?php // /php/client/tests/Unit/HeaderTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;
use Ferro\Protocol\Header;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use PHPUnit\Framework\TestCase;

final class HeaderTest extends TestCase
{
    public function testRoundtrip(): void
    {
        $h = new Header(C::FLAG_END, C::SERVICE_CORE, C::METHOD_CORE_PING, 0x0A0B0C0D, 1);
        $b = $h->encode();
        $this->assertSame(16, strlen($b));
        $this->assertSame(C::MAGIC, ord($b[0]));
        $d = Header::decode($b);
        $this->assertSame($h->requestId, $d->requestId);
        $this->assertSame($h->payloadLen, $d->payloadLen);
    }
    public function testRejectsBadMagic(): void
    {
        $b = (new Header(0, 1, 3, 1, 0))->encode(); $b[0] = "\x00";
        $this->expectException(CodecException::class);
        Header::decode($b);
    }
    public function testRejectsOversizeLen(): void
    {
        $b = (new Header(0, 2, 1, 1, 0))->encode();
        $big = pack('V', C::MAX_FRAME_PAYLOAD + 1);
        $b = substr($b, 0, 12) . $big;
        $this->expectException(CodecException::class);
        Header::decode($b);
    }
}
