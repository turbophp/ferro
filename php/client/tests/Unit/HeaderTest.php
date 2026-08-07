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
    /**
     * The PHP half of the M1-S8a skew tripwire (the Rust half is
     * `ferro-proto/tests/header.rs::a_frame_from_the_previous_protocol_version_is_rejected_by_the_header`).
     * A frame written by an OLDER-protocol engine is refused at byte 1, before a single payload byte
     * is unpacked — which is what makes the `HelloAck.pools` reshape safe: an old engine's ack never
     * reaches {@see \Ferro\Protocol\HelloAck::decode}.
     *
     * The expectation is derived from `C::PROTOCOL_VERSION`, never a literal — a hand-written
     * protocol constant is a charter rule 2 defect, tests included.
     *
     * What this does NOT prove: that the failure is a TYPED handshake rejection. It is not. It is a
     * codec error, and the message the operator sees is "bad version N" (PROTOCOL.md §1).
     */
    public function testRejectsThePreviousProtocolVersionBeforeReadingAnyPayload(): void
    {
        $stale = C::PROTOCOL_VERSION - 1;
        $b = (new Header(0, C::SERVICE_CORE, C::METHOD_CORE_HELLO_ACK, 1, 0))->encode();
        $b[1] = chr($stale);
        $this->expectException(CodecException::class);
        $this->expectExceptionMessage('bad version ' . $stale);
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
