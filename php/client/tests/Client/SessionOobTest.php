<?php // /php/client/tests/Client/SessionOobTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\ProtocolException;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Client\Transport;
use Ferro\Protocol\Codec;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\OobRef;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeFdTransport;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * The session half of the M3-D3 out-of-band path (SPEC §5.1), offline: `MEMFD_RX` is advertised
 * exactly when the transport receives fds, an `END | OOB_FD` terminal is resolved into the frame it
 * stands for by reading its fd (and closing it), and every way the two ends could disagree about
 * the byte stream is a desync that poisons the session rather than a guess.
 *
 * A temp file stands in for the memfd: what the session does with the fd — `fstat`, read from
 * offset 0, close — is the same for both. The kernel side (sealing, `SCM_RIGHTS`) is the live
 * test's (`Live/OobLiveTest`).
 */
final class SessionOobTest extends TestCase
{
    private const RID = 1;

    public function testHelloAdvertisesMemfdRxExactlyWhenTheTransportReceivesFds(): void
    {
        foreach ([true, false] as $receives) {
            $t = new FakeFdTransport($receives);
            $t->feed(self::helloAck());
            (new Session($t))->hello();
            $features = self::sentHelloFeatures($t->written);
            $this->assertSame($receives, ($features & C::FEATURE_CLIENT_MEMFD_RX) !== 0,
                'MEMFD_RX iff the transport receives fds');
            $this->assertNotSame(0, $features & C::FEATURE_CLIENT_FIBERS, 'FIBERS is still sent');
        }
        // A transport that cannot receive fds at all never advertises it.
        $plain = new FakeTransport();
        $plain->feed(self::helloAck());
        (new Session($plain))->hello();
        $this->assertSame(0, self::sentHelloFeatures($plain->written) & C::FEATURE_CLIENT_MEMFD_RX);
    }

    public function testAnOobTerminalIsReadFromItsFdWhichIsThenClosed(): void
    {
        $p = PackerFactory::forEncode();
        $inline = Outcome::ok($p->packStr(str_repeat('r', 5000)))->encode($p);
        $t = new FakeFdTransport();
        $fd = self::memfdStandIn($inline);
        $t->queueFd($fd);
        $t->feed(self::oobFrame(self::RID, strlen($inline)));

        $session = new Session($t, new RequestIdAllocator(0));
        $outcome = $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'req');

        $this->assertTrue($outcome->isOk());
        $off = 0;
        $this->assertSame(str_repeat('r', 5000), (new PurePacker())->unpack($outcome->body(), $off),
            'the memfd bytes are decoded exactly as the inline payload would be');
        $this->assertFalse(is_resource($fd), 'the received fd is closed once read');
        $this->assertSame(1, $session->oobPayloadsReceived());
    }

    /** The fd's shared OFFSET must not matter: a sender that left it at EOF still reads in full. */
    public function testTheMemfdIsReadFromOffsetZeroWhateverItsSharedOffset(): void
    {
        $p = PackerFactory::forEncode();
        $inline = Outcome::ok($p->packStr('offset'))->encode($p);
        $t = new FakeFdTransport();
        $fd = self::memfdStandIn($inline);
        fseek($fd, 0, SEEK_END);
        $t->queueFd($fd);
        $t->feed(self::oobFrame(self::RID, strlen($inline)));
        $outcome = (new Session($t, new RequestIdAllocator(0)))->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'r');
        $this->assertTrue($outcome->isOk());
    }

    /**
     * FIFO pairing: two OOB terminals for two requests in flight, their fds queued in frame order.
     * Each request must get ITS payload — a pairing by anything but order would swap them.
     */
    public function testTwoOobTerminalsTakeTheirFdsInFrameOrder(): void
    {
        $p = PackerFactory::forEncode();
        $a = Outcome::ok($p->packStr('first'))->encode($p);
        $b = Outcome::ok($p->packStr('second-longer'))->encode($p);
        $t = new FakeFdTransport();
        $t->queueFd(self::memfdStandIn($a));
        $t->queueFd(self::memfdStandIn($b));
        $t->feed(self::oobFrame(1, strlen($a)) . self::oobFrame(2, strlen($b)));

        $session = new Session($t, new RequestIdAllocator(0));
        $r1 = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $r2 = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b');
        // Await in REVERSE: the router reads both frames while waiting for the second.
        $o2 = $session->awaitTerminal($r2);
        $o1 = $session->awaitTerminal($r1);
        $off = 0;
        $this->assertSame('second-longer', (new PurePacker())->unpack($o2->body(), $off));
        $off = 0;
        $this->assertSame('first', (new PurePacker())->unpack($o1->body(), $off));
    }

    /** @return iterable<string, array{0:\Closure(FakeFdTransport):void, 1:bool}> */
    public static function desyncs(): iterable
    {
        $p = PackerFactory::forEncode();
        $inline = Outcome::ok($p->packNil())->encode($p);
        yield 'no fd queued for the frame' => [static function (FakeFdTransport $t) use ($inline): void {
            $t->feed(self::oobFrame(self::RID, strlen($inline)));
        }, true];
        yield 'memfd shorter than len' => [static function (FakeFdTransport $t) use ($inline): void {
            $t->queueFd(self::memfdStandIn($inline));
            $t->feed(self::oobFrame(self::RID, strlen($inline) + 1));
        }, true];
        yield 'memfd longer than len' => [static function (FakeFdTransport $t) use ($inline): void {
            $t->queueFd(self::memfdStandIn($inline . 'x'));
            $t->feed(self::oobFrame(self::RID, strlen($inline)));
        }, true];
        yield 'fd_index other than 0' => [static function (FakeFdTransport $t) use ($inline): void {
            $t->queueFd(self::memfdStandIn($inline));
            $t->feed(self::oobFrame(self::RID, strlen($inline), fdIndex: 1));
        }, true];
        yield 'an encoding this client does not implement' => [static function (FakeFdTransport $t) use ($inline): void {
            $t->queueFd(self::memfdStandIn($inline));
            $t->feed(self::oobFrame(self::RID, strlen($inline), encoding: C::OOB_ENCODING_FRAME_PAYLOAD + 1));
        }, true];
        yield 'len above MAX_FRAME_PAYLOAD' => [static function (FakeFdTransport $t) use ($inline): void {
            $t->queueFd(self::memfdStandIn($inline));
            $t->feed(self::oobFrame(self::RID, C::MAX_FRAME_PAYLOAD + 1));
        }, true];
        yield 'an OOB frame on a session that never advertised MEMFD_RX' => [static function (FakeFdTransport $t) use ($inline): void {
            $t->queueFd(self::memfdStandIn($inline));
            $t->feed(self::oobFrame(self::RID, strlen($inline)));
        }, false];
    }

    /**
     * @param \Closure(FakeFdTransport):void $arrange
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('desyncs')]
    public function testEveryDisagreementIsADesyncThatPoisonsTheSession(\Closure $arrange, bool $receives): void
    {
        $t = new FakeFdTransport($receives);
        $arrange($t);
        $session = new Session($t, new RequestIdAllocator(0));
        try {
            $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'req');
            $this->fail('a disagreement about the OOB frame must not be accepted');
        } catch (ProtocolException) {
            // expected
        }
        $this->assertTrue($session->isPoisoned(), 'the byte stream can no longer be trusted');
        $this->assertTrue($t->closed);
        $this->assertSame(0, $session->oobPayloadsReceived());
    }

    /** The capability probe is exactly Linux + ext-sockets — what the HELLO bit is gated on. */
    public function testTheRealTransportsFdCapabilityIsHonest(): void
    {
        $expected = PHP_OS_FAMILY === 'Linux' && extension_loaded('sockets');
        $this->assertSame($expected, Transport::canReceiveFds());
    }

    // ---- helpers ------------------------------------------------------------------------------

    private static function helloAck(): string
    {
        $p = PackerFactory::forEncode();
        $payload = Message::encode('hello_ack', [
            'engine_version' => 1,
            'boot_epoch' => 7,
            'features' => C::FEATURE_ENGINE_MEMFD,
            'pools' => [],
            'type_registry_hash' => C::TYPE_REGISTRY_HASH,
        ], $p);
        return (new Codec())->encodeFrame(
            new Header(0, C::SERVICE_CORE, C::METHOD_CORE_HELLO_ACK, 1, strlen($payload)),
            $payload,
        );
    }

    private static function sentHelloFeatures(string $written): int
    {
        $h = Header::decode(substr($written, 0, 16));
        $off = 0;
        $w = (new PurePacker())->unpack(substr($written, 16, $h->payloadLen), $off);
        self::assertIsArray($w);
        $features = array_values($w)[4] ?? null;
        self::assertIsInt($features);
        return $features;
    }

    private static function oobFrame(
        int $rid,
        int $len,
        int $fdIndex = 0,
        int $encoding = C::OOB_ENCODING_FRAME_PAYLOAD,
    ): string {
        $ref = OobRef::encode(['fd_index' => $fdIndex, 'len' => $len, 'encoding' => $encoding], PackerFactory::forEncode());
        return (new Codec())->encodeFrame(
            new Header(C::FLAG_END | C::FLAG_OOB_FD, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, strlen($ref)),
            $ref,
        );
    }

    /** @return resource */
    private static function memfdStandIn(string $bytes)
    {
        $f = tmpfile();
        self::assertIsResource($f);
        fwrite($f, $bytes);
        return $f;
    }
}
