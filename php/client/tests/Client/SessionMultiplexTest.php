<?php // /php/client/tests/Client/SessionMultiplexTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * M3-D1 (SPEC §10.1): several requests in flight on one session, with every frame routed by its
 * `request_id`. Each test owns its wire bytes, so what is asserted is exactly what the router did.
 */
final class SessionMultiplexTest extends TestCase
{
    /** Terminal frame for `$rid` whose Ok body is the msgpack string `$marker`. */
    private static function okTerminal(int $rid, string $marker): string
    {
        $packer = PackerFactory::forEncode();
        $payload = Outcome::ok($packer->packStr($marker))->encode($packer);
        return (new Codec())->encodeFrame(
            new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, strlen($payload)),
            $payload,
        );
    }

    private static function marker(Outcome $outcome): string
    {
        $off = 0;
        $value = PackerFactory::forDecode()->unpack($outcome->body(), $off);
        self::assertIsString($value);
        return $value;
    }

    /** @return list<Header> every frame header the session wrote, in order. */
    private static function writtenHeaders(FakeTransport $t): array
    {
        $out = [];
        $off = 0;
        while ($off < strlen($t->written)) {
            $h = Header::decode(substr($t->written, $off, 16));
            $out[] = $h;
            $off += 16 + $h->payloadLen;
        }
        return $out;
    }

    /**
     * The point of the slice: both requests are WRITTEN before either terminal is read, and a
     * terminal that arrives out of order is kept for its own awaiter rather than misread.
     */
    public function testTerminalsArrivingOutOfOrderReachTheirOwnAwaiters(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));

        $first = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $second = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b');
        $this->assertSame([1, 2], [$first, $second]);
        $this->assertCount(2, self::writtenHeaders($t), 'both requests are on the wire before any read');

        // The engine answers the SECOND request first.
        $t->feed(self::okTerminal(2, 'second') . self::okTerminal(1, 'first'));

        $this->assertSame('first', self::marker($session->awaitTerminal($first)));
        $this->assertSame('second', self::marker($session->awaitTerminal($second)));
        $this->assertFalse($session->isPending($first));
        $this->assertFalse($session->isPending($second));
    }

    /** A terminal can be consumed only once; awaiting it again is a usage error, not a wire read. */
    public function testAConsumedTerminalCannotBeAwaitedAgain(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $rid = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $t->feed(self::okTerminal($rid, 'x'));
        $session->awaitTerminal($rid);

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('not in flight');
        $session->awaitTerminal($rid);
    }

    /**
     * A frame for an id that is not in flight means the two ends disagree, so nothing after it can
     * be trusted: ProtocolException now, and the session refuses to send anything more.
     */
    public function testAFrameForAnIdNotInFlightPoisonsTheSession(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $rid = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $t->feed(self::okTerminal(99, 'stray'));

        try {
            $session->awaitTerminal($rid);
            $this->fail('a stray frame must not be accepted');
        } catch (ProtocolException $e) {
            $this->assertStringContainsString('request_id 99', $e->getMessage());
        }
        $this->assertTrue($session->isPoisoned());
        $this->assertTrue($t->closed);

        $writes = $t->writeCalls;
        try {
            $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b');
            $this->fail('a poisoned session must not send');
        } catch (TransportException $e) {
            $this->assertTrue($e->requestUnsent());
        }
        $this->assertSame($writes, $t->writeCalls, 'nothing reached the transport');
    }

    /** A session-fatal terminal on request_id 0 (an engine-side protocol error). */
    private static function fatalFrame(string $message): string
    {
        $packer = PackerFactory::forEncode();
        $ep = new ErrorPayload(C::ERR_PROTOCOL, C::BRANCH_NON_RETRYABLE, null, null, $message, null, null);
        $payload = Outcome::error($ep)->encode($packer);
        return (new Codec())->encodeFrame(new Header(C::FLAG_END, C::SERVICE_CORE, 0, 0, strlen($payload)), $payload);
    }

    /**
     * M3-D1a review F1. A session-fatal terminal decides NO pending request's fate: it may concern
     * any of them, or a control frame. The session keeps reading, because the engine drains each
     * in-flight request's OWN terminal before it closes — and those real terminals are what decide.
     */
    public function testASessionFatalDoesNotDecideThePendingRequestsTheEngineStillAnswers(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $a = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $b = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b');

        // The fatal arrives first, then the engine's drain delivers request a's real terminal, then
        // EOF (nothing more is fed) before request b is answered.
        $t->feed(self::fatalFrame('a malformed frame on another request') . self::okTerminal($a, 'applied'));

        $this->assertSame('applied', self::marker($session->awaitTerminal($a)), 'the drained terminal is read, not discarded');

        try {
            $session->awaitTerminal($b);
            $this->fail('request b got no terminal');
        } catch (ConnectionLostException $e) {
            $this->assertNull($e->errorPayload(), 'the fatal payload is NOT attributed to request b');
            $this->assertStringContainsString('a malformed frame on another request', $e->getMessage());
        }
    }

    /** After a fatal the session sends nothing: a new request is refused as never sent. */
    public function testAfterASessionFatalNothingMoreIsSent(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $a = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $t->feed(self::fatalFrame('protocol fault') . self::okTerminal($a, 'x'));
        $session->awaitTerminal($a);

        $writes = $t->writeCalls;
        try {
            $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b');
            $this->fail('nothing may be sent after a fatal');
        } catch (TransportException $e) {
            $this->assertTrue($e->requestUnsent());
        }
        $this->assertSame($writes, $t->writeCalls);
    }

    /**
     * A dead transport fails every pending request as SENT: each frame was completely written, so
     * none of them may be reported "not sent" (that would let a caller retry a write that ran).
     */
    public function testATransportFailureFailsEveryPendingRequestAsSent(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $a = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $b = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b');
        // Nothing is fed: the first read fails (the fake transport's EOF).

        foreach ([$a, $b] as $rid) {
            try {
                $session->awaitTerminal($rid);
                $this->fail("request {$rid} must fail");
            } catch (TransportException $e) {
                $this->assertFalse($e->requestUnsent(), "request {$rid} was sent; its fate is unknown, not 'unsent'");
            }
        }
        $this->assertTrue($session->isPoisoned());
    }

    /**
     * At the in-flight limit, submit READS before it writes: it waits for a terminal to free a slot,
     * and keeps that terminal for its awaiter.
     */
    public function testAtTheLimitSubmitReadsATerminalBeforeWriting(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0), maxInFlight: 1);
        $a = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $t->feed(self::okTerminal($a, 'first'));

        $b = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b');
        $this->assertCount(2, self::writtenHeaders($t));
        // w (request a), then the reads of a's terminal (header + payload), THEN w (request b).
        $this->assertSame('w', $t->events[0]);
        $this->assertSame('w', $t->events[array_key_last($t->events)]);
        $this->assertContains('r', array_slice($t->events, 1, -1), 'a terminal was read before the second write');
        $this->assertSame('first', self::marker($session->awaitTerminal($a)), 'the terminal read to free the slot was kept');

        $t->feed(self::okTerminal($b, 'second'));
        $this->assertSame('second', self::marker($session->awaitTerminal($b)));
    }

    /** The control: below the limit, submit never reads (nothing is fed, and nothing fails). */
    public function testBelowTheLimitSubmitDoesNotRead(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0), maxInFlight: 2);
        $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b');
        $this->assertCount(2, self::writtenHeaders($t));
        $this->assertFalse($session->isPoisoned(), 'no read was attempted');
    }

    /**
     * Review F2/F2b: a request waiting for an in-flight slot on a session that dies was never
     * written, so it is `requestNotSent` — never a sent-and-lost write.
     */
    public function testARequestWaitingForASlotOnADyingSessionIsNotSent(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0), maxInFlight: 1);
        $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        // Nothing fed: the read for a free slot hits EOF.
        try {
            $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b');
            $this->fail('the second request must not be reported sent');
        } catch (TransportException $e) {
            $this->assertTrue($e->requestUnsent());
        }
        $this->assertSame(1, $t->writeCalls, 'only request a was written');
    }

    /** Review F3: closing with requests pending fails them as SENT, not with a read on a closed socket. */
    public function testClosingWithRequestsPendingFailsThemAsSent(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $a = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $session->close();
        $reads = count(array_filter($t->events, static fn (string $e): bool => $e === 'r'));
        try {
            $session->awaitTerminal($a);
            $this->fail('a closed session cannot answer');
        } catch (TransportException $e) {
            $this->assertFalse($e->requestUnsent(), 'request a was sent');
        }
        $this->assertSame($reads, count(array_filter($t->events, static fn (string $e): bool => $e === 'r')), 'no read after close');
    }

    /** Review F6: a discarded request's terminal is thrown away on arrival, not kept. */
    public function testADiscardedRequestsTerminalIsNotKept(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $a = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $b = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b');
        $session->discard($a);
        $this->assertFalse($session->isPending($a));
        $t->feed(self::okTerminal($a, 'dropped') . self::okTerminal($b, 'kept'));
        $this->assertSame('kept', self::marker($session->awaitTerminal($b)));
        $inbox = new \ReflectionProperty(Session::class, 'inbox');
        $this->assertSame([], $inbox->getValue($session), 'nothing left behind');
    }

    /** Review F6: a non-terminal frame for a buffered request is a desync, so it poisons the session. */
    public function testANonTerminalFrameForABufferedRequestPoisons(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $a = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
        $packer = PackerFactory::forEncode();
        $payload = Outcome::ok($packer->packNil())->encode($packer);
        $t->feed((new Codec())->encodeFrame(new Header(0, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $a, strlen($payload)), $payload));
        try {
            $session->awaitTerminal($a);
            $this->fail('a non-terminal frame for a buffered request is a fault');
        } catch (ProtocolException) {
        }
        $this->assertTrue($session->isPoisoned());
    }

    /** A PING while requests are in flight: its PONG is routed, and the request terminals are kept. */
    public function testAPingWhileRequestsAreInFlight(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $a = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a'); // rid 1; the ping takes rid 2

        $packer = PackerFactory::forEncode();
        $pong = Message::encode('pong', ['token' => 7], $packer);
        $t->feed(self::okTerminal($a, 'first')
            . (new Codec())->encodeFrame(new Header(0, C::SERVICE_CORE, C::METHOD_CORE_PONG, 2, strlen($pong)), $pong));

        $session->ping(7);
        $this->assertSame('first', self::marker($session->awaitTerminal($a)));
    }

    /** After the u32 wrap, an id still in flight is skipped rather than reused. */
    public function testAWrappedIdThatIsStillPendingIsSkipped(): void
    {
        $t = new FakeTransport();
        // The allocator's next value is 0xFFFFFFFF, then it wraps to 1.
        $session = new Session($t, new RequestIdAllocator(0xFFFFFFFE));
        $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a'); // 0xFFFFFFFF
        $first = $session->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b'); // 1
        $this->assertSame(1, $first);

        $session2 = new Session($t2 = new FakeTransport(), $ids = new RequestIdAllocator(0));
        $pending = $session2->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'x'); // 1, left pending
        // Force the allocator back to just below 1 so the next value would collide with it.
        $rewind = new \ReflectionProperty(RequestIdAllocator::class, 'last');
        $rewind->setValue($ids, 0);
        $next = $session2->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'y');
        $this->assertSame(1, $pending);
        $this->assertSame(2, $next, 'id 1 is still in flight, so it is skipped');
        $this->assertCount(2, self::writtenHeaders($t2));
    }
}
