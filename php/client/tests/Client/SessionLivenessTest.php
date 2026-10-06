<?php // /php/client/tests/Client/SessionLivenessTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\TransportException;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeTimeoutTransport;
use PHPUnit\Framework\TestCase;

/**
 * M3-D1c: silence is a liveness question, not a failure — and a request deadline cancels only that
 * request. Before this, a read that waited the transport's timeout closed the whole session, so any
 * statement slower than the read timeout (5 s by default) failed — a write as `Indeterminate` — and
 * took every other in-flight request with it.
 */
final class SessionLivenessTest extends TestCase
{
    private static function frame(int $flags, int $service, int $method, int $rid, string $payload): string
    {
        return (new Codec())->encodeFrame(new Header($flags, $service, $method, $rid, strlen($payload)), $payload);
    }

    private static function helloAck(): string
    {
        $payload = Message::encode('hello_ack', [
            'engine_version' => 1, 'boot_epoch' => 1, 'features' => 0,
            'pools' => [['name' => 'default', 'kind' => 'postgres', 'server_version' => null, 'literals_are_standard' => true]],
            'type_registry_hash' => C::TYPE_REGISTRY_HASH,
        ], PackerFactory::forEncode());
        return self::frame(0, C::SERVICE_CORE, C::METHOD_CORE_HELLO_ACK, 0, $payload);
    }

    private static function ok(int $rid): string
    {
        $p = PackerFactory::forEncode();
        $body = ExecOk::encode([
            'cols' => [], 'rows' => [], 'affected' => 3, 'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
        ], $p);
        return self::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, Outcome::ok($body)->encode($p));
    }

    private static function pong(int $rid): string
    {
        return self::frame(0, C::SERVICE_CORE, C::METHOD_CORE_PONG, $rid, Message::encode('pong', ['token' => $rid], PackerFactory::forEncode()));
    }

    /** @return list<Header> every frame the session wrote */
    private static function written(FakeTimeoutTransport $t): array
    {
        $out = [];
        $bytes = $t->written;
        while ($bytes !== '') {
            [$h] = (new Codec())->decodeFrame($bytes);
            $bytes = (string) substr($bytes, 16 + $h->payloadLen);
            $out[] = $h;
        }
        return $out;
    }

    private static function session(FakeTimeoutTransport $t): Session
    {
        $t->feed(self::helloAck());
        $s = new Session($t, new RequestIdAllocator(0));
        $s->hello();
        return $s;
    }

    /**
     * A statement slower than the read timeout: the session PINGs, the engine answers the PING, and
     * the statement's own terminal arrives later — the request SUCCEEDS on the same session.
     */
    public function testSilenceIsProbedAndAStatementSlowerThanTheReadTimeoutSucceeds(): void
    {
        $t = new FakeTimeoutTransport();
        $s = self::session($t);
        $t->onTimeout = static function (FakeTimeoutTransport $t): void {
            // 1st silence: the session has just been asked to probe; the engine answers the PING
            // (id 2: the request is 1). 2nd silence: the statement finishes.
            if ($t->timeouts === 1) {
                $t->feed(self::pong(2));
            } elseif ($t->timeouts === 2) {
                $t->feed(self::ok(1));
            }
        };
        $out = $s->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'x');
        $this->assertTrue($out->isOk());
        $this->assertFalse($s->isPoisoned());
        $methods = array_map(static fn (Header $h): int => $h->method, self::written($t));
        $this->assertContains(C::METHOD_CORE_PING, $methods, 'silence was probed with a PING');
    }

    /** No answer to the PING within another read timeout: the session closes, the request is lost-as-sent. */
    public function testAnUnansweredProbeClosesTheSession(): void
    {
        $t = new FakeTimeoutTransport(0.01);
        $s = self::session($t);
        try {
            $s->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'x');
            $this->fail('a dead engine must not hang the request');
        } catch (TransportException $e) {
            $this->assertFalse($e->requestUnsent(), 'the request WAS sent: its fate is unknown');
            $this->assertStringContainsString('liveness PING', $e->getMessage());
        }
        $this->assertTrue($s->isPoisoned());
        $pings = array_filter(self::written($t), static fn (Header $h): bool => $h->method === C::METHOD_CORE_PING);
        $this->assertCount(1, $pings, 'one probe, not one per timeout');
    }

    /**
     * A timeout in the MIDDLE of a frame keeps the stream in step: the header was read, the payload
     * timed out, and the next read resumes with the payload instead of reading it as a new header.
     */
    public function testATimeoutMidFrameKeepsTheStreamInStep(): void
    {
        $t = new FakeTimeoutTransport();
        $s = self::session($t);
        $full = self::ok(1);
        $t->onTimeout = static function (FakeTimeoutTransport $t) use ($full): void {
            if ($t->timeouts === 1) {
                $t->feed(substr($full, 16 + 3)); // the rest of the payload; then the PONG
                $t->feed(self::pong(2));
            }
        };
        $t->feed(substr($full, 0, 16 + 3)); // the header and 3 bytes of payload
        $out = $s->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'x');
        $this->assertTrue($out->isOk());
        $this->assertFalse($s->isPoisoned());
    }

    /**
     * A request deadline CANCELs only that request; the engine's terminal decides its fate, and the
     * session — and every other request on it — carries on.
     */
    public function testADeadlineCancelsOnlyThatRequest(): void
    {
        $t = new FakeTimeoutTransport();
        $s = self::session($t);
        $slow = $s->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'slow');
        $other = $s->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'other');
        $s->setDeadline($slow, microtime(true) - 1.0); // already due
        $t->onTimeout = static function (FakeTimeoutTransport $t) use ($slow, $other): void {
            // The engine answers the CANCEL with the cancelled terminal, and the other request.
            $t->feed(self::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $slow, Outcome::cancelled()->encode(PackerFactory::forEncode())));
            $t->feed(self::ok($other));
            $t->onTimeout = null;
        };
        $out = $s->awaitTerminal($slow);
        $this->assertTrue($out->isCancelled());
        $this->assertTrue($s->awaitTerminal($other)->isOk(), 'the other request is unaffected');
        $this->assertFalse($s->isPoisoned());
        $cancels = array_filter(self::written($t), static fn (Header $h): bool => ($h->flags & C::FLAG_CANCEL) !== 0);
        $this->assertCount(1, $cancels);
        $this->assertSame($slow, array_values($cancels)[0]->requestId, 'CANCEL names the due request only');
    }

    /** An engine that answers neither the deadline nor the CANCEL: after one more read timeout, the session closes. */
    public function testADeadlineWhoseCancelGoesUnansweredClosesTheSession(): void
    {
        $t = new FakeTimeoutTransport(0.01);
        $s = self::session($t);
        $rid = $s->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'slow');
        $s->setDeadline($rid, microtime(true) - 1.0);
        try {
            $s->awaitTerminal($rid);
            $this->fail('must not hang');
        } catch (TransportException $e) {
            $this->assertFalse($e->requestUnsent());
        }
        $this->assertTrue($s->isPoisoned());
    }

    /** The read wait is shortened to a nearer deadline, so the session acts on time. */
    public function testTheReadWaitIsShortenedToTheNearestDeadline(): void
    {
        $t = new FakeTimeoutTransport(5.0);
        $s = self::session($t);
        $rid = $s->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'x');
        $s->setDeadline($rid, microtime(true) + 0.2);
        $t->onTimeout = static function (FakeTimeoutTransport $t) use ($rid): void {
            $t->feed(self::ok($rid));
        };
        $s->awaitTerminal($rid);
        $this->assertNotEmpty($t->waits);
        $this->assertLessThanOrEqual(0.2, end($t->waits));
    }

    /** `setRequestTimeout` arms a deadline on every buffered request; none by default. */
    public function testSendRequestArmsTheConfiguredRequestTimeout(): void
    {
        $t = new FakeTimeoutTransport(0.01);
        $s = self::session($t);
        $s->setRequestTimeout(0.0001);
        $t->onTimeout = static function (FakeTimeoutTransport $t): void {
            if ($t->timeouts === 1) {
                usleep(2000); // the 0.1 ms deadline has certainly passed when this read returns
            } elseif ($t->timeouts === 2) {
                $t->feed(self::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, Outcome::cancelled()->encode(PackerFactory::forEncode())));
            }
        };
        $this->assertTrue($s->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'x')->isCancelled());
        $cancels = array_filter(self::written($t), static fn (Header $h): bool => ($h->flags & C::FLAG_CANCEL) !== 0);
        $this->assertCount(1, $cancels);
    }
}
