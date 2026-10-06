<?php // /php/client/tests/Client/HttpUpstreamTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\InFlightLimitException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\ReentrantWriteException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\Backoff;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\RetryPolicy;
use Ferro\Client\Session;
use Ferro\Client\TraceContext;
use Ferro\Client\TransportInterface;
use Ferro\Http\Error\HttpCancelledException;
use Ferro\Http\Error\HttpException;
use Ferro\Http\Error\HttpIndeterminateException;
use Ferro\Http\Error\HttpNonRetryableException;
use Ferro\Http\Error\HttpRetryableException;
use Ferro\Http\Error\RequestTooLargeException;
use Ferro\Http\Error\ResponseIncompleteException;
use Ferro\Http\FateClass;
use Ferro\Http\Upstream;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\HttpRequest;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\StreamData;
use Ferro\Protocol\StreamHead;
use Ferro\Tests\Support\DuplexDouble;
use Ferro\Tests\Support\FakeTransport;
use Ferro\Tests\Support\SignalDouble;
use Ferro\Tests\Support\ForkedFakeEngine as Fake;
use Ferro\Tests\Support\HttpFrames as F;
use PHPUnit\Framework\TestCase;

/**
 * M6-F8: the native Ferro HTTP API over an in-memory transport — the frames a scripted engine sends,
 * and every frame the client writes back (REQUEST, WINDOW_UPDATE, CANCEL), asserted exactly. The
 * time-dependent rules (deadlines, Fiber suspension) are {@see HttpFakeEngineTest}'s.
 */
final class HttpUpstreamTest extends TestCase
{
    protected function tearDown(): void
    {
        TraceContext::useProvider(null);
        gc_enable(); // the collector tests switch automatic collection off
    }

    /** A handshaken Connection over `$t`; the first request id is 1. */
    private function conn(FakeTransport $t, int $features = C::FEATURE_ENGINE_HTTP): Connection
    {
        $t->feed(F::helloAck($features));
        $session = new Session($t, new RequestIdAllocator(0));
        $session->hello();
        return new Connection($session, 'default');
    }

    private static function sqlOk(int $rid, int $value): string
    {
        $p = PackerFactory::forEncode();
        $body = ExecOk::encode([
            'cols' => [['name' => 'v', 'tag' => C::TAG_I64]],
            'rows' => [[['tag' => C::TAG_I64, 'data' => $value]]],
            'affected' => 1, 'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ], $p);
        return F::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, Outcome::ok($body)->encode($p));
    }

    // ---- the buffered form -----------------------------------------------------------------------

    public function testABufferedRequestReadsHeadBodyAndTerminal(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::head(1, 200, [['content-type', 'text/plain'], ['set-cookie', 'a=1'], ['set-cookie', 'b=2']], true));
        $t->feed(F::body(1, 'hel'));
        $t->feed(F::body(1, 'lo'));
        $t->feed(F::done(1, [['x-trailer', 't']]));

        $res = $conn->upstream('up', 'http://api.example')->request(
            'GET', '/x?y=1', headers: ['Accept' => 'text/plain'], timeoutMs: 1500, idempotent: true, route: '/x',
        );
        $this->assertSame(200, $res->status);
        $this->assertSame('hello', $res->body);
        $this->assertTrue($res->idempotent);
        $this->assertSame(['content-type' => ['text/plain'], 'set-cookie' => ['a=1', 'b=2']], $res->headers);
        $this->assertSame([['content-type', 'text/plain'], ['set-cookie', 'a=1'], ['set-cookie', 'b=2']], $res->headerLines);
        $this->assertSame('a=1', $res->header('Set-Cookie'));
        $this->assertSame([['x-trailer', 't']], $res->trailers);
        $this->assertSame(4, $res->stats['ttfb_us']);
        $this->assertTrue($res->stats['reused']);
        $this->assertSame(FateClass::NotAFailure, $res->statusFate()->fate);

        $req = F::requests($t->written)[1];
        $this->assertSame('up', $req['upstream']);
        $this->assertSame('GET', $req['method']);
        $this->assertSame('/x?y=1', $req['target']);
        $this->assertSame('http://api.example', $req['origin']);
        $this->assertSame([['Accept', 'text/plain']], $req['headers']);
        $this->assertNull($req['body']);
        $this->assertSame(1500, $req['timeout_ms']);
        $this->assertNull($req['connect_timeout_ms']);
        $this->assertTrue($req['idempotent']);
        $this->assertFalse($req['decode']);
        $this->assertSame('/x', $req['route']);
        $this->assertNull($req['traceparent']);
    }

    public function testEveryRequestFieldReachesTheWire(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        TraceContext::useProvider(static fn () => '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');
        $t->feed(F::head(1) . F::done(1));
        $conn->upstream('up')->request('POST', '/p', body: '', connectTimeoutMs: 7, readTimeoutMs: 9, idempotent: false, decode: true);
        $req = F::requests($t->written)[1];
        $this->assertSame('', $req['body'], 'an empty body is sent as an empty body, not as none');
        $this->assertSame(7, $req['connect_timeout_ms']);
        $this->assertSame(9, $req['read_timeout_ms']);
        $this->assertFalse($req['idempotent'], 'false is sent: it downgrades the operator declaration');
        $this->assertTrue($req['decode']);
        $this->assertNull($req['timeout_ms']);
        $this->assertNull($req['origin']);
        $this->assertSame('00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01', $req['traceparent']);
    }

    public function testHeadersAcceptMapsListsAndPairsInOrder(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::head(1) . F::done(1));
        $conn->upstream('up')->request('GET', '/', headers: [
            'A' => 'one',
            ['B', 'two'],
            'C' => ['three', 'four'],
            '123' => 5,
        ]);
        $this->assertSame(
            [['A', 'one'], ['B', 'two'], ['C', 'three'], ['C', 'four'], ['123', '5']],
            F::requests($t->written)[1]['headers'],
        );
    }

    public function testABadHeaderIsRefusedBeforeAnythingIsWritten(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $before = $t->writeCalls;
        $f = $conn->upstream('up')->requestAsync('GET', '/', headers: ['A' => 1.5]);
        $this->assertSame($before, $t->writeCalls, 'nothing written');
        try {
            $f->await();
            $this->fail('expected the refusal at await');
        } catch (\InvalidArgumentException $e) {
            $this->assertStringContainsString("'A'", $e->getMessage());
        }
    }

    public function testAStreamReturnsCreditFrameForFrameByWhatWasDebited(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::head(1) . F::body(1, 'abc') . F::body(1, str_repeat('z', 300)) . F::done(1));
        $conn->upstream('up')->stream('GET', '/')->body(); // a buffered request batches instead
        $this->assertSame([
            [1, strlen(F::headPayload())],
            [1, strlen(F::bodyPayload('abc'))],
            [1, strlen(F::bodyPayload(str_repeat('z', 300)))],
        ], F::windowUpdates($t->written, 1));
    }

    // ---- the streamed form -----------------------------------------------------------------------

    public function testAStreamReturnsAChunksCreditOnlyOnceTheLoopHasTakenIt(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::head(1, 200, [], false) . F::body(1, 'one') . F::body(1, 'two') . F::done(1));
        $s = $conn->upstream('up')->stream('POST', '/s', body: 'x');
        $this->assertSame(200, $s->status);
        $this->assertSame([[1, strlen(F::headPayload())]], F::windowUpdates($t->written, 1), 'the HEAD is replenished at once');

        $it = $s->getIterator();
        $this->assertSame('one', $it->current());
        $this->assertCount(1, F::windowUpdates($t->written, 1), 'a chunk handed to the loop but not yet taken returns no credit');
        $it->next();
        $this->assertSame('two', $it->current());
        $this->assertCount(2, F::windowUpdates($t->written, 1), 'taking it returned its credit');
        $it->next();
        $this->assertFalse($it->valid());
        $this->assertCount(3, F::windowUpdates($t->written, 1));
        $this->assertTrue($s->isComplete());
        $this->assertSame([], $s->trailers());
        $this->assertSame([], F::cancels($t->written), 'a stream read to its end is never cancelled');
    }

    public function testBreakingOutOfAStreamCancelsAndDrainsSoTheNextQueryReadsItsOwnReply(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::head(1) . F::body(1, 'one') . F::body(1, 'two') . F::cancelled(1));
        $t->feed(self::sqlOk(2, 42));
        $s = $conn->upstream('up')->stream('GET', '/s');
        foreach ($s as $chunk) {
            $this->assertSame('one', $chunk);
            break;
        }
        $this->assertSame([1], F::cancels($t->written));
        $session = $conn->session();
        $this->assertInstanceOf(Session::class, $session);
        $this->assertFalse($session->isPending(1), 'drained to its terminal');
        $this->assertFalse($session->hasRequestsInFlight(), 'its terminal was READ, not left to arrive (the drain, not a discard)');
        $this->assertTrue($s->isClosed());
        $this->assertFalse($s->isComplete());
        // The C1d lesson: the abandoned exchange must not damage the NEXT request.
        $this->assertSame(42, $conn->scalar('select 42'));
    }

    public function testCloseBeforeIteratingCancelsAndAnIterationAfterwardsIsRefused(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::head(1) . F::cancelled(1));
        $s = $conn->upstream('up')->stream('GET', '/s');
        $s->close();
        $s->close(); // idempotent
        $this->assertSame([1], F::cancels($t->written));
        $this->expectException(\LogicException::class);
        foreach ($s as $_) {
        }
    }

    public function testAStreamIsSinglePass(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::head(1) . F::body(1, 'a') . F::done(1));
        $s = $conn->upstream('up')->stream('GET', '/s');
        $this->assertSame('a', $s->body());
        $this->expectException(\LogicException::class);
        $s->body();
    }

    public function testADroppedStreamCancelsWithoutWaitingAndItsFramesAreDiscarded(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::head(1));
        $s = $conn->upstream('up')->stream('GET', '/s');
        unset($s);
        $this->assertSame([1], F::cancels($t->written), 'CANCEL written at once');
        // The rest of the exchange arrives while the next request is awaited, and is thrown away.
        $t->feed(F::body(1, 'late') . F::cancelled(1) . self::sqlOk(2, 7));
        $this->assertSame(7, $conn->scalar('select 7'));
        $session = $conn->session();
        $this->assertInstanceOf(Session::class, $session);
        $this->assertFalse($session->isPending(1));
    }

    public function testADroppedFutureCancelsItsRequest(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $f = $conn->upstream('up')->requestAsync('GET', '/slow');
        $this->assertSame([], F::cancels($t->written));
        unset($f);
        $this->assertSame([1], F::cancels($t->written));
    }

    // ---- (cj)'s exclusivity, lifted for HTTP -----------------------------------------------------

    public function testAnHttpRequestGoesOutWhileASqlStreamIsOpen(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $p = PackerFactory::forEncode();
        $t->feed(F::frame(0, C::SERVICE_STREAM, C::METHOD_STREAM_HEAD, 1, StreamHead::encode(['cols' => [['name' => 'n', 'tag' => C::TAG_I64]]], $p)));
        $t->feed(F::frame(C::FLAG_STREAM, C::SERVICE_STREAM, C::METHOD_STREAM_DATA, 1, StreamData::encode(['rows' => [[['tag' => C::TAG_I64, 'data' => 1]]]], $p)));
        $rows = $conn->stream('select n');
        $this->assertSame(['n' => 1], $rows->current());

        // A SQL statement here would be refused (the SQL stream is exclusive); an HTTP request is not.
        $t->feed(F::head(2) . F::body(2, 'ok') . F::done(2));
        $this->assertSame('ok', $conn->upstream('up')->request('GET', '/')->body);

        $t->feed(F::frame(C::FLAG_STREAM, C::SERVICE_STREAM, C::METHOD_STREAM_DATA, 1, StreamData::encode(['rows' => [[['tag' => C::TAG_I64, 'data' => 2]]]], $p)));
        $t->feed(F::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, Outcome::ok($p->packNil())->encode($p)));
        $rows->next();
        $this->assertSame(['n' => 2], $rows->current());
        $rows->next();
        $this->assertFalse($rows->valid());
    }

    public function testSqlAndOtherHttpExchangesGoOnWhileAnHttpStreamIsOpen(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $http = $conn->upstream('up');
        $t->feed(F::head(1, 200, [['x-s', 'a']]));
        $a = $http->stream('GET', '/a');
        $t->feed(F::head(2, 200, [['x-s', 'b']]));
        $b = $http->stream('GET', '/b');
        // Frames of both streams and a SQL reply, interleaved on the wire.
        $t->feed(F::body(2, 'b1') . F::body(1, 'a1') . self::sqlOk(3, 9) . F::body(2, 'b2') . F::done(2) . F::body(1, 'a2') . F::done(1));
        $this->assertSame(9, $conn->scalar('select 9'), 'a SQL statement while two HTTP streams are open');
        $this->assertSame(['a1', 'a2'], iterator_to_array($a, false));
        $this->assertSame(['b1', 'b2'], iterator_to_array($b, false));
        $this->assertSame('a', $a->header('x-s'));
        $this->assertSame('b', $b->header('x-s'));
    }

    // ---- refusals before a byte is written --------------------------------------------------------

    public function testAnEngineWithoutTheHttpBitIsNotSentTheRequest(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t, 0);
        $before = $t->writeCalls;
        try {
            $conn->upstream('up')->request('GET', '/');
            $this->fail('expected the refusal');
        } catch (NonRetryableException $e) {
            $this->assertNotInstanceOf(HttpException::class, $e);
            $this->assertSame(C::ERR_UNSUPPORTED, $e->errorCode());
        }
        $this->assertSame($before, $t->writeCalls, 'nothing written');
    }

    public function testABodyPastTheFrameCapIsRefusedBeforeWritingAndTheSessionGoesOn(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $before = $t->writeCalls;
        try {
            $conn->upstream('up')->request('POST', '/u', body: str_repeat('b', C::MAX_FRAME_PAYLOAD));
            $this->fail('expected the refusal');
        } catch (RequestTooLargeException $e) {
            $this->assertStringContainsString((string) C::MAX_FRAME_PAYLOAD, $e->getMessage());
        }
        $this->assertSame($before, $t->writeCalls);
        $t->feed(F::head(1) . F::done(1));
        $this->assertSame(200, $conn->upstream('up')->request('GET', '/')->status);
    }

    public function testATraceContextThatAlonePushesTheFramePastTheCapIsDropped(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $trace = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
        $p = PackerFactory::forEncode();
        $base = ['upstream' => 'up', 'method' => 'POST', 'target' => '/u', 'origin' => null, 'headers' => [],
            'timeout_ms' => null, 'connect_timeout_ms' => null, 'read_timeout_ms' => null, 'idempotent' => null,
            'decode' => false, 'route' => null, 'traceparent' => null];
        // The largest body whose frame fits WITHOUT the trace context.
        $len = C::MAX_FRAME_PAYLOAD - 64;
        while (strlen(HttpRequest::encode($base + ['body' => str_repeat('b', $len + 1)], $p)) <= C::MAX_FRAME_PAYLOAD) {
            ++$len;
        }
        $this->assertGreaterThan(C::MAX_FRAME_PAYLOAD, strlen(HttpRequest::encode(['body' => str_repeat('b', $len), 'traceparent' => $trace] + $base, $p)));
        TraceContext::useProvider(static fn () => $trace);
        $t->feed(F::head(1) . F::done(1));
        $conn->upstream('up')->request('POST', '/u', body: str_repeat('b', $len));
        $this->assertNull(F::requests($t->written)[1]['traceparent']);
    }

    // ---- §23.7.3: the client classifies a lost link ----------------------------------------------

    public function testARequestWhoseFrameNeverLeftIsRetryableAndNotSent(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->failNextWrite = new TransportException('broken pipe');
        try {
            $conn->upstream('up')->request('POST', '/w', body: 'x');
            $this->fail('expected the fate');
        } catch (HttpRetryableException $e) {
            $this->assertTrue($e->clientSynthesised());
            $this->assertSame(HttpException::CLIENT_LINK_LOST, $e->cause());
            $this->assertStringContainsString('not sent', $e->getMessage());
        }
    }

    public function testALinkLostBeforeTheHeadIsIndeterminateUnlessTheRequestDeclaredItself(): void
    {
        $t = new FakeTransport(); // nothing fed: the read fails as a lost link
        $conn = $this->conn($t);
        try {
            $conn->upstream('up')->request('GET', '/g');
            $this->fail('expected the fate');
        } catch (HttpIndeterminateException $e) {
            $this->assertSame(HttpException::CLIENT_LINK_LOST, $e->cause(), 'an undeclared GET: the method licenses nothing');
            $this->assertTrue($e->clientSynthesised());
        }

        $t2 = new FakeTransport();
        $conn2 = $this->conn($t2);
        $this->expectException(HttpRetryableException::class);
        $conn2->upstream('up')->request('GET', '/g', idempotent: true);
    }

    public function testALinkLostAfterTheHeadFollowsTheEngineIdempotencyFromTheHead(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::head(1, 201, [], false) . F::body(1, 'par'));
        try {
            $conn->upstream('up')->request('POST', '/p');
            $this->fail('expected the fate');
        } catch (ResponseIncompleteException $e) {
            $this->assertSame(201, $e->status());
            $this->assertTrue($e->wasApplied());
            $this->assertTrue($e->clientSynthesised());
            $this->assertSame(FateClass::NonRetryable, $e->fate());
        }

        // The operator declared it idempotent; the client learns that only from the HEAD.
        $t2 = new FakeTransport();
        $conn2 = $this->conn($t2);
        $t2->feed(F::head(1, 200, [], true));
        $this->expectException(HttpRetryableException::class);
        $conn2->upstream('ops')->request('GET', '/p');
    }

    public function testAStreamThrowsItsFateAfterTheChunksThatArrived(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::head(1, 200) . F::body(1, 'a') . F::body(1, 'b'));
        $s = $conn->upstream('up')->stream('POST', '/s');
        $got = [];
        try {
            foreach ($s as $chunk) {
                $got[] = $chunk;
            }
            $this->fail('expected the fate');
        } catch (ResponseIncompleteException $e) {
            $this->assertSame(['a', 'b'], $got);
            $this->assertTrue($e->wasApplied());
        }
    }

    public function testNothingIsEverReSentEvenWithAReconnectLoopAndARetryableFate(): void
    {
        $first = new FakeTransport();
        $first->feed(F::helloAck());
        $session = new Session($first, new RequestIdAllocator(0));
        $session->hello();
        $fresh = [];
        $loop = new ReconnectLoop($session, static function () use (&$fresh): Session {
            $t = new FakeTransport();
            $t->feed(F::helloAck());
            $fresh[] = $t;
            $s = new Session($t, new RequestIdAllocator(100));
            $s->hello();
            return $s;
        }, new Backoff(0, 0), 2);
        $conn = new Connection($session, 'default', reconnect: $loop, policy: RetryPolicy::default());

        try {
            $conn->upstream('up')->request('GET', '/g', idempotent: true);
            $this->fail('expected the fate');
        } catch (HttpRetryableException) {
            // Retryable: the CALLER may retry. The client must not have.
        }
        $requests = count(F::requests($first->written));
        foreach ($fresh as $t) {
            $requests += count(F::requests($t->written));
        }
        $this->assertSame(1, $requests, 'exactly one REQUEST was ever written');
        $this->assertSame([], $fresh, 'and no reconnect was made to send it again');

        // A NEW request replaces the closed session first — not a retry of anything.
        $conn->upstream('up')->requestAsync('GET', '/next');
        $this->assertCount(1, $fresh);
        $this->assertCount(1, F::requests($fresh[0]->written));
    }

    // ---- engine terminals -----------------------------------------------------------------------

    public function testEngineTerminalsSurfaceAsTheirFate(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $http = $conn->upstream('up');
        $t->feed(F::error(1, C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, C::HTTP_CAUSE_EOF_EMPTY));
        $t->feed(F::error(2, C::ERR_RATE_LIMITED, C::BRANCH_RETRYABLE, C::HTTP_CAUSE_RATE_LIMITED, 2500));
        $t->feed(F::head(3, 503) . F::error(3, C::ERR_RESPONSE_INCOMPLETE, C::BRANCH_NON_RETRYABLE, C::HTTP_CAUSE_BODY_EOF));
        $t->feed(F::cancelled(4));
        $t->feed(F::error(5, C::ERR_UNSUPPORTED, C::BRANCH_NON_RETRYABLE, null));
        $t->feed(F::error(6, C::ERR_FORBIDDEN, C::BRANCH_NON_RETRYABLE, C::HTTP_CAUSE_FORBIDDEN_UPSTREAM));
        $f = [];
        for ($i = 1; $i <= 6; ++$i) {
            $f[$i] = $http->requestAsync('POST', "/r{$i}");
        }
        $caught = [];
        foreach ($f as $i => $future) {
            try {
                $future->await();
            } catch (\Throwable $e) {
                $caught[$i] = $e;
            }
        }
        $this->assertInstanceOf(HttpIndeterminateException::class, $caught[1]);
        $this->assertSame(C::HTTP_CAUSE_EOF_EMPTY, $caught[1]->cause());
        $this->assertFalse($caught[1]->clientSynthesised());
        $this->assertInstanceOf(HttpRetryableException::class, $caught[2]);
        $this->assertSame(2500, $caught[2]->retryAfterMs());
        $this->assertInstanceOf(ResponseIncompleteException::class, $caught[3]);
        $this->assertSame(503, $caught[3]->status());
        $this->assertSame(FateClass::Indeterminate, $caught[3]->fate(), '503 without Retry-After, non-idempotent');
        $this->assertInstanceOf(HttpCancelledException::class, $caught[4]);
        $this->assertInstanceOf(NonRetryableException::class, $caught[5]);
        $this->assertNotInstanceOf(HttpException::class, $caught[5]);
        $this->assertInstanceOf(HttpNonRetryableException::class, $caught[6]);
        $this->assertSame(C::HTTP_CAUSE_FORBIDDEN_UPSTREAM, $caught[6]->cause());
    }

    public function testAnOkTerminalWithoutAHeadIsAProtocolFault(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::done(1));
        $this->expectException(ProtocolException::class);
        $conn->upstream('up')->request('GET', '/');
    }

    /** @return iterable<string, array{string}> */
    public static function outOfOrder(): iterable
    {
        yield 'BODY before HEAD' => [F::body(1, 'x')];
        yield 'two HEADs' => [F::head(1) . F::head(1)];
        yield 'HEAD flagged STREAM' => [F::frame(C::FLAG_STREAM, C::SERVICE_HTTP, C::METHOD_HTTP_HEAD, 1, F::headPayload())];
        yield 'BODY not flagged STREAM' => [F::head(1) . F::frame(0, C::SERVICE_HTTP, C::METHOD_HTTP_BODY, 1, F::bodyPayload('x'))];
        yield 'a SQL stream frame' => [F::frame(0, C::SERVICE_STREAM, C::METHOD_STREAM_HEAD, 1, F::headPayload())];
        yield 'a malformed BODY' => [F::head(1) . F::frame(C::FLAG_STREAM, C::SERVICE_HTTP, C::METHOD_HTTP_BODY, 1, "\x91\x01")];
    }

    /** @dataProvider-like: each disagreement about an exchange poisons the session */
    #[\PHPUnit\Framework\Attributes\DataProvider('outOfOrder')]
    public function testAFrameOutOfOrderIsADesyncThatClosesTheSession(string $frames): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed($frames . F::body(1, 'pad') . F::done(1));
        try {
            $conn->upstream('up')->request('GET', '/');
            $this->fail('expected a protocol fault');
        } catch (ProtocolException) {
        }
        $this->assertTrue($conn->session()->isPoisoned());
        $this->assertTrue($t->closed);
    }

    public function testATimeoutArmsTheClientBackstopAndNoTimeoutArmsNone(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $session = $conn->session();
        $this->assertInstanceOf(Session::class, $session);
        $http = $conn->upstream('up');
        $before = microtime(true);
        $kept = $http->requestAsync('GET', '/a', timeoutMs: 3000); // held: a dropped Future cancels
        $deadline = $session->nearestDeadline();
        $this->assertNotNull($deadline);
        $this->assertEqualsWithDelta($before + 3.0 + 2.0, $deadline, 0.5, 'timeout + the 2 s margin of §23.11.0');

        $t2 = new FakeTransport();
        $conn2 = $this->conn($t2);
        $s2 = $conn2->session();
        $this->assertInstanceOf(Session::class, $s2);
        $kept2 = $conn2->upstream('up')->requestAsync('GET', '/a');
        $this->assertNull($s2->nearestDeadline(), 'no timeoutMs, no client deadline: liveness only');
    }

    public function testAnUnknownUpstreamNameIsJustSent(): void
    {
        // The engine decides (forbidden_upstream); the client holds no list of upstreams (C8).
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::error(1, C::ERR_FORBIDDEN, C::BRANCH_NON_RETRYABLE, C::HTTP_CAUSE_FORBIDDEN_UPSTREAM));
        try {
            (new Upstream('nope', null, static fn () => $conn->session(), PackerFactory::forEncode(), PackerFactory::forDecode()))
                ->request('GET', '/');
            $this->fail('expected the refusal');
        } catch (HttpNonRetryableException $e) {
            $this->assertSame(C::HTTP_CAUSE_FORBIDDEN_UPSTREAM, $e->cause());
        }
        $this->assertSame('nope', F::requests($t->written)[1]['upstream']);
    }

    // ---- review round (M6-F8): HEAD values, credit and the slot wait, re-entrancy ------------------

    /**
     * A HEAD whose VALUES the client cannot use is that exchange's failure, never the session's: the
     * frames are intact, so poisoning would fail every unrelated request on the socket (review LOW).
     * A 1xx status and an unknown version pass through; the codec, not the client, bounds them.
     */
    public function testAHeadStatusThatIsNotAnHttpStatusFailsOnlyItsExchange(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $http = $conn->upstream('up');
        $other = $http->requestAsync('GET', '/other');               // rid 1, answered later
        $t->feed(F::head(2, 1000) . F::cancelled(2));                 // rid 2: not even three digits
        try {
            $http->request('GET', '/bad');
            $this->fail('expected the exchange to fail');
        } catch (ProtocolException $e) {
            $this->assertStringContainsString('1000', $e->getMessage());
        }
        $this->assertFalse($conn->session()->isPoisoned(), 'only that exchange failed');
        $this->assertSame([2], F::cancels($t->written), 'it was stopped');
        $t->feed(F::head(1) . F::body(1, 'fine') . F::done(1));
        $this->assertSame('fine', $other->await()->body);

        $t->feed(F::head(3, 199, [], false, 12) . F::done(3));
        $odd = $http->request('GET', '/odd');
        $this->assertSame([199, 12], [$odd->status, $odd->version]);
    }

    /**
     * Review F2(a): a buffered exchange returns its credit as its frames are FILED — here by the
     * slot wait of a later request, long before its own await — half a window at a time and never
     * twice, so an unawaited Future runs to its terminal and frees its slot.
     */
    public function testABufferedExchangeReturnsCreditAsItsFramesAreFiled(): void
    {
        $t = new FakeTransport();
        $t->feed(F::helloAck());
        $session = new Session($t, new RequestIdAllocator(0), maxInFlight: 1);
        $session->hello();
        $http = (new Connection($session, 'default'))->upstream('up');
        $first = $http->requestAsync('GET', '/big');                  // rid 1 holds the only slot
        $half = intdiv(C::DEFAULT_CREDIT_FRAMES, 2);
        $t->feed(F::head(1));
        $owedBytes = strlen(F::headPayload());
        for ($i = 1; $i < C::DEFAULT_CREDIT_FRAMES; ++$i) {           // HEAD + 63 BODY: a whole window
            $t->feed(F::body(1, 'x'));
            if ($i < $half) {
                $owedBytes += strlen(F::bodyPayload('x'));
            }
        }
        $t->feed(F::done(1) . F::head(2) . F::done(2));
        $second = $http->requestAsync('GET', '/next');                // waits for the slot
        $updates = F::windowUpdates($t->written, 1);
        $this->assertSame([$half, $owedBytes], $updates[0] ?? null, 'half a window returned in one update, on receipt');
        $requestAt = null;
        $firstUpdateAt = null;
        foreach (F::written($t->written) as $i => [$h]) {
            if ($firstUpdateAt === null && $h->service === C::SERVICE_CORE && $h->method === C::METHOD_CORE_WINDOW_UPDATE && $h->requestId === 1) {
                $firstUpdateAt = $i;
            }
            if ($h->service === C::SERVICE_HTTP && $h->method === C::METHOD_HTTP_REQUEST && $h->requestId === 2) {
                $requestAt = $i;
            }
        }
        $this->assertNotNull($requestAt);
        $this->assertLessThan($requestAt, $firstUpdateAt, 'credited during the slot wait, before the next REQUEST');
        $this->assertCount(2, $updates, 'two halves for a whole window');
        $this->assertSame(C::DEFAULT_CREDIT_FRAMES - 1, strlen($first->await()->body));
        $this->assertSame(200, $second->await()->status);
        $this->assertCount(2, F::windowUpdates($t->written, 1), 'never credited again by the awaiter');
    }

    /**
     * Review F2(b): when every in-flight slot holds an HTTP stream parked on credit, no slot can
     * free, so the next request is refused unsent — Retryable — instead of blocking until the
     * engine's own timeouts. It is NOT a transport failure: a SQL read refused this way does not
     * reconnect (which would close the session and both streams).
     */
    public function testWhenEverySlotIsAStreamParkedOnCreditTheNextRequestIsRefusedNotBlocked(): void
    {
        $t = new FakeTransport();
        $t->feed(F::helloAck());
        $session = new Session($t, new RequestIdAllocator(0), maxInFlight: 2);
        $session->hello();
        $dials = 0;
        $loop = new ReconnectLoop($session, static function () use (&$dials): Session {
            ++$dials;
            throw new TransportException('no dial expected');
        }, new Backoff(0, 0), 1);
        $conn = new Connection($session, 'default', reconnect: $loop, policy: RetryPolicy::default());
        $http = $conn->upstream('up');
        $t->feed(F::head(1));
        $a = $http->stream('GET', '/a');
        $t->feed(F::head(2));
        $b = $http->stream('GET', '/b');
        // Each stream's window fills: 64 BODY frames each, unconsumed.
        for ($i = 0; $i < C::DEFAULT_CREDIT_FRAMES; ++$i) {
            $t->feed(F::body(1, 'a') . F::body(2, 'b'));
        }
        $before = count(F::requests($t->written));
        $f = $http->requestAsync('GET', '/third');
        try {
            $f->await();
            $this->fail('expected the refusal');
        } catch (InFlightLimitException $e) {
            $this->assertStringContainsString('not sent', $e->getMessage());
            $this->assertSame(C::BRANCH_RETRYABLE, $e->branch());
        }
        $this->assertSame($before, count(F::requests($t->written)), 'the REQUEST was never written');
        try {
            $conn->scalar('SELECT 1');
            $this->fail('expected the refusal');
        } catch (InFlightLimitException) {
        }
        $this->assertSame(0, $dials, 'a refused read does not reconnect');
        $this->assertFalse($session->isPoisoned());
        // Consuming a stream returns its credit; the streams were never harmed.
        $it = $a->getIterator();
        $this->assertSame('a', $it->current());
        $t->feed(F::done(1) . F::done(2));
        $n = 1;
        for ($it->next(); $it->valid(); $it->next()) {
            ++$n;
        }
        $this->assertSame(C::DEFAULT_CREDIT_FRAMES, $n);
        $this->assertSame(C::DEFAULT_CREDIT_FRAMES, strlen($b->body()));
    }

    /**
     * Review LOW (speculation, CONFIRMED): the cycle collector can destroy a partly read stream
     * while the session is in the middle of reading a frame. Its abandonment must not read — that
     * would consume the outer read's payload — nor write mid-frame: it discards, and its CANCEL is
     * written once the read is over.
     */
    public function testAStreamCollectedMidReadOnlyDiscardsAndCancelsAfterTheRead(): void
    {
        $inner = new FakeTransport();
        $hooked = new class ($inner) implements TransportInterface {
            public ?\Closure $onPayloadRead = null;

            public function __construct(public readonly FakeTransport $inner) {}

            public function readExact(int $n): string
            {
                $bytes = $this->inner->readExact($n);
                if ($n !== 16 && $this->onPayloadRead !== null) {
                    $hook = $this->onPayloadRead;
                    $this->onPayloadRead = null;
                    $hook(); // between this frame's header and the rest of the session's handling
                }
                return $bytes;
            }

            public function writeAll(string $bytes): void { $this->inner->writeAll($bytes); }

            public function close(): void { $this->inner->close(); }
        };
        $inner->feed(F::helloAck());
        $session = new Session($hooked, new RequestIdAllocator(0));
        $session->hello();
        $conn = new Connection($session, 'default');
        $inner->feed(F::head(1) . F::body(1, 'one'));
        $s = $conn->upstream('up')->stream('GET', '/s');
        $it = $s->getIterator();
        $this->assertSame('one', $it->current());
        $cycle = new \stdClass();
        $cycle->self = $cycle;
        $cycle->stream = $s;
        $cycle->it = $it;
        unset($s, $it, $cycle);

        $inner->feed(self::sqlOk(2, 5));
        $writesBefore = $inner->writeCalls;
        $hooked->onPayloadRead = static function () use ($inner, &$writesDuringRead, &$writesBefore): void {
            gc_collect_cycles(); // destroys the stream mid-read
            $writesDuringRead = $inner->writeCalls - $writesBefore;
        };
        $writesDuringRead = null;
        $f = $conn->scalarAsync('SELECT 5');
        $writesBefore = $inner->writeCalls;
        $this->assertSame(5, $f->await(), 'the outer read was not disturbed');
        $this->assertSame(0, $writesDuringRead, 'nothing was written in the middle of the read');
        $this->assertSame([1], F::cancels($inner->written), 'the CANCEL followed the read');
        $this->assertFalse($session->isPoisoned());
        $this->assertFalse($session->isPending(1), 'discarded');
        $inner->feed(F::body(1, 'late') . F::cancelled(1) . self::sqlOk(3, 6));
        $this->assertSame(6, $conn->scalar('SELECT 6'), 'its late frames were thrown away');
    }

    /**
     * The same, when ONLY the stream's generator is collected (the stream object itself is still
     * held), so it is the generator's `finally` — not the stream's destructor — that abandons the
     * exchange: `abandonHttp` itself must see the read in progress and only discard.
     */
    public function testAGeneratorCollectedMidReadOnlyDiscards(): void
    {
        $inner = new FakeTransport();
        $hooked = new class ($inner) implements TransportInterface {
            public ?\Closure $onPayloadRead = null;

            public function __construct(public readonly FakeTransport $inner) {}

            public function readExact(int $n): string
            {
                $bytes = $this->inner->readExact($n);
                if ($n !== 16 && $this->onPayloadRead !== null) {
                    $hook = $this->onPayloadRead;
                    $this->onPayloadRead = null;
                    $hook();
                }
                return $bytes;
            }

            public function writeAll(string $bytes): void { $this->inner->writeAll($bytes); }

            public function close(): void { $this->inner->close(); }
        };
        $inner->feed(F::helloAck());
        $session = new Session($hooked, new RequestIdAllocator(0));
        $session->hello();
        $conn = new Connection($session, 'default');
        $inner->feed(F::head(1) . F::body(1, 'one'));
        $s = $conn->upstream('up')->stream('GET', '/s');
        $it = $s->getIterator();
        $this->assertSame('one', $it->current());
        $cycle = new \stdClass();
        $cycle->self = $cycle;
        $cycle->it = $it;
        unset($it, $cycle); // the generator alone is garbage; $s is still held

        $inner->feed(self::sqlOk(2, 5));
        $f = $conn->scalarAsync('SELECT 5');
        $writesBefore = $inner->writeCalls;
        $writesDuringRead = null;
        $hooked->onPayloadRead = static function () use ($inner, &$writesDuringRead, &$writesBefore): void {
            gc_collect_cycles();
            $writesDuringRead = $inner->writeCalls - $writesBefore;
        };
        $this->assertSame(5, $f->await(), 'the outer read was not disturbed');
        $this->assertSame(0, $writesDuringRead, 'nothing was written in the middle of the read');
        $this->assertSame([1], F::cancels($inner->written), 'the CANCEL followed the read');
        $this->assertTrue($s->isClosed());
        $this->assertFalse($session->isPoisoned());
    }

    // ---- review round 2 (M6-F8) ------------------------------------------------------------------

    /**
     * R2-4: a status in 600..=999 is not an HTTP status, but `ferrod` passes one through (it is a
     * valid `http::StatusCode`), and RFC 9110 §15 says a client SHOULD process it as a 5xx. So it is
     * DELIVERED, with its head, and {@see \Ferro\Http\StatusFate} reads it as a 5xx: Indeterminate for
     * a POST the upstream received and answered, Retryable for an idempotent request. No CANCEL.
     */
    public function testAStatusAbove599IsDeliveredAsA5xx(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $http = $conn->upstream('up');
        $t->feed(F::head(1, 600) . F::body(1, 'odd') . F::done(1));
        $r = $http->request('POST', '/x', body: 'p');
        $this->assertSame([600, 'odd'], [$r->status, $r->body]);
        $this->assertSame(FateClass::Indeterminate, $r->statusFate()->fate);
        $t->feed(F::head(2, 999, [], true) . F::done(2));
        $r = $http->request('GET', '/y', idempotent: true);
        $this->assertSame(999, $r->status);
        $this->assertSame(FateClass::Retryable, $r->statusFate()->fate);
        $this->assertSame([], F::cancels($t->written), 'neither exchange was stopped');
        $this->assertFalse($conn->session()->isPoisoned());
    }

    /**
     * R2-5: a deadline CANCEL is a duplex write, which may read frames. Here the read during X's
     * CANCEL files Y's END — Y completed. The deadline pass must not then CANCEL Y and re-arm a
     * deadline nothing would ever clear: that closed the session one grace later ("request 2 passed
     * its deadline and the engine did not answer its CANCEL").
     */
    public function testACancelWriteThatReadsAnotherRequestsAnswerDoesNotCancelIt(): void
    {
        $inner = new FakeTransport();
        $dup = new DuplexDouble($inner, 0.2);
        $inner->feed(F::helloAck());
        $session = new Session($dup, new RequestIdAllocator(0));
        $session->hello();
        $session->setRequestTimeout(0.05);
        $conn = new Connection($session, 'default');
        $x = $conn->scalarAsync('SELECT 1'); // rid 1
        $y = $conn->scalarAsync('SELECT 2'); // rid 2
        usleep(100_000); // both deadlines pass
        $inner->feed(self::sqlOk(2, 2) . Fake::cancelled(1));
        $dup->onWrite = static function (Header $h, \Closure $onReadable) use ($dup): void {
            if (($h->flags & C::FLAG_CANCEL) !== 0) {
                $dup->onWrite = null;
                $onReadable(); // the socket would not take the CANCEL; the engine had Y's answer for us
            }
        };
        try {
            $x->await();
            $this->fail('X was cancelled');
        } catch (FerroException) {
        }
        $this->assertSame(2, $y->await(), 'Y completed normally');
        $this->assertSame([1], F::cancels($inner->written), 'no CANCEL for a request that had completed');
        usleep(300_000); // past the grace a re-armed deadline would have had
        $inner->feed(self::sqlOk(3, 3));
        $this->assertSame(3, $conn->scalar('SELECT 3'));
        $this->assertFalse($session->isPoisoned());
    }

    /**
     * R2-5, the HEAD variant: the read during X's CANCEL files Y's HTTP HEAD, which ends Y's client
     * deadline. The pass must not CANCEL Y — a live exchange after its HEAD (the F1 class).
     */
    public function testACancelWriteThatReadsAnotherExchangesHeadDoesNotCancelIt(): void
    {
        $inner = new FakeTransport();
        $dup = new DuplexDouble($inner, 0.2);
        $inner->feed(F::helloAck());
        $session = new Session($dup, new RequestIdAllocator(0));
        $session->hello();
        $session->setRequestTimeout(0.05);
        $conn = new Connection($session, 'default');
        $x = $conn->scalarAsync('SELECT 1');                      // rid 1
        $y = $conn->upstream('up')->requestAsync('GET', '/y');    // rid 2
        $session->setDeadline(2, microtime(true) + 0.05);
        usleep(100_000); // both deadlines pass
        $inner->feed(F::head(2) . Fake::cancelled(1));
        $dup->onWrite = static function (Header $h, \Closure $onReadable) use ($dup): void {
            if (($h->flags & C::FLAG_CANCEL) !== 0) {
                $dup->onWrite = null;
                $onReadable();
            }
        };
        try {
            $x->await();
            $this->fail('X was cancelled');
        } catch (FerroException) {
        }
        $this->assertSame([1], F::cancels($inner->written), 'Y had its HEAD: its deadline was over');
        $inner->feed(F::body(2, 'y') . F::done(2));
        $this->assertSame('y', $y->await()->body);
        $this->assertFalse($session->isPoisoned());
    }

    /**
     * R2-3: code run in the middle of a write — a destructor the cycle collector runs during a read
     * inside the write — must not write a frame of its own: it would land in the middle of the
     * half-written one, which the engine reads as garbage and closes the session over. It is refused
     * (nothing written); a destructor that catches it leaves the outer write and the session intact.
     */
    public function testAWriteFromADestructorRunMidWriteIsRefusedNotSpliced(): void
    {
        [$inner, $dup, $session, $conn] = $this->duplexConn();
        $buffered = $conn->upstream('up')->requestAsync('GET', '/a'); // rid 1
        $inner->feed(F::head(1));
        $seen = new \stdClass();
        $seen->refused = null;
        gc_disable(); // collected only by the hook (see tearDown)
        $this->cyclicDestructor($conn, $seen, catch: true);
        $dup->splitAt = 100;
        $dup->onWrite = static function (Header $h, \Closure $onReadable) use ($dup): void {
            $dup->onWrite = null;
            $dup->onPayloadRead = static function (): void { gc_collect_cycles(); };
            $onReadable(); // reads HEAD(1); the collector runs the destructor in the middle of it
        };
        $before = strlen($inner->written);
        $post = $conn->upstream('up')->requestAsync('POST', '/b', body: str_repeat('z', 4000)); // rid 2
        $this->assertInstanceOf(ReentrantWriteException::class, $seen->refused, 'the destructor\'s query was refused');
        $frames = DuplexDouble::frames(substr($inner->written, $before));
        $this->assertCount(1, $frames, 'only the REQUEST is on the wire: ' . implode(', ', $frames));
        $this->assertStringStartsWith(C::SERVICE_HTTP . '/' . C::METHOD_HTTP_REQUEST . '/2/', $frames[0]);
        $this->assertFalse($session->isPoisoned());
        $inner->feed(F::done(1) . F::head(2) . F::done(2));
        $this->assertSame(200, $buffered->await()->status);
        $this->assertSame(200, $post->await()->status);
    }

    /**
     * R2-3, uncaught: the refusal propagates out of the destructor and through the outer write,
     * leaving part of that frame on the wire, so the session is closed — and nothing is written
     * after the partial frame. Never a spliced frame.
     */
    public function testAnUncaughtRefusalMidWriteClosesTheSessionWithoutSplicing(): void
    {
        [$inner, $dup, $session, $conn] = $this->duplexConn();
        $pending = $conn->upstream('up')->requestAsync('GET', '/a'); // rid 1, never awaited
        $inner->feed(F::head(1));
        $seen = new \stdClass();
        $seen->refused = null;
        gc_disable(); // collected only by the hook (see tearDown)
        $this->cyclicDestructor($conn, $seen, catch: false);
        $dup->splitAt = 100;
        $dup->onWrite = static function (Header $h, \Closure $onReadable) use ($dup): void {
            $dup->onWrite = null;
            $dup->onPayloadRead = static function (): void { gc_collect_cycles(); };
            $onReadable();
        };
        $before = strlen($inner->written);
        $post = $conn->upstream('up')->requestAsync('POST', '/b', body: str_repeat('z', 4000));
        $this->assertSame(100, strlen($inner->written) - $before, 'nothing after the partial frame');
        $this->assertTrue($session->isPoisoned(), 'a partial frame on the wire: nothing may follow it');
        try {
            $post->await();
            $this->fail('the interrupted request failed');
        } catch (HttpRetryableException $e) {
            // Its frame never completely left: not sent, so it cannot have reached the upstream.
            $this->assertStringContainsString('not sent', $e->getMessage());
            $this->assertStringContainsString('while another frame was being written', $e->getMessage());
        }
        unset($pending);
    }

    /**
     * R2-3, ordering: the credit a request's own write made owed is written AFTER the write — and
     * that write may read frames, among them this request's answer. The request must be in flight
     * by then, or its answer is "a frame for a request not in flight": a desync.
     */
    public function testARequestsAnswerReadDuringTheDeferredWritesAfterItIsItsAnswer(): void
    {
        [$inner, $dup, $session, $conn] = $this->duplexConn();
        $buffered = $conn->upstream('up')->requestAsync('GET', '/a'); // rid 1: credit on receipt
        $inner->feed(F::head(1) . str_repeat(F::body(1, 'x'), 32) . self::sqlOk(2, 7));
        $dup->onWrite = static function (Header $h, \Closure $onReadable) use ($dup): void {
            if ($h->service === C::SERVICE_SQL) {
                for ($i = 0; $i < 33; ++$i) {
                    $onReadable(); // the EXEC's write is blocked: read rid 1's head and 32 chunks
                }
            } elseif ($h->method === C::METHOD_CORE_WINDOW_UPDATE) {
                $dup->onWrite = null;
                $onReadable(); // the deferred credit's write is blocked: read rid 2's answer
            }
        };
        $x = $conn->scalarAsync('SELECT 7'); // rid 2
        $this->assertFalse($session->isPoisoned());
        $this->assertSame(7, $x->await());
        $this->assertNotSame([], F::windowUpdates($inner->written, 1), 'the deferred credit was written');
        $inner->feed(F::done(1));
        $this->assertSame(32, strlen($buffered->await()->body));
    }

    /**
     * R2-3, the reviewer's window: between two of the deferred writes no transport operation is in
     * progress, but the flush is. A stream's generator collected there (by the cycle collector, on
     * an allocation) abandons through {@see Session::abandonHttp}, which must still only discard — a
     * blocking drain from inside the flush would read the wire under it — and its CANCEL joins the
     * flush. Only the GENERATOR is garbage: the stream object is held, so its destructor (the
     * non-blocking path) cannot run first and make the test pass without reaching the guard.
     */
    public function testAStreamCollectedBetweenTheDeferredWritesOnlyDiscards(): void
    {
        $inner = new FakeTransport();
        $dup = new DuplexDouble($inner);
        $packer = new \Ferro\Tests\Support\HookedPacker();
        $inner->feed(F::helloAck());
        $session = new Session($dup, new RequestIdAllocator(0), encodePacker: $packer);
        $session->hello();
        $conn = new Connection($session, 'default');
        $http = $conn->upstream('up');
        $inner->feed(F::head(1) . F::body(1, 'one'));
        $s = $http->stream('GET', '/s');                 // rid 1
        $it = $s->getIterator();
        $this->assertSame('one', $it->current());
        $cycle = new \stdClass();
        $cycle->self = $cycle;
        $cycle->it = $it;
        $collected = new \stdClass();
        $collected->at = null;
        $cycle->probe = new class ($collected) {
            public function __construct(private \stdClass $c) {}

            public function __destruct() { $this->c->at ??= 'elsewhere'; }
        };
        // Automatic collection off, so the cycle is collected exactly where the hook asks — not at a
        // random allocation earlier, which would make this test pass vacuously.
        gc_disable();
        unset($it, $cycle);                               // the generator is garbage; $s is held
        $b2 = $http->requestAsync('GET', '/b2');          // rid 2: credit on receipt
        $b3 = $http->requestAsync('GET', '/b3');          // rid 3: credit on receipt
        $inner->feed(F::head(2) . str_repeat(F::body(2, 'x'), 31) . F::head(3) . str_repeat(F::body(3, 'y'), 31)
            . F::done(1));
        $readsInHook = null;
        $dup->onWrite = static function (Header $h, \Closure $onReadable) use ($dup, $packer, &$readsInHook, $collected): void {
            if ($h->service === C::SERVICE_SQL) {
                for ($i = 0; $i < 64; ++$i) {
                    $onReadable(); // both buffered exchanges reach half a window: two deferred credits
                }
            } elseif ($h->method === C::METHOD_CORE_WINDOW_UPDATE) {
                $dup->onWrite = null;
                // The next pack call encodes the SECOND deferred credit, between the two writes.
                $packer->onPack = static function () use ($dup, &$readsInHook, $collected): void {
                    $before = $dup->reads;
                    $collected->at = 'between the deferred writes';
                    gc_collect_cycles();
                    $readsInHook = $dup->reads - $before;
                };
            }
        };
        $x = $conn->scalarAsync('SELECT 7');              // rid 4
        $this->assertSame('between the deferred writes', $collected->at, 'the stream was collected inside the flush');
        $this->assertSame(0, $readsInHook, 'nothing was read from inside the flush');
        $this->assertSame([1], F::cancels($inner->written), 'the stream was CANCELled, by the flush');
        $this->assertFalse($session->isPoisoned());
        $inner->feed(self::sqlOk(4, 7) . F::done(2) . F::done(3));
        $this->assertSame(7, $x->await());
        $this->assertSame(31, strlen($b2->await()->body));
        $this->assertSame(31, strlen($b3->await()->body));
        $this->assertFalse($session->hasRequestsInFlight(), 'the stream\'s END was read and dropped');
    }

    /**
     * R2-2, in process (the reviewer's probe, review round 3): both slots are streams with full
     * windows, but the END that frees one is waiting and the socket says so. The request is sent.
     */
    public function testTheSlotRefusalReadsFirstWhenSomethingIsReadable(): void
    {
        $t = new FakeTransport();
        $d = new SignalDouble($t);
        $t->feed(F::helloAck());
        $session = new Session($d, new RequestIdAllocator(0), maxInFlight: 2);
        $session->hello();
        $http = (new Connection($session, 'default'))->upstream('up');
        $t->feed(F::head(1));
        $a = $http->stream('GET', '/a');
        $t->feed(F::head(2));
        $b = $http->stream('GET', '/b');
        for ($i = 0; $i < 64; ++$i) {
            $t->feed(F::body(1, 'a') . F::body(2, 'b'));
        }
        $t->feed(F::done(1));
        $d->signal(); // the socket is readable: the END is waiting
        $out = 'sent';
        try {
            $session->submitHttp('x');
        } catch (InFlightLimitException) {
            $out = 'refused';
        }
        $this->assertSame('sent', $out, 'a slot was about to free: its END was already readable');
        unset($a, $b);
    }

    /**
     * R2-5's re-ask (mutation R5b; the reviewer's pin, review round 3): rid 2 was CANCELled and its
     * grace has run out, but its answer arrives while rid 1's CANCEL is being written. The pass must
     * ask again whether anything is readable before judging rid 2's grace — a stale "nothing is
     * readable" from before that write closed the session under an engine that had answered.
     */
    public function testR5bReadabilityIsReAskedAfterACancel(): void
    {
        $t = new FakeTransport();
        $d = new SignalDouble($t, 0.1);
        $t->feed(F::helloAck());
        $session = new Session($d, new RequestIdAllocator(0));
        $session->hello();
        $conn = new Connection($session, 'default');
        $x = $conn->scalarAsync('SELECT 1'); // rid 1
        $y = $conn->scalarAsync('SELECT 2'); // rid 2
        $session->setDeadline(1, microtime(true) + 0.3);
        $session->setDeadline(2, microtime(true) + 0.01);
        usleep(20_000);
        $session->enforceDeadlines(); // CANCEL rid 2; its grace (0.1 s) starts
        $this->assertSame([2], F::cancels($t->written));
        usleep(350_000); // rid 1 has expired, and so has rid 2's grace
        $d->onWrite = static function (Header $h, \Closure $onReadable) use ($t, $d): void {
            if (($h->flags & C::FLAG_CANCEL) !== 0 && $h->requestId === 1) {
                // rid 2's terminal arrives while rid 1's CANCEL is written (the write did not block)
                $t->feed(Fake::cancelled(2));
                $d->signal();
            }
        };
        $session->enforceDeadlines();
        $this->assertFalse($session->isPoisoned(), "rid 2's answer was readable; its grace must not be judged silent");
        $d->unsignal();
        $t->feed(Fake::cancelled(1));
        foreach ([$y, $x] as $f) {
            try {
                $f->await();
            } catch (FerroException) {
            }
        }
        $this->assertFalse($session->hasRequestsInFlight());
    }

    /** @return array{0:FakeTransport,1:DuplexDouble,2:Session,3:Connection} */
    private function duplexConn(): array
    {
        $inner = new FakeTransport();
        $dup = new DuplexDouble($inner);
        $inner->feed(F::helloAck());
        $session = new Session($dup, new RequestIdAllocator(0));
        $session->hello();
        return [$inner, $dup, $session, new Connection($session, 'default')];
    }

    /**
     * Leave an object in a reference cycle whose destructor queries `$conn` (an RAII release).
     * Caught: an ASYNC query, whose refusal settles its Future — awaited here, as the release would.
     * Uncaught: a SYNC query, whose refusal is thrown out of the destructor.
     */
    private function cyclicDestructor(Connection $conn, \stdClass $seen, bool $catch): void
    {
        $o = new class ($conn, $seen, $catch) {
            public mixed $self;

            public function __construct(private Connection $c, private \stdClass $seen, private bool $catch)
            {
                $this->self = $this;
            }

            public function __destruct()
            {
                if (!$this->catch) {
                    $this->c->scalar('SELECT 9');
                    return;
                }
                $f = $this->c->scalarAsync('SELECT 9');
                try {
                    $f->await();
                } catch (FerroException $e) {
                    $this->seen->refused = $e;
                }
            }
        };
        unset($o);
    }
}
