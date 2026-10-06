<?php // /php/client/tests/Client/HttpUpstreamTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\Backoff;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\RetryPolicy;
use Ferro\Client\Session;
use Ferro\Client\TraceContext;
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
use Ferro\Protocol\HttpRequest;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\StreamData;
use Ferro\Protocol\StreamHead;
use Ferro\Tests\Support\FakeTransport;
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

    public function testCreditIsReturnedFrameForFrameByWhatWasDebited(): void
    {
        $t = new FakeTransport();
        $conn = $this->conn($t);
        $t->feed(F::head(1) . F::body(1, 'abc') . F::body(1, str_repeat('z', 300)) . F::done(1));
        $conn->upstream('up')->request('GET', '/');
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
        yield 'status 199' => [F::head(1, 199)];
        yield 'status 600' => [F::head(1, 600)];
        yield 'version 12' => [F::head(1, 200, [], false, 12)];
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
}
