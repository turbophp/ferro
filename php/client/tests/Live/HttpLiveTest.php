<?php // /php/client/tests/Live/HttpLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Session;
use Ferro\Client\Transport;
use Ferro\Ferro;
use Ferro\Http\Error\HttpException;
use Ferro\Http\Error\HttpIndeterminateException;
use Ferro\Http\Error\HttpNonRetryableException;
use Ferro\Http\Error\HttpRetryableException;
use Ferro\Http\Error\RequestTooLargeException;
use Ferro\Http\FateClass;
use Ferro\Loop;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Revolt;
use Ferro\Tests\Support\RevoltTasks;
use PHPUnit\Framework\Attributes\DataProvider;
use Revolt\EventLoop;
use function Ferro\await;

/**
 * M6-F8 (SPEC §23.11.1): the native Ferro HTTP API against a real `ferrod` and a recording loopback
 * upstream — one socket carrying SQL and HTTP together, streams under credit, abandonment, fates,
 * and the D1c deadline/liveness rules applied to HTTP.
 */
final class HttpLiveTest extends HttpLiveTestCase
{
    /** @var list<Connection> */
    private array $conns = [];

    protected function tearDown(): void
    {
        foreach ($this->conns as $c) {
            try {
                $c->exec('DROP TABLE IF EXISTS f8_http_writes');
            } catch (\Throwable) {
            }
            $c->session()->close();
        }
        $this->conns = [];
        parent::tearDown();
    }

    private function conn(bool $receiveFds = false, float $ioTimeout = 5.0): Connection
    {
        return $this->conns[] = Ferro::connect($this->socketPath, ioTimeout: $ioTimeout, receiveFds: $receiveFds);
    }

    /** @return iterable<string, array{bool}> both client read paths (M3-D3) */
    public static function readPaths(): iterable
    {
        yield 'fread' => [false];
        if (Transport::canReceiveFds()) { // the CI `fread` lane disables socket_recvmsg
            yield 'recvmsg' => [true];
        }
    }

    #[DataProvider('readPaths')]
    public function testABufferedRequestRoundTrips(bool $receiveFds): void
    {
        $session = $this->conn($receiveFds)->session();
        $this->assertInstanceOf(Session::class, $session);
        $this->assertSame($receiveFds, $session->receivesFds(), 'the read path under test');
        $this->assertNotSame(0, $session->engineFeatures() & C::FEATURE_ENGINE_HTTP, 'the engine advertises HTTP');

        $res = $this->conn($receiveFds)->upstream('up')->request(
            'POST', '/echo?x=1', headers: ['X-Test' => 'v', 'X-Multi' => ['1', '2']], body: "a\x00b\xff",
        );
        $this->assertSame(200, $res->status);
        $this->assertSame(11, $res->version);
        $this->assertFalse($res->idempotent);
        $this->assertSame('echo', $res->header('X-Upstream'));
        $this->assertSame(['echo'], $res->headers['x-upstream'], 'response header names are lowercase');
        $echo = json_decode($res->body, true);
        $this->assertIsArray($echo);
        $this->assertSame('POST', $echo['method']);
        $this->assertSame('/echo?x=1', $echo['target']);
        $this->assertSame("a\x00b\xff", base64_decode((string) $echo['body']));
        $sent = array_values(array_filter($echo['headers'], static fn (array $h): bool => str_starts_with(strtolower($h[0]), 'x-')));
        $this->assertSame([['X-Test', 'v'], ['X-Multi', '1'], ['X-Multi', '2']], $sent);
        $this->assertSame(1, $this->received('/echo'));
        $this->assertGreaterThan(0, $res->stats['bytes_sent']);
        $this->assertGreaterThan(0, $res->stats['total_us']);
    }

    public function testAStatusIsAResponseAndItsFateIsAdvisory(): void
    {
        $http = $this->conn()->upstream('up');
        $r404 = $http->request('GET', '/status/404');
        $this->assertSame(404, $r404->status);
        $this->assertSame('status 404', $r404->body);
        $this->assertSame(FateClass::NonRetryable, $r404->statusFate()->fate);

        $r503 = $http->request('POST', '/status/503?retry_after=3');
        $this->assertSame(FateClass::Retryable, $r503->statusFate()->fate);
        $this->assertSame(3000, $r503->statusFate()->retryAfterMs);

        $r500 = $http->request('POST', '/status/500');
        $this->assertSame(FateClass::Indeterminate, $r500->statusFate()->fate, 'a non-idempotent 500 promises nothing');
        $ops500 = $this->conn()->upstream('ops')->request('GET', '/status/500');
        $this->assertTrue($ops500->idempotent, 'the operator declared GET idempotent on ops');
        $this->assertSame(FateClass::Retryable, $ops500->statusFate()->fate);
    }

    public function testIdempotencyIsADeclarationNeverTheMethod(): void
    {
        $conn = $this->conn();
        $this->assertFalse($conn->upstream('up')->request('GET', '/echo')->idempotent, 'GET alone licenses nothing');
        $this->assertTrue($conn->upstream('up')->request('POST', '/echo', idempotent: true)->idempotent, 'the caller declared it');
        $this->assertTrue($conn->upstream('ops')->request('GET', '/echo')->idempotent, 'the operator declared it');
        $this->assertFalse($conn->upstream('ops')->request('GET', '/echo', idempotent: false)->idempotent, 'false downgrades');
    }

    public function testPolicyRefusalsAreForbiddenAndNeverSent(): void
    {
        $conn = $this->conn();
        $cases = [
            'forbidden_upstream' => fn () => $conn->upstream('nope')->request('GET', '/echo'),
            'forbidden_target' => fn () => $conn->upstream('up')->request('GET', '/echo/..;/admin'),
            'forbidden_method' => fn () => $conn->upstream('up')->request('TRACE', '/echo'),
            'forbidden_origin' => fn () => $conn->upstream('up', 'http://elsewhere.example')->request('GET', '/echo'),
            'forbidden_header' => fn () => $conn->upstream('up')->request('GET', '/echo', headers: ['X-Forwarded-For' => '1.2.3.4']),
        ];
        foreach ($cases as $cause => $call) {
            try {
                $call();
                $this->fail("{$cause}: expected a refusal");
            } catch (HttpNonRetryableException $e) {
                $this->assertSame(C::ERR_FORBIDDEN, $e->errorCode(), $cause);
                $this->assertSame($cause, $e->cause());
                $this->assertFalse($e->clientSynthesised());
            }
        }
        $this->assertSame(0, $this->received('/'), 'nothing reached the upstream');
        $this->assertSame(200, $conn->upstream('up')->request('GET', '/echo')->status, 'the session goes on');
    }

    /**
     * Dial failures never send anything, so even a POST is Retryable: a refused port, and an
     * `https` origin whose port speaks plain HTTP — the TLS handshake never completes, so the dial
     * times out before any request byte exists (§23.7.1's dial rows; `https` is served since F5a).
     */
    public function testADialFailureIsRetryableAndNeverSent(): void
    {
        $conn = $this->conn();
        $cases = [
            'dead' => C::HTTP_CAUSE_CONNECT_REFUSED,
            'tls' => C::HTTP_CAUSE_CONNECT_TIMEOUT,
        ];
        foreach ($cases as $upstream => $cause) {
            try {
                $conn->upstream($upstream)->request('POST', '/echo', body: 'x');
                $this->fail("{$upstream}: expected the dial failure");
            } catch (HttpRetryableException $e) {
                $this->assertSame(C::ERR_UPSTREAM_UNAVAILABLE, $e->errorCode(), $upstream);
                $this->assertSame($cause, $e->cause(), $upstream);
                $this->assertFalse($e->clientSynthesised());
                $this->assertSame(FateClass::Retryable, $e->fate(), 'never sent, so Retryable even for a POST');
            }
        }
        $this->assertSame(0, $this->received('/'));
    }

    public function testTheEnginesTimeoutIsTheEnginesFate(): void
    {
        $conn = $this->conn();
        $http = $conn->upstream('up');
        try {
            $http->request('POST', '/hold?case=post', body: 'x', timeoutMs: 300);
            $this->fail('expected Indeterminate');
        } catch (HttpIndeterminateException $e) {
            $this->assertSame(C::HTTP_CAUSE_TIMEOUT, $e->cause());
            $this->assertFalse($e->clientSynthesised(), 'the ENGINE answered, long before the client backstop');
        }
        try {
            $http->request('GET', '/hold?case=get', timeoutMs: 300, idempotent: true);
            $this->fail('expected QueryTimeout');
        } catch (HttpNonRetryableException $e) {
            $this->assertSame(C::ERR_QUERY_TIMEOUT, $e->errorCode(), "§9.2's read rule for a declared-idempotent request");
            $this->assertSame(C::HTTP_CAUSE_TIMEOUT, $e->cause());
        }
        $this->assertSame(1, $this->received('/hold?case=post'), 'sent once, never again');
        $this->assertSame(1, $this->received('/hold?case=get'));
        $this->assertSame(1, $conn->scalar('SELECT 1'), 'only those requests failed');
    }

    public function testABodyPastTheFrameCapIsRefusedLocally(): void
    {
        $conn = $this->conn();
        try {
            $conn->upstream('up')->request('POST', '/echo', body: str_repeat('b', C::MAX_FRAME_PAYLOAD));
            $this->fail('expected the refusal');
        } catch (RequestTooLargeException) {
        }
        $this->assertSame(0, $this->received('/echo'));
        $this->assertSame(1, $conn->scalar('SELECT 1'), 'the session was not killed by an oversize frame');
    }

    /**
     * §23.15 F8's claim, with its control: SQL and HTTP fan out on ONE socket, so three slow API calls
     * and a slow query cost about the slowest, not the sum.
     */
    #[DataProvider('readPaths')]
    public function testDbAndHttpFanOutOnOneSocket(bool $receiveFds): void
    {
        $conn = $this->conn($receiveFds);
        $http = $conn->upstream('up');
        $conn->exec('CREATE TABLE IF NOT EXISTS f8_http_writes (id serial PRIMARY KEY, note text)');

        $t0 = microtime(true);
        $http->request('GET', '/delay/400');
        $http->request('GET', '/delay/400');
        $conn->scalar('SELECT pg_sleep(0.4) IS NULL');
        $sequential = microtime(true) - $t0;
        $this->assertGreaterThanOrEqual(1.2, $sequential, 'control: the delays really delay');

        $t0 = microtime(true);
        $out = await([
            'a' => $http->requestAsync('GET', '/delay/400'),
            'b' => $http->requestAsync('POST', '/delay/400', body: 'x'),
            'sleep' => $conn->scalarAsync('SELECT pg_sleep(0.4) IS NULL'),
            'write' => $conn->execAsync("INSERT INTO f8_http_writes (note) VALUES ('fan-out')"),
        ]);
        $concurrent = microtime(true) - $t0;
        $this->assertLessThan(0.9, $concurrent, sprintf('fan-out %.3fs vs sequential %.3fs', $concurrent, $sequential));
        $this->assertSame('delayed', $out['a']->body);
        $this->assertSame('delayed', $out['b']->body);
        $this->assertSame(1, $out['write']);
        $this->assertSame(1, $conn->scalar("SELECT count(*) FROM f8_http_writes WHERE note = 'fan-out'"));
    }

    /**
     * §23.11.0's acceptance, scaled: an upstream whose time to first byte is several times the
     * client's read timeout, beside a DB write on the same session — both complete, because silence
     * is probed by PING, not treated as death.
     */
    public function testASlowTtfbBesideADbWriteOnOneSession(): void
    {
        $conn = $this->conn(ioTimeout: 0.5);
        $conn->exec('CREATE TABLE IF NOT EXISTS f8_http_writes (id serial PRIMARY KEY, note text)');
        $slow = $conn->upstream('up')->requestAsync('POST', '/delay/2500', body: 'x');
        $this->assertSame(1, $conn->exec("INSERT INTO f8_http_writes (note) VALUES ('beside')"), 'the write completes while the call is in flight');
        $this->assertSame('delayed', $slow->await()->body);
        $this->assertSame(1, $this->received('/delay/2500'));
    }

    /** §23.11.0's acceptance, literally: a 30 s TTFB and a DB write, under the default configuration. */
    public function testAThirtySecondTtfbBesideADbWriteUnderTheDefaultConfiguration(): void
    {
        $conn = $this->conn();
        $conn->exec('CREATE TABLE IF NOT EXISTS f8_http_writes (id serial PRIMARY KEY, note text)');
        $t0 = microtime(true);
        $slow = $conn->upstream('up')->requestAsync('POST', '/delay/30000', body: 'x');
        $this->assertSame(1, $conn->exec("INSERT INTO f8_http_writes (note) VALUES ('thirty')"));
        $this->assertLessThan(5.0, microtime(true) - $t0, 'the write did not wait for the call');
        $this->assertSame('delayed', $slow->await()->body);
        $this->assertGreaterThanOrEqual(30.0, microtime(true) - $t0);
    }

    /**
     * Incremental delivery (§23.12's SSE case), with a stall probe calibrated so a buffering client
     * FAILS: the first chunk is in hand long before the upstream has written the last one.
     */
    #[DataProvider('readPaths')]
    public function testAStreamDeliversEachChunkAsItArrives(bool $receiveFds): void
    {
        $s = $this->conn($receiveFds)->upstream('up')->stream('GET', '/stream?chunks=4&size=8&gap=400');
        $this->assertSame(200, $s->status);
        $this->assertSame('text/event-stream', $s->header('content-type'));
        $t0 = microtime(true);
        $at = [];
        $chunks = [];
        foreach ($s as $chunk) {
            $at[] = microtime(true) - $t0;
            $chunks[] = $chunk;
        }
        $this->assertSame(['.......0', '.......1', '.......2', '.......3'], $chunks);
        $this->assertLessThan(0.3, $at[0], 'the first chunk did not wait for the rest');
        $this->assertGreaterThanOrEqual(1.1, $at[3], 'control: the chunks really were spaced');
        $this->assertTrue($s->isComplete());
        $this->assertSame(0, $s->stats()['tls_us'] ?? -1);
    }

    public function testStreamsAreNotExclusive(): void
    {
        $conn = $this->conn();
        $http = $conn->upstream('up');
        $a = $http->stream('GET', '/stream?chunks=3&size=4&gap=50');
        $b = $http->stream('GET', '/stream?chunks=3&size=6&gap=50');
        $ia = $a->getIterator();
        $ib = $b->getIterator();
        $got = [];
        while ($ia->valid() || $ib->valid()) {
            if ($ia->valid()) {
                $got['a'][] = $ia->current();
                $ia->next();
            }
            $this->assertSame(1, $conn->scalar('SELECT 1'), 'SQL between the chunks of two open streams');
            if ($ib->valid()) {
                $got['b'][] = $ib->current();
                $ib->next();
            }
        }
        $this->assertSame(['...0', '...1', '...2'], $got['a']);
        $this->assertSame(['.....0', '.....1', '.....2'], $got['b']);

        // …and an HTTP request goes out while a SQL stream is open.
        $rows = $conn->stream('SELECT generate_series(1, 3) AS n');
        $first = $rows->current();
        $this->assertSame(['n' => 1], $first);
        $this->assertSame(200, $http->request('GET', '/echo')->status);
        $rest = [];
        for ($rows->next(); $rows->valid(); $rows->next()) {
            $rest[] = $rows->current()['n'];
        }
        $this->assertSame([2, 3], $rest);
    }

    /**
     * The C1d lesson, for HTTP: abandoning a stream must not damage the NEXT request — and it must
     * stop the exchange, which only a CANCEL does (the upstream sees its connection closed).
     */
    #[DataProvider('readPaths')]
    public function testAnAbandonedStreamIsStoppedAndTheNextQueryIsUnharmed(bool $receiveFds): void
    {
        $conn = $this->conn($receiveFds);
        $s = $conn->upstream('up')->stream('GET', '/stream?chunks=2000&size=100&gap=10');
        $n = 0;
        foreach ($s as $_) {
            if (++$n === 2) {
                break;
            }
        }
        $this->assertTrue($s->isClosed());
        $session = $conn->session();
        $this->assertInstanceOf(Session::class, $session);
        $this->assertFalse($session->hasRequestsInFlight(), 'drained to its terminal');
        $t0 = microtime(true);
        $this->assertSame(7, $conn->scalar('SELECT 7'));
        $this->assertLessThan(1.0, microtime(true) - $t0);
        $this->eventually(fn (): bool => $this->closedConnections('/stream') === 1, 3.0, 'the engine closed the upstream connection');
    }

    /**
     * Credit: a consumer that stops reading stops the upstream (it can write only about one window
     * past what was consumed), the client holds about one window, and replenishing as it consumes
     * moves the whole body — without WINDOW_UPDATE it would stall at the first 16 MiB.
     */
    public function testBackpressureBoundsTheUpstreamAndPhpMemory(): void
    {
        $total = 64 * 1024 * 1024;
        $conn = $this->conn();
        $base = memory_get_usage();
        memory_reset_peak_usage();
        $s = $conn->upstream('up')->stream('GET', "/big?bytes={$total}", timeoutMs: 60_000);
        $it = $s->getIterator();
        $read = strlen($it->current());
        usleep(1_500_000); // stalled: no credit returned
        $written = max([0, ...array_map(static fn (array $l): int => (int) ($l['written'] ?? 0), $this->upstreamLog())]);
        $this->assertLessThan(28 * 1024 * 1024, $written, sprintf('the upstream wrote %d bytes against one 16 MiB window', $written));
        $this->assertGreaterThan(8 * 1024 * 1024, $written, 'control: it did write ahead into the window');
        for ($it->next(); $it->valid(); $it->next()) {
            $read += strlen($it->current());
        }
        $this->assertSame($total, $read);
        // Measured ~1 MB: unread frames wait in the socket and the engine, not in PHP. One window
        // (16 MiB) is the stated bound; a buffering client holds the whole 64 MiB.
        $this->assertLessThan(16 * 1024 * 1024, memory_get_peak_usage() - $base, 'at most one window held, never the body');
    }

    /**
     * Overlap, AND suspension per frame: the stream's body takes ~1.2 s, and the call and the query
     * (~0.3 s) finish in the middle of it — which they can only do if the stream's Fiber suspends
     * BETWEEN chunks, not only until its head.
     */
    public function testFibersOverlapHttpAndSqlUnderLoop(): void
    {
        $conn = $this->conn();
        $http = $conn->upstream('up');
        $done = [];
        $t0 = microtime(true);
        $out = Loop::run([
            'stream' => static function () use ($http, &$done): string {
                $body = $http->stream('GET', '/stream?chunks=5&size=2&gap=300')->body();
                $done[] = 'stream';
                return $body;
            },
            'call' => static function () use ($http, &$done): string {
                $body = $http->requestAsync('GET', '/delay/300')->await()->body;
                $done[] = 'call';
                return $body;
            },
            'sql' => static function () use ($conn, &$done): mixed {
                $v = $conn->scalarAsync('SELECT 5 FROM pg_sleep(0.3)')->await();
                $done[] = 'sql';
                return $v;
            },
        ]);
        $this->assertLessThan(1.6, microtime(true) - $t0);
        $this->assertSame(['stream' => '.0.1.2.3.4', 'call' => 'delayed', 'sql' => 5], $out);
        $this->assertSame('stream', $done[2], 'the call and the query finished while the stream was between chunks');
    }

    public function testFibersOverlapHttpAndSqlUnderTheRevoltAdapter(): void
    {
        $conn = $this->conn();
        $http = $conn->upstream('up');
        Revolt::install();
        try {
            $ticks = 0;
            $timer = EventLoop::repeat(0.05, static function () use (&$ticks): void { ++$ticks; });
            $t0 = microtime(true);
            $out = RevoltTasks::run([
                'call' => static fn (): string => $http->requestAsync('GET', '/delay/500')->await()->body,
                'sql' => static fn (): mixed => $conn->scalarAsync('SELECT 5 FROM pg_sleep(0.5)')->await(),
                'stop' => static function () use ($timer): void {
                    RevoltTasks::delay(0.6);
                    EventLoop::cancel($timer);
                },
            ]);
            $this->assertLessThan(0.95, microtime(true) - $t0);
            $this->assertSame('delayed', $out['call']);
            $this->assertSame(5, $out['sql']);
            $this->assertGreaterThanOrEqual(5, $ticks, 'the loop ran timers while both were in flight');
        } finally {
            foreach (EventLoop::getIdentifiers() as $id) {
                EventLoop::cancel($id);
            }
            Revolt::uninstall();
        }
    }
}
