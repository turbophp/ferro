<?php // /php/client/tests/Client/HttpFakeEngineTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\RetryPolicy;
use Ferro\Ferro;
use Ferro\Http\Error\HttpCancelledException;
use Ferro\Http\Error\HttpException;
use Ferro\Http\Error\HttpIndeterminateException;
use Ferro\Http\Error\HttpRetryableException;
use Ferro\Loop;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\HttpRequest;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Revolt;
use Ferro\Tests\Support\ForkedFakeEngine as Fake;
use Ferro\Tests\Support\HttpFrames as F;
use Ferro\Tests\Support\RevoltTasks;
use PHPUnit\Framework\Attributes\Medium;
use PHPUnit\Framework\Attributes\RequiresFunction;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

/**
 * M6-F8: Ferro HTTP's time-dependent rules against a scripted engine in a forked process (the
 * {@see DeadlineFakeEngineTest} pattern) — an engine that leaves a request unanswered, answers a
 * `CANCEL` or ignores it, or spaces a response's frames out, which a real `ferrod` cannot be made
 * to do on cue.
 *
 *  - §23.11.0: a request's client deadline is `timeoutMs` + 2 s; past it the client CANCELs and the
 *    ENGINE's answer decides the fate (F8 adopts D1c's wait, §22.2 (dd)); with no answer within one
 *    liveness interval the session closes and every request in flight gets its §23.7.3 fate.
 *  - Silence while a request runs is probed by PING, never a failure: a slow head and slow chunks
 *    complete.
 *  - Fiber-aware per frame: under {@see Loop} and the {@see Revolt} adapter a Fiber reading a body
 *    suspends between chunks, so another Fiber's request completes in the middle of it.
 */
#[RequiresFunction('pcntl_fork')]
#[RequiresFunction('posix_kill')]
#[Medium]
final class HttpFakeEngineTest extends TestCase
{
    /** @var list<Fake> */
    private array $fakes = [];

    protected function tearDown(): void
    {
        foreach ($this->fakes as $f) {
            $f->stop();
        }
        $this->fakes = [];
    }

    /**
     * An engine (advertising HTTP) that hands every frame to `$on` with a `$send(delay, bytes)` that
     * schedules a write, for `$seconds`.
     *
     * @param \Closure(Header, string, \Closure(float, string): void): void $on
     */
    private function engine(float $seconds, \Closure $on): Fake
    {
        return $this->fakes[] = Fake::start(static function ($c) use ($seconds, $on): void {
            /** @var list<array{0:float,1:string}> $queue */
            $queue = [];
            $send = static function (float $delay, string $bytes) use (&$queue): void {
                $queue[] = [microtime(true) + $delay, $bytes];
                usort($queue, static fn (array $a, array $b): int => $a[0] <=> $b[0]);
            };
            $end = microtime(true) + $seconds;
            while (microtime(true) < $end) {
                while ($queue !== [] && $queue[0][0] <= microtime(true)) {
                    fwrite($c, array_shift($queue)[1]);
                }
                $wait = $queue === [] ? 0.02 : max(0.0, min(0.02, $queue[0][0] - microtime(true)));
                $r = [$c];
                $w = $e = null;
                if (@stream_select($r, $w, $e, 0, max(1000, (int) ($wait * 1_000_000))) > 0) {
                    $f = Fake::readFrame($c);
                    if ($f === null) {
                        return;
                    }
                    $on($f[0], $f[1], $send);
                }
            }
        }, C::FEATURE_ENGINE_HTTP);
    }

    private static function target(Header $h, string $payload): ?string
    {
        if ($h->service !== C::SERVICE_HTTP || $h->method !== C::METHOD_HTTP_REQUEST || Fake::isCancel($h)) {
            return null;
        }
        $off = 0;
        $w = PackerFactory::forDecode()->unpack($payload, $off);
        return HttpRequest::mapFromWire(is_array($w) ? array_values($w) : [])['target'];
    }

    private static function connect(Fake $f, float $ioTimeout): Connection
    {
        return Ferro::connect($f->path, ioTimeout: $ioTimeout, policy: RetryPolicy::none());
    }

    public function testPastItsDeadlineARequestIsCancelledAndTheEnginesAnswerDecides(): void
    {
        $f = $this->engine(8.0, static function (Header $h, string $p, \Closure $send): void {
            if (Fake::isPing($h)) {
                $send(0, Fake::pong($h->requestId));
            } elseif (Fake::isCancel($h)) {
                $send(0, F::cancelled($h->requestId));
            } elseif (self::target($h, $p) === '/ok') {
                $send(0, F::head($h->requestId) . F::done($h->requestId));
            }
            // '/ignored' is never answered: only the client's backstop can end it.
        });
        $conn = self::connect($f, 0.5);
        $t0 = microtime(true);
        try {
            $conn->upstream('up')->request('GET', '/ignored', timeoutMs: 100, idempotent: true);
            $this->fail('expected the engine\'s answer to the backstop CANCEL');
        } catch (HttpCancelledException) {
            $elapsed = microtime(true) - $t0;
            $this->assertGreaterThanOrEqual(2.0, $elapsed, 'not before timeoutMs + the 2 s margin');
            $this->assertLessThan(3.5, $elapsed);
        }
        $this->assertSame(200, $conn->upstream('up')->request('GET', '/ok')->status, 'only that request failed; the session goes on');
    }

    public function testAnUnansweredCancelClosesTheSessionAndEachRequestGetsItsClientFate(): void
    {
        $f = $this->engine(8.0, static function (Header $h, string $p, \Closure $send): void {
            if (Fake::isPing($h)) {
                $send(0, Fake::pong($h->requestId)); // alive — it just never answers a request or a CANCEL
            }
        });
        $conn = self::connect($f, 0.3);
        $http = $conn->upstream('up');
        $post = $http->requestAsync('POST', '/a', body: 'x', timeoutMs: 50);
        $get = $http->requestAsync('GET', '/b', idempotent: true);
        $t0 = microtime(true);
        try {
            $post->await();
            $this->fail('expected the client fate');
        } catch (HttpIndeterminateException $e) {
            $this->assertTrue($e->clientSynthesised());
            $this->assertSame(HttpException::CLIENT_LINK_LOST, $e->cause());
            $this->assertGreaterThanOrEqual(2.0, microtime(true) - $t0);
            $this->assertLessThan(4.0, microtime(true) - $t0);
        }
        try {
            $get->await();
            $this->fail('expected the client fate');
        } catch (HttpRetryableException $e) {
            $this->assertTrue($e->clientSynthesised(), 'declared idempotent: Retryable — and still not re-sent');
        }
    }

    public function testASlowHeadAndSlowChunksAreNotADeadEngine(): void
    {
        $f = $this->engine(8.0, static function (Header $h, string $p, \Closure $send): void {
            if (Fake::isPing($h)) {
                $send(0, Fake::pong($h->requestId));
            } elseif (self::target($h, $p) === '/slow') {
                $rid = $h->requestId;
                $send(1.0, F::head($rid));
                $send(1.8, F::body($rid, 'late'));
                $send(1.9, F::done($rid));
            }
        });
        $conn = self::connect($f, 0.3); // a read timeout far below the gaps
        $t0 = microtime(true);
        $res = $conn->upstream('up')->request('GET', '/slow');
        $this->assertSame('late', $res->body);
        $this->assertGreaterThanOrEqual(1.8, microtime(true) - $t0);
    }

    /**
     * The engine answers B's request in the middle of A's body. B finishing first proves A's Fiber
     * suspended BETWEEN chunks, not only until its head.
     *
     * @return \Closure(Header, string, \Closure(float, string): void): void
     */
    private static function interleaved(): \Closure
    {
        return static function (Header $h, string $p, \Closure $send): void {
            if (Fake::isPing($h)) {
                $send(0, Fake::pong($h->requestId));
                return;
            }
            $target = self::target($h, $p);
            $rid = $h->requestId;
            if ($target === '/a') {
                $send(0.0, F::head($rid));
                $send(0.3, F::body($rid, 'a1'));
                $send(0.6, F::body($rid, 'a2'));
                $send(0.9, F::body($rid, 'a3') . F::done($rid));
            } elseif ($target === '/b') {
                $send(0.4, F::head($rid) . F::body($rid, 'b') . F::done($rid));
            }
        };
    }

    public function testUnderLoopAFiberReadingABodySuspendsBetweenChunks(): void
    {
        $conn = self::connect($this->engine(6.0, self::interleaved()), 2.0);
        $http = $conn->upstream('up');
        $done = [];
        $results = Loop::run([
            'a' => static function () use ($http, &$done): string {
                $body = $http->stream('GET', '/a')->body();
                $done[] = 'a';
                return $body;
            },
            'b' => static function () use ($http, &$done): string {
                $body = $http->requestAsync('GET', '/b')->await()->body;
                $done[] = 'b';
                return $body;
            },
        ]);
        $this->assertSame(['a' => 'a1a2a3', 'b' => 'b'], $results);
        $this->assertSame(['b', 'a'], $done, 'B completed while A was between chunks');
    }

    public function testUnderTheRevoltAdapterAFiberReadingABodySuspendsBetweenChunks(): void
    {
        $conn = self::connect($this->engine(6.0, self::interleaved()), 2.0);
        $http = $conn->upstream('up');
        Revolt::install();
        try {
            $done = [];
            $results = RevoltTasks::run([
                'a' => static function () use ($http, &$done): string {
                    $body = '';
                    foreach ($http->stream('GET', '/a') as $chunk) {
                        $body .= $chunk;
                    }
                    $done[] = 'a';
                    return $body;
                },
                'b' => static function () use ($http, &$done): string {
                    $body = $http->requestAsync('GET', '/b')->await()->body;
                    $done[] = 'b';
                    return $body;
                },
            ]);
            $this->assertSame(['a' => 'a1a2a3', 'b' => 'b'], $results);
            $this->assertSame(['b', 'a'], $done, 'B completed while A was between chunks');
        } finally {
            $left = EventLoop::getIdentifiers();
            foreach ($left as $id) {
                EventLoop::cancel($id);
            }
            Revolt::uninstall();
            $this->assertSame([], $left, 'a Revolt callback outlived the test');
        }
    }
}
