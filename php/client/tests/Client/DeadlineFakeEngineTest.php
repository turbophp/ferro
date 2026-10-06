<?php // /php/client/tests/Client/DeadlineFakeEngineTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\FerroException;
use Ferro\Client\RetryPolicy;
use Ferro\Ferro;
use Ferro\Loop;
use Ferro\Protocol\Header;
use Ferro\Tests\Support\ForkedFakeEngine as Fake;
use PHPUnit\Framework\Attributes\RequiresFunction;
use PHPUnit\Framework\TestCase;

/**
 * M3-D1c review F4, F6 and F7: the deadline and liveness rules against a scripted engine in a
 * forked process — one that can ignore `timeout_ms`, stop reading, or answer a PING late, which a
 * real `ferrod` cannot be made to do and the in-memory fakes cannot do while the client is blocked.
 *
 * Every connection here uses `RetryPolicy::none()`: a fake accepts exactly one connection, and these
 * tests are about what the first one reports.
 */
#[RequiresFunction('pcntl_fork')]
#[RequiresFunction('posix_kill')]
final class DeadlineFakeEngineTest extends TestCase
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

    /** @param \Closure(resource): void $serve */
    private function fake(\Closure $serve): Fake
    {
        return $this->fakes[] = Fake::start($serve);
    }

    /**
     * Serve frames for `$seconds`, handing each to `$on` (with the stream), then return — which
     * closes the connection, so a client still waiting sees EOF instead of hanging the suite.
     *
     * @param \Closure(Header, resource): void $on
     * @return \Closure(resource): void
     */
    private static function serveFor(float $seconds, \Closure $on): \Closure
    {
        return static function ($c) use ($seconds, $on): void {
            $end = microtime(true) + $seconds;
            while (microtime(true) < $end) {
                $r = [$c];
                $w = $e = null;
                if (@stream_select($r, $w, $e, 0, 20_000) > 0) {
                    $f = Fake::readFrame($c);
                    if ($f === null) {
                        return;
                    }
                    $on($f[0], $c);
                }
            }
        };
    }

    /** An engine that reads everything and answers nothing — not even a PING. */
    private static function silent(float $seconds): \Closure
    {
        return self::serveFor($seconds, static function (): void {});
    }

    /**
     * An engine that IGNORES `timeout_ms` (so only the client's backstop can end the statement):
     * PINGs are answered, an EXEC is not, and a CANCEL gets that EXEC its cancelled terminal.
     */
    private static function ignoresTimeouts(float $seconds): \Closure
    {
        return self::serveFor($seconds, static function (Header $h, $c): void {
            if (Fake::isPing($h)) {
                fwrite($c, Fake::pong($h->requestId));
            } elseif (Fake::isCancel($h)) {
                fwrite($c, Fake::cancelled($h->requestId));
            }
        });
    }

    /** An engine that answers each EXEC (and PING) after `$delay` seconds. */
    private static function answersAfter(float $delay, float $seconds): \Closure
    {
        return self::serveFor($seconds, static function (Header $h, $c) use ($delay): void {
            if (Fake::isPing($h)) {
                fwrite($c, Fake::pong($h->requestId));
            } elseif (Fake::isExec($h)) {
                usleep((int) ($delay * 1_000_000));
                fwrite($c, Fake::ok($h->requestId));
            }
        });
    }

    /** @param \Closure(): mixed $work */
    private static function outcome(\Closure $work): string
    {
        try {
            $work();
            return 'ok';
        } catch (FerroException $e) {
            return get_class($e);
        }
    }

    /**
     * An engine that stops READING after the request: the deadline's CANCEL cannot be written.
     *
     * @return \Closure(resource): void
     */
    private static function stopsReadingAfterTheExec(): \Closure
    {
        return static function ($c): void {
            while (($f = Fake::readFrame($c)) !== null) {
                if (Fake::isExec($f[0])) {
                    stream_socket_shutdown($c, STREAM_SHUT_RD);
                    usleep(4_000_000);
                    return;
                }
            }
        };
    }

    /**
     * Review F4: a CANCEL whose write fails (the peer stopped reading — a restart, a reset link) is
     * recorded on the session and fails ITS task, with every other task still run to completion;
     * it used to escape `Loop::run` past every task's catch. Several sessions: the select path.
     */
    public function testAFailedCancelWriteFailsItsTaskNotTheLoop(): void
    {
        $f1 = $this->fake(self::stopsReadingAfterTheExec());
        $f2 = $this->fake(self::silent(4.0));
        $c1 = Ferro::connect($f1->path, statementTimeout: 0.2, policy: RetryPolicy::none());
        // The second session outlives the first one's deadline (1.2 s), so the CANCEL is due while
        // the loop selects across both.
        $c2 = Ferro::connect($f2->path, ioTimeout: 1.0, policy: RetryPolicy::none());
        $r = Loop::run([
            'a' => static fn (): string => self::outcome(static fn () => $c1->scalarAsync('SELECT 1')->await()),
            'b' => static fn (): string => self::outcome(static fn () => $c2->scalarAsync('SELECT 2')->await()),
        ]);
        $this->assertNotSame('ok', $r['a'], 'the request whose CANCEL could not be written failed');
        $this->assertNotSame('ok', $r['b'], 'the silent session failed by liveness');
    }

    /** Review F4, the one-session path (`Session::pollOnce`'s deadline branch). */
    public function testAFailedCancelWriteOnTheOnlySessionFailsItsTaskNotTheLoop(): void
    {
        $f1 = $this->fake(self::stopsReadingAfterTheExec());
        $c1 = Ferro::connect($f1->path, statementTimeout: 0.2, policy: RetryPolicy::none());
        $r = Loop::run([
            'a' => static fn (): string => self::outcome(static fn () => $c1->scalarAsync('SELECT 1')->await()),
        ]);
        $this->assertNotSame('ok', $r['a']);
    }

    /**
     * Review F6: the engine answers a slow request's terminal and THEN the liveness PONG (a legal
     * order). The PONG sits unread while nothing waits on that session; the next request on it must
     * not find the session "silent with a PING unanswered" and close it — it is selected on first.
     */
    public function testALatePongDoesNotCloseAHealthySession(): void
    {
        $f1 = $this->fake(static function ($c): void {
            $r1 = Fake::readFrame($c);
            $ping = Fake::readFrame($c); // sent at the 0.5 s read timeout, while R1 still runs
            if ($r1 === null || $ping === null) {
                return;
            }
            usleep(200_000);
            fwrite($c, Fake::ok($r1[0]->requestId)); // the terminal first...
            usleep(50_000);
            fwrite($c, Fake::pong($ping[0]->requestId)); // ...then the PONG
            while (($f = Fake::readFrame($c)) !== null) {
                if (Fake::isExec($f[0])) {
                    fwrite($c, Fake::ok($f[0]->requestId, 2));
                } elseif (Fake::isPing($f[0])) {
                    fwrite($c, Fake::pong($f[0]->requestId));
                }
            }
        });
        $f2 = $this->fake(self::answersAfter(2.0, 4.0)); // keeps the loop multi-session throughout
        $f3 = $this->fake(self::answersAfter(0.8, 4.0)); // the gap between R1 and R2
        $c1 = Ferro::connect($f1->path, ioTimeout: 0.5, policy: RetryPolicy::none());
        $c2 = Ferro::connect($f2->path, policy: RetryPolicy::none());
        $c3 = Ferro::connect($f3->path, policy: RetryPolicy::none());
        $r = Loop::run([
            'a' => static function () use ($c1, $c3): string {
                return self::outcome(static function () use ($c1, $c3): void {
                    $c1->scalarAsync('R1')->await();
                    $c3->scalarAsync('gap')->await();
                    $c1->scalarAsync('R2')->await();
                });
            },
            'b' => static fn (): string => self::outcome(static fn () => $c2->scalarAsync('B')->await()),
        ]);
        $this->assertSame(['a' => 'ok', 'b' => 'ok'], $r);
    }

    /**
     * Review F7 (M8): a silent session is PROBED without blocking the others. A blocking read on it
     * would stall every other session for up to two read timeouts.
     */
    public function testASilentSessionDoesNotStallTheOthers(): void
    {
        $fs = $this->fake(self::silent(4.0));
        $fb = $this->fake(self::answersAfter(0.03, 6.0));
        $silent = Ferro::connect($fs->path, ioTimeout: 0.5, policy: RetryPolicy::none());
        $busy = Ferro::connect($fb->path, policy: RetryPolicy::none());
        $r = Loop::run([
            'silent' => static fn (): string => self::outcome(static fn () => $silent->scalarAsync('x')->await()),
            'busy' => static function () use ($busy): float {
                $last = microtime(true);
                $gap = 0.0;
                for ($i = 0; $i < 50; ++$i) {
                    $busy->scalarAsync('y')->await();
                    $gap = max($gap, microtime(true) - $last);
                    $last = microtime(true);
                }
                return $gap;
            },
        ]);
        $this->assertNotSame('ok', $r['silent'], 'the silent session failed by liveness');
        $this->assertLessThan(0.4, $r['busy'], sprintf('the busy session stalled for %.2f s', $r['busy']));
    }

    /**
     * Review F7 (M12, M15, M17, M13c): under `Ferro\Loop`, an asynchronous request whose engine
     * ignores its `timeout_ms` is CANCELled by the client's backstop on time — the loop wakes for
     * the deadline and acts on it while it selects across several sessions. The backstop is the
     * statement timeout plus two seconds.
     */
    public function testTheLoopCancelsADueRequestOnTime(): void
    {
        $fa = $this->fake(self::ignoresTimeouts(4.0));
        $fb = $this->fake(self::answersAfter(3.5, 5.0));
        $a = Ferro::connect($fa->path, statementTimeout: 0.05, policy: RetryPolicy::none());
        $b = Ferro::connect($fb->path, policy: RetryPolicy::none());
        $start = microtime(true);
        $r = Loop::run([
            'a' => static function () use ($a, $start): array {
                return [self::outcome(static fn () => $a->scalarAsync('x')->await()), microtime(true) - $start];
            },
            'b' => static fn (): string => self::outcome(static fn () => $b->scalarAsync('y')->await()),
        ]);
        [$outcome, $at] = $r['a'];
        $this->assertSame(\Ferro\Client\Error\CancelledException::class, $outcome, 'the engine answered the CANCEL with the cancelled terminal');
        $this->assertGreaterThan(1.9, $at, 'not before the backstop (0.05 s + 2 s)');
        $this->assertLessThan(2.6, $at, sprintf('CANCELled at the backstop, not later: %.2f s', $at));
        $this->assertSame('ok', $r['b']);
    }

    /** Review F7 (M13c): the synchronous path through `Ferro::connect` is given the backstop too. */
    public function testTheSyncBackstopCancelsAnEngineThatIgnoresTimeoutMs(): void
    {
        $fa = $this->fake(self::ignoresTimeouts(4.0));
        $a = Ferro::connect($fa->path, statementTimeout: 0.05, policy: RetryPolicy::none());
        $start = microtime(true);
        $outcome = self::outcome(static fn () => $a->scalar('x'));
        $at = microtime(true) - $start;
        $this->assertSame(\Ferro\Client\Error\CancelledException::class, $outcome);
        $this->assertGreaterThan(1.9, $at);
        $this->assertLessThan(2.6, $at, sprintf('CANCELled at the backstop, not later: %.2f s', $at));
        $this->assertFalse($a->session()->isPoisoned(), 'only the request was cancelled');
    }
}
