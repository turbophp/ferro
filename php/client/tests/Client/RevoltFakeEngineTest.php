<?php // /php/client/tests/Client/RevoltFakeEngineTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\Error\CancelledException;
use Ferro\Client\RetryPolicy;
use Ferro\Client\RevoltWatch;
use Ferro\Client\Session;
use Ferro\Client\Transport;
use Ferro\Client\Waiter;
use Ferro\Ferro;
use Ferro\Loop;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\StreamData;
use Ferro\Protocol\StreamHead;
use Ferro\Revolt;
use Ferro\Tests\Support\FdOnlyDriver;
use Ferro\Tests\Support\ForkedFakeEngine as Fake;
use Ferro\Tests\Support\OpaqueDriver;
use Ferro\Tests\Support\RevoltTasks;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Medium;
use PHPUnit\Framework\Attributes\RequiresFunction;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;
use Revolt\EventLoop\Driver;
use Revolt\EventLoop\Driver\StreamSelectDriver;
use Revolt\EventLoop\Driver\TracingDriver;

/**
 * M3-D1d: the Revolt adapter against a scripted engine in a forked process — one that can answer
 * out of order, ignore `timeout_ms`, stop reading or stay silent, which a real `ferrod` cannot be
 * made to do. The same rules {@see DeadlineFakeEngineTest} pins for `Ferro\Loop`, driven by the
 * Revolt event loop instead.
 *
 * Every test ends with NO Revolt callback left registered ({@see tearDown}): a watcher that outlived
 * its waiters would keep every `EventLoop::run()` alive.
 */
#[RequiresFunction('pcntl_fork')]
#[RequiresFunction('posix_kill')]
#[Medium] // a 10 s limit per test, enforced (phpunit.xml.dist): a hang fails loudly
final class RevoltFakeEngineTest extends TestCase
{
    /** @var list<Fake> */
    private array $fakes = [];

    protected function setUp(): void
    {
        Revolt::install();
    }

    protected function tearDown(): void
    {
        foreach ($this->fakes as $f) {
            $f->stop();
        }
        $this->fakes = [];
        $left = EventLoop::getIdentifiers();
        foreach ($left as $id) {
            EventLoop::cancel($id);
        }
        Revolt::uninstall();
        $this->assertSame([], $left, 'a Revolt callback outlived the test');
    }

    /** @param \Closure(resource): void $serve */
    private function fake(\Closure $serve): Fake
    {
        return $this->fakes[] = Fake::start($serve);
    }

    private static function connect(Fake $f, float $ioTimeout = 5.0, ?float $statementTimeout = null): Connection
    {
        return Ferro::connect($f->path, ioTimeout: $ioTimeout, policy: RetryPolicy::none(), statementTimeout: $statementTimeout);
    }

    /**
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

    /** Answers each EXEC (and PING) after `$delay` seconds, one at a time. */
    private static function answersAfter(float $delay, float $seconds): \Closure
    {
        return self::serveFor($seconds, static function (Header $h, $c) use ($delay): void {
            if (Fake::isPing($h)) {
                fwrite($c, Fake::pong($h->requestId));
            } elseif (Fake::isExec($h)) {
                usleep((int) ($delay * 1_000_000));
                fwrite($c, Fake::ok($h->requestId, 7));
            }
        });
    }

    private static function outcome(mixed $r): string
    {
        return $r instanceof \Throwable ? get_class($r) : 'ok';
    }

    /**
     * Two Fibers, two requests: the engine reads BOTH requests before answering either, then answers
     * in REVERSE order. That only completes if the first Fiber's await suspended (letting the second
     * submit), and each Fiber must get its own value — a misrouted terminal would swap them.
     */
    public function testAwaitsSuspendAndEachFiberGetsItsOwnTerminal(): void
    {
        $f = $this->fake(static function ($c): void {
            $a = Fake::readFrame($c);
            $b = Fake::readFrame($c);
            if ($a === null || $b === null) {
                return;
            }
            fwrite($c, Fake::ok($b[0]->requestId, 2));
            usleep(50_000);
            fwrite($c, Fake::ok($a[0]->requestId, 1));
            while (Fake::readFrame($c) !== null) {
            }
        });
        $conn = self::connect($f, ioTimeout: 2.0);
        $order = [];
        $r = RevoltTasks::run([
            'a' => static function () use ($conn, &$order): mixed {
                $v = $conn->scalarAsync('A')->await();
                $order[] = 'a';
                return $v;
            },
            'b' => static function () use ($conn, &$order): mixed {
                $v = $conn->scalarAsync('B')->await();
                $order[] = 'b';
                return $v;
            },
        ]);
        $this->assertSame(['a' => 1, 'b' => 2], $r);
        $this->assertSame(['b', 'a'], $order, 'the Fiber whose terminal came first resumed first');
    }

    /** The loop is not blocked while a request is in flight: a timer keeps firing. */
    public function testATimerFiresWhileAnAwaitIsPending(): void
    {
        $f = $this->fake(self::answersAfter(0.6, 2.0));
        $conn = self::connect($f);
        $ticks = 0;
        $ticksAtAnswer = -1;
        $timer = EventLoop::repeat(0.05, static function () use (&$ticks): void {
            ++$ticks;
        });
        $r = RevoltTasks::run([
            'q' => static function () use ($conn, &$ticks, &$ticksAtAnswer, $timer): mixed {
                try {
                    return $conn->scalarAsync('slow')->await();
                } finally {
                    $ticksAtAnswer = $ticks;
                    EventLoop::cancel($timer);
                }
            },
        ]);
        $this->assertSame(['q' => 7], $r);
        $this->assertGreaterThanOrEqual(8, $ticksAtAnswer, "only {$ticksAtAnswer} ticks in a 0.6 s await");
    }

    /** `{main}` suspends too: awaiting there runs the event loop, so queued work progresses. */
    public function testAnAwaitInMainRunsTheEventLoop(): void
    {
        $f = $this->fake(self::answersAfter(0.4, 2.0));
        $conn = self::connect($f);
        $ticks = 0;
        $timer = EventLoop::repeat(0.05, static function () use (&$ticks): void {
            ++$ticks;
        });
        try {
            $this->assertSame(7, $conn->scalarAsync('slow')->await());
        } finally {
            EventLoop::cancel($timer);
        }
        $this->assertGreaterThanOrEqual(5, $ticks, "only {$ticks} ticks while {main} awaited 0.4 s");
    }

    /**
     * A Fiber awaiting while the Revolt loop is NOT running is not suspended into it — nothing would
     * ever resume it. It blocks, and finishes inside its own `start()`.
     */
    public function testAFiberOutsideARunningLoopBlocksInsteadOfSuspending(): void
    {
        $f = $this->fake(self::answersAfter(0.1, 2.0));
        $conn = self::connect($f);
        $fiber = new \Fiber(static fn (): mixed => $conn->scalarAsync('x')->await());
        $fiber->start();
        $this->assertTrue($fiber->isTerminated(), 'the Fiber was suspended into a loop nobody runs');
        $this->assertSame(7, $fiber->getReturn());
    }

    /**
     * A Fiber another scheduler drives, suspended through the adapter and then resumed by that
     * scheduler, gets a LogicException at its await — and the event loop does not crash later
     * trying to resume it.
     */
    public function testAFiberResumedByAnotherSchedulerIsRefusedNotCrashed(): void
    {
        $f = $this->fake(self::answersAfter(0.2, 2.0));
        $conn = self::connect($f);
        $caught = null;
        EventLoop::queue(static function () use ($conn, &$caught): void {
            $foreign = new \Fiber(static fn (): mixed => $conn->scalarAsync('x')->await());
            $foreign->start(); // suspends through Revolt: the loop is running
            try {
                $foreign->resume('not the loop');
            } catch (\LogicException $e) {
                $caught = $e;
            }
        });
        EventLoop::run(); // must return normally, with no FiberError from a later resume
        $this->assertInstanceOf(\LogicException::class, $caught);
        $this->assertStringContainsString('other than the Revolt event loop', $caught->getMessage());
    }

    /**
     * M3-D1c's backstop under Revolt: the engine ignores `timeout_ms`, so only the client's deadline
     * (statement timeout + 2 s) can end the request. The adapter's timer must wake for it and CANCEL;
     * the engine's cancelled terminal then decides the fate.
     */
    public function testTheBackstopCancelsADueRequestOnTime(): void
    {
        $f = $this->fake(self::serveFor(4.0, static function (Header $h, $c): void {
            if (Fake::isPing($h)) {
                fwrite($c, Fake::pong($h->requestId));
            } elseif (Fake::isCancel($h)) {
                fwrite($c, Fake::cancelled($h->requestId));
            }
        }));
        $conn = self::connect($f, statementTimeout: 0.05);
        $start = microtime(true);
        $r = RevoltTasks::run([
            'a' => static fn (): array => [self::outcome(self::attempt(static fn () => $conn->scalarAsync('x')->await())), microtime(true) - $start],
        ]);
        [$outcome, $at] = $r['a'];
        $this->assertSame(CancelledException::class, $outcome);
        $this->assertGreaterThan(1.9, $at, 'not before the backstop (0.05 s + 2 s)');
        $this->assertLessThan(2.6, $at, sprintf('CANCELled at the backstop, not later: %.2f s', $at));
        $this->assertFalse($conn->session()->isPoisoned(), 'only the request was cancelled');
    }

    /**
     * M3-D1c review F4 under Revolt: the CANCEL cannot be written (the peer stopped reading). That
     * fails the request's own task; nothing escapes `EventLoop::run()`.
     */
    public function testAFailedCancelWriteFailsItsTaskNotTheLoop(): void
    {
        $f = $this->fake(static function ($c): void {
            while (($fr = Fake::readFrame($c)) !== null) {
                if (Fake::isExec($fr[0])) {
                    stream_socket_shutdown($c, STREAM_SHUT_RD);
                    usleep(4_000_000);
                    return;
                }
            }
        });
        $other = $this->fake(self::answersAfter(0.1, 4.0));
        $conn = self::connect($f, statementTimeout: 0.2);
        $ok = self::connect($other);
        $r = RevoltTasks::run([
            'a' => static fn (): mixed => $conn->scalarAsync('x')->await(),
            'b' => static fn (): mixed => $ok->scalarAsync('y')->await(),
        ]);
        $this->assertInstanceOf(\Ferro\Client\Error\FerroException::class, $r['a']);
        $this->assertSame(7, $r['b']);
    }

    /**
     * Liveness under Revolt: a silent engine is PINGed after one read timeout and the session closed
     * after a second — failing ITS task — while a busy session beside it is never stalled.
     */
    public function testASilentSessionFailsByLivenessWithoutStallingTheOthers(): void
    {
        $fs = $this->fake(self::serveFor(4.0, static function (): void {}));
        $fb = $this->fake(self::answersAfter(0.03, 6.0));
        $silent = self::connect($fs, ioTimeout: 0.5);
        $busy = self::connect($fb);
        $start = microtime(true);
        $r = RevoltTasks::run([
            'silent' => static function () use ($silent, $start): array {
                $outcome = self::outcome(self::attempt(static fn () => $silent->scalarAsync('x')->await()));
                return [$outcome, microtime(true) - $start];
            },
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
        [$outcome, $at] = $r['silent'];
        $this->assertNotSame('ok', $outcome, 'the silent session failed by liveness');
        $this->assertGreaterThan(0.9, $at, 'after two read timeouts, not one');
        $this->assertLessThan(1.6, $at, sprintf('the silent session failed at %.2f s', $at));
        $this->assertLessThan(0.4, $r['busy'], sprintf('the busy session stalled for %.2f s', $r['busy']));
    }

    /**
     * M3-D1c review F6 under Revolt: the engine answers a slow request's terminal and THEN the PONG
     * to the liveness PING the silence provoked. Neither may make the healthy session look dead.
     */
    public function testALatePongDoesNotCloseAHealthySession(): void
    {
        $f = $this->fake(static function ($c): void {
            $r1 = Fake::readFrame($c);
            $ping = Fake::readFrame($c); // sent at the 0.5 s read timeout
            if ($r1 === null || $ping === null) {
                return;
            }
            usleep(200_000);
            fwrite($c, Fake::ok($r1[0]->requestId));
            usleep(50_000);
            fwrite($c, Fake::pong($ping[0]->requestId));
            while (($fr = Fake::readFrame($c)) !== null) {
                if (Fake::isExec($fr[0])) {
                    usleep(300_000);
                    fwrite($c, Fake::ok($fr[0]->requestId, 2));
                } elseif (Fake::isPing($fr[0])) {
                    fwrite($c, Fake::pong($fr[0]->requestId));
                }
            }
        });
        $conn = self::connect($f, ioTimeout: 0.5);
        $r = RevoltTasks::run([
            'a' => static function () use ($conn): mixed {
                $conn->scalarAsync('R1')->await();
                return $conn->scalarAsync('R2')->await();
            },
        ]);
        $this->assertSame(['a' => 2], $r);
    }

    /**
     * An exception thrown out of one Revolt callback stops `EventLoop::run()` — Revolt's rule, not
     * Ferro's — but strands nothing: the other Fiber's request is still in flight and its terminal
     * still reaches it when the loop runs again.
     */
    public function testAnUncaughtExceptionInOneFiberStrandsNoOtherRequest(): void
    {
        $f = $this->fake(self::answersAfter(0.3, 3.0));
        $conn = self::connect($f);
        $got = null;
        EventLoop::queue(static function () use ($conn, &$got): void {
            $got = $conn->scalarAsync('x')->await();
        });
        EventLoop::queue(static function (): void {
            RevoltTasks::delay(0.05);
            throw new \DomainException('task b failed');
        });
        try {
            EventLoop::run();
            $this->fail('the uncaught exception should stop the loop');
        } catch (EventLoop\UncaughtThrowable $e) {
            $this->assertInstanceOf(\DomainException::class, $e->getPrevious());
        }
        $this->assertNull($got, 'the query had not finished when the loop stopped');
        EventLoop::run();
        $this->assertSame(7, $got);
    }

    /**
     * A suspended Fiber's terminal can be read by SOMEONE ELSE: here `{main}`'s synchronous call on
     * the same session reads it while reading its own. Nothing is left on the socket to wake the
     * watcher, so the Fiber must be woken by the session's router (`Session::observe`) — not left
     * until the next liveness tick (5 s).
     */
    public function testATerminalReadByAnotherCallerWakesItsFiber(): void
    {
        $f = $this->fake(static function ($c): void {
            $a = Fake::readFrame($c); // the Fiber's request
            $b = Fake::readFrame($c); // {main}'s synchronous one
            if ($a === null || $b === null) {
                return;
            }
            usleep(100_000);
            fwrite($c, Fake::ok($a[0]->requestId, 1) . Fake::ok($b[0]->requestId, 2)); // one write
            while (Fake::readFrame($c) !== null) {
            }
        });
        $conn = self::connect($f);
        $start = microtime(true);
        $doneAt = null;
        $syncResult = null;
        EventLoop::queue(static function () use ($conn, $start, &$doneAt): void {
            $conn->scalarAsync('A')->await();
            $doneAt = microtime(true) - $start;
        });
        EventLoop::queue(static function () use ($conn, &$syncResult): void {
            $syncResult = $conn->scalar('B'); // blocks the loop; files A's terminal on the way
        });
        EventLoop::run();
        $this->assertSame(2, $syncResult);
        $this->assertNotNull($doneAt);
        $this->assertLessThan(1.0, $doneAt, sprintf('the Fiber woke at %.2f s', $doneAt));
    }

    /**
     * The session is closed by another Fiber while one waits on it: the waiter fails at its own
     * await as sent-and-lost, and the event loop never selects on the closed socket (a closed
     * resource in `stream_select` is a TypeError that would escape `EventLoop::run()`).
     */
    public function testASessionClosedUnderAWaitingFiberFailsItAndNotTheLoop(): void
    {
        $f = $this->fake(self::serveFor(3.0, static function (Header $h, $c): void {
            if (Fake::isPing($h)) {
                fwrite($c, Fake::pong($h->requestId));
            }
        }));
        $conn = self::connect($f);
        $start = microtime(true);
        $r = RevoltTasks::run([
            'waiter' => static fn (): array => [self::attempt(static fn () => $conn->scalarAsync('never answered')->await()), microtime(true) - $start],
            'closer' => static function () use ($conn): bool {
                RevoltTasks::delay(0.2);
                $conn->session()->close();
                return true;
            },
        ]);
        [$outcome, $at] = $r['waiter'];
        $this->assertInstanceOf(\Ferro\Client\Error\FerroException::class, $outcome);
        $this->assertLessThan(1.0, $at, sprintf('the waiter failed at %.2f s', $at));
    }

    /**
     * A failed session releases EVERY waiter, even one whose own condition does not look at the
     * session's state — the adapter must stop watching a closed socket, and it can only do that
     * once nobody waits on it.
     */
    public function testAFailedSessionReleasesEveryWaiter(): void
    {
        $f = $this->fake(self::serveFor(3.0, static function (): void {}));
        $s = new Session(Transport::connectUnix($f->path, 2.0, 5.0));
        $s->hello();
        $r = RevoltTasks::run([
            'waiter' => static function () use ($s): string {
                $s->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'x');
                Loop::waitFor(new Waiter($s, 0, static fn (): bool => false));
                return 'released';
            },
            'closer' => static function () use ($s): bool {
                RevoltTasks::delay(0.1);
                $s->close();
                return true;
            },
        ]);
        $this->assertSame(['waiter' => 'released', 'closer' => true], $r);
    }

    /**
     * A deadline set on a session that is ALREADY watched (another Fiber waits on it with no
     * deadline) must pull the adapter's timer in. Otherwise the CANCEL waits for the next liveness
     * tick or the next frame.
     */
    public function testADeadlineSetWhileTheSessionIsWatchedIsActedOnTime(): void
    {
        $f = $this->fake(self::serveFor(4.0, static function (Header $h, $c): void {
            if (Fake::isPing($h)) {
                fwrite($c, Fake::pong($h->requestId));
            } elseif (Fake::isCancel($h)) {
                fwrite($c, Fake::cancelled($h->requestId));
            }
        }));
        $s = new Session(Transport::connectUnix($f->path, 2.0, 5.0));
        $s->hello();
        $start = microtime(true);
        $r = RevoltTasks::run([
            'plain' => static function () use ($s): string {
                $rid = $s->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'x'); // no deadline, never answered
                Loop::waitFor(new Waiter($s, $rid));
                return 'released';
            },
            'deadline' => static function () use ($s, $start): float {
                RevoltTasks::delay(0.1); // 'plain' is parked by now: the session is watched
                $rid = $s->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'y');
                $s->setDeadline($rid, microtime(true) + 0.2);
                Loop::waitFor(new Waiter($s, $rid));
                $outcome = $s->awaitTerminal($rid);
                $at = microtime(true) - $start;
                \assert($outcome->isError());
                $s->close(); // releases 'plain'
                return $at;
            },
        ]);
        $this->assertIsFloat($r['deadline'], $r['deadline'] instanceof \Throwable ? $r['deadline']->getMessage() : '');
        $this->assertLessThan(0.8, $r['deadline'], sprintf('the CANCEL was answered at %.2f s', $r['deadline']));
        $this->assertSame('released', $r['plain']);
    }

    /**
     * A failure inside the adapter's own callbacks is delivered to the Fiber waiting on it, never
     * thrown out of `EventLoop::run()`. Forced with a wait condition that throws when the LOOP, not
     * the waiting Fiber, evaluates it — once when a frame is read (the socket watcher) and once when
     * the timer fires on silence — and keyed to no request (id 0), so the router's per-request wake
     * never evaluates it first.
     */
    public function testAFailureInsideTheAdapterReachesTheWaiterNotTheLoop(): void
    {
        foreach (['frame' => self::answersAfter(0.2, 2.0), 'silence' => self::serveFor(2.0, static function (): void {})] as $case => $serve) {
            $f = $this->fake($serve);
            $s = new Session(Transport::connectUnix($f->path, 2.0, 0.3));
            $s->hello();
            $r = RevoltTasks::run([
                'a' => static function () use ($s): string {
                    $s->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'x');
                    $mine = \Fiber::getCurrent();
                    Loop::waitFor(new Waiter($s, 0, static function () use ($mine): bool {
                        if (\Fiber::getCurrent() !== $mine) {
                            throw new \DomainException('the condition broke');
                        }
                        return false;
                    }));
                    return 'not reached';
                },
            ]);
            $this->assertInstanceOf(\DomainException::class, $r['a'], $case);
            $s->close();
        }
    }

    /**
     * `Ferro\Loop` keeps its own Fibers with the adapter installed — here run from INSIDE a Revolt
     * callback, so its Fibers do run under the Revolt loop: they must still suspend into
     * `Ferro\Loop`, which refuses any other suspension.
     */
    public function testFerroLoopStillOwnsItsFibers(): void
    {
        $f = $this->fake(static function ($c): void {
            $a = Fake::readFrame($c);
            $b = Fake::readFrame($c);
            if ($a === null || $b === null) {
                return;
            }
            fwrite($c, Fake::ok($b[0]->requestId, 2));
            fwrite($c, Fake::ok($a[0]->requestId, 1));
            while (Fake::readFrame($c) !== null) {
            }
        });
        $conn = self::connect($f, ioTimeout: 2.0);
        $order = [];
        $tasks = [
            'a' => static function () use ($conn, &$order): mixed {
                $v = $conn->scalarAsync('A')->await();
                $order[] = 'a';
                return $v;
            },
            'b' => static function () use ($conn, &$order): mixed {
                $v = $conn->scalarAsync('B')->await();
                $order[] = 'b';
                return $v;
            },
        ];
        $r = RevoltTasks::run(['loop' => static fn (): array => Loop::run($tasks)])['loop'];
        $this->assertSame(['a' => 1, 'b' => 2], $r);
        $this->assertSame(['b', 'a'], $order, 'suspended by Ferro\\Loop (a blocked Fiber would finish first)');
    }

    /** @param \Closure(): mixed $work */
    private static function attempt(\Closure $work): mixed
    {
        try {
            return $work();
        } catch (\Throwable $e) {
            return $e;
        }
    }

    // ---- M3-D1d review round -------------------------------------------------------------------

    /**
     * Gaps between consecutive ticks of a repeat timer, in seconds.
     *
     * @param list<float> $ticks
     */
    private static function maxGap(array $ticks): float
    {
        $max = 0.0;
        for ($i = 1, $n = count($ticks); $i < $n; ++$i) {
            $max = max($max, $ticks[$i] - $ticks[$i - 1]);
        }
        return $max;
    }

    /** @return list<array{bool}> */
    public static function readPaths(): array
    {
        $paths = ['fread' => [false]];
        if (Transport::canReceiveFds()) {
            $paths['recvmsg'] = [true];
        }
        return $paths;
    }

    /**
     * Review F1 (HIGH), one session: a synchronous call leaves ANOTHER request's terminal unread
     * (in PHP's stream buffer on the `fread` path), and a Fiber then parks on that request. The
     * adapter can be handed the same readable event twice in one tick; the second is STALE — its
     * bytes are gone — and reading on it blocked the whole loop until the session's next frame
     * (2 s here; up to a full read timeout). A 50 ms timer must keep ticking throughout.
     */
    #[DataProvider('readPaths')]
    public function testAStaleReadableEventNeverBlocksTheLoop(bool $receiveFds): void
    {
        $f = $this->fake(static function ($c): void {
            $r1 = Fake::readFrame($c);
            $r2 = Fake::readFrame($c);
            $s = Fake::readFrame($c);
            if ($r1 === null || $r2 === null || $s === null) {
                return;
            }
            fwrite($c, Fake::ok($s[0]->requestId, 3) . Fake::ok($r2[0]->requestId, 2)); // one write: S, then R2
            usleep(2_000_000);
            fwrite($c, Fake::ok($r1[0]->requestId, 1));
            while (Fake::readFrame($c) !== null) {
            }
        });
        $a = Ferro::connect($f->path, ioTimeout: 10.0, policy: RetryPolicy::none(), receiveFds: $receiveFds);
        $ticks = [];
        $rep = EventLoop::repeat(0.05, static function () use (&$ticks): void {
            $ticks[] = microtime(true);
        });
        $r = RevoltTasks::run([
            'W1' => static function () use ($a, $rep, &$ticks): mixed {
                try {
                    return $a->scalarAsync('R1')->await();
                } finally {
                    $ticks[] = microtime(true); // so a freeze that lasts to the end is a gap too
                    EventLoop::cancel($rep);
                }
            },
            'X' => static function () use ($a): array {
                $fut = $a->scalarAsync('R2');
                RevoltTasks::delay(0.1);
                $v = $a->scalar('S'); // reads S's terminal, and R2's arrives with it
                return [$v, $fut->await()];
            },
        ]);
        $this->assertSame(['W1' => 1, 'X' => [3, 2]], $r);
        $this->assertGreaterThan(30, count($ticks), sprintf('only %d ticks in ~2 s', count($ticks)));
        $this->assertLessThan(0.5, self::maxGap($ticks), sprintf('the loop froze for %.2f s', self::maxGap($ticks)));
    }

    /**
     * Review F1, two sessions: session B's readable callback resumes a Fiber whose SYNCHRONOUS call
     * on session A reads A's pending terminal — in the same tick in which A's readable callback was
     * already queued. That callback is stale and must not read.
     */
    #[DataProvider('readPaths')]
    public function testAStaleReadableEventAfterAnotherFibersSyncCallNeverBlocksTheLoop(bool $receiveFds): void
    {
        $fa = $this->fake(static function ($c): void {
            $f1 = Fake::readFrame($c);
            $f2 = Fake::readFrame($c);
            if ($f1 === null || $f2 === null) {
                return;
            }
            usleep(300_000);
            fwrite($c, Fake::ok($f1[0]->requestId, 1));
            while (($fr = Fake::readFrame($c)) !== null) {
                if (Fake::isExec($fr[0])) {
                    fwrite($c, Fake::ok($fr[0]->requestId, 3)); // the sync query: at once
                    break;
                }
            }
            usleep(2_000_000);
            fwrite($c, Fake::ok($f2[0]->requestId, 2));
            while (Fake::readFrame($c) !== null) {
            }
        });
        $fb = $this->fake(static function ($c): void {
            $g = Fake::readFrame($c);
            if ($g === null) {
                return;
            }
            usleep(300_000);
            fwrite($c, Fake::ok($g[0]->requestId, 9));
            while (Fake::readFrame($c) !== null) {
            }
        });
        $a = Ferro::connect($fa->path, ioTimeout: 10.0, policy: RetryPolicy::none(), receiveFds: $receiveFds);
        $b = Ferro::connect($fb->path, ioTimeout: 10.0, policy: RetryPolicy::none(), receiveFds: $receiveFds);
        $ticks = [];
        $rep = EventLoop::repeat(0.05, static function () use (&$ticks): void {
            $ticks[] = microtime(true);
        });
        EventLoop::delay(0.2, static function (): void {
            usleep(200_000); // both sockets become readable before the next select
        });
        $r = RevoltTasks::run([
            'G' => static function () use ($a, $b): mixed {
                $v = $b->scalarAsync('G')->await();
                return [$v, $a->scalar('sync')]; // consumes F1's terminal on A
            },
            'F1' => static fn (): mixed => $a->scalarAsync('F1')->await(),
            'F2' => static function () use ($a, $rep, &$ticks): mixed {
                try {
                    return $a->scalarAsync('F2')->await();
                } finally {
                    $ticks[] = microtime(true); // so a freeze that lasts to the end is a gap too
                    EventLoop::cancel($rep);
                }
            },
        ]);
        $this->assertSame(['G' => [9, 3], 'F1' => 1, 'F2' => 2], $r);
        $this->assertLessThan(0.5, self::maxGap($ticks), sprintf('the loop froze for %.2f s', self::maxGap($ticks)));
    }

    /**
     * Review F2: the liveness timer must look for an answer already on the socket before judging
     * the session silent. Two overdue user timers make the adapter's timer run in the SAME dispatch
     * as them, after the answer (and the PONG) arrived but before any read: judging first closed a
     * HEALTHY session, and a write would have been reported `Indeterminate`.
     */
    #[DataProvider('readPaths')]
    public function testTheLivenessTimerReadsAWaitingAnswerBeforeJudging(bool $receiveFds): void
    {
        $f = $this->fake(static function ($c): void {
            $r1 = Fake::readFrame($c);
            $ping = Fake::readFrame($c); // ~0.5 s
            if ($r1 === null || $ping === null) {
                return;
            }
            usleep(540_000);
            fwrite($c, Fake::ok($r1[0]->requestId, 42) . Fake::pong($ping[0]->requestId));
            while (Fake::readFrame($c) !== null) {
            }
        });
        $conn = Ferro::connect($f->path, ioTimeout: 0.5, policy: RetryPolicy::none(), receiveFds: $receiveFds);
        $t0 = microtime(true);
        EventLoop::delay(0.90, static function () use ($t0): void {
            while (microtime(true) - $t0 < 1.02) {
            }
        });
        EventLoop::delay(0.95, static function () use ($t0): void {
            while (microtime(true) - $t0 < 1.08) {
            }
        });
        $r = RevoltTasks::run(['a' => static fn (): mixed => self::attempt(static fn () => $conn->scalarAsync('R1')->await())]);
        $this->assertSame(['a' => 42], $r, $r['a'] instanceof \Throwable ? $r['a']->getMessage() : '');
    }

    /**
     * Review F3: a socket that stays readable with nothing to read for (here: EOF, after the stream
     * a Fiber is waiting on has had its END filed) must not spin the loop. The waiting Fiber is
     * released when the stream's Fiber closes it, 1.5 s later.
     */
    public function testAReadableSocketWithNothingToReadForDoesNotSpin(): void
    {
        $f = $this->fake(static function ($c): void {
            $p = PackerFactory::forEncode();
            $s = Fake::readFrame($c);
            if ($s === null) {
                return;
            }
            $rid = $s[0]->requestId;
            fwrite($c, Fake::frame(0, C::SERVICE_STREAM, C::METHOD_STREAM_HEAD, $rid, StreamHead::encode(['cols' => [['name' => 'n', 'tag' => C::TAG_I64]]], $p)));
            fwrite($c, Fake::frame(C::FLAG_STREAM, C::SERVICE_STREAM, C::METHOD_STREAM_DATA, $rid, StreamData::encode(['rows' => [[['tag' => C::TAG_I64, 'data' => 7]]]], $p)));
            usleep(100_000);
            fwrite($c, Fake::ok($rid, 0)); // the stream's END
            usleep(100_000);
            fclose($c); // EOF, with nothing in flight
        });
        $conn = self::connect($f);
        $cpu = static function (): float {
            $u = getrusage();
            return $u['ru_utime.tv_sec'] + $u['ru_utime.tv_usec'] / 1e6 + $u['ru_stime.tv_sec'] + $u['ru_stime.tv_usec'] / 1e6;
        };
        $before = $cpu();
        $r = RevoltTasks::run([
            'S' => static function () use ($conn): int {
                $n = 0;
                foreach ($conn->streamRaw('S', [], true)->rows() as $_) {
                    ++$n;
                    RevoltTasks::delay(1.5);
                }
                return $n;
            },
            'W' => static function () use ($conn): mixed {
                RevoltTasks::delay(0.05);
                return self::attempt(static fn () => $conn->scalarAsync('W')->await());
            },
        ]);
        $spent = $cpu() - $before;
        $this->assertLessThan(0.5, $spent, sprintf('%.2f s of CPU in a ~1.5 s wait: the loop spun', $spent));
        // The engine went away: both Fibers fail (the stream on its next WINDOW_UPDATE), neither hangs.
        $this->assertInstanceOf(\Ferro\Client\Error\FerroException::class, $r['W']);
    }

    /**
     * Review F4: Revolt's own debug driver (`REVOLT_DRIVER_DEBUG_TRACE=1` wraps the driver in a
     * `TracingDriver`) must not weaken the under-the-loop test to `isRunning()`, which stays true
     * once `{main}` has awaited: a Fiber `{main}` then starts must block, not be suspended into a
     * loop nobody runs.
     */
    public function testUnderTheTracingDriverAFiberMainStartsStillBlocks(): void
    {
        self::withDriver(new TracingDriver(new StreamSelectDriver()), function (): void {
            $f = $this->fake(self::answersAfter(0.05, 2.0));
            $conn = self::connect($f);
            $this->assertSame(7, $conn->scalarAsync('main')->await()); // the loop Fiber is now suspended
            $fiber = new \Fiber(static fn (): mixed => $conn->scalarAsync('fiber')->await());
            $fiber->start();
            $this->assertTrue($fiber->isTerminated(), 'the Fiber was suspended into a loop nobody runs');
            $this->assertSame(7, $fiber->getReturn());
        });
    }

    /**
     * Review MB: when a Fiber is refused for being resumed by another scheduler, nothing of the
     * adapter is left behind — no watch, no Revolt callback — at that moment, not at the next tick.
     */
    public function testARefusedForeignResumeLeavesNoWatchBehind(): void
    {
        $f = $this->fake(self::serveFor(3.0, static function (Header $h, $c): void {
            if (Fake::isPing($h)) {
                fwrite($c, Fake::pong($h->requestId));
            }
        }));
        $conn = self::connect($f, ioTimeout: 2.0);
        $after = null;
        EventLoop::queue(static function () use ($conn, &$after): void {
            $foreign = new \Fiber(static fn (): mixed => $conn->scalarAsync('x')->await());
            $foreign->start();
            try {
                $foreign->resume('not the loop');
            } catch (\LogicException) {
            }
            $after = [RevoltWatch::active(), EventLoop::getIdentifiers()];
        });
        $start = microtime(true);
        EventLoop::run();
        $this->assertSame([0, []], $after);
        $this->assertLessThan(0.5, microtime(true) - $start, 'the loop returned at once');
    }

    /** Review MS: the adapter cannot be uninstalled from under a suspended Fiber. */
    public function testUninstallIsRefusedWhileAFiberIsSuspended(): void
    {
        $f = $this->fake(self::answersAfter(0.3, 2.0));
        $conn = self::connect($f);
        $refused = null;
        $r = RevoltTasks::run([
            'a' => static fn (): mixed => $conn->scalarAsync('x')->await(),
            'b' => static function () use (&$refused): bool {
                RevoltTasks::delay(0.1);
                try {
                    Revolt::uninstall();
                    $refused = false;
                } catch (\LogicException) {
                    $refused = true;
                }
                return true;
            },
        ]);
        $this->assertSame(['a' => 7, 'b' => true], $r);
        $this->assertTrue($refused);
        $this->assertTrue(Revolt::isInstalled());
    }

    /**
     * Review MK: frames read off a busy session are PROGRESS. The liveness clock must count them,
     * or a session answering a stream of quick statements is PINGed every read timeout while one
     * slow statement is in flight beside them.
     */
    public function testABusySessionIsNotPinged(): void
    {
        $pings = sys_get_temp_dir() . '/ferro-d1d-pings-' . getmypid();
        @unlink($pings);
        $f = $this->fake(static function ($c) use ($pings): void {
            $slow = null;
            $start = microtime(true);
            while (($fr = Fake::readFrame($c)) !== null) {
                if (Fake::isPing($fr[0])) {
                    file_put_contents($pings, 'P', FILE_APPEND);
                    fwrite($c, Fake::pong($fr[0]->requestId));
                } elseif (Fake::isExec($fr[0]) && $slow === null) {
                    $slow = $fr[0]->requestId; // answered once the quick ones are done
                } elseif (Fake::isExec($fr[0])) {
                    usleep(30_000);
                    fwrite($c, Fake::ok($fr[0]->requestId, 2));
                    if (microtime(true) - $start > 1.2) {
                        fwrite($c, Fake::ok($slow, 1));
                    }
                }
            }
        });
        $conn = self::connect($f, ioTimeout: 0.3);
        $done = false;
        $r = RevoltTasks::run([
            'slow' => static function () use ($conn, &$done): mixed {
                try {
                    return $conn->scalarAsync('slow')->await();
                } finally {
                    $done = true;
                }
            },
            'busy' => static function () use ($conn, &$done): int {
                $n = 0;
                while (!$done && $n < 200) {
                    $conn->scalarAsync('quick')->await();
                    ++$n;
                }
                return $n;
            },
        ]);
        $this->assertSame(1, $r['slow']);
        $this->assertSame('', (string) @file_get_contents($pings), 'a session answering every 30 ms was PINGed');
        @unlink($pings);
    }

    /**
     * Review MN: a frame the watcher reads re-evaluates EVERY waiter on that session, not only the
     * one whose request it answers — a waiter keyed to no request (id 0) here, whose condition the
     * frame makes true. Otherwise it waits for the next liveness tick (5 s).
     */
    public function testAFrameReadByTheWatcherReEvaluatesEveryWaiter(): void
    {
        $f = $this->fake(self::answersAfter(0.2, 3.0));
        $s = new Session(Transport::connectUnix($f->path, 2.0, 5.0));
        $s->hello();
        $start = microtime(true);
        $r = RevoltTasks::run([
            'a' => static function () use ($s, $start): float {
                $rid = $s->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'x');
                Loop::waitFor(new Waiter($s, 0, static fn (): bool => $s->isReady($rid)));
                $s->awaitTerminal($rid);
                return microtime(true) - $start;
            },
        ]);
        $s->close();
        $this->assertIsFloat($r['a']);
        $this->assertLessThan(1.0, $r['a'], sprintf('woken at %.2f s', $r['a']));
    }

    /**
     * Run `$body` with `$driver` as the Revolt driver, then put the original back. A loop Fiber an
     * earlier `{main}` await left suspended is run to its end first: Revolt refuses to swap a
     * driver whose loop is "running".
     *
     * @param \Closure(): void $body
     */
    private static function withDriver(Driver $driver, \Closure $body): void
    {
        EventLoop::run();
        $original = EventLoop::getDriver();
        EventLoop::setDriver($driver);
        try {
            $body();
        } finally {
            EventLoop::run();
            EventLoop::setDriver($original);
        }
    }

    /** An engine that reads two requests and answers them in REVERSE order (1 for the first). */
    private static function answersTwoReversed(): \Closure
    {
        return static function ($c): void {
            $a = Fake::readFrame($c);
            $b = Fake::readFrame($c);
            if ($a === null || $b === null) {
                return;
            }
            fwrite($c, Fake::ok($b[0]->requestId, 2));
            usleep(50_000);
            fwrite($c, Fake::ok($a[0]->requestId, 1));
            while (($fr = Fake::readFrame($c)) !== null) {
                if (Fake::isExec($fr[0])) {
                    fwrite($c, Fake::ok($fr[0]->requestId, 7));
                } elseif (Fake::isPing($fr[0])) {
                    fwrite($c, Fake::pong($fr[0]->requestId));
                }
            }
        };
    }

    /**
     * Review F4, the other half: under the `TracingDriver`, Fibers the loop drives must still
     * SUSPEND — the adapter looks through the wrapper rather than giving up on it. Two tasks whose
     * answers come back in reverse order finish in reverse order only if the first one's await
     * suspended.
     */
    public function testUnderTheTracingDriverLoopDrivenFibersStillSuspend(): void
    {
        self::withDriver(new TracingDriver(new StreamSelectDriver()), function (): void {
            $f = $this->fake(self::answersTwoReversed());
            $conn = self::connect($f, ioTimeout: 2.0);
            $order = [];
            $r = RevoltTasks::run([
                'a' => static function () use ($conn, &$order): mixed {
                    $v = $conn->scalarAsync('A')->await();
                    $order[] = 'a';
                    return $v;
                },
                'b' => static function () use ($conn, &$order): mixed {
                    $v = $conn->scalarAsync('B')->await();
                    $order[] = 'b';
                    return $v;
                },
            ]);
            $this->assertSame(['a' => 1, 'b' => 2], $r);
            $this->assertSame(['b', 'a'], $order);
        });
    }

    /**
     * Review F4: a driver the adapter cannot see into answers "not under the loop", so a Fiber
     * `{main}` starts after a `{main}` await blocks. `isRunning()`, the old fallback, said yes and
     * stranded it.
     */
    public function testUnderADriverItCannotSeeIntoAFiberBlocks(): void
    {
        self::withDriver(new OpaqueDriver(new StreamSelectDriver()), function (): void {
            $f = $this->fake(self::answersAfter(0.05, 2.0));
            $conn = self::connect($f);
            $this->assertSame(7, $conn->scalarAsync('main')->await()); // {main} suspends: the loop runs
            $fiber = new \Fiber(static fn (): mixed => $conn->scalarAsync('fiber')->await());
            $fiber->start();
            $this->assertTrue($fiber->isTerminated(), 'the Fiber was suspended into a loop nobody runs');
            $this->assertSame(7, $fiber->getReturn());
        });
    }

    /**
     * Review F3, the release half: once a read has been parked for having nothing to read for, a
     * Fiber that parks with a NEW request in flight must unpark it — here the readable bytes are the
     * engine's EOF, and the request must fail at once rather than at the next liveness tick (5 s).
     * Another waiter keeps the same watch alive throughout.
     */
    public function testANewRequestUnparksAParkedRead(): void
    {
        $f = $this->fake(static function ($c): void {
            $a = Fake::readFrame($c);
            if ($a === null) {
                return;
            }
            fwrite($c, Fake::ok($a[0]->requestId, 1));
            stream_socket_shutdown($c, STREAM_SHUT_WR); // EOF to the client; its writes still land
            usleep(3_000_000);
        });
        $s = new Session(Transport::connectUnix($f->path, 2.0, 5.0, receiveFds: false));
        $s->hello();
        $hold = false;
        $start = microtime(true);
        $r = RevoltTasks::run([
            'hold' => static function () use ($s, &$hold): string {
                Loop::waitFor(new Waiter($s, 0, static function () use (&$hold): bool {
                    return $hold;
                }));
                return 'released';
            },
            'w' => static function () use ($s, $start, &$hold): array {
                RevoltTasks::delay(0.05);
                $a = $s->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a');
                Loop::waitFor(new Waiter($s, $a));
                $s->awaitTerminal($a);
                RevoltTasks::delay(0.2); // the EOF is readable now, with nothing in flight: parked
                $b = $s->submit(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'b');
                Loop::waitFor(new Waiter($s, $b));
                $outcome = self::attempt(static fn () => $s->awaitTerminal($b));
                $hold = true;
                return [$outcome, microtime(true) - $start];
            },
        ]);
        [$outcome, $at] = $r['w'];
        $this->assertInstanceOf(\Ferro\Client\Error\FerroException::class, $outcome, 'the engine went away');
        $this->assertLessThan(1.0, $at, sprintf('the lost request failed at %.2f s', $at));
        $this->assertSame('released', $r['hold']);
    }

    /**
     * Under a driver that sees only the kernel's readiness (as ev, uv and event do): a synchronous
     * call that pulls another request's terminal into PHP's stream buffer before anyone waits for
     * it. The await that follows must read it from the buffer at once — the descriptor will never
     * report it — not at the next liveness tick (5 s).
     */
    public function testUnderAnFdDriverAnAwaitReadsWhatASyncCallBuffered(): void
    {
        self::withDriver(new FdOnlyDriver(new StreamSelectDriver()), function (): void {
            $f = $this->fake(static function ($c): void {
                $r2 = Fake::readFrame($c);
                $s = Fake::readFrame($c);
                if ($r2 === null || $s === null) {
                    return;
                }
                fwrite($c, Fake::ok($s[0]->requestId, 3) . Fake::ok($r2[0]->requestId, 2)); // one write
                while (Fake::readFrame($c) !== null) {
                }
            });
            $conn = Ferro::connect($f->path, ioTimeout: 5.0, policy: RetryPolicy::none(), receiveFds: false);
            $fut = $conn->scalarAsync('R2');
            $this->assertSame(3, $conn->scalar('S'));
            $start = microtime(true);
            $this->assertSame(2, $fut->await());
            $this->assertLessThan(0.5, microtime(true) - $start, 'read from the buffer at once');
        });
    }

    /**
     * The same under an fd driver, but the synchronous call runs in a Revolt callback while `{main}`
     * is already suspended waiting: the frame it buffers belongs to `{main}`, and the session's
     * router telling the adapter about the call's own frame is what gets it read.
     */
    public function testUnderAnFdDriverAWaiterIsWokenForWhatAnotherCallBuffered(): void
    {
        self::withDriver(new FdOnlyDriver(new StreamSelectDriver()), function (): void {
            $f = $this->fake(static function ($c): void {
                $r2 = Fake::readFrame($c);
                $s = Fake::readFrame($c);
                if ($r2 === null || $s === null) {
                    return;
                }
                fwrite($c, Fake::ok($s[0]->requestId, 3) . Fake::ok($r2[0]->requestId, 2)); // one write
                while (Fake::readFrame($c) !== null) {
                }
            });
            $conn = Ferro::connect($f->path, ioTimeout: 5.0, policy: RetryPolicy::none(), receiveFds: false);
            $sync = null;
            EventLoop::delay(0.1, static function () use ($conn, &$sync): void {
                $sync = $conn->scalar('S');
            });
            $start = microtime(true);
            $this->assertSame(2, $conn->scalarAsync('R2')->await());
            $this->assertLessThan(0.6, microtime(true) - $start, 'woken for the buffered frame at once');
            $this->assertSame(3, $sync);
        });
    }

    /**
     * Under an fd driver, one readable event reads at most 64 frames, so the loop is not held by a
     * burst. The rest are by then in PHP's stream buffer, which the descriptor will not report:
     * they must be read on the next tick, not at the next liveness tick (5 s).
     */
    public function testUnderAnFdDriverABurstLargerThanOneEventIsReadInFull(): void
    {
        self::withDriver(new FdOnlyDriver(new StreamSelectDriver()), function (): void {
            $n = 100;
            $f = $this->fake(static function ($c) use ($n): void {
                $out = '';
                for ($i = 0; $i < $n; ++$i) {
                    $fr = Fake::readFrame($c);
                    if ($fr === null) {
                        return;
                    }
                    $out .= Fake::ok($fr[0]->requestId, $i);
                }
                fwrite($c, $out); // one write: every terminal at once
                while (Fake::readFrame($c) !== null) {
                }
            });
            $conn = Ferro::connect($f->path, ioTimeout: 5.0, policy: RetryPolicy::none(), receiveFds: false);
            $futures = [];
            for ($i = 0; $i < $n; ++$i) {
                $futures[] = $conn->scalarAsync("q{$i}");
            }
            $start = microtime(true);
            $this->assertSame($n - 1, $futures[$n - 1]->await()); // the LAST one, awaited first
            $this->assertLessThan(1.0, microtime(true) - $start, 'the burst was read in full at once');
            $this->assertSame(range(0, $n - 1), \Ferro\await($futures));
        });
    }
}
