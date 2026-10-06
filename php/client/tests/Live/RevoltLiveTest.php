<?php // /php/client/tests/Live/RevoltLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Session;
use Ferro\Client\Transport;
use Ferro\Client\Error\FerroException;
use Ferro\Ferro;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Revolt;
use Ferro\Tests\Support\RevoltTasks;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Large;
use Revolt\EventLoop;

/**
 * M3-D1d (SPEC §10.1, §22.2 (cv)) against a real `ferrod`: with {@see Revolt} installed, an `await`
 * inside a Revolt-driven Fiber (what `Amp\async()` gives you) suspends, the event loop keeps
 * running, and k Fibers' statements are in flight at once on one socket.
 */
#[Large] // a 60 s limit per test, enforced (phpunit.xml.dist): a hang fails loudly
final class RevoltLiveTest extends LiveTestCase
{
    private const SLEEP_S = 0.5;

    protected function setUp(): void
    {
        parent::setUp();
        Revolt::install();
    }

    protected function tearDown(): void
    {
        $left = EventLoop::getIdentifiers();
        try {
            foreach ($left as $id) {
                EventLoop::cancel($id);
            }
            Revolt::uninstall();
        } finally {
            parent::tearDown(); // ferrod is stopped whatever the adapter's state (review F6)
        }
        $this->assertSame([], $left, 'a Revolt callback outlived the test');
    }

    private function connection(?float $statementTimeout = null, string $pool = 'default', ?bool $receiveFds = null): Connection
    {
        return Ferro::connect($this->socketPath, $pool, 2.0, 5.0, statementTimeout: $statementTimeout, receiveFds: $receiveFds);
    }

    /**
     * Both read paths (M3-D3): `fread` on the stream, and `recvmsg` on the imported socket when
     * ext-sockets can receive fds. The adapter watches the same descriptor either way; only the
     * second has no PHP-side buffer.
     *
     * @return array<string, array{bool}>
     */
    public static function readPaths(): array
    {
        $paths = ['fread' => [false]];
        if (Transport::canReceiveFds()) {
            $paths['recvmsg'] = [true];
        }
        return $paths;
    }

    /**
     * Bar 1: four Fibers, each awaiting a 0.5 s sleep that reports its backend pid. Together they
     * take about one sleep, and they ran on four DIFFERENT backends — four statements genuinely in
     * flight at once — against a sequential control in one Fiber that takes about four sleeps.
     */
    #[DataProvider('readPaths')]
    public function testFibersOverlapTheirStatementsOnDistinctBackends(bool $receiveFds): void
    {
        $conn = $this->connection(receiveFds: $receiveFds);
        $this->assertSame($receiveFds, $conn->session()->receivesFds());
        $sql = sprintf('SELECT pg_backend_pid() FROM pg_sleep(%F)', self::SLEEP_S);
        try {
            $tasks = [];
            for ($i = 0; $i < 4; ++$i) {
                $tasks[] = static fn (): mixed => $conn->scalarAsync($sql)->await();
            }
            $start = microtime(true);
            $pids = RevoltTasks::run($tasks);
            $elapsed = microtime(true) - $start;

            foreach ($pids as $pid) {
                $this->assertIsInt($pid);
            }
            $this->assertCount(4, array_unique($pids), 'four statements on four backends: ' . json_encode($pids));
            $this->assertLessThan(self::SLEEP_S * 2.5, $elapsed, sprintf('four sleeps took %.3fs', $elapsed));

            $start = microtime(true);
            RevoltTasks::run([static function () use ($conn, $sql): void {
                for ($i = 0; $i < 4; ++$i) {
                    $conn->scalarAsync($sql)->await();
                }
            }]);
            $sequential = microtime(true) - $start;
            $this->assertGreaterThan(self::SLEEP_S * 3.5, $sequential, sprintf('the sequential control took only %.3fs', $sequential));
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * Bar 2: the event loop is not blocked while a statement is in flight. A 50 ms timer keeps
     * firing during a 1 s `pg_sleep`; a blocking await would let it fire at most once.
     */
    public function testATimerFiresWhileAStatementIsInFlight(): void
    {
        $conn = $this->connection();
        try {
            $ticks = 0;
            $timer = EventLoop::repeat(0.05, static function () use (&$ticks): void {
                ++$ticks;
            });
            $r = RevoltTasks::run([static function () use ($conn, &$ticks, $timer): array {
                try {
                    $v = $conn->scalarAsync('SELECT 1 FROM pg_sleep(1)')->await();
                    return [$v, $ticks];
                } finally {
                    EventLoop::cancel($timer);
                }
            }]);
            [$v, $ticksDuring] = $r[0];
            $this->assertSame(1, $v);
            $this->assertGreaterThanOrEqual(15, $ticksDuring, "the timer fired {$ticksDuring} times during a 1 s statement");
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * Bar 3: exactly one terminal per request, and no frame reaches the wrong Fiber. Twenty Fibers
     * on one socket, each with a distinct value and a jittered sleep so the terminals arrive in an
     * order unrelated to the submissions; every Fiber gets its own value, then a second round on the
     * same session proves nothing was left unread.
     */
    #[DataProvider('readPaths')]
    public function testEveryFiberGetsItsOwnTerminal(bool $receiveFds): void
    {
        $conn = $this->connection(receiveFds: $receiveFds);
        $this->assertSame($receiveFds, $conn->session()->receivesFds());
        try {
            for ($round = 0; $round < 2; ++$round) {
                $tasks = [];
                for ($i = 0; $i < 20; ++$i) {
                    $sleep = (($i * 7) % 10) / 50; // 0 .. 0.18 s, not monotonic in $i
                    $tasks[$i] = static fn (): mixed => $conn->scalarAsync('SELECT ?::int8 FROM pg_sleep(?)', [$i * 1000 + $round, $sleep])->await();
                }
                $r = RevoltTasks::run($tasks);
                $expected = [];
                for ($i = 0; $i < 20; ++$i) {
                    $expected[$i] = $i * 1000 + $round;
                }
                $this->assertSame($expected, $r, "round {$round}");
            }
            $this->assertSame(5, $conn->scalar('SELECT 5::int8'), 'the session is in step afterwards');
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * Bar 4: a statement past its `statementTimeout` under Revolt gets the ENGINE's fate (its
     * `Cancelled` code, NonRetryable for this read) at about the timeout, while another Fiber on the same socket succeeds, and the
     * session stays usable.
     */
    public function testAStatementTimeoutCancelsOnlyItsStatement(): void
    {
        $conn = $this->connection(statementTimeout: 0.3);
        try {
            $start = microtime(true);
            $r = RevoltTasks::run([
                'slow' => static function () use ($conn, $start): array {
                    try {
                        $conn->scalarAsync('SELECT 1 FROM pg_sleep(3)')->await();
                        return ['ok', microtime(true) - $start];
                    } catch (NonRetryableException $e) {
                        return [$e->errorCode(), microtime(true) - $start];
                    }
                },
                'fast' => static fn (): mixed => $conn->scalarAsync('SELECT 2 FROM pg_sleep(0.1)')->await(),
            ]);
            [$outcome, $at] = $r['slow'];
            // The ENGINE's answer (its `Cancelled` code) at the timeout: the client's backstop would
            // end the wait only at 0.3 s + 2 s.
            $this->assertSame(C::ERR_CANCELLED, $outcome);
            $this->assertLessThan(1.0, $at, sprintf('cancelled at %.2f s', $at));
            $this->assertSame(2, $r['fast']);
            $this->assertFalse($conn->session()->isPoisoned());
            $this->assertSame(3, $conn->scalar('SELECT 3'));
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * Bar 5: a Fiber whose statement fails gets its own error; the Fibers beside it on the same
     * socket get their own results.
     */
    public function testAFailingStatementStrandsNoOtherFiber(): void
    {
        $conn = $this->connection();
        try {
            $r = RevoltTasks::run([
                'a' => static fn (): mixed => $conn->scalarAsync('SELECT 1 FROM pg_sleep(0.3)')->await(),
                'bad' => static fn (): mixed => $conn->scalarAsync('SELECT 1/0')->await(),
                'c' => static fn (): mixed => $conn->scalarAsync('SELECT 3 FROM pg_sleep(0.2)')->await(),
            ]);
            $this->assertSame(1, $r['a']);
            $this->assertInstanceOf(FerroException::class, $r['bad']);
            $this->assertSame(3, $r['c']);
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * A Fiber that wants the session while another Fiber's stream is open on it waits for that
     * stream to close — and is woken when it does, though nothing more arrives on the socket for it
     * to read: the stream's own Fiber consumed the END.
     */
    public function testAFiberWaitsForAnotherFibersStreamToClose(): void
    {
        $conn = $this->connection();
        try {
            $events = [];
            $start = microtime(true);
            $r = RevoltTasks::run([
                'stream' => static function () use ($conn, &$events): int {
                    $n = 0;
                    foreach ($conn->stream('SELECT g FROM generate_series(1, 3) g') as $row) {
                        ++$n;
                        $events[] = 'row';
                        RevoltTasks::delay(0.1); // lets the other Fiber run while the stream is open
                    }
                    $events[] = 'stream-closed';
                    return $n;
                },
                'other' => static function () use ($conn, &$events): mixed {
                    RevoltTasks::delay(0.02);
                    $v = $conn->scalarAsync('SELECT 9')->await();
                    $events[] = 'other';
                    return $v;
                },
            ]);
            $this->assertSame(['stream' => 3, 'other' => 9], $r);
            $this->assertSame(['row', 'row', 'row', 'stream-closed', 'other'], $events);
            // Three 0.1 s pauses: the waiter is woken when the stream closes, not at the next
            // liveness tick (the read timeout, 5 s).
            $elapsed = microtime(true) - $start;
            $this->assertLessThan(1.5, $elapsed, sprintf('the waiting Fiber finished at %.2f s', $elapsed));
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * M3-D3 under Revolt: a result large enough to arrive through a sealed memfd, awaited in one
     * Fiber while small statements are awaited in others on the same socket. Each Fiber gets its
     * own result, and the memfd path was really taken.
     */
    #[Group('oob')]
    public function testAMemfdResultUnderRevoltReachesItsOwnFiber(): void
    {
        if (!Transport::canReceiveFds()) {
            $this->markTestSkipped('the memfd path needs Linux and ext-sockets');
        }
        $conn = $this->connection(receiveFds: true);
        $big = 2 * 1024 * 1024;
        try {
            $tasks = ['big' => static fn (): mixed => $conn->scalarAsync("SELECT ?::text || repeat('x', {$big})", ['tag-'])->await()];
            for ($i = 0; $i < 6; ++$i) {
                $tasks["small{$i}"] = static fn (): mixed => $conn->scalarAsync('SELECT ?::int8 FROM pg_sleep(0.05)', [$i])->await();
            }
            $r = RevoltTasks::run($tasks);
            $this->assertIsString($r['big']);
            $this->assertStringStartsWith('tag-xxx', $r['big']);
            $this->assertSame(strlen('tag-') + $big, strlen($r['big']));
            for ($i = 0; $i < 6; ++$i) {
                $this->assertSame($i, $r["small{$i}"]);
            }
            $session = $conn->session();
            $this->assertInstanceOf(Session::class, $session);
            $this->assertSame(1, $session->oobPayloadsReceived(), 'the result came through a memfd');
        } finally {
            $conn->session()->close();
        }
    }

    /** The same fan-out on the MySQL-family pool: the adapter is backend-agnostic. */
    public function testFanOutOnTheMysqlPool(): void
    {
        $conn = $this->connection(pool: $this->requireMysqlPool());
        try {
            $tasks = [];
            for ($i = 0; $i < 4; ++$i) {
                $tasks[] = static fn (): mixed => $conn->scalarAsync(sprintf('SELECT CAST(CONNECTION_ID() AS SIGNED) + SLEEP(%F)', self::SLEEP_S))->await();
            }
            $start = microtime(true);
            $ids = RevoltTasks::run($tasks);
            $elapsed = microtime(true) - $start;
            $this->assertCount(4, array_unique(array_map('strval', $ids)), 'four statements on four connections: ' . json_encode($ids));
            $this->assertLessThan(self::SLEEP_S * 2.5, $elapsed, sprintf('four sleeps took %.3fs', $elapsed));
        } finally {
            $conn->session()->close();
        }
    }
}
