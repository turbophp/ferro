<?php // /php/client/tests/Live/LoopLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Loop;

/**
 * M3-D1b (SPEC §10.1) against a real `ferrod`: Fibers run under `Ferro\Loop` share the socket, and
 * an await suspends instead of blocking, so k sleeping queries cost about one sleep.
 */
final class LoopLiveTest extends LiveTestCase
{
    private const SLEEP_S = 0.5;

    private function connection(): Connection
    {
        return new Connection($this->connect(), 'default');
    }

    private static function sleepSql(): string
    {
        return sprintf('SELECT 1 FROM pg_sleep(%F)', self::SLEEP_S);
    }

    /**
     * Four tasks on ONE session, each submitting and awaiting its own sleep: about one sleep in
     * total. Each task runs its query only when its Fiber runs, so this can only be fast if an await
     * suspends and lets the next task submit.
     */
    public function testFibersOnOneSessionOverlapTheirQueries(): void
    {
        $conn = $this->connection();
        try {
            $tasks = [];
            for ($i = 0; $i < 4; ++$i) {
                $tasks[] = static fn (): mixed => $conn->scalarAsync(self::sleepSql())->await();
            }
            $start = microtime(true);
            $results = Loop::run($tasks);
            $elapsed = microtime(true) - $start;

            $this->assertSame([1, 1, 1, 1], $results);
            $this->assertLessThan(self::SLEEP_S * 2.5, $elapsed, sprintf('four sleeps took %.3fs', $elapsed));
        } finally {
            $conn->session()->close();
        }
    }

    /** Two sessions, two tasks each: the loop selects across both sockets. */
    public function testFibersAcrossTwoSessions(): void
    {
        $a = $this->connection();
        $b = $this->connection();
        try {
            $tasks = [];
            foreach ([$a, $b, $a, $b] as $conn) {
                $tasks[] = static fn (): mixed => $conn->scalarAsync(self::sleepSql())->await();
            }
            $start = microtime(true);
            $results = Loop::run($tasks);
            $elapsed = microtime(true) - $start;

            $this->assertSame([1, 1, 1, 1], $results);
            $this->assertLessThan(self::SLEEP_S * 2.5, $elapsed, sprintf('four sleeps over two sessions took %.3fs', $elapsed));
        } finally {
            $a->session()->close();
            $b->session()->close();
        }
    }

    /** A transaction inside one Fiber, plain reads in others: the transaction's statements stay in order. */
    public function testATransactionInOneFiberWhileOthersRead(): void
    {
        $setup = $this->connection();
        $setup->exec('CREATE TABLE IF NOT EXISTS ferro_d1b_loop (v int)');
        $setup->exec('TRUNCATE ferro_d1b_loop');
        $setup->session()->close();

        $conn = $this->connection();
        try {
            $results = Loop::run([
                'tx' => static fn (): mixed => $conn->transaction(static function ($tx): int {
                    $tx->exec('INSERT INTO ferro_d1b_loop (v) VALUES (1)');
                    $tx->exec('INSERT INTO ferro_d1b_loop (v) VALUES (2)');
                    return 2;
                }),
                'read' => static fn (): mixed => $conn->scalarAsync(self::sleepSql())->await(),
            ]);
            $this->assertSame(['tx' => 2, 'read' => 1], $results);
            $this->assertSame(3, $conn->scalar('SELECT sum(v)::int FROM ferro_d1b_loop'));
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * Review F3(d): two sessions with ASYMMETRIC work. The fast session's five short queries must
     * not wait behind the slow session's long one. A loop that always blocked on one session would
     * finish them only after the long query.
     */
    public function testAFastSessionIsNotHeldBehindASlowOne(): void
    {
        $slow = $this->connection();
        $fast = $this->connection();
        try {
            $fastDone = 0.0;
            $start = microtime(true);
            Loop::run([
                'slow' => static fn (): mixed => $slow->scalarAsync('SELECT 1 FROM pg_sleep(2)')->await(),
                'fast' => static function () use ($fast, $start, &$fastDone): int {
                    for ($i = 0; $i < 5; ++$i) {
                        $fast->scalarAsync('SELECT 1 FROM pg_sleep(0.05)')->await();
                    }
                    $fastDone = microtime(true) - $start;
                    return 5;
                },
            ]);
            $this->assertLessThan(1.0, $fastDone, sprintf('the fast session finished at %.3fs, behind the slow one', $fastDone));
        } finally {
            $slow->session()->close();
            $fast->session()->close();
        }
    }

    /**
     * Review F2: a peer that accepts and never answers fails on ITS OWN read timeout even while
     * another session stays busy. One global idle counter used to let the busy session keep it
     * waiting. Since M3-D1c silence is probed first: one read timeout of silence sends a liveness
     * PING, and a second with the PING unanswered closes the session (~2 x 0.5 s here).
     */
    public function testASilentPeerFailsAtItsOwnDeadlineWhileAnotherSessionIsBusy(): void
    {
        $path = sys_get_temp_dir() . '/ferro-silent-' . getmypid() . '.sock';
        @unlink($path);
        $server = stream_socket_server('unix://' . $path, $errno, $errstr);
        $this->assertNotFalse($server, $errstr);
        $busy = $this->connection();
        try {
            $silentTransport = \Ferro\Client\Transport::connectUnix($path, 1.0, 0.5);
            $accepted = stream_socket_accept($server, 1.0);
            $this->assertNotFalse($accepted);
            $silent = new Connection(new \Ferro\Client\Session($silentTransport), 'default');

            $failedAt = null;
            $start = microtime(true);
            Loop::run([
                'busy' => static function () use ($busy): int {
                    for ($i = 0; $i < 12; ++$i) {
                        $busy->scalarAsync('SELECT 1 FROM pg_sleep(0.25)')->await();
                    }
                    return 12;
                },
                'silent' => static function () use ($silent, $start, &$failedAt): string {
                    try {
                        $silent->scalarAsync('SELECT 1')->await();
                        return 'answered';
                    } catch (\Ferro\Client\Error\FerroException) {
                        $failedAt = microtime(true) - $start;
                        return 'failed';
                    }
                },
            ]);
            $this->assertNotNull($failedAt);
            $this->assertLessThan(2.0, $failedAt, sprintf('the silent peer failed at %.3fs (read timeout 0.5s, busy work ~3s)', $failedAt));
        } finally {
            $busy->session()->close();
            fclose($server);
            @unlink($path);
        }
    }
}
