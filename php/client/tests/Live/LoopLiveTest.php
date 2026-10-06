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
}
