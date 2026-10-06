<?php // /php/client/tests/Live/AsyncLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Future;
use function Ferro\await;

/**
 * M3-D1 (SPEC §10.1) against a real `ferrod`: asynchronous calls over ONE session run concurrently
 * in the engine, so awaiting k of them costs about the slowest one.
 */
final class AsyncLiveTest extends LiveTestCase
{
    private const SLEEP_S = 0.5;
    private const FAN_OUT = 4;

    private function connection(): Connection
    {
        return new Connection($this->connect(), 'default');
    }

    /**
     * The claim, with its CONTROL: the same four sleeps cost about four sleeps when run one after
     * another, and about one when submitted together. Without the control, a fast fan-out would be
     * equally consistent with a sleep that never slept.
     */
    public function testFanOutCostsAboutTheSlowestNotTheSum(): void
    {
        $conn = $this->connection();
        $sql = sprintf('SELECT pg_sleep(%F) IS NULL AS slept, ?::int AS n', self::SLEEP_S);
        try {
            $start = microtime(true);
            for ($i = 0; $i < self::FAN_OUT; ++$i) {
                $conn->query($sql, [$i]);
            }
            $sequential = microtime(true) - $start;
            $this->assertGreaterThanOrEqual(
                self::FAN_OUT * self::SLEEP_S * 0.95,
                $sequential,
                'control: the sleeps really sleep',
            );

            $start = microtime(true);
            $futures = [];
            for ($i = 0; $i < self::FAN_OUT; ++$i) {
                $futures["q{$i}"] = $conn->queryOneAsync($sql, [$i]);
            }
            $results = await($futures);
            $concurrent = microtime(true) - $start;

            $this->assertLessThan(
                self::SLEEP_S * 2.5,
                $concurrent,
                sprintf('fan-out took %.3fs against %.3fs sequential', $concurrent, $sequential),
            );
            // Every value came back to its own key.
            foreach ($results as $key => $row) {
                $this->assertIsArray($row);
                $this->assertSame((int) substr((string) $key, 1), $row['n']);
            }
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * An error terminal is thrown at await, with the type a synchronous call would throw, and the
     * Futures beside it still resolve. The session stays usable afterwards.
     */
    public function testAFailingFutureThrowsAtAwaitAndTheOthersStillResolve(): void
    {
        $conn = $this->connection();
        try {
            $ok = $conn->scalarAsync('SELECT 41 + 1');
            $bad = $conn->scalarAsync('SELECT 1 / 0');
            $alsoOk = $conn->scalarAsync('SELECT 7');

            try {
                $bad->await();
                $this->fail('division by zero must throw at await');
            } catch (NonRetryableException $e) {
                $this->assertSame('22012', $e->sqlstate());
            }
            $this->assertSame(42, $ok->await());
            $this->assertSame(7, $alsoOk->await());

            // `Ferro\await` awaits every Future, then throws the first failure.
            $pair = [$conn->scalarAsync('SELECT 1 / 0'), $after = $conn->scalarAsync('SELECT 3')];
            try {
                await($pair);
                $this->fail('the first failure must be thrown');
            } catch (NonRetryableException) {
            }
            $this->assertTrue($after->isSettled(), 'the Future after the failure was awaited too');
            $this->assertSame(3, $after->await());

            $this->assertSame(5, $conn->scalar('SELECT 5'), 'the session is still usable');
        } finally {
            $conn->session()->close();
        }
    }

    /** A Future settles once: a second await neither reads the wire nor changes the answer. */
    public function testASecondAwaitReturnsTheSameValue(): void
    {
        $conn = $this->connection();
        try {
            $future = $conn->queryAsync('SELECT 1 AS a');
            $first = $future->await();
            $this->assertSame($first, $future->await());
            $this->assertSame(9, $conn->scalar('SELECT 9'), 'nothing was left unread on the wire');
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * Inside an imperative transaction an async statement settles at once: it is part of the
     * transaction's order and sees the transaction's own writes.
     */
    public function testInsideATransactionAnAsyncStatementSettlesAtOnce(): void
    {
        $conn = $this->connection();
        try {
            $conn->exec('CREATE TABLE IF NOT EXISTS ferro_d1_async (v int)');
            $conn->exec('TRUNCATE ferro_d1_async');
            $conn->begin();
            $insert = $conn->execAsync('INSERT INTO ferro_d1_async (v) VALUES (1)');
            $this->assertTrue($insert->isSettled());
            $count = $conn->scalarAsync('SELECT count(*) FROM ferro_d1_async');
            $this->assertSame(1, $count->await(), 'the async read sees the transaction\'s own write');
            $conn->rollBack();
            $this->assertSame(0, $conn->scalar('SELECT count(*) FROM ferro_d1_async'), 'and it rolled back with it');
        } finally {
            $conn->session()->close();
        }
    }

    /** A synchronous call made while Futures are in flight keeps their terminals for them. */
    public function testASynchronousCallBetweenSubmitAndAwait(): void
    {
        $conn = $this->connection();
        try {
            $slow = $conn->scalarAsync(sprintf('SELECT 1 FROM pg_sleep(%F)', self::SLEEP_S));
            $this->assertSame(2, $conn->scalar('SELECT 2'), 'the synchronous call is not blocked behind it');
            $this->assertInstanceOf(Future::class, $slow);
            $this->assertSame(1, $slow->await());
        } finally {
            $conn->session()->close();
        }
    }
}
