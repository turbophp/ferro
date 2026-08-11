<?php // /php/doctrine-dbal/tests/Live/StreamInTransactionLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Connection as DbalConnection;
use PHPUnit\Framework\Attributes\DataProvider;

/**
 * **An ordinary read inside a transaction must never destroy that transaction.**
 *
 * The whole-branch review (`review/wb-xslice.md`, BLOCKER) measured the opposite: on PostgreSQL the
 * driver streams every parameterless read and declares `readonly = false` for every statement
 * (§22.2 (ac)), so when the temporary `Doctrine\DBAL\Result` was destroyed at the end of the
 * statement the driver `Result::__destruct` sent an out-of-band `CANCEL`. `ferrod`'s S5 abort path
 * (`run_streamed_exec`, `StreamStep::Cancelled`) fires that as a REAL PostgreSQL `CancelRequest`
 * against the backend, which aborts the running statement with `57014` — and a statement error
 * inside a `BEGIN` block puts the whole transaction into PG's ABORTED state. The engine therefore
 * rolled the transaction back and tombstoned the `tx_id` (§19.3 `TxDeadline{Retryable}`), and the
 * caller's committed-looking work was GONE. Three normal lines — begin, insert, `fetchAssociative()`
 * — lost a write.
 *
 * That backend cancel is why the fix is on THIS side of the wire: once the `CancelRequest` has
 * landed there is no fate left for the engine to reclassify. Inside an open transaction the driver
 * now DRAINS the remainder to the stream's one terminal instead of cancelling it
 * ({@see \Ferro\DBAL\Result::free}), so no cancel ever reaches the backend and the transaction is
 * untouched. Autocommit still cancels — `StreamingLiveTest` owns that half.
 *
 * **The threshold is the reason a functional-looking test here is nearly useless on its own.**
 * Measured, PG 17: a `LIMIT` of 1..1024 SURVIVED the bug (one `StreamBatch::DEFAULT` frame is
 * 1024 rows / 256 KiB, so the producer had already finished and the cancel landed on an idle
 * backend), and 2000+ was KILLED. So every assertion below runs at four row counts spanning that
 * boundary, and the small ones additionally assert the MECHANISM — `abandonDrainedRowCount()` — so
 * they cannot pass for the wrong reason.
 */
final class StreamInTransactionLiveTest extends DbalLiveTestCase
{
    /** The row the transaction writes before the read that used to kill it. */
    private const MARKER = 999_999;

    /** The second write, issued AFTER the abandonment to prove the transaction is still usable. */
    private const MARKER2 = 999_998;

    private const ROWS = 50_000;

    private function seed(DbalConnection $c): void
    {
        $c->executeStatement('DROP TABLE IF EXISTS s8b_txstream');
        $c->executeStatement('CREATE TABLE s8b_txstream (id int primary key, note text)');
        $c->executeStatement(
            'INSERT INTO s8b_txstream SELECT g, \'seed\' FROM generate_series(1, ' . self::ROWS . ') g',
        );
    }

    /**
     * The four row counts that span the measured threshold: 1024 is exactly one
     * `StreamBatch::DEFAULT` frame and SURVIVED the bug, 2000 is the first count the review measured
     * as fatal, 50 000 is a plain real table.
     *
     * @return iterable<string, array{int}>
     */
    public static function limits(): iterable
    {
        yield 'one row' => [1];
        yield 'exactly one DATA frame' => [1024];
        yield 'just past one DATA frame' => [2000];
        yield 'a real table' => [self::ROWS];
    }

    /**
     * **SHAPE [B] — the review's headline: three normal lines, no early-exit idiom anywhere.**
     * `Doctrine\DBAL\Connection::fetchAssociative()` is `executeQuery(…)->fetchAssociative()`, so the
     * `Doctrine\DBAL\Result` is a TEMPORARY: it dies at the end of that statement, taking the driver
     * `Result` with it, and that destructor is where the `CANCEL` came from. `fetchOne()` and
     * `fetchFirstColumn()` inherit the same shape through `FetchUtils`.
     */
    #[DataProvider('limits')]
    public function testAPlainFetchInsideATransactionLeavesItIntact(int $limit): void
    {
        $c = $this->dbal();
        $this->seed($c);

        $c->beginTransaction();
        $c->executeStatement('INSERT INTO s8b_txstream VALUES (' . self::MARKER . ', \'inside-tx\')');

        $row = $c->fetchAssociative('SELECT id, note FROM s8b_txstream ORDER BY id LIMIT ' . $limit);
        self::assertIsArray($row);
        self::assertSame(1, $row['id'], 'the streamed read rides the pinned tx_id and returns real rows');

        // (3) the transaction must be GENUINELY usable afterwards — another read AND another write.
        $seen = (int) $c->fetchOne('SELECT count(*) FROM s8b_txstream WHERE id = ' . self::MARKER);
        self::assertSame(1, $seen, 'the post-abandonment read still sees this transaction\'s own uncommitted write');
        $c->executeStatement('INSERT INTO s8b_txstream VALUES (' . self::MARKER2 . ', \'after-abandon\')');

        $c->commit();

        self::assertSame(
            2,
            (int) $c->fetchOne(
                'SELECT count(*) FROM s8b_txstream WHERE id IN (' . self::MARKER . ',' . self::MARKER2 . ')',
            ),
            'BOTH writes must have survived the commit — the review measured the first one silently lost',
        );

        // THE MECHANISM, so the sub-threshold rows cannot pass for the wrong reason (at LIMIT 1 the
        // producer had already finished and the old CANCEL was a harmless no-op — green, and green
        // for a reason that evaporates at 2000 rows). The drain is `LIMIT - 1`: one row was fetched.
        self::assertSame(
            $limit - 1,
            $this->driverConnection($c)->abandonDrainedRowCount(),
            'inside a transaction an abandoned stream must DRAIN to its terminal, never CANCEL',
        );
        self::assertSame(
            0,
            $this->driverConnection($c)->settledRowCount(),
            'and it must drain at abandonment, not leave the remainder for the next statement',
        );

        $c->executeStatement('DROP TABLE s8b_txstream');
    }

    /**
     * **SHAPE [A] — the explicit `break` out of `iterateAssociative()`**, the idiom the review drove
     * first. The generator is a TEMPORARY, so `break` destroys the driver `Result` by refcount there
     * and then ({@see \Ferro\DBAL\Connection::$openStream}) — which is exactly what made the old
     * `CANCEL` fire mid-producer.
     */
    #[DataProvider('limits')]
    public function testBreakingOutOfAnIterationInsideATransactionLeavesItIntact(int $limit): void
    {
        $c = $this->dbal();
        $this->seed($c);

        $c->beginTransaction();
        $c->executeStatement('INSERT INTO s8b_txstream VALUES (' . self::MARKER . ', \'inside-tx\')');

        $seen = 0;
        foreach ($c->iterateAssociative('SELECT id, note FROM s8b_txstream ORDER BY id LIMIT ' . $limit) as $_row) {
            if (++$seen === 1) {
                break;
            }
        }
        self::assertSame(1, $seen);

        $c->executeStatement('INSERT INTO s8b_txstream VALUES (' . self::MARKER2 . ', \'after-abandon\')');
        $c->commit();

        self::assertSame(
            2,
            (int) $c->fetchOne(
                'SELECT count(*) FROM s8b_txstream WHERE id IN (' . self::MARKER . ',' . self::MARKER2 . ')',
            ),
            'the write issued BEFORE the abandoned iteration must still be there after COMMIT',
        );
        self::assertSame(
            $limit - 1,
            $this->driverConnection($c)->abandonDrainedRowCount(),
            'inside a transaction an abandoned iteration must DRAIN to its terminal, never CANCEL',
        );

        $c->executeStatement('DROP TABLE s8b_txstream');
    }

    /**
     * The same abandonment in AUTOCOMMIT still CANCELS — asserted here, next to its in-transaction
     * twin, so the two branches are told apart at one vantage point. `StreamingLiveTest` owns the
     * rest of the autocommit contract (the peak-memory guard and both abandonment shapes); this is
     * the assertion that the fix did not simply turn cancelling off everywhere, which would pass
     * every test in THIS file while quietly reintroducing the OOM trap `settledRowCount()` was
     * built to catch.
     */
    public function testAutocommitAbandonmentStillCancels(): void
    {
        $c = $this->dbal();
        $this->seed($c);

        $seen = 0;
        foreach ($c->iterateAssociative('SELECT id, note FROM s8b_txstream ORDER BY id') as $_row) {
            if (++$seen === 25) {
                break;
            }
        }
        self::assertSame(25, $seen);
        self::assertSame(self::ROWS, (int) $c->fetchOne('SELECT count(*) FROM s8b_txstream'));

        self::assertSame(
            0,
            $this->driverConnection($c)->abandonDrainedRowCount(),
            'outside a transaction there is nothing to protect, so abandonment must still CANCEL',
        );
        self::assertSame(
            0,
            $this->driverConnection($c)->settledRowCount(),
            'and it must not have deferred the remainder to the next statement either',
        );

        $c->executeStatement('DROP TABLE s8b_txstream');
    }

    /**
     * The INTERLEAVE idiom inside a transaction: the remainder is settled by the next statement
     * (`materialize()`), not drained by an abandonment — the two counters must not collapse into
     * each other, or a future change that swaps one path for the other goes unnoticed.
     */
    public function testInterleavingInsideATransactionStillSettlesRatherThanDrains(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_txinter');
        $c->executeStatement('CREATE TABLE s8b_txinter (id int primary key, n int)');
        $c->executeStatement('INSERT INTO s8b_txinter SELECT g, 0 FROM generate_series(1, 2000) g');

        $c->beginTransaction();
        $touched = 0;
        foreach ($c->iterateAssociative('SELECT id FROM s8b_txinter ORDER BY id') as $row) {
            $c->executeStatement('UPDATE s8b_txinter SET n = 1 WHERE id = ?', [$row['id']]);
            ++$touched;
        }
        $c->commit();

        self::assertSame(2000, $touched);
        self::assertSame(2000, (int) $c->fetchOne('SELECT count(*) FROM s8b_txinter WHERE n = 1'));
        self::assertSame(1999, $this->driverConnection($c)->settledRowCount());
        self::assertSame(0, $this->driverConnection($c)->abandonDrainedRowCount());

        $c->executeStatement('DROP TABLE s8b_txinter');
    }

    /** @see StreamingLiveTest::driverConnection — DBAL's `connect()` is protected. */
    private function driverConnection(DbalConnection $c): \Ferro\DBAL\Connection
    {
        $driver = (new \ReflectionMethod($c, 'connect'))->invoke($c);
        self::assertInstanceOf(\Ferro\DBAL\Connection::class, $driver);
        return $driver;
    }
}
