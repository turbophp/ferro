<?php // /php/doctrine-dbal/tests/Live/StreamingLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

/**
 * M1-S8b Task 12, live — §14's "`iterateAssociative()` et al. never buffer", MEASURED rather than
 * asserted structurally.
 *
 * A functional assertion ("the rows come out in order") passes just as well over a fully-buffered
 * result, so the guard here is a MEMORY DELTA. **It has to be PEAK and/or MID-LOOP, never the
 * residual after the loop** (hazard 79): `Doctrine\DBAL\Connection::iterateAssociative()` returns
 * `$this->executeQuery(…)->iterateAssociative()`, so the only reference to the buffered rows is the
 * Generator's bound `$this`, and PHP releases the whole array when the `foreach` ends — BEFORE a
 * post-loop `memory_get_usage()` runs. Measured over dbal 4.4.4's real code paths at this row count
 * and shape, plan v1's post-loop metric was **552 B streamed vs 472 B buffered — both green**, i.e.
 * the headline guard for the whole task could not fail.
 *
 * The two arms are compared in the same process against the same query, so the threshold is a RATIO
 * rather than an absolute number and does not depend on the machine. The assertion is on
 * `max(peak, midLoop)` for each mode so the guard is not hostage to one metric — if a future PHP
 * changes how peak is accounted, the mid-loop sample still discriminates, and vice versa.
 *
 * The ratio is grounded in what THIS stack measures, not in a synthetic figure: the streamed arm's
 * floor is one DATA frame, because `ferrod` batches rows into frames of ~1024 rows or ~256 KiB
 * (`StreamBatch::DEFAULT`) and the client decodes a whole frame at a time. The measured numbers for
 * this fixture are recorded in the commit message and in the task journal.
 */
final class StreamingLiveTest extends DbalLiveTestCase
{
    private const ROWS = 100_000;

    private function seed(\Doctrine\DBAL\Connection $c): void
    {
        $c->executeStatement('DROP TABLE IF EXISTS s8b_stream');
        $c->executeStatement('CREATE TABLE s8b_stream (id int primary key, note text)');
        $c->executeStatement(
            'INSERT INTO s8b_stream SELECT g, repeat(\'x\', 64) FROM generate_series(1, ' . self::ROWS . ') g',
        );
    }

    /**
     * Iterate `$rows` of the fixture and return the memory the iteration cost, as
     * `max(peak, mid-loop)` — see the class docblock for why the residual after the loop is useless.
     *
     * @return int bytes
     */
    private function iterateCost(\Doctrine\DBAL\Connection $c, int $rows): int
    {
        $sql = 'SELECT id, note FROM s8b_stream ORDER BY id LIMIT ' . $rows;
        gc_collect_cycles();
        memory_reset_peak_usage();
        $before = memory_get_usage();
        $seen = 0;
        $mid = 0;
        foreach ($c->iterateAssociative($sql) as $row) {
            ++$seen;
            if ($seen === $rows) {
                self::assertSame($rows, $row['id'], 'the rows arrive in order, to the last one');
            }
            if ($seen === intdiv($rows, 2)) {
                $mid = memory_get_usage() - $before;
            }
        }
        self::assertSame($rows, $seen, 'every row arrived');
        return max(memory_get_peak_usage() - $before, $mid, 1);
    }

    public function testIteratingDoesNotBufferWhileFetchAllDoes(): void
    {
        $c = $this->dbal();
        $this->seed($c);
        $sql = 'SELECT id, note FROM s8b_stream ORDER BY id';

        // ---- STREAMED, at two row counts a factor of four apart. Peak is reset first (PHP >= 8.2,
        // this package's floor) and a sample is taken mid-loop, while the rows — if they were being
        // buffered — would still be held. max() of the two means neither metric alone carries the
        // guard.
        $streamedQuarter = $this->iterateCost($c, intdiv(self::ROWS, 4));
        $streamed = $this->iterateCost($c, self::ROWS);

        // ---- BUFFERED, measured exactly the same way, in the same process, on the same query.
        gc_collect_cycles();
        memory_reset_peak_usage();
        $before = memory_get_usage();
        $all = $c->fetchAllAssociative($sql);
        $bufferedMid = memory_get_usage() - $before;      // still holding $all
        $buffered = max(memory_get_peak_usage() - $before, $bufferedMid);
        self::assertCount(self::ROWS, $all);
        unset($all);

        $report = sprintf(
            'streamed %d B at %d rows and %d B at %d rows; fetchAll %d B (mid-loop %d)',
            $streamed,
            self::ROWS,
            $streamedQuarter,
            intdiv(self::ROWS, 4),
            $buffered,
            $bufferedMid,
        );

        self::assertGreaterThan(
            10_000_000,
            $buffered,
            'the BUFFERED arm must actually buffer, or the comparisons below are vacuous',
        );

        // (1) THE SCALE-INDEPENDENCE PROPERTY, which is what "never buffers" actually means: four
        //     times the rows must cost essentially nothing extra, because the working set is one
        //     DATA frame regardless of the result size. A materialising implementation grows with
        //     the row count instead (measured: the buffered arm is ~4x its own quarter).
        self::assertLessThan(
            intdiv(3 * $streamedQuarter, 2) + self::FRAME_HEADROOM,
            $streamed,
            "iterating must not grow with the result size — $report",
        );

        // (2) …and the absolute separation from a buffered read of the SAME query, in the same
        //     process. See the class docblock for the calibration behind the ratio.
        self::assertLessThan(
            intdiv($buffered, self::SEPARATION),
            $streamed,
            "iterating must not buffer — $report",
        );

        $c->executeStatement('DROP TABLE s8b_stream');
    }

    /**
     * The ratio the streamed arm must beat, CALIBRATED against this stack rather than copied from
     * the plan (which said 50, derived from a synthetic 2 728 B streamed figure that no real client
     * can reach). MEASURED here, PG 17.10 + this driver, 100 000 x (int, 64-char text):
     * streamed peak **3 126 192 B**, mid-loop 1 248 984 B; buffered peak **51 395 416 B**, mid-loop
     * 49 304 816 B — a separation of **16.4x**, whose floor is one DATA frame (`ferrod` batches
     * ~1024 rows or ~256 KiB per frame, `StreamBatch::DEFAULT`, and the client decodes a whole frame
     * at a time). A materialising implementation lands at ~1x, so 5 keeps ~3x of headroom on both
     * sides: it cannot be reached by buffering and is not tripped by frame-size noise.
     */
    private const SEPARATION = 5;

    /** Slack for one DATA frame's worth of decoded rows in the scale-independence assertion. */
    private const FRAME_HEADROOM = 4 * 1024 * 1024;

    /**
     * THE INTERLEAVE IDIOM. The Ferro session is single-in-flight, so the inner statement would
     * throw a `ProtocolException` without `settleOpenStream()`. This is the test that keeps the
     * streaming optimisation from shipping as a user-visible defect.
     */
    public function testWritingInsideAnIterationWorks(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_inter');
        $c->executeStatement('CREATE TABLE s8b_inter (id int primary key, n int)');
        $c->executeStatement('INSERT INTO s8b_inter SELECT g, 0 FROM generate_series(1, 200) g');

        $touched = 0;
        foreach ($c->iterateAssociative('SELECT id FROM s8b_inter ORDER BY id') as $row) {
            $c->executeStatement('UPDATE s8b_inter SET n = 1 WHERE id = ?', [$row['id']]);
            ++$touched;
        }
        self::assertSame(200, $touched);
        self::assertSame(200, (int) $c->fetchOne('SELECT count(*) FROM s8b_inter WHERE n = 1'));

        // …and it worked by MATERIALISING, which is the honest description of what interleaving
        // costs. Asserted so the two paths are told apart: the abandonment test below asserts the
        // opposite on the same counter, and one of them being wrong makes the other red.
        self::assertSame(
            199,
            $this->driverConnection($c)->settledRowCount(),
            'the interleave idiom degrades to buffering — that is the documented cost, and it is '
            . 'paid ONCE, on the first inner statement, for the whole remainder',
        );

        $c->executeStatement('DROP TABLE s8b_inter');
    }

    /**
     * **Abandoning an iteration CANCELS it** — and the assertion that says so is the row counter,
     * not "the connection still works".
     *
     * Plan v1's version of this test asserted only that a later statement succeeded, and it passed
     * through `materialize()`: the driver held a STRONG reference to the open result, so `break`
     * cancelled nothing and the next statement quietly transferred the remaining 99 975 rows. It
     * went green while silently blessing an OOM trap, and its named mutation (deleting `close()`
     * from `free()`) could not fail because `free()` was never reached.
     *
     * With the `\WeakReference` the caller's `break` destroys the driver `Result`, whose
     * `__destruct` sends the `CANCEL`. `settledRowCount()` is how that is observed: **0** here,
     * non-zero in the interleave test above.
     */
    public function testAbandoningAnIterationCancelsInsteadOfDrainingTheRemainder(): void
    {
        $c = $this->dbal();
        $this->seed($c);

        $seen = 0;
        foreach ($c->iterateAssociative('SELECT id, note FROM s8b_stream ORDER BY id') as $_row) {
            if (++$seen === 25) {
                break;
            }
        }
        self::assertSame(25, $seen);

        // The connection is usable — necessary, and on its own not sufficient.
        self::assertSame(self::ROWS, (int) $c->fetchOne('SELECT count(*) FROM s8b_stream'));

        // THE assertion: the remainder was never transferred.
        self::assertSame(
            0,
            $this->driverConnection($c)->settledRowCount(),
            'an abandoned iteration must CANCEL, not drain 99 975 rows into memory on the next statement',
        );

        $c->executeStatement('DROP TABLE s8b_stream');
    }

    /**
     * **The MEASURED LIMIT of the abandonment design, pinned rather than discovered in production.**
     *
     * The `\WeakReference` closes the CANONICAL idiom (the generator is a temporary, so `break`
     * destroys the driver `Result` by refcount and its `__destruct` sends the `CANCEL` — the test
     * above). It does NOT close a BOUND iterator: while `$it` is in scope the `WeakReference` is
     * still live, and a live reference is indistinguishable from a caller who may still fetch. So
     * the next statement materialises the remainder — the expensive-but-correct branch, and the
     * one an operator has to know about.
     *
     * This is a PHP refcount fact, not a design choice (MEASURED on PHP 8.4.18 + doctrine/dbal
     * 4.4.4, both by the controller's `destruct.php` probe and here). It is asserted with the exact
     * remainder rather than "greater than zero" so that a future change which DOES close this shape
     * fails here and gets its documentation updated instead of silently improving.
     *
     * The operator-facing statement of it belongs in `docs/known-incompatibilities.md` (Task 14):
     * *bind an iterator to a variable, abandon it, and the rest of the result set is transferred on
     * your next statement — iterate the call directly, or `unset()` the iterator.*
     */
    public function testABoundIteratorThatIsAbandonedStillTransfersTheRemainder(): void
    {
        $c = $this->dbal();
        $this->seed($c);

        $seen = 0;
        $it = $c->iterateAssociative('SELECT id, note FROM s8b_stream ORDER BY id');
        foreach ($it as $_row) {
            if (++$seen === 25) {
                break;
            }
        }
        self::assertSame(25, $seen);

        self::assertSame(self::ROWS, (int) $c->fetchOne('SELECT count(*) FROM s8b_stream'));
        self::assertSame(
            self::ROWS - 25,
            $this->driverConnection($c)->settledRowCount(),
            'a BOUND iterator keeps the result alive, so its remainder is drained, not cancelled — '
            . 'the documented cost of not iterating the call directly',
        );

        // And once the reference is gone the connection is back to normal: nothing further drains.
        unset($it);
        $c->executeStatement('DROP TABLE s8b_stream');
        self::assertSame(self::ROWS - 25, $this->driverConnection($c)->settledRowCount());
    }

    /**
     * **A failure that happens MID-STREAM must reach the application as a converted DBAL exception,
     * after the rows that already arrived** — the streamed path's equivalent of Task 11's live
     * mapping test, and the one live vantage on `Result::advance()`'s wrapping.
     *
     * A streamed statement's error does not arrive at `executeQuery()` time (the open reads only the
     * `HEAD`); it arrives from the pump on whichever fetch reaches the terminal. `1/(id - 4000)`
     * over an ordered 5 000-row table is a genuine one: PostgreSQL evaluates row by row, so the first
     * DATA frames are already on the wire when row 4 000 divides by zero. Unwrapped, the
     * `Ferro\Client\Error\*` would sail past `Doctrine\DBAL\Result::fetchAssociative()`'s
     * `catch (Driver\Exception)` and past every `catch (Doctrine\DBAL\Exception)` an application
     * has (hazard 9).
     *
     * The session must also RECOVER: charter rule 4 says the request ends in exactly one terminal,
     * and the statement after the failure is what proves the wire is not desynced.
     */
    public function testAMidStreamFailureArrivesConvertedAndTheSessionRecovers(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_midfail');
        $c->executeStatement('CREATE TABLE s8b_midfail (id int primary key)');
        $c->executeStatement('INSERT INTO s8b_midfail SELECT g FROM generate_series(1, 5000) g');

        $seen = 0;
        try {
            foreach ($c->iterateAssociative('SELECT id, 1/(id - 4000) AS q FROM s8b_midfail ORDER BY id') as $_row) {
                ++$seen;
            }
            self::fail('the division by zero at row 4000 must surface as an exception');
        } catch (\Doctrine\DBAL\Exception $e) {
            self::assertInstanceOf(\Doctrine\DBAL\Exception\DriverException::class, $e);
            self::assertSame('22012', $e->getSQLState(), 'division_by_zero, keyed the way DBAL keys PG errors');
        }
        self::assertGreaterThan(0, $seen, 'the rows that arrived before the failure were delivered, not swallowed');

        // Exactly one terminal, so the session is usable — a desynced wire fails HERE.
        self::assertSame(5000, (int) $c->fetchOne('SELECT count(*) FROM s8b_midfail'));

        $c->executeStatement('DROP TABLE s8b_midfail');
    }

    /**
     * **A streamed result discarded WITHOUT a single fetch must still cancel** — the one abandonment
     * shape whose only net is `Result::free()`'s `close()`.
     *
     * `Ferro\Client\Connection::pumpRaw()`'s generator carries a `finally` that abandons the stream
     * when it is destroyed, so for a result that was iterated at all, the `CANCEL` happens even if
     * the driver does nothing (MEASURED: deleting `close()` from `free()` leaves every other live
     * test in this file GREEN — see the task journal's mutation 4). But **a generator that never
     * STARTS never runs its `finally`** (hazard 27, and the whole reason `RawStream::close()` exists),
     * and `Result::streamed()` only OBTAINS the generator — the first `valid()` is what starts it.
     *
     * So `$conn->executeQuery($sql);` in statement position — a real shape: a query issued for its
     * side effects, or a result abandoned before the first `fetch` — leaves an open stream on a
     * strictly single-in-flight session, and the NEXT statement dies with a `ProtocolException`
     * about a stream that is still open. This is the input that makes the plan's mutation 4
     * falsifiable on the wire, and without it that mutation is caught only by the unit fixture
     * (whose hand-rolled generator has no `finally` to stand in).
     */
    public function testAStreamedResultDiscardedWithoutASingleFetchStillCancels(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_nofetch');
        $c->executeStatement('CREATE TABLE s8b_nofetch (id int primary key)');
        $c->executeStatement('INSERT INTO s8b_nofetch SELECT g FROM generate_series(1, 500) g');

        // Opened (the HEAD is read eagerly), never iterated, dropped at the end of this statement.
        $c->executeQuery('SELECT id FROM s8b_nofetch ORDER BY id');

        // The session is single-in-flight: if the stream were still open this would throw.
        self::assertSame(500, (int) $c->fetchOne('SELECT count(*) FROM s8b_nofetch'));
        self::assertSame(
            0,
            $this->driverConnection($c)->settledRowCount(),
            'a never-iterated result must be CANCELled, not drained',
        );

        $c->executeStatement('DROP TABLE s8b_nofetch');
    }

    /**
     * `Doctrine\DBAL\Connection::connect()` is `protected`, but `getNativeConnection()` hands back
     * the `Ferro\Client\Connection`, not our driver `Connection` — so reach the driver connection
     * the way DBAL's own tests do, through the wrapper's protected accessor.
     */
    private function driverConnection(\Doctrine\DBAL\Connection $c): \Ferro\DBAL\Connection
    {
        $driver = (new \ReflectionMethod($c, 'connect'))->invoke($c);
        self::assertInstanceOf(\Ferro\DBAL\Connection::class, $driver);
        return $driver;
    }

    /**
     * MySQL BUFFERS, and that is a documented asymmetry rather than a defect (SPEC §22.2 (n) —
     * MySQL row streaming is deferred, controller decision D-S8b-2). Asserted so the asymmetry is
     * known, and so the day MySQL streaming lands, this test is what says "now change the driver
     * too".
     *
     * `settledRowCount()` is the discriminator: on a buffering family an interleaved statement has
     * NOTHING to settle, because no stream was ever opened. Without that half, a driver that
     * streamed on MySQL (and therefore threw on `supports_row_streaming() == false`) would be the
     * only thing this test could catch, and a driver that streamed successfully one day would pass
     * it while silently changing behaviour.
     */
    public function testMysqlIteratesCorrectlyEvenThoughItBuffers(): void
    {
        $c = $this->dbal($this->requireMysqlPool());
        $c->executeStatement('DROP TABLE IF EXISTS s8b_stream_my');
        $c->executeStatement('CREATE TABLE s8b_stream_my (id INT PRIMARY KEY)');
        $c->executeStatement('INSERT INTO s8b_stream_my (id) VALUES (1),(2),(3)');

        $ids = [];
        foreach ($c->iterateAssociative('SELECT id FROM s8b_stream_my ORDER BY id') as $row) {
            $ids[] = (int) $row['id'];
            // The interleave that would be fatal on a real stream: on MySQL there is no stream at
            // all, so nothing is settled and nothing throws.
            $c->executeStatement('UPDATE s8b_stream_my SET id = id WHERE id = ?', [$row['id']]);
        }
        self::assertSame([1, 2, 3], $ids);
        self::assertSame(
            0,
            $this->driverConnection($c)->settledRowCount(),
            'MySQL never opens a stream, so an interleaved statement settles nothing',
        );

        $c->executeStatement('DROP TABLE s8b_stream_my');
    }
}
