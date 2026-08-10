<?php // /php/doctrine-dbal/tests/Unit/StreamedResultTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\RawStream;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\Result;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 12 — the streamed `Result` mode, over a hand-built `RawStream` (no engine).
 *
 * What is pinned here is the LAZINESS itself: a `Result` that pulled its generator eagerly would
 * satisfy every functional assertion in this file and still buffer 50 000 rows, so the tests below
 * count how far the generator has advanced rather than only checking the values that come out.
 *
 * The counter is incremented INSIDE the generator immediately before each `yield`, so it reads
 * "rows the producer has been asked to produce" — which is exactly the quantity a streaming result
 * must keep proportional to the number of rows the caller has actually fetched.
 */
final class StreamedResultTest extends TestCase
{
    /**
     * @param list<list<mixed>> $rows
     * @param ?FakeSession $session pass one to observe the `CANCEL`; `null` builds a wire-less
     *   stream, which is fine for the pure fetch/laziness tests and USELESS for anything asserting
     *   that `close()` reached the engine — `RawStream::close()` is `$this->session?->abandonStream()`,
     *   so with a null session it provably touches nothing.
     */
    private function stream(array $rows, ?int &$pulled = null, ?FakeSession $session = null): RawStream
    {
        $pulled = 0;
        $gen = (static function () use ($rows, &$pulled): \Generator {
            foreach ($rows as $r) {
                ++$pulled;
                yield $r;
            }
        })();
        return new RawStream(['id', 'note'], $gen, $session, 7);
    }

    public function testFetchingPullsExactlyOneRowAtATime(): void
    {
        $pulled = 0;
        $r = Result::streamed($this->stream([[1, 'a'], [2, 'b'], [3, 'c']], $pulled));

        self::assertSame(0, $pulled, 'constructing a streamed Result must not pull anything');
        self::assertSame(['id' => 1, 'note' => 'a'], $r->fetchAssociative());
        self::assertSame(1, $pulled, 'one fetch, one row');
        self::assertSame([2, 'b'], $r->fetchNumeric());
        self::assertSame(2, $pulled);
        self::assertSame(['id' => 3, 'note' => 'c'], $r->fetchAssociative());
        self::assertSame(3, $pulled);
        self::assertFalse($r->fetchNumeric(), 'exhausted');
        self::assertFalse($r->fetchAssociative(), 'and stays exhausted');
    }

    /** Columns are readable before the first row — DBAL calls `columnCount()` before any fetch. */
    public function testColumnsAreAvailableWithoutPullingARow(): void
    {
        $pulled = 0;
        $r = Result::streamed($this->stream([[1, 'a']], $pulled));
        self::assertSame(2, $r->columnCount());
        self::assertSame('note', $r->getColumnName(1));
        self::assertSame(0, $pulled);
    }

    /**
     * **The row that arrived before a mid-stream error is DELIVERED, and the error surfaces on the
     * NEXT fetch.** This is the guard behind this task's one deviation from the plan's literal
     * `fetchNumeric()` body, and it is the reason the streamed cursor advances LAZILY (it holds the
     * generator parked on the row it just returned and advances on the following call) instead of
     * calling `next()` eagerly the way plan v2's Step-3 snippet does.
     *
     * `Ferro\Client\Connection::pumpRaw()` yields every row of a DATA frame and only then reads the
     * next frame, so a mid-stream error terminal throws AFTER the rows that already arrived —
     * `RawStream::rows()`'s docblock states exactly that contract. An eager `next()` runs that read
     * INSIDE the same `fetchNumeric()` call that already holds the row, so the exception replaces
     * the return value and the last row before the error is silently LOST (MEASURED on PHP 8.4.18:
     * `current()` returns the row, `next()` throws, the row never reaches the caller).
     *
     * The same lazy advance is what makes {@see testFetchingPullsExactlyOneRowAtATime}'s
     * `assertSame(1, $pulled)` true: an eager `next()` leaves the producer one row ahead of the
     * consumer forever (measured: `$pulled === 2` after a single fetch).
     */
    public function testARowThatArrivedBeforeAMidStreamErrorIsDeliveredBeforeItThrows(): void
    {
        $gen = (static function (): \Generator {
            yield [1, 'a'];
            throw new \RuntimeException('mid-stream error terminal');
        })();
        $r = Result::streamed(new RawStream(['id', 'note'], $gen, null, 7));

        self::assertSame([1, 'a'], $r->fetchNumeric(), 'the row that arrived must not be swallowed by the error behind it');

        try {
            $r->fetchNumeric();
            self::fail('the mid-stream error must surface on the following fetch');
        } catch (\RuntimeException $e) {
            self::assertSame('mid-stream error terminal', $e->getMessage());
        }

        self::assertFalse($r->fetchNumeric(), 'and the result is exhausted afterwards, not stuck');
    }

    /**
     * **A mid-stream failure must cross the driver boundary as a `Doctrine\DBAL\Driver\Exception`,
     * carrying its SQLSTATE and errno.** Found while running Task 11's live suite against this
     * task's `query()`, and the plan does not mention it.
     *
     * A streamed statement's error does NOT arrive at `executeQuery()` time — the open reads only
     * the `HEAD` — it arrives from the pump on whichever fetch reaches the terminal, as a
     * `Ferro\Client\Error\*`. `Doctrine\DBAL\Result::fetchAssociative()` (and every sibling, dbal
     * 4.4.4 `src/Result.php:34-101`) catches exactly `Doctrine\DBAL\Driver\Exception` before calling
     * `Connection::convertException()`, so an unwrapped client exception sails past DBAL's
     * conversion, past `Ferro\DBAL\ExceptionConverter`, and past every
     * `catch (Doctrine\DBAL\Exception)` the application has — taking the §9.2 branch and the
     * `IndeterminateWriteException` mapping with it (hazard 9). The buffered path wraps in
     * `Connection::runPrepared()`; the streamed path wraps here.
     */
    public function testAMidStreamEngineErrorArrivesAsADbalDriverExceptionWithItsPayload(): void
    {
        $gen = (static function (): \Generator {
            yield [1, 'a'];
            throw new NonRetryableException(new ErrorPayload(
                C::ERR_UNIQUE,
                3,
                '23505',
                1062,
                'duplicate key value violates unique constraint',
                null,
                null,
            ));
        })();
        $r = Result::streamed(new RawStream(['id', 'note'], $gen, null, 7));
        self::assertSame([1, 'a'], $r->fetchNumeric());

        try {
            $r->fetchNumeric();
            self::fail('the mid-stream error must reach the caller');
        } catch (DriverException $e) {
            self::assertSame('23505', $e->getSQLState(), 'the SQLSTATE the stock PG converter keys on');
            self::assertSame(1062, $e->getCode(), 'the vendor errno the stock MySQL converter keys on');
            self::assertSame(3, $e->branch(), 'and the §9.2 branch our own converter keys on');
        }
    }

    /**
     * **The error terminal that arrives BEFORE the first row must be wrapped too** — the third and
     * least obvious of the three places a streamed pull touches the wire.
     *
     * `Generator::valid()` is what STARTS a generator, and starting `pumpRaw()` runs it up to its
     * first `yield`, i.e. through `readStreamFrame()`. A statement that fails with no rows produced
     * — a server-side cancel, a `statement_timeout`, a constraint hit on the first row — therefore
     * throws out of `valid()`, not out of `next()`. Wrapping only `next()` leaves that case escaping
     * raw, past `Doctrine\DBAL\Result`'s `catch (Driver\Exception)` and past
     * `Ferro\DBAL\ExceptionConverter` (hazard 9).
     *
     * FOUND LIVE, not reasoned: Task 11's `testACancelledSelectIsIndeterminateOnAWriteConnection…`
     * started failing the moment `query()` streamed, with
     * `Ferro\Client\Error\IndeterminateException` escaping from `Result::fetchNumeric()`'s
     * `$gen->valid()` — i.e. the §19.3 fate was correct and the exception was UNCONVERTIBLE, which
     * is the worse half of hazard 9: an application's `catch (Doctrine\DBAL\Exception)` misses it
     * entirely, and so does every framework's.
     */
    public function testAnErrorTerminalBeforeTheFirstRowIsWrappedToo(): void
    {
        $gen = (static function (): \Generator {
            throw new NonRetryableException(new ErrorPayload(
                C::ERR_UNIQUE,
                3,
                '23505',
                1062,
                'failed before a single row',
                null,
                null,
            ));
            yield [1, 'a']; // @phpstan-ignore-line unreachable, and that is the point
        })();
        $r = Result::streamed(new RawStream(['id', 'note'], $gen, null, 7));

        try {
            $r->fetchNumeric();
            self::fail('the terminal must surface on the first fetch');
        } catch (DriverException $e) {
            self::assertSame('23505', $e->getSQLState());
            self::assertSame(3, $e->branch());
        }
    }

    /**
     * The same boundary, on the OTHER streamed path that touches the wire: `materialize()`, which
     * runs from `Connection::settleOpenStream()` in the middle of an unrelated statement. An
     * unwrapped client exception there would surface from `executeStatement()`/`beginTransaction()`
     * as a class DBAL cannot convert.
     */
    public function testAFailureDuringMaterializeIsWrappedToo(): void
    {
        $gen = (static function (): \Generator {
            yield [1, 'a'];
            yield [2, 'b'];
            throw new NonRetryableException(new ErrorPayload(
                C::ERR_UNIQUE,
                3,
                '23505',
                null,
                'boom',
                null,
                null,
            ));
        })();
        $r = Result::streamed(new RawStream(['id', 'note'], $gen, null, 7));
        $r->fetchNumeric();

        $this->expectException(DriverException::class);
        $r->materialize();
    }

    /**
     * `materialize()` drains the REMAINDER into memory and leaves the already-fetched rows
     * consumed — the interleaving escape hatch. Idempotent, and afterwards the result is an
     * ordinary buffered one. The RETURN value is how many rows the drain cost, which is what
     * {@see \Ferro\DBAL\Connection::settledRowCount} surfaces.
     *
     * **It must NOT be written as `foreach ($this->gen as $row)`.** `foreach` calls
     * `Generator::rewind()`, which throws `Cannot rewind a generator that was already run` once the
     * generator has advanced past its first yield — and the streamed `fetchNumeric()` above advances
     * it on every call (hazard 78, measured on PHP 8.4.18). That is why this test fetches a row
     * BEFORE materialising: without the fetch, the bug is invisible.
     */
    public function testMaterializeDrainsTheRemainderAndIsIdempotent(): void
    {
        $pulled = 0;
        $r = Result::streamed($this->stream([[1, 'a'], [2, 'b'], [3, 'c']], $pulled));
        $r->fetchNumeric();

        self::assertSame(2, $r->materialize(), 'the drain cost two rows');
        self::assertSame(0, $r->materialize(), 'idempotent: the second call drains nothing');
        self::assertSame(3, $pulled, 'everything is now in memory');
        self::assertFalse($r->isStreaming());
        self::assertSame([[2, 'b'], [3, 'c']], $r->fetchAllNumeric(), 'the UNCONSUMED rows remain, in order');
    }

    /**
     * The `fetchAll*` family over a STREAMED result — a second vantage point on the same cursor.
     *
     * `Result` delegates all four to `Doctrine\DBAL\Driver\FetchUtils`, which is built purely on
     * `fetchNumeric()`/`fetchAssociative()`, so this passes for free — as long as nobody "optimises"
     * one of them into `return $this->rows;`. In streamed mode `$this->rows` is invariantly `[]`, so
     * that shortcut is not a slow answer, it is a silently EMPTY one. Task 8's
     * `testTheFetchAllFamilyDrainsFromTheCursorNotFromTheBuffer` forbids it from the buffered
     * vantage, where the same mutation merely returns too many rows; this is the vantage where it
     * returns none.
     */
    public function testTheFetchAllFamilyDrainsAStreamedResultThroughTheOneCursor(): void
    {
        $pulled = 0;
        $r = Result::streamed($this->stream([[1, 'a'], [2, 'b'], [3, 'c']], $pulled));
        $r->fetchNumeric();

        self::assertSame(
            [['id' => 2, 'note' => 'b'], ['id' => 3, 'note' => 'c']],
            $r->fetchAllAssociative(),
            'only the REMAINDER, in order, associatively keyed by the HEAD columns',
        );
        self::assertSame(3, $pulled);
        self::assertSame([], $r->fetchAllNumeric(), 'and nothing is left');
    }

    /**
     * `free()` on a streamed result closes the stream and empties it — and the close reaches the
     * WIRE, which is what a `FakeSession` (rather than v1's `null`) is here to witness. With a null
     * session `RawStream::close()` is `$this->session?->abandonStream(...)`, i.e. provably a no-op,
     * so the v1 form of this test could not tell a real `CANCEL` from no `CANCEL` at all.
     */
    public function testFreeClosesTheStreamOnTheWire(): void
    {
        $pulled = 0;
        $session = new FakeSession();
        $stream = $this->stream([[1, 'a'], [2, 'b']], $pulled, $session);
        $r = Result::streamed($stream);
        $r->fetchNumeric();
        self::assertSame(0, $session->abandonCount, 'nothing abandoned while the result is live');

        $r->free();
        self::assertSame(1, $session->abandonCount, 'free() must CANCEL + drain to the ONE terminal');
        self::assertTrue($stream->isClosed());
        self::assertFalse($r->fetchNumeric());
        self::assertSame(0, $r->columnCount());
    }

    /**
     * **Destruction frees.** This is the whole abandonment design in one assertion: when the caller
     * drops the result (which is what `break`-ing out of `Doctrine\DBAL\Result::iterateAssociative()`
     * does — the Generator holds the only reference, and `Doctrine\DBAL\Result` has no `__destruct`,
     * hazard 80), the driver `Result` must send the `CANCEL` itself. Nothing else will: DBAL never
     * calls the driver's `free()` on abandonment, and from Step 4 the driver `Connection` holds only
     * a `\WeakReference`, precisely so this destruction can happen.
     */
    public function testDroppingAStreamedResultCancelsTheStream(): void
    {
        $pulled = 0;
        $session = new FakeSession();
        $r = Result::streamed($this->stream([[1, 'a'], [2, 'b'], [3, 'c']], $pulled, $session));
        $r->fetchNumeric();

        unset($r);            // the last reference — PHP frees it here, by refcount
        self::assertSame(1, $session->abandonCount, 'a dropped streamed Result must abandon its stream');
        self::assertSame(1, $pulled, 'and must NOT have drained the remainder to get there');
    }

    /**
     * THE MIRROR of the two guards above: a stream that ENDED on its own has nothing to cancel, and
     * a result that drained it must not send one anyway.
     *
     * Without this, `free()`/`__destruct` could abandon UNCONDITIONALLY — even for a request the
     * engine already terminated — and both abandonment assertions would still be green. On the real
     * wire that call is harmless (`Session::abandonStream()` returns immediately when no stream with
     * that id is open, `Session.php:344-353`), which is precisely why nothing downstream would
     * notice the difference and why the discrimination has to be made here, where the fixture counts
     * every call it receives.
     */
    public function testAFullyDrainedStreamedResultCancelsNothing(): void
    {
        $pulled = 0;
        $session = new FakeSession();
        $r = Result::streamed($this->stream([[1, 'a'], [2, 'b']], $pulled, $session));
        while ($r->fetchNumeric() !== false) {
            // drain to exhaustion, which is what a completed `iterateAssociative()` does
        }
        self::assertFalse($r->isStreaming(), 'an exhausted stream is no longer a stream');

        $r->free();
        unset($r);
        self::assertSame(0, $session->abandonCount, 'a stream that reached its own terminal must not be CANCELled');
        self::assertSame(2, $pulled);
    }

    /**
     * A streamed read reports `rowCount() === 0`, because the HEAD/DATA/END producer carries no
     * `affected` field at all. This is the reason the PREPARED path does not stream:
     * `Doctrine\DBAL\Connection::executeStatement()` RETURNS this number.
     */
    public function testAStreamedResultReportsNoAffectedCount(): void
    {
        $pulled = 0;
        self::assertSame(0, Result::streamed($this->stream([[1, 'a']], $pulled))->rowCount());
    }
}
