<?php // /php/client/tests/Live/RawStreamLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\RetryPolicy;

/**
 * M1-S8b Task 2, live: `streamRaw()` against a real ferrod on real PostgreSQL. The properties that
 * matter are
 *   (a) the rows arrive POSITIONAL and in order, and the columns are readable before the first row;
 *   (b) a stream abandoned by `close()` leaves the session usable — the only observable proof that
 *       the CANCEL+drain really happened, since `FakeSession` can only COUNT the call;
 *   (c) a stream abandoned by DROPPING the handle recovers too, through a different mechanism
 *       (the pump generator's own `finally`) — (b) and (c) are separately mutation-proven, because
 *       gutting either mechanism leaves the other one's test green;
 *   (d) inside an imperative transaction the stream is tx-SCOPED, which `streamRaw()`'s docblock
 *       claims and nothing else proves.
 */
final class RawStreamLiveTest extends LiveTestCase
{
    public function testColumnsAreReadableBeforeAnyRowAndRowsArePositional(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8b_stream');
        $c->exec('CREATE TABLE s8b_stream (id int primary key, note text)');
        $c->exec('INSERT INTO s8b_stream SELECT g, \'n\' || g FROM generate_series(1, 500) g');

        $stream = $c->streamRaw('SELECT id, note FROM s8b_stream ORDER BY id', [], true);
        self::assertSame(['id', 'note'], $stream->columns(), 'columns must be readable before the first row');

        $seen = 0;
        foreach ($stream->rows() as $row) {
            self::assertSame($seen + 1, $row[0], 'rows are POSITIONAL and in order');
            ++$seen;
        }
        self::assertSame(500, $seen);

        // A FULL drain reached the ONE terminal (charter rule 4) and needs no close(): if it had
        // not, this next statement would be refused by the session's single-in-flight guard.
        $c->exec('DROP TABLE s8b_stream');
    }

    /**
     * ABANDONMENT. Break out after 10 rows, `close()` the handle, then run another statement on the
     * SAME connection. If the CANCEL+drain did not happen, the next request reads the leftover
     * DATA frames as its own reply and this throws a ProtocolException — so the plain assertion
     * below IS the guard.
     *
     * Note that the `break` alone does NOT free anything: `$stream` (and therefore the pump
     * generator) is still referenced by this frame, so the generator's own `finally` has not run.
     * `close()` is the only thing between here and a desynced wire — exactly the hazard the eager
     * open creates and the reason {@see \Ferro\Client\RawStream::close} exists.
     */
    public function testAbandoningAStreamLeavesTheSessionUsable(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8b_abandon');
        $c->exec('CREATE TABLE s8b_abandon (id int primary key)');
        $c->exec('INSERT INTO s8b_abandon SELECT g FROM generate_series(1, 5000) g');

        $stream = $c->streamRaw('SELECT id FROM s8b_abandon ORDER BY id', [], true);
        $seen = 0;
        foreach ($stream->rows() as $_row) {
            if (++$seen === 10) {
                break;
            }
        }
        self::assertSame(10, $seen, 'the break must land mid-stream, not after a short result set');
        $stream->close();

        self::assertSame(5000, $c->scalar('SELECT count(*) FROM s8b_abandon'));
        $c->exec('DROP TABLE s8b_abandon');
    }

    /**
     * The OTHER abandonment path, and the reason {@see \Ferro\Client\Connection::pumpRaw} carries a
     * `finally` of its own: a handle DROPPED mid-iteration without `close()`. {@see RawStream} has
     * no `__destruct`, so nothing here calls `abandonStream()` explicitly — destroying the handle
     * destroys the pump generator, and a STARTED generator runs its `finally` on destruction, which
     * is what issues the CANCEL+drain.
     *
     * This is the complement of {@see testAbandoningAStreamLeavesTheSessionUsable}: there the
     * generator is still referenced and `close()` is the only thing that saves the wire; here
     * `close()` is never called and the generator's `finally` is. Both paths exist in the code, so
     * both need a guard — otherwise deleting either one leaves the whole suite green.
     */
    public function testDroppingAStartedStreamHandleWithoutClosingItStillRecovers(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8b_drop');
        $c->exec('CREATE TABLE s8b_drop (id int primary key)');
        $c->exec('INSERT INTO s8b_drop SELECT g FROM generate_series(1, 5000) g');

        $seen = 0;
        $stream = $c->streamRaw('SELECT id FROM s8b_drop ORDER BY id', [], true);
        foreach ($stream->rows() as $_row) {
            if (++$seen === 10) {
                break;
            }
        }
        self::assertSame(10, $seen, 'the break must land mid-stream');
        self::assertFalse($stream->isClosed(), 'nothing may close this handle — the drop is the subject');
        unset($stream);

        self::assertSame(5000, $c->scalar('SELECT count(*) FROM s8b_drop'));
        $c->exec('DROP TABLE s8b_drop');
    }

    /**
     * The tx-SCOPED half, which `streamRaw()`'s docblock asserts and nothing else proves: a stream
     * opened inside an imperative transaction must carry that transaction's `tx_id`, so it sees the
     * transaction's OWN uncommitted writes. An autocommit-scoped stream would land on a different
     * pooled connection, see none of them, and return zero rows — a silent wrong answer rather than
     * an error, which is precisely why it needs a live guard rather than a comment.
     */
    public function testAStreamInsideATransactionSeesThatTransactionsUncommittedRows(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8b_tx_stream');
        $c->exec('CREATE TABLE s8b_tx_stream (id int primary key)');

        $c->begin();
        $c->exec('INSERT INTO s8b_tx_stream SELECT g FROM generate_series(1, 20) g');

        $stream = $c->streamRaw('SELECT id FROM s8b_tx_stream ORDER BY id', [], true);
        $ids = [];
        foreach ($stream->rows() as $row) {
            $ids[] = $row[0];
        }
        self::assertSame(range(1, 20), $ids, 'a tx-scoped stream must see the open transaction\'s own writes');

        $c->rollBack();

        // The mirror: those rows really were uncommitted, so the guard above measured tx scoping
        // rather than a table that happened to be populated.
        self::assertSame(0, $c->scalar('SELECT count(*) FROM s8b_tx_stream'));
        $c->exec('DROP TABLE s8b_tx_stream');
    }
}
