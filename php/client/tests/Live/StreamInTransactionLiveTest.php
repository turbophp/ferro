<?php // /php/client/tests/Live/StreamInTransactionLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\RetryPolicy;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\HydrationException;
use Ferro\Tests\Support\UnhydratableRowDto;
use PHPUnit\Framework\Attributes\DataProvider;

/**
 * The whole-branch review's BLOCKER, one tier below the Doctrine driver (which fixed it in 416e6fd):
 * abandoning a streamed read INSIDE an imperative transaction used to roll that transaction back and
 * tombstone its `tx_id`, silently discarding every write the caller had already made.
 *
 * The mechanism, measured live on PG 17 before the fix: {@see \Ferro\Client\Session::abandonStream}
 * sends an out-of-band `CANCEL`; `ferrod` turns it into a REAL backend `CancelRequest`; PostgreSQL
 * aborts the running statement with `57014`; a statement error inside a `BEGIN` block puts the whole
 * transaction into PG's ABORTED state; so the engine (correctly, §19.3) rolls back and tombstones.
 * The engine's fate is HONEST — by the time it classifies anything the transaction is already dead
 * at the database level — which is why the fix, and therefore this guard, live on the client side.
 *
 * **Why these guards are not functional-only, and why that is not a stylistic preference.** Asserting
 * merely "the transaction survived" CANNOT FAIL at or below one `StreamBatch::DEFAULT` frame
 * (1024 rows / 256 KiB): the producer has already finished, so even the destructive `CANCEL` lands on
 * an idle backend. MEASURED on this tree, pre-fix, PG 17: the data loss reproduces at `LIMIT 50000`
 * and NOT at 1, 1024 or 2000 — and the review recorded 2000 flipping between two identical runs. So
 * the functional assertion passes for the wrong reason at exactly the sizes a fixture reaches for.
 * These guards read {@see Connection::abandonDrainedRowCount} instead, which is the MECHANISM: it is
 * exactly the rows the drain had to move because cancelling was not safe, it is EXACT at every size,
 * and it is 0 whenever the `CANCEL` path was taken.
 *
 * The three abandonment sites are covered separately, because gutting any one of them leaves the
 * other two's tests green:
 *   - {@see \Ferro\Client\RawStream::close} — the site a `foreach { break; }` on a raw stream reaches,
 *     since a live handle keeps the pump generator alive and no `finally` runs;
 *   - {@see \Ferro\Client\Connection::pumpRaw}'s `finally` — a started handle DROPPED without close();
 *   - {@see \Ferro\Client\Connection::stream}'s own `finally` — the assoc generator.
 */
final class StreamInTransactionLiveTest extends LiveTestCase
{
    /**
     * One row per DATA frame. `StreamBatch::DEFAULT` flushes at 1024 rows OR ~256 KiB (whichever
     * trips first, `ferrod/src/services/sql.rs`), so a row comfortably over 256 KiB forces a frame
     * per row. That is what makes the generator-path counters EXACT at a tiny row count — the size
     * band where a functional "the transaction survived" assertion provably cannot fail.
     */
    private const FAT_ROW_BYTES = 300 * 1024;
    private const FAT_ROWS = 8;

    /** @return array<string, array{int}> the four sizes the review named, spanning the 1024 threshold. */
    public static function limits(): array
    {
        return ['1' => [1], '1024' => [1024], '2000' => [2000], '50000' => [50000]];
    }

    // ---- site 1: RawStream::close() -------------------------------------------------------------

    /**
     * The EXACT mechanism, at all four sizes: a raw stream opened inside a transaction and closed
     * WITHOUT being iterated must drain every one of its rows — `abandonDrainedRowCount() === $limit`,
     * on the nose, including at `LIMIT 1`.
     *
     * The "without being iterated" shape is what makes it exact: the eager open has read only the
     * `HEAD`, so the drain starts at the very first DATA frame and the expected count is the whole
     * result set rather than "the result set minus whatever the first frame happened to hold".
     *
     * The three trailing assertions are the OTHER half — that the transaction is not merely
     * un-rolled-back but genuinely alive and still the SAME one: it reads back its own UNCOMMITTED
     * write (invisible to any other connection, so this cannot pass on a fresh autocommit checkout),
     * accepts a further write, and commits both.
     */
    #[DataProvider('limits')]
    public function testARawStreamClosedInsideATransactionDrainsEveryRowAndKeepsTheTransaction(int $limit): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $this->seed($c, 'sitx_close', 60_000);

        $c->begin();
        $c->exec("INSERT INTO sitx_marks VALUES (1, 'before-stream')");
        $stream = $c->streamRaw("SELECT id FROM sitx_close ORDER BY id LIMIT {$limit}", [], true);
        $stream->close();

        self::assertSame(
            $limit,
            $c->abandonDrainedRowCount(),
            "the abandoned stream must have DRAINED all {$limit} rows, not cancelled them",
        );

        // Alive, and the same transaction: only this transaction can see its own uncommitted row.
        self::assertSame(1, $c->scalar('SELECT count(*) FROM sitx_marks'), 'the tx cannot read its own write');
        $c->exec("INSERT INTO sitx_marks VALUES (2, 'after-stream')");
        $c->commit();

        self::assertSame(2, $c->scalar('SELECT count(*) FROM sitx_marks'), 'COMMIT must persist BOTH writes');
    }

    /**
     * The same site under AUTOCOMMIT, where the `CANCEL` is the whole point: it is what keeps a
     * `break` at row 25 of 50 000 from moving the other 49 975. `abandonDrainedRowCount()` must stay
     * 0, which is the only thing that distinguishes "cancelled" from "drained" — the session recovers
     * either way, so a functional guard blesses both.
     *
     * Without this, "fix it by never cancelling" would ship and silently restore an unbounded
     * transfer on every abandoned autocommit stream.
     */
    public function testAnAutocommitStreamStillCancelsAndDrainsNothing(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $this->seed($c, 'sitx_auto', 50_000);

        $stream = $c->streamRaw('SELECT id FROM sitx_auto ORDER BY id', [], true);
        $seen = 0;
        foreach ($stream->rows() as $_row) {
            if (++$seen === 25) {
                break;
            }
        }
        $stream->close();

        self::assertSame(0, $c->abandonDrainedRowCount(), 'an AUTOCOMMIT abandon must CANCEL, never drain');
        // ...and the session is still usable, i.e. the cancel really did reach its one terminal.
        self::assertSame(50_000, $c->scalar('SELECT count(*) FROM sitx_auto'));
    }

    // ---- site 2: Connection::pumpRaw()'s finally ------------------------------------------------

    /**
     * A started raw handle DROPPED inside a transaction without `close()`. Nothing calls
     * `abandonStream()` explicitly here — destroying the handle destroys the pump generator, and a
     * STARTED generator runs its `finally` on destruction. That `finally` is a SEPARATE line of code
     * from {@see \Ferro\Client\RawStream::close}, so it needs its own guard.
     *
     * Fat rows (one per DATA frame) make the count exact: one row is consumed before the drop, so
     * exactly {@see FAT_ROWS} - 1 must be drained. At 8 rows a functional guard could not fail.
     */
    public function testDroppingAStartedRawHandleInsideATransactionDrainsTheRemainder(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $this->seedFat($c, 'sitx_drop');

        $c->begin();
        $c->exec("INSERT INTO sitx_marks VALUES (1, 'before-stream')");
        $stream = $c->streamRaw('SELECT id, blob FROM sitx_drop ORDER BY id', [], true);
        foreach ($stream->rows() as $_row) {
            break;
        }
        self::assertFalse($stream->isClosed(), 'nothing may close this handle — the DROP is the subject');
        unset($stream);

        self::assertSame(
            self::FAT_ROWS - 1,
            $c->abandonDrainedRowCount(),
            'the dropped handle\'s pump finally must DRAIN the remainder, not cancel it',
        );
        self::assertSame(1, $c->scalar('SELECT count(*) FROM sitx_marks'));
        $c->exec("INSERT INTO sitx_marks VALUES (2, 'after-stream')");
        $c->commit();
        self::assertSame(2, $c->scalar('SELECT count(*) FROM sitx_marks'));
    }

    // ---- site 3: Connection::stream()'s finally -------------------------------------------------

    /**
     * The assoc generator — `foreach ($c->stream(...) as $row) { break; }`, the idiom the Doctrine
     * tier's `iterate*()` is built on. Its `finally` is the third and last abandonment site.
     *
     * Fat rows again, for the same reason: one row is consumed, so exactly {@see FAT_ROWS} - 1 must
     * be drained, at a row count where "the transaction survived" is guaranteed to pass pre-fix.
     */
    public function testBreakingOutOfAStreamGeneratorInsideATransactionDrainsTheRemainder(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $this->seedFat($c, 'sitx_gen');

        $c->begin();
        $c->exec("INSERT INTO sitx_marks VALUES (1, 'before-stream')");
        foreach ($c->stream('SELECT id, blob FROM sitx_gen ORDER BY id') as $_row) {
            break;
        }

        self::assertSame(
            self::FAT_ROWS - 1,
            $c->abandonDrainedRowCount(),
            'stream()\'s finally must DRAIN the remainder inside a transaction, not cancel it',
        );
        self::assertSame(1, $c->scalar('SELECT count(*) FROM sitx_marks'));
        $c->exec("INSERT INTO sitx_marks VALUES (2, 'after-stream')");
        $c->commit();
        self::assertSame(2, $c->scalar('SELECT count(*) FROM sitx_marks'));
    }

    /**
     * The generator path at the four review sizes, FUNCTIONAL half — deliberately kept even though
     * it cannot fail below 1024, because at 50 000 it IS the original bug verbatim and it is the
     * assertion a reader recognises: `begin`, a write, an abandoned read, `commit`, the write is
     * still there. The exact-mechanism guards above are what make the small sizes meaningful.
     */
    #[DataProvider('limits')]
    public function testABrokenStreamGeneratorNeverCostsTheTransactionItsWrites(int $limit): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $this->seed($c, 'sitx_fn', 60_000);

        $c->begin();
        $c->exec("INSERT INTO sitx_marks VALUES (1, 'before-stream')");
        foreach ($c->stream("SELECT id FROM sitx_fn ORDER BY id LIMIT {$limit}") as $_row) {
            break;
        }
        $c->commit();

        self::assertSame(1, $c->scalar('SELECT count(*) FROM sitx_marks'), "the write was lost at LIMIT {$limit}");
    }

    // ---- the drain's own two properties ----------------------------------------------------------

    /**
     * The drain REPLENISHES the credit window, and this is the guard that proves it.
     *
     * `abandonStream()`'s cancel path deliberately sends no `WINDOW_UPDATE` — the producer is being
     * torn down. A DRAIN has no such excuse: the producer is alive and will park on backpressure once
     * the per-request window (`DEFAULT_CREDIT_FRAMES` = 64 frames, `DEFAULT_CREDIT_BYTES` = 16 MiB)
     * is exhausted, and a drain that never replenishes then waits forever on a stream nobody
     * cancelled — a HANG, not an error.
     *
     * {@see FAT_ROWS_WIDE} rows of {@see FAT_ROW_BYTES} is ~30 MiB, comfortably past the 16 MiB byte
     * budget, so the producer parks partway through unless the drain keeps paying. Deleting the
     * `sendWindowUpdate` line makes this test hang rather than fail, which is an ugly RED but an
     * unambiguous one.
     */
    public function testTheDrainReplenishesTheCreditWindowAndDoesNotParkTheProducer(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $this->seedFat($c, 'sitx_window', self::FAT_ROWS_WIDE);

        $c->begin();
        $c->exec("INSERT INTO sitx_marks VALUES (1, 'before-stream')");
        $stream = $c->streamRaw('SELECT id, blob FROM sitx_window ORDER BY id', [], true);
        $stream->close();

        self::assertSame(
            self::FAT_ROWS_WIDE,
            $c->abandonDrainedRowCount(),
            'the drain must cross the 16 MiB credit budget, which it can only do by replenishing it',
        );
        $c->commit();
        self::assertSame(1, $c->scalar('SELECT count(*) FROM sitx_marks'));
    }

    /**
     * The review's related MINOR, decided: {@see \Ferro\Client\Session::abandonStream} DISCARDS the
     * stream's one terminal including an ERROR one, and the DRAIN path deliberately does not.
     *
     * The asymmetry is the point. The cancel path's terminal is the ANSWER to a `CANCEL` we just
     * sent, so surfacing it would turn every ordinary `break` into an exception carrying news the
     * caller already has. The drain path's terminal is UNSOLICITED — nothing was sent — so an error
     * in it is the engine declaring a REAL fate about a transaction the caller still holds and is
     * about to commit. Discarding THAT is exactly the silence the review indicted: the failure
     * resurfaces two statements later wearing a false label.
     *
     * `1/(2000 - i)` divides by zero at row 2000, i.e. AFTER a full 1024-row DATA frame has already
     * been delivered — which is what puts the error terminal on the DRAIN path rather than on the
     * open or on the caller's own first read.
     */
    public function testTheDrainSurfacesAMidStreamErrorTerminalInsteadOfSwallowingIt(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS sitx_marks');
        $c->exec('CREATE TABLE sitx_marks (id int primary key, note text)');

        $c->begin();
        $raised = null;
        try {
            foreach ($c->stream('SELECT i, 1/(2000 - i) AS q FROM generate_series(1, 5000) i') as $_row) {
                break;
            }
        } catch (FerroException $e) {
            $raised = $e;
        }

        self::assertNotNull($raised, 'the drain must not swallow the stream\'s error terminal');
        self::assertStringContainsStringIgnoringCase('division by zero', $raised->getMessage());

        try {
            $c->rollBack();
        } catch (FerroException) {
            // The statement error already poisoned the transaction; rollBack() is best-effort here
            // and its own behaviour is not this test's subject.
        }
    }

    /**
     * The OTHER half of that decision, and the reason the drain's throw is GATED: a `finally` that
     * throws DISCARDS whatever exception was already propagating (measured on PHP 8.4 — a generator
     * `finally` is not a destructor and its throw really does replace the interrupted one).
     *
     * So when the caller's OWN code has already failed — here a hydration failure, where the wire is
     * perfectly healthy — the drain still runs (the next request must not read leftover frames) but
     * must stay SILENT. Burying a precise "this DTO cannot take this row" under the stream's
     * incidental terminal would trade the actionable error for a confusing one. Same discipline as
     * `$wireFailed`, different failure.
     *
     * The setup deliberately arranges BOTH failures at once: `1/(2000 - i)` guarantees an ERROR
     * terminal waiting on the drain, and {@see UnhydratableRowDto} guarantees the caller's error
     * arrives first, on row 1.
     */
    public function testAHydrationFailureIsNotOverwrittenByTheDrainsOwnTerminal(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS sitx_marks');
        $c->exec('CREATE TABLE sitx_marks (id int primary key, note text)');

        $c->begin();
        $raised = null;
        try {
            $rows = $c->stream(
                'SELECT i, 1/(2000 - i) AS q FROM generate_series(1, 5000) i',
                [],
                UnhydratableRowDto::class,
            );
            foreach ($rows as $_row) {
                break;
            }
        } catch (\Throwable $e) {
            $raised = $e;
        }

        self::assertInstanceOf(HydrationException::class, $raised, 'the CALLER\'s error must survive');
        self::assertStringNotContainsStringIgnoringCase(
            'division by zero',
            $raised->getMessage(),
            'the drain\'s terminal must not REPLACE the hydration failure it interrupted',
        );

        try {
            $c->rollBack();
        } catch (FerroException) {
            // Best-effort: the stream's own statement error already poisoned the transaction.
        }
    }

    // ---- fixtures -------------------------------------------------------------------------------

    /** ~30 MiB at {@see FAT_ROW_BYTES} per row — past the 16 MiB credit-byte budget. */
    private const FAT_ROWS_WIDE = 100;

    /** A narrow table of `$rows` ints, plus the marker table every transaction guard writes to. */
    private function seed(Connection $c, string $table, int $rows): void
    {
        $c->exec("DROP TABLE IF EXISTS {$table}");
        $c->exec("CREATE TABLE {$table} (id int primary key)");
        $c->exec("INSERT INTO {$table} SELECT g FROM generate_series(1, {$rows}) g");
        $c->exec('DROP TABLE IF EXISTS sitx_marks');
        $c->exec('CREATE TABLE sitx_marks (id int primary key, note text)');
    }

    /** A table whose rows each exceed one DATA frame's byte budget — see {@see FAT_ROW_BYTES}. */
    private function seedFat(Connection $c, string $table, int $rows = self::FAT_ROWS): void
    {
        $c->exec("DROP TABLE IF EXISTS {$table}");
        $c->exec("CREATE TABLE {$table} (id int primary key, blob text)");
        $c->exec(
            "INSERT INTO {$table} SELECT g, repeat('x', " . self::FAT_ROW_BYTES . ") "
                . "FROM generate_series(1, {$rows}) g",
        );
        $c->exec('DROP TABLE IF EXISTS sitx_marks');
        $c->exec('CREATE TABLE sitx_marks (id int primary key, note text)');
    }
}
