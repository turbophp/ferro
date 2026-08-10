<?php // /php/doctrine-dbal/tests/Unit/ResultTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Driver\Exception as DoctrineDriverException;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception\InvalidColumnIndex;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\Result;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 8 — the driver `Result` contract.
 *
 * `getColumnName()` is declared on `Doctrine\DBAL\Driver\Result` only as a docblock
 * `@method`, which makes it look optional. It is not: `Doctrine\DBAL\Result::getColumnName()`
 * throws a `LogicException` via `method_exists` when it is missing,
 * `Connection::executeCacheQuery()` loops it to build the cache key, and
 * `AbstractResultMiddleware` forwards it with the same guard — so omitting it silently disables
 * DBAL's result cache and breaks any middleware wrapping our result. All eight bundled driver
 * Results implement it; so do we, and {@see testGetColumnNameIsReachableThroughDoctrinesOwnWrapper}
 * asserts it from the vantage point where the `method_exists` guard actually lives.
 *
 * This file also carries the two `rowCount()` fixtures Task 5 added out of line (its own probe found
 * the class docblock's "never count($rows)" claim was decorative), folded in here because Task 8
 * owns the whole contract.
 */
final class ResultTest extends TestCase
{
    private function sample(): Result
    {
        return Result::buffered(['id', 'note'], [[1, 'a'], [2, 'b']], 2);
    }

    public function testTheWholeFetchFamily(): void
    {
        self::assertSame([1, 'a'], $this->sample()->fetchNumeric());
        self::assertSame(['id' => 1, 'note' => 'a'], $this->sample()->fetchAssociative());
        self::assertSame(1, $this->sample()->fetchOne());
        self::assertSame([[1, 'a'], [2, 'b']], $this->sample()->fetchAllNumeric());
        self::assertSame(
            [['id' => 1, 'note' => 'a'], ['id' => 2, 'note' => 'b']],
            $this->sample()->fetchAllAssociative(),
        );
        self::assertSame([1, 2], $this->sample()->fetchFirstColumn());
    }

    /**
     * End-of-result is `false` from every method, repeatedly.
     *
     * The plan wrote `assertNull()` for `fetchOne()` on an empty result. That is the WRONG side of
     * the contract and the assertion is corrected here rather than the code: dbal 4.4.4's
     * `Doctrine\DBAL\Result::fetchOne()` is documented "or FALSE if there are no more rows", its
     * `Driver\FetchUtils::fetchOne()` returns `false`, and — MEASURED against real `FetchUtils` —
     * a driver Result answering `null` at the end makes `fetchFirstColumn()`
     * (`while (($v = fetchOne()) !== false)`) never terminate.
     */
    public function testAnExhaustedResultReturnsFalseAndThenKeepsReturningFalse(): void
    {
        $r = $this->sample();
        $r->fetchNumeric();
        $r->fetchNumeric();
        self::assertFalse($r->fetchNumeric());
        self::assertFalse($r->fetchNumeric());
        self::assertFalse($r->fetchAssociative());
        self::assertFalse(Result::buffered(['x'], [], 0)->fetchOne(), 'fetchOne on an empty result');
    }

    /**
     * The MIRROR of the row above, and the reason it is not enough on its own: `false` means
     * end-of-result and `null` means "this row's first cell is NULL". A `$row[0] ?? false`
     * implementation satisfies the exhaustion assertion and silently TRUNCATES `fetchFirstColumn()`
     * at the first NULL — a wrong ANSWER, not an error, on any nullable column.
     */
    public function testFetchOneTellsANullFirstCellApartFromEndOfResult(): void
    {
        $r = Result::buffered(['x'], [[null], [1]], 2);
        self::assertNull($r->fetchOne(), 'a NULL first cell is null, not false');
        self::assertSame(1, $r->fetchOne());
        self::assertFalse($r->fetchOne(), 'and only the END is false');

        self::assertSame(
            [null, 1],
            Result::buffered(['x'], [[null], [1]], 2)->fetchFirstColumn(),
            'fetchFirstColumn must not stop at a NULL',
        );
    }

    /**
     * `rowCount()` is the TERMINAL's `affected`, never `count($rows)`. The research spike shipped
     * `rowCount() === 0` for an `UPDATE` that changed one row precisely because it conflated them,
     * and `Doctrine\DBAL\Connection::executeStatement()` returns exactly this number.
     *
     * Every row here keeps the two quantities DIFFERENT, so neither can pass for the other's reason.
     */
    public function testRowCountIsTheAffectedCountNotTheRowCount(): void
    {
        self::assertSame(7, Result::buffered([], [], 7)->rowCount(), 'a write: rows empty, affected 7');
        self::assertSame(0, Result::buffered(['id'], [[1], [2], [3]], 0)->rowCount(), 'never count($rows)');
        self::assertSame(9, Result::buffered(['id'], [[1]], 9)->rowCount(), '1 row, 9 affected');
        self::assertSame(3, Result::buffered([], [], 3)->rowCount(), 'an UPDATE that returns no rows');
    }

    /**
     * `fetchAll*()` and `fetchFirstColumn()` read FROM THE CURSOR — they are `FetchUtils` drains of
     * `fetchNumeric()`/`fetchAssociative()`, not copies of the buffer. Returning `$this->rows`
     * wholesale would pass every fresh-result assertion above and be wrong for a partially consumed
     * one; it is also the shape that could not survive Task 12's streamed mode, where there IS no
     * buffer to return.
     */
    public function testTheFetchAllFamilyDrainsFromTheCursorNotFromTheBuffer(): void
    {
        $r = $this->sample();
        self::assertSame([1, 'a'], $r->fetchNumeric());
        self::assertSame([[2, 'b']], $r->fetchAllNumeric(), 'only the remainder');
        self::assertSame([], $r->fetchAllNumeric(), 'and then nothing');

        $r2 = $this->sample();
        $r2->fetchNumeric();
        self::assertSame([['id' => 2, 'note' => 'b']], $r2->fetchAllAssociative(), 'only the remainder');

        $r3 = $this->sample();
        $r3->fetchNumeric();
        self::assertSame([2], $r3->fetchFirstColumn(), 'only the remainder');

        // ONE cursor, shared by both real fetch methods — not one index per shape.
        $r4 = $this->sample();
        self::assertSame([1, 'a'], $r4->fetchNumeric());
        self::assertSame(['id' => 2, 'note' => 'b'], $r4->fetchAssociative(), 'the SECOND row');
    }

    public function testColumnNamesAndTheInvalidIndexContract(): void
    {
        $r = $this->sample();
        self::assertSame(2, $r->columnCount());
        self::assertSame('id', $r->getColumnName(0));
        self::assertSame('note', $r->getColumnName(1));

        $this->expectException(InvalidColumnIndex::class);
        $r->getColumnName(2);
    }

    /**
     * The vantage point hazard 6 is actually about. `Doctrine\DBAL\Result::getColumnName()` guards
     * the call with `method_exists($this->result, 'getColumnName')` and throws
     * `LogicException("The driver result … does not support accessing the column name.")` when the
     * driver has no such method — so a driver Result missing it fails HERE, in DBAL's own wrapper
     * (and in `executeCacheQuery()` and every result middleware), not in our own class.
     *
     * The wrapper is constructed directly, which needs no database: `DriverManager::getConnection()`
     * does not connect, and `Doctrine\DBAL\Result` only touches its Connection on the exception path.
     */
    public function testGetColumnNameIsReachableThroughDoctrinesOwnWrapper(): void
    {
        $conn = DriverManager::getConnection([
            'driverClass' => \Ferro\DBAL\Driver::class,
            'unix_socket' => '/nonexistent/ferro-task8.sock', // never dialled: nothing here connects
            'driverOptions' => ['pool' => 'default'],
        ]);
        $wrapped = new \Doctrine\DBAL\Result($this->sample(), $conn);

        self::assertSame('id', $wrapped->getColumnName(0));
        self::assertSame('note', $wrapped->getColumnName(1));
        self::assertSame(['id' => 1, 'note' => 'a'], $wrapped->fetchAssociative());
        self::assertSame(2, $wrapped->rowCount());
    }

    /**
     * `free()` is idempotent and leaves no rows and no columns, the same post-free state the stock
     * `Driver\PgSQL\Result` reaches by nulling its handle.
     *
     * `rowCount()` SURVIVES it, matching the stock `Driver\SQLite3\Result` (whose `$changes` is a
     * captured int, not a handle). That divergence from `PgSQL\Result` — which answers 0 after
     * `free()` because it reads `pg_affected_rows()` off the released handle — is pinned here so it
     * stays a decision rather than an accident.
     */
    public function testFreeIsIdempotentAndLeavesAnEmptyResultButKeepsTheAffectedCount(): void
    {
        $r = $this->sample();
        $r->free();
        $r->free();
        self::assertFalse($r->fetchNumeric());
        self::assertSame(0, $r->columnCount());
        self::assertSame([], $r->fetchAllAssociative());
        self::assertSame([], $r->fetchAllNumeric());
        self::assertSame([], $r->fetchFirstColumn());
        self::assertSame(2, $r->rowCount(), 'the terminal already delivered this number');

        $this->expectException(InvalidColumnIndex::class);
        $r->getColumnName(0);
    }

    /**
     * DUPLICATE COLUMN NAMES collapse in the associative shape and survive in the numeric one. That
     * is PDO's behaviour too, and it is exactly why `fetchNumeric()` had to be built on positional
     * rows rather than on `array_values()` of an associative row.
     */
    public function testDuplicateColumnNamesCollapseAssociativelyAndSurviveNumerically(): void
    {
        $r = Result::buffered(['x', 'x'], [[1, 2]], 1);
        self::assertSame([1, 2], $r->fetchNumeric());

        $r2 = Result::buffered(['x', 'x'], [[1, 2]], 1);
        self::assertSame(['x' => 2], $r2->fetchAssociative(), 'the last column wins, as in PDO');
    }

    /**
     * A row whose arity disagrees with the header is a REFUSAL that DBAL can convert, not a raw
     * `ValueError` from `array_combine()`. `Doctrine\DBAL\Connection::executeQuery()` catches
     * exactly `Driver\Exception`, so an unwrapped error escapes DBAL's conversion entirely and
     * reaches the application past every `catch (Doctrine\DBAL\Exception)` (hazard 9).
     */
    public function testAMisframedRowIsARefusalDbalCanConvertNotARawValueError(): void
    {
        try {
            Result::buffered(['a', 'b'], [[1]], 1)->fetchAssociative();
            self::fail('a row whose arity disagrees with the header must be refused');
        } catch (DriverException $e) {
            self::assertInstanceOf(
                DoctrineDriverException::class,
                $e,
                'must be catchable as a DBAL driver exception, or it escapes conversion',
            );
            self::assertStringContainsString('1 cells but the header declared 2', $e->getMessage());
        }
    }
}
