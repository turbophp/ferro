<?php // /php/laravel/tests/Live/SelectLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

/**
 * C1b's acceptance: an Illuminate connection resolved by `driver => 'ferro-pgsql'` executes
 * `select()` through `ferrod` against real PostgreSQL.
 *
 * Every test here inherits the contact assertion in {@see LaravelLiveTestCase::connection} — a
 * green run cannot mean "zero engine contact".
 */
final class SelectLiveTest extends LaravelLiveTestCase
{
    public function testSelectReturnsStdClassRowsInOrder(): void
    {
        $conn = $this->connection();

        $rows = $conn->select('select n from generate_series(1, 3) as n order by n');

        self::assertCount(3, $rows);
        self::assertContainsOnlyInstancesOf(\stdClass::class, $rows,
            'Illuminate\'s Processor and Eloquent both expect stdClass rows, not arrays');
        self::assertSame([1, 2, 3], array_map(static fn (\stdClass $r): int => $r->n, $rows));
    }

    public function testBindingsArePassedPositionally(): void
    {
        $conn = $this->connection();

        $rows = $conn->select('select $1::int + $2::int as sum', [40, 2]);

        self::assertSame(42, $rows[0]->sum);
    }

    /**
     * An empty result is an empty array, NOT null and not a row of nulls — Eloquent's `->first()`
     * and `->get()` both key on that.
     */
    public function testAnEmptyResultIsAnEmptyArray(): void
    {
        $conn = $this->connection();

        self::assertSame([], $conn->select('select 1 where false'));
    }

    /**
     * DUPLICATE COLUMN NAMES. `select 1 as id, 2 as id` is ordinary SQL (it falls out of any join
     * selecting both sides' keys), and the obvious `array_combine($cols, $row)` hydration silently
     * COLLAPSES it — returning a 1-property object where PDO's FETCH_OBJ returns the last value
     * under that name. This asserts the arity survives: the row still has one property, the LAST
     * value wins, and — the part `array_combine` gets wrong — no exception and no short row.
     */
    public function testDuplicateColumnNamesDoNotCollapseTheRow(): void
    {
        $conn = $this->connection();

        $rows = $conn->select('select 1 as id, 2 as id');

        self::assertCount(1, $rows);
        self::assertSame(2, $rows[0]->id, 'the later column wins, exactly as PDO FETCH_OBJ does');
    }

    /**
     * `pretending()` must short-circuit BEFORE the engine is touched — `php artisan migrate
     * --pretend` and `DB::pretend()` rely on it, and a tier that executed anyway would run
     * migrations for real.
     */
    public function testPretendingExecutesNothing(): void
    {
        $conn = $this->connection();

        $ran = $conn->pretend(static function ($c): void {
            $c->select('select 1 / 0');   // would raise SQLSTATE 22012 if it reached PostgreSQL
        });

        self::assertNotSame([], $ran, 'pretend() still logs the query it did not run');
    }
}
