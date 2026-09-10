<?php // /php/laravel/tests/Live/CursorLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Illuminate\Support\LazyCollection;

/** C1d: `cursor()` streams rather than buffers, and abandonment leaves the session usable. */
final class CursorLiveTest extends LaravelLiveTestCase
{
    public function testCursorYieldsEveryRowInOrder(): void
    {
        $conn = $this->connection();

        $seen = [];
        foreach ($conn->cursor('select n from generate_series(1, 2000) as n order by n') as $row) {
            $seen[] = $row->n;
        }

        self::assertCount(2000, $seen);
        self::assertSame([1, 2, 3], array_slice($seen, 0, 3));
        self::assertSame(2000, end($seen));
    }

    public function testCursorBindsParameters(): void
    {
        $conn = $this->connection();

        $rows = iterator_to_array($conn->cursor('select $1::int + $2::int as sum', [40, 2]));

        self::assertSame(42, $rows[0]->sum);
    }

    /**
     * The `LazyCollection` path, which is what `Model::lazy()` and `chunkById()` actually use.
     * `take(5)` abandons the underlying Generator after five rows — the shape below is tested for
     * wire safety, but this asserts the framework integration works at all.
     */
    public function testLazyCollectionOverTheCursor(): void
    {
        $conn = $this->connection();

        $first = LazyCollection::make(fn () => yield from $conn->cursor(
            'select n from generate_series(1, 5000) as n order by n',
        ))->take(5)->pluck('n')->all();

        self::assertSame([1, 2, 3, 4, 5], $first);
    }

    /**
     * **THE ABANDONMENT GUARD, and it asserts the NEXT query on purpose.**
     *
     * Breaking out of a cursor leaves unread DATA frames in flight. If the tier failed to `CANCEL`
     * and drain, those frames would sit on the session socket and the FOLLOWING request would read
     * them as its own reply — so the damage never appears on the abandoned query, only on the one
     * after it. A test that only checked `break` worked would pass against a desynced wire.
     *
     * The follow-up query is deliberately shaped so a stale frame cannot be mistaken for its answer:
     * a different column name and a value the abandoned stream never produced.
     */
    public function testBreakingOutOfACursorLeavesTheSessionUsable(): void
    {
        $conn = $this->connection();

        $taken = 0;
        foreach ($conn->cursor('select n from generate_series(1, 50000) as n order by n') as $row) {
            $taken++;
            if ($taken === 10) {
                break;
            }
        }
        self::assertSame(10, $taken, 'the break must actually stop the loop');

        $after = $conn->select("select 'still-here' as marker, 7 as seven");
        self::assertCount(1, $after, 'the session must still answer after an abandoned cursor');
        self::assertSame('still-here', $after[0]->marker,
            'a stale DATA frame from the abandoned stream would surface HERE, as a wrong answer');
        self::assertSame(7, $after[0]->seven);
    }

    /** The same property for a cursor dropped without `break` — a Generator that simply goes out of scope. */
    public function testADroppedCursorAlsoLeavesTheSessionUsable(): void
    {
        $conn = $this->connection();

        (function () use ($conn): void {
            $gen = $conn->cursor('select n from generate_series(1, 50000) as n order by n');
            $gen->current();          // force the open + first batch
            // $gen goes out of scope here, un-drained.
        })();

        $after = $conn->select("select 'alive' as marker");
        self::assertSame('alive', $after[0]->marker);
    }

    /**
     * NOT BUFFERING is the whole reason this method exists, so it is measured rather than assumed.
     * A buffered implementation of 200k rows would hold them all; a streaming one holds a window.
     */
    public function testCursorDoesNotBufferTheWholeResult(): void
    {
        $conn = $this->connection();

        $before = memory_get_usage(true);
        $count = 0;
        $peak = 0;
        foreach ($conn->cursor('select n, repeat(\'x\', 100) as pad from generate_series(1, 200000) as n') as $_row) {
            $count++;
            if ($count % 20000 === 0) {
                $peak = max($peak, memory_get_usage(true) - $before);
            }
        }

        self::assertSame(200000, $count, 'every row must still arrive');
        // 200k rows x ~100 bytes of payload is ~20 MB buffered. A window is a small fraction of it.
        self::assertLessThan(16 * 1024 * 1024, $peak,
            'cursor() must stream: holding 200k rows would dwarf this bound');
    }

    public function testPretendingYieldsNothingAndTouchesNothing(): void
    {
        $conn = $this->connection();

        $conn->pretend(static function ($c): void {
            foreach ($c->cursor('select 1 / 0 as boom') as $_r) {
                throw new \LogicException('pretending must yield no rows');
            }
        });

        // Reaching here at all is the assertion: a real execution would have raised 22012.
        self::assertTrue(true);
    }
}
