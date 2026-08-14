<?php // /php/doctrine-dbal/tests/Unit/FetchFirstColumnBooleanTest.php
declare(strict_types=1);

namespace Ferro\DBAL\Tests\Unit;

use Ferro\DBAL\Result;
use PHPUnit\Framework\TestCase;

/**
 * `fetchFirstColumn()` must not stop at a boolean `false` in column 0.
 *
 * Upstream's `FetchUtils::fetchFirstColumn()` is `while (($v = fetchOne()) !== false)`, and
 * `fetchOne()` returns `$row[0]`, so a real PHP `false` value is indistinguishable from
 * end-of-result. Bundled drivers escape it because PDO returns booleans as strings; Ferro decodes
 * `TAG_BOOL` to a real `bool`, which is what exposes the conflation. Found by the first-ever
 * Doctrine ORM functional-suite run (upstream GH9230) — silent wrong answers, never an exception.
 *
 * MUTATION that must make every case here RED: restore
 * `return FetchUtils::fetchFirstColumn($this);` in {@see Result::fetchFirstColumn}.
 */
final class FetchFirstColumnBooleanTest extends TestCase
{
    /** A `false` in the MIDDLE truncates the tail. */
    public function testAFalseInTheMiddleDoesNotTruncate(): void
    {
        $r = Result::buffered(['flag'], [[true], [false], [true]], 0);
        self::assertSame([true, false, true], $r->fetchFirstColumn());
    }

    /** A LEADING `false` is the worst shape: upstream returns an empty array. */
    public function testALeadingFalseDoesNotEmptyTheResult(): void
    {
        $r = Result::buffered(['flag'], [[false], [true], [false]], 0);
        self::assertSame([false, true, false], $r->fetchFirstColumn());
    }

    /**
     * The NULL case {@see Result::fetchOne}'s docblock reasons about was ALREADY correct and must
     * stay correct — `null` is not `false`, and it was never the failing shape. Without this, a
     * "fix" that special-cased NULL instead of the real cause would look complete.
     */
    public function testNullsStillSurvive(): void
    {
        $r = Result::buffered(['n'], [[null], [1], [null]], 0);
        self::assertSame([null, 1, null], $r->fetchFirstColumn());
    }

    /** Falsy-but-not-false values must be untouched, or the fix has over-reached. */
    public function testOtherFalsyValuesAreUnaffected(): void
    {
        $r = Result::buffered(['v'], [[0], [''], ['0'], [0.0], [false], ['x']], 0);
        self::assertSame([0, '', '0', 0.0, false, 'x'], $r->fetchFirstColumn());
    }

    /** An empty result is still an empty array, not a row of nulls. */
    public function testAnEmptyResultStaysEmpty(): void
    {
        self::assertSame([], Result::buffered(['flag'], [], 0)->fetchFirstColumn());
    }
}
