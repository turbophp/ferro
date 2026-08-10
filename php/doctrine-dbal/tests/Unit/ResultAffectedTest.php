<?php // /php/doctrine-dbal/tests/Unit/ResultAffectedTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Ferro\DBAL\Result;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 5, ADDED beyond the plan — `Result::rowCount()` is the TERMINAL's `affected`, never
 * `count($rows)`.
 *
 * The class docblock says exactly that, and MEASURED at this task nothing enforced it: replacing the
 * body with `count($this->rows)` left the whole package GREEN, unit and live alike (the live smoke
 * reads its affected count through `exec()`, which returns the terminal field directly and never
 * builds a `Result`). A docblock claim with no guard behind it is the defect species this project
 * keeps finding, so the claim gets a guard here rather than an IOU.
 *
 * The two numbers are DIFFERENT in the fixture by construction — 1 row, 9 affected — so neither
 * assertion can pass for the other's reason. Task 8 owns the full nine-method contract (including
 * the per-family SELECT `rowCount()` divergence) and may fold this in.
 */
final class ResultAffectedTest extends TestCase
{
    public function testRowCountIsTheTerminalAffectedNotTheNumberOfRows(): void
    {
        $r = Result::buffered(['id'], [[1]], 9);

        self::assertSame(9, $r->rowCount(), 'rowCount() is the terminal `affected`');
        self::assertSame([[1]], $r->fetchAllNumeric(), '…and the rows are a different quantity');
    }

    /** The mirror: an UPDATE that returns no rows still reports what it changed. */
    public function testAnUpdateWithNoRowsStillReportsItsAffectedCount(): void
    {
        $r = Result::buffered([], [], 3);

        self::assertSame(3, $r->rowCount());
        self::assertSame(0, $r->columnCount());
        self::assertFalse($r->fetchNumeric());
    }
}
