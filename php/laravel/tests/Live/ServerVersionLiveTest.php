<?php // /php/laravel/tests/Live/ServerVersionLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

/**
 * The version the tier reports must be one Illuminate's own `version_compare` branches read
 * correctly — asserted through a REAL connection to a real PostgreSQL, and then through the
 * observable consequence rather than the number.
 *
 * Mutation check for anyone touching this: drop the `ServerVersion::normalise` call in
 * {@see \Ferro\Laravel\FerroPdoShim::getAttribute} and both tests below go red — the second one on
 * the introspection result itself, which is the thing users actually lose.
 */
final class ServerVersionLiveTest extends LaravelLiveTestCase
{
    public function testTheReportedVersionCompareRightAgainstIlluminatesOwnThresholds(): void
    {
        $conn = $this->connection();

        $v = $conn->getServerVersion();

        // The engine is PostgreSQL >= 12 in every environment this suite runs in (testkit pins 17;
        // this container has 16), so BOTH of Illuminate's thresholds must answer "not older".
        self::assertFalse(
            version_compare($v, '12.0', '<'),
            "PostgresGrammar::compileColumns() branches on this: reported '$v'",
        );
        self::assertFalse(version_compare($v, '14.0', '<'), "reported '$v'");
        // ...and it is a version, not a banner: nothing before the first digit.
        self::assertMatchesRegularExpression('/^\d/', $v);
    }

    /**
     * THE CONSEQUENCE, not the number. Stock `PostgresGrammar::compileColumns()` selects
     * `'' as generated` instead of `a.attgenerated` when it believes the server is pre-12, so a
     * `GENERATED ALWAYS AS … STORED` column comes back looking like an ordinary writable one — a
     * wrong answer, from a query that succeeds, with no exception anywhere.
     */
    public function testAGeneratedColumnIsReportedAsGenerated(): void
    {
        $conn = $this->connection();
        $t = 'ferro_c2_generated_' . bin2hex(random_bytes(4));

        $conn->statement("create table \"$t\" (id int, doubled int generated always as (id * 2) stored)");
        try {
            $columns = $conn->getSchemaBuilder()->getColumns($t);
            $by = [];
            foreach ($columns as $c) {
                $by[$c['name']] = $c;
            }

            self::assertArrayHasKey('doubled', $by, 'the introspection query must see the column');
            // `PostgresProcessor::processColumns()` turns the raw `attgenerated` byte into this
            // shape: `['type' => 's' => 'stored', 'expression' => …]`, or NULL when the column is
            // ordinary. Under the pre-12 SQL every column arrives with `'' as generated`, so every
            // column reports `null` here — indistinguishable from "not generated".
            self::assertSame(
                'stored',
                $by['doubled']['generation']['type'] ?? null,
                "PostgreSQL reports a STORED generated column as attgenerated='s'; a null generation "
                . 'here means compileColumns() emitted its pre-12 SQL against a modern server',
            );
            self::assertNull(
                $by['id']['generation'],
                'and an ordinary column must still be reported as NOT generated',
            );
        } finally {
            $conn->statement("drop table if exists \"$t\"");
        }
    }
}
