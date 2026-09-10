<?php // /php/laravel/tests/Unit/ServerVersionTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Unit;

use Ferro\Laravel\ServerVersion;
use PHPUnit\Framework\TestCase;

/**
 * The assertions that matter here are the `version_compare` OUTCOMES, not the strings.
 *
 * A test that only checked `normalise()` returns `'16.13 (…)'` would pass against a rule that still
 * compared wrong — the defect this class exists for was never a wrong-looking string, it was a
 * correct-looking string that `version_compare` reads as older than everything.
 */
final class ServerVersionTest extends TestCase
{
    /** The exact string `ferrod` advertised for PostgreSQL 16.13, captured live. */
    private const PG_BANNER = 'PostgreSQL 16.13 (Ubuntu 16.13-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, '
        . 'compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit';

    /**
     * THE DEFECT, stated as the thing that was actually wrong: the RAW banner compares as older than
     * every version Illuminate branches on. Both thresholds are real call sites —
     * `PostgresGrammar::compileColumns()` uses 12.0, upstream's own `SchemaBuilderTest
     * ::testGetAndDropTypes` uses 14.0.
     */
    public function testTheRawBannerCompareWrongWhichIsWhyThisClassExists(): void
    {
        self::assertTrue(version_compare(self::PG_BANNER, '12.0', '<'));
        self::assertTrue(version_compare(self::PG_BANNER, '14.0', '<'));
    }

    public function testTheNormalisedPostgresVersionCompareRight(): void
    {
        $v = ServerVersion::normalise(ServerVersion::KIND_POSTGRES, self::PG_BANNER);

        self::assertFalse(version_compare($v, '12.0', '<'), 'a PG 16 server is not pre-12');
        self::assertFalse(version_compare($v, '14.0', '<'), 'a PG 16 server is not pre-14');
        self::assertTrue(version_compare($v, '17.0', '<'), '...and it IS pre-17');
        self::assertStringStartsWith('16.13', $v);
    }

    /**
     * Only the leading product name goes. The packaging suffix is kept because there is no measured
     * reason to strip it and every extra rule is a chance to discard something load-bearing — see
     * the MySQL-family case below, which is exactly that.
     */
    public function testNothingButTheLeadingProductNameIsStripped(): void
    {
        self::assertSame(
            '16.13 (Ubuntu 16.13-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, '
            . 'compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit',
            ServerVersion::normalise(ServerVersion::KIND_POSTGRES, self::PG_BANNER),
        );
    }

    /**
     * The MySQL family passes through BYTE-IDENTICAL, and that is load-bearing rather than lazy:
     * MariaDB is distinguishable from MySQL only by the `-MariaDB-` substring, so any normalisation
     * that tidied it away would turn a MariaDB server into a MySQL one silently. `ferro/laravel`
     * registers no MySQL driver today; this pins the rule before one exists.
     */
    public function testTheMysqlFamilyStringIsUntouched(): void
    {
        foreach (['8.4.11', '11.8.8-MariaDB-ubu2404'] as $raw) {
            self::assertSame($raw, ServerVersion::normalise(ServerVersion::KIND_MYSQL, $raw));
        }
    }

    /** An unknown kind is passed through rather than guessed at — the rule is PG-specific. */
    public function testAnUnknownKindIsUntouched(): void
    {
        self::assertSame(self::PG_BANNER, ServerVersion::normalise('', self::PG_BANNER));
        self::assertSame(self::PG_BANNER, ServerVersion::normalise('sqlite', self::PG_BANNER));
    }
}
