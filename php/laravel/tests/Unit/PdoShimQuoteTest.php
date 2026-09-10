<?php // /php/laravel/tests/Unit/PdoShimQuoteTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Unit;

use Ferro\Client\Connection as FerroClient;
use Ferro\Laravel\FerroPdoShim;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * The half of {@see FerroPdoShim::quote} the LIVE suite structurally cannot reach.
 *
 * `EscapeLiveTest` proves the escaping is correct against PostgreSQL's own parser — and it caught
 * the escaping mutation (drop the `'`-doubling and 9 of its cases go red). It could NOT catch the
 * `standard_conforming_strings` verification being removed, because every server this project runs
 * against has it `on`, so deleting the check changes nothing observable there. MEASURED, and stated
 * rather than left as an assumption: that mutation was green live. This file is where it goes red.
 */
final class PdoShimQuoteTest extends TestCase
{
    /** A one-row, one-column TEXT `ExecOk`, the shape `SHOW standard_conforming_strings` returns. */
    private static function showValue(string $value): FakeSession
    {
        $session = new FakeSession();
        $session->push(
            FakeSession::execOk([
                'cols' => [['name' => 'standard_conforming_strings', 'tag' => C::TAG_TEXT]],
                'rows' => [[['tag' => C::TAG_TEXT, 'data' => $value]]],
                'affected' => 0,
                'last_insert_id' => null,
                'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
            ]),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        return $session;
    }

    public function testAnOnBackendQuotesByDoublingTheSingleQuote(): void
    {
        $shim = new FerroPdoShim(new FerroClient(self::showValue('on'), 'main'));

        self::assertSame("'Hello''World'", $shim->quote("Hello'World"));
    }

    /**
     * THE MUTATION TARGET. With `standard_conforming_strings = off` PostgreSQL reads `\` as an
     * escape, so a value ending in a backslash would consume the closing quote — doubling `'` alone
     * is not enough, and this driver does not implement the alternative. It refuses rather than
     * emitting a literal it cannot vouch for.
     */
    public function testAnOffBackendRefusesRatherThanEmittingAnUnsafeLiteral(): void
    {
        $shim = new FerroPdoShim(new FerroClient(self::showValue('off'), 'main'));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/standard_conforming_strings=off/');
        $shim->quote("anything");
    }

    /**
     * Anything that is not an unambiguous `on` refuses too — the check is written so that an
     * unreadable answer fails CLOSED. That shape is not theoretical: routing the probe through the
     * shim's `guard()` helper (which coerces its result for the boolean-returning PDO methods)
     * turned the row set into `true`, and this is the branch that caught it.
     */
    public function testAnUnreadableAnswerAlsoRefuses(): void
    {
        $session = new FakeSession();
        $session->push(
            FakeSession::execOk([
                'cols' => [['name' => 'standard_conforming_strings', 'tag' => C::TAG_TEXT]],
                'rows' => [],
                'affected' => 0,
                'last_insert_id' => null,
                'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
            ]),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $shim = new FerroPdoShim(new FerroClient($session, 'main'));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/\(unreadable\)/');
        $shim->quote('x');
    }

    /**
     * The probe is paid for ONCE. `Grammar::substituteBindingsIntoRawSql()` escapes every binding of
     * a query, so a per-call `SHOW` would turn one `toRawSql()` into N round trips. Asserted by
     * scripting only ONE reply and quoting twice — a second probe would find the script exhausted.
     */
    public function testTheProbeRunsOnlyOnce(): void
    {
        $shim = new FerroPdoShim(new FerroClient(self::showValue('on'), 'main'));

        self::assertSame("'a'", $shim->quote('a'));
        self::assertSame("'b'", $shim->quote('b'));
    }

    /** `PDO::PARAM_LOB` is a different literal shape entirely; it is refused, not silently ignored. */
    public function testABinaryParamTypeIsRefusedByName(): void
    {
        $shim = new FerroPdoShim(new FerroClient(self::showValue('on'), 'main'));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/PARAM_STR/');
        $shim->quote('x', \PDO::PARAM_LOB);
    }
}
