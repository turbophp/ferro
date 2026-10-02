<?php // /php/laravel/tests/Unit/PdoShimQuoteTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Unit;

use Ferro\Client\Connection as FerroClient;
use Ferro\Laravel\FerroPdoShim;
use Ferro\Protocol\PoolInfo;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The half of {@see FerroPdoShim::quote} the LIVE suite cannot reach: the exact form per family, the
 * refusals, and — the property M2-C1g rests on — that the output does NOT depend on the pool's
 * advertised `literals_are_standard`.
 *
 * History: C2f/C2g quoted by that bit and refused unless it was `true`; C1g first extended the same
 * design to MySQL, and its adversarial review broke it three ways (an app changing the mode inside
 * its own transaction, an operator changing the server's global mode under a cached bit, and an
 * `init_connect` that applies to fresh dials but not to reset connections), each a live breakout. A
 * pool-level bit cannot describe the session a statement runs on, so the rule no longer reads it
 * (SPEC §22.2 (cc)). `EscapeLiveTest`/`MySqlEscapeLiveTest`/`SqliteEscapeLiveTest` prove each form
 * against the servers' own parsers, in every escape mode.
 */
final class PdoShimQuoteTest extends TestCase
{
    /** A session whose HELLO_ACK advertised this pool. */
    private static function advertising(string $kind, ?bool $literalsAreStandard = true): FakeSession
    {
        $session = new FakeSession();
        $session->poolInfo = [new PoolInfo('main', $kind, null, $literalsAreStandard)];
        return $session;
    }

    private static function shim(string $kind, ?bool $literalsAreStandard = true): FerroPdoShim
    {
        return new FerroPdoShim(new FerroClient(self::advertising($kind, $literalsAreStandard), 'main'));
    }

    /**
     * @return iterable<string,array{string,string,string}> [family, input, the exact literal]
     */
    public static function forms(): iterable
    {
        foreach (['postgres', 'mysql', 'sqlite'] as $k) {
            // No backslash: doubling `'` is the whole rule, on every family and in every mode.
            yield "$k plain" => [$k, 'plain', "'plain'"];
            yield "$k quote" => [$k, "O'Brien", "'O''Brien'"];
            yield "$k no-backslash control characters stay raw" => [$k, "a\nb\"c", "'a\nb\"c'"];
        }
        // A backslash: a form whose meaning does not depend on the escape mode.
        yield 'postgres backslash' => ['postgres', 'a\\b', "E'a\\\\b'"];
        yield 'postgres backslash-then-quote' => ['postgres', "\\'", "E'\\\\'''"];
        yield 'postgres trailing backslash' => ['postgres', 'x\\', "E'x\\\\'"];
        yield 'mysql backslash' => ['mysql', 'a\\b', "_utf8mb4 X'615c62'"];
        yield 'mysql backslash-then-quote' => ['mysql', "\\'", "_utf8mb4 X'5c27'"];
        yield 'mysql multi-byte with backslash' => ['mysql', "é\\", "_utf8mb4 X'c3a95c'"];
        yield 'sqlite backslash is ordinary' => ['sqlite', "a\\'b", "'a\\''b'"];
    }

    #[DataProvider('forms')]
    public function testEachFamilyQuotesWithItsModeIndependentForm(string $kind, string $in, string $want): void
    {
        self::assertSame($want, self::shim($kind)->quote($in));
    }

    /**
     * THE PROPERTY. The same input gives the same literal whatever the pool advertised — `true`,
     * `false`, or unknown. The first C1g design (and C2g's PostgreSQL arm) chose the rule FROM this bit
     * and refused on `false`/`nil`; this is the test that goes red if a future change makes the output
     * depend on it again.
     */
    #[DataProvider('forms')]
    public function testTheAdvertisedQuotingBitDoesNotChangeTheLiteral(string $kind, string $in, string $want): void
    {
        foreach ([true, false, null] as $bit) {
            self::assertSame($want, self::shim($kind, $bit)->quote($in), 'advertised ' . var_export($bit, true));
        }
    }

    /** An unknown FAMILY refuses: there is no form this driver can vouch for. */
    public function testAnUnknownFamilyRefuses(): void
    {
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/family "mssql" is not one this driver knows/');
        self::shim('mssql')->quote('x');
    }

    /** No pool metadata at all is the same refusal — the family cannot be known. */
    public function testAbsentPoolMetadataRefuses(): void
    {
        $session = new FakeSession();
        $session->poolInfo = [];
        $shim = new FerroPdoShim(new FerroClient($session, 'main'));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/advertised no metadata/');
        $shim->quote('x');
    }

    /**
     * Quoting costs NO round trip at all — SPEC §21 D5's actual requirement. Asserted on the session's
     * own record of what it SENT, which is the only thing that can tell a cached round trip from none.
     */
    public function testQuotingSendsNothingOnTheWire(): void
    {
        $session = self::advertising('mysql');
        $shim = new FerroPdoShim(new FerroClient($session, 'main'));

        self::assertSame("'a'", $shim->quote('a'));
        self::assertSame("_utf8mb4 X'5c'", $shim->quote('\\'));
        self::assertSame([], $session->sent, 'quote() must not send a frame — SPEC §21 D5');
    }

    /** `PDO::PARAM_LOB` is a different literal shape entirely; it is refused, not silently ignored. */
    public function testABinaryParamTypeIsRefusedByName(): void
    {
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/PARAM_STR/');
        self::shim('postgres')->quote('x', \PDO::PARAM_LOB);
    }
}
