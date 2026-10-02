<?php // /php/laravel/tests/Unit/PdoShimQuoteTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Unit;

use Ferro\Client\Connection as FerroClient;
use Ferro\Laravel\FerroPdoShim;
use Ferro\Protocol\PoolInfo;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * The half of {@see FerroPdoShim::quote} the LIVE suite structurally cannot reach.
 *
 * `EscapeLiveTest` proves the escaping is correct against PostgreSQL's own parser — and it caught
 * the escaping mutation (drop the `'`-doubling and 9 of its cases go red). It could NOT catch the
 * quoting-rule verification being removed, because every server this project runs against reports
 * `literals_are_standard: true`, so deleting the check changes nothing observable there. MEASURED,
 * and stated rather than left as an assumption: that mutation was green live. This file is where it
 * goes red.
 *
 * M2-C2g moved the source of that verification from a cached `SHOW` to `HELLO_ACK`'s advertised
 * `literals_are_standard` (SPEC §21 D5 forbids the round trip the `SHOW` was), so these fixtures
 * script POOL METADATA rather than a query reply. The PROPERTY under test is unchanged: only an
 * unambiguous `true` may proceed.
 */
final class PdoShimQuoteTest extends TestCase
{
    /** A session whose HELLO_ACK advertised this pool's quoting rule. */
    private static function advertising(?bool $literalsAreStandard, string $kind = 'postgres'): FakeSession
    {
        $session = new FakeSession();
        $session->poolInfo = [new PoolInfo('main', $kind, $kind === 'postgres' ? 'PostgreSQL 17.10' : '8.4.11', $literalsAreStandard)];
        return $session;
    }

    public function testAnOnBackendQuotesByDoublingTheSingleQuote(): void
    {
        $shim = new FerroPdoShim(new FerroClient(self::advertising(true), 'main'));

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
        $shim = new FerroPdoShim(new FerroClient(self::advertising(false), 'main'));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/literals_are_standard=false on a postgres pool/');
        $shim->quote('anything');
    }

    /**
     * UNKNOWN refuses too, and this is the arm that actually happens in production: an engine that
     * could not probe a pool advertises `nil`, and a client must not read that as either answer.
     * Reading it as `false` would merely be over-strict; reading it as `true` would hand a caller an
     * escaping rule the server never confirmed.
     */
    public function testAnUnknownQuotingRuleAlsoRefuses(): void
    {
        $shim = new FerroPdoShim(new FerroClient(self::advertising(null), 'main'));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/literals_are_standard=NULL/');
        $shim->quote('x');
    }

    /**
     * M2-C1g: on a MySQL-family pool `false` is a rule this driver implements —
     * `mysql_real_escape_string()`'s, which `pdo_mysql` applies when backslashes are escapes (every
     * default `sql_mode`). Each case is the byte string `pdo_mysql` emits; `MySqlEscapeLiveTest`
     * proves the equality against the real driver and the round trip through a real server.
     *
     * @return iterable<string,array{string,string}>
     */
    public static function mysqlBackslashCases(): iterable
    {
        yield 'quote' => ["Hello'World", "'Hello\\'World'"];
        yield 'double quote' => ['say "hi"', "'say \\\"hi\\\"'"];
        yield 'backslash' => ['a\\b', "'a\\\\b'"];
        yield 'trailing backslash' => ['a\\', "'a\\\\'"];
        yield 'backslash then quote' => ["\\'", "'\\\\\\''"];
        yield 'NUL' => ["a\0b", "'a\\0b'"];
        yield 'LF and CR' => ["a\nb\rc", "'a\\nb\\rc'"];
        yield 'Ctrl-Z' => ["a\x1ab", "'a\\Zb'"];
        yield 'multi-byte untouched' => ['héllo — ✓', "'héllo — ✓'"];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('mysqlBackslashCases')]
    public function testAMySqlPoolWithBackslashEscapesQuotesAsPdoMysqlDoes(string $in, string $want): void
    {
        $shim = new FerroPdoShim(new FerroClient(self::advertising(false, 'mysql'), 'main'));

        self::assertSame($want, $shim->quote($in));
    }

    /**
     * Under `NO_BACKSLASH_ESCAPES` a MySQL pool advertises `true`, and the rule is the standard one —
     * a backslash must NOT be doubled there, or every value containing one would gain a character.
     */
    public function testAMySqlPoolWithStandardLiteralsDoublesTheQuoteOnly(): void
    {
        $shim = new FerroPdoShim(new FerroClient(self::advertising(true, 'mysql'), 'main'));

        self::assertSame("'it''s a\\b'", $shim->quote("it's a\\b"));
    }

    /** SQLite always advertises `true` (it has no backslash escape mode): doubling, nothing else. */
    public function testASqlitePoolDoublesTheQuoteOnly(): void
    {
        $shim = new FerroPdoShim(new FerroClient(self::advertising(true, 'sqlite'), 'main'));

        self::assertSame("'Hello''World\\'", $shim->quote("Hello'World\\"));
    }

    /** UNKNOWN refuses on the MySQL family too — `false` is honoured, `null` never is. */
    public function testAnUnknownRuleRefusesOnAMySqlPool(): void
    {
        $shim = new FerroPdoShim(new FerroClient(self::advertising(null, 'mysql'), 'main'));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/literals_are_standard=NULL on a mysql pool/');
        $shim->quote('x');
    }

    /** No pool metadata at all is the same refusal — there is nothing to have verified. */
    public function testAbsentPoolMetadataRefuses(): void
    {
        $session = new FakeSession();
        $session->poolInfo = [];
        $shim = new FerroPdoShim(new FerroClient($session, 'main'));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/no pool metadata/');
        $shim->quote('x');
    }

    /**
     * Quoting costs NO round trip at all — SPEC §21 D5's actual requirement, and the reason this
     * moved off the `SHOW`. `Grammar::substituteBindingsIntoRawSql()` escapes every binding of a
     * query, so even a once-cached probe was N-times-nothing but still one more than D5 allows.
     * Asserted on the session's own record of what it SENT, which is the only thing that can tell a
     * cached round trip from no round trip.
     */
    public function testQuotingSendsNothingOnTheWire(): void
    {
        $session = self::advertising(true);
        $shim = new FerroPdoShim(new FerroClient($session, 'main'));

        self::assertSame("'a'", $shim->quote('a'));
        self::assertSame("'b'", $shim->quote('b'));
        self::assertSame([], $session->sent, 'quote() must not send a frame — SPEC §21 D5');
    }

    /** `PDO::PARAM_LOB` is a different literal shape entirely; it is refused, not silently ignored. */
    public function testABinaryParamTypeIsRefusedByName(): void
    {
        $shim = new FerroPdoShim(new FerroClient(self::advertising(true), 'main'));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/PARAM_STR/');
        $shim->quote('x', \PDO::PARAM_LOB);
    }
}
