<?php // /php/laravel/tests/Live/EscapeLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use PHPUnit\Framework\Attributes\DataProvider;

/**
 * `DB::escape()` / `Builder::toRawSql()` — the `FerroPdoShim::quote()` path.
 *
 * **The load-bearing assertion is the ROUND TRIP, not the string shape.** Comparing the quoted
 * output against a hand-written expectation only proves this driver agrees with itself; feeding the
 * literal back through `SELECT` and requiring the original BYTES proves it agrees with PostgreSQL's
 * parser, which is the only agreement that matters for an escaping function. Both are asserted, in
 * that order of importance.
 *
 * The corpus is chosen for the two shapes that break naive escaping under the wrong
 * `standard_conforming_strings`: a backslash immediately before a quote, and a quote immediately
 * before a backslash.
 */
final class EscapeLiveTest extends LaravelLiveTestCase
{
    /** @return list<array{0:string}> */
    public static function nastyStrings(): array
    {
        return [
            ["plain"],
            ["Hello'World"],
            ["'"],
            ["''"],
            ["back\\slash"],
            ["backslash-then-quote \\'"],
            ["quote-then-backslash '\\"],
            ["trailing backslash \\"],
            ["é — multibyte"],
            ["tab\tand newline\n"],
            ["'; DROP TABLE users; --"],
            ["\\'; DROP TABLE users; --"],
        ];
    }

    #[DataProvider('nastyStrings')]
    public function testAnEscapedLiteralParsesBackToTheOriginalBytes(string $raw): void
    {
        $conn = $this->connection();

        $literal = $conn->escape($raw);

        // (1) the shape: PostgreSQL's own rule under standard_conforming_strings=on.
        self::assertSame("'" . str_replace("'", "''", $raw) . "'", $literal);

        // (2) THE ONE THAT MATTERS: PostgreSQL's parser must give the bytes back unchanged, which is
        // what "correctly escaped" actually means. A rule that merely looked plausible would pass
        // (1) and fail here.
        $back = $conn->select("select {$literal} as v")[0]->v;
        self::assertSame($raw, $back, 'the literal did not parse back to the original bytes');
    }

    /**
     * An injection attempt must stay INSIDE the literal — one row, one column, the payload as data.
     * Asserted through a statement whose shape would visibly change if the quoting broke out.
     */
    public function testAnInjectionPayloadStaysInsideTheLiteral(): void
    {
        $conn = $this->connection();

        $payload = "x' || (select 'PWNED') || '";
        $rows = $conn->select('select ' . $conn->escape($payload) . ' as v');

        self::assertCount(1, $rows);
        self::assertSame($payload, $rows[0]->v, 'the payload must come back as DATA, unevaluated');
        // The NEGATIVE control, spelled as the value a SUCCESSFUL injection would produce: if the
        // quoting broke out, `select 'x' || (select 'PWNED') || '' as v` evaluates to `xPWNED`.
        // Asserting merely that the result lacks the substring "PWNED" is unfalsifiable here — the
        // payload contains that word itself, which is how the first version of this test failed.
        self::assertNotSame('xPWNED', $rows[0]->v, 'the literal was broken out of');
    }

    /** The non-string arms are Illuminate's own and must keep working through this connection. */
    public function testTheNonStringArmsAreUnchanged(): void
    {
        $conn = $this->connection();

        self::assertSame('null', $conn->escape(null));
        self::assertSame('42', $conn->escape(42));
        self::assertSame('true', $conn->escape(true));
        self::assertSame('false', $conn->escape(false));
        self::assertSame("'\\xdead00beef'::bytea", $conn->escape(hex2bin('dead00beef'), true));
    }

    /**
     * `toRawSql()` is the reason this matters beyond `DB::escape()`: it is what `->dd()` and every
     * query-log consumer render, so a refusing `quote()` made ordinary debugging throw.
     */
    public function testToRawSqlRendersBindings(): void
    {
        $conn = $this->connection();

        $sql = $conn->query()->selectRaw('?::text as v', ["O'Brien"])->toRawSql();

        self::assertStringContainsString("'O''Brien'", $sql);
    }
}
