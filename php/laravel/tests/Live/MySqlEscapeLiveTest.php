<?php // /php/laravel/tests/Live/MySqlEscapeLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use PHPUnit\Framework\Attributes\DataProvider;

/**
 * `DB::escape()` / `toRawSql()` on a MySQL-family pool (M2-C1g) — `FerroPdoShim::quote()`'s
 * mode-independent forms: `'…'` with `'` doubled for a string with no backslash, and
 * `_utf8mb4 X'<hex>'` for one with a backslash (SPEC §22.2 (cc)).
 *
 * **The load-bearing assertion is the EXACT read-back in EVERY escape mode.** The shim does not know
 * the live session's `NO_BACKSLASH_ESCAPES` — the C1g review showed the pool's advertised bit can
 * describe a different session (an app's own `SET`, an operator's global change, `init_connect`), and
 * that a literal built from it broke out — so each literal must mean the same bytes whichever mode the
 * statement runs in. Both modes are driven here on one pinned connection; a rule that is merely
 * "safe" in the other mode (it mis-renders but stays one string) fails these, which is the point.
 */
final class MySqlEscapeLiveTest extends MySqlLiveTestCase
{
    /** @return list<array{0:string}> */
    public static function nastyStrings(): array
    {
        return [
            ['plain'],
            ["Hello'World"],
            ["'"],
            ["''"],
            ['back\\slash'],
            ["backslash-then-quote \\'"],
            ["quote-then-backslash '\\"],
            ['trailing backslash \\'],
            ['double "quotes"'],
            ["NUL \0 inside"],
            ["CR \r LF \n tab \t"],
            ["Ctrl-Z \x1a"],
            ['é — multibyte ✓'],
            ["'; DROP TABLE users; --"],
            ["\\'; DROP TABLE users; --"],
            // C1g review F1's breakout payload, and F3's GBK one (valid UTF-8).
            ["\\' union select 0x50574e4544 -- "],
            ["中\\' union select 0x58, 0x50574e4544 -- "],
        ];
    }

    private static ?\PDO $pdo = null;

    /** Stock `pdo_mysql` at the same server — the reference for inputs where the bytes must agree. */
    private static function pdoMysql(): \PDO
    {
        if (self::$pdo === null) {
            $u = parse_url((string) getenv('FERRO_TEST_MYSQL_URL'));
            self::assertIsArray($u);
            self::$pdo = new \PDO(
                sprintf(
                    'mysql:host=%s;port=%d;dbname=%s;charset=utf8mb4',
                    $u['host'] ?? '127.0.0.1',
                    $u['port'] ?? 3306,
                    ltrim($u['path'] ?? '/ferro', '/'),
                ),
                rawurldecode($u['user'] ?? ''),
                rawurldecode($u['pass'] ?? ''),
                [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION],
            );
        }
        return self::$pdo;
    }

    /**
     * The engine still advertises the probed session's bit truthfully (a default `sql_mode`: backslashes
     * ARE escapes) — it is simply no longer what `quote()` is built from.
     */
    public function testThePoolAdvertisesThatBackslashesAreEscapes(): void
    {
        $info = $this->mysqlConnection()->getFerroConnection()->poolInfo();
        self::assertNotNull($info);
        self::assertSame('mysql', $info->kind);
        self::assertFalse($info->literalsAreStandard);
    }

    /** The default mode (backslashes are escapes): exact read-back. */
    #[DataProvider('nastyStrings')]
    public function testAnEscapedLiteralReadsBackExactlyInTheDefaultMode(string $raw): void
    {
        $conn = $this->mysqlConnection();
        $quoted = $conn->getPdo()->quote($raw);

        $rows = $conn->select("select {$quoted} as v");
        self::assertCount(1, $rows, 'a broken-out literal would have produced a second row or an error');
        self::assertSame($raw, $rows[0]->v, 'the literal did not parse back to the original bytes');

        // With no backslash and no character pdo_mysql escapes, the bytes are pdo_mysql's own.
        if (strpbrk($raw, "\\'\"\0\n\r\x1a") === false) {
            self::assertSame(self::pdoMysql()->quote($raw), $quoted);
        }
    }

    /**
     * THE CASE THE PROPERTY EXISTS FOR, in the other direction: inside one transaction (one pinned
     * connection) the application turns `NO_BACKSLASH_ESCAPES` ON, and the same literal must still read
     * back as EXACTLY the same bytes — not merely stay one string.
     */
    #[DataProvider('nastyStrings')]
    public function testTheSameLiteralReadsBackExactlyWithNoBackslashEscapesOn(string $raw): void
    {
        $conn = $this->mysqlConnection();
        $quoted = $conn->getPdo()->quote($raw);

        $rows = $conn->transaction(function ($c) use ($quoted): array {
            $c->statement("set session sql_mode = concat_ws(',', @@session.sql_mode, 'NO_BACKSLASH_ESCAPES')");
            return $c->select("select {$quoted} as v");
        });

        self::assertCount(1, $rows);
        self::assertSame($raw, $rows[0]->v, 'the literal read differently under NO_BACKSLASH_ESCAPES');
    }

    /**
     * C1g review F3: under a GBK connection charset — reachable through a server's `init_connect` on a
     * fresh dial — byte-wise backslash escaping breaks out (`pdo_mysql` included), because `0x5c` can be
     * a GBK trail byte. The hex form puts no byte of the value in the SQL text, so it cannot.
     */
    public function testTheGbkBreakoutPayloadStaysOneRowUnderAGbkConnection(): void
    {
        $conn = $this->mysqlConnection();
        $payload = "中\\' union select 0x58, 0x50574e4544 -- ";
        $quoted = $conn->getPdo()->quote($payload);

        // A NUMBER, not the value: under `SET NAMES gbk` the server returns text in GBK, which the
        // engine (correctly) refuses as invalid UTF-8 in a TEXT column. A broken-out literal would
        // add a UNION arm of a different width — an error, not one row.
        $rows = $conn->transaction(function ($c) use ($quoted): array {
            $c->statement('set names gbk');
            return $c->select("select length({$quoted}) as n");
        });

        self::assertCount(1, $rows, 'the literal was broken out of under GBK');
        self::assertSame(strlen($payload), (int) $rows[0]->n, 'and it is the payload, byte for byte');
    }

    /**
     * `DB::escape()` is Illuminate's path, which rejects NUL and invalid UTF-8 before `quote()` and
     * routes the non-string arms elsewhere; upstream's `MySql/EscapeTest` asserts exactly these.
     */
    public function testDbEscapeMatchesUpstreamsMySqlExpectations(): void
    {
        $conn = $this->mysqlConnection();

        // Upstream's MySql/EscapeTest expects pdo_mysql's `'Hello\'World'`; the shim doubles the
        // quote instead (§22.2 (cc)) — the same string to MySQL in every mode.
        self::assertSame("'Hello''World'", $conn->escape("Hello'World"));
        self::assertSame("'2147483647'", $conn->escape('2147483647'));
        self::assertSame('null', $conn->escape(null));
        self::assertSame('42', $conn->escape(42));
        self::assertSame("x'dead00beef'", $conn->escape(hex2bin('dead00beef'), true));
    }

    /** An injection attempt stays INSIDE the literal, spelled with MySQL's backslash escape. */
    public function testAnInjectionPayloadStaysInsideTheLiteral(): void
    {
        $conn = $this->mysqlConnection();

        $payload = "x\\' union select 'PWNED' -- ";
        $rows = $conn->select('select ' . $conn->escape($payload) . ' as v');

        self::assertCount(1, $rows, 'a broken-out literal would have produced a second row');
        self::assertSame($payload, $rows[0]->v, 'the payload must come back as DATA, unevaluated');
    }

    /** `toRawSql()` — what `->dd()` and query-log consumers render — no longer throws on MySQL. */
    public function testToRawSqlRendersBindings(): void
    {
        $sql = $this->mysqlConnection()->query()->selectRaw('? as v', ["O'Brien"])->toRawSql();

        self::assertStringContainsString("'O''Brien'", $sql);
    }

    /**
     * `DB::pretend()` with a STRING binding: Illuminate renders each statement it did not run through
     * `substituteBindingsIntoRawSql()`, i.e. through `quote()`. C1f could only test `pretend()` with
     * an integer binding because this refused.
     */
    public function testPretendWithAStringBindingLogsTheRenderedStatement(): void
    {
        $conn = $this->mysqlConnection();

        $log = $conn->pretend(function ($c): void {
            $c->table('never_created')->insert(['name' => "O'Brien"]);
        });

        self::assertCount(1, $log);
        self::assertStringContainsString("'O''Brien'", $log[0]['query']);
    }
}
