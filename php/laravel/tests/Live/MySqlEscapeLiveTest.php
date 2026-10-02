<?php // /php/laravel/tests/Live/MySqlEscapeLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use PHPUnit\Framework\Attributes\DataProvider;

/**
 * `DB::escape()` / `toRawSql()` on a MySQL-family pool (M2-C1g) — `FerroPdoShim::quote()`'s
 * backslash rule, which the pool's advertised `literals_are_standard = false` selects.
 *
 * Three agreements, in order of importance — the same discipline as {@see EscapeLiveTest}:
 *
 *  1. **The ROUND TRIP**: the literal fed back through `SELECT` over Ferro must give the original
 *     BYTES. That is what "correctly escaped" means, and it is checked against the server's own
 *     parser rather than against this driver's opinion of itself.
 *  2. **Safety in the OTHER mode too**: the shim quotes by the POOL's advertised rule, not the live
 *     session's, so the same literal must also stay one string in a session that turned
 *     `NO_BACKSLASH_ESCAPES` ON inside its own transaction — the reason `'` is doubled rather than
 *     backslash-escaped (§22.2 (cc)). There it may mis-render (`\\` reads as two characters), but
 *     it must never break out.
 *  3. **`pdo_mysql`'s output, byte for byte, for every input without a `'`** — the real driver's
 *     `quote()` on a connection to the SAME server. With a `'` the shim deliberately emits `''`
 *     where `pdo_mysql` emits `\'`; MySQL reads the two identically in the default mode.
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
        ];
    }

    private static ?\PDO $pdo = null;

    /** Stock `pdo_mysql` at the same server — the reference the shim must equal. */
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

    public function testThePoolAdvertisesThatBackslashesAreEscapes(): void
    {
        $info = $this->mysqlConnection()->getFerroConnection()->poolInfo();
        self::assertNotNull($info);
        self::assertSame('mysql', $info->kind);
        self::assertFalse(
            $info->literalsAreStandard,
            'a default sql_mode has no NO_BACKSLASH_ESCAPES, and the engine must say so rather than nil',
        );
    }

    #[DataProvider('nastyStrings')]
    public function testAnEscapedLiteralParsesBackAndEqualsPdoMysql(string $raw): void
    {
        $conn = $this->mysqlConnection();
        $quoted = $conn->getPdo()->quote($raw);

        $back = $conn->select("select {$quoted} as v")[0]->v;
        self::assertSame($raw, $back, 'the literal did not parse back to the original bytes');

        if (!str_contains($raw, "'")) {
            self::assertSame(self::pdoMysql()->quote($raw), $quoted, 'pdo_mysql renders this literal differently');
        } else {
            // The one deliberate divergence, asserted as the exact rewrite it is: pdo_mysql's `\'`
            // becomes `''` and NOTHING else changes. A rule that diverged anywhere else would fail.
            $pdo = self::pdoMysql()->quote($raw);
            self::assertSame(
                strtr(substr($pdo, 1, -1), ['\\\\' => '\\\\', "\\'" => "''"]),
                substr($quoted, 1, -1),
            );
        }
    }

    /**
     * THE CASE THE DOUBLED QUOTE EXISTS FOR. Inside one transaction (one pinned connection), the
     * application turns `NO_BACKSLASH_ESCAPES` on, then runs a literal the shim built by the pool's
     * BACKSLASH rule. Every byte of it must still be inside one string: the value read back is the
     * literal's own content with `''` collapsed — backslash escapes read literally, the quote intact.
     * Under `\'` the backslash would be ordinary there and the quote would END the literal; the
     * injection payload's `union` would then run and add a row.
     */
    #[DataProvider('nastyStrings')]
    public function testTheLiteralStaysOneStringInASessionThatTurnedNoBackslashEscapesOn(string $raw): void
    {
        $conn = $this->mysqlConnection();
        $quoted = $conn->getPdo()->quote($raw);
        $inner = str_replace("''", "'", substr($quoted, 1, -1));

        $rows = $conn->transaction(function ($c) use ($quoted): array {
            $c->statement("set session sql_mode = concat_ws(',', @@session.sql_mode, 'NO_BACKSLASH_ESCAPES')");
            return $c->select("select {$quoted} as v");
        });

        self::assertCount(1, $rows);
        self::assertSame($inner, $rows[0]->v, 'the literal was not read as one string under NO_BACKSLASH_ESCAPES');
    }

    /**
     * `DB::escape()` is Illuminate's path, which rejects NUL and invalid UTF-8 before `quote()` and
     * routes the non-string arms elsewhere; upstream's `MySql/EscapeTest` asserts exactly these.
     */
    public function testDbEscapeMatchesUpstreamsMySqlExpectations(): void
    {
        $conn = $this->mysqlConnection();

        // Upstream's MySql/EscapeTest expects pdo_mysql's `'Hello\'World'`; the shim doubles the
        // quote instead (§22.2 (cc)) — the same string to MySQL, and safe in both modes.
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
