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
 *  2. **`pdo_mysql`'s output, byte for byte**: the real driver's `quote()` on a connection to the
 *     SAME server. This is what makes the shim a drop-in for code that compares rendered SQL —
 *     upstream's own `MySql/EscapeTest` asserts `'Hello\'World'`.
 *  3. The server ADVERTISED the rule: a default `sql_mode` reports `false`, which is the arm this
 *     whole file exercises; a server configured with `NO_BACKSLASH_ESCAPES` would take the
 *     standard arm instead, and the unit tests cover it.
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

        self::assertSame(self::pdoMysql()->quote($raw), $quoted, 'pdo_mysql renders this literal differently');
    }

    /**
     * `DB::escape()` is Illuminate's path, which rejects NUL and invalid UTF-8 before `quote()` and
     * routes the non-string arms elsewhere; upstream's `MySql/EscapeTest` asserts exactly these.
     */
    public function testDbEscapeMatchesUpstreamsMySqlExpectations(): void
    {
        $conn = $this->mysqlConnection();

        self::assertSame("'Hello\\'World'", $conn->escape("Hello'World"));
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

        self::assertStringContainsString("'O\\'Brien'", $sql);
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
        self::assertStringContainsString("'O\\'Brien'", $log[0]['query']);
    }
}
