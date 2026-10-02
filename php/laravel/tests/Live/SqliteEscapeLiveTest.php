<?php // /php/laravel/tests/Live/SqliteEscapeLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use PHPUnit\Framework\Attributes\DataProvider;

/**
 * `DB::escape()` / `toRawSql()` on a SQLite pool (M2-C1g). SQLite has no backslash escape mode, so
 * the engine advertises `literals_are_standard = true` for every SQLite pool and the rule is
 * doubling `'` — what `pdo_sqlite` emits and upstream's `Sqlite/EscapeTest` asserts. Before C1g the
 * pool advertised nothing and every one of these refused.
 */
final class SqliteEscapeLiveTest extends SqliteLiveTestCase
{
    /** @return list<array{0:string}> */
    public static function nastyStrings(): array
    {
        return [
            ["Hello'World"],
            ["'"],
            ['trailing backslash \\'],
            ["backslash-then-quote \\'"],
            ['é — multibyte ✓'],
            ["'; DROP TABLE users; --"],
        ];
    }

    #[DataProvider('nastyStrings')]
    public function testAnEscapedLiteralParsesBackAndEqualsPdoSqlite(string $raw): void
    {
        $conn = $this->sqliteConnection();
        $quoted = $conn->escape($raw);

        self::assertSame($raw, $conn->select("select {$quoted} as v")[0]->v);
        self::assertSame((new \PDO('sqlite::memory:'))->quote($raw), $quoted, 'pdo_sqlite renders this differently');
    }

    public function testUpstreamsSqliteExpectationAndToRawSql(): void
    {
        $conn = $this->sqliteConnection();

        self::assertSame("'Hello''World'", $conn->escape("Hello'World"));
        self::assertStringContainsString(
            "'O''Brien'",
            $conn->query()->selectRaw('? as v', ["O'Brien"])->toRawSql(),
        );
    }
}
