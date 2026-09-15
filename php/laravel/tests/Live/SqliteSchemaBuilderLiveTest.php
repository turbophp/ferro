<?php // /php/laravel/tests/Live/SqliteSchemaBuilderLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Laravel\Schema\FerroSQLiteBuilder;

/**
 * C3-6b: `dropAllTables()` over a pooled connection — what `migrate:fresh` drives, and therefore
 * what every `DatabaseMigrations` test in the framework suite runs before its first assertion.
 */
final class SqliteSchemaBuilderLiveTest extends SqliteLiveTestCase
{
    public function testTheConnectionUsesFerrosSchemaBuilder(): void
    {
        self::assertInstanceOf(FerroSQLiteBuilder::class, $this->sqliteConnection()->getSchemaBuilder());
    }

    /**
     * The whole point. Upstream's `dropAllTables()` would take its `refreshDatabaseFile()` branch
     * (the database is not `:memory:`), truncate a file named after the config LABEL, leave every
     * table in place and report success — so this asserts the tables are GONE, not that the call
     * returned.
     *
     * Deleting `FerroSQLiteBuilder::dropAllTables()` fails this with the tables still present;
     * removing the transaction from `wipe()` fails it with SQLite's own `table sqlite_master may
     * not be modified`, because `PRAGMA writable_schema` is connection-scoped and each statement
     * would otherwise be its own checkout.
     */
    public function testDropAllTablesRemovesEveryTable(): void
    {
        $conn = $this->sqliteConnection();
        $conn->statement('create table a (id integer primary key autoincrement, v text)');
        $conn->statement('create index a_v on a (v)');
        $conn->statement('create table b (id integer primary key autoincrement)');
        self::assertCount(2, $this->tables($conn));

        $conn->getSchemaBuilder()->dropAllTables();

        self::assertSame([], $this->tables($conn), 'dropAllTables() left tables behind');
    }

    /** The same for views, which upstream runs through the identical four statements. */
    public function testDropAllViewsRemovesEveryView(): void
    {
        $conn = $this->sqliteConnection();
        $conn->statement('create table a (id integer primary key autoincrement, v text)');
        $conn->statement('create view av as select v from a');
        self::assertCount(1, $this->rowsOfType($conn, 'view'));

        $conn->getSchemaBuilder()->dropAllViews();

        self::assertSame([], $this->rowsOfType($conn, 'view'));
        self::assertCount(1, $this->tables($conn), 'dropAllViews() must not drop tables');
    }

    /**
     * A SAFETY refusal rather than a tidy-up: left reachable, `refreshDatabaseFile()` would
     * `file_put_contents()` whatever path the config label happens to name in the working
     * directory. It is unreachable through `dropAllTables()` now, so this pins the direct call.
     */
    public function testRefreshDatabaseFileRefuses(): void
    {
        $builder = $this->sqliteConnection()->getSchemaBuilder();
        self::assertInstanceOf(FerroSQLiteBuilder::class, $builder);

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/config label/');
        $builder->refreshDatabaseFile();
    }

    /** @return list<string> */
    private function tables(\Illuminate\Database\Connection $conn): array
    {
        return $this->rowsOfType($conn, 'table');
    }

    /** @return list<string> */
    private function rowsOfType(\Illuminate\Database\Connection $conn, string $type): array
    {
        $rows = $conn->select(
            "select name from sqlite_master where type = ? and name not like 'sqlite_%' order by name",
            [$type],
        );
        return array_values(array_map(static fn ($r): string => (string) $r->name, $rows));
    }
}
