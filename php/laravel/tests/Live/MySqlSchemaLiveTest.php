<?php // /php/laravel/tests/Live/MySqlSchemaLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Laravel\FerroMariaDbConnection;
use Ferro\Laravel\FerroMySqlConnection;
use Ferro\Laravel\Schema\FerroMariaDbBuilder;
use Ferro\Laravel\Schema\FerroMySqlBuilder;

/**
 * M2-C1f review: the MySQL family's schema builder, on the dedicated `laravel_tests` pool.
 *
 * F14 — foreign-key toggling is SESSION state, so outside a transaction it has no effect on a pooled
 * connection; stock `dropAllTables()` therefore failed 1451 on MariaDB on any parent whose name sorts
 * before its child, and dropping or truncating a referenced parent inside the toggles fails on both
 * servers. F12 — the `database` label is what `MySqlBuilder` queries `information_schema` with.
 */
final class MySqlSchemaLiveTest extends MySqlLiveTestCase
{
    private ?FerroMySqlConnection $c = null;

    protected function tearDown(): void
    {
        if ($this->c !== null) {
            $this->c->getSchemaBuilder()->dropAllTables();
        }
        parent::tearDown();
    }

    /** A parent named BEFORE its child — the order `dropAllTables()` drops in (by name). */
    private function parentAndChild(FerroMySqlConnection|FerroMariaDbConnection $c): void
    {
        $s = $c->getSchemaBuilder();
        $s->dropAllTables();
        $c->statement('create table categories (id int primary key)');
        $c->statement('create table products (id int primary key, category_id int, '
            . 'foreign key (category_id) references categories(id))');
        $c->table('categories')->insert(['id' => 1]);
        $c->table('products')->insert(['id' => 1, 'category_id' => 1]);
    }

    private function conn(): FerroMySqlConnection
    {
        $c = $this->schemaConnection();
        self::assertInstanceOf(FerroMySqlConnection::class, $c);
        return $this->c = $c;
    }

    public function testTheSchemaBuilderIsTheFerroOne(): void
    {
        self::assertInstanceOf(FerroMySqlBuilder::class, $this->conn()->getSchemaBuilder());
    }

    /**
     * `migrate:fresh`, `db:wipe` and `RefreshDatabase` all end here.
     *
     * THE CONTROL is the pin itself — an unpinned `SET` cannot reach the next statement — and it
     * drops the PARENT ALONE, deliberately. The first version dropped `categories, products` in one
     * statement, as `MySqlBuilder::dropAllTables()` does, and that control failed on MySQL 8.4 in
     * CI: MySQL accepts a single `DROP TABLE` naming a parent together with its child even with
     * checks ON, where MariaDB refuses it (1451, measured on 10.11). So stock `dropAllTables()`
     * itself fails only on MariaDB; the parent-alone drop is refused on both (MySQL 8.4 `3730`,
     * MariaDB `1451`), which is the shape `withoutForeignKeyConstraints(fn () => Schema::drop(…))`
     * produces on either server.
     */
    public function testDropAllTablesDropsAParentNamedBeforeItsChild(): void
    {
        $c = $this->conn();
        $this->parentAndChild($c);

        try {
            $c->statement('set foreign_key_checks=0');
            $c->statement('drop table `categories`');
            self::fail('the control: an unpinned SET cannot reach the DROP on a pooled connection');
        } catch (\Illuminate\Database\QueryException $e) {
            self::assertMatchesRegularExpression('/\b(1451|3730)\b/', $e->getMessage());
        }

        $c->getSchemaBuilder()->dropAllTables();
        self::assertSame([], $c->getSchemaBuilder()->getTables());
    }

    /** The seeder idiom: truncate a referenced parent with checks off. */
    public function testWithoutForeignKeyConstraintsReachesTheStatementsInside(): void
    {
        $c = $this->conn();
        $this->parentAndChild($c);
        $c->getSchemaBuilder()->withoutForeignKeyConstraints(function () use ($c): void {
            $c->table('categories')->truncate();
        });
        self::assertSame(0, $c->table('categories')->count());
        self::assertSame(1, (int) $c->selectOne('select @@foreign_key_checks as f')?->f, 'checks are on again');
    }

    /** Outside a transaction it could have no effect, so it refuses; inside one it works. */
    public function testABareDisableRefusesOutsideATransactionAndWorksInsideOne(): void
    {
        $c = $this->conn();
        $this->parentAndChild($c);
        try {
            $c->getSchemaBuilder()->disableForeignKeyConstraints();
            self::fail('a bare disable outside a transaction must refuse');
        } catch (\LogicException $e) {
            self::assertStringContainsString('withoutForeignKeyConstraints', $e->getMessage());
        }

        $c->transaction(function () use ($c): void {
            $c->getSchemaBuilder()->disableForeignKeyConstraints();
            $c->statement('delete from categories');
            $c->getSchemaBuilder()->enableForeignKeyConstraints();
        });
        self::assertSame(0, $c->table('categories')->count());
    }

    /**
     * F12: `hasTable()` asks `information_schema` for the LABEL's schema. A label naming another
     * database refuses before any introspection runs; the matching label (the control) works.
     */
    public function testAMismatchedDatabaseLabelRefusesAtTheSchemaBuilder(): void
    {
        $c = $this->conn();
        $c->getSchemaBuilder()->dropAllTables();
        $c->statement('create table c1f_present (id int)');
        self::assertTrue($c->getSchemaBuilder()->hasTable('c1f_present'), 'the control');

        $wrong = $this->mysqlConnection(self::SCHEMA_POOL, 'not_the_pools_database');
        try {
            $wrong->getSchemaBuilder()->hasTable('c1f_present');
            self::fail('a label that is not the pool\'s database must refuse');
        } catch (\LogicException $e) {
            self::assertStringContainsString('not_the_pools_database', $e->getMessage());
            self::assertStringContainsString(self::SCHEMA_DATABASE, $e->getMessage());
        }
    }

    /**
     * `ferro-mariadb` (Laravel 11's `mariadb` driver) brings its own builder; the same pinning
     * applies. Run against whichever server the lane has — the statements are common to both.
     */
    public function testFerroMariaDbHasTheSameSchemaBehaviour(): void
    {
        $this->conn()->getSchemaBuilder()->dropAllTables();
        $m = $this->schemaConnection('ferro-mariadb');
        self::assertInstanceOf(FerroMariaDbBuilder::class, $m->getSchemaBuilder());
        $this->parentAndChild($m);
        // The family's insert() on the MariaDB connection: a non-auto-increment key reports 0, as
        // pdo_mysql does, and the row is written.
        self::assertSame(0, $m->table('products')->insertGetId(['id' => 2, 'category_id' => 1]));
        self::assertSame(2, $m->table('products')->count());
        $m->getSchemaBuilder()->dropAllTables();
        self::assertSame([], $m->getSchemaBuilder()->getTables());
    }
}
