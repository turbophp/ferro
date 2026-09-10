<?php // /php/laravel/tests/Live/SchemaBuilderLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Illuminate\Database\Schema\Blueprint;

/**
 * **C2's gating verification, done BEFORE the suite runner is built.**
 *
 * Every test in `laravel/framework`'s `tests/Integration/Database/` uses `DatabaseMigrations`, whose
 * `refreshTestDatabase()` runs `artisan migrate:fresh`. On PostgreSQL that drives the STOCK schema
 * builder's `dropAllTables()`, which introspects the catalog and then issues `DROP TABLE … CASCADE`.
 * If that does not work through this tier, the whole suite cannot even reach its first assertion —
 * so it is verified here rather than discovered inside a half-built runner.
 *
 * This is not a hypothetical worry. The sibling Doctrine suite's single largest failure cluster —
 * 50 tests — was exactly a stock-schema-manager introspection gap (`pg_index.indkey` arriving as an
 * `int2vector` the PG read path had no mapping for). Stock introspection reaching for a catalog type
 * the tier cannot decode is a shape this project has already been bitten by once.
 *
 * The schema builder is INHERITED, not written: `PostgresConnection::getSchemaBuilder()` returns the
 * stock `PostgresBuilder`, and charter rule 6 keeps it that way. What is under test is whether the
 * execution layer beneath it carries what stock introspection asks for.
 */
final class SchemaBuilderLiveTest extends LaravelLiveTestCase
{
    public function testTheStockSchemaBuilderIsTheOneWeInherit(): void
    {
        $conn = $this->connection();

        self::assertInstanceOf(
            \Illuminate\Database\Schema\PostgresBuilder::class,
            $conn->getSchemaBuilder(),
            'the schema builder must be STOCK — charter rule 6 forbids replacing it',
        );
    }

    public function testCreateInspectAndDropThroughTheStockBuilder(): void
    {
        $conn = $this->connection();
        $schema = $conn->getSchemaBuilder();

        $schema->dropIfExists('c2_schema_probe');
        $schema->create('c2_schema_probe', static function (Blueprint $t): void {
            $t->id();
            $t->string('name');
            $t->timestamps();
        });

        self::assertTrue($schema->hasTable('c2_schema_probe'),
            'hasTable() introspects the catalog — the read path must carry what it selects');
        self::assertTrue($schema->hasColumn('c2_schema_probe', 'name'));

        $cols = $schema->getColumnListing('c2_schema_probe');
        self::assertContains('id', $cols);
        self::assertContains('created_at', $cols);

        $schema->drop('c2_schema_probe');
        self::assertFalse($schema->hasTable('c2_schema_probe'));
    }

    /**
     * **THE ONE THAT GATES C2.** `migrate:fresh` calls exactly this, and it is the step that must
     * work before a single upstream integration test can run.
     */
    public function testDropAllTablesWorksThroughTheTier(): void
    {
        $conn = $this->connection();
        $schema = $conn->getSchemaBuilder();

        $schema->dropIfExists('c2_drop_all_a');
        $schema->dropIfExists('c2_drop_all_b');
        $schema->create('c2_drop_all_a', static function (Blueprint $t): void {
            $t->id();
        });
        // A dependent FK, because `dropAllTables()` must handle ordering/CASCADE rather than
        // failing on the first constraint — a plain DROP TABLE loop would.
        $schema->create('c2_drop_all_b', static function (Blueprint $t): void {
            $t->id();
            $t->foreignId('a_id')->constrained('c2_drop_all_a');
        });

        self::assertTrue($schema->hasTable('c2_drop_all_a'));
        self::assertTrue($schema->hasTable('c2_drop_all_b'));

        $schema->dropAllTables();

        self::assertFalse($schema->hasTable('c2_drop_all_a'),
            'dropAllTables() is what migrate:fresh runs — if this fails, C2 cannot start');
        self::assertFalse($schema->hasTable('c2_drop_all_b'));
    }
}
