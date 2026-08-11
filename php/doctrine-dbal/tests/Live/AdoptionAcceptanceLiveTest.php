<?php // /php/doctrine-dbal/tests/Live/AdoptionAcceptanceLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Schema\PostgreSQLSchemaManager;
use PDO;

/**
 * **M1-S8c Task 4 — the two adoption claims the acceptance re-measurement had to prove and that
 * nothing else in the tree covers.**
 *
 * `TextFallbackLiveTest` (Task 1) proves the READ half of D-S8b-6 — columns, indexes, the primary
 * key, and byte-identity with `pdo_pgsql`. Two things it does not cover are exactly what the
 * adoption claim rests on, so they live here:
 *
 * 1. **FOREIGN KEYS**, through the stock `PostgreSQLSchemaManager`. `doctrine/migrations` and every
 *    schema diff read them on every introspection, and the interesting fact — measured below and
 *    stated rather than glossed — is that `selectForeignKeyColumns()` alone never touches a
 *    non-canonical type. It is `introspectTable()` that does, because it also reads the indexes.
 *    So the FK assertions are made through BOTH doors, and only the second one is a fallback test.
 * 2. **THE MIRROR: a custom-OID value must ROUND TRIP.** D-S8b-6 states it as a rule — "a value read
 *    as text must be writable back as text, or a read → write-back round trip breaks". Reading is
 *    half a claim. Here the value is read out through the fallback, BOUND BACK IN as an ordinary
 *    PHP string parameter, and the stored result is verified by an INDEPENDENT `pdo_pgsql`
 *    connection — libpq, i.e. the thing the round trip has to agree with.
 *
 * Everything runs against a real `ferrod` on real PostgreSQL; {@see DbalLiveTestCase} asserts
 * `getNativeConnection() instanceof Ferro\Client\Connection` before any test body runs.
 */
final class AdoptionAcceptanceLiveTest extends DbalLiveTestCase
{
    /** A direct libpq connection to the SAME database — the independent oracle. */
    private function pdoOracle(): PDO
    {
        $url = getenv('FERRO_TEST_PG_URL');
        self::assertIsString($url);
        $p = parse_url($url);
        self::assertIsArray($p, "FERRO_TEST_PG_URL must be a URL: {$url}");
        $host = is_string($p['host'] ?? null) ? $p['host'] : '127.0.0.1';
        $port = is_int($p['port'] ?? null) ? $p['port'] : 5432;
        $db = ltrim(is_string($p['path'] ?? null) ? $p['path'] : '/ferro', '/');
        $user = is_string($p['user'] ?? null) ? $p['user'] : 'ferro';
        $pass = is_string($p['pass'] ?? null) ? $p['pass'] : 'ferro';

        $pdo = new PDO("pgsql:host={$host};port={$port};dbname={$db}", $user, $pass, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        self::assertSame('pgsql', $pdo->getAttribute(PDO::ATTR_DRIVER_NAME));
        return $pdo;
    }

    /**
     * **FOREIGN KEYS through the stock schema manager.**
     *
     * `listTableForeignKeys()` is asserted first and `introspectTable()` second, and the ORDER is
     * the point. MEASURED with the M1-S8c result-format policy removed (the mutation that restores
     * pre-S8c behaviour): `listTableForeignKeys` still PASSES — its query selects only
     * `pg_get_constraintdef` (text) and two booleans — while `introspectTable` DIES on
     * `pg_index.indkey`. So the honest claim is not "the fallback made FK introspection work"; it
     * is "the fallback made the WHOLE-TABLE introspection that carries the FKs work", which is the
     * call `doctrine/migrations` and `SchemaDiff` actually make.
     */
    public function testForeignKeysAreIntrospectedByTheStockSchemaManager(): void
    {
        $c = $this->dbal();
        $sm = $c->createSchemaManager();
        self::assertInstanceOf(
            PostgreSQLSchemaManager::class,
            $sm,
            'the schema manager must be the STOCK Doctrine one (charter rule 6)',
        );

        $this->dropFkFixture($c);
        $c->executeStatement(
            'CREATE TABLE s8c_t4_parent (id int NOT NULL, code varchar(16) NOT NULL,'
            . ' CONSTRAINT s8c_t4_parent_pkey PRIMARY KEY (id, code))',
        );
        $c->executeStatement(
            'CREATE TABLE s8c_t4_child ('
            . ' id int NOT NULL PRIMARY KEY,'
            . ' p_id int NOT NULL,'
            . ' p_code varchar(16) NOT NULL,'
            . ' CONSTRAINT s8c_t4_child_fk FOREIGN KEY (p_id, p_code)'
            . '   REFERENCES s8c_t4_parent (id, code) ON DELETE CASCADE ON UPDATE RESTRICT)',
        );
        $c->executeStatement('CREATE INDEX s8c_t4_child_idx ON s8c_t4_child (p_code, p_id)');

        // --- door 1: the FK-only query.
        $fks = $sm->listTableForeignKeys('s8c_t4_child');
        self::assertCount(1, $fks);
        $fk = $fks[0];
        self::assertSame('s8c_t4_child_fk', $fk->getName());
        // TWO columns, in this order — a composite FK, so an order bug is visible.
        self::assertSame(['p_id', 'p_code'], $fk->getLocalColumns());
        self::assertSame('s8c_t4_parent', $fk->getForeignTableName());
        self::assertSame(['id', 'code'], $fk->getForeignColumns());
        self::assertSame('CASCADE', $fk->getOption('onDelete'));

        // --- door 2: the whole-table introspection doctrine/migrations goes through. THIS is the
        //     one the fallback unblocked; it reads the indexes as well.
        $table = $sm->introspectTable('s8c_t4_child');
        self::assertCount(1, $table->getForeignKeys());
        $viaTable = array_values($table->getForeignKeys())[0];
        self::assertSame(['p_id', 'p_code'], $viaTable->getLocalColumns());
        self::assertSame(['id', 'code'], $viaTable->getForeignColumns());
        // …and the index whose column ORDER is decoded from the int2vector `indkey`.
        self::assertSame(['p_code', 'p_id'], $table->getIndex('s8c_t4_child_idx')->getColumns());
        self::assertNotNull($table->getPrimaryKey());
        self::assertSame(['id'], $table->getPrimaryKey()->getColumns());

        $this->dropFkFixture($c);
    }

    private function dropFkFixture(DbalConnection $c): void
    {
        $c->executeStatement('DROP TABLE IF EXISTS s8c_t4_child');
        $c->executeStatement('DROP TABLE IF EXISTS s8c_t4_parent');
    }

    /**
     * **THE MIRROR (D-S8b-6): read a custom-OID value as text, WRITE IT BACK, and land the same
     * bytes.**
     *
     * For each type: a row is seeded server-side with a literal cast, Ferro READS it (TEXT, via the
     * fallback), Ferro BINDS THAT STRING BACK into a second row of the same column type, and then
     * a direct `pdo_pgsql` connection reads the second row. `assertSame` on the two strings is a
     * BYTE comparison against libpq, so neither half can be blessed by the other:
     *
     * - if the read were wrong, row 2 would be written from wrong text and the oracle would differ;
     * - if the write were wrong, the oracle reads whatever actually landed and differs;
     * - if BOTH agreed on something wrong, PG itself would have had to accept and re-emit it, which
     *   for these types means it is right.
     *
     * A third check asks PostgreSQL to compare the two rows itself, so "round trip" is also the
     * database's opinion and not only a string comparison in PHP.
     *
     * The list is the one D-S8b-6 names — the extension types (PostGIS `geometry` when the image
     * has it, `hstore`, `ltree`, `citext`), a native ENUM, and the non-canonical builtins that share
     * the same code path.
     */
    public function testCustomOidValuesRoundTripThroughTheTextFallback(): void
    {
        $c = $this->dbal();
        $pdo = $this->pdoOracle();

        foreach (['hstore', 'ltree', 'citext'] as $ext) {
            $c->executeStatement("CREATE EXTENSION IF NOT EXISTS {$ext}");
        }
        $c->executeStatement('DROP TABLE IF EXISTS s8c_t4_rt');
        $c->executeStatement('DROP TYPE IF EXISTS s8c_t4_mood');
        $c->executeStatement("CREATE TYPE s8c_t4_mood AS ENUM ('sad','ok','happy')");

        /** @var array<string, array{0: string, 1: string}> $cases  label => [column type, literal] */
        $cases = [
            'hstore' => ['hstore', "'a=>1,b=>2'::hstore"],
            'ltree' => ['ltree', "'top.sub.leaf'::ltree"],
            'citext' => ['citext', "'MiXeD'::citext"],
            'enum' => ['s8c_t4_mood', "'happy'::s8c_t4_mood"],
            'interval' => ['interval', "'1 day 02:03:04'::interval"],
            'timetz' => ['timetz', "'12:34:56+02'::timetz"],
            'inet' => ['inet', "'10.0.0.1'::inet"],
            'cidr' => ['cidr', "'10.0.0.0/8'::cidr"],
            'macaddr' => ['macaddr', "'08:00:2b:01:02:03'::macaddr"],
            'int2vector' => ['int2vector', "'1 2'::int2vector"],
            'text[]' => ['text[]', "ARRAY['a','b']::text[]"],
            'bit' => ['bit(4)', "B'1011'"],
        ];

        // PostGIS is not in the stock `postgres:17` image. If this container HAS it, the case
        // D-S8b-6 names FIRST is proved directly; if not, the branch taken is PRINTED, so a fresh
        // container degrades loudly instead of silently dropping the spatial claim.
        $postgisSchema = $pdo->query(
            "SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace"
            . " WHERE e.extname = 'postgis'",
        )->fetchColumn();
        $hasPostgis = is_string($postgisSchema) && $postgisSchema !== '';
        if ($hasPostgis) {
            $cases = ['geometry' => [
                "{$postgisSchema}.geometry",
                "'SRID=4326;POINT(1 2)'::{$postgisSchema}.geometry",
            ]] + $cases;
        }

        $checked = 0;
        foreach ($cases as $label => [$colType, $literal]) {
            $c->executeStatement('DROP TABLE IF EXISTS s8c_t4_rt');
            $c->executeStatement("CREATE TABLE s8c_t4_rt (id int PRIMARY KEY, v {$colType})");
            $c->executeStatement("INSERT INTO s8c_t4_rt (id, v) VALUES (1, {$literal})");

            // READ through the fallback.
            $read = $c->fetchOne('SELECT v FROM s8c_t4_rt WHERE id = 1');
            self::assertIsString($read, "[{$label}] the fallback must hand back a string");

            // WRITE IT BACK — an ordinary bound PHP string, which is all stock Doctrine ever sends.
            $c->executeStatement('INSERT INTO s8c_t4_rt (id, v) VALUES (2, ?)', [$read]);

            // THE ORACLE: libpq reads row 2.
            $oracle = $pdo->query('SELECT v FROM s8c_t4_rt WHERE id = 2')->fetchColumn();
            self::assertSame(
                $read,
                $oracle,
                "[{$label}] the value written back through the fallback must read back BYTE-"
                . "identically through pdo_pgsql",
            );

            // …and PostgreSQL's own opinion that the two rows hold the same value.
            $same = $pdo->query(
                'SELECT (SELECT v FROM s8c_t4_rt WHERE id = 1)::text'
                . ' = (SELECT v FROM s8c_t4_rt WHERE id = 2)::text',
            )->fetchColumn();
            self::assertTrue(
                $same === true || $same === 't' || $same === 1 || $same === '1',
                "[{$label}] PostgreSQL itself must consider the seeded row and the written-back "
                . "row equal (got " . var_export($same, true) . ')',
            );
            $checked++;
        }

        // The sweep really ran. Without this an empty or truncated $cases reports green.
        self::assertSame(count($cases), $checked);
        self::assertGreaterThanOrEqual(
            12,
            $checked,
            'the round-trip list is 12 types plus PostGIS when present; a shorter sweep means the '
            . 'fixture list was truncated',
        );

        if ($hasPostgis) {
            print "\n  PostGIS: PRESENT — geometry ROUND TRIPPED (read as EWKB hex, bound back)\n";
        } else {
            print "\n  PostGIS: ABSENT from this image. The custom-oid round trip is still proved "
                . "by hstore, ltree, citext and a native enum — all extension/user-assigned oids.\n";
        }

        $c->executeStatement('DROP TABLE IF EXISTS s8c_t4_rt');
        $c->executeStatement('DROP TYPE IF EXISTS s8c_t4_mood');
    }
}
