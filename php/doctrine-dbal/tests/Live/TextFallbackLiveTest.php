<?php // /php/doctrine-dbal/tests/Live/TextFallbackLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Schema\PostgreSQLSchemaManager;
use PDO;

/**
 * **M1-S8c Task 1 — the TEXT FALLBACK, proved where it has to pay off (D-S8b-6).**
 *
 * The engine-side proof lives in `ferro-backend-pg`'s `pg_text_fallback_it.rs`. This file proves
 * the two things only the PHP tier can:
 *
 * 1. **The STOCK `PostgreSQLSchemaManager` introspects a real table end to end.** That is the
 *    actual goal of the slice, not a proxy for it: S8b measured that 50 of PostgreSQL's 78
 *    non-passing upstream-DBAL tests shared ONE cause — the stock manager selects `pg_index.indkey`
 *    (`int2vector`, OID 22), which the read path refused. `listTableIndexes`, `listTableColumns`
 *    and `introspectTable` are called on the untouched Doctrine class (charter rule 6: the schema
 *    managers stay stock), so nothing here can pass because of something Ferro-specific.
 *
 * 2. **The bytes are byte-identical to `pdo_pgsql`.** D-S8b-6's whole justification is that PG's own
 *    text output is "the exact bytes every existing Doctrine application is already written
 *    against". The oracle is therefore a REAL `PDO('pgsql:…')` connection to the same database,
 *    running the same SQL — not a literal, and not the engine's own idea of what libpq would say.
 *    `pdo_pgsql` IS libpq, so this is the definition, not an approximation.
 *
 * Both run against a real `ferrod` on real PostgreSQL (see {@see DbalLiveTestCase}, whose
 * `waitUntilReady` + `getNativeConnection()` contact assertion is the structural proof that these
 * numbers are not another in-memory-SQLite false green).
 */
final class TextFallbackLiveTest extends DbalLiveTestCase
{
    /**
     * A direct `pdo_pgsql` connection to the SAME upstream database, built from `FERRO_TEST_PG_URL`.
     * This is the oracle: whatever it returns is by definition what a Doctrine application on
     * `pdo_pgsql` sees today, because it is libpq.
     */
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
        // The oracle must be a genuine pdo_pgsql handle, not something that silently degraded.
        self::assertSame('pgsql', $pdo->getAttribute(PDO::ATTR_DRIVER_NAME));
        return $pdo;
    }

    /**
     * **(1) The actual goal.** The stock `PostgreSQLSchemaManager` reads a real table's columns,
     * indexes and full `Table` object.
     *
     * Every assertion is on DOCTRINE's parsed model — column names, the primary key's columns, the
     * two-column index's ORDER — rather than on a raw string. That matters: `indkey` reaching PHP
     * as the string `"1 2"` is necessary but not sufficient; the manager then splits it and looks
     * the attnums up, so getting the ordered column list back is what proves the whole path.
     *
     * Before this slice this method threw: `listTableIndexes` alone was unreachable, and with it
     * `doctrine/migrations` and every schema diff on PostgreSQL.
     */
    public function testTheStockPostgreSqlSchemaManagerIntrospectsARealTable(): void
    {
        $c = $this->dbal();
        $sm = $c->createSchemaManager();
        self::assertInstanceOf(
            PostgreSQLSchemaManager::class,
            $sm,
            'the schema manager must be the STOCK Doctrine one (charter rule 6)',
        );

        $c->executeStatement('DROP TABLE IF EXISTS s8c_schema');
        $c->executeStatement(
            'CREATE TABLE s8c_schema ('
            . ' id int NOT NULL,'
            . ' a int NOT NULL,'
            . ' b varchar(32) NOT NULL,'
            . ' note text,'
            . ' CONSTRAINT s8c_schema_pkey PRIMARY KEY (id))',
        );
        $c->executeStatement('CREATE INDEX s8c_schema_ab ON s8c_schema (a, b)');
        $c->executeStatement('CREATE UNIQUE INDEX s8c_schema_b_uq ON s8c_schema (b)');

        // --- listTableIndexes: THE method that `pg_index.indkey` blocked.
        $indexes = $sm->listTableIndexes('s8c_schema');
        self::assertArrayHasKey('s8c_schema_ab', $indexes);
        self::assertSame(
            ['a', 'b'],
            $indexes['s8c_schema_ab']->getColumns(),
            'the two-column index must come back with BOTH columns IN ORDER — that ordering is '
            . 'decoded from int2vector `indkey` ("1 2"), so a wrong or unreadable indkey shows up '
            . 'here and nowhere else',
        );
        self::assertFalse($indexes['s8c_schema_ab']->isUnique());
        self::assertTrue($indexes['s8c_schema_b_uq']->isUnique());
        self::assertSame(['b'], $indexes['s8c_schema_b_uq']->getColumns());

        $primary = $indexes['primary'] ?? null;
        self::assertNotNull($primary, 'the PRIMARY KEY must be introspected');
        self::assertSame(['id'], $primary->getColumns());
        self::assertTrue($primary->isPrimary());

        // --- listTableColumns.
        $columns = $sm->listTableColumns('s8c_schema');
        self::assertSame(
            ['id', 'a', 'b', 'note'],
            array_map(static fn ($col) => $col->getName(), array_values($columns)),
        );
        self::assertSame(32, $columns['b']->getLength());
        self::assertTrue($columns['b']->getNotnull());
        self::assertFalse($columns['note']->getNotnull());

        // --- introspectTable: the whole model in one call, which is what doctrine/migrations and
        //     every schema diff go through.
        $table = $sm->introspectTable('s8c_schema');
        self::assertSame('s8c_schema', $table->getName());
        self::assertCount(4, $table->getColumns());
        self::assertNotNull($table->getPrimaryKey());
        self::assertSame(['id'], $table->getPrimaryKey()->getColumns());
        self::assertSame(['a', 'b'], $table->getIndex('s8c_schema_ab')->getColumns());

        $c->executeStatement('DROP TABLE s8c_schema');
    }

    /**
     * **(2) Byte-identical to `pdo_pgsql`.** Each expression is evaluated twice — once through
     * ferrod + the Ferro DBAL driver, once through a direct libpq connection — and the two strings
     * must be identical.
     *
     * The list spans exactly the classes D-S8b-6 names: the catalog types the stock schema manager
     * reads, the still-non-canonical scalars, and CUSTOM (database-local) oids — a native enum, a
     * composite, and the extension-assigned `hstore`/`ltree`/`citext`.
     *
     * `assertSame` on strings is a BYTE comparison, and the oracle is fetched independently, so
     * this cannot pass by both sides agreeing on something wrong.
     */
    public function testFallbackValuesAreByteIdenticalToPdoPgsql(): void
    {
        $c = $this->dbal();
        $pdo = $this->pdoOracle();

        foreach (['hstore', 'ltree', 'citext'] as $ext) {
            $c->executeStatement("CREATE EXTENSION IF NOT EXISTS {$ext}");
        }
        $c->executeStatement('DROP TYPE IF EXISTS s8c_mood');
        $c->executeStatement('DROP TYPE IF EXISTS s8c_pt');
        $c->executeStatement("CREATE TYPE s8c_mood AS ENUM ('sad','ok','happy')");
        $c->executeStatement('CREATE TYPE s8c_pt AS (x int, y text)');

        $exprs = [
            // The catalog types the stock PostgreSQLSchemaManager reads.
            'int2vector' => '(SELECT indkey FROM pg_index ORDER BY indexrelid LIMIT 1)',
            'oidvector' => "(SELECT proargtypes FROM pg_proc WHERE proname = 'int4pl' LIMIT 1)",
            '_int2' => 'ARRAY[1,2]::int2[]',
            '_text' => "ARRAY['a','b']::text[]",
            '_oid' => 'ARRAY[1,2]::oid[]',
            '_aclitem' => "'{ferro=arwdDxt/ferro}'::aclitem[]",
            'regproc' => "'int4pl'::regproc",
            'xid' => "'42'::xid",
            // Still-non-canonical scalars.
            'interval' => "'1 day 02:03:04'::interval",
            'timetz' => "'12:34:56+02'::timetz",
            'inet' => "'10.0.0.1'::inet",
            'cidr' => "'10.0.0.0/8'::cidr",
            'macaddr' => "'08:00:2b:01:02:03'::macaddr",
            'money' => "'12.34'::money",
            'bit' => "B'1011'",
            // CUSTOM, database-local oids.
            'enum' => "'happy'::s8c_mood",
            'composite' => "ROW(1,'foo')::s8c_pt",
            'hstore' => "'a=>1,b=>2'::hstore",
            'ltree' => "'top.sub.leaf'::ltree",
            'citext' => "'MiXeD'::citext",
        ];

        // PostGIS is not in the `postgres:17` image. If the container HAS it, prove the case
        // D-S8b-6 names explicitly (a geometry must reach PHP as EWKB hex, which is what
        // longitude-one/doctrine-spatial parses out of a plain string); if not, say so.
        $hasPostgis = (int) $pdo
            ->query("SELECT count(*) FROM pg_extension WHERE extname = 'postgis'")
            ->fetchColumn() > 0;
        if ($hasPostgis) {
            $schema = (string) $pdo->query(
                "SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace"
                . " WHERE e.extname = 'postgis'",
            )->fetchColumn();
            $exprs['geometry'] = "'SRID=4326;POINT(1 2)'::{$schema}.geometry";
        }

        $checked = 0;
        foreach ($exprs as $label => $expr) {
            $oracle = $pdo->query("SELECT {$expr}")->fetchColumn();
            self::assertIsString(
                $oracle,
                "[{$label}] the pdo_pgsql oracle must return a non-NULL string for `{$expr}`",
            );
            $ferro = $c->fetchOne("SELECT {$expr}");
            self::assertSame(
                $oracle,
                $ferro,
                "[{$label}] `{$expr}` must reach PHP byte-identically to pdo_pgsql",
            );
            $checked++;
        }

        // MEASURED, not assumed: the sweep really compared every row. Without this, an $exprs that
        // silently became empty — or a loop body that stopped running — would report green.
        self::assertSame(
            count($exprs),
            $checked,
            'every expression must have been compared against the oracle',
        );
        self::assertGreaterThanOrEqual(
            20,
            $checked,
            'the D-S8b-6 list is 20 expressions plus PostGIS when present; a shorter sweep means '
            . 'the fixture list was truncated',
        );

        if ($hasPostgis) {
            $geom = $c->fetchOne('SELECT ' . $exprs['geometry']);
            self::assertSame('0101000020E6100000000000000000F03F0000000000000040', $geom);
            print "\n  PostGIS: PRESENT — geometry proved as EWKB hex through pdo_pgsql\n";
        } else {
            print "\n  PostGIS: ABSENT from this image (postgres:17 ships none). The custom-oid "
                . "class is still covered by the enum, the composite and hstore/ltree/citext.\n";
        }

        $c->executeStatement('DROP TYPE IF EXISTS s8c_mood');
        $c->executeStatement('DROP TYPE IF EXISTS s8c_pt');
    }

    /**
     * NULL through the fallback stays NULL in PHP — the same as `pdo_pgsql` — and does not become
     * the empty string. One byte apart on the wire (`-1` vs `0` length) and the classic text-format
     * confusion; asserted against the oracle so it cannot be blessed by hand.
     */
    public function testNullAndEmptyThroughTheFallbackMatchPdoPgsql(): void
    {
        $c = $this->dbal();
        $pdo = $this->pdoOracle();
        $c->executeStatement('CREATE EXTENSION IF NOT EXISTS ltree');

        self::assertNull($pdo->query('SELECT NULL::int2vector')->fetchColumn());
        self::assertNull($c->fetchOne('SELECT NULL::int2vector'));

        self::assertSame('', $pdo->query("SELECT ''::ltree")->fetchColumn());
        self::assertSame(
            '',
            $c->fetchOne("SELECT ''::ltree"),
            'a zero-length text payload is the EMPTY STRING in PHP, exactly as pdo_pgsql returns it',
        );
    }
}
