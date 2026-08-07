<?php // /php/client/tests/Live/LastInsertIdLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

/**
 * M1-S8a Task 2 — the generated key, end to end: MySQL OK packet → `QueryResult.last_insert_id` →
 * `ExecOk.last_insert_id` on the wire → `ExecCodec::decode` → {@see \Ferro\Client\Connection::lastInsertId}.
 *
 * Drives BOTH pools of the two-pool harness in one process, which is the point: the MySQL case
 * proves the value arrives, the PG case proves it is NOT invented. Neither is emulated with a
 * follow-up query — on a transaction-mode pool that lands on another connection (measured:
 * `SELECT LAST_INSERT_ID()` → 0; `SELECT lastval()` → SQLSTATE 55000, or the WRONG session's value
 * once that session has used a sequence).
 */
final class LastInsertIdLiveTest extends LiveTestCase
{
    public function testMysqlInsertReportsAnAdvancingLastInsertId(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->connectConnection(null, $pool);

        // A persistent table, NOT a TEMPORARY one: a transaction-mode pool sends each autocommit
        // statement on whichever connection is free, and a MySQL temp table is per-connection.
        $c->exec('DROP TABLE IF EXISTS s8a_lid_php');
        $c->exec('CREATE TABLE s8a_lid_php (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)');
        $this->assertNull($c->lastInsertId(), 'DDL reports no generated key');

        $c->exec('INSERT INTO s8a_lid_php (v) VALUES (?)', [1]);
        $first = $c->lastInsertId();
        $this->assertIsInt($first);
        $this->assertGreaterThan(0, $first);

        $c->exec('INSERT INTO s8a_lid_php (v) VALUES (?)', [2]);
        $this->assertSame($first + 1, $c->lastInsertId(), 'AUTO_INCREMENT must advance');

        // A read must not leave a stale key behind.
        $c->query('SELECT v FROM s8a_lid_php ORDER BY id');
        $this->assertNull($c->lastInsertId(), 'a SELECT reports no generated key');

        $c->exec('DROP TABLE IF EXISTS s8a_lid_php');
    }

    public function testPostgresReportsNullAndRetainsTheReturningRow(): void
    {
        $c = $this->connectConnection();
        $c->exec('DROP TABLE IF EXISTS s8a_lid_pg');
        $c->exec('CREATE TABLE s8a_lid_pg (id serial PRIMARY KEY, v int)');

        $rows = $c->query('INSERT INTO s8a_lid_pg (v) VALUES (1) RETURNING id');
        $this->assertNull(
            $c->lastInsertId(),
            'PG has no LAST_INSERT_ID protocol field — RETURNING is the documented route',
        );
        $this->assertIsInt($rows[0]['id']);

        $c->exec('DROP TABLE IF EXISTS s8a_lid_pg');
    }
}
