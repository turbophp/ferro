<?php // /php/doctrine-dbal/tests/Live/LastInsertIdLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Driver\Exception\NoIdentityValue;

/**
 * M1-S8b Task 10 — `lastInsertId()`, and the honest answer on PostgreSQL.
 *
 * DBAL 4's SPI is `lastInsertId(): int|string` with NO sequence-name argument (that overload was
 * REMOVED in 4.0, which makes SPEC §14's "sequence-name argument supported for PG" unimplementable
 * — Task 14 amends it) and it must THROW when there is no identity value.
 *
 * On PostgreSQL the protocol carries no such field. OUTSIDE a transaction Ferro refuses to emulate
 * it with `SELECT lastval()`, because on a transaction-mode pool the follow-up runs on a DIFFERENT
 * connection and returns a silently wrong key — so it throws, naming the working answers. INSIDE one
 * the connection is pinned, so it runs `lastval()` exactly as `pdo_pgsql` does (SPEC §22.2 (ci)) —
 * which is what Doctrine ORM's identity generator needs, since the unit of work always inserts in a
 * transaction.
 */
final class LastInsertIdLiveTest extends DbalLiveTestCase
{
    public function testMysqlReportsTheGeneratedKey(): void
    {
        $c = $this->dbal($this->requireMysqlPool());
        $c->executeStatement('DROP TABLE IF EXISTS s8b_lid');
        $c->executeStatement('CREATE TABLE s8b_lid (id BIGINT AUTO_INCREMENT PRIMARY KEY, note VARCHAR(16))');

        $c->executeStatement('INSERT INTO s8b_lid (note) VALUES (?)', ['a']);
        $first = (int) $c->lastInsertId();
        self::assertGreaterThan(0, $first);

        $c->executeStatement('INSERT INTO s8b_lid (note) VALUES (?)', ['b']);
        self::assertSame($first + 1, (int) $c->lastInsertId());

        $c->executeStatement('DROP TABLE s8b_lid');
    }

    public function testPostgresThrowsOutsideATransactionAndNamesTheAlternatives(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_lid');
        $c->executeStatement('CREATE TABLE s8b_lid (id serial primary key, note text)');
        $c->executeStatement('INSERT INTO s8b_lid (note) VALUES (?)', ['a']);

        try {
            $c->lastInsertId();
            self::fail('outside a transaction PostgreSQL reports no generated key; the SPI requires a throw');
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            $prev = $e->getPrevious();
            // The SPI's own "no identity value" signal, per major: DBAL 4 defines
            // `NoIdentityValue`; DBAL 3 has no such class, and its driver throws a plain
            // `Driver\Exception` — never `false`, which ORM 2 would cast to the key 0 (M2-C5).
            self::assertInstanceOf(self::isDbal3() ? \Ferro\DBAL\Exception\DriverException::class : NoIdentityValue::class, $prev);
            self::assertStringContainsString('RETURNING', $prev->getMessage());
            self::assertStringContainsString('SEQUENCE', $prev->getMessage());
            self::assertStringContainsString('transaction', $prev->getMessage());
        }

        // …and the documented alternative genuinely works through the same driver.
        $id = $c->fetchOne('INSERT INTO s8b_lid (note) VALUES (?) RETURNING id', ['b']);
        self::assertIsInt($id);

        $c->executeStatement('DROP TABLE s8b_lid');
    }

    /**
     * INSIDE a transaction PostgreSQL answers with `lastval()` on the pinned connection — the key of
     * THIS insert, then the next one's. Under DBAL 3 the named form is `currval(name)`.
     */
    public function testPostgresAnswersInsideATransaction(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_lid3');
        $c->executeStatement('CREATE TABLE s8b_lid3 (id serial primary key, note text)');

        $c->beginTransaction();
        $c->executeStatement('INSERT INTO s8b_lid3 (note) VALUES (?)', ['a']);
        $first = (int) $c->lastInsertId();
        $c->executeStatement('INSERT INTO s8b_lid3 (note) VALUES (?)', ['b']);
        $second = (int) $c->lastInsertId();
        if (self::isDbal3()) {
            self::assertSame($second, (int) $c->lastInsertId('s8b_lid3_id_seq'), 'DBAL 3 named: currval');
        }
        $c->commit();

        self::assertSame(
            [$first, $second],
            array_map('intval', $c->fetchFirstColumn('SELECT id FROM s8b_lid3 ORDER BY id')),
            'the keys lastInsertId() answered are the rows the inserts created',
        );
        $c->executeStatement('DROP TABLE s8b_lid3');
    }

    /**
     * **The safety half.** `lastval()` is SESSION state, and a pooled session outlives its tenant —
     * so a transaction that lands on a RECYCLED connection must not answer with the PREVIOUS tenant's
     * `nextval()`. Hygiene's `DISCARD SEQUENCES` (§7.3) is what clears it. The test proves it got the
     * same backend (`pg_backend_pid()`) before asserting, so a fresh dial cannot make it pass for the
     * wrong reason; and on that backend `lastval()` must be UNDEFINED (`55000`), never the old value.
     */
    public function testPostgresNeverAnswersWithThePreviousTenantsSequenceValue(): void
    {
        $a = $this->dbal();
        $a->executeStatement('DROP SEQUENCE IF EXISTS s8b_lid_seq');
        $a->executeStatement('CREATE SEQUENCE s8b_lid_seq START 4242');
        $row = $a->fetchNumeric('SELECT nextval(\'s8b_lid_seq\'), pg_backend_pid()');
        self::assertIsArray($row);
        [$leaked, $pid] = [(int) $row[0], (int) $row[1]];
        self::assertSame(4242, $leaked);

        $b = $this->dbal();
        $same = false;
        for ($i = 0; $i < 20 && !$same; $i++) {
            $b->beginTransaction();
            $same = (int) $b->fetchOne('SELECT pg_backend_pid()') === $pid;
            if (!$same) {
                $b->rollBack();
            }
        }
        self::assertTrue($same, 'precondition: the transaction must land on the connection that ran nextval()');

        try {
            $key = $b->lastInsertId();
            self::fail("a recycled connection answered lastInsertId() with $key — the previous tenant's nextval()");
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            self::assertSame('55000', $e->getSQLState(), $e->getMessage());
        } finally {
            $b->rollBack();
        }
        $a->executeStatement('DROP SEQUENCE s8b_lid_seq');
    }

    /**
     * The key survives being read after a statement inside a TRANSACTION, which is where nearly
     * every real INSERT happens — the client propagates it up from the tx path deliberately.
     */
    public function testTheKeyIsVisibleInsideATransaction(): void
    {
        $c = $this->dbal($this->requireMysqlPool());
        $c->executeStatement('DROP TABLE IF EXISTS s8b_lid2');
        $c->executeStatement('CREATE TABLE s8b_lid2 (id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT) ENGINE=InnoDB');

        $c->beginTransaction();
        $c->executeStatement('INSERT INTO s8b_lid2 (n) VALUES (1)');
        $inTx = (int) $c->lastInsertId();
        self::assertGreaterThan(0, $inTx);
        $c->commit();

        self::assertSame($inTx, (int) $c->fetchOne('SELECT id FROM s8b_lid2 LIMIT 1'));
        $c->executeStatement('DROP TABLE s8b_lid2');
    }
}
