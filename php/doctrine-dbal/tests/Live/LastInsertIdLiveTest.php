<?php // /php/doctrine-dbal/tests/Live/LastInsertIdLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Ferro\DBAL\Exception\NoIdentityValue;

/**
 * M1-S8b Task 10 — `lastInsertId()`, and the honest answer on PostgreSQL.
 *
 * DBAL 4's SPI is `lastInsertId(): int|string` with NO sequence-name argument (that overload was
 * REMOVED in 4.0, which makes SPEC §14's "sequence-name argument supported for PG" unimplementable
 * — Task 14 amends it) and it must THROW when there is no identity value.
 *
 * On PostgreSQL there is never one: the protocol carries no such field, and Ferro refuses to
 * emulate it with `SELECT lastval()` because on a transaction-mode pool the follow-up runs on a
 * DIFFERENT connection and returns a silently wrong key. So PG throws, and the message names the
 * two working answers (`INSERT … RETURNING`, or the ORM's SEQUENCE identity strategy).
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

    public function testPostgresThrowsAndNamesTheAlternative(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_lid');
        $c->executeStatement('CREATE TABLE s8b_lid (id serial primary key, note text)');
        $c->executeStatement('INSERT INTO s8b_lid (note) VALUES (?)', ['a']);

        try {
            $c->lastInsertId();
            self::fail('PostgreSQL reports no generated key; the SPI requires a throw');
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            $prev = $e->getPrevious();
            self::assertInstanceOf(NoIdentityValue::class, $prev);
            self::assertStringContainsString('RETURNING', $prev->getMessage());
            self::assertStringContainsString('SEQUENCE', $prev->getMessage());
        }

        // …and the documented alternative genuinely works through the same driver.
        $id = $c->fetchOne('INSERT INTO s8b_lid (note) VALUES (?) RETURNING id', ['b']);
        self::assertIsInt($id);

        $c->executeStatement('DROP TABLE s8b_lid');
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
