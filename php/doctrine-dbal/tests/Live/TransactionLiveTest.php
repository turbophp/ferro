<?php // /php/doctrine-dbal/tests/Live/TransactionLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

/**
 * M1-S8b Task 10 — the driver connection's most important internal invariant: while a transaction
 * is open, EVERY plain statement rides its pinned `tx_id`.
 *
 * DBAL nests transactions client-side. Only level 1 calls the driver's `beginTransaction()`; deeper
 * levels call `createSavepoint()`, which is `executeStatement($platform->createSavePoint($name))` —
 * an ORDINARY statement. On a transaction-mode pool a statement that did not carry the `tx_id`
 * would be checked out onto a DIFFERENT backend connection, so the `SAVEPOINT` would be created in
 * a session that knows nothing about the `BEGIN`, and `ROLLBACK TO` would fail or (worse) silently
 * roll back nothing.
 *
 * The test drives DBAL's real nesting API rather than issuing savepoint SQL by hand, because the
 * point is that the STOCK path works.
 */
final class TransactionLiveTest extends DbalLiveTestCase
{
    public function testDbalNestedTransactionsUseSavepointsOnThePinnedTransaction(): void
    {
        foreach ($this->families() as $kind => $pool) {
            $c = $this->dbal($pool);
            // NO `setNestTransactionsWithSavepoints(true)` — READ from dbal 4.4.4's source, not
            // assumed: in DBAL 4 savepoint nesting is UNCONDITIONAL (`beginTransaction()` calls
            // `createSavepoint()` for every level > 1), the setter is `@deprecated … removed in 5.0`
            // and THROWS when passed false. Calling it would configure nothing and would make this
            // guard fail on DBAL 5 for a reason that has nothing to do with the invariant.
            $c->executeStatement('DROP TABLE IF EXISTS s8b_tx');
            $c->executeStatement(
                $kind === 'postgres'
                    ? 'CREATE TABLE s8b_tx (id int primary key, n int)'
                    : 'CREATE TABLE s8b_tx (id INT PRIMARY KEY, n INT) ENGINE=InnoDB',
            );
            $c->executeStatement('INSERT INTO s8b_tx (id, n) VALUES (1, 0)');

            $c->beginTransaction();                       // level 1 -> the driver's beginTransaction
            $c->executeStatement('UPDATE s8b_tx SET n = 1 WHERE id = 1');

            $c->beginTransaction();                       // level 2 -> SAVEPOINT, as ordinary SQL
            $c->executeStatement('UPDATE s8b_tx SET n = 2 WHERE id = 1');
            self::assertSame(2, (int) $c->fetchOne('SELECT n FROM s8b_tx WHERE id = 1'), "[$kind] inner write visible");
            $c->rollBack();                               // level 2 -> ROLLBACK TO SAVEPOINT

            self::assertSame(
                1,
                (int) $c->fetchOne('SELECT n FROM s8b_tx WHERE id = 1'),
                "[$kind] the savepoint rollback undid ONLY the inner write — proving the SAVEPOINT "
                . 'was created on the same pinned connection as the BEGIN',
            );

            $c->commit();                                 // level 1 -> the driver's commit
            self::assertSame(1, (int) $c->fetchOne('SELECT n FROM s8b_tx WHERE id = 1'), "[$kind] committed");

            $c->executeStatement('DROP TABLE s8b_tx');
        }
    }

    /** A rollback at level 1 really reaches the engine, on both families. */
    public function testTopLevelRollbackDiscardsEverything(): void
    {
        foreach ($this->families() as $kind => $pool) {
            $c = $this->dbal($pool);
            $c->executeStatement('DROP TABLE IF EXISTS s8b_tx2');
            $c->executeStatement(
                $kind === 'postgres'
                    ? 'CREATE TABLE s8b_tx2 (id int primary key)'
                    : 'CREATE TABLE s8b_tx2 (id INT PRIMARY KEY) ENGINE=InnoDB',
            );

            $c->beginTransaction();
            $c->executeStatement('INSERT INTO s8b_tx2 (id) VALUES (1)');
            self::assertSame(1, (int) $c->fetchOne('SELECT count(*) FROM s8b_tx2'), "[$kind] visible inside");
            $c->rollBack();
            self::assertSame(0, (int) $c->fetchOne('SELECT count(*) FROM s8b_tx2'), "[$kind] gone after rollback");

            $c->executeStatement('DROP TABLE s8b_tx2');
        }
    }

    /**
     * `autoCommit = false` makes DBAL open a transaction at connect and re-open one after every
     * commit. It is LEGAL under Ferro and it pins a backend connection for the whole request — i.e.
     * it turns the engine's central win off. Asserted here so the behaviour is known and recorded,
     * and listed in `docs/known-incompatibilities.md`.
     *
     * **The mode is set through `Doctrine\DBAL\Configuration`, not through
     * `Connection::setAutoCommit()`, and that is a MEASURED correction to this task's plan.** The
     * `beginTransaction()` that "opens a transaction at connect" lives in
     * `Doctrine\DBAL\Connection::connect()`, which returns early once `$this->_conn !== null`;
     * `setAutoCommit(false)` on an ALREADY-CONNECTED connection only commits transactions that are
     * already open (nesting level 0 here), so it opens nothing and the first `commit()` raises
     * `NoActiveTransaction` — measured exactly that way before this shape replaced it. Since
     * {@see DbalLiveTestCase::dbal} connects (its contact assertion calls `getNativeConnection()`),
     * the only way to observe the documented behaviour is to configure the mode BEFORE the first
     * connect.
     *
     * The pin is asserted from the DATABASE's own vantage point: `pg_current_xact_id()` is constant
     * across statements of ONE transaction and different for each autocommit statement. A
     * client-side `inTransaction()` alone would only say what the client believes.
     */
    public function testAutoCommitFalseOpensATransactionAtConnectAndPinsIt(): void
    {
        $c = $this->dbalNoAutoCommit();
        $native = $c->getNativeConnection();

        // Nothing has been executed yet: connecting alone opened a transaction.
        self::assertSame(1, $c->getTransactionNestingLevel(), 'a transaction is open before any statement');
        self::assertTrue($native->inTransaction(), 'and the client holds a pinned tx_id for it');

        $xid = $c->fetchOne('SELECT pg_current_xact_id()::text');
        self::assertSame(
            $xid,
            $c->fetchOne('SELECT pg_current_xact_id()::text'),
            'two statements under autoCommit=false share ONE backend transaction — the pin',
        );

        $c->executeStatement('DROP TABLE IF EXISTS s8b_tx3');
        $c->executeStatement('CREATE TABLE s8b_tx3 (id int primary key)');
        $c->commit();

        // …and the pin never lets go: DBAL immediately re-opens a transaction after the commit.
        self::assertSame(1, $c->getTransactionNestingLevel(), 'DBAL re-opened one straight away');
        self::assertTrue($native->inTransaction(), 'so the backend connection is still pinned');

        $c->executeStatement('INSERT INTO s8b_tx3 (id) VALUES (1)');
        $c->commit();
        self::assertSame(1, (int) $c->fetchOne('SELECT count(*) FROM s8b_tx3'));

        $c->executeStatement('DROP TABLE s8b_tx3');
        $c->commit();

        // THE MIRROR. Without it, "the transaction is open / the xid is stable" cannot be
        // distinguished from a connection that always looks that way.
        $plain = $this->dbal();
        self::assertSame(0, $plain->getTransactionNestingLevel(), 'an ordinary connection opens nothing');
        self::assertFalse($plain->getNativeConnection()->inTransaction());
        self::assertNotSame(
            $plain->fetchOne('SELECT pg_current_xact_id()::text'),
            $plain->fetchOne('SELECT pg_current_xact_id()::text'),
            'under autocommit each statement is its own transaction — nothing is pinned',
        );
    }

    /**
     * A connection whose `autoCommit` is off BEFORE it ever connects — the only shape in which
     * Doctrine's "open a transaction at connect" branch is reachable (see the test above). It
     * repeats `dbal()`'s contact assertion rather than skipping it: a connection that quietly fell
     * back to another driver would make every assertion here pass.
     */
    private function dbalNoAutoCommit(): \Doctrine\DBAL\Connection
    {
        $config = new \Doctrine\DBAL\Configuration();
        $config->setAutoCommit(false);
        $c = \Doctrine\DBAL\DriverManager::getConnection([
            'driverClass' => \Ferro\DBAL\Driver::class,
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => 'default'],
        ], $config);
        self::assertInstanceOf(
            \Ferro\Client\Connection::class,
            $c->getNativeConnection(),
            'this DBAL connection is not a Ferro one — the test would be measuring the wrong engine',
        );
        return $c;
    }

    /** @return array<string,string> */
    private function families(): array
    {
        return ['postgres' => 'default', 'mysql' => $this->requireMysqlPool()];
    }
}
