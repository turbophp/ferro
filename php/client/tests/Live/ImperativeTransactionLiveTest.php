<?php // /php/client/tests/Live/ImperativeTransactionLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Error\InvalidTransactionStateException;
use Ferro\Client\RetryPolicy;

/**
 * M1-S8a Task 9 — the IMPERATIVE transaction trio (`begin`/`commit`/`rollBack`) against a real
 * `ferrod` + Docker Postgres. This is the shape a Doctrine DBAL driver needs: three unrelated calls
 * with the caller's code in between, the caller owning the retry decision (charter rule 3), and
 * nesting emulated by the caller with `SAVEPOINT` SQL (the Task 7 passthrough).
 */
final class ImperativeTransactionLiveTest extends LiveTestCase
{
    public function testCommitPersistsAndRollbackDiscards(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8a_imp');
        $c->exec('CREATE TABLE s8a_imp (v int)');

        $c->begin();
        $this->assertTrue($c->inTransaction());
        $c->exec('INSERT INTO s8a_imp (v) VALUES (1)');
        $c->commit();
        $this->assertFalse($c->inTransaction());

        $c->begin();
        $c->exec('INSERT INTO s8a_imp (v) VALUES (2)');
        $c->rollBack();

        $this->assertSame([['v' => 1]], $c->query('SELECT v FROM s8a_imp ORDER BY v'));
        $c->exec('DROP TABLE IF EXISTS s8a_imp');
    }

    /**
     * THE property that makes the trio correct: while a transaction is open, every statement issued
     * through the SAME Connection must carry its tx_id. If it silently ran autocommit, the insert
     * below would survive the rollback — which is exactly the failure this asserts against.
     */
    public function testStatementsInsideAnImperativeTransactionAreScopedToIt(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8a_imp_scope');
        $c->exec('CREATE TABLE s8a_imp_scope (v int)');

        $c->begin();
        $c->exec('INSERT INTO s8a_imp_scope (v) VALUES (7)');
        // Visible INSIDE the transaction...
        $this->assertSame([['v' => 7]], $c->query('SELECT v FROM s8a_imp_scope'));
        $c->rollBack();
        // ...and gone after it.
        $this->assertSame([], $c->query('SELECT v FROM s8a_imp_scope'));
        $c->exec('DROP TABLE IF EXISTS s8a_imp_scope');
    }

    /** Doctrine's nested-transaction emulation, reached exactly the way DBAL reaches it. */
    public function testDoctrineStyleSavepointSqlWorksThroughTheImperativeApi(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8a_imp_sp');
        $c->exec('CREATE TABLE s8a_imp_sp (v int)');

        $c->begin();
        $c->exec('INSERT INTO s8a_imp_sp (v) VALUES (1)');
        $c->exec('SAVEPOINT DOCTRINE_1');
        $c->exec('INSERT INTO s8a_imp_sp (v) VALUES (2)');
        $c->exec('ROLLBACK TO SAVEPOINT DOCTRINE_1');
        $c->exec('RELEASE SAVEPOINT DOCTRINE_1');
        $c->commit();

        $this->assertSame([['v' => 1]], $c->query('SELECT v FROM s8a_imp_sp ORDER BY v'));
        $c->exec('DROP TABLE IF EXISTS s8a_imp_sp');
    }

    public function testMisuseIsLoud(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        // The LEAF class, not FerroException: the root passes for any Ferro error, including one
        // thrown by this test's own setup (hazard 68).
        $this->expectException(InvalidTransactionStateException::class);
        $c->commit(); // no open transaction
    }

    public function testTheClosureFormStillWorksAndRefusesToNest(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8a_imp_mix');
        $c->exec('CREATE TABLE s8a_imp_mix (v int)');

        $c->transaction(static function ($tx): void {
            $tx->exec('INSERT INTO s8a_imp_mix (v) VALUES (1)');
        });
        $this->assertSame([['v' => 1]], $c->query('SELECT v FROM s8a_imp_mix'));

        $c->begin();
        try {
            $c->transaction(static fn ($tx) => null);
            $this->fail('the closure form must refuse to nest inside an imperative transaction');
        } catch (InvalidTransactionStateException) {
            // expected — the leaf class, so this cannot pass because the closure form failed for
            // some unrelated reason.
        } finally {
            $c->rollBack();
        }
        $c->exec('DROP TABLE IF EXISTS s8a_imp_mix');
    }
}
