<?php // /php/client/tests/Live/BeginIsolationLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\RetryPolicy;
use Ferro\Protocol\Isolation;

/**
 * M1-S8b Task 3, live: the isolation byte really changes the transaction, on both engine families.
 *
 * PostgreSQL exposes the level of the CURRENTLY OPEN transaction through
 * `current_setting('transaction_isolation')`, so it can be asserted directly. MySQL cannot be
 * asserted that way — `SET TRANSACTION …` (the non-SESSION form the engine emits, deliberately) is
 * not reflected in `@@transaction_isolation`, which keeps reporting the session default with a
 * HYPHEN (`REPEATABLE-READ`). SPEC §22.2 (s) records that trap in full. The MySQL half therefore
 * asserts the level took by its EFFECT: under SERIALIZABLE a plain `SELECT` becomes a locking read,
 * so a row read inside the transaction cannot be updated by a second connection until it commits.
 */
final class BeginIsolationLiveTest extends LiveTestCase
{
    public function testPostgresBeginCarriesTheRequestedLevel(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());

        $c->begin(false, Isolation::Serializable);
        self::assertSame('serializable', $c->scalar("SELECT current_setting('transaction_isolation')"));
        $c->commit();

        $c->begin(false, Isolation::RepeatableRead);
        self::assertSame('repeatable read', $c->scalar("SELECT current_setting('transaction_isolation')"));
        $c->commit();

        // The absent case must NOT be silently coerced to a level — it must be the pool default.
        $c->begin();
        self::assertSame('read committed', $c->scalar("SELECT current_setting('transaction_isolation')"));
        $c->commit();
    }

    public function testMysqlSerializableTurnsAPlainSelectIntoALockingRead(): void
    {
        $pool = $this->requireMysqlPool();
        $a = $this->connectConnection(RetryPolicy::none(), $pool);
        $b = $this->connectConnection(RetryPolicy::none(), $pool);

        $a->exec('DROP TABLE IF EXISTS s8b_iso');
        $a->exec('CREATE TABLE s8b_iso (id INT PRIMARY KEY, v INT) ENGINE=InnoDB');
        $a->exec('INSERT INTO s8b_iso (id, v) VALUES (1, 1)');

        $a->begin(false, Isolation::Serializable);
        self::assertSame(1, $a->scalar('SELECT v FROM s8b_iso WHERE id = 1'));

        // Connection B must now BLOCK and time out (1205), because A's plain SELECT took a shared
        // lock under SERIALIZABLE. Under the pool default (REPEATABLE READ) this update succeeds.
        //
        // `innodb_lock_wait_timeout` is set on B's OWN checkout, inside B's own transaction: a bare
        // `SET SESSION` before an autocommit UPDATE would land on whichever pooled connection the
        // NEXT checkout hands out (a non-local SET taints but does not PIN — SPEC §7.4/§22.2 (s)),
        // so the UPDATE could easily run on a connection that still has the 50 s default and this
        // test would hang for 50 s instead of failing in 1.
        $blocked = false;
        $b->begin();
        try {
            $b->exec('SET SESSION innodb_lock_wait_timeout = 1');
            $b->exec('UPDATE s8b_iso SET v = 2 WHERE id = 1');
        } catch (\Ferro\Client\Error\RetryableException $e) {
            $blocked = true;
            self::assertSame(1205, $e->errno(), 'lock wait timeout is MySQL errno 1205');
        } finally {
            // MEASURED, and the reason this is a `finally` rather than a `$b->commit()` on the happy
            // path: a 1205 does NOT end B's transaction. MySQL's default
            // `innodb_rollback_on_timeout = OFF` rolls back only the STATEMENT, so B stays InTx
            // holding the metadata lock its UPDATE already took — and the `DROP TABLE` below then
            // blocked until the client's 5 s io timeout and surfaced as an IndeterminateException
            // (autocommit write lost mid-flight) instead of the failure this test is about.
            $b->rollBack();
        }
        self::assertTrue($blocked, 'SERIALIZABLE must make the read block a concurrent write');

        $a->commit();
        $a->exec('DROP TABLE s8b_iso');
    }
}
