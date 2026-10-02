<?php // /php/doctrine-dbal/tests/Live/CommitFailureLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\RetryableException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\TransactionIsolationLevel;

/**
 * M2-C5 review F3 — a COMMIT that FAILS, against real PostgreSQL, on whichever DBAL major is
 * installed. DBAL 4's wrapper converts a COMMIT failure; DBAL 3's never does, so before C5's fix a
 * `40001` at COMMIT reached a DBAL 3 application as a bare driver exception instead of a retryable
 * `DeadlockException`. Both cells are failures that exist ONLY at COMMIT — the commonest real shape
 * of each — and both go through DBAL's own `transactional()` as well as the imperative trio.
 */
final class CommitFailureLiveTest extends DbalLiveTestCase
{
    private function serializable(): DbalConnection
    {
        $c = DriverManager::getConnection([
            'driverClass' => self::driverClass(),
            'wrapperClass' => self::wrapperClass(),
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => 'default'],
        ]);
        $c->setTransactionIsolation(TransactionIsolationLevel::SERIALIZABLE);
        return $c;
    }

    /**
     * A DEFERRED unique constraint is checked at COMMIT, so the violation is a COMMIT failure by
     * construction — deterministic, one connection, no race.
     */
    public function testADeferredConstraintViolationAtCommitIsTheStockUniqueException(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS c5_deferred');
        $c->executeStatement(
            'CREATE TABLE c5_deferred (id int, CONSTRAINT c5_deferred_u UNIQUE (id) DEFERRABLE INITIALLY DEFERRED)',
        );

        $c->beginTransaction();
        $c->executeStatement('INSERT INTO c5_deferred (id) VALUES (1)');
        $c->executeStatement('INSERT INTO c5_deferred (id) VALUES (1)'); // accepted: checked at COMMIT
        try {
            $c->commit();
            self::fail('the deferred violation must fail the COMMIT');
        } catch (UniqueConstraintViolationException) {
        }

        $t = $this->dbal();
        try {
            $t->transactional(static function (DbalConnection $x): void {
                $x->executeStatement('INSERT INTO c5_deferred (id) VALUES (2)');
                $x->executeStatement('INSERT INTO c5_deferred (id) VALUES (2)');
            });
            self::fail('the deferred violation must fail transactional()');
        } catch (UniqueConstraintViolationException) {
        }

        self::assertSame(0, (int) $this->dbal()->fetchOne('SELECT count(*) FROM c5_deferred'), 'neither transaction committed');
        $this->dbal()->executeStatement('DROP TABLE c5_deferred');
    }

    /**
     * SERIALIZABLE write skew: both transactions read the whole table before either writes, so
     * PostgreSQL's SSI finds a dangerous structure and fails the SECOND commit with `40001` — the
     * shape §19.3 and DBAL both call retryable, and the one SSI defers to COMMIT by design.
     */
    public function testASerializationFailureAtCommitIsARetryableDeadlockException(): void
    {
        $setup = $this->dbal();
        $setup->executeStatement('DROP TABLE IF EXISTS c5_ssi');
        $setup->executeStatement('CREATE TABLE c5_ssi (v int)');
        $setup->executeStatement('INSERT INTO c5_ssi (v) VALUES (1)');

        foreach (['imperative', 'transactional'] as $shape) {
            $a = $this->serializable();
            $b = $this->serializable();
            $a->beginTransaction();
            $a->fetchOne('SELECT sum(v) FROM c5_ssi');
            $b->beginTransaction();
            $b->fetchOne('SELECT sum(v) FROM c5_ssi');
            $a->executeStatement('INSERT INTO c5_ssi (v) VALUES (10)');
            $b->executeStatement('INSERT INTO c5_ssi (v) VALUES (20)');
            $a->commit();

            try {
                if ($shape === 'imperative') {
                    $b->commit();
                } else {
                    // transactional() owns BEGIN+COMMIT, so it needs a transaction of its own: open
                    // it at the same conflicting point, B's open one is rolled back first.
                    $b->rollBack();
                    $c = $this->serializable();
                    $d = $this->serializable();
                    $c->beginTransaction();
                    $c->fetchOne('SELECT sum(v) FROM c5_ssi');
                    $d->transactional(static function (DbalConnection $x) use ($c): void {
                        $x->fetchOne('SELECT sum(v) FROM c5_ssi');
                        $c->executeStatement('INSERT INTO c5_ssi (v) VALUES (30)');
                        $x->executeStatement('INSERT INTO c5_ssi (v) VALUES (40)');
                        $c->commit();
                    });
                }
                self::fail("[$shape] the second COMMIT must fail with a serialization failure");
            } catch (DeadlockException $e) {
                self::assertInstanceOf(RetryableException::class, $e, "[$shape] DBAL's retry marker");
                self::assertSame('40001', $e->getSQLState(), "[$shape] the SQLSTATE survives");
            }
        }

        $this->dbal()->executeStatement('DROP TABLE c5_ssi');
    }
}
