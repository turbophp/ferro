<?php // /php/doctrine-dbal/tests/Live/TransactionalIndeterminateLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Exception\NoActiveTransaction;
use Doctrine\DBAL\Exception\RetryableException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\DBAL\IndeterminateWriteException;
use Ferro\Protocol\Generated\Constants as C;

/**
 * The whole-branch review's BLOCKER 2, live: **an indeterminate write must reach the caller AS an
 * indeterminate write through `Doctrine\DBAL\Connection::transactional()`** — the single most
 * common transaction idiom in Doctrine, and the one the slice never tested.
 *
 * `TransactionalMaskingTest` proves the MECHANISM deterministically over a scripted session. This
 * file proves the CONSEQUENCE against real engines, because the scripted version cannot answer the
 * question that actually matters — does `ferrod` really classify this event `Indeterminate`, and
 * does that class really survive the whole DBAL stack to the application's `catch`?
 *
 * **The recipe.** Inside the closure, read the pinned backend's own connection id and tear that
 * backend connection down from a SECOND Ferro connection (a different pooled upstream connection).
 * The closure then returns normally, `transactional()` issues its COMMIT, and the engine finds the
 * link gone — `PoolError::ConnectionLost` on a `TX/COMMIT`, which `sql.rs::declare_ctl` classifies
 * with `readonly = false, sent = true, in_tx = false`, i.e. §19.3's `WriteUnconfirmed`. It is the
 * one shape that produces a genuine lost COMMIT from a single-threaded PHP process: a client that
 * blocks on its own socket can never race its own commit.
 *
 * Both families, same shape, different verbs (`pg_terminate_backend` / `KILL`) — the masking itself
 * is family-independent, but running both proves the ENGINE reaches the same §19.3 cell on each,
 * through two entirely different drivers.
 */
final class TransactionalIndeterminateLiveTest extends DbalLiveTestCase
{
    /** @return array<string, array{0:string, 1:string, 2:string, 3:string}> */
    private function families(): array
    {
        return [
            'postgres' => ['default', 'CREATE TABLE s8b_tx_ind (id int primary key)', 'SELECT pg_backend_pid()', 'SELECT pg_terminate_backend(%d)'],
            'mysql' => [$this->requireMysqlPool(), 'CREATE TABLE s8b_tx_ind (id INT PRIMARY KEY) ENGINE=InnoDB', 'SELECT CONNECTION_ID()', 'KILL %d'],
        ];
    }

    /**
     * **THE GUARD, both halves of the review's requirement in one run per family.**
     *
     *  1. the fate reaches the caller as `Ferro\DBAL\IndeterminateWriteException` — NOT as
     *     `Doctrine\DBAL\Exception\NoActiveTransaction`, which is what the application was told
     *     before this fix ("There is no active transaction.", a message that reads like a
     *     programming error and is routinely logged and ignored); and
     *  2. a `catch (RetryableException)` handler — the shape Symfony Messenger, the ORM retry
     *     helpers and every hand-rolled retry loop use — does NOT fire on it. A single wrong
     *     ancestor here would turn the project's headline at-most-once guarantee into an
     *     at-least-once write (charter rule 3).
     *
     * The retryable half is a REAL `catch` block, ordered ahead of the indeterminate one, so it is
     * the exception's own class hierarchy being measured rather than an `instanceof` written by the
     * test.
     *
     * The `/proto` error code is asserted too, and it is not decoration: it pins WHICH §19.3 cell
     * was reached. Without it the test would pass for a connection torn down at any other moment.
     */
    public function testALostCommitInsideTransactionalReachesTheCallerAsAnIndeterminateWrite(): void
    {
        foreach ($this->families() as $kind => [$pool, $ddl, $pidSql, $killSql]) {
            $c = $this->dbalWrapped($pool);
            $killer = $this->dbal($pool);
            $this->freshTable($c, $ddl);

            $caughtRetryable = false;
            $indeterminate = null;
            try {
                $c->transactional(function (DbalConnection $inner) use ($killer, $pidSql, $killSql): int {
                    $inner->executeStatement('INSERT INTO s8b_tx_ind (id) VALUES (1)');
                    $killer->executeStatement(sprintf($killSql, self::scalarInt($inner, $pidSql)));
                    return 1;
                });
                self::fail("[$kind] a COMMIT on a torn-down backend connection must not report success");
            } catch (RetryableException) {
                // ORDERED FIRST on purpose: if the exception ever acquired DBAL's retryable marker
                // this arm would swallow it and the assertion below would name the failure.
                $caughtRetryable = true;
            } catch (IndeterminateWriteException $e) {
                $indeterminate = $e;
            }

            self::assertFalse(
                $caughtRetryable,
                "[$kind] an indeterminate write must NEVER be catchable as retryable — a framework "
                . 'that replayed it would apply the write twice',
            );
            self::assertInstanceOf(
                IndeterminateWriteException::class,
                $indeterminate,
                "[$kind] BLOCKER 2: transactional() must not hand the application "
                . 'NoActiveTransaction in place of the write\'s real fate',
            );
            // No `assertNotInstanceOf(NoActiveTransaction)` here: after the catch above it CANNOT
            // fail, and an assertion that cannot fail is worse than none. The classes an
            // indeterminate write must NOT be catchable as are measured where the measurement can
            // go red — `Unit\ExceptionAncestryTest::testExactlyTheseCatchBlocksFireOnAnIndeterminateWrite`
            // — and the masked shape is pinned by the stock-wrapper test below.
            self::assertSame(
                C::ERR_WRITE_UNCONFIRMED,
                self::wireCode($indeterminate),
                "[$kind] the §19.3 lost-COMMIT cell specifically — not merely 'something failed'",
            );

            self::assertSame(
                0,
                self::scalarInt($killer, 'SELECT count(*) FROM s8b_tx_ind'),
                "[$kind] charter rule 3: nothing re-ran the transaction. The ENGINE cannot know "
                . 'whether the COMMIT landed — that is what Indeterminate means — but it must never '
                . 'have replayed it, and here the torn backend means it did not land at all',
            );
            $killer->executeStatement('DROP TABLE s8b_tx_ind');
        }
    }

    /**
     * **The premise lock, live: the STOCK wrapper still masks.**
     *
     * The same event, the same engine, the same driver — only `wrapperClass` differs. It asserts the
     * WRONG behaviour on purpose, because that is what makes the override's necessity measurable
     * rather than asserted: without this, "the wrapper is required" is a docblock claim, and the
     * test above could be green for a reason that has nothing to do with the wrapper.
     *
     * It is also the deletion trigger. If a doctrine/dbal release exempts our class (or stops
     * rolling back at nesting level 0), this goes red and
     * {@see \Ferro\DBAL\Wrapper\IndeterminateSafeTransactional} can be removed instead of being
     * carried forever.
     */
    public function testWithoutTheWrapperTheSameEventIsMaskedAsNoActiveTransaction(): void
    {
        [$pool, $ddl, $pidSql, $killSql] = $this->families()['postgres'];
        $c = $this->dbal($pool);                       // STOCK Doctrine\DBAL\Connection
        $killer = $this->dbal($pool);
        $this->freshTable($c, $ddl);

        try {
            $c->transactional(function (DbalConnection $inner) use ($killer, $pidSql, $killSql): int {
                $inner->executeStatement('INSERT INTO s8b_tx_ind (id) VALUES (1)');
                $killer->executeStatement(sprintf($killSql, self::scalarInt($inner, $pidSql)));
                return 1;
            });
            self::fail('a COMMIT on a torn-down backend connection must not report success');
        } catch (\Throwable $e) {
            self::assertInstanceOf(
                NoActiveTransaction::class,
                $e,
                'MEASURED UPSTREAM BEHAVIOUR, pinned: on the stock wrapper DBAL replaces the fate '
                . 'with its own cleanup failure. This is the defect the Ferro wrapperClass repairs.',
            );
            self::assertInstanceOf(
                IndeterminateWriteException::class,
                $e->getPrevious(),
                'the real fate survives only as a cause, which no catch block keys on',
            );
        }
        $killer->executeStatement('DROP TABLE s8b_tx_ind');
    }

    /**
     * A scalar integer read back off `$c`. Used for the pinned backend's connection id (read INSIDE
     * the transaction, so it is the pinned connection and not some other pooled one) and for the
     * row-count read-back.
     */
    private static function scalarInt(DbalConnection $c, string $sql): int
    {
        $v = $c->fetchOne($sql);
        if (!is_int($v) && !is_string($v)) {
            self::fail("expected an integer from: $sql");
        }
        return (int) $v;
    }

    private function freshTable(DbalConnection $c, string $ddl): void
    {
        $c->executeStatement('DROP TABLE IF EXISTS s8b_tx_ind');
        $c->executeStatement($ddl);
    }

    /** The `/proto` error code the ENGINE chose, read off the client exception under the DBAL one. */
    private static function wireCode(\Throwable $e): ?int
    {
        for ($x = $e; $x !== null; $x = $x->getPrevious()) {
            if ($x instanceof IndeterminateException) {
                return $x->errorPayload()->code;
            }
        }
        return null;
    }
}
