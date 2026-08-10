<?php // /php/doctrine-dbal/tests/Live/ExceptionMappingLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\DriverException as DbalDriverException;
use Doctrine\DBAL\Exception\LockWaitTimeoutException;
use Doctrine\DBAL\Exception\RetryableException;
use Doctrine\DBAL\Exception\SyntaxErrorException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException as FerroRetryable;
use Ferro\DBAL\IndeterminateWriteException;
use Ferro\Protocol\Generated\Constants as C;

/**
 * M1-S8b Task 11, live — the converter driven from REAL backend errors on both families, because a
 * table-driven unit test proves the table and not the wire. In particular the MySQL half is what
 * proves the S8a errno-on-wire carry actually arrives: DBAL's MySQL converter keys exclusively on
 * `getCode()`, so if the errno were missing every one of these would fall through to a bare
 * `DriverException` and this test would go red.
 */
final class ExceptionMappingLiveTest extends DbalLiveTestCase
{
    public function testRealErrorsMapToTheStockClassesOnBothFamilies(): void
    {
        foreach ($this->families() as $kind => $pool) {
            $c = $this->dbal($pool);
            $c->executeStatement('DROP TABLE IF EXISTS s8b_err');
            $c->executeStatement(
                $kind === 'postgres'
                    ? 'CREATE TABLE s8b_err (id int primary key)'
                    : 'CREATE TABLE s8b_err (id INT PRIMARY KEY)',
            );
            $c->executeStatement('INSERT INTO s8b_err (id) VALUES (1)');

            try {
                $c->executeStatement('INSERT INTO s8b_err (id) VALUES (1)');
                self::fail("[$kind] a duplicate key must throw");
            } catch (UniqueConstraintViolationException $e) {
                self::assertNotInstanceOf(RetryableException::class, $e, "[$kind] a unique violation is deterministic");
            }

            try {
                $c->executeStatement('SELECT * FROM s8b_no_such_table');
                self::fail("[$kind] a missing table must throw");
            } catch (TableNotFoundException) {
            }

            try {
                $c->executeStatement('SELEKT 1');
                self::fail("[$kind] a syntax error must throw");
            } catch (SyntaxErrorException) {
            }

            $c->executeStatement('DROP TABLE s8b_err');
        }
    }

    /**
     * A REAL §9.2 `Retryable`, produced by each engine's own concurrency control, and the point is
     * not the specific class but that the class carries DBAL's `RetryableException` marker while an
     * indeterminate write does not: the two branches must be distinguishable by an application's
     * retry loop, from the outside, without reading a message.
     *
     * **PLAN DEVIATION, MEASURED — the plan's two-connection deadlock cannot run.** A deadlock needs
     * two transactions BLOCKED on each other, and the Ferro client is synchronous: after
     * `$a->executeStatement('UPDATE … WHERE id = 2')` blocks on b's row lock, nothing in a
     * single-threaded PHP process can ever issue b's opposing statement. Measured verbatim from the
     * plan: `Ferro\Client\Error\TransportException: read timed out after 0 of 16 bytes` at 10.3 s
     * (the client's 5 s io_timeout, twice) — no deadlock, no `RetryableException`, an ERROR. So each
     * family provokes its Retryable with a recipe that needs no concurrency:
     *
     *  - **PostgreSQL: first-updater-wins.** Under REPEATABLE READ, a row updated and COMMITTED by
     *    another transaction after this one's snapshot makes the update fail IMMEDIATELY with
     *    SQLSTATE `40001` — no blocking, no timing, and `40001` is the very first case in DBAL's
     *    PostgreSQL SQLSTATE table (`DeadlockException`, which carries the marker). The `SET
     *    TRANSACTION ISOLATION LEVEL` here is the per-TRANSACTION form issued as the first statement
     *    inside the transaction; it is NOT the `SET SESSION …` form SPEC §22.2 (s) forbids and Task
     *    13 refuses, and it lands on the pinned `tx_id` like any other in-transaction statement.
     *  - **MySQL: a row-lock wait that gives up in 1 s** → errno `1205` →
     *    `LockWaitTimeoutException`, the ONLY other class DBAL marks retryable. This half runs
     *    entirely through the ERRNO-keyed table, so it also fails if the S8a errno carry stops
     *    arriving.
     *
     * Each leg then READS THE ROW BACK. "Retryable" means the statement provably did not apply, and
     * that is the claim worth checking — an engine that silently re-ran the victim (charter rule 3)
     * would show up here as a changed value, not as a changed exception class.
     */
    public function testARealEngineRetryableCarriesTheMarkerOnBothFamilies(): void
    {
        $this->postgresSerializationFailure();
        $this->mysqlLockWaitTimeout();
    }

    /** PostgreSQL: SQLSTATE 40001 → `DeadlockException`, via the SQLSTATE-keyed stock table. */
    private function postgresSerializationFailure(): void
    {
        $a = $this->dbal();
        $b = $this->dbal();
        $a->executeStatement('DROP TABLE IF EXISTS s8b_retryable');
        $a->executeStatement('CREATE TABLE s8b_retryable (id int primary key, n int)');
        $a->executeStatement('INSERT INTO s8b_retryable (id, n) VALUES (1, 0)');

        $a->beginTransaction();
        $a->executeStatement('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
        $a->fetchOne('SELECT n FROM s8b_retryable WHERE id = 1'); // takes the snapshot

        $b->beginTransaction();
        $b->executeStatement('UPDATE s8b_retryable SET n = n + 1 WHERE id = 1');
        $b->commit();

        $caught = null;
        try {
            $a->executeStatement('UPDATE s8b_retryable SET n = n + 100 WHERE id = 1');
            self::fail('[postgres] the outdated snapshot must lose: 40001 could not serialize access');
        } catch (DbalDriverException $e) {
            $caught = $e;
        } finally {
            $this->rollBackQuietly($a);
        }

        self::assertInstanceOf(RetryableException::class, $caught, '[postgres] the §9.2 branch must survive the boundary');
        self::assertInstanceOf(DeadlockException::class, $caught, '[postgres] 40001 is the stock SQLSTATE table\'s first case');
        self::assertNotInstanceOf(
            IndeterminateWriteException::class,
            $caught,
            '[postgres] a serialization victim provably did NOT apply — Retryable, never Indeterminate',
        );
        self::assertSame('40001', $caught->getSQLState(), '[postgres] the SQLSTATE the stock table keyed on');
        self::assertInstanceOf(FerroRetryable::class, self::ferroCause($caught), '[postgres] and the WIRE branch really was Retryable');

        self::assertSame(
            1,
            (int) $a->fetchOne('SELECT n FROM s8b_retryable WHERE id = 1'),
            '[postgres] only the winner applied: the refused +100 must not appear, and nothing may re-run it',
        );
        $a->executeStatement('DROP TABLE s8b_retryable');
    }

    /** MySQL: errno 1205 → `LockWaitTimeoutException`, via the ERRNO-keyed stock table. */
    private function mysqlLockWaitTimeout(): void
    {
        $pool = $this->requireMysqlPool();
        $a = $this->dbal($pool);
        $b = $this->dbal($pool);
        $a->executeStatement('DROP TABLE IF EXISTS s8b_retryable');
        $a->executeStatement('CREATE TABLE s8b_retryable (id INT PRIMARY KEY, n INT) ENGINE=InnoDB');
        $a->executeStatement('INSERT INTO s8b_retryable (id, n) VALUES (1, 0)');

        $a->beginTransaction();
        $a->executeStatement('UPDATE s8b_retryable SET n = 1 WHERE id = 1'); // holds the row lock

        $b->beginTransaction();
        $b->executeStatement('SET SESSION innodb_lock_wait_timeout = 1'); // on b's PINNED connection
        $caught = null;
        try {
            $b->executeStatement('UPDATE s8b_retryable SET n = 2 WHERE id = 1');
            self::fail('[mysql] the second updater must give up after 1 s: errno 1205');
        } catch (DbalDriverException $e) {
            $caught = $e;
        } finally {
            $this->rollBackQuietly($b);
            $this->rollBackQuietly($a);
        }

        self::assertInstanceOf(RetryableException::class, $caught, '[mysql] the §9.2 branch must survive the boundary');
        self::assertInstanceOf(LockWaitTimeoutException::class, $caught, '[mysql] 1205, from the ERRNO-keyed stock table');
        self::assertNotInstanceOf(
            IndeterminateWriteException::class,
            $caught,
            '[mysql] a lock-wait victim provably did NOT apply — Retryable, never Indeterminate',
        );
        self::assertSame(1205, $caught->getCode(), '[mysql] the vendor errno is what the stock table read');
        self::assertInstanceOf(FerroRetryable::class, self::ferroCause($caught), '[mysql] and the WIRE branch really was Retryable');

        self::assertSame(
            0,
            (int) $a->fetchOne('SELECT n FROM s8b_retryable WHERE id = 1'),
            '[mysql] both transactions rolled back: neither update survives, and nothing re-ran the victim',
        );
        $a->executeStatement('DROP TABLE s8b_retryable');
    }

    /**
     * **The COST of `readonly = false`, pinned so it cannot change silently.**
     *
     * `readonly` is read in TWO places in `fate.rs`, and the second is the **57014 override**
     * (`engine/crates/ferrod/src/services/fate.rs:71-114`): with `!in_tx`, a cancelled or
     * timed-out statement is `Cancelled{NonRetryable}` when the client declared `readonly` and
     * `WriteUnconfirmed{INDETERMINATE}` when it did not. The driver declares WRITE for everything
     * (hazard 22 — the DBAL 4 SPI carries no read/write signal and charter rule 6 forbids inferring
     * one), so **a plain `SELECT` killed by a server-side cancel or an operator's
     * `statement_timeout` surfaces as `Ferro\DBAL\IndeterminateWriteException`** — "your write may
     * or may not have landed", for a statement that wrote nothing.
     *
     * That is the price of the decision, not a bug, and it is listed in
     * `docs/known-incompatibilities.md` and in §22.2 (ac). What this test does is make it
     * FALSIFIABLE in both directions, so it can neither be quietly "fixed" by inferring
     * read-vs-write from SQL text nor quietly forgotten. It is also the ONLY behavioural test of
     * `driverOptions.readonly` anywhere in the slice — every other assertion about it only proves
     * the option is parsed.
     *
     * `SELECT pg_cancel_backend(pg_backend_pid())` cancels its OWN statement, producing a genuine
     * `57014` on an ordinary autocommit statement with no session state and no second connection
     * (hazard 82). It has to be that: the PHP client never sends `ExecRequest.timeout_ms`, and a
     * preceding `SET statement_timeout` would land on a different pooled connection — a non-local
     * `SET` taints the checkout but does not pin it.
     *
     * **The assertion that stops this passing for the wrong reason** is the exact `/proto` error
     * CODE, read off the client exception under the DBAL one: `WRITE_UNCONFIRMED` on the write
     * half, `CANCELLED` on the readonly half. It is not decoration. MEASURED: pointing the readonly
     * half at an ordinary missing-table error instead leaves ALL THREE class assertions green
     * (not `IndeterminateWriteException`, not `RetryableException`, still a `DriverException`) and
     * is caught HERE and only here — `Failed asserting that 12289 is identical to 12296`.
     *
     * The `SELECT 42` that follows each half is a LIVENESS net, not a discriminator, and the
     * difference is worth stating plainly. It catches a future change that leaves the session or the
     * wire poisoned after a cancel — `pg_cancel_backend` cancels the STATEMENT and the connection
     * must stay usable. It does NOT distinguish a cancel from a torn backend: MEASURED with
     * `pg_terminate_backend(pg_backend_pid())`, the pool discards the dead connection and the
     * follow-up still answers 42. What DOES notice that route is the readonly half, which comes back
     * `ConnectionLost{Retryable}` instead of `Cancelled{NonRetryable}`.
     *
     * **PLAN DEVIATION, MEASURED.** The plan asserted `getSQLState() === '57014'` on both halves.
     * The engine's own 57014 override does not carry one: it REBUILDS the payload
     * (`fate.rs::payload()`, `sqlstate: None`, `errno: None`), so both halves arrive with a null
     * SQLSTATE and `getCode() === 0`. Measured live: `sqlstate => NULL` on both. The assertion is
     * therefore made on the wire code (strictly more specific — it pins WHICH fate cell, not just
     * that the statement was cancelled) and the null SQLSTATE is pinned explicitly rather than
     * dropped, so a future engine change that starts carrying it is noticed here.
     *
     * PostgreSQL only, deliberately. `fate.rs` is shared VERBATIM across backends (the S6 slice
     * reused it untouched), the 57014 override's own unit table
     * (`fate.rs::fate_57014_total_over_all_axes`) proves the cell for every `(readonly, sent,
     * in_tx)` combination, and `mysql_chaos_it.rs` already drives the MySQL errno mapping into it.
     * One family pins the SHAPE; duplicating it would add a second flaky path, not a second proof.
     */
    public function testACancelledSelectIsIndeterminateOnAWriteConnectionAndNotOnAReadonlyOne(): void
    {
        $sql = 'SELECT pg_cancel_backend(pg_backend_pid())';

        $write = $this->dbal();
        try {
            $write->executeQuery($sql);
            self::fail('a self-cancelled statement must raise 57014');
        } catch (DbalDriverException $e) {
            self::assertInstanceOf(
                IndeterminateWriteException::class,
                $e,
                'on the DEFAULT (write-declared) connection a 57014 is §19.3 Indeterminate — this is '
                . 'the documented cost of declaring every DBAL statement a write',
            );
            self::assertSame(
                C::ERR_WRITE_UNCONFIRMED,
                self::wireCode($e),
                'the §19.3 write-cancel cell specifically, not merely "something indeterminate" — a '
                . 'torn connection would reach the same class by a different route',
            );
            self::assertNull($e->getSQLState(), 'MEASURED: the 57014 override rebuilds the payload and drops the SQLSTATE');
        }
        self::assertSame(42, (int) $write->fetchOne('SELECT 42'), 'the STATEMENT was cancelled, not the session');

        $read = $this->dbal('default', ['readonly' => true]);
        try {
            $read->executeQuery($sql);
            self::fail('a self-cancelled statement must raise 57014 here too');
        } catch (DbalDriverException $e) {
            self::assertNotInstanceOf(
                IndeterminateWriteException::class,
                $e,
                'driverOptions.readonly is what buys back the clean "statement cancelled" answer',
            );
            self::assertNotInstanceOf(RetryableException::class, $e, 'Cancelled is NonRetryable on the wire');
            self::assertSame(
                C::ERR_CANCELLED,
                self::wireCode($e),
                'the readonly cell of the same override — the SAME statement, a different fate',
            );
            self::assertNull($e->getSQLState(), 'MEASURED: the 57014 override rebuilds the payload and drops the SQLSTATE');
        }
        self::assertSame(42, (int) $read->fetchOne('SELECT 42'), 'the STATEMENT was cancelled, not the session');
    }

    /** @return array<string,string> */
    private function families(): array
    {
        return ['postgres' => 'default', 'mysql' => $this->requireMysqlPool()];
    }

    private function rollBackQuietly(DbalConnection $c): void
    {
        if (!$c->isTransactionActive()) {
            return;
        }
        try {
            $c->rollBack();
        } catch (\Throwable) {
            // the victim's transaction may already be gone engine-side
        }
    }

    /**
     * The `/proto` error code the ENGINE chose, read off the client exception chained under the DBAL
     * one. It is the only place the fate CELL is visible: the DBAL exception carries the SQLSTATE
     * and the errno, and the 57014 override supplies neither.
     */
    private static function wireCode(\Throwable $e): ?int
    {
        for ($x = $e; $x !== null; $x = $x->getPrevious()) {
            if ($x instanceof IndeterminateException || $x instanceof NonRetryableException || $x instanceof FerroRetryable) {
                return $x->errorPayload()->code;
            }
        }
        return null;
    }

    /** The client-side taxonomy exception under a converted DBAL one, if any. */
    private static function ferroCause(\Throwable $e): ?\Throwable
    {
        for ($x = $e; $x !== null; $x = $x->getPrevious()) {
            if ($x instanceof IndeterminateException || $x instanceof NonRetryableException || $x instanceof FerroRetryable) {
                return $x;
            }
        }
        return null;
    }
}
