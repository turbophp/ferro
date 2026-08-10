<?php // /php/doctrine-dbal/tests/Unit/TransactionTerminalTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Ferro\Client\Connection as FerroClientConnection;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\RetryableException;
use Ferro\DBAL\Connection as FerroDriverConnection;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\PlatformVersion;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 10 — a FAILED transaction terminal must reach Doctrine, and the §9.2 branch it
 * carries must survive the crossing.
 *
 * **This file exists because a named mutation came back GREEN.** The plan's Step-5 mutation 3
 * (`Connection::commit()` swallows the exception instead of rethrowing) was MEASURED green across
 * the whole package — 138 unit tests and 24 live tests, on both live backends — and the plan says to
 * record that as a coverage gap. Recording it is not enough: a swallowed COMMIT failure is the
 * worst outcome this driver can produce. Doctrine's `Connection::commit()` treats a driver `commit()`
 * that returns normally as SUCCESS — it decrements the nesting level and, under
 * `autoCommit = false`, opens the next transaction — so the application is told its writes are
 * durable when the engine said the opposite. Nothing in the transaction's own path would ever
 * notice, and §19.3's `Indeterminate` — the one fate the whole project is built to report — would
 * be silently discarded.
 *
 * A live test cannot reach these cells cheaply (a SERIALIZABLE conflict at COMMIT needs two
 * concurrent sessions; a lost COMMIT needs a killed connection mid-frame). The scripted
 * {@see FakeSession} reaches all four deterministically, and each one is the shape the engine really
 * produces: `ferrod` answers a `TX/COMMIT` with an `ErrorPayload` whose `branch` byte IS the fate
 * (M1-S4), and the client turns a lost frame into the fate exception itself.
 */
final class TransactionTerminalTest extends TestCase
{
    private static function conn(FakeSession $session): FerroDriverConnection
    {
        return new FerroDriverConnection(
            new FerroClientConnection(session: $session),
            'default',
            PlatformVersion::KIND_POSTGRES,
            false,
        );
    }

    private static function payload(int $code, int $branch, ?string $sqlstate, string $message): ErrorPayload
    {
        return new ErrorPayload(
            code: $code,
            branch: $branch,
            sqlstate: $sqlstate,
            errno: null,
            message: $message,
            detail: null,
            retryAfterMs: null,
        );
    }

    /**
     * The dominant real-world shape on PostgreSQL: SSI defers the pivot check to COMMIT, so a
     * SERIALIZABLE conflict arrives as a `40001` on the COMMIT frame itself (CLAUDE.md, M1-S4).
     */
    public function testAFailedCommitReachesDoctrineWithItsSqlstateAndBranch(): void
    {
        $session = FakeSession::withTxBegin(txId: 9)->push(
            FakeSession::errorOutcome(self::payload(
                C::ERR_SERIALIZATION_FAILURE,
                C::BRANCH_RETRYABLE,
                '40001',
                'could not serialize access due to read/write dependencies among transactions',
            )),
            [C::SERVICE_TX, C::METHOD_TX_COMMIT],
        );
        $c = self::conn($session);
        $c->beginTransaction();

        try {
            $c->commit();
            self::fail('a COMMIT the engine refused must not return normally — Doctrine would call it durable');
        } catch (DriverException $e) {
            self::assertSame('40001', $e->getSQLState(), 'the stock PostgreSQL converter keys on this');
            self::assertSame(C::BRANCH_RETRYABLE, $e->branch(), 'the §9.2 fate must survive the crossing');
            self::assertInstanceOf(RetryableException::class, $e->getPrevious());
        }
    }

    /**
     * The §19.3 cell that matters most: a COMMIT whose reply never came. The client classifies it
     * `Indeterminate` — we cannot say whether the transaction committed — and
     * {@see \Ferro\DBAL\ExceptionConverter} turns exactly that branch into
     * `Ferro\DBAL\IndeterminateWriteException`. If the driver swallowed it, the branch would never
     * reach the converter and the caller would be told the writes are durable.
     */
    public function testALostCommitArrivesAsTheIndeterminateBranch(): void
    {
        $c = self::conn(FakeSession::withTxBegin(txId: 10)->thenThrowOnCommit());
        $c->beginTransaction();

        try {
            $c->commit();
            self::fail('a lost COMMIT must surface');
        } catch (DriverException $e) {
            self::assertSame(
                C::BRANCH_INDETERMINATE,
                $e->branch(),
                'the fate of a lost COMMIT is UNKNOWN and must be reported as such',
            );
            self::assertInstanceOf(IndeterminateException::class, $e->getPrevious());
        }
    }

    /** A lost BEGIN opened nothing, so it is Retryable — and it must still cross as a driver error. */
    public function testALostBeginReachesDoctrineAsARetryableDriverError(): void
    {
        $c = self::conn(FakeSession::thatThrowsTransportOnBegin());

        try {
            $c->beginTransaction();
            self::fail('a lost BEGIN must surface');
        } catch (DriverException $e) {
            self::assertSame(C::BRANCH_RETRYABLE, $e->branch());
            self::assertInstanceOf(RetryableException::class, $e->getPrevious());
        }
    }

    /**
     * `rollBack()` is the deliberate ASYMMETRY, and the driver must neither add a swallow nor remove
     * one. The client already swallows the two "that transaction is gone anyway" cases — a lost
     * frame, and a terminal whose code says the engine has already ended the transaction — because
     * Doctrine calls `rollBack()` from a `catch`/`finally` that is carrying the real error. Anything
     * else it rethrows, and that must reach Doctrine.
     *
     * All three rows run through the SAME driver method, so this is a mirror rather than a
     * one-sided negative: a driver that swallowed everything fails row 3, and one that swallowed
     * nothing fails rows 1-2.
     */
    public function testRollbackSwallowsOnlyWhatTheClientSwallows(): void
    {
        // 1. lost on the wire — quiet.
        $c = self::conn(FakeSession::withTxBegin(txId: 11)->thenThrowOnRollback());
        $c->beginTransaction();
        $c->rollBack();
        self::assertFalse($c->ferro()->inTransaction(), 'the handle is cleared either way');

        // 2. the engine has already ended this transaction (a tombstoned tx_id) — quiet.
        $session = FakeSession::withTxBegin(txId: 12)->push(
            FakeSession::errorOutcome(self::payload(
                C::ERR_PROTOCOL,
                C::BRANCH_NON_RETRYABLE,
                null,
                'unknown tx_id 12',
            )),
            [C::SERVICE_TX, C::METHOD_TX_ROLLBACK],
        );
        $c = self::conn($session);
        $c->beginTransaction();
        $c->rollBack();
        self::assertFalse($c->ferro()->inTransaction());

        // 3. anything else — LOUD.
        $session = FakeSession::withTxBegin(txId: 13)->push(
            FakeSession::errorOutcome(self::payload(
                C::ERR_UNSUPPORTED,
                C::BRANCH_NON_RETRYABLE,
                '0A000',
                'rollback refused',
            )),
            [C::SERVICE_TX, C::METHOD_TX_ROLLBACK],
        );
        $c = self::conn($session);
        $c->beginTransaction();
        try {
            $c->rollBack();
            self::fail('a rollback failure that is NOT "the transaction is already gone" must surface');
        } catch (DriverException $e) {
            self::assertSame('0A000', $e->getSQLState());
            self::assertSame(C::BRANCH_NON_RETRYABLE, $e->branch());
        }
    }
}
