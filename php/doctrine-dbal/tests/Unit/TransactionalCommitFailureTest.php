<?php // /php/doctrine-dbal/tests/Unit/TransactionalCommitFailureTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\ConnectionException;
use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\RetryableException as DbalRetryableException;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\DBAL\Connection as FerroDriverConnection;
use Ferro\DBAL\IndeterminateWriteException;
use Ferro\DBAL\PlatformVersion;
use Ferro\DBAL\Tests\Support\FixedDriver;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M2-C5 — a failed COMMIT through DBAL 4's own `transactional()`, the DBAL 4 twin of
 * `Dbal3TransactionTest`. DBAL 4's wrapper converts the COMMIT failure itself (DBAL 3's does not —
 * that is review F3), so the 40001 cell is a regression guard here; the indeterminate cell is the
 * TRIPWIRE on upstream behaviour the DBAL 3 file describes, and it is identical on both majors.
 */
final class TransactionalCommitFailureTest extends TestCase
{
    private static function wrapper(FakeSession $session): DbalConnection
    {
        $driverConn = new FerroDriverConnection(new FerroClientConnection($session, 'default'), 'p', PlatformVersion::KIND_POSTGRES, false);
        return new DbalConnection(['serverVersion' => 'PostgreSQL 17.10'], new FixedDriver($driverConn));
    }

    public function testASerializationFailureAtCommitReachesTheCallerRetryable(): void
    {
        $session = FakeSession::withTxBegin(txId: 9)->push(
            FakeSession::errorOutcome(new ErrorPayload(
                C::ERR_SERIALIZATION_FAILURE,
                C::BRANCH_RETRYABLE,
                '40001',
                null,
                'could not serialize access due to read/write dependencies among transactions',
                null,
                null,
            )),
            [C::SERVICE_TX, C::METHOD_TX_COMMIT],
        );
        try {
            self::wrapper($session)->transactional(static fn (): int => 1);
            self::fail('transactional() must surface the COMMIT failure');
        } catch (DeadlockException $e) {
            self::assertInstanceOf(DbalRetryableException::class, $e);
        }
    }

    /** See `Dbal3TransactionTest::testTransactionalMasksAnIndeterminateCommitUnderNoActiveTransaction`. */
    public function testTransactionalMasksAnIndeterminateCommitUnderNoActiveTransaction(): void
    {
        try {
            self::wrapper(FakeSession::withTxBegin(txId: 11)->thenThrowOnCommit())->transactional(static fn (): int => 1);
            self::fail('an indeterminate COMMIT must surface');
        } catch (ConnectionException $e) {
            self::assertSame('There is no active transaction.', $e->getMessage(), 'upstream masks it under its own rollback');
            self::assertNotInstanceOf(DbalRetryableException::class, $e, 'nothing retryable may reach the caller');
            self::assertInstanceOf(IndeterminateWriteException::class, $e->getPrevious(), 'the real fate is chained beneath');
        }
    }

    /**
     * The shared core's driver-level no-op, on the DBAL 4 shell: after a COMMIT that failed and ended
     * the transaction, `rollBack()` has nothing to roll back and sends nothing. DBAL 4's own wrapper
     * never reaches it (its rollback throws first, above), but a caller holding the driver connection
     * does — and DBAL 3 before 3.9.4 does from `transactional()` (`Dbal3TransactionTest`).
     */
    public function testARollbackAfterACommitThatEndedTheTransactionIsANoOp(): void
    {
        $session = FakeSession::withTxBegin(txId: 12)->thenThrowOnCommit();
        $c = new FerroDriverConnection(new FerroClientConnection($session, 'default'), 'p', PlatformVersion::KIND_POSTGRES, false);
        $c->beginTransaction();
        try {
            $c->commit();
            self::fail('the COMMIT must fail');
        } catch (\Ferro\DBAL\Exception\DriverException) {
        }
        $sent = $session->sendCount();
        $c->rollBack();
        self::assertSame($sent, $session->sendCount(), 'nothing to roll back, so nothing is sent');
    }

    /** The CONTROL: a rollback with no transaction that NO commit ended is still a caller bug. */
    public function testARollbackWithNoTransactionStillThrows(): void
    {
        $c = new FerroDriverConnection(new FerroClientConnection(new FakeSession(), 'default'), 'p', PlatformVersion::KIND_POSTGRES, false);
        $this->expectException(\Ferro\DBAL\Exception\DriverException::class);
        $c->rollBack();
    }
}
