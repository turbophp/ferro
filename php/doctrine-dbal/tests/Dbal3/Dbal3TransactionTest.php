<?php // /php/doctrine-dbal/tests/Dbal3/Dbal3TransactionTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Dbal3;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\RetryableException as DbalRetryableException;
use Doctrine\DBAL\Platforms\PostgreSQL100Platform;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\DBAL\Dbal3\Connection;
use Ferro\DBAL\Dbal3\Driver;
use Ferro\DBAL\Exception\DriverException as FerroDriverException;
use Ferro\DBAL\IndeterminateWriteException;
use Ferro\DBAL\PlatformVersion;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M2-C5 review F3 — DBAL 3's wrapper never converts a driver exception from `beginTransaction()`,
 * `commit()` or `rollBack()` (DBAL 4's converts all three), so the DBAL 3 connection converts its
 * own. These are the two COMMIT cells that matter most, asserted at BOTH vantage points: the driver
 * connection itself, and DBAL 3's own `transactional()` — which rethrows the driver's exception
 * raw, so the driver's conversion is the only one that can happen.
 */
final class Dbal3TransactionTest extends TestCase
{
    private static function serializationFailureAtCommit(): FakeSession
    {
        return FakeSession::withTxBegin(txId: 9)->push(
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
    }

    private static function conn(FakeSession $session): Connection
    {
        return new Connection(new FerroClientConnection($session, 'default'), 'p', PlatformVersion::KIND_POSTGRES, false);
    }

    /** A DBAL 3 wrapper over a driver whose connection is the given one — no socket, no engine. */
    private static function wrapper(Connection $driverConn): DbalConnection
    {
        $driver = new class ($driverConn) extends \Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware {
            public function __construct(private readonly Connection $conn)
            {
                parent::__construct(new Driver());
            }

            /** @param array<string,mixed> $params */
            public function connect(#[\SensitiveParameter] array $params): Connection
            {
                return $this->conn;
            }

            public function getExceptionConverter(): \Doctrine\DBAL\Driver\API\ExceptionConverter
            {
                return new \Ferro\DBAL\ExceptionConverter(PlatformVersion::KIND_POSTGRES);
            }
        };
        return new DbalConnection(['serverVersion' => 'PostgreSQL 17.10'], $driver);
    }

    public function testASerializationFailureAtCommitIsARetryableDeadlockException(): void
    {
        $c = self::conn(self::serializationFailureAtCommit());
        $c->beginTransaction();
        try {
            $c->commit();
            self::fail('a COMMIT the engine refused must not return normally');
        } catch (DeadlockException $e) {
            self::assertInstanceOf(DbalRetryableException::class, $e, 'DBAL\'s retry marker must survive on DBAL 3');
            self::assertInstanceOf(FerroDriverException::class, $e->getPrevious());
        }
    }

    public function testALostCommitIsAnIndeterminateWriteException(): void
    {
        $c = self::conn(FakeSession::withTxBegin(txId: 10)->thenThrowOnCommit());
        $c->beginTransaction();
        $this->expectException(IndeterminateWriteException::class);
        $c->commit();
    }

    /**
     * Through DBAL 3's own `transactional()`: it catches the COMMIT failure only to decide whether
     * to roll back, hands it to the converter AGAIN for that decision, and rethrows the original.
     * Asserted on the class the CALLER receives.
     */
    public function testTransactionalHandsTheCallerTheConvertedCommitFailure(): void
    {
        $conn = self::wrapper(self::conn(self::serializationFailureAtCommit()));
        self::assertInstanceOf(PostgreSQL100Platform::class, $conn->getDatabasePlatform());
        try {
            $conn->transactional(static fn (): int => 1);
            self::fail('transactional() must surface the COMMIT failure');
        } catch (DeadlockException $e) {
            self::assertInstanceOf(DbalRetryableException::class, $e);
        }
    }

    /**
     * Whether this DBAL resets the nesting level in `commit()`'s `finally` — 3.9.4's "Fix incorrect
     * `transactional()` handling when DB auto-rolled back the transaction" (found with `git log -S`
     * on the 3.10.6 clone). Before it, `transactional()` rolls back from a `catch` THROUGH the driver;
     * from it, the wrapper's own rollback throws first. This file runs against both: the locked
     * 3.10.x, and the `^3.8` floor (3.8.0) in CI.
     */
    private static function wrapperResetsNestingOnAFailedCommit(): bool
    {
        return version_compare((string) \Composer\InstalledVersions::getVersion('doctrine/dbal'), '3.9.4', '>=');
    }

    /**
     * An indeterminate COMMIT through `transactional()`, release by release.
     *
     * **Before 3.9.4** the wrapper rolls back THROUGH the driver from a `catch`, and the driver's
     * rollback after a COMMIT that ended the transaction is a no-op, so the caller receives the
     * `IndeterminateWriteException` itself. (Without that no-op it received the client's "no open
     * transaction" error INSTEAD, with the commit's fate gone — measured on 3.8.0.)
     *
     * **From 3.9.4** this is a TRIPWIRE on upstream behaviour, not a statement of what should happen
     * (SPEC §22.2 (by), `docs/followups/2026-10-02-transactional-masks-a-failed-commit.md`): `commit()`
     * resets the nesting level even when it fails, so `transactional()`'s own rollback throws "There
     * is no active transaction." and PHP chains the real failure beneath it. Identical on DBAL 4
     * (`TransactionalCommitFailureTest`), and not reachable from a driver: the wrapper throws before
     * it calls one. If upstream fixes it, the first assertion goes red.
     *
     * Either way, nothing retryable may reach the caller.
     */
    public function testAnIndeterminateCommitThroughTransactional(): void
    {
        $conn = self::wrapper(self::conn(FakeSession::withTxBegin(txId: 11)->thenThrowOnCommit()));
        try {
            $conn->transactional(static fn (): int => 1);
            self::fail('an indeterminate COMMIT must surface');
        } catch (\Throwable $e) {
            self::assertNotInstanceOf(DbalRetryableException::class, $e, 'nothing retryable may reach the caller');
            if (self::wrapperResetsNestingOnAFailedCommit()) {
                self::assertInstanceOf(\Doctrine\DBAL\ConnectionException::class, $e);
                self::assertSame('There is no active transaction.', $e->getMessage(), 'upstream masks it under its own rollback');
                self::assertInstanceOf(IndeterminateWriteException::class, $e->getPrevious(), 'the real fate is chained beneath');
            } else {
                self::assertInstanceOf(IndeterminateWriteException::class, $e, 'the caller receives the fate itself');
            }
        }
    }

    /**
     * The driver half of that: after a COMMIT that failed and ended the transaction, `rollBack()`
     * has nothing to roll back and sends nothing.
     */
    public function testARollbackAfterACommitThatEndedTheTransactionIsANoOp(): void
    {
        $session = self::serializationFailureAtCommit();
        $c = self::conn($session);
        $c->beginTransaction();
        try {
            $c->commit();
            self::fail('the COMMIT must fail');
        } catch (DeadlockException) {
        }
        $sent = $session->sendCount();
        self::assertTrue($c->rollBack());
        self::assertSame($sent, $session->sendCount(), 'nothing to roll back, so nothing is sent');
    }

    /** The CONTROL: a rollback with no transaction that NO commit ended is still a caller bug. */
    public function testARollbackWithNoTransactionStillThrows(): void
    {
        $this->expectException(\Doctrine\DBAL\Driver\Exception::class);
        self::conn(new FakeSession())->rollBack();
    }

    public function testALostBeginIsConvertedToo(): void
    {
        $c = self::conn(FakeSession::thatThrowsTransportOnBegin());
        try {
            $c->beginTransaction();
            self::fail('a lost BEGIN must surface');
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            self::assertInstanceOf(FerroDriverException::class, $e->getPrevious(), 'converted, carrying the driver error');
        }
    }
}
