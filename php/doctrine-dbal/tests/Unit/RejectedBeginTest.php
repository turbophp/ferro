<?php // /php/doctrine-dbal/tests/Unit/RejectedBeginTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Connection as DbalConnection;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\DBAL\Connection as FerroDriverConnection;
use Ferro\DBAL\Exception\UnsupportedStatement;
use Ferro\DBAL\PlatformVersion;
use Ferro\DBAL\RetryableDriverException;
use Ferro\DBAL\Tests\Support\FixedDriver;
use Ferro\DBAL\Wrapper\FerroConnection;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * The whole-branch review's MAJOR: a REJECTED `beginTransaction()` desyncs Doctrine's nesting
 * counter and wedges the connection for good — "which is worse than the original failure".
 *
 * `Doctrine\DBAL\Connection::beginTransaction()` does `++$this->transactionNestingLevel` BEFORE
 * calling the driver and never undoes it. Under PDO a failed BEGIN means a dead connection, so it
 * barely matters. **Ferro rejects a BEGIN retryably on a perfectly healthy session** — a pool
 * checkout timeout, an unavailable replica, a BEGIN that never reached the backend — so the shape is
 * ROUTINE, and the driver marks it `RetryableDriverException`, telling the framework to retry
 * something that could never succeed: nesting stays ≥ 1, so the retry emits `SAVEPOINT DOCTRINE_2`,
 * which the engine correctly refuses, and the counter climbs instead of coming back.
 *
 * Everything above the driver SPI here is REAL Doctrine code — its counter, its savepoint
 * generation, its conversion. Only the transport is scripted ({@see FakeSession}), and the shape it
 * scripts (`TX/BEGIN` answered `PoolTimeout{Retryable}`) is the one `fate.rs` emits under pool
 * exhaustion.
 *
 * The fix has two halves, and both are exercised below:
 *  * the **wrapper** ({@see \Ferro\DBAL\Wrapper\ResyncsNestingOnARejectedBegin}) resynchronises the
 *    counter, so the retry the driver invited actually works;
 *  * the **driver** refuses every statement in the desynced window and treats `rollBack()` as the
 *    resync — which is what an application on the STOCK `Doctrine\DBAL\Connection` gets, with no
 *    configuration.
 */
final class RejectedBeginTest extends TestCase
{
    /** A session whose FIRST `TX/BEGIN` is rejected `PoolTimeout{Retryable}`. */
    private static function rejectingBegin(): FakeSession
    {
        return (new FakeSession())->push(
            FakeSession::errorOutcome(new ErrorPayload(
                code: C::ERR_POOL_TIMEOUT,
                branch: C::BRANCH_RETRYABLE,
                sqlstate: null,
                errno: null,
                message: 'timed out waiting for a pooled connection',
                detail: null,
                retryAfterMs: null,
            )),
            [C::SERVICE_TX, C::METHOD_TX_BEGIN],
        );
    }

    /** @param class-string<DbalConnection> $wrapperClass */
    private static function over(FakeSession $session, string $wrapperClass): DbalConnection
    {
        $driverConn = new FerroDriverConnection(
            new FerroClientConnection(session: $session),
            'default',
            PlatformVersion::KIND_POSTGRES,
            false,
        );
        return new $wrapperClass(
            ['serverVersion' => '17.10'],
            new FixedDriver($driverConn, PlatformVersion::KIND_POSTGRES),
        );
    }

    /** @return array<string,mixed> the decoded `ExecRequest` of the LAST send */
    private static function lastExec(FakeSession $session): array
    {
        $off = 0;
        return ExecRequest::mapFromWire(
            array_values((array) (new PurePacker())->unpack($session->lastRequest()['payload'], $off)),
        );
    }

    /**
     * **A PREMISE LOCK, not a guard on our code** — the same shape `TransactionalMaskingTest` uses.
     * It asserts the WRONG behaviour on purpose, at the version this package is tested against, so
     * that "why is there an override at all?" has a running answer and so a doctrine/dbal release
     * that stops counting a failed BEGIN as open makes this red and lets the override be deleted.
     */
    public function testStockDoctrineCountsAREJECTEDTransactionAsOpen(): void
    {
        $c = self::over(self::rejectingBegin(), DbalConnection::class);

        try {
            $c->beginTransaction();
            self::fail('a rejected BEGIN must throw');
        } catch (RetryableDriverException) {
        }

        self::assertSame(1, $c->getTransactionNestingLevel(), 'UPSTREAM BEHAVIOUR, pinned');
        self::assertTrue($c->isTransactionActive(), 'Doctrine believes a transaction is open');
    }

    /**
     * **THE GUARD (wrapper half).** With this package's `wrapperClass` the counter comes back, so the
     * `RetryableDriverException` the driver hands out is honest: the retry OPENS A REAL TRANSACTION.
     *
     * The last assertion is the one that cannot pass for the wrong reason. It is not "the retry did
     * not throw" — it reads the `tx_id` off the ENCODED `ExecRequest` and requires it to be the
     * transaction's. Without the resync the retry's statement would be `SAVEPOINT DOCTRINE_2` with a
     * NULL `tx_id`, which is precisely the wedged state the review measured.
     */
    public function testTheWrapperResynchronisesSoTheRetryActuallyOpensATransaction(): void
    {
        $session = self::rejectingBegin()->push(FakeSession::beginOk(77), [C::SERVICE_TX, C::METHOD_TX_BEGIN])
            ->thenExecOk(null);
        $c = self::over($session, FerroConnection::class);

        try {
            $c->beginTransaction();
            self::fail('a rejected BEGIN must throw');
        } catch (RetryableDriverException) {
        }

        self::assertSame(0, $c->getTransactionNestingLevel(), 'the counter is back');
        self::assertFalse($c->isTransactionActive());

        // The retry the driver invited.
        $c->beginTransaction();
        $c->executeStatement('INSERT INTO t (v) VALUES (1)');

        $req = self::lastExec($session);
        self::assertSame('INSERT INTO t (v) VALUES (1)', $req['sql'], 'not a SAVEPOINT — a real statement');
        self::assertSame(77, $req['tx_id'], 'riding the RETRIED transaction, not autocommit');
    }

    /**
     * **THE GUARD (driver half, no wrapper — what an unconfigured application gets).**
     *
     * The desynced window is CLOSED rather than silently permitted: the statement the review measured
     * going out `tx_id = NULL` while `isTransactionActive()` said true is refused pre-send, and
     * `sendCount()` proves nothing reached the engine.
     */
    public function testWithoutTheWrapperAStatementInTheDesyncedWindowIsRefusedInsteadOfRunningAutocommit(): void
    {
        // The ExecOk is scripted but must never be consumed. It is here so that a regression REACHES
        // the assertion below instead of dying on an unscripted send — the difference between a RED
        // that says "the statement ran in autocommit" and one that says "FakeSession ran out".
        $session = self::rejectingBegin()->thenExecOk(null);
        $c = self::over($session, DbalConnection::class);

        try {
            $c->beginTransaction();
            self::fail('a rejected BEGIN must throw');
        } catch (RetryableDriverException) {
        }
        self::assertSame(1, $session->sendCount(), 'only the BEGIN went out');

        try {
            $c->executeStatement('INSERT INTO t (v) VALUES (1)');
            self::fail('a statement behind a believed-open transaction must be refused');
        } catch (\Doctrine\DBAL\Exception $e) {
            self::assertStringContainsString('beginTransaction() on this connection was REJECTED', $e->getMessage());
            self::assertStringContainsString('rollBack()', $e->getMessage(), 'and it names the way out');
        }
        self::assertSame(1, $session->sendCount(), 'STILL only the BEGIN — nothing ran in autocommit');
    }

    /**
     * **THE GUARD (driver half, recovery).** `rollBack()` — the only public path that lowers DBAL's
     * counter — resynchronises instead of raising "rollBack() with no open transaction", and the
     * connection is usable afterwards.
     *
     * It is honest, not convenient: nothing ran in the window (the test above proves the refusal), so
     * there is provably nothing to undo.
     */
    public function testWithoutTheWrapperARollBackResynchronisesAndTheConnectionRecovers(): void
    {
        $session = self::rejectingBegin()->push(FakeSession::beginOk(88), [C::SERVICE_TX, C::METHOD_TX_BEGIN])
            ->thenExecOk(null);
        $c = self::over($session, DbalConnection::class);

        try {
            $c->beginTransaction();
            self::fail('a rejected BEGIN must throw');
        } catch (RetryableDriverException) {
        }

        $c->rollBack();                                   // must not throw
        self::assertSame(0, $c->getTransactionNestingLevel());
        self::assertFalse($c->isTransactionActive());

        $c->beginTransaction();
        $c->executeStatement('INSERT INTO t (v) VALUES (1)');
        self::assertSame(88, self::lastExec($session)['tx_id'], 'a real transaction again');
    }

    /**
     * **MIRROR — the refusal is scoped to the desynced window.** A connection whose BEGIN succeeded
     * and then COMMITTED runs autocommit statements normally; a flag that were never cleared would
     * turn one rejected BEGIN into a permanently dead connection, which is the failure being fixed,
     * not a fix.
     */
    public function testAStatementAfterASUCCESSFULTransactionIsNotRefused(): void
    {
        $session = self::rejectingBegin()
            ->push(FakeSession::beginOk(99), [C::SERVICE_TX, C::METHOD_TX_BEGIN])
            ->thenControlOk()                              // the COMMIT
            ->thenExecOk(null);                            // the autocommit statement after it
        $c = self::over($session, FerroConnection::class);

        try {
            $c->beginTransaction();
        } catch (RetryableDriverException) {
        }
        $c->beginTransaction();
        $c->commit();

        $c->executeStatement('INSERT INTO t (v) VALUES (2)');
        self::assertNull(self::lastExec($session)['tx_id'], 'autocommit, and NOT refused');
    }

    /**
     * **MIRROR — a savepoint inside a REAL transaction is untouched.** The refusal keys on the
     * desync, never on the SQL, so Doctrine's own nesting keeps working.
     */
    public function testNestedTransactionsStillWorkOnAConnectionThatNeverFailedABegin(): void
    {
        $session = FakeSession::withTxBegin(txId: 55)->thenExecOk(null);
        $c = self::over($session, FerroConnection::class);

        $c->beginTransaction();
        $c->beginTransaction();                            // -> SAVEPOINT DOCTRINE_2 via exec()

        $req = self::lastExec($session);
        self::assertSame('SAVEPOINT DOCTRINE_2', $req['sql']);
        self::assertSame(55, $req['tx_id'], 'on the pinned transaction');
    }

    /**
     * A `commit()` in the desynced window must NOT report success — the transaction being committed
     * was never opened — and it must leave the connection consistent (DBAL's own `finally` zeroes the
     * counter around this call, so the flag is cleared here too).
     */
    public function testACommitInTheDesyncedWindowFailsLoudlyAndLeavesTheConnectionUsable(): void
    {
        $session = self::rejectingBegin()->push(FakeSession::beginOk(66), [C::SERVICE_TX, C::METHOD_TX_BEGIN])
            ->thenExecOk(null);
        $c = self::over($session, DbalConnection::class);

        try {
            $c->beginTransaction();
        } catch (RetryableDriverException) {
        }

        try {
            $c->commit();
            self::fail('committing a transaction that was never opened must not report success');
        } catch (\Doctrine\DBAL\Exception $e) {
            self::assertStringContainsString('Nothing was committed because nothing ran', $e->getMessage());
        }

        self::assertSame(0, $c->getTransactionNestingLevel());
        $c->beginTransaction();
        $c->executeStatement('INSERT INTO t (v) VALUES (1)');
        self::assertSame(66, self::lastExec($session)['tx_id']);
    }

    /**
     * The driver-level surface, without DBAL in the picture: the flag is per-connection state, and
     * this is the shortest statement of what it does.
     */
    public function testTheDriverConnectionRefusesAndThenRecoversOnItsOwn(): void
    {
        // Both replies are scripted and must never be consumed — see the note above.
        $session = self::rejectingBegin()->thenExecOk(null)->thenStreamHead([['name' => 'n', 'tag' => C::TAG_I64]]);
        $c = new FerroDriverConnection(
            new FerroClientConnection(session: $session),
            'default',
            PlatformVersion::KIND_POSTGRES,
            false,
        );

        try {
            $c->beginTransaction();
            self::fail('a rejected BEGIN must throw');
        } catch (\Ferro\DBAL\Exception\DriverException) {
        }

        foreach (['exec' => fn () => $c->exec('DELETE FROM t'), 'query' => fn () => $c->query('SELECT 1')] as $path => $call) {
            try {
                $call();
                self::fail("$path must be refused while the nesting counter is desynced");
            } catch (UnsupportedStatement) {
            }
        }

        $c->rollBack();
        self::assertSame(1, $session->sendCount(), 'the resync sent nothing');
    }
}
