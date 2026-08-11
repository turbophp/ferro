<?php // /php/doctrine-dbal/tests/Unit/TransactionalMaskingTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Exception\NoActiveTransaction;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\Client\Error\IndeterminateException;
use Ferro\DBAL\Connection as FerroDriverConnection;
use Ferro\DBAL\IndeterminateWriteException;
use Ferro\DBAL\PlatformVersion;
use Ferro\DBAL\Tests\Support\FixedDriver;
use Ferro\DBAL\Wrapper\FerroConnection;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * The whole-branch review's BLOCKER 2: `Doctrine\DBAL\Connection::transactional()` — the canonical
 * Doctrine transaction idiom — REPLACES `Ferro\DBAL\IndeterminateWriteException` with
 * `Doctrine\DBAL\Exception\NoActiveTransaction` before it ever reaches the caller.
 *
 * Nothing in the slice tested `transactional()` at all, which is why the blocker shipped: every
 * transaction guard was written against the imperative `beginTransaction()`/`commit()` pair, where
 * the exception surfaces correctly. The masking happens strictly ABOVE the driver SPI, so no test
 * whose vantage point is a `Ferro\DBAL\Connection` can see it — review species (c).
 *
 * Everything above the driver SPI here is the REAL Doctrine code: its nesting counter, its
 * `transactional()` body, its exception conversion. Only the transport is scripted
 * ({@see FakeSession}), and the shape it scripts — a `TX/COMMIT` whose reply never comes — is the
 * one `fate.rs` classifies `WriteUnconfirmed{Indeterminate}` (`sql.rs::declare_ctl`, `readonly =
 * false`, `sent = true`, `in_tx = false`). The live counterpart, which tears the pinned backend
 * connection out from under a real COMMIT, is
 * `Live\TransactionalIndeterminateLiveTest`.
 */
final class TransactionalMaskingTest extends TestCase
{
    /**
     * A `Doctrine\DBAL\Connection` (or a subclass) whose COMMIT is lost on the wire.
     *
     * @param class-string<DbalConnection> $wrapperClass
     */
    private static function withLostCommit(string $wrapperClass): DbalConnection
    {
        return self::over(FakeSession::withTxBegin(txId: 10)->thenThrowOnCommit(), $wrapperClass);
    }

    /**
     * @param class-string<DbalConnection> $wrapperClass
     */
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

    private static function payload(int $code, int $branch, string $message): ErrorPayload
    {
        return new ErrorPayload(
            code: $code,
            branch: $branch,
            sqlstate: null,
            errno: null,
            message: $message,
            detail: null,
            retryAfterMs: null,
        );
    }

    /**
     * **A PREMISE LOCK, not a guard on our code.** It pins the upstream defect the wrapper exists to
     * repair, at the exact version the package is tested against, so that "why is there an override
     * at all?" has a running answer — and so that a doctrine/dbal release which fixes the masking
     * makes this red and lets the override be deleted rather than kept forever out of superstition.
     *
     * Read the assertion carefully: it asserts the WRONG behaviour on purpose. On stock
     * `Doctrine\DBAL\Connection` the application is told "There is no active transaction." for a
     * write that may have landed.
     */
    public function testStockDoctrineTransactionalMasksTheIndeterminateWrite(): void
    {
        $c = self::withLostCommit(DbalConnection::class);

        try {
            $c->transactional(static fn (): int => 1);
            self::fail('a lost COMMIT must not let transactional() return');
        } catch (\Throwable $e) {
            self::assertInstanceOf(
                NoActiveTransaction::class,
                $e,
                'UPSTREAM BEHAVIOUR, pinned: DBAL 4 rolls back after a non-exempt failed commit, at '
                . 'nesting level 0, and that NoActiveTransaction replaces the driver\'s verdict',
            );
            self::assertInstanceOf(
                IndeterminateWriteException::class,
                $e->getPrevious(),
                'the real fate survives only as a cause — which no catch block keys on',
            );
        }
    }

    /**
     * **THE GUARD.** With this package's `wrapperClass`, an indeterminate write reaches the caller AS
     * an indeterminate write — the SAME instance the converter minted, with its cause chain intact.
     *
     * The cause chain is asserted alongside the class, and that is not decoration: a repair that
     * caught the masking and MINTED A FRESH exception of the right class would lose the chained
     * `Ferro\Client\Error\IndeterminateException` — which is where the `/proto` error code, and
     * therefore which §19.3 cell was reached, actually lives.
     */
    public function testTheFerroWrapperSurfacesTheIndeterminateWriteThroughTransactional(): void
    {
        $c = self::withLostCommit(FerroConnection::class);
        $minted = null;

        try {
            $c->transactional(static fn (): int => 1);
            self::fail('a lost COMMIT must not let transactional() return');
        } catch (IndeterminateWriteException $e) {
            $minted = $e;
        }

        self::assertInstanceOf(IndeterminateWriteException::class, $minted);
        self::assertInstanceOf(
            IndeterminateException::class,
            self::ferroCause($minted),
            'the client-side fate exception must still be chained under it — that is where the '
            . '/proto error code lives',
        );
        self::assertSame(
            0,
            $c->getTransactionNestingLevel(),
            'and the connection is left usable: DBAL already unwound its own counter',
        );
        self::assertFalse($c->isTransactionActive());
    }

    /**
     * The happy path must be untouched: `transactional()` still returns the closure's value.
     *
     * Without it the override could "fix" the blocker by swallowing every return.
     */
    public function testASuccessfulTransactionalStillReturnsTheClosureValue(): void
    {
        $c = self::over(FakeSession::withTxBegin(txId: 20)->thenControlOk(), FerroConnection::class);

        self::assertSame('done', $c->transactional(static fn (): string => 'done'));
        self::assertSame(0, $c->getTransactionNestingLevel());
    }

    /**
     * **MIRROR 1 — the override must not fire when DBAL is telling the truth.**
     *
     * A closure that rolls its own transaction back leaves nesting at 0, so `transactional()`'s own
     * `commit()` raises `NoActiveTransaction` from the LEVEL CHECK, and the `rollBack()` in the
     * `finally` then raises a SECOND one that chains the first. MEASURED, and both halves matter:
     * no driver ever spoke here, so there is no verdict to restore and the OUTER exception — the one
     * carrying the chain — is what must reach the caller.
     *
     * This is what stops the override being written as the shorter "always rethrow the previous":
     * that mutation hands back the inner `NoActiveTransaction`, whose own cause is null, and this
     * test is the only place it shows up.
     */
    public function testAGenuineNoActiveTransactionIsStillReportedAsOne(): void
    {
        $c = self::over(FakeSession::withTxBegin(txId: 21)->thenControlOk(), FerroConnection::class);

        try {
            $c->transactional(static function (DbalConnection $inner): int {
                $inner->rollBack();          // the application ended it itself
                return 1;
            });
            self::fail('committing with no open transaction is a programming error and must throw');
        } catch (NoActiveTransaction $e) {
            self::assertInstanceOf(
                NoActiveTransaction::class,
                $e->getPrevious(),
                'the OUTER of DBAL\'s two NoActiveTransactions, with the first chained under it — a '
                . 'repair that substituted the cause here would silently truncate that chain',
            );
        }
    }

    /**
     * **MIRROR 2 — an application that HANDLED the indeterminate write is not second-guessed.**
     *
     * The closure catches the write's fate and throws its own exception carrying it as a cause. That
     * application has made a decision; the wrapper must deliver ITS exception, not reach into the
     * chain and substitute the one it recognises.
     *
     * This is the test that forces the unmask to stay narrow (`NoActiveTransaction` only, immediate
     * cause only). Widen it to a chain walk — the obvious "be more helpful" refactor — and this goes
     * red.
     */
    public function testTheUnmaskDoesNotOverruleAnApplicationThatHandledTheWriteItself(): void
    {
        $session = FakeSession::withTxBegin(txId: 22)
            ->push(
                FakeSession::errorOutcome(self::payload(
                    C::ERR_WRITE_UNCONFIRMED,
                    C::BRANCH_INDETERMINATE,
                    'the reply to this write never arrived',
                )),
                [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
            )
            ->thenControlOk();               // the ROLLBACK DBAL issues from its first finally
        $c = self::over($session, FerroConnection::class);

        try {
            $c->transactional(static function (DbalConnection $inner): int {
                try {
                    $inner->executeStatement('INSERT INTO t (id) VALUES (1)');
                } catch (IndeterminateWriteException $fate) {
                    throw new \DomainException('reconciliation queued', 0, $fate);
                }
                return 1;
            });
            self::fail('the application\'s own exception must propagate');
        } catch (\DomainException $e) {
            self::assertSame('reconciliation queued', $e->getMessage());
            self::assertInstanceOf(
                IndeterminateWriteException::class,
                $e->getPrevious(),
                'the fate is still reachable as the cause the application chose to attach',
            );
        }
    }

    /** The client-side taxonomy exception chained under a converted DBAL one, if any. */
    private static function ferroCause(\Throwable $e): ?\Throwable
    {
        for ($x = $e->getPrevious(); $x !== null; $x = $x->getPrevious()) {
            if ($x instanceof IndeterminateException) {
                return $x;
            }
        }
        return null;
    }
}
