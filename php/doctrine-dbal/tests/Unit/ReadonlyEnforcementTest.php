<?php // /php/doctrine-dbal/tests/Unit/ReadonlyEnforcementTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Exception\RetryableException;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\DBAL\Connection;
use Ferro\DBAL\Exception\UnsupportedStatement;
use Ferro\DBAL\IndeterminateWriteException;
use Ferro\DBAL\PlatformVersion;
use Ferro\DBAL\RetryableDriverException;
use Ferro\DBAL\Tests\Support\FixedDriver;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The whole-branch review's MAJOR: `driverOptions.readonly` was ENFORCED in a transaction and not in
 * autocommit — the one place it decides §19.3 fate.
 *
 * `fate.rs` reads `readonly` twice. The 57014 override is the RELIEF (`Cancelled{NonRetryable}`
 * instead of `WriteUnconfirmed{Indeterminate}`) and is untouched by this fix — `ExceptionMappingLive
 * Test` still pins both of its cells. The `PoolError::ConnectionLost` arm is the HAZARD:
 * `sent && !readonly && !in_tx` is `Indeterminate`, and the same event with `readonly = true` is
 * `ConnectionLost{Retryable}`, which this driver's converter upgrades to `RetryableDriverException
 * implements Doctrine\DBAL\Exception\RetryableException` — the marker a framework replays on. On a
 * connection wrongly marked readonly that replays a write that may already have landed.
 *
 * Two halves, both guarded here and both again live:
 *  1. **Enforcement.** `executeStatement()` — DBAL's write entry point — is refused in autocommit.
 *  2. **Fate.** A LOST autocommit statement on a readonly connection is reported §19.3
 *     `Indeterminate`, exactly as the same event is on the default write-declared connection.
 *
 * The NEGATIVE rows are what keep the rule narrow: the write connection must be untouched, an
 * in-transaction statement must keep its `Retryable` (there the declaration is server-enforced, and
 * `fate.rs` classifies an in-tx loss as "the tx is dead" regardless of `readonly`), a pool-checkout
 * timeout must keep its `Retryable` (its verdict never depended on the declaration), and the tx
 * CONTROL boundaries must keep theirs.
 */
final class ReadonlyEnforcementTest extends TestCase
{
    private static function driverConn(FakeSession $session, bool $readonly): Connection
    {
        return new Connection(
            new FerroClientConnection($session, 'default'),
            'default',
            PlatformVersion::KIND_POSTGRES,
            $readonly,
        );
    }

    /** A real `Doctrine\DBAL\Connection` over a scripted session, so the CONVERTER runs. */
    private static function dbal(FakeSession $session, bool $readonly): DbalConnection
    {
        return new DbalConnection(
            ['serverVersion' => '17.10'],
            new FixedDriver(self::driverConn($session, $readonly), PlatformVersion::KIND_POSTGRES),
        );
    }

    private static function outcome(int $code, int $branch, string $message): ErrorPayload
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

    // ---- 1. ENFORCEMENT -------------------------------------------------------------------------

    /**
     * The autocommit write entry point is refused PRE-SEND: nothing reaches the wire at all, which is
     * the assertion `$session->sendCount() === 0` makes non-negotiable. (Before the fix the same call
     * measured `affected = 1` against live PG.)
     */
    public function testTheAutocommitWriteEntryPointIsRefusedOnAReadonlyConnection(): void
    {
        $session = (new FakeSession())->thenExecOk(null);
        $c = self::driverConn($session, readonly: true);

        try {
            $c->exec('UPDATE t SET v = 1');
            self::fail('executeStatement() on a readonly connection must be refused');
        } catch (UnsupportedStatement $e) {
            self::assertStringContainsString('driverOptions.readonly', $e->getMessage());
            self::assertStringContainsString('WRITE entry point', $e->getMessage());
        }
        self::assertSame(0, $session->sendCount(), 'refused PRE-SEND — nothing reached the engine');
    }

    /** The mirror: the DEFAULT connection is untouched. */
    public function testTheSameStatementRunsOnAWriteConnection(): void
    {
        $session = (new FakeSession())->thenExecOk(null);

        self::assertSame(1, self::driverConn($session, readonly: false)->exec('UPDATE t SET v = 1'));
        self::assertSame(1, $session->sendCount());
    }

    /**
     * The second mirror, and the one that stops the refusal from being written as a blanket ban:
     * INSIDE a transaction it must pass through. Doctrine's own `SAVEPOINT DOCTRINE_2` /
     * `RELEASE SAVEPOINT` / `ROLLBACK TO SAVEPOINT` arrive at `exec()` (they carry no parameters), so
     * a blanket refusal would break nested transactions on every readonly connection — and there the
     * SERVER is enforcing the declaration anyway (`BEGIN READ ONLY` → `25006`).
     */
    public function testInsideATransactionTheSamePathIsNotRefused(): void
    {
        $session = FakeSession::withTxBegin(txId: 41)->thenExecOk(null);
        $c = self::driverConn($session, readonly: true);
        $c->beginTransaction();

        self::assertSame(1, $c->exec('SAVEPOINT DOCTRINE_2'));
        self::assertSame(2, $session->sendCount(), 'BEGIN + the savepoint both went out');
    }

    // ---- 2. FATE --------------------------------------------------------------------------------

    /**
     * THE CELL. The same wire terminal — `ConnectionLost{Retryable}`, which is what `fate.rs` emits
     * for a lost statement once `readonly = true` — reaches the application as a REPLAY INVITATION on
     * a readonly connection and as an indeterminate write on the default one. After the fix both are
     * indeterminate, because the driver cannot back the declaration for an autocommit statement.
     *
     * The parameterized path is used deliberately: it is the one a readonly connection can still
     * reach for a write (`executeStatement('UPDATE … WHERE id = ?', [1])`), i.e. exactly the shape
     * the enforcement half above cannot refuse.
     *
     * **Each row scripts the terminal `fate.rs` ACTUALLY emits for that declaration**, which is the
     * only way to state this cell honestly: the two branches are computed ENGINE-side, from the flag
     * this driver sends, so injecting one terminal for both connections would be testing a shape the
     * engine cannot produce. The write row is therefore the MIRROR (it says what the readonly row is
     * being made to match); the readonly row is the guard.
     *
     * @param bool $readonly the connection-level declaration
     * @param int $code the `/proto` error code `fate.rs` emits for a lost autocommit statement under
     *   that declaration (`PoolError::ConnectionLost`: `WRITE_UNCONFIRMED` when `sent && !readonly &&
     *   !in_tx`, `CONNECTION_LOST` otherwise)
     * @param int $branch its §9.2 branch byte
     */
    #[DataProvider('lostAutocommitTerminals')]
    public function testALostAutocommitStatementIsIndeterminateOnBothConnections(
        bool $readonly,
        int $code,
        int $branch,
    ): void {
        $session = (new FakeSession())->push(
            FakeSession::errorOutcome(self::outcome($code, $branch, 'connection lost mid-flight')),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $c = self::dbal($session, $readonly);

        try {
            $c->executeStatement('UPDATE t SET v = 1 WHERE id = ?', [1]);
            self::fail('a lost statement must throw');
        } catch (\Doctrine\DBAL\Exception $e) {
            self::assertInstanceOf(
                IndeterminateWriteException::class,
                $e,
                'the readonly declaration must not be able to promise "safe to retry" for a '
                . 'statement nothing enforced it on',
            );
            self::assertNotInstanceOf(RetryableException::class, $e, 'and it must carry no retry marker');
        }
    }

    /** @return array<string, array{0: bool}> */
    public static function fates(): array
    {
        return ['a write connection (the default)' => [false], 'driverOptions.readonly' => [true]];
    }

    /** @return array<string, array{0: bool, 1: int, 2: int}> */
    public static function lostAutocommitTerminals(): array
    {
        return [
            'a write connection (the default) — the engine already says Indeterminate' =>
                [false, C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE],
            'driverOptions.readonly — the engine says Retryable BECAUSE of the declaration' =>
                [true, C::ERR_CONNECTION_LOST, C::BRANCH_RETRYABLE],
        ];
    }

    /**
     * **NEGATIVE 1 — a verdict that never depended on the declaration keeps it.** A pool checkout
     * timeout is `Retryable` for reads and writes alike (`PoolError::Timeout`, no `readonly` in that
     * arm), so re-minting it would destroy true information and make a readonly connection strictly
     * worse than a write one.
     */
    public function testAPoolTimeoutStaysRetryableOnAReadonlyConnection(): void
    {
        $session = (new FakeSession())->push(
            FakeSession::errorOutcome(self::outcome(
                C::ERR_POOL_TIMEOUT,
                C::BRANCH_RETRYABLE,
                'timed out waiting for a pooled connection',
            )),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $c = self::dbal($session, readonly: true);

        try {
            $c->executeQuery('SELECT 1 WHERE id = ?', [1]);
            self::fail('a pool timeout must throw');
        } catch (\Doctrine\DBAL\Exception $e) {
            self::assertInstanceOf(RetryableDriverException::class, $e);
        }
    }

    /**
     * **NEGATIVE 2 — an IN-TRANSACTION loss keeps its `Retryable`.** There the declaration IS
     * enforced (`BEGIN READ ONLY`), and `fate.rs` classifies an in-tx loss as "the whole transaction
     * is dead, it will never commit" — a KNOWN fate, independent of `readonly`. Re-minting it would
     * turn every mid-transaction link loss on a readonly connection into a false indeterminate.
     */
    public function testAnInTransactionLossStaysRetryableOnAReadonlyConnection(): void
    {
        $session = FakeSession::withTxBegin(txId: 42)->push(
            FakeSession::errorOutcome(self::outcome(
                C::ERR_CONNECTION_LOST,
                C::BRANCH_RETRYABLE,
                'connection lost; the transaction is dead',
            )),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $c = self::dbal($session, readonly: true);
        $c->beginTransaction();

        try {
            $c->executeQuery('SELECT 1 WHERE id = ?', [1]);
            self::fail('a lost in-tx statement must throw');
        } catch (\Doctrine\DBAL\Exception $e) {
            self::assertInstanceOf(RetryableDriverException::class, $e);
            self::assertNotInstanceOf(IndeterminateWriteException::class, $e);
        }
    }

    /**
     * **NEGATIVE 3 — the tx CONTROL boundaries are not statements.** A lost `BEGIN` opened nothing,
     * whatever the connection declared, so it stays `Retryable`; `fate.rs` makes the same
     * distinction (`in_tx: false` for every control op, and a separate arm for each). This is what
     * stops the re-mint from being applied by `Connection::beginTransaction()`/`commit()`/
     * `rollBack()`, which do NOT route through `statementException()`.
     */
    public function testALostBeginStaysRetryableOnAReadonlyConnection(): void
    {
        $session = (new FakeSession())->push(
            FakeSession::errorOutcome(self::outcome(
                C::ERR_CONNECTION_LOST,
                C::BRANCH_RETRYABLE,
                'connection lost before BEGIN reached the backend',
            )),
            [C::SERVICE_TX, C::METHOD_TX_BEGIN],
        );
        $c = self::dbal($session, readonly: true);

        try {
            $c->beginTransaction();
            self::fail('a rejected BEGIN must throw');
        } catch (\Doctrine\DBAL\Exception $e) {
            self::assertInstanceOf(RetryableDriverException::class, $e);
            self::assertNotInstanceOf(IndeterminateWriteException::class, $e);
        }
    }
}
