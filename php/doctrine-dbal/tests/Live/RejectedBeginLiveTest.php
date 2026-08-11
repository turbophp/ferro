<?php // /php/doctrine-dbal/tests/Live/RejectedBeginLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Exception as DbalException;
use Ferro\DBAL\RetryableDriverException;

/**
 * The rejected-BEGIN MAJOR against a REAL `ferrod`: a BEGIN that the engine refuses **retryably on a
 * perfectly healthy client session**, which is the shape a PDO driver essentially never produces and
 * the reason the desync matters here.
 *
 * The review reproduced it by exhausting a 16-connection pool. This uses an UNREACHABLE pool instead
 * — the `extraPoolDsns()` hook `ServerVersionLiveTest` already established, port 1 so the dial fails
 * immediately with `ECONNREFUSED` — because the two produce the same thing where it counts: a
 * `Retryable` rejection of `TX/BEGIN` with the client session intact and reusable. It is also
 * deterministic and takes milliseconds, where pool exhaustion costs a 5 s checkout timeout per
 * attempt.
 *
 * What the fix must make true, and what was measured FALSE before it:
 *  * the SECOND `beginTransaction()` must fail the same honest way, not as `SAVEPOINT DOCTRINE_2`
 *    ("savepoint statement outside a transaction", naming a savepoint the application never wrote),
 *    and the nesting counter must not climb;
 *  * on an UNWRAPPED connection, a statement in the desynced window must be refused rather than run
 *    in autocommit behind a transaction the caller believes is open;
 *  * `rollBack()` must resynchronise instead of raising "rollBack() with no open transaction".
 */
final class RejectedBeginLiveTest extends DbalLiveTestCase
{
    /** @return array<string, string> */
    protected function extraPoolDsns(): array
    {
        // Port 1 refuses immediately (ECONNREFUSED) — the same fast-failure DSN ServerVersionLiveTest
        // uses. Pools are lazy, so declaring it costs nothing until a request names it.
        return ['dead' => 'postgres://ferro:ferro@127.0.0.1:1/ferro'];
    }

    /**
     * With this package's `wrapperClass`, the retry the driver invites is a REAL retry: same honest
     * verdict, nesting back at 0, and the connection still usable afterwards.
     */
    public function testARejectedBeginIsRetryableForRealOnAWrappedConnection(): void
    {
        $c = $this->dbalWrapped('dead');

        for ($attempt = 1; $attempt <= 3; $attempt++) {
            try {
                $c->beginTransaction();
                self::fail("attempt $attempt: a BEGIN on an unreachable pool must fail");
            } catch (DbalException $e) {
                self::assertInstanceOf(
                    RetryableDriverException::class,
                    $e,
                    "attempt $attempt: the SAME honest rejection every time — before the fix the "
                    . 'second attempt was a refused SAVEPOINT DOCTRINE_2 instead',
                );
                self::assertStringNotContainsString('SAVEPOINT', $e->getMessage());
            }
            self::assertSame(0, $c->getTransactionNestingLevel(), "attempt $attempt: the counter is back");
            self::assertFalse($c->isTransactionActive());
        }

        // And the connection is not poisoned: a healthy pool on the same ferrod still works. (A new
        // DBAL connection, because a driver connection is bound to its pool.)
        self::assertSame(1, (int) $this->dbal()->fetchOne('SELECT 1'));
    }

    /**
     * The half that needs NO configuration: on a stock `Doctrine\DBAL\Connection` the desynced window
     * is closed by refusal, and `rollBack()` gets the connection back.
     */
    public function testAnUnwrappedConnectionRefusesTheWindowAndRecoversOnRollBack(): void
    {
        $c = $this->dbal('dead');

        try {
            $c->beginTransaction();
            self::fail('a BEGIN on an unreachable pool must fail');
        } catch (RetryableDriverException) {
        }
        self::assertTrue($c->isTransactionActive(), 'UPSTREAM: Doctrine counted the failed BEGIN as open');

        try {
            $c->executeStatement('CREATE TABLE s8b_never (id int)');
            self::fail('a statement in the desynced window must be refused');
        } catch (DbalException $e) {
            self::assertStringContainsString('beginTransaction() on this connection was REJECTED', $e->getMessage());
        }

        $c->rollBack();                                   // the resync — must not throw
        self::assertSame(0, $c->getTransactionNestingLevel());
        self::assertFalse($c->isTransactionActive());
    }
}
