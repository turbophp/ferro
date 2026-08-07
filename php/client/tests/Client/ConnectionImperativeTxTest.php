<?php // /php/client/tests/Client/ConnectionImperativeTxTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Backoff;
use Ferro\Client\Connection;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\InvalidTransactionStateException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\ExecCodec;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\SessionInterface;
use Ferro\Client\TxHandle;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8a Task 9 — the IMPERATIVE transaction trio over the scripted {@see FakeSession} seam (no
 * ferrod, no database). The live tier proves the trio against real Postgres; THIS tier pins the
 * things a live test cannot reach: what the encoded request carried, which session it went out on,
 * and what happens when the link dies at each of BEGIN / COMMIT / ROLLBACK.
 */
final class ConnectionImperativeTxTest extends TestCase
{
    /**
     * Decode a recorded `ExecRequest` payload back to its field map.
     *
     * `ExecRequest` has **no `decode()`** — it exposes `encode()` and `mapFromWire()`, so the
     * payload is unpacked first and then mapped. `PurePacker` is the right packer here for two
     * reasons: it is what `PackerFactory::forEncode()` returns, and `ExtPacker::unpack` consumes the
     * WHOLE buffer regardless of the offset it is handed.
     *
     * @return array<string,mixed>
     */
    private static function decodeExec(string $payload): array
    {
        $off = 0;
        $w = (new PurePacker())->unpack($payload, $off);
        return ExecRequest::mapFromWire((array) $w);
    }

    public function testBeginOpensATransactionAndScopesTheNextStatementToIt(): void
    {
        $session = FakeSession::withTxBegin(txId: 41)->thenExecOk();
        $c = new Connection(session: $session);

        $this->assertFalse($c->inTransaction());
        $c->begin();
        $this->assertTrue($c->inTransaction());

        $c->exec('INSERT INTO t VALUES (1)');

        // THE assertion: decode the recorded ExecRequest and read its tx_id back off the wire.
        // Asserting on the encoded payload — not on a getter — is what makes this falsifiable if the
        // delegation in Connection::exec() is ever dropped.
        $sent = $session->lastRequest();
        $this->assertSame(C::SERVICE_SQL, $sent['service']);
        $req = self::decodeExec($sent['payload']);
        $this->assertSame(41, $req['tx_id'], 'a statement inside begin() must carry the tx_id');
    }

    /** Every statement METHOD delegates, not just `exec` — a read that skipped it would run outside the tx. */
    public function testEveryStatementMethodCarriesTheTxId(): void
    {
        $session = FakeSession::withTxBegin(txId: 51)
            ->thenExecOk()->thenEmptyRows()->thenEmptyRows()->thenEmptyRows()->thenEmptyRows();
        $c = new Connection(session: $session);
        $c->begin();

        /** @var array<string, \Closure(Connection): mixed> $methods */
        $methods = [
            'exec'     => static fn (Connection $x) => $x->exec('INSERT INTO t VALUES (1)'),
            'query'    => static fn (Connection $x) => $x->query('SELECT v FROM t'),
            'queryOne' => static fn (Connection $x) => $x->queryOne('SELECT v FROM t'),
            'scalar'   => static fn (Connection $x) => $x->scalar('SELECT v FROM t'),
            'rows'     => static fn (Connection $x) => $x->rows('SELECT v FROM t'),
        ];
        foreach ($methods as $name => $call) {
            $call($c);
            $req = self::decodeExec($session->lastRequest()['payload']);
            $this->assertSame(51, $req['tx_id'], "Connection::{$name}() must carry the tx_id");
        }
    }

    public function testAnAutocommitStatementCarriesANullTxId(): void
    {
        $session = FakeSession::withExecOk();
        $c = new Connection(session: $session);
        $c->exec('INSERT INTO t VALUES (1)');
        $req = self::decodeExec($session->lastRequest()['payload']);
        $this->assertNull($req['tx_id'], 'outside a transaction the tx_id stays null');
    }

    public function testStreamInsideATransactionCarriesTheTxId(): void
    {
        $session = FakeSession::withTxBegin(txId: 42)->thenStreamEnd();
        $c = new Connection(session: $session);
        $c->begin();
        foreach ($c->stream('SELECT 1') as $_) {
            // drain
        }
        $req = self::decodeExec($session->lastRequest()['payload']);
        $this->assertSame(42, $req['tx_id'], 'a stream inside a transaction must be tx-scoped');
        // The fetch modes live on the CODEC (`ExecCodec::FETCH_STREAM`), not on the generated
        // protocol constants — there is no `C::FETCH_*`.
        $this->assertSame(ExecCodec::FETCH_STREAM, $req['fetch']);
    }

    /**
     * A tx-scoped stream rides the TRANSACTION's session, not the reconnect loop's current one.
     *
     * The two are the same object in every path `Connection` itself can take today — nothing
     * reconnects while a transaction is open — so the reconnect below is driven directly on the
     * loop. That is the honest way to exercise a divergence that is currently unreachable from the
     * public API but would be a `NotFoundOrForbidden` (the engine's tx registry is per-session) the
     * moment any future path reconnects mid-transaction.
     */
    public function testATxScopedStreamGoesOutOnTheTransactionsOwnSession(): void
    {
        $txSession = FakeSession::withTxBegin(txId: 61)->thenStreamEnd();
        $freshSession = (new FakeSession(2))->thenStreamEnd();
        $loop = new ReconnectLoop(
            $txSession,
            static fn (): SessionInterface => $freshSession,
            new Backoff(0.0, 0.0, rng: static fn (): float => 0.0, sleep: static function (float $s): void {}),
        );
        $c = new Connection(session: $txSession, reconnect: $loop);

        $c->begin();
        $loop->reconnect(); // the session swaps underneath the open transaction
        foreach ($c->stream('SELECT 1') as $_) {
            // drain
        }

        $this->assertSame(0, $freshSession->sendCount(), 'the tx_id is meaningless on the new session');
        $req = self::decodeExec($txSession->lastRequest()['payload']);
        $this->assertSame(61, $req['tx_id']);
        $this->assertSame(ExecCodec::FETCH_STREAM, $req['fetch']);
    }

    public function testCommitClearsTheHandleEvenWhenTheSessionThrows(): void
    {
        $session = FakeSession::withTxBegin(txId: 43)->thenThrowOnCommit();
        $c = new Connection(session: $session);
        $c->begin();
        try {
            $c->commit();
            $this->fail('a lost COMMIT must surface');
        } catch (IndeterminateException) {
            // §19.3's one transactional Indeterminate — the LEAF class, not FerroException:
            // asserting the root would also pass if commit() threw an unrelated protocol error.
        }
        $this->assertFalse(
            $c->inTransaction(),
            'a failed COMMIT must not leave the connection wedged in a transaction that is gone',
        );
    }

    /**
     * A LOST BEGIN reaches the caller as a FATE, not a raw transport error.
     *
     * "The caller owns retry" is only meaningful if the caller is told what may be retried. A lost
     * BEGIN opened nothing, so it is Retryable — and it must arrive as `RetryableException`, exactly
     * as the closure form's BEGIN arm already classifies it. This is the one assertion that would go
     * RED if `begin()` let a `TransportException` escape untyped.
     */
    public function testALostBeginIsClassifiedAsRetryableAndLeavesNoOpenTransaction(): void
    {
        $c = new Connection(session: FakeSession::thatThrowsTransportOnBegin());
        try {
            $c->begin();
            $this->fail('a lost BEGIN must surface');
        } catch (RetryableException) {
            // expected: nothing was opened, so re-running is safe — the caller's call.
        }
        $this->assertFalse($c->inTransaction(), 'a failed begin() opens nothing');
    }

    /**
     * A LOST ROLLBACK does NOT throw — DBAL calls rollBack() from a catch/finally that is already
     * carrying the real error, and the transaction is dead either way.
     */
    public function testALostRollbackIsSwallowedSoItCannotMaskTheCallersError(): void
    {
        $session = FakeSession::withTxBegin(txId: 45)->thenThrowOnRollback();
        $c = new Connection(session: $session);
        $c->begin();

        $c->rollBack(); // must not throw

        $this->assertFalse($c->inTransaction(), 'the handle is cleared either way');
    }

    /**
     * The realistic DBAL shape: a statement dies mid-transaction, the caller's `finally` rolls back.
     * The handle must SURVIVE the failed statement (so `rollBack()` is legal) and the rollback on
     * the now-dead session must stay quiet, or the `finally` would replace the caller's real error
     * with a transport failure.
     */
    public function testARollbackAfterAMidTransactionLossCannotMaskTheOriginalError(): void
    {
        $session = FakeSession::withTxBegin(txId: 46)->push(
            new \Ferro\Client\Error\TransportException('link died mid-statement'),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $c = new Connection(session: $session);
        $c->begin();

        $original = null;
        try {
            try {
                $c->exec('INSERT INTO t VALUES (1)');
            } catch (\Throwable $e) {
                $original = $e;
                throw $e;
            } finally {
                $c->rollBack(); // the session is dead — this must NOT throw
            }
        } catch (\Throwable $seen) {
            $this->assertSame($original, $seen, "the caller's original error must reach the caller");
        }
        $this->assertNotNull($original);
        $this->assertFalse($c->inTransaction());
    }

    // ---- last_insert_id propagation (M1-S8a C3) -------------------------------------------------

    /**
     * The Task-2 carry-over: an INSERT inside an IMPERATIVE transaction propagates its generated key
     * to the connection level, so a driver's `lastInsertId()` is right for the transactional insert
     * (which is where nearly every real one happens). Task 2 could not build this guard — it had no
     * BEGIN/EXEC/COMMIT fake-transport machinery; this file is that machinery.
     */
    public function testAnInsertInsideAnImperativeTransactionPropagatesItsKey(): void
    {
        $session = FakeSession::withTxBegin(txId: 47)->thenExecOk(99)->thenEmptyRows();
        $c = new Connection(session: $session);
        $c->begin();

        $c->exec('INSERT INTO t (v) VALUES (1)');
        $this->assertSame(99, $c->lastInsertId(), 'an in-transaction key must reach lastInsertId()');

        // …and the same "every statement overwrites it" rule applies inside a transaction: a read
        // that reports no key CLEARS it rather than serving the INSERT's key a second time.
        $c->query('SELECT v FROM t');
        $this->assertNull($c->lastInsertId(), 'a read inside the transaction clears the key');
    }

    /**
     * The other half of the contract: the CLOSURE form still does NOT propagate. The key lives on
     * the handle the closure was given ({@see TxHandle::lastInsertId}) and is deliberately not
     * copied back — reading it off the Connection afterwards would report a value from a transaction
     * that has already ended.
     */
    public function testTheClosureFormStillDoesNotPropagateTheKey(): void
    {
        $session = (new FakeSession())
            ->push(FakeSession::beginOk(48), [C::SERVICE_TX, C::METHOD_TX_BEGIN])
            ->thenExecOk(77)
            ->thenControlOk();
        $c = new Connection(session: $session);

        $handleKey = $c->transaction(static function (TxHandle $tx): int|string|null {
            $tx->exec('INSERT INTO t (v) VALUES (1)');
            return $tx->lastInsertId();
        });

        $this->assertSame(77, $handleKey, 'the handle records its own key');
        $this->assertNull($c->lastInsertId(), 'the closure form does not propagate it to the Connection');
    }

    /**
     * A streamed read clears the key like any other statement (M1-S8a C4). Unreachable against a
     * live backend today — PG never reports a key and MySQL `fetch:stream` is `Unsupported` — which
     * is exactly why it is pinned offline: it goes live the moment MySQL streaming lands.
     *
     * It also pins WHERE the reset lives. `stream()` is a Generator, so nothing runs until the
     * caller iterates; the key must therefore survive an un-iterated `stream()` call and be cleared
     * only when the request actually goes out.
     */
    public function testAStreamClearsThePreviousStatementsKey(): void
    {
        $session = FakeSession::withExecOk(42)->thenStreamEnd();
        $c = new Connection(session: $session);

        $c->exec('INSERT INTO t (v) VALUES (1)');
        $this->assertSame(42, $c->lastInsertId());

        $gen = $c->stream('SELECT v FROM t');
        $this->assertSame(42, $c->lastInsertId(), 'an un-iterated stream() has not run anything yet');

        foreach ($gen as $_) {
            // drain
        }
        $this->assertNull($c->lastInsertId(), 'a streamed read reports no key and must not leave a stale one');
    }

    // ---- misuse ----------------------------------------------------------------------------------

    /** @return iterable<string, array{0: \Closure(Connection): void}> */
    public static function misuses(): iterable
    {
        yield 'commit with no transaction' => [static fn (Connection $c) => $c->commit()];
        yield 'rollBack with no transaction' => [static fn (Connection $c) => $c->rollBack()];
        yield 'nested begin' => [static function (Connection $c): void { $c->begin(); $c->begin(); }];
        yield 'closure form while open' => [
            static function (Connection $c): void { $c->begin(); $c->transaction(static fn () => null); },
        ];
    }

    /**
     * Misuse throws the dedicated LEAF class.
     *
     * Deliberately NOT `FerroException`, which is the ROOT of the whole tree: every taxonomy error,
     * every protocol error and every transport error extends it, so
     * `expectException(FerroException::class)` passes when the misuse is not detected at all and
     * something else fails instead.
     *
     * @param \Closure(Connection): void $misuse
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('misuses')]
    public function testMisuseThrowsInvalidTransactionState(\Closure $misuse): void
    {
        $c = new Connection(session: FakeSession::withTxBegin(txId: 44));
        $this->expectException(InvalidTransactionStateException::class);
        $misuse($c);
    }

    /** The closure form is refused BEFORE any frame goes out — nothing is left dangling engine-side. */
    public function testTheRefusedClosureFormSendsNothing(): void
    {
        $session = FakeSession::withTxBegin(txId: 49);
        $c = new Connection(session: $session);
        $c->begin();
        $before = $session->sendCount();

        try {
            $c->transaction(static fn () => null);
            $this->fail('nesting must be refused');
        } catch (InvalidTransactionStateException) {
            // expected
        }
        $this->assertSame($before, $session->sendCount(), 'the refusal wrote no frame');
        $this->assertTrue($c->inTransaction(), 'and it left the open transaction alone');
    }
}
