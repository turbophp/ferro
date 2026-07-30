<?php // /php/client/tests/Client/TransactionTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Backoff;
use Ferro\Client\Connection;
use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\EpochChangedException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\RetryPolicy;
use Ferro\Client\SessionInterface;
use Ferro\Client\TxHandle;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\TxControl;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * `transaction(closure)` and the §19.3-CRITICAL lost-COMMIT carve-out, over the scripted
 * {@see FakeSession} seam (no ferrod). Proves: a normal return COMMITs (tx-scoped `tx_id` threaded);
 * a thrown closure ROLLs BACK and rethrows; a lost COMMIT is surfaced Indeterminate and NEVER re-runs
 * the closure; and a RetryPolicy re-runs a provably-dead tx within its attempt bound.
 */
final class TransactionTest extends TestCase
{
    private static function instantBackoff(): Backoff
    {
        return new Backoff(0.0, 0.0, rng: static fn (): float => 0.0, sleep: static function (float $s): void {});
    }

    /**
     * A resilient Connection over a list of scripted sessions: the first is the initial session, the
     * rest are handed out (in order) on each reconnect.
     *
     * @param list<FakeSession> $sessions
     */
    private function connection(array $sessions, RetryPolicy $policy): Connection
    {
        $initial = $sessions[0];
        $queue = array_slice($sessions, 1);
        $factory = static function () use (&$queue): SessionInterface {
            if ($queue === []) {
                throw new ConnectionLostException('no more scripted sessions');
            }
            /** @var FakeSession $next */
            $next = array_shift($queue);
            return $next;
        };
        $loop = new ReconnectLoop($initial, $factory, self::instantBackoff(), $policy->maxAttempts);
        return new Connection(session: $initial, pool: 'default', reconnect: $loop, policy: $policy);
    }

    private static function retryablePayload(): ErrorPayload
    {
        return new ErrorPayload(C::ERR_SERIALIZATION_FAILURE, C::BRANCH_RETRYABLE, '40001', null, 'serialization failure', null, null);
    }

    private static function nonRetryablePayload(): ErrorPayload
    {
        return new ErrorPayload(C::ERR_SYNTAX, C::BRANCH_NON_RETRYABLE, '42601', null, 'syntax', null, null);
    }

    // ---- happy path -----------------------------------------------------------------------------

    public function testCommitPathThreadsTxIdAndReturnsClosureValue(): void
    {
        $s = new FakeSession();
        $s->push(FakeSession::beginOk(7))
          ->push(FakeSession::execOk([
              'cols' => [], 'rows' => [], 'affected' => 1, 'last_insert_id' => null,
              'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
          ]))
          ->push(FakeSession::controlOk()); // COMMIT ack

        $conn = $this->connection([$s], RetryPolicy::default());

        $result = $conn->transaction(function (TxHandle $tx): string {
            $affected = $tx->exec('INSERT INTO t (x) VALUES (?)', [1]);
            $this->assertSame(1, $affected);
            return 'committed';
        });

        $this->assertSame('committed', $result);
        $this->assertSame([C::SERVICE_TX, C::METHOD_TX_BEGIN], [$s->sent[0][0], $s->sent[0][1]]);
        $this->assertSame([C::SERVICE_SQL, C::METHOD_SQL_EXEC], [$s->sent[1][0], $s->sent[1][1]]);
        $this->assertSame([C::SERVICE_TX, C::METHOD_TX_COMMIT], [$s->sent[2][0], $s->sent[2][1]]);

        // The mid-tx EXEC and the COMMIT both carry the BEGIN's tx_id (tx-scoped routing).
        $dec = PackerFactory::forDecode();
        $off = 0;
        $execWire = $dec->unpack($s->sent[1][2], $off);
        $this->assertIsArray($execWire);
        $this->assertSame(7, ExecRequest::mapFromWire($execWire)['tx_id']);
        $off = 0;
        $commitWire = $dec->unpack($s->sent[2][2], $off);
        $this->assertIsArray($commitWire);
        $this->assertSame(7, TxControl::mapFromWire($commitWire)['tx_id']);
    }

    // ---- closure throws → ROLLBACK + rethrow ----------------------------------------------------

    public function testThrownClosureExceptionRollsBackAndRethrows(): void
    {
        $s = new FakeSession();
        $s->push(FakeSession::beginOk(3))->push(FakeSession::controlOk()); // BEGIN, then ROLLBACK ack

        $conn = $this->connection([$s], RetryPolicy::default());
        $calls = 0;

        try {
            $conn->transaction(function (TxHandle $tx) use (&$calls): void {
                $calls++;
                throw new \RuntimeException('application error');
            });
            $this->fail('expected the application error to propagate');
        } catch (\RuntimeException $e) {
            $this->assertSame('application error', $e->getMessage());
        }

        $this->assertSame(1, $calls, 'an application error is not retried');
        $this->assertSame([C::SERVICE_TX, C::METHOD_TX_BEGIN], [$s->sent[0][0], $s->sent[0][1]]);
        $this->assertSame([C::SERVICE_TX, C::METHOD_TX_ROLLBACK], [$s->sent[1][0], $s->sent[1][1]], 'a ROLLBACK frame was written');
    }

    /** A NonRetryable thrown from the closure also rolls back and propagates (never re-run). */
    public function testNonRetryableClosureErrorRollsBackAndPropagates(): void
    {
        $s = new FakeSession();
        $s->push(FakeSession::beginOk(3))->push(FakeSession::controlOk());
        $conn = $this->connection([$s], RetryPolicy::default());
        $calls = 0;

        $this->expectException(NonRetryableException::class);
        try {
            $conn->transaction(function (TxHandle $tx) use (&$calls): void {
                $calls++;
                throw new NonRetryableException(self::nonRetryablePayload());
            });
        } finally {
            $this->assertSame(1, $calls);
            $this->assertSame([C::SERVICE_TX, C::METHOD_TX_ROLLBACK], [$s->sent[1][0], $s->sent[1][1]]);
        }
    }

    // ---- the lost-COMMIT carve-out (§19.3-CRITICAL) ---------------------------------------------

    public function testLostCommitIsIndeterminateAndNeverReRunsTheClosure(): void
    {
        $s = new FakeSession();
        $s->push(FakeSession::beginOk(9))              // BEGIN ok
          ->push(new ConnectionLostException('link dropped during COMMIT')); // COMMIT loses the link

        // A generous budget: prove the closure still does NOT re-run despite maxAttempts=5.
        $conn = $this->connection([$s, new FakeSession(2)], new RetryPolicy(maxAttempts: 5));
        $calls = 0;

        try {
            $conn->transaction(function (TxHandle $tx) use (&$calls): string {
                $calls++;
                return 'done'; // closure succeeds; the loss is on the COMMIT frame
            });
            $this->fail('expected an IndeterminateException for the lost COMMIT');
        } catch (IndeterminateException $e) {
            $this->assertSame(C::ERR_WRITE_UNCONFIRMED, $e->errorCode());
            $this->assertSame(C::BRANCH_INDETERMINATE, $e->branch());
            // No reconnect has happened yet on this connection — the honest generic label (M1-S4 T5:
            // cause() is a client-side inference, never a wire field, and never a fabricated "timeout").
            $this->assertSame(IndeterminateException::CAUSE_LINK_LOST, $e->cause());
        }

        $this->assertSame(1, $calls, 'a lost COMMIT must NEVER re-run the closure');
        $this->assertSame(0, $conn->reconnectCount(), 'a lost COMMIT must not reconnect+retry');
        // The session confirms the last in-flight frame was TX/COMMIT — the carve-out's signal.
        $this->assertSame([C::SERVICE_TX, C::METHOD_TX_COMMIT], $s->lastInFlight());
    }

    /**
     * The OTHER Indeterminate path (contrast with the lost-link COMMIT above): the engine actually
     * REPLIES to COMMIT with `Outcome::Error{WRITE_UNCONFIRMED, Indeterminate}` (no dropped link).
     * `cause()` is `engine_reported` (M1-S4 T5) — never `link_lost`/`engine_restart`, which are
     * reserved for the no-response inference — and the closure still never re-runs.
     */
    public function testEngineReportedIndeterminateCommitCauseIsEngineReported(): void
    {
        $ep = new ErrorPayload(C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, null, null, 'commit outcome unconfirmed', null, null);
        $s = new FakeSession();
        $s->push(FakeSession::beginOk(11))->push(FakeSession::errorOutcome($ep)); // COMMIT reply, not a lost link

        $conn = $this->connection([$s, new FakeSession(2)], new RetryPolicy(maxAttempts: 5));
        $calls = 0;

        try {
            $conn->transaction(function (TxHandle $tx) use (&$calls): string {
                $calls++;
                return 'done';
            });
            $this->fail('expected an IndeterminateException for the engine-reported COMMIT');
        } catch (IndeterminateException $e) {
            $this->assertSame(IndeterminateException::CAUSE_ENGINE_REPORTED, $e->cause());
        }

        $this->assertSame(1, $calls, 'an engine-reported Indeterminate COMMIT must NEVER re-run the closure');
        $this->assertSame(0, $conn->reconnectCount());
    }

    // ---- RetryPolicy honored within its attempt bound -------------------------------------------

    public function testRetryPolicyReRunsClosureWithinAttemptBound(): void
    {
        // Two attempts scripted on ONE live session: a retryable (serialization) abort re-runs in place.
        $s = new FakeSession();
        $s->push(FakeSession::beginOk(1))->push(FakeSession::controlOk())  // attempt 1: BEGIN, ROLLBACK
          ->push(FakeSession::beginOk(2))->push(FakeSession::controlOk()); // attempt 2: BEGIN, ROLLBACK

        $conn = $this->connection([$s], new RetryPolicy(maxAttempts: 2));
        $calls = 0;

        try {
            $conn->transaction(function (TxHandle $tx) use (&$calls): void {
                $calls++;
                throw new RetryableException(self::retryablePayload());
            });
            $this->fail('expected the RetryableException to propagate after the budget is spent');
        } catch (RetryableException $e) {
            $this->assertSame(C::BRANCH_RETRYABLE, $e->branch());
        }

        $this->assertSame(2, $calls, 'the closure re-ran exactly maxAttempts times');
        $this->assertSame(0, $conn->reconnectCount(), 'a live-session retryable abort re-runs WITHOUT reconnecting');
    }

    // ---- mid-tx connection loss → reconnect + re-run; epoch change surfaced ----------------------

    public function testMidTxLossReconnectsAndReRunsThenSurfacesEpochChange(): void
    {
        $s1 = new FakeSession(1); // initial epoch 1
        $s1->push(FakeSession::beginOk(1))->push(new ConnectionLostException('lost mid-statement'));
        $s2 = new FakeSession(2); // reconnected epoch 2 (CHANGED)
        $s2->push(FakeSession::beginOk(2))->push(new ConnectionLostException('lost mid-statement again'));

        $conn = $this->connection([$s1, $s2], new RetryPolicy(maxAttempts: 2));
        $calls = 0;

        try {
            $conn->transaction(function (TxHandle $tx) use (&$calls): void {
                $calls++;
                $tx->exec('INSERT INTO t DEFAULT VALUES'); // dies mid-tx → tx is void
            });
            $this->fail('expected an EpochChangedException after the budget is spent');
        } catch (EpochChangedException $e) {
            $this->assertTrue($e->epochChanged());
        }

        $this->assertSame(2, $calls, 'the whole closure re-ran on the new epoch');
        $this->assertSame(1, $conn->reconnectCount(), 'one reconnect between the two attempts');
        $this->assertTrue($conn->lastReconnectEpochChanged(), 'the reconnect observed a changed epoch (tx void)');
    }
}
