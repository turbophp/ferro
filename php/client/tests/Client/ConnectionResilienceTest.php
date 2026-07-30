<?php // /php/client/tests/Client/ConnectionResilienceTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Backoff;
use Ferro\Client\Connection;
use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\RetryPolicy;
use Ferro\Client\SessionInterface;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * The autocommit read-retry path (SPEC §19.1, `retry_reads`): a lost READ transparently reconnects
 * (epoch-aware) and re-issues, bounded by the policy; a lost WRITE is Indeterminate and NEVER
 * retries; and with `retry_reads = false` even a lost read just propagates.
 */
final class ConnectionResilienceTest extends TestCase
{
    private static function instantBackoff(): Backoff
    {
        return new Backoff(0.0, 0.0, rng: static fn (): float => 0.0, sleep: static function (float $s): void {});
    }

    /** @param list<FakeSession> $sessions */
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

    /** A lost READ transparently reconnects onto a changed epoch and re-issues, returning the value. */
    public function testLostReadReconnectsAndRetriesSucceeds(): void
    {
        $dead = new FakeSession(1);
        $dead->push(new ConnectionLostException('daemon went away'));
        $fresh = new FakeSession(2); // restarted daemon → changed epoch
        $fresh->push(FakeSession::scalarRow(1));

        $conn = $this->connection([$dead, $fresh], RetryPolicy::default());

        $this->assertSame(1, $conn->scalar('SELECT 1'));
        $this->assertSame(1, $conn->reconnectCount());
        $this->assertTrue($conn->lastReconnectEpochChanged(), 'the epoch changed on reconnect');
        $this->assertSame(2, $conn->currentEpoch());
    }

    /** With retry_reads=false a lost read is NOT retried — it surfaces as Retryable and propagates. */
    public function testLostReadDoesNotRetryWhenRetryReadsDisabled(): void
    {
        $dead = new FakeSession(1);
        $dead->push(new ConnectionLostException('daemon went away'));

        $conn = $this->connection([$dead, new FakeSession(2)], new RetryPolicy(retryReads: false, maxAttempts: 3));

        $this->expectException(RetryableException::class);
        try {
            $conn->scalar('SELECT 1');
        } finally {
            $this->assertSame(0, $conn->reconnectCount(), 'retry_reads=false must not reconnect a read');
        }
    }

    /** A lost autocommit WRITE is Indeterminate and NEVER retried, even with a budget. */
    public function testLostWriteIsIndeterminateNeverRetried(): void
    {
        $dead = new FakeSession(1);
        $dead->push(new ConnectionLostException('link dropped mid-write'));

        $conn = $this->connection([$dead, new FakeSession(2)], new RetryPolicy(maxAttempts: 3));

        try {
            $conn->exec('UPDATE accounts SET balance = balance + 1 WHERE id = ?', [42]);
            $this->fail('expected an IndeterminateException for a lost write');
        } catch (IndeterminateException $e) {
            $this->assertSame(C::ERR_WRITE_UNCONFIRMED, $e->errorCode());
            // No reconnect has happened on this connection yet — the honest generic label, not a
            // fabricated "timeout" (M1-S4 T5: cause() is a client-side inference, not a wire field).
            $this->assertSame(IndeterminateException::CAUSE_LINK_LOST, $e->cause());
        }
        $this->assertSame(0, $conn->reconnectCount(), 'a lost write must never reconnect+retry (§19.3)');
    }

    /**
     * A lost autocommit WRITE with no response, on a connection that has ALREADY seen the daemon
     * restart (an earlier read's reconnect observed a changed `boot_epoch`), is labeled
     * `cause() == engine_restart` — the client's best honest inference, still NEVER retried.
     */
    public function testLostWriteAfterKnownEpochChangeCauseIsEngineRestart(): void
    {
        $s1 = new FakeSession(1);
        $s1->push(new ConnectionLostException('daemon went away')); // read lost
        $s2 = new FakeSession(2); // restarted daemon → changed epoch
        $s2->push(FakeSession::scalarRow(1))                        // read retried, succeeds
           ->push(new ConnectionLostException('link dropped mid-write')); // a later write is lost, no response

        $conn = $this->connection([$s1, $s2], new RetryPolicy(maxAttempts: 3));

        // First: a lost read transparently reconnects, observing the changed epoch.
        $this->assertSame(1, $conn->scalar('SELECT 1'));
        $this->assertTrue($conn->lastReconnectEpochChanged());

        // Then: a lost WRITE with no response, on the same (now-known-restarted) connection.
        try {
            $conn->exec('UPDATE accounts SET balance = balance + 1 WHERE id = ?', [42]);
            $this->fail('expected an IndeterminateException for a lost write');
        } catch (IndeterminateException $e) {
            $this->assertSame(C::ERR_WRITE_UNCONFIRMED, $e->errorCode());
            $this->assertSame(IndeterminateException::CAUSE_ENGINE_RESTART, $e->cause());
        }
        $this->assertSame(1, $conn->reconnectCount(), 'the lost write itself must never reconnect+retry (§19.3)');
    }

    /** Read retry is bounded: a persistently-down daemon exhausts the budget and then propagates. */
    public function testReadRetryIsBoundedByMaxAttempts(): void
    {
        $s0 = (new FakeSession(1))->push(new ConnectionLostException('down'));
        $s1 = (new FakeSession(1))->push(new ConnectionLostException('down'));
        $s2 = (new FakeSession(1))->push(new ConnectionLostException('down'));

        $conn = $this->connection([$s0, $s1, $s2], new RetryPolicy(maxAttempts: 3));

        $this->expectException(RetryableException::class);
        try {
            $conn->scalar('SELECT 1');
        } finally {
            // maxAttempts=3 total sends ⇒ 2 reconnects between them.
            $this->assertSame(2, $conn->reconnectCount());
        }
    }
}
