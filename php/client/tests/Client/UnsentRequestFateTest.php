<?php // /php/client/tests/Client/UnsentRequestFateTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Backoff;
use Ferro\Client\Connection;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\FateClassifier;
use Ferro\Client\OpKind;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\RetryPolicy;
use Ferro\Client\Session;
use Ferro\Client\TxHandle;
use Ferro\Protocol\BeginResponse;
use Ferro\Protocol\Codec;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\StreamHead;
use Ferro\Tests\Support\FakeSession;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * M2-C1e-3: a request whose frame never completely left the client was never dispatched, so its
 * loss is `Retryable` — SPEC §19.3's engine-side "not-yet-dispatched → Retryable", applied to the
 * client's half of the link. A request that WAS written and then lost its reply keeps every fate it
 * had (the controls).
 *
 * The defect this closes was measured through the Laravel tier: after a `ferrod` restart, every
 * statement on a long-lived connection failed `Indeterminate` — "autocommit write lost mid-flight …
 * write failed after 0 of 47 bytes" — although zero bytes had been written, so nothing above the
 * client could ever safely recover the connection.
 */
final class UnsentRequestFateTest extends TestCase
{
    private const SHORT_WRITE = 'write failed after 0 of 47 bytes';

    private static function connection(FakeTransport $t): Connection
    {
        return new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
    }

    /** The defect, through a real `Session`: a write that fails is Retryable, not Indeterminate. */
    public function testAnAutocommitWriteThatFailsWhileBeingWrittenIsRetryable(): void
    {
        $t = new FakeTransport();
        $t->failNextWrite = new TransportException(self::SHORT_WRITE);

        try {
            self::connection($t)->exec('INSERT INTO t VALUES (1)');
            self::fail('a failed write must surface');
        } catch (RetryableException $e) {
            self::assertSame(C::ERR_CONNECTION_LOST, $e->errorCode());
            self::assertStringContainsString('not sent', $e->getMessage());
        }
        self::assertTrue($t->closed, 'a failed write must close the socket, so no later frame can complete it');
    }

    /** The control: the request WAS written and the reply never came — the fate stays unknown. */
    public function testAWriteWhoseReplyIsLostStaysIndeterminate(): void
    {
        $t = new FakeTransport(); // nothing queued: the write succeeds, the read finds nothing
        $this->expectException(IndeterminateException::class);
        self::connection($t)->exec('INSERT INTO t VALUES (1)');
    }

    /**
     * After ANY transport failure the session is closed and writes nothing more. That is what makes
     * the first test's claim sound: a later frame cannot complete an earlier partial one, and a
     * later request cannot read an earlier one's late reply.
     */
    public function testAPoisonedSessionRefusesBeforeWritingAndThatIsNotSent(): void
    {
        $t = new FakeTransport();
        $c = self::connection($t);
        try {
            $c->exec('INSERT INTO t VALUES (1)'); // written, reply lost → Indeterminate
            self::fail('the reply is missing');
        } catch (IndeterminateException) {
        }
        self::assertTrue($t->closed, 'a failed READ must close the socket too');
        $writes = $t->writeCalls;

        try {
            $c->exec('INSERT INTO t VALUES (2)');
            self::fail('a poisoned session must refuse');
        } catch (RetryableException $e) {
            self::assertStringContainsString('closed after an earlier transport failure', $e->getMessage());
        }
        self::assertSame($writes, $t->writeCalls, 'the refused request must not have touched the socket');
    }

    /** A streamed write's OPEN is classified like every buffered path (review finding F6). */
    public function testAStreamOpenThatFailsWhileBeingWrittenIsRetryable(): void
    {
        $t = new FakeTransport();
        $t->failNextWrite = new TransportException(self::SHORT_WRITE);
        $this->expectException(RetryableException::class);
        self::connection($t)->streamRaw('INSERT INTO t VALUES (1) RETURNING id', [], readonly: false);
    }

    /** …and a streamed write whose open reply is lost is Indeterminate, not a raw transport error. */
    public function testAStreamOpenWhoseReplyIsLostIsIndeterminateNotRaw(): void
    {
        $t = new FakeTransport();
        $this->expectException(IndeterminateException::class);
        self::connection($t)->streamRaw('INSERT INTO t VALUES (1) RETURNING id', [], readonly: false);
    }

    /**
     * The COMMIT carve-out is for a COMMIT frame that was SENT. One that never fully left the client
     * cannot have committed: Retryable. The sent one stays the one transactional Indeterminate.
     */
    public function testAnUnsentCommitIsRetryableAndASentOneStaysIndeterminate(): void
    {
        $unsent = FakeSession::withTxBegin(txId: 9)->push(
            TransportException::requestNotSent(self::SHORT_WRITE),
            [C::SERVICE_TX, C::METHOD_TX_COMMIT],
        );
        $c = new Connection($unsent, 'default');
        $c->begin();
        try {
            $c->commit();
            self::fail('the commit failed');
        } catch (RetryableException) {
        }

        $sent = FakeSession::withTxBegin(txId: 10)->thenThrowOnCommit();
        $c = new Connection($sent, 'default');
        $c->begin();
        $this->expectException(IndeterminateException::class);
        $c->commit();
    }

    /** Frame a server reply onto the fake transport. */
    private static function feed(FakeTransport $t, int $flags, int $service, int $method, int $rid, string $payload): void
    {
        $t->feed((new Codec())->encodeFrame(new Header($flags, $service, $method, $rid, strlen($payload)), $payload));
    }

    /** A stream whose HEAD arrived (rid 1) and whose next frame never will. */
    private static function headThenSilence(): FakeTransport
    {
        $t = new FakeTransport();
        self::feed($t, 0, C::SERVICE_STREAM, C::METHOD_STREAM_HEAD, 1, StreamHead::encode(
            ['cols' => [['name' => 'id', 'tag' => C::TAG_I64]]],
            PackerFactory::forEncode(),
        ));
        return $t;
    }

    /**
     * Review F5: a transport failure WHILE A STREAM IS OPEN must leave the session reporting "not
     * sent" to the next request — not `ProtocolException` "a stream is open", which nothing above
     * can recognise as a dead session. Reproduced live first: a Laravel `cursor()` across a
     * `ferrod` restart left the connection unable to recover.
     */
    public function testAFailureMidStreamLeavesTheSessionReportingNotSentNotAnOpenStream(): void
    {
        $t = self::headThenSilence();
        $c = self::connection($t);
        $stream = $c->streamRaw('SELECT id FROM t', [], readonly: true);
        try {
            foreach ($stream->rows() as $_) {
            }
            self::fail('the stream died mid-flight');
        } catch (TransportException) {
        }
        $stream->close(); // must not throw: nothing to cancel on a closed socket (review F2/F5)

        try {
            $c->exec('INSERT INTO t VALUES (1)');
            self::fail('the session is dead');
        } catch (RetryableException $e) {
            self::assertStringContainsString('not sent', $e->getMessage());
        } catch (ProtocolException $e) {
            self::fail('a dead session still claimed an open stream: ' . $e->getMessage());
        }
    }

    /**
     * Review F2: a CONTROL frame's failure is not "request not sent" — a WINDOW_UPDATE mid-stream
     * follows rows the engine already produced, so the statement HAS run. Only request frames carry
     * the flag; a caller that trusted it on a control frame would be told a lie.
     */
    public function testAControlFrameFailureIsNotMarkedUnsent(): void
    {
        $t = self::headThenSilence();
        $session = new Session($t, new RequestIdAllocator(0));
        $session->openStream(C::SERVICE_SQL, C::METHOD_SQL_EXEC, '');
        $t->failNextWrite = new TransportException('write failed after 0 of 30 bytes');
        try {
            $session->sendWindowUpdate(1, 1, 100);
            self::fail('the write failed');
        } catch (TransportException $e) {
            self::assertFalse($e->requestUnsent(), 'a WINDOW_UPDATE is about a request that already ran');
        }
        try {
            $session->sendCancel(1); // the session is poisoned now
            self::fail('a poisoned session refuses');
        } catch (TransportException $e) {
            self::assertFalse($e->requestUnsent(), 'a refused CANCEL is not a request either');
        }
    }

    /**
     * Review F4: with a reconnect loop, a session a failure already closed is REPLACED before the
     * next request — so a write-only caller recovers. Not a retry: the write is sent once, on the new
     * session. The control is the same sequence with no loop, which can only say "not sent" again.
     */
    public function testAPoisonedSessionIsReplacedBeforeTheNextRequest(): void
    {
        $first = (new FakeSession())->push(TransportException::requestNotSent('write failed after 0 of 47 bytes'));
        $second = (new FakeSession())->thenExecOk();
        $loop = new ReconnectLoop($first, static fn (): FakeSession => $second, new Backoff(0, 0, rng: static fn (): float => 0.0, sleep: static function (float $_): void {}), 1);
        $c = new Connection(session: $first, pool: 'default', reconnect: $loop, policy: RetryPolicy::none());

        try {
            $c->exec('INSERT INTO t VALUES (1)');
            self::fail('the first write was not sent');
        } catch (RetryableException) {
        }
        self::assertSame(0, $loop->reconnectCount(), 'a write is never retried by the client');

        $c->exec('INSERT INTO t VALUES (2)');
        self::assertSame(1, $loop->reconnectCount(), 'the dead session was replaced before the write');
        self::assertSame(1, $second->sendCount(), 'the second write went out exactly once, on the new session');

        $alone = new Connection((new FakeSession())->push(TransportException::requestNotSent('x')), 'default');
        try {
            $alone->exec('INSERT INTO t VALUES (1)');
        } catch (RetryableException) {
        }
        $this->expectException(RetryableException::class); // control: no loop, nothing to recover with
        $alone->exec('INSERT INTO t VALUES (2)');
    }

    /** Review F3: `stream()` (the native read stream) is classified too, not only `streamRaw()`. */
    public function testTheNativeStreamOpenIsClassified(): void
    {
        $t = new FakeTransport();
        $t->failNextWrite = new TransportException('write failed after 0 of 47 bytes');
        $this->expectException(RetryableException::class);
        foreach (self::connection($t)->stream('SELECT 1') as $_) {
        }
    }

    /**
     * Review F3: an in-transaction stream whose open reply is lost is `TxStatement` — the transaction
     * is dead, its fate KNOWN — so `Retryable` even though the frame WAS sent. The same statement in
     * autocommit is `Indeterminate` ({@see testAStreamOpenWhoseReplyIsLostIsIndeterminateNotRaw}),
     * so this row is decided by the transaction, not by the transport.
     */
    public function testAnInTransactionStreamOpenLossIsATxStatement(): void
    {
        $t = new FakeTransport();
        $packer = PackerFactory::forEncode();
        self::feed($t, C::FLAG_END, C::SERVICE_TX, C::METHOD_TX_BEGIN, 1,
            Outcome::ok(BeginResponse::encode(['tx_id' => 5], $packer))->encode($packer));
        $c = self::connection($t);
        $c->begin();
        $this->expectException(RetryableException::class);
        $c->streamRaw('INSERT INTO t VALUES (1) RETURNING id', [], readonly: false); // written; reply lost
    }

    /**
     * Review F3: the CLOSURE form's COMMIT site honours the axis too — unsent is `Retryable`, sent
     * stays `Indeterminate`.
     */
    public function testTheClosureFormCommitHonoursTheSentAxis(): void
    {
        $unsent = FakeSession::withTxBegin(txId: 11)->push(
            TransportException::requestNotSent(self::SHORT_WRITE),
            [C::SERVICE_TX, C::METHOD_TX_COMMIT],
        );
        try {
            (new Connection($unsent, 'default'))->transaction(static fn (TxHandle $tx): int => 1);
            self::fail('the commit failed');
        } catch (RetryableException) {
        }

        $sent = FakeSession::withTxBegin(txId: 12)->thenThrowOnCommit();
        $this->expectException(IndeterminateException::class);
        (new Connection($sent, 'default'))->transaction(static fn (TxHandle $tx): int => 1);
    }

    /**
     * Review F1: the closure form's BEGIN passed the sent flag into the `$epochChanged` slot, so an
     * unsent BEGIN was described as a "connection lost" rather than "not sent". The class was right
     * either way (BEGIN is always Retryable) — the MESSAGE is what this pins.
     */
    public function testTheClosureFormBeginIsDescribedAsNotSent(): void
    {
        $s = (new FakeSession())->push(TransportException::requestNotSent(self::SHORT_WRITE), [C::SERVICE_TX, C::METHOD_TX_BEGIN]);
        try {
            (new Connection($s, 'default'))->transaction(static fn (TxHandle $tx): int => 1, RetryPolicy::none());
            self::fail('the BEGIN failed');
        } catch (RetryableException $e) {
            self::assertStringContainsString('not sent', $e->getMessage());
        }
    }

    /**
     * `sent: false` decides for EVERY kind — and `sent: true` (the default) leaves each kind's fate
     * exactly as it was, which the second half asserts so the new axis cannot have moved anything.
     *
     * @return iterable<string,array{OpKind,bool,class-string}>
     */
    public static function kinds(): iterable
    {
        foreach (OpKind::cases() as $k) {
            yield "{$k->value}, not sent" => [$k, false, RetryableException::class];
        }
        yield 'write, sent' => [OpKind::Write, true, IndeterminateException::class];
        yield 'commit, sent' => [OpKind::TxCommit, true, IndeterminateException::class];
        yield 'read, sent' => [OpKind::Read, true, RetryableException::class];
        yield 'tx statement, sent' => [OpKind::TxStatement, true, RetryableException::class];
    }

    /** @param class-string $expected */
    #[DataProvider('kinds')]
    public function testClassifyLossHonoursTheSentAxis(OpKind $kind, bool $sent, string $expected): void
    {
        $fate = (new FateClassifier())->classifyLoss($kind, $kind === OpKind::Read, 'lost', null, false, $sent);
        self::assertInstanceOf($expected, $fate);
    }

    /** Not-sent is Retryable, and still never re-issued BY THE CLIENT: that decision stays the caller's. */
    public function testTheClientStillNeverRetriesAWriteItself(): void
    {
        $f = new FateClassifier();
        $fate = $f->classifyLoss(OpKind::Write, false, 'lost', null, false, false);
        self::assertFalse($f->mayRetryException($fate, false, OpKind::Write));
    }
}
