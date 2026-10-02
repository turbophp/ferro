<?php // /php/client/tests/Client/ConnectionBackupTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Backoff;
use Ferro\Client\Connection;
use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\RetryPolicy;
use Ferro\Protocol\BackupRequest;
use Ferro\Protocol\BackupResponse;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M2-C3-7b-2 — `Connection::backup()` against a scripted session: what goes on the wire, what comes
 * back, and how each failure is typed. The engine half is proven in `ferrod`'s `admin_it.rs`; the
 * live round trip in `BackupLiveTest`.
 */
final class ConnectionBackupTest extends TestCase
{
    /** @return array{pool:string,file:string,replace:bool,timeout_ms:?int} */
    private static function sent(FakeSession $session): array
    {
        $req = $session->lastRequest();
        self::assertSame(C::SERVICE_ADMIN, $req['service']);
        self::assertSame(C::METHOD_ADMIN_BACKUP, $req['method']);
        $off = 0;
        $w = PackerFactory::forEncode()->unpack($req['payload'], $off);
        self::assertSame(strlen($req['payload']), $off);
        return BackupRequest::mapFromWire(array_values((array) $w));
    }

    private static function ok(int $bytes, int $queueUs, int $execUs): Outcome
    {
        return Outcome::ok(BackupResponse::encode(
            ['bytes' => $bytes, 'queue_us' => $queueUs, 'exec_us' => $execUs],
            PackerFactory::forEncode(),
        ));
    }

    public function testTheRequestCarriesThePoolTheFileTheFlagAndTheBound(): void
    {
        $session = (new FakeSession())->push(self::ok(8192, 3, 1500), [C::SERVICE_ADMIN, C::METHOD_ADMIN_BACKUP]);
        $result = (new Connection($session, 'lite'))->backup('nightly.db', true, 30_000);

        self::assertSame(
            ['pool' => 'lite', 'file' => 'nightly.db', 'replace' => true, 'timeout_ms' => 30_000],
            self::sent($session),
        );
        self::assertSame(8192, $result->bytes);
        self::assertSame(3, $result->queueUs);
        self::assertSame(1500, $result->execUs);
    }

    public function testTheDefaultsAreNoReplaceAndNoBound(): void
    {
        $session = (new FakeSession())->push(self::ok(1, 0, 1));
        (new Connection($session, 'lite'))->backup('a.db');
        $sent = self::sent($session);
        self::assertFalse($sent['replace'], 'replace must be opted into');
        self::assertNull($sent['timeout_ms']);
    }

    /** A u64 size past 2^32 survives (the vector locks the wire width; this locks the PHP int). */
    public function testALargeSnapshotSizeIsANativeInt(): void
    {
        $session = (new FakeSession())->push(self::ok(5_000_000_000, 0, 1));
        self::assertSame(5_000_000_000, (new Connection($session, 'lite'))->backup('big.db')->bytes);
    }

    /** The D15 refusal surfaces as the engine sent it: NonRetryable, code FORBIDDEN. */
    public function testAForbiddenRefusalIsNonRetryableWithItsCode(): void
    {
        $session = (new FakeSession())->push(Outcome::error(new ErrorPayload(
            C::ERR_FORBIDDEN,
            C::BRANCH_NON_RETRYABLE,
            null,
            null,
            'admin verb BACKUP is an OPERATE verb and this peer is not authorized for OPERATE verbs',
            null,
            null,
        )));
        try {
            (new Connection($session, 'lite'))->backup('a.db');
            self::fail('a Forbidden terminal must throw');
        } catch (NonRetryableException $e) {
            self::assertSame(C::ERR_FORBIDDEN, $e->errorPayload()->code);
        }
    }

    /**
     * A link lost after the BACKUP went out is UNCONFIRMED: the engine may still publish the snapshot
     * (the C3-7b-2 review measured it, 500 ms after the client gave up), so it is a lost WRITE —
     * Indeterminate — and never Retryable.
     */
    public function testALostBackupIsIndeterminateNotRetryable(): void
    {
        $session = (new FakeSession())->push(new ConnectionLostException('peer closed'));
        $this->expectException(IndeterminateException::class);
        (new Connection($session, 'lite'))->backup('a.db');
    }

    /**
     * The client's own retry machinery never re-issues it: inside `transaction()` with the default
     * policy and a reconnect loop — the shape that re-ran the closure, and so re-sent the BACKUP, in
     * the review's probe — the backup goes out ONCE and the Indeterminate propagates.
     */
    public function testTransactionNeverReIssuesALostBackup(): void
    {
        $first = FakeSession::withTxBegin(1)->push(new ConnectionLostException('peer closed'));
        $second = FakeSession::withTxBegin(2)->push(self::ok(1, 0, 1));
        $loop = new ReconnectLoop($first, static fn (): FakeSession => $second, new Backoff(0, 0, rng: static fn (): float => 0.0, sleep: static function (float $_): void {}), 3);
        $c = new Connection(session: $first, pool: 'lite', reconnect: $loop, policy: RetryPolicy::default());
        try {
            $c->transaction(static fn () => $c->backup('nightly.db', true));
            self::fail('a lost backup must not be re-run');
        } catch (IndeterminateException) {
        }
        $backups = 0;
        foreach ([$first, $second] as $s) {
            for ($i = 0; $i < $s->sendCount(); $i++) {
                $backups += $s->sentAt($i)['service'] === C::SERVICE_ADMIN ? 1 : 0;
            }
        }
        self::assertSame(1, $backups, 'the BACKUP was sent exactly once');
    }

    /** The engine spoke a definite fate before the link died — that fate is reported, not guessed. */
    public function testAFateTheEngineReportedBeforeTheLinkDiedIsTrusted(): void
    {
        $session = (new FakeSession())->push(new ConnectionLostException('peer closed', new ErrorPayload(
            C::ERR_FORBIDDEN,
            C::BRANCH_NON_RETRYABLE,
            null,
            null,
            'refused',
            null,
            null,
        )));
        try {
            (new Connection($session, 'lite'))->backup('a.db');
            self::fail('must throw');
        } catch (NonRetryableException $e) {
            self::assertSame(C::ERR_FORBIDDEN, $e->errorPayload()->code);
        }
    }

    /** With a reconnect loop, a session a failure already closed is replaced BEFORE the backup is
     * sent — not a retry: it goes out once, on the new session. */
    public function testAPoisonedSessionIsReplacedBeforeTheBackup(): void
    {
        $first = (new FakeSession())->push(TransportException::requestNotSent('write failed after 0 bytes'));
        $second = (new FakeSession())->push(self::ok(4096, 0, 1));
        $loop = new ReconnectLoop($first, static fn (): FakeSession => $second, new Backoff(0, 0, rng: static fn (): float => 0.0, sleep: static function (float $_): void {}), 1);
        $c = new Connection(session: $first, pool: 'lite', reconnect: $loop, policy: RetryPolicy::none());
        try {
            $c->backup('a.db');
            self::fail('the first backup was not sent');
        } catch (RetryableException) {
        }
        self::assertSame(4096, $c->backup('a.db')->bytes);
        self::assertSame(1, $loop->reconnectCount());
        self::assertSame(1, $second->sendCount());
    }

    public function testAnUnsentBackupIsRetryable(): void
    {
        $session = (new FakeSession())->push(TransportException::requestNotSent('write failed after 0 bytes'));
        $this->expectException(RetryableException::class);
        (new Connection($session, 'lite'))->backup('a.db');
    }

    /** A malformed success body is a codec defect, reported as one — never a fabricated result. */
    public function testAMalformedSuccessBodyIsAProtocolError(): void
    {
        $p = PackerFactory::forEncode();
        foreach ([
            'wrong arity' => $p->packArrayLen(2) . $p->packUint(1) . $p->packUint(2),
            'not an array' => $p->packUint(7),
            'trailing bytes' => BackupResponse::encode(['bytes' => 1, 'queue_us' => 0, 'exec_us' => 1], $p) . $p->packNil(),
            // C3-7b-2 review F3: these used to decode as a success with invented numbers.
            'nils' => $p->packArrayLen(3) . $p->packNil() . $p->packNil() . $p->packNil(),
            'wrong types' => $p->packArrayLen(3) . $p->packStr('abc') . $p->packBool(true) . $p->packUint(1),
            'negative size' => $p->packArrayLen(3) . $p->packInt(-5) . $p->packUint(0) . $p->packUint(0),
            'u64 above PHP_INT_MAX' => $p->packArrayLen(3) . "\xcf\xff\xff\xff\xff\xff\xff\xff\xff" . $p->packUint(0) . $p->packUint(1),
        ] as $case => $body) {
            $session = (new FakeSession())->push(Outcome::ok($body));
            try {
                (new Connection($session, 'lite'))->backup('a.db');
                self::fail("{$case}: a malformed body must throw");
            } catch (ProtocolException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    /** A non-UTF-8 name is the caller's error, refused before anything is sent — not a wire fault. */
    public function testANonUtf8NameIsRefusedBeforeAnythingIsSent(): void
    {
        $session = new FakeSession();
        try {
            (new Connection($session, 'lite'))->backup("\xff.db");
            self::fail('a non-UTF-8 name must be refused');
        } catch (\InvalidArgumentException) {
            self::assertSame(0, $session->sendCount());
        }
    }

    public function testAnOutOfRangeBoundIsRefusedBeforeAnythingIsSent(): void
    {
        foreach ([-1, 0x1_0000_0000] as $bad) {
            $session = new FakeSession();
            try {
                (new Connection($session, 'lite'))->backup('a.db', false, $bad);
                self::fail("timeoutMs {$bad} must be refused");
            } catch (\InvalidArgumentException) {
                self::assertSame(0, $session->sendCount(), 'nothing was sent');
            }
        }
    }
}
