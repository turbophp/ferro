<?php // /php/client/tests/Client/ConnectionBackupTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\Error\TransportException;
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
     * A link lost mid-BACKUP is a lost READ: Retryable, never Indeterminate — a backup writes nothing
     * to the source database, and a retry without `replace` answers "already exists" if the first
     * attempt had completed.
     */
    public function testALostBackupIsRetryableNotIndeterminate(): void
    {
        $session = (new FakeSession())->push(new ConnectionLostException('peer closed'));
        $this->expectException(RetryableException::class);
        (new Connection($session, 'lite'))->backup('a.db');
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
