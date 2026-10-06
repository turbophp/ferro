<?php // /php/client/tests/Client/AsyncFateTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Backoff;
use Ferro\Client\Connection;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\RetryPolicy;
use Ferro\Client\Session;
use Ferro\Protocol\BeginResponse;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * M3-D1a review F8: the asynchronous path's FATE rules, driven through `Future::await` over a real
 * {@see Session}. Each test is the one the review showed missing: with the rule under test broken,
 * every other test stayed green.
 */
final class AsyncFateTest extends TestCase
{
    private static function frame(int $flags, int $service, int $method, int $rid, string $payload): string
    {
        return (new Codec())->encodeFrame(new Header($flags, $service, $method, $rid, strlen($payload)), $payload);
    }

    /** A HELLO_ACK, so a Session has a boot epoch and a ReconnectLoop can be built over it. */
    private static function helloAck(int $epoch): string
    {
        $payload = Message::encode('hello_ack', [
            'engine_version' => 1,
            'boot_epoch' => $epoch,
            'features' => 0,
            'pools' => [['name' => 'default', 'kind' => 'postgres', 'server_version' => null, 'literals_are_standard' => true]],
            'type_registry_hash' => C::TYPE_REGISTRY_HASH,
        ], PackerFactory::forEncode());
        return self::frame(0, C::SERVICE_CORE, C::METHOD_CORE_HELLO_ACK, 0, $payload);
    }

    /** @param list<list<array{tag:int,data:mixed}>> $rows */
    private static function execOk(int $rid, array $rows, ?int $key = null): string
    {
        $packer = PackerFactory::forEncode();
        $body = ExecOk::encode([
            'cols' => $rows === [] ? [] : [['name' => 'n', 'tag' => C::TAG_I64]],
            'rows' => $rows,
            'affected' => 1,
            'last_insert_id' => $key === null ? null : ['tag' => C::TAG_I64, 'data' => $key],
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => count($rows), 'bytes' => 0],
        ], $packer);
        return self::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, Outcome::ok($body)->encode($packer));
    }

    /** A Session over `$t`, handshaken against the HELLO_ACK the caller already fed. */
    private static function handshaken(FakeTransport $t): Session
    {
        $session = new Session($t, new RequestIdAllocator(0));
        $session->hello();
        return $session;
    }

    /**
     * @param list<FakeTransport> $fresh transports for the sessions a reconnect would dial
     * @return array{0: Connection, 1: \ArrayObject<int, int>}
     */
    private static function resilient(FakeTransport $first, array $fresh): array
    {
        $dials = new \ArrayObject();
        $loop = new ReconnectLoop(
            (static function () use ($first): Session {
                $first->feed(self::helloAck(1));
                return self::handshaken($first);
            })(),
            static function () use (&$fresh, $dials): Session {
                $dials->append(1);
                $t = array_shift($fresh);
                self::assertNotNull($t, 'no more sessions to dial');
                return self::handshaken($t); // the test fed its HELLO_ACK and replies
            },
            new Backoff(0, 0, rng: static fn (): float => 0.0, sleep: static function (float $_): void {}),
            1,
        );
        $conn = new Connection(session: $loop->session(), pool: 'default', reconnect: $loop, policy: new RetryPolicy(maxAttempts: 3, baseDelaySeconds: 0.0, maxDelaySeconds: 0.0));
        return [$conn, $dials];
    }

    /**
     * A sent async WRITE whose reply is lost is Indeterminate, and is NEVER sent again — even with a
     * reconnect loop and a retry budget available (review mutations (i) and (l)).
     */
    public function testALostAsyncWriteIsIndeterminateAndNeverReissued(): void
    {
        $first = new FakeTransport();
        $second = new FakeTransport();
        [$conn, $dials] = self::resilient($first, [$second]);

        $future = $conn->execAsync('INSERT INTO t VALUES (1)');
        $writesBefore = $first->writeCalls + $second->writeCalls;
        // Nothing is fed for the request: the read hits EOF.
        try {
            $future->await();
            $this->fail('a lost write must not succeed');
        } catch (IndeterminateException $e) {
            $this->assertSame(C::BRANCH_INDETERMINATE, $e->errorPayload()->branch);
        }
        $this->assertSame($writesBefore, $first->writeCalls + $second->writeCalls, 'the write was not sent again');
        $this->assertCount(0, $dials, 'no reconnect was made to re-send it');
    }

    /** The control: a lost async READ is re-issued once on a fresh session and answers from there. */
    public function testALostAsyncReadIsReissuedOnAFreshSession(): void
    {
        $first = new FakeTransport();
        $second = new FakeTransport();
        [$conn, $dials] = self::resilient($first, [$second]);

        $future = $conn->scalarAsync('SELECT 7');
        // The fresh session's HELLO_ACK, then its first request (id 1) answered.
        $second->feed(self::helloAck(1));
        $second->feed(self::execOk(1, [[['tag' => C::TAG_I64, 'data' => 7]]]));

        $this->assertSame(7, $future->await());
        $this->assertCount(1, $dials);
    }

    /**
     * Review F4: no async path touches lastInsertId() — not the multiplexed one, and not the one that
     * settles at once inside a transaction.
     */
    public function testAsyncStatementsNeverTouchLastInsertId(): void
    {
        $t = new FakeTransport();
        $packer = PackerFactory::forEncode();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');

        $t->feed(self::execOk(1, [], key: 5));
        $conn->exec('INSERT INTO t VALUES (1)');
        $this->assertSame(5, $conn->lastInsertId());

        $t->feed(self::execOk(2, [], key: 9));
        $conn->execAsync('INSERT INTO t VALUES (2)')->await();
        $this->assertSame(5, $conn->lastInsertId(), 'the multiplexed async path left it alone');

        $t->feed(self::frame(C::FLAG_END, C::SERVICE_TX, C::METHOD_TX_BEGIN, 3, Outcome::ok(BeginResponse::encode(['tx_id' => 77], $packer))->encode($packer)));
        $conn->begin();
        $t->feed(self::execOk(4, [], key: 11));
        $inTx = $conn->execAsync('INSERT INTO t VALUES (3)');
        $this->assertTrue($inTx->isSettled());
        $this->assertSame(5, $conn->lastInsertId(), 'the settle-at-once path inside a transaction left it alone too');
    }

    /** Review F7: an async call never throws at the call; a fault surfaces at await. */
    public function testAFaultWhileSubmittingSurfacesAtAwaitNotAtTheCall(): void
    {
        $t = new FakeTransport();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0), maxInFlight: 1), 'default');
        $first = $conn->execAsync('INSERT INTO t VALUES (1)');
        // While the second submit waits for a slot it reads a stray frame for an id never sent.
        $t->feed(self::execOk(42, []));

        $second = $conn->execAsync('INSERT INTO t VALUES (2)'); // must not throw here
        $this->assertTrue($second->isSettled());
        try {
            $second->await();
            $this->fail('the fault must surface at await');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(\Ferro\Client\Error\FerroException::class, $e);
        }
        unset($first);
    }

    /** Review F6, end to end: a Future dropped unawaited lets its terminal be thrown away. */
    public function testADroppedFutureDoesNotLeaveItsTerminalBehind(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $conn = new Connection($session, 'default');
        $conn->execAsync('INSERT INTO t VALUES (1)'); // dropped at once
        $kept = $conn->scalarAsync('SELECT 2');
        $t->feed(self::execOk(1, []) . self::execOk(2, [[['tag' => C::TAG_I64, 'data' => 2]]]));

        $this->assertSame(2, $kept->await());
        $inbox = new \ReflectionProperty(Session::class, 'inbox');
        $this->assertSame([], $inbox->getValue($session));
    }

    /** A ProtocolException at the call would break `Ferro\await`'s "all of them were awaited" rule. */
    public function testAwaitAwaitsEveryFutureThenThrowsTheFirstFailure(): void
    {
        $t = new FakeTransport();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
        $bad = $conn->scalarAsync('SELECT 1');
        $good = $conn->scalarAsync('SELECT 2');
        $packer = PackerFactory::forEncode();
        $err = new \Ferro\Protocol\ErrorPayload(C::ERR_SYNTAX, C::BRANCH_NON_RETRYABLE, '42601', null, 'syntax', null, null);
        $t->feed(self::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, Outcome::error($err)->encode($packer))
            . self::execOk(2, [[['tag' => C::TAG_I64, 'data' => 2]]]));
        try {
            \Ferro\await([$bad, $good]);
            $this->fail('the failure must be thrown');
        } catch (\Ferro\Client\Error\NonRetryableException) {
        }
        $this->assertTrue($good->isSettled(), 'the Future after the failure was awaited too');
        $this->assertSame(2, $good->await());
    }
}
