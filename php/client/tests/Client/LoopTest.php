<?php // /php/client/tests/Client/LoopTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\InvalidTransactionStateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Loop;
use Ferro\Protocol\BeginResponse;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\StreamData;
use Ferro\Protocol\StreamHead;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * M3-D1b (SPEC §10.1): `Ferro\Loop` runs tasks as Fibers, and an `await` inside one SUSPENDS it
 * until its terminal arrives, so the other tasks run meanwhile over the same socket.
 */
final class LoopTest extends TestCase
{
    private static function scalarOk(int $rid, int $value): string
    {
        $packer = PackerFactory::forEncode();
        $body = ExecOk::encode([
            'cols' => [['name' => 'n', 'tag' => C::TAG_I64]],
            'rows' => [[['tag' => C::TAG_I64, 'data' => $value]]],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ], $packer);
        $payload = Outcome::ok($body)->encode($packer);
        return (new Codec())->encodeFrame(new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, strlen($payload)), $payload);
    }

    /**
     * The claim. Each task submits, then awaits. With suspension both requests are written before
     * anything is read, and the task whose reply arrives FIRST finishes first. Under a blocking
     * await, task A would read until its own reply and finish first, and task B would not even
     * submit until A was done.
     */
    public function testAnAwaitInsideTheLoopSuspendsAndTheOtherTaskRuns(): void
    {
        $t = new FakeTransport();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
        // B's reply (request 2) arrives before A's (request 1).
        $t->feed(self::scalarOk(2, 20) . self::scalarOk(1, 10));

        $finished = [];
        $results = Loop::run([
            'a' => static function () use ($conn, &$finished): mixed {
                $v = $conn->scalarAsync('SELECT 10')->await();
                $finished[] = 'a';
                return $v;
            },
            'b' => static function () use ($conn, &$finished): mixed {
                $v = $conn->scalarAsync('SELECT 20')->await();
                $finished[] = 'b';
                return $v;
            },
        ]);

        $this->assertSame(['a' => 10, 'b' => 20], $results);
        $this->assertSame(['b', 'a'], $finished, 'the task whose reply came first finished first');
        $this->assertSame(['w', 'w'], array_slice($t->events, 0, 2), 'both requests were written before any read');
    }

    /** A failing task does not stop the others; the first failure (in task order) is thrown at the end. */
    public function testAFailingTaskDoesNotStopTheOthers(): void
    {
        $t = new FakeTransport();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
        $packer = PackerFactory::forEncode();
        $err = Outcome::error(new ErrorPayload(C::ERR_SYNTAX, C::BRANCH_NON_RETRYABLE, '42601', null, 'syntax', null, null))->encode($packer);
        $t->feed((new Codec())->encodeFrame(new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, strlen($err)), $err)
            . self::scalarOk(2, 2));

        $ranB = false;
        try {
            Loop::run([
                static fn (): mixed => $conn->scalarAsync('SELEC 1')->await(),
                static function () use ($conn, &$ranB): mixed {
                    $v = $conn->scalarAsync('SELECT 2')->await();
                    $ranB = true;
                    return $v;
                },
            ]);
            $this->fail('the failure must be thrown');
        } catch (NonRetryableException $e) {
            $this->assertSame('42601', $e->sqlstate());
        }
        $this->assertTrue($ranB, 'the second task ran to completion');
    }

    /** The loop does not nest: awaiting inside it is the way to wait for more work. */
    public function testTheLoopDoesNotNest(): void
    {
        $this->expectException(\LogicException::class);
        Loop::run([static fn (): array => Loop::run([static fn (): int => 1])]);
    }

    /** A task that suspends its Fiber for some other scheduler is refused, not silently resumed. */
    public function testAForeignSuspensionIsRefused(): void
    {
        $this->expectException(\LogicException::class);
        $this->expectExceptionMessage('Ferro await');
        Loop::run([static fn (): mixed => \Fiber::suspend('something else')]);
    }

    /** Outside the loop, await blocks as before: no Fiber is suspended. */
    public function testOutsideTheLoopAwaitBlocks(): void
    {
        $t = new FakeTransport();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
        $t->feed(self::scalarOk(1, 5));
        $this->assertSame(5, $conn->scalarAsync('SELECT 5')->await());

        // Inside a Fiber this loop did not start, too.
        $fiber = new \Fiber(static fn (): mixed => $conn->scalarAsync('SELECT 6')->await());
        $t->feed(self::scalarOk(2, 6));
        $fiber->start();
        $this->assertTrue($fiber->isTerminated(), 'a foreign Fiber was not suspended');
        $this->assertSame(6, $fiber->getReturn());
    }

    /** A synchronous call inside a task still works, and keeps the other task's reply for it. */
    public function testASynchronousCallInsideATask(): void
    {
        $t = new FakeTransport();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
        $t->feed(self::scalarOk(2, 2) . self::scalarOk(1, 1));
        $results = Loop::run([
            static fn (): mixed => $conn->scalarAsync('SELECT 1')->await(),
            static fn (): mixed => $conn->scalar('SELECT 2'),
        ]);
        $this->assertSame([1, 2], $results);
    }

    /** The HELLO advertises the FIBERS client feature (informational: the engine does not read it). */
    public function testHelloAdvertisesTheFibersFeature(): void
    {
        $t = new FakeTransport();
        $packer = PackerFactory::forEncode();
        $ack = Message::encode('hello_ack', [
            'engine_version' => 1, 'boot_epoch' => 1, 'features' => 0, 'pools' => [],
            'type_registry_hash' => C::TYPE_REGISTRY_HASH,
        ], $packer);
        $t->feed((new Codec())->encodeFrame(new Header(0, C::SERVICE_CORE, C::METHOD_CORE_HELLO_ACK, 0, strlen($ack)), $ack));
        (new Session($t, new RequestIdAllocator(0)))->hello();

        $header = Header::decode(substr($t->written, 0, 16));
        $off = 0;
        $wire = PackerFactory::forDecode()->unpack(substr($t->written, 16, $header->payloadLen), $off);
        $this->assertIsArray($wire);
        // HELLO is positional; `features` is its fifth field (PROTOCOL.md §4).
        $features = array_values($wire)[4];
        $this->assertIsInt($features);
        $this->assertSame(C::FEATURE_CLIENT_FIBERS, $features & C::FEATURE_CLIENT_FIBERS);
    }

    private static function frame(int $flags, int $service, int $method, int $rid, string $payload): string
    {
        return (new Codec())->encodeFrame(new Header($flags, $service, $method, $rid, strlen($payload)), $payload);
    }

    private static function txOk(int $method, int $rid, ?int $txId = null): string
    {
        $packer = PackerFactory::forEncode();
        $body = $txId !== null ? BeginResponse::encode(['tx_id' => $txId], $packer) : $packer->packNil();
        return self::frame(C::FLAG_END, C::SERVICE_TX, $method, $rid, Outcome::ok($body)->encode($packer));
    }

    /**
     * Review F1 (measured live as a deleted, acknowledged write): an imperative transaction belongs
     * to the Fiber that began it. Another Fiber cannot run a statement in it or end it.
     */
    public function testAnImperativeTransactionBelongsToTheFiberThatBeganIt(): void
    {
        $t1 = new FakeTransport();
        $t2 = new FakeTransport();
        $conn = new Connection(new Session($t1, new RequestIdAllocator(0)), 'default');
        $other = new Connection(new Session($t2, new RequestIdAllocator(0)), 'default');
        $t1->feed(self::txOk(C::METHOD_TX_BEGIN, 1, txId: 77));

        $seen = [];
        $results = Loop::run([
            'owner' => static function () use ($conn, $other, $t1, $t2): string {
                $conn->begin();
                // Suspend while holding the transaction, on a request on ANOTHER session.
                $t2->feed(self::scalarOk(1, 1));
                $other->scalarAsync('SELECT 1')->await();
                $t1->feed(self::txOk(C::METHOD_TX_ROLLBACK, 2));
                $conn->rollBack();
                return 'rolled back';
            },
            'intruder' => static function () use ($conn, &$seen): string {
                $future = $conn->execAsync('INSERT INTO t VALUES (2)');
                try {
                    $future->await();
                } catch (InvalidTransactionStateException $e) {
                    $seen[] = 'statement refused: ' . (str_contains($e->getMessage(), 'another Fiber') ? 'another Fiber' : $e->getMessage());
                }
                try {
                    $conn->commit();
                } catch (InvalidTransactionStateException) {
                    $seen[] = 'commit refused';
                }
                return 'done';
            },
        ]);

        $this->assertSame(['owner' => 'rolled back', 'intruder' => 'done'], $results);
        $this->assertSame(['statement refused: another Fiber', 'commit refused'], $seen);
        $this->assertCount(2, array_filter($t1->events, static fn (string $e): bool => $e === 'w'), 'only BEGIN and ROLLBACK reached the transaction\'s session');
    }

    /** A transaction whose owner Fiber ended without finishing it can still be rolled back. */
    public function testAnAbandonedTransactionCanBeRolledBackByAnyone(): void
    {
        $t = new FakeTransport();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
        $t->feed(self::txOk(C::METHOD_TX_BEGIN, 1, txId: 5));
        Loop::run([static fn (): bool => (bool) $conn->begin() || true]);

        $this->assertTrue($conn->inTransaction());
        $t->feed(self::txOk(C::METHOD_TX_ROLLBACK, 2));
        $conn->rollBack();
        $this->assertFalse($conn->inTransaction());
    }

    /** Review F3(b): a Fiber that a TASK starts is not the loop's, so an await inside it blocks and returns. */
    public function testAFiberStartedInsideATaskDoesNotSuspendIntoTheLoop(): void
    {
        $t = new FakeTransport();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
        $t->feed(self::scalarOk(1, 9));
        $results = Loop::run([static function () use ($conn): mixed {
            $inner = new \Fiber(static fn (): mixed => $conn->scalarAsync('SELECT 9')->await());
            $inner->start();
            return $inner->isTerminated() ? $inner->getReturn() : 'suspended into the loop';
        }]);
        $this->assertSame([9], $results);
    }

    /**
     * Review F3(f): a dead link reaches each waiting task as that task's own fate. Nothing escapes
     * the loop raw.
     */
    public function testATransportFailureReachesEachTaskNotTheLoop(): void
    {
        $t = new FakeTransport();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
        // Nothing is fed: the first read is EOF.
        $task = static function () use ($conn): string {
            try {
                $conn->scalarAsync('SELECT 1')->await();
                return 'answered';
            } catch (FerroException $e) {
                return $e::class;
            }
        };
        $results = Loop::run([$task, $task]);
        $this->assertCount(2, $results);
        foreach ($results as $r) {
            $this->assertStringStartsWith('Ferro\\Client\\Error\\', $r);
        }
    }

    /**
     * Review F4: an undecodable frame is a desync. It reaches each waiting task as that task's own
     * typed fate (a lost read is Retryable, a lost write Indeterminate), and never escapes the loop
     * as a raw CodecException past every task's catch.
     */
    public function testAnUndecodableFrameReachesEachTaskAsItsOwnFate(): void
    {
        $t = new FakeTransport();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
        $t->feed(str_repeat("\0", 16));
        $results = Loop::run([
            'read' => static function () use ($conn): string {
                try {
                    $conn->scalarAsync('SELECT 1')->await();
                    return 'answered';
                } catch (FerroException $e) {
                    return $e::class;
                }
            },
            'write' => static function () use ($conn): string {
                try {
                    $conn->execAsync('INSERT INTO t VALUES (1)')->await();
                    return 'answered';
                } catch (FerroException $e) {
                    return $e::class;
                }
            },
        ]);
        $this->assertContains($results['read'], [\Ferro\Client\Error\RetryableException::class, ProtocolException::class]);
        $this->assertContains($results['write'], [\Ferro\Client\Error\IndeterminateException::class, ProtocolException::class],
            'a write lost to a desync is never reported Retryable');
    }

    /**
     * Review F5: a Fiber that needs the session while ANOTHER Fiber's stream is open waits for the
     * stream to close instead of failing.
     */
    public function testAFiberWaitsForAnotherFibersStreamToClose(): void
    {
        $packer = PackerFactory::forEncode();
        $t = new FakeTransport();
        $t2 = new FakeTransport();
        $conn = new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
        $other = new Connection(new Session($t2, new RequestIdAllocator(0)), 'default');
        $cols = [['name' => 'n', 'tag' => C::TAG_I64]];
        $t->feed(self::frame(0, C::SERVICE_STREAM, C::METHOD_STREAM_HEAD, 1, StreamHead::encode(['cols' => $cols], $packer)));

        $order = [];
        $results = Loop::run([
            'streamer' => static function () use ($conn, $other, $t, $t2, $packer, &$order): int {
                $stream = $conn->streamRaw('SELECT n FROM big', [], true);
                // Suspend with the stream still open, on a request on another session.
                $t2->feed(self::scalarOk(1, 1));
                $other->scalarAsync('SELECT 1')->await();
                $t->feed(self::frame(C::FLAG_STREAM, C::SERVICE_STREAM, C::METHOD_STREAM_DATA, 1,
                    StreamData::encode(['rows' => [[['tag' => C::TAG_I64, 'data' => 7]]]], $packer)));
                $t->feed(self::scalarOk(1, 0)); // the stream's terminal (ExecOk on the same id)
                $n = 0;
                foreach ($stream->rows() as $_) {
                    ++$n;
                }
                $order[] = 'stream closed';
                // The waiting Fiber's request comes next on this session, as request 2.
                $t->feed(self::scalarOk(2, 2));
                return $n;
            },
            'waiter' => static function () use ($conn, &$order): mixed {
                $v = $conn->scalar('SELECT 2');
                $order[] = 'waiter ran';
                return $v;
            },
        ]);
        $this->assertSame(['streamer' => 1, 'waiter' => 2], $results);
        $this->assertSame(['stream closed', 'waiter ran'], $order);
    }
}
