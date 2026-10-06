<?php // /php/client/tests/Client/LoopTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Loop;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
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
}
