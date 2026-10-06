<?php // /php/client/tests/Unit/StatementTimeoutTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Connection;
use Ferro\Client\Error\TransportException;
use Ferro\Ferro;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * M3-D1c: what `statementTimeout` puts on the wire, and what it refuses.
 *
 * A buffered EXEC carries `timeout_ms` for the ENGINE to enforce (the live suite proves the engine
 * answers by it; this pins that the client sends it at all — without it the client's backstop alone
 * made the live test pass, review F7). A streamed EXEC carries none (review F5), and a value the wire
 * cannot say is refused instead of being mis-encoded (review F3).
 */
final class StatementTimeoutTest extends TestCase
{
    /** @return array<string, mixed> */
    private static function decodeExec(string $payload): array
    {
        $off = 0;
        return ExecRequest::mapFromWire((array) (new PurePacker())->unpack($payload, $off));
    }

    public function testABufferedExecCarriesTimeoutMs(): void
    {
        $session = (new FakeSession())->thenExecOk();
        (new Connection($session, 'default', statementTimeout: 0.25))->exec('UPDATE t SET n = 1');
        [$service, $method, $payload] = $session->sent[0];
        $this->assertSame([C::SERVICE_SQL, C::METHOD_SQL_EXEC], [$service, $method]);
        $this->assertSame(250, self::decodeExec($payload)['timeout_ms']);
    }

    public function testNoStatementTimeoutSendsNone(): void
    {
        $session = (new FakeSession())->thenExecOk();
        (new Connection($session, 'default'))->exec('UPDATE t SET n = 1');
        $this->assertNull(self::decodeExec($session->sent[0][2])['timeout_ms']);
    }

    /**
     * The engine bounds a streamed request's WHOLE life by `timeout_ms` — every pull and every wait
     * for the caller to replenish the credit window — so sending it would cut off a caller that
     * merely consumes rows more slowly than the timeout. A stream is bounded by liveness instead.
     */
    public function testAStreamedExecCarriesNoTimeoutMs(): void
    {
        $session = (new FakeSession())->thenStreamEnd();
        foreach ((new Connection($session, 'default', statementTimeout: 0.25))->stream('SELECT 1') as $_) {
        }
        $sent = self::decodeExec($session->sent[0][2]);
        $this->assertSame(2, $sent['fetch'], 'this is the streamed shape');
        $this->assertNull($sent['timeout_ms']);
    }

    /** @return array<string, array{0: float}> */
    public static function unsayable(): array
    {
        return [
            'INF' => [INF],
            '-INF' => [-INF],
            'NAN' => [NAN],
            'zero' => [0.0],
            'negative' => [-1.0],
            'past the u32 by one ms' => [4294967.296],
            'far past the u32' => [5e6],
        ];
    }

    #[DataProvider('unsayable')]
    public function testAConnectionRefusesATimeoutTheWireCannotSay(float $seconds): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Connection(new FakeSession(), 'default', statementTimeout: $seconds);
    }

    /** `Ferro::connect` refuses it before dialling: the socket path below does not exist. */
    #[DataProvider('unsayable')]
    public function testConnectRefusesATimeoutTheWireCannotSayBeforeDialling(float $seconds): void
    {
        try {
            Ferro::connect('/nonexistent/ferro-' . getmypid() . '.sock', statementTimeout: $seconds);
            $this->fail('must refuse');
        } catch (TransportException $e) {
            $this->fail('validated only after dialling: ' . $e->getMessage());
        } catch (\InvalidArgumentException) {
            $this->addToAssertionCount(1);
        }
    }

    /** @return array<string, array{0: float, 1: int}> */
    public static function sayable(): array
    {
        return [
            'a sub-millisecond value is the smallest the wire can say' => [0.0001, 1],
            'milliseconds' => [0.3, 300],
            'the u32 maximum exactly' => [4294967.295, 4294967295],
        ];
    }

    #[DataProvider('sayable')]
    public function testTheWireValue(float $seconds, int $ms): void
    {
        $this->assertSame($ms, Connection::statementTimeoutMs($seconds));
    }
}
