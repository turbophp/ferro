<?php // /php/guzzle/tests/Unit/HandlerTestCase.php
declare(strict_types=1);
namespace Ferro\Guzzle\Tests\Unit;

use Ferro\Client\Connection;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Guzzle\FerroHandler;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\HttpHead;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Tests\Support\FakeTransport;
use Ferro\Tests\Support\HttpFrames as F;
use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use PHPUnit\Framework\TestCase;

/**
 * The Guzzle handler over `ferro/client`'s in-memory transport: a scripted engine's frames are fed
 * before the request, and every frame the client writes is read back — so what reached "the
 * engine" is asserted exactly, with no daemon.
 */
abstract class HandlerTestCase extends TestCase
{
    protected const ORIGIN = 'https://api.example.com';

    /** The session of the last connection {@see conn} built. */
    protected ?Session $session = null;

    /** A handshaken Connection over `$t`; the first request id is 1. */
    protected function conn(FakeTransport $t): Connection
    {
        $t->feed(F::helloAck(C::FEATURE_ENGINE_HTTP));
        $session = new Session($t, new RequestIdAllocator(0));
        $session->hello();
        $this->session = $session;
        return new Connection($session, 'default');
    }

    protected function handler(FakeTransport $t, ?callable $fallback = null): FerroHandler
    {
        return new FerroHandler($this->conn($t), [self::ORIGIN => 'up'], $fallback);
    }

    /** @param ?callable(HandlerStack): void $configure */
    protected function client(FerroHandler $handler, ?callable $configure = null): Client
    {
        $stack = HandlerStack::create($handler);
        if ($configure !== null) {
            $configure($stack);
        }
        return new Client(['handler' => $stack, 'base_uri' => self::ORIGIN]);
    }

    /**
     * A HEAD frame with every field chosen.
     *
     * @param list<array{0:string,1:string}> $headers
     * @param ?array{0:string,1:?int} $decoded
     */
    protected static function head(int $rid, int $status, array $headers = [], bool $idempotent = false, ?array $decoded = null, ?string $reason = 'OK', int $version = 11): string
    {
        $payload = HttpHead::encode([
            'status' => $status, 'version' => $version, 'reason' => $reason, 'headers' => $headers,
            'decoded' => $decoded, 'idempotent' => $idempotent,
        ], PackerFactory::forEncode());
        return F::frame(0, C::SERVICE_HTTP, C::METHOD_HTTP_HEAD, $rid, $payload);
    }
}
