<?php // /php/psr18/tests/Unit/ClientTest.php
declare(strict_types=1);
namespace Ferro\Psr18\Tests\Unit;

use Ferro\Client\Connection;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Http\Adapter\BodyStream;
use Ferro\Http\Error\ResponseIncompleteException;
use Ferro\Http\Exception\BodyReadException;
use Ferro\Http\Fate;
use Ferro\Http\Fate\Indeterminate;
use Ferro\Http\Fate\NonRetryable;
use Ferro\Http\Fate\Retryable;
use Ferro\Http\FateClass;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\HttpHead;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Psr18\Client;
use Ferro\Psr18\FatedResponse;
use Ferro\Psr18\IndeterminateNetworkException;
use Ferro\Psr18\IndeterminateUnrepresentableResponseException;
use Ferro\Psr18\NetworkException;
use Ferro\Psr18\RequestException;
use Ferro\Psr18\RetryableNetworkException;
use Ferro\Psr18\RetryableUnrepresentableResponseException;
use Ferro\Psr18\UnmappedOriginException;
use Ferro\Tests\Support\FakeTransport;
use Ferro\Tests\Support\HttpFrames as F;
use GuzzleHttp\Psr7\HttpFactory;
use GuzzleHttp\Psr7\Request;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\NetworkExceptionInterface;
use Psr\Http\Client\RequestExceptionInterface;

/**
 * M6-F9: `Ferro\Psr18\Client` over `ferro/client`'s in-memory transport (SPEC §23.11.5, §23.11.3's
 * PSR-18 column).
 */
final class ClientTest extends TestCase
{
    private const ORIGIN = 'https://api.example.com';

    private ?Session $session = null;

    private function client(FakeTransport $t, bool $stream = true): Client
    {
        $t->feed(F::helloAck(C::FEATURE_ENGINE_HTTP));
        $session = new Session($t, new RequestIdAllocator(0));
        $session->hello();
        $this->session = $session;
        $factory = new HttpFactory();
        return new Client(new Connection($session, 'default'), $factory, $factory, [self::ORIGIN => 'up'], stream: $stream);
    }

    /** @param list<array{0:string,1:string}> $headers */
    private static function head(int $rid, int $status, array $headers = [], bool $idempotent = false): string
    {
        return F::frame(0, C::SERVICE_HTTP, C::METHOD_HTTP_HEAD, $rid, HttpHead::encode([
            'status' => $status, 'version' => 11, 'reason' => 'Reason', 'headers' => $headers,
            'decoded' => null, 'idempotent' => $idempotent,
        ], PackerFactory::forEncode()));
    }

    public function testAResponseIsTheFactorysWrappedWithItsFateAndItsBodyIsLazy(): void
    {
        $t = new FakeTransport();
        $client = $this->client($t);
        $t->feed(self::head(1, 200, [['content-type', 'application/json']], true));
        $t->feed(F::body(1, '{"a"') . F::body(1, ':1}') . F::done(1));
        $res = $client->sendRequest(new Request('POST', self::ORIGIN . '/v1/chat?x=1', ['X-K' => 'v'], 'payload'));

        $this->assertInstanceOf(FatedResponse::class, $res);
        $this->assertInstanceOf(\GuzzleHttp\Psr7\Response::class, $res->inner(), 'the application\'s own factory built it');
        $this->assertSame(200, $res->getStatusCode());
        $this->assertSame('Reason', $res->getReasonPhrase());
        $this->assertSame(['application/json'], $res->getHeader('Content-Type'));
        $this->assertTrue(Fate::of($res)?->idempotent);
        $body = $res->getBody();
        $this->assertInstanceOf(BodyStream::class, $body);
        $this->assertCount(1, F::windowUpdates($t->written, 1), 'returned at the head: no body read yet');
        $this->assertSame('{"a":1}', (string) $body);

        $req = F::requests($t->written)[1];
        $this->assertSame(['up', 'POST', '/v1/chat?x=1', self::ORIGIN, 'payload'], [$req['upstream'], $req['method'], $req['target'], $req['origin'], $req['body']]);
        $this->assertContains(['X-K', 'v'], $req['headers']);
        $this->assertTrue($req['decode']);
        $this->assertNull($req['idempotent']);

        // with*() keeps the fate.
        $copy = $res->withHeader('x-added', '1')->withStatus(201);
        $this->assertInstanceOf(FatedResponse::class, $copy);
        $this->assertSame(FateClass::NotAFailure, Fate::of($copy)?->fate);
    }

    public function testDeclarationsAndBoundsTravelWithTheRequest(): void
    {
        $t = new FakeTransport();
        $client = $this->client($t)->withIdempotent(true)->withTimeouts(1500, 200, 900);
        $t->feed(self::head(1, 204) . F::done(1));
        $client->sendRequest(new Request('GET', self::ORIGIN . '/'));
        $req = F::requests($t->written)[1];
        $this->assertTrue($req['idempotent']);
        $this->assertSame([1500, 200, 900], [$req['timeout_ms'], $req['connect_timeout_ms'], $req['read_timeout_ms']]);
        $this->assertNull($req['body']);
    }

    public function testABufferedClientReadsTheBodyFirst(): void
    {
        $t = new FakeTransport();
        $client = $this->client($t, false);
        $t->feed(self::head(1, 200) . F::body(1, 'ab') . F::done(1));
        $res = $client->sendRequest(new Request('GET', self::ORIGIN . '/'));
        $this->assertNotInstanceOf(BodyStream::class, $res->getBody());
        $this->assertSame('ab', (string) $res->getBody());
    }

    public function testAnyStatusIsAResponse(): void
    {
        $t = new FakeTransport();
        $client = $this->client($t);
        $t->feed(self::head(1, 500) . F::body(1, 'boom') . F::done(1));
        $res = $client->sendRequest(new Request('POST', self::ORIGIN . '/'));
        $this->assertSame(500, $res->getStatusCode());
        $this->assertSame(FateClass::Indeterminate, Fate::of($res)?->fate, 'a non-idempotent 5xx (§23.7.4)');
    }

    public function testAnUnmappedOriginIsARequestExceptionAndNothingIsSent(): void
    {
        $t = new FakeTransport();
        $client = $this->client($t);
        try {
            $client->sendRequest(new Request('GET', 'http://invalid.php-http.org/'));
            $this->fail('expected the refusal');
        } catch (UnmappedOriginException $e) {
            $this->assertInstanceOf(RequestExceptionInterface::class, $e);
            $this->assertNotInstanceOf(NetworkExceptionInterface::class, $e);
            $this->assertInstanceOf(NonRetryable::class, $e);
        }
        $this->assertSame([], F::requests($t->written));
    }

    public function testAPolicyRefusalIsARequestExceptionAndEveryOtherFailureANetworkException(): void
    {
        $refused = ['forbidden_upstream', 'forbidden_origin', 'forbidden_target', 'forbidden_method', 'forbidden_header', 'forbidden_body', 'forbidden_address'];
        foreach (C::HTTP_CAUSES as $cause) {
            $t = new FakeTransport();
            $client = $this->client($t);
            $isRefusal = in_array($cause, $refused, true);
            $t->feed(F::error(1, $isRefusal ? C::ERR_FORBIDDEN : C::ERR_CONNECTION_LOST, $isRefusal ? C::BRANCH_NON_RETRYABLE : C::BRANCH_RETRYABLE, $cause));
            try {
                $client->sendRequest(new Request('POST', self::ORIGIN . '/', [], 'x'));
                $this->fail("{$cause}: expected an exception");
            } catch (ClientExceptionInterface $e) {
                if ($isRefusal) {
                    $this->assertInstanceOf(RequestException::class, $e, $cause);
                    $this->assertNotInstanceOf(NetworkExceptionInterface::class, $e);
                } else {
                    $this->assertInstanceOf(RetryableNetworkException::class, $e, $cause);
                    $this->assertInstanceOf(Retryable::class, $e);
                }
                $this->assertSame($cause, Fate::of($e)?->cause);
            }
        }
    }

    public function testAnIndeterminateFailureIsANetworkExceptionMarkedIndeterminate(): void
    {
        $t = new FakeTransport();
        $client = $this->client($t);
        $t->feed(F::error(1, C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'eof_empty'));
        try {
            $client->sendRequest(new Request('POST', self::ORIGIN . '/charge', [], '{}'));
            $this->fail('expected the exception');
        } catch (IndeterminateNetworkException $e) {
            $this->assertInstanceOf(Indeterminate::class, $e);
            $this->assertSame('/charge', $e->getRequest()->getUri()->getPath());
        }
    }

    public function testABodyOverTheFrameCapIsARequestExceptionBeforeAnythingIsWritten(): void
    {
        $t = new FakeTransport();
        $client = $this->client($t);
        $before = $t->writeCalls;
        $this->expectException(RequestException::class);
        try {
            $client->sendRequest(new Request('POST', self::ORIGIN . '/', [], str_repeat('x', C::MAX_FRAME_PAYLOAD + 1)));
        } finally {
            $this->assertSame($before, $t->writeCalls);
        }
    }

    public function testAStatusTheFactoryCannotRepresentIsAClientExceptionWithTheStatusesFate(): void
    {
        foreach ([[false, IndeterminateUnrepresentableResponseException::class], [true, RetryableUnrepresentableResponseException::class]] as [$idem, $class]) {
            $t = new FakeTransport();
            $client = $this->client($t);
            $t->feed(self::head(1, 799, [], $idem) . F::cancelled(1));
            try {
                $client->sendRequest(new Request('POST', self::ORIGIN . '/', [], 'x'));
                $this->fail('expected the exception');
            } catch (ClientExceptionInterface $e) {
                $this->assertInstanceOf($class, $e);
                $this->assertNotInstanceOf(NetworkExceptionInterface::class, $e);
                $this->assertSame(799, Fate::of($e)?->status);
            }
            $this->assertSame([1], F::cancels($t->written));
            $this->assertFalse($this->session?->hasRequestsInFlight(), 'CANCELled AND drained to its terminal, not left to a destructor');
        }
    }

    public function testALazyBodyThatFailsThrowsFromReadWithTheCombinedFate(): void
    {
        $t = new FakeTransport();
        $client = $this->client($t);
        $t->feed(self::head(1, 201) . F::body(1, 'ok') . F::error(1, C::ERR_RESPONSE_INCOMPLETE, C::BRANCH_NON_RETRYABLE, 'body_reset'));
        $body = $client->sendRequest(new Request('POST', self::ORIGIN . '/', [], 'x'))->getBody();
        $this->assertSame('ok', $body->read(100));
        try {
            $body->read(100);
            $this->fail('expected the failure');
        } catch (BodyReadException $e) {
            $this->assertInstanceOf(\RuntimeException::class, $e);
            $this->assertInstanceOf(NonRetryable::class, $e, 'a 201 head: applied');
            $prev = $e->getPrevious();
            $this->assertInstanceOf(ResponseIncompleteException::class, $prev);
            $this->assertTrue($prev->wasApplied());
        }
    }

    public function testABufferedBodyFailureIsANetworkException(): void
    {
        $t = new FakeTransport();
        $client = $this->client($t, false);
        $t->feed(self::head(1, 502) . F::body(1, 'x') . F::error(1, C::ERR_RESPONSE_INCOMPLETE, C::BRANCH_NON_RETRYABLE, 'body_eof'));
        try {
            $client->sendRequest(new Request('POST', self::ORIGIN . '/', [], 'x'));
            $this->fail('expected the exception');
        } catch (NetworkException $e) {
            $this->assertInstanceOf(IndeterminateNetworkException::class, $e, 'a truncated non-idempotent 502');
        }
    }

    public function testClosingALazyBodyCancels(): void
    {
        $t = new FakeTransport();
        $client = $this->client($t);
        $t->feed(self::head(1, 200) . F::cancelled(1));
        $client->sendRequest(new Request('GET', self::ORIGIN . '/'))->getBody()->close();
        $this->assertSame([1], F::cancels($t->written));
    }

    public function testAConnectionThatCannotBeOpenedIsARetryableNetworkException(): void
    {
        $factory = new HttpFactory();
        $client = new Client(static fn (): Connection => throw new \RuntimeException('no socket'), $factory, $factory, [self::ORIGIN => 'up']);
        $this->expectException(RetryableNetworkException::class);
        $client->sendRequest(new Request('POST', self::ORIGIN . '/', [], 'x'));
    }
}
