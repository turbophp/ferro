<?php // /php/psr18/tests/Live/Psr18LiveTest.php
declare(strict_types=1);
namespace Ferro\Psr18\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Ferro;
use Ferro\Http\Error\HttpException;
use Ferro\Http\Exception\NonRetryableBodyReadException;
use Ferro\Http\Fate;
use Ferro\Http\FateClass;
use Ferro\Psr18\Client;
use Ferro\Psr18\FatedResponse;
use Ferro\Psr18\IndeterminateNetworkException;
use Ferro\Psr18\IndeterminateUnrepresentableResponseException;
use Ferro\Psr18\NonRetryableNetworkException;
use Ferro\Psr18\RequestException;
use Ferro\Psr18\RetryableNetworkException;
use Ferro\Psr18\UnmappedOriginException;
use Ferro\Tests\Live\HttpLiveTestCase;
use GuzzleHttp\Psr7\HttpFactory;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Client\NetworkExceptionInterface;
use Psr\Http\Client\RequestExceptionInterface;

/**
 * M6-F9: `Ferro\Psr18\Client` against a REAL `ferrod` and the recording loopback upstream: the
 * §23.7.1 engine cells and the §23.7.3 link-loss cells through the PSR-18 interfaces, each with its
 * receive count.
 */
final class Psr18LiveTest extends HttpLiveTestCase
{
    private ?Connection $conn = null;
    private string $deadOrigin = '';

    protected function tearDown(): void
    {
        try {
            $this->conn?->session()->close();
        } catch (\Throwable) {
        }
        $this->conn = null;
        parent::tearDown();
    }

    /** @return array<string, string> */
    protected function extraEnv(): array
    {
        $env = parent::extraEnv();
        $this->deadOrigin = $env['FERRO_UPSTREAM_DEAD_ORIGIN'];
        return $env;
    }

    private function origin(): string
    {
        return 'http://127.0.0.1:' . $this->upstreamPort;
    }

    private function client(bool $stream = true): Client
    {
        $this->conn ??= Ferro::connect($this->socketPath);
        $f = new HttpFactory();
        return new Client($this->conn, $f, $f, [$this->origin() => 'up', $this->deadOrigin => 'dead'], stream: $stream);
    }

    public function testARequestRoundTripsAndItsLazyBodyArrivesAsItIsProduced(): void
    {
        $res = $this->client()->sendRequest(new Request('POST', $this->origin() . '/echo?p=1', ['Content-Type' => 'text/plain'], 'hello'));
        $this->assertInstanceOf(FatedResponse::class, $res);
        $echo = json_decode((string) $res->getBody(), true);
        $this->assertIsArray($echo);
        $this->assertSame('hello', base64_decode((string) $echo['body']));
        $this->assertSame(1, $this->received('/echo?p=1'));

        $start = microtime(true);
        $body = $this->client()->sendRequest(new Request('GET', $this->origin() . '/stream?chunks=4&size=3&gap=300'))->getBody();
        $headAt = microtime(true) - $start;
        $first = $body->read(3);
        $firstAt = microtime(true) - $start;
        $rest = $body->getContents();
        $this->assertSame('..0', $first);
        $this->assertSame('..1..2..3', $rest);
        $this->assertLessThan(0.25, $headAt, 'sendRequest() returned at the head');
        $this->assertLessThan(0.25, $firstAt, 'the first chunk before the second was produced');
        $this->assertGreaterThan(0.85, microtime(true) - $start, 'the rest after three 300 ms gaps');
    }

    public function testEngineFatesAreNetworkOrRequestExceptionsWithTheirMarker(): void
    {
        $client = $this->client()->withTimeouts(300);
        try {
            $client->sendRequest(new Request('POST', $this->origin() . '/hold?c=p', [], 'x'));
            $this->fail('expected the timeout');
        } catch (IndeterminateNetworkException $e) {
            $this->assertSame('timeout', Fate::of($e)?->cause);
        }
        try {
            $client->withIdempotent(true)->sendRequest(new Request('GET', $this->origin() . '/hold?c=g'));
            $this->fail('expected the timeout');
        } catch (NonRetryableNetworkException $e) {
            $this->assertSame('timeout', Fate::of($e)?->cause);
        }
        try {
            $client->sendRequest(new Request('POST', $this->deadOrigin . '/x', [], 'x'));
            $this->fail('expected the dial failure');
        } catch (RetryableNetworkException $e) {
            $this->assertSame('connect_refused', Fate::of($e)?->cause);
        }
        try {
            $client->sendRequest(new Request('TRACE', $this->origin() . '/echo'));
            $this->fail('expected the refusal');
        } catch (RequestException $e) {
            $this->assertInstanceOf(RequestExceptionInterface::class, $e);
            $this->assertNotInstanceOf(NetworkExceptionInterface::class, $e);
            $this->assertSame('forbidden_method', Fate::of($e)?->cause);
        }
        try {
            $client->sendRequest(new Request('GET', 'http://invalid.php-http.org/'));
            $this->fail('expected the refusal');
        } catch (UnmappedOriginException) {
        }
        $this->eventually(fn (): bool => $this->received('/hold?c=p') === 1 && $this->received('/hold?c=g') === 1, 3.0, 'both holds read');
        $this->assertSame(0, $this->received('/echo'));
    }

    public function testA600StatusIsAClientExceptionWithItsStatusesFate(): void
    {
        try {
            $this->client()->sendRequest(new Request('POST', $this->origin() . '/status/600?c=s', [], 'x'));
            $this->fail('expected the exception');
        } catch (IndeterminateUnrepresentableResponseException $e) {
            $this->assertSame(600, $e->status());
            $this->assertInstanceOf(\InvalidArgumentException::class, $e->getPrevious(), 'guzzlehttp/psr7 refused it');
        }
        $this->assertSame(1, $this->received('/status/600?c=s'));
        $this->assertSame(1, $this->conn?->scalar('SELECT 1'), 'the cancelled exchange left the session usable');
    }

    /**
     * §23.7.3 through PSR-18. `sendRequest()` is synchronous, so each "no head" cell kills the engine
     * while the call is blocked in it; the "head received" cell kills it while the lazy body is open.
     */
    public function testLinkLossCellsThroughThePsr18Client(): void
    {
        // Written, no HEAD, non-idempotent: Indeterminate.
        $this->killIn(0.4);
        try {
            $this->client()->sendRequest(new Request('POST', $this->origin() . '/hold?c=A', [], 'a'));
            $this->fail('expected the link loss');
        } catch (IndeterminateNetworkException $e) {
            $this->assertSame(HttpException::CLIENT_LINK_LOST, Fate::of($e)?->cause);
        }
        $this->relaunch();

        // Written, no HEAD, declared idempotent: Retryable.
        $this->killIn(0.4);
        try {
            $this->client()->withIdempotent(true)->sendRequest(new Request('GET', $this->origin() . '/hold?c=B'));
            $this->fail('expected the link loss');
        } catch (RetryableNetworkException $e) {
            $this->assertSame(FateClass::Retryable, Fate::of($e)?->fate);
        }
        $this->relaunch();

        // HEAD received, non-idempotent, 201: the lazy body fails NonRetryable, applied.
        $body = $this->client()->sendRequest(new Request('POST', $this->origin() . '/head-then-hold?status=201&c=D', [], 'd'))->getBody();
        $this->assertSame('partial', $body->read(7));
        $this->killFerrod();
        try {
            $body->read(10);
            $this->fail('expected the link loss');
        } catch (NonRetryableBodyReadException $e) {
            $prev = $e->getPrevious();
            $this->assertInstanceOf(\Ferro\Http\Error\ResponseIncompleteException::class, $prev);
            $this->assertTrue($prev->wasApplied());
        }
        $this->relaunch();

        // REQUEST never completely written: Retryable, not sent.
        $pid = $this->stopFerrodAndWait();
        exec(sprintf('(sleep 1; kill -KILL %d) > /dev/null 2>&1 &', $pid));
        try {
            $this->client()->sendRequest(new Request('POST', $this->origin() . '/echo?c=H', [], str_repeat('h', 15 * 1024 * 1024)));
            $this->fail('expected the link loss');
        } catch (RetryableNetworkException $e) {
            $this->assertStringContainsString('not sent', $e->getMessage());
        }
        $this->killFerrod();
        $this->relaunch();

        $this->assertSame(200, $this->client()->sendRequest(new Request('GET', $this->origin() . '/status/200'))->getStatusCode());
        $this->assertSame(1, $this->received('/hold?c=A'));
        $this->assertSame(1, $this->received('/hold?c=B'));
        $this->assertSame(1, $this->received('/head-then-hold?status=201&c=D'));
        $this->assertSame(0, $this->received('/echo?c=H'), 'never reached the upstream');
    }

    private function killIn(float $seconds): void
    {
        exec(sprintf('(sleep %.2f; kill -KILL %d) > /dev/null 2>&1 &', $seconds, $this->ferrodPid()));
    }

    private function relaunch(): void
    {
        $this->killFerrod();
        $this->restartFerrod();
    }
}
