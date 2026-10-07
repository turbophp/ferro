<?php // /php/guzzle/tests/Live/GuzzleLiveTest.php
declare(strict_types=1);
namespace Ferro\Guzzle\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Ferro;
use Ferro\Guzzle\FerroHandler;
use Ferro\Guzzle\IndeterminateConnectException;
use Ferro\Guzzle\IndeterminateRequestException;
use Ferro\Guzzle\NonRetryableConnectException;
use Ferro\Guzzle\NonRetryableRequestException;
use Ferro\Guzzle\Retry;
use Ferro\Guzzle\RetryableConnectException;
use Ferro\Guzzle\UnmappedOriginException;
use Ferro\Http\Adapter\BodyStream;
use Ferro\Http\Fate;
use Ferro\Http\FateClass;
use Ferro\Http\FerroResponse;
use Ferro\Tests\Live\HttpLiveTestCase;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Handler\CurlHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Pool;
use GuzzleHttp\Promise\Utils;
use GuzzleHttp\Psr7\Request;
use Psr\Http\Message\RequestInterface;

/**
 * M6-F9: `Ferro\Guzzle\FerroHandler` against a REAL `ferrod` and the recording loopback upstream
 * (`php/client`'s `HttpLiveTestCase`), so what the upstream received is a read-back.
 */
final class GuzzleLiveTest extends HttpLiveTestCase
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

    /** @param ?callable(HandlerStack): void $configure */
    private function client(?callable $configure = null, string $upstream = 'up'): Client
    {
        $this->conn ??= Ferro::connect($this->socketPath);
        $stack = HandlerStack::create(new FerroHandler($this->conn, [$this->origin() => $upstream, $this->deadOrigin => 'dead']));
        if ($configure !== null) {
            $configure($stack);
        }
        return new Client(['handler' => $stack, 'base_uri' => $this->origin()]);
    }

    public function testARequestRoundTripsThroughAStockClientAndReachesTheUpstreamOnce(): void
    {
        $res = $this->client()->post('/echo?a=1', ['json' => ['x' => 1], 'headers' => ['X-Trace' => 'f9']]);
        $this->assertInstanceOf(FerroResponse::class, $res);
        $this->assertSame(200, $res->getStatusCode());
        $this->assertSame('echo', $res->getHeaderLine('X-Upstream'));
        $echo = json_decode((string) $res->getBody(), true);
        $this->assertIsArray($echo);
        $this->assertSame('POST', $echo['method']);
        $this->assertSame('/echo?a=1', $echo['target']);
        $this->assertSame('{"x":1}', base64_decode((string) $echo['body']));
        $this->assertContains(['X-Trace', 'f9'], $echo['headers']);
        $this->assertSame(1, $this->received('/echo?a=1'), 'the contact assertion: the upstream read it, once');
        $this->assertFalse($res->ferroFate()->idempotent, 'IDEMPOTENT_METHODS empty: a POST is not idempotent');
    }

    public function testStatusesAreResponsesAndHttpErrorsThrowsAsStock(): void
    {
        $client = $this->client();
        try {
            $client->get('/status/404');
            $this->fail('expected http_errors');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getResponse()->getStatusCode());
            $this->assertSame(FateClass::NonRetryable, Fate::of($e)?->fate);
        }
        $this->assertSame(503, $client->get('/status/503', ['http_errors' => false])->getStatusCode());
    }

    /** §23.7.1's engine-side cells through Guzzle: curl's class, Ferro's marker, and the receive count. */
    public function testEngineFatesRejectWithCurlsClassAndTheirMarker(): void
    {
        $client = $this->client();

        try {
            $client->post('/hold?c=post', ['body' => 'p', 'timeout' => 0.3]);
            $this->fail('expected the timeout');
        } catch (IndeterminateConnectException $e) {
            $this->assertSame('timeout', Fate::of($e)?->cause);
        }
        try {
            $client->get('/hold?c=get', ['timeout' => 0.3, 'ferro' => ['idempotent' => true]]);
            $this->fail('expected the timeout');
        } catch (NonRetryableConnectException $e) {
            $this->assertSame('timeout', Fate::of($e)?->cause, 'a declared read times out NonRetryable (§9.2\'s read rule)');
        }
        try {
            $client->post($this->deadOrigin . '/x', ['body' => 'p']);
            $this->fail('expected the dial failure');
        } catch (RetryableConnectException $e) {
            $this->assertSame('connect_refused', Fate::of($e)?->cause, 'never sent: Retryable even for a POST');
        }
        try {
            $client->get('/a/%2e%2e/admin'); // a literal `..` is resolved away by Guzzle's UriResolver
            $this->fail('expected the refusal');
        } catch (NonRetryableRequestException $e) {
            $this->assertSame('forbidden_target', Fate::of($e)?->cause);
            $this->assertFalse($e->hasResponse());
        }
        try {
            $client->get('/echo?c=fwd', ['headers' => ['X-Forwarded-For' => '10.0.0.1']]);
            $this->fail('expected the refusal');
        } catch (NonRetryableRequestException $e) {
            $this->assertSame('forbidden_header', Fate::of($e)?->cause);
        }
        try {
            $client->get('http://localhost:' . $this->upstreamPort . '/echo');
            $this->fail('expected the refusal');
        } catch (UnmappedOriginException) {
        }

        $this->eventually(fn (): bool => $this->received('/hold?c=post') === 1 && $this->received('/hold?c=get') === 1, 3.0, 'both holds read');
        $this->assertSame(0, $this->received('/a/'));
        $this->assertSame(0, $this->received('/echo'), 'refused before any dial');
    }

    /**
     * §23.11.4's test, live: `Middleware::retry` with Ferro's decider does NOT re-send an
     * Indeterminate POST (upstream receive count 1), and DOES re-send what was never sent.
     */
    public function testTheDeciderNeverResendsAnIndeterminatePostAndResendsWhatWasNeverSent(): void
    {
        $attempts = [];
        $client = $this->client(static function (HandlerStack $s) use (&$attempts): void {
            $s->push(Retry::middleware(2, 0, 0));
            $s->push(Middleware::tap(static function (RequestInterface $r) use (&$attempts): void {
                $attempts[] = (string) $r->getUri();
            }));
        });

        try {
            $client->post('/hold?c=retry', ['body' => 'once', 'timeout' => 0.3]);
            $this->fail('expected the timeout');
        } catch (IndeterminateConnectException) {
        }
        try {
            $client->post($this->deadOrigin . '/never', ['body' => 'x']);
            $this->fail('expected the dial failure');
        } catch (RetryableConnectException) {
        }
        $this->assertSame(1, count(array_filter($attempts, static fn (string $u): bool => str_contains($u, '/hold?c=retry'))));
        $this->assertSame(3, count(array_filter($attempts, static fn (string $u): bool => str_contains($u, '/never'))), 'one attempt and two retries');

        // Statuses by their fate: a non-idempotent 500 is Indeterminate (never retried); a 503 with
        // Retry-After is Retryable for any method.
        $this->assertSame(500, $client->post('/status/500?c=r500', ['http_errors' => false])->getStatusCode());
        $this->assertSame(503, $client->post('/status/503?retry_after=0&c=r503', ['http_errors' => false])->getStatusCode());

        usleep(300_000);
        $this->assertSame(1, $this->received('/hold?c=retry'), 'the Indeterminate POST reached the upstream exactly once');
        $this->assertSame(1, $this->received('/status/500?c=r500'));
        $this->assertSame(3, $this->received('/status/503?retry_after=0&c=r503'));
    }

    public function testAStreamedBodyArrivesAsItIsProduced(): void
    {
        $start = microtime(true);
        $res = $this->client()->get('/stream?chunks=5&size=4&gap=300', ['stream' => true]);
        $body = $res->getBody();
        $this->assertInstanceOf(BodyStream::class, $body);
        $first = $body->read(4);
        $firstAt = microtime(true) - $start;
        $rest = $body->getContents();
        $endAt = microtime(true) - $start;
        $this->assertSame('...0', $first);
        $this->assertSame('...1...2...3...4', $rest);
        $this->assertLessThan(0.25, $firstAt, 'the first chunk before the second was produced');
        $this->assertGreaterThan(1.1, $endAt, 'the last after four 300 ms gaps');
        $this->assertTrue($body->eof());
    }

    public function testAnAbandonedStreamedBodyIsCancelledAndTheNextRequestIsUnharmed(): void
    {
        $client = $this->client();
        $body = $client->get('/stream?chunks=200&size=1024&gap=20', ['stream' => true])->getBody();
        $this->assertSame(1024, strlen($body->read(1024)));
        $body->close();
        $this->assertSame(1, $this->conn?->scalar('SELECT 1'), 'the next request on the session (the C1d lesson)');
        $this->assertSame(200, $client->get('/status/200')->getStatusCode());
        $this->eventually(fn (): bool => $this->closedConnections('/stream') >= 1, 3.0, 'the engine closed the abandoned exchange');
    }

    public function testAPoolOfDelayedRequestsCompletesInAboutOneDelay(): void
    {
        $client = $this->client();
        $requests = (function () {
            for ($i = 0; $i < 5; ++$i) {
                yield new Request('GET', $this->origin() . "/delay/0?c=d{$i}");
            }
        })();
        $ok = 0;
        $start = microtime(true);
        (new Pool($client, $requests, [
            'concurrency' => 5,
            'options' => ['delay' => 400],
            'fulfilled' => static function () use (&$ok): void { ++$ok; },
        ]))->promise()->wait();
        $elapsed = microtime(true) - $start;
        $this->assertSame(5, $ok);
        $this->assertGreaterThanOrEqual(0.39, $elapsed);
        $this->assertLessThan(1.2, $elapsed, 'about d (0.4 s), not N × d (2 s)');
        $this->assertSame(5, $this->received('/delay/0?c=d'));
    }

    public function testAsyncRequestsRunConcurrentlyInTheEngine(): void
    {
        $client = $this->client();
        $start = microtime(true);
        $responses = Utils::unwrap(array_map(static fn (int $i) => $client->getAsync("/delay/500?c=c{$i}"), [1, 2, 3, 4]));
        $elapsed = microtime(true) - $start;
        $this->assertCount(4, $responses);
        $this->assertLessThan(1.4, $elapsed, 'four 500 ms calls in about 0.5 s, not 2 s');
    }

    /**
     * F8's hand-off: `ferrod` passes a status of 600..=999 through and `guzzlehttp/psr7` refuses one.
     * The CONTROL — stock curl against the same upstream — measures what Guzzle does with it; Ferro
     * must match its class and message, and add the status's fate.
     */
    public function testA600StatusIsRejectedExactlyAsStockGuzzleRejectsIt(): void
    {
        $stock = new Client(['handler' => HandlerStack::create(new CurlHandler()), 'proxy' => '']);
        try {
            $stock->post($this->origin() . '/status/600?c=curl', ['body' => 'x']);
            $this->fail('stock Guzzle accepted a 600');
        } catch (\GuzzleHttp\Exception\RequestException $control) {
            $this->assertFalse($control->hasResponse());
        }

        try {
            $this->client()->post('/status/600?c=ferro', ['body' => 'x']);
            $this->fail('Ferro accepted a 600');
        } catch (IndeterminateRequestException $e) {
            $this->assertSame($control->getMessage(), $e->getMessage(), 'stock\'s own words');
            $this->assertSame(get_class($control->getPrevious() ?? $control), get_class($e->getPrevious() ?? $e));
            $this->assertSame(600, Fate::of($e)?->status);
        }
        $this->assertSame(1, $this->received('/status/600?c=ferro'), 'the upstream received it: that is why it is Indeterminate');
    }
}
