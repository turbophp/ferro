<?php // /php/guzzle/tests/Unit/RetryTest.php
declare(strict_types=1);
namespace Ferro\Guzzle\Tests\Unit;

use Ferro\Guzzle\IndeterminateConnectException;
use Ferro\Guzzle\IndeterminateRequestException;
use Ferro\Guzzle\Retry;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeTransport;
use Ferro\Tests\Support\HttpFrames as F;
use GuzzleHttp\Exception\ServerException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\ResponseInterface;

/**
 * M6-F9: `Ferro\Guzzle\Retry` through a REAL `Middleware::retry()` stack (SPEC §23.11.4): what is
 * re-sent is read back off the wire, so "never re-sends an Indeterminate" is a count of REQUEST
 * frames, not a return value.
 */
final class RetryTest extends HandlerTestCase
{
    private function retrying(FakeTransport $t, int $max = 3): \GuzzleHttp\Client
    {
        return $this->client($this->handler($t), static function (HandlerStack $s) use ($max): void {
            $s->push(Retry::middleware($max, 0, 0));
        });
    }

    public function testAnIndeterminatePostIsNeverResent(): void
    {
        foreach ([['timeout', IndeterminateConnectException::class], ['reset', IndeterminateRequestException::class], ['eof_empty', IndeterminateConnectException::class]] as [$cause, $class]) {
            $t = new FakeTransport();
            $client = $this->retrying($t);
            $t->feed(F::error(1, C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, $cause));
            try {
                $client->post('/charge', ['body' => '{}']);
                $this->fail('expected the rejection');
            } catch (\Throwable $e) {
                $this->assertInstanceOf($class, $e, $cause);
            }
            $this->assertCount(1, F::requests($t->written), "{$cause}: sent exactly once");
        }
    }

    public function testANaiveConnectExceptionDeciderWouldHaveResentIt(): void
    {
        // The control: the same Indeterminate POST under the decider people write by hand is re-sent,
        // which is exactly the hazard the marker exists for (§23.11.3's consequence).
        $t = new FakeTransport();
        $client = $this->client($this->handler($t), static function (HandlerStack $s): void {
            $s->push(Middleware::retry(
                static fn (int $n, $req, $res = null, $e = null): bool => $n < 1 && $e instanceof \GuzzleHttp\Exception\ConnectException,
                static fn (): int => 0,
            ));
        });
        $t->feed(F::error(1, C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'timeout'));
        $t->feed(self::head(2, 200) . F::done(2));
        $this->assertSame(200, $client->post('/charge', ['body' => '{}'])->getStatusCode());
        $this->assertCount(2, F::requests($t->written), 'the naive decider sent the POST twice');
    }

    public function testARetryableFailureIsResentUpToTheLimit(): void
    {
        $t = new FakeTransport();
        $client = $this->retrying($t);
        $t->feed(F::error(1, C::ERR_UPSTREAM_UNAVAILABLE, C::BRANCH_RETRYABLE, 'connect_refused'));
        $t->feed(F::error(2, C::ERR_CONNECTION_LOST, C::BRANCH_RETRYABLE, 'unsent_closed'));
        $t->feed(self::head(3, 201) . F::done(3));
        $this->assertSame(201, $client->post('/x', ['body' => 'b'])->getStatusCode());
        $this->assertCount(3, F::requests($t->written));

        $t = new FakeTransport();
        $client = $this->retrying($t, 2);
        for ($i = 1; $i <= 3; ++$i) {
            $t->feed(F::error($i, C::ERR_UPSTREAM_UNAVAILABLE, C::BRANCH_RETRYABLE, 'connect_refused'));
        }
        try {
            $client->post('/x', ['body' => 'b']);
            $this->fail('expected the last rejection');
        } catch (\GuzzleHttp\Exception\ConnectException) {
        }
        $this->assertCount(3, F::requests($t->written), 'one attempt and two retries');
    }

    /**
     * The review's MA: a retried request is the SAME PSR-7 object, its body stream already read to the
     * end by the first attempt; the adapter must rewind it, or the retry sends an empty body.
     */
    public function testARetriedRequestSendsItsWholeBodyAgain(): void
    {
        $t = new FakeTransport();
        $client = $this->retrying($t);
        $t->feed(F::error(1, C::ERR_UPSTREAM_UNAVAILABLE, C::BRANCH_RETRYABLE, 'connect_refused'));
        $t->feed(self::head(2, 201) . F::done(2));
        $this->assertSame(201, $client->post('/x', ['body' => 'payload'])->getStatusCode());
        $requests = F::requests($t->written);
        $this->assertSame(['payload', 'payload'], [$requests[1]['body'], $requests[2]['body']]);
    }

    /**
     * The review's MP: the SYNCHRONOUS path (every `Client::request()`) sleeps a request's `delay`, so
     * `Retry::middleware()`'s backoff — here the engine's own `retry_after_ms` — is honoured there too,
     * not only by the async wait loop.
     */
    public function testASynchronousRetryHonoursTheEnginesRetryAfter(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t), static function (HandlerStack $s): void {
            $s->push(Retry::middleware(2, 0, 10_000));
        });
        $t->feed(F::error(1, C::ERR_RATE_LIMITED, C::BRANCH_RETRYABLE, 'rate_limited', 400));
        $t->feed(self::head(2, 200) . F::done(2));
        $start = microtime(true);
        $this->assertSame(200, $client->get('/x')->getStatusCode());
        $this->assertGreaterThan(0.35, microtime(true) - $start, 'retry_after_ms 400 waited out before the retry');
        $this->assertCount(2, F::requests($t->written));
    }

    public function testStatusesAreRetriedByTheirFate(): void
    {
        // POST 500: Indeterminate — never. GET declared idempotent 500: Retryable. POST 503 with
        // Retry-After and POST 429: Retryable whatever the method.
        $cases = [
            ['POST', null, 500, [], 1],
            ['GET', true, 500, [], 2],
            ['POST', null, 503, [['retry-after', '0']], 2],
            ['POST', null, 429, [], 2],
            ['POST', null, 503, [], 1],
            ['POST', null, 404, [], 1],
        ];
        foreach ($cases as [$method, $idem, $status, $headers, $sent]) {
            $t = new FakeTransport();
            $client = $this->retrying($t);
            $t->feed(self::head(1, $status, $headers, $idem === true) . F::done(1));
            $t->feed(self::head(2, 200, [], $idem === true) . F::done(2));
            $res = $client->request($method, '/s', ['http_errors' => false, 'ferro' => ['idempotent' => $idem]]);
            $this->assertCount($sent, F::requests($t->written), "{$method} {$status}");
            $this->assertSame($sent === 2 ? 200 : $status, $res->getStatusCode());
        }
    }

    public function testAResponseAMiddlewareRebuiltHasNoFateAndIsNotRetried(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t), static function (HandlerStack $s): void {
            // The first pushed is the outermost: the decider sees the response the inner middleware rebuilt.
            $s->push(Retry::middleware(3, 0, 0));
            $s->push(Middleware::mapResponse(static fn (ResponseInterface $r): ResponseInterface => new Response($r->getStatusCode())));
        });
        $t->feed(self::head(1, 503, [['retry-after', '0']], true) . F::done(1));
        $this->assertSame(503, $client->get('/s', ['http_errors' => false])->getStatusCode());
        $this->assertCount(1, F::requests($t->written), 'null fate: treated as non-idempotent');
    }

    public function testHttpErrorsAboveTheDeciderStillRetriesByTheResponsesFate(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t), static function (HandlerStack $s): void {
            $s->push(Retry::middleware(1, 0, 0));
        });
        $t->feed(self::head(1, 500, [], true) . F::done(1));
        $t->feed(self::head(2, 500, [], true) . F::done(2));
        try {
            $client->get('/s', ['ferro' => ['idempotent' => true]]);
            $this->fail('expected http_errors');
        } catch (ServerException $e) {
            $this->assertSame(500, $e->getResponse()->getStatusCode());
        }
        $this->assertCount(2, F::requests($t->written));
    }

    public function testTheDelayHonoursRetryAfterAndOtherwiseBacksOff(): void
    {
        $retry = new Retry(5, 100, 2_000);
        $request = new Request('GET', 'https://api.example.com/');
        $this->assertSame(100, $retry->delayMs(1, $request));
        $this->assertSame(400, $retry->delayMs(3, $request));
        $this->assertSame(2_000, $retry->delayMs(9, $request), 'capped');

        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 429, [['retry-after', '1']], false) . F::done(1));
        $res = $client->get('/s', ['http_errors' => false]);
        $this->assertTrue($retry->shouldRetry(0, $request, $res));
        $this->assertSame(1_000, $retry->delayMs(1, $request), 'Retry-After: 1 s');

        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(F::error(1, C::ERR_RATE_LIMITED, C::BRANCH_RETRYABLE, 'rate_limited', 1_500));
        try {
            $client->get('/s');
            $this->fail('expected the refusal');
        } catch (\Throwable $e) {
            $this->assertTrue($retry->shouldRetry(0, $request, null, $e));
            $this->assertSame(1_500, $retry->delayMs(1, $request), 'the engine\'s retry_after_ms');
        }
        $this->assertFalse($retry->shouldRetry(5, $request, $res), 'the limit');
        $this->assertFalse($retry->shouldRetry(0, $request, null, new \RuntimeException('not Ferro')), 'unreadable: never');
    }
}
