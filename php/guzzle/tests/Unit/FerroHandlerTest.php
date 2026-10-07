<?php // /php/guzzle/tests/Unit/FerroHandlerTest.php
declare(strict_types=1);
namespace Ferro\Guzzle\Tests\Unit;

use Ferro\Guzzle\ConnectException;
use Ferro\Guzzle\FerroHandler;
use Ferro\Guzzle\IndeterminateConnectException;
use Ferro\Guzzle\IndeterminateRequestException;
use Ferro\Guzzle\NonRetryableConnectException;
use Ferro\Guzzle\NonRetryableRequestException;
use Ferro\Guzzle\RequestException;
use Ferro\Guzzle\RetryableConnectException;
use Ferro\Guzzle\RetryableRequestException;
use Ferro\Guzzle\UnmappedOriginException;
use Ferro\Http\Adapter\BodyStream;
use Ferro\Http\Adapter\Failure;
use Ferro\Http\Error\HttpIndeterminateException;
use Ferro\Http\Error\RequestTooLargeException;
use Ferro\Http\Error\ResponseIncompleteException;
use Ferro\Http\Fate;
use Ferro\Http\Fate\Indeterminate;
use Ferro\Http\Fate\NonRetryable;
use Ferro\Http\Fate\Retryable;
use Ferro\Http\FateClass;
use Ferro\Http\FerroResponse;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeTransport;
use Ferro\Tests\Support\HttpFrames as F;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Pool;
use GuzzleHttp\Promise\CancellationException;
use GuzzleHttp\Promise\Utils;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\TransferStats;
use PHPUnit\Framework\Attributes\DataProvider;

/**
 * M6-F9: `Ferro\Guzzle\FerroHandler` under a real `GuzzleHttp\Client` and `HandlerStack`, over the
 * in-memory transport (SPEC §23.11.2, §23.11.3).
 */
final class FerroHandlerTest extends HandlerTestCase
{
    // ---- the round trip ---------------------------------------------------------------------------

    public function testASynchronousRequestRoundTripsEveryFieldAndOption(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 200, [['content-type', 'text/plain'], ['set-cookie', 'a=1'], ['set-cookie', 'b=2']], true));
        $t->feed(F::body(1, 'hel') . F::body(1, 'lo') . F::done(1, [['x-trailer', 't']]));

        $res = $client->post('/v1/x?y=1', [
            'body' => 'payload',
            'headers' => ['X-A' => ['1', '2']],
            'timeout' => 1.5,
            'connect_timeout' => 0.25,
            'read_timeout' => 2,
            'ferro' => ['idempotent' => true, 'route' => '/v1/x'],
        ]);

        $this->assertInstanceOf(FerroResponse::class, $res);
        $this->assertSame(200, $res->getStatusCode());
        $this->assertSame('hello', (string) $res->getBody());
        $this->assertSame(['content-type', 'set-cookie'], array_keys($res->getHeaders()), 'names lowercase, as delivered (C9 item 5)');
        $this->assertSame(['a=1', 'b=2'], $res->getHeader('Set-Cookie'));
        $this->assertSame('1.1', $res->getProtocolVersion());
        $this->assertSame('OK', $res->getReasonPhrase());
        $this->assertTrue($res->ferroFate()->idempotent);
        $this->assertSame(FateClass::NotAFailure, $res->ferroFate()->fate);

        $req = F::requests($t->written)[1];
        $this->assertSame('up', $req['upstream']);
        $this->assertSame('POST', $req['method']);
        $this->assertSame('/v1/x?y=1', $req['target']);
        $this->assertSame(self::ORIGIN, $req['origin'], 'the normalised origin is sent, so a drifted map is a loud forbidden_origin');
        $this->assertSame('payload', $req['body']);
        $this->assertContains(['X-A', '1'], $req['headers']);
        $this->assertContains(['X-A', '2'], $req['headers'], 'duplicates kept');
        $this->assertContains(['Host', 'api.example.com'], $req['headers']);
        $this->assertSame(1500, $req['timeout_ms']);
        $this->assertSame(250, $req['connect_timeout_ms']);
        $this->assertSame(2000, $req['read_timeout_ms']);
        $this->assertTrue($req['idempotent']);
        $this->assertTrue($req['decode'], 'decode_content defaults to true in GuzzleHttp\\Client');
        $this->assertSame('/v1/x', $req['route']);
    }

    public function testAnEmptyPathIsSlashAFragmentIsNeverSentAndAGetHasNoBody(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 204) . F::done(1));
        $client->get('https://API.example.com:443#frag', ['timeout' => 0, 'decode_content' => false]);
        $req = F::requests($t->written)[1];
        $this->assertSame('/', $req['target']);
        $this->assertNull($req['body'], 'a GET with an empty body sends none');
        $this->assertNull($req['timeout_ms'], 'timeout 0 is "none": the upstream\'s ceiling applies (C9 item 3)');
        $this->assertFalse($req['decode']);
    }

    public function testAnEmptyPostBodyIsSentAsAnEmptyBody(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 201) . F::done(1));
        $client->post('/p');
        $this->assertSame('', F::requests($t->written)[1]['body']);
    }

    // ---- routing and refusals ---------------------------------------------------------------------

    public function testAnUnmappedOriginIsRefusedAndNothingIsSent(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        try {
            $client->get('https://elsewhere.example.com/x');
            $this->fail('expected the refusal');
        } catch (UnmappedOriginException $e) {
            $this->assertInstanceOf(\GuzzleHttp\Exception\RequestException::class, $e);
            $this->assertInstanceOf(NonRetryable::class, $e);
            $this->assertFalse($e->hasResponse());
            $this->assertStringContainsString('https://elsewhere.example.com', $e->getMessage());
        }
        $this->assertSame([], F::requests($t->written));
    }

    public function testAnExplicitFallbackServesUnmappedOriginsOnly(): void
    {
        $t = new FakeTransport();
        $mock = new MockHandler([new Response(299, [], 'from the fallback')]);
        $client = $this->client($this->handler($t, $mock));
        $this->assertSame('from the fallback', (string) $client->get('https://elsewhere.example.com/x')->getBody());
        $this->assertSame([], F::requests($t->written));
    }

    /** @return array<string, array{0: array<string, mixed>, 1: string}> */
    public static function refusedOptions(): array
    {
        return [
            'verify false' => [['verify' => false], 'CA_FILE'],
            'cert' => [['cert' => '/x.pem'], 'CLIENT_CERT_FILE'],
            'ssl_key' => [['ssl_key' => '/x.key'], 'CLIENT_KEY_FILE'],
            'crypto_method tls 1.3' => [['crypto_method' => STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT], 'MIN_TLS'],
            'crypto_method_max' => [['crypto_method_max' => STREAM_CRYPTO_METHOD_TLSv1_2_CLIENT], 'MIN_TLS'],
            'force_ip_resolve' => [['force_ip_resolve' => 'v4'], 'force_ip_resolve'],
            'proxy' => [['proxy' => 'http://proxy.internal:3128'], 'no outbound proxy'],
            'multiplex require' => [['multiplex' => 'require_eager'], 'HTTP/2'],
        ];
    }

    /** @param array<string, mixed> $options */
    #[DataProvider('refusedOptions')]
    public function testAnOptionTheDaemonOwnsIsRefusedNamingWhatReplacesIt(array $options, string $names): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        try {
            $client->get('/x', $options);
            $this->fail('expected the refusal');
        } catch (NonRetryableRequestException $e) {
            $this->assertStringContainsString($names, $e->getMessage());
            $this->assertFalse($e->hasResponse());
        }
        $this->assertSame([], F::requests($t->written), 'nothing sent');
    }

    public function testTheProxyGuzzleDerivesFromTheEnvironmentIsIgnoredNotRefused(): void
    {
        $_SERVER['HTTPS_PROXY'] = 'http://egress.internal:8080';
        try {
            $t = new FakeTransport();
            $client = $this->client($this->handler($t)); // Client reads the environment here
            $t->feed(self::head(1, 200) . F::done(1));
            $this->assertSame(200, $client->get('/x')->getStatusCode());
            $this->assertCount(1, F::requests($t->written));
        } finally {
            unset($_SERVER['HTTPS_PROXY']);
        }
    }

    public function testVerifyTrueOrACaPathAndTheVersionAreIgnored(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 200) . F::done(1) . self::head(2, 200) . F::done(2));
        $client->get('/a', ['verify' => '/etc/ssl/ca.pem', 'version' => '2.0', 'curl' => [1 => 2], 'debug' => false]);
        $client->get('/b', ['verify' => true, 'version' => '1.0']);
        $this->assertCount(2, F::requests($t->written));
    }

    public function testABodyOverTheFrameCapIsRefusedBeforeAByteIsWritten(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $before = $t->writeCalls;
        try {
            $client->post('/up', ['body' => str_repeat('x', C::MAX_FRAME_PAYLOAD + 1)]);
            $this->fail('expected the refusal');
        } catch (NonRetryableRequestException $e) {
            $this->assertInstanceOf(RequestTooLargeException::class, $e->getPrevious());
            $this->assertStringContainsString('MAX_FRAME_PAYLOAD', $e->getMessage());
        }
        $this->assertSame($before, $t->writeCalls);
    }

    // ---- §23.11.3: the cause decides the class, the fate decides the marker -----------------------

    /**
     * §23.11.3's table, transcribed independently of {@see Failure::CAUSE_KIND}: the Guzzle class
     * curl produces for the same physical event.
     *
     * @return array<string, array{0: string, 1: class-string}>
     */
    public static function causes(): array
    {
        $connect = ['dns', 'connect_refused', 'connect_unreachable', 'connect_timeout', 'tls_handshake', 'tls_version',
            'tls_alpn', 'breaker_open', 'breaker_probe_busy', 'rate_limited', 'retry_after_hold', 'queue_full',
            'queue_timeout', 'body_budget', 'deadline', 'draining', 'timeout', 'eof_empty', 'read_idle', 'cancelled'];
        $out = [];
        foreach (C::HTTP_CAUSES as $cause) {
            $out[$cause] = [$cause, in_array($cause, $connect, true) ? ConnectException::class : RequestException::class];
        }
        return $out;
    }

    /** @param class-string $class */
    #[DataProvider('causes')]
    public function testEveryCauseRejectsWithCurlsClassAndItsFatesMarker(string $cause, string $class): void
    {
        foreach ([
            [C::BRANCH_RETRYABLE, C::ERR_CONNECTION_LOST, Retryable::class],
            [C::BRANCH_INDETERMINATE, C::ERR_WRITE_UNCONFIRMED, Indeterminate::class],
            [C::BRANCH_NON_RETRYABLE, C::ERR_QUERY_TIMEOUT, NonRetryable::class],
        ] as [$branch, $code, $marker]) {
            $t = new FakeTransport();
            $client = $this->client($this->handler($t));
            $t->feed(F::error(1, $code, $branch, $cause, 1234));
            try {
                $client->post('/x', ['body' => 'b']);
                $this->fail("{$cause}: expected a rejection");
            } catch (\Throwable $e) {
                $this->assertInstanceOf($class, $e, "{$cause} on branch {$branch}");
                $this->assertInstanceOf($marker, $e, "{$cause} on branch {$branch}: the marker is the fate");
                $this->assertInstanceOf(\Ferro\Http\Error\HttpException::class, $e->getPrevious(), 'the ferro/client exception is chained');
                $fate = Fate::of($e);
                $this->assertNotNull($fate);
                $this->assertSame($cause, $fate->cause);
                $this->assertSame(1234, $fate->retryAfterMs);
                if ($e instanceof \GuzzleHttp\Exception\RequestException) {
                    $this->assertFalse($e->hasResponse(), 'no head arrived: no response');
                }
            }
        }
    }

    public function testTheCauseTableIsTotalOverTheRegistry(): void
    {
        $this->assertEqualsCanonicalizing(C::HTTP_CAUSES, array_keys(Failure::CAUSE_KIND));
    }

    public function testATimeoutOfASentPostIsAConnectExceptionMarkedIndeterminate(): void
    {
        // The defining cell: curl's class (28 → ConnectException), so a naive decider behaves as under
        // curl, and Ferro's marker, so Ferro's deciders never re-send it.
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(F::error(1, C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'timeout'));
        try {
            $client->post('/charge', ['body' => '{}']);
            $this->fail('expected the rejection');
        } catch (IndeterminateConnectException $e) {
            $this->assertInstanceOf(\GuzzleHttp\Exception\ConnectException::class, $e);
            $this->assertInstanceOf(HttpIndeterminateException::class, $e->getPrevious());
            $this->assertSame('timeout', $e->getHandlerContext()['ferro_cause']);
        }
    }

    public function testABodyThatFailsAfterAHeadCarriesTheResponseAndTheCombinedFate(): void
    {
        foreach ([[201, NonRetryableRequestException::class, true], [500, IndeterminateRequestException::class, null]] as [$status, $class, $applied]) {
            $t = new FakeTransport();
            $client = $this->client($this->handler($t));
            $t->feed(self::head(1, $status) . F::body(1, 'par'));
            $t->feed(F::error(1, C::ERR_RESPONSE_INCOMPLETE, C::BRANCH_NON_RETRYABLE, 'body_reset'));
            try {
                $client->post('/x', ['body' => 'b', 'http_errors' => false]);
                $this->fail('expected the rejection');
            } catch (RequestException $e) {
                $this->assertInstanceOf($class, $e, "after a {$status} head");
                $this->assertTrue($e->hasResponse(), 'the response the upstream did send');
                $this->assertSame($status, $e->getResponse()?->getStatusCode());
                $prev = $e->getPrevious();
                $this->assertInstanceOf(ResponseIncompleteException::class, $prev);
                $this->assertSame($applied, $prev->wasApplied());
            }
        }
    }

    public function testADialFailureOfAPostIsARetryableConnectException(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(F::error(1, C::ERR_UPSTREAM_UNAVAILABLE, C::BRANCH_RETRYABLE, 'connect_refused'));
        $this->expectException(RetryableConnectException::class);
        $client->post('/x', ['body' => 'b']);
    }

    public function testDispatchedNotSentIsARetryableRequestException(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(F::error(1, C::ERR_CONNECTION_LOST, C::BRANCH_RETRYABLE, 'unsent_closed'));
        $this->expectException(RetryableRequestException::class);
        $client->post('/x', ['body' => 'b']);
    }

    public function testADeclaredReadsTimeoutIsNonRetryable(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(F::error(1, C::ERR_QUERY_TIMEOUT, C::BRANCH_NON_RETRYABLE, 'timeout'));
        $this->expectException(NonRetryableConnectException::class);
        $client->get('/x', ['ferro' => ['idempotent' => true]]);
    }

    // ---- statuses ---------------------------------------------------------------------------------

    public function testAnyStatusIsAResponseAndHttpErrorsThrowsAsStock(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 404) . F::body(1, 'nope') . F::done(1));
        $t->feed(self::head(2, 404) . F::body(2, 'nope') . F::done(2));
        try {
            $client->get('/missing');
            $this->fail('http_errors must throw on a 404');
        } catch (ClientException $e) {
            $this->assertInstanceOf(FerroResponse::class, $e->getResponse());
            $this->assertSame(FateClass::NonRetryable, Fate::of($e)?->fate, 'read off the response the exception carries');
        }
        $this->assertSame(404, $client->get('/missing', ['http_errors' => false])->getStatusCode());
    }

    /**
     * §23.5.2 / R2-4, carried to F9: `ferrod` passes a status of 600..=999 through, and
     * `guzzlehttp/psr7`'s `Response` refuses one ≥ 600. Stock Guzzle rejects such a response with a
     * `RequestException` "An error was encountered while creating the response" (no response); so does
     * this handler — with the status's fate (RFC 9110 §15 reads it as a 5xx) as the marker, and the
     * exchange CANCELled rather than read.
     */
    public function testAStatusPsr7CannotRepresentIsRejectedAsStockWithTheStatusesFate(): void
    {
        foreach ([[false, IndeterminateRequestException::class], [true, RetryableRequestException::class]] as [$idempotent, $class]) {
            $t = new FakeTransport();
            $client = $this->client($this->handler($t));
            $t->feed(self::head(1, 600, [], $idempotent) . F::cancelled(1));
            try {
                $client->post('/x', ['body' => 'b']);
                $this->fail('expected the rejection');
            } catch (RequestException $e) {
                $this->assertInstanceOf($class, $e);
                $this->assertSame('An error was encountered while creating the response', $e->getMessage());
                $this->assertFalse($e->hasResponse());
                $this->assertInstanceOf(\InvalidArgumentException::class, $e->getPrevious());
                $this->assertSame(600, Fate::of($e)?->status);
            }
            $this->assertSame([1], F::cancels($t->written), 'the body is not read: the exchange is stopped');
            $this->assertFalse($this->session?->hasRequestsInFlight(), 'CANCELled AND drained to its terminal, not left to a destructor');
        }
    }

    // ---- decoding, sink, callbacks ------------------------------------------------------------------

    public function testAnEngineDecodedBodyRenamesTheRemovedHeadersAsCurlFactoryDoes(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 200, [['content-type', 'text/plain']], false, ['gzip', 42]) . F::body(1, 'abc') . F::done(1));
        $res = $client->get('/z');
        $this->assertSame('gzip', $res->getHeaderLine('x-encoded-content-encoding'));
        $this->assertSame('42', $res->getHeaderLine('x-encoded-content-length'));
        $this->assertSame('3', $res->getHeaderLine('Content-Length'), 'EasyHandle\'s rule: the decoded length');
        $this->assertFalse($res->hasHeader('Content-Encoding'));
    }

    public function testTheSinkOptionReceivesTheBody(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $file = tempnam(sys_get_temp_dir(), 'ferro-sink-');
        $this->assertIsString($file);
        try {
            $t->feed(self::head(1, 200) . F::body(1, 'to ') . F::body(1, 'disk') . F::done(1));
            $client->get('/f', ['sink' => $file]);
            $this->assertSame('to disk', file_get_contents($file));
        } finally {
            @unlink($file);
        }
    }

    public function testOnStatsOnTrailersAndProgressFireAsStock(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 200, [['content-length', '5']]) . F::body(1, 'ab') . F::body(1, 'cde') . F::done(1, [['X-T', 'v']]));
        $stats = null;
        $trailers = null;
        $progress = [];
        $client->post('/s', [
            'body' => 'xyz',
            'on_stats' => static function (TransferStats $s) use (&$stats): void { $stats = $s; },
            'on_trailers' => static function (array $tr, $res) use (&$trailers): void { $trailers = $tr; },
            'progress' => static function (int $dt, int $d, int $ut, int $u) use (&$progress): void { $progress[] = [$dt, $d, $ut, $u]; },
        ]);
        $this->assertInstanceOf(TransferStats::class, $stats);
        $this->assertSame(200, $stats->getResponse()?->getStatusCode());
        $this->assertEqualsWithDelta(5e-6, $stats->getTransferTime(), 1e-9);
        $this->assertSame(7, $stats->getHandlerStat('size_download'));
        $this->assertSame(6, $stats->getHandlerStat('size_upload'));
        $this->assertSame(['x-t' => ['v']], $trailers);
        $this->assertSame([[0, 0, 3, 3], [5, 2, 3, 3], [5, 5, 3, 3]], $progress, 'upload once at the head, then each chunk');
    }

    public function testOnHeadersThrowingCancelsAndRejectsWithTheResponse(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 200) . F::cancelled(1));
        try {
            $client->get('/h', ['on_headers' => static function (): void { throw new \DomainException('too big'); }]);
            $this->fail('expected the rejection');
        } catch (RequestException $e) {
            $this->assertSame('An error was encountered during the on_headers event', $e->getMessage());
            $this->assertInstanceOf(\DomainException::class, $e->getPrevious());
            $this->assertTrue($e->hasResponse());
        }
        $this->assertSame([1], F::cancels($t->written));
    }

    // ---- stream => true ---------------------------------------------------------------------------

    public function testAStreamedBodyIsLazyAndReturnsCreditOnlyAsItIsRead(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 200) . F::body(1, 'one') . F::body(1, 'two') . F::done(1));
        $res = $client->get('/s', ['stream' => true]);
        $body = $res->getBody();
        $this->assertInstanceOf(BodyStream::class, $body);
        $this->assertCount(1, F::windowUpdates($t->written, 1), 'only the HEAD replenished before a read');
        $this->assertSame('one', $body->read(1024));
        $this->assertCount(1, F::windowUpdates($t->written, 1), 'a chunk read but not passed: its credit is still owed');
        $this->assertSame('two', $body->read(1024));
        $this->assertCount(2, F::windowUpdates($t->written, 1));
        $this->assertSame('', $body->read(1024));
        $this->assertTrue($body->eof());
        $this->assertSame([], F::cancels($t->written));
    }

    public function testClosingAStreamedBodyEarlyCancels(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 200) . F::body(1, 'one') . F::cancelled(1));
        $body = $client->get('/s', ['stream' => true])->getBody();
        $this->assertSame('o', $body->read(1));
        $body->close();
        $this->assertSame([1], F::cancels($t->written));
    }

    public function testAStreamedBodyFailureIsARuntimeExceptionCarryingTheFate(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $t->feed(self::head(1, 500) . F::body(1, 'par') . F::error(1, C::ERR_RESPONSE_INCOMPLETE, C::BRANCH_NON_RETRYABLE, 'body_eof'));
        $body = $client->post('/s', ['stream' => true, 'body' => 'x', 'http_errors' => false])->getBody();
        $this->assertSame('par', $body->read(10));
        try {
            $body->read(10);
            $this->fail('expected the failure');
        } catch (\RuntimeException $e) {
            $this->assertInstanceOf(Indeterminate::class, $e, 'a non-idempotent 500 truncated: the combined fate');
            $this->assertInstanceOf(ResponseIncompleteException::class, $e->getPrevious());
        }
    }

    // ---- asynchronous: concurrency, delay, cancel -------------------------------------------------

    public function testEveryRequestOfABatchIsWrittenAtInvokeTime(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $promises = [$client->getAsync('/a'), $client->getAsync('/b'), $client->getAsync('/c')];
        $this->assertSame([1, 2, 3], array_keys(F::requests($t->written)), 'all three on the wire before any wait (P11)');
        $t->feed(self::head(2, 200) . F::body(2, 'b') . F::done(2) . self::head(1, 200) . F::body(1, 'a') . F::done(1));
        $t->feed(self::head(3, 200) . F::body(3, 'c') . F::done(3));
        $res = Utils::unwrap($promises);
        $this->assertSame(['a', 'b', 'c'], array_map(static fn ($r): string => (string) $r->getBody(), $res));
    }

    public function testADelayedRequestIsQueuedAndSubmittedByTheWaitLoop(): void
    {
        $t = new FakeTransport();
        $handler = $this->handler($t);
        $client = $this->client($handler);
        $p = $client->getAsync('/d', ['delay' => 30]);
        $this->assertSame([], F::requests($t->written), 'nothing sent at invoke');
        $this->assertSame(1, $handler->pending());
        $t->feed(self::head(1, 200) . F::done(1));
        $start = microtime(true);
        $this->assertSame(200, $p->wait()->getStatusCode());
        $this->assertGreaterThanOrEqual(0.025, microtime(true) - $start);
        $this->assertSame(0, $handler->pending());
    }

    public function testAPoolOfDelayedRequestsCompletesInAboutOneDelayNotN(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        for ($i = 1; $i <= 5; ++$i) {
            $t->feed(self::head($i, 200) . F::done($i));
        }
        $requests = (static function () {
            for ($i = 0; $i < 5; ++$i) {
                yield new Request('GET', self::ORIGIN . "/p{$i}");
            }
        })();
        $ok = 0;
        $start = microtime(true);
        (new Pool($client, $requests, [
            'concurrency' => 5,
            'options' => ['delay' => 200],
            'fulfilled' => static function () use (&$ok): void { ++$ok; },
        ]))->promise()->wait();
        $elapsed = microtime(true) - $start;
        $this->assertSame(5, $ok);
        $this->assertGreaterThanOrEqual(0.19, $elapsed);
        $this->assertLessThan(0.6, $elapsed, 'about d (0.2 s), not N × d (1.0 s)');
    }

    public function testCancellingAQueuedRequestSendsNothing(): void
    {
        $t = new FakeTransport();
        $handler = $this->handler($t);
        $p = $this->client($handler)->getAsync('/d', ['delay' => 10_000]);
        $p->cancel();
        $this->assertSame(0, $handler->pending());
        $this->assertSame([], F::requests($t->written));
        $this->expectException(CancellationException::class);
        $p->wait();
    }

    public function testCancellingAnInFlightRequestCancelsItInTheEngine(): void
    {
        $t = new FakeTransport();
        $handler = $this->handler($t);
        $p = $this->client($handler)->getAsync('/slow');
        $this->assertCount(1, F::requests($t->written));
        $p->cancel();
        gc_collect_cycles();
        $this->assertSame([1], F::cancels($t->written));
        $this->assertSame(0, $handler->pending());
    }

    public function testAStreamedAsyncRequestIsOpenedByTheWaitLoop(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $p = $client->getAsync('/s', ['stream' => true]);
        $this->assertSame([], F::requests($t->written), 'opened when waited, as CurlMultiHandler attaches on tick');
        $t->feed(self::head(1, 200) . F::body(1, 'x') . F::done(1));
        $this->assertSame('x', (string) $p->wait()->getBody());
    }

    public function testAnAsyncFailureIsMappedLikeASynchronousOne(): void
    {
        $t = new FakeTransport();
        $client = $this->client($this->handler($t));
        $p = $client->postAsync('/x', ['body' => 'b']);
        $t->feed(F::error(1, C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'reset'));
        try {
            $p->wait();
            $this->fail('expected the rejection');
        } catch (IndeterminateRequestException $e) {
            $this->assertFalse($e->hasResponse());
        }
    }

    public function testAConnectionThatCannotBeOpenedIsRetryableAndNothingWasSent(): void
    {
        $handler = new FerroHandler(static fn () => throw new \RuntimeException('no socket'), [self::ORIGIN => 'up']);
        $client = $this->client($handler);
        try {
            $client->post('/x', ['body' => 'b']);
            $this->fail('expected the rejection');
        } catch (RetryableConnectException $e) {
            $this->assertStringContainsString('no socket', $e->getMessage());
        }
    }
}
