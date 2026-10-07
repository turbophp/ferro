<?php // /php/laravel/tests/Live/HttpFacadeLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Guzzle\IndeterminateConnectException;
use Ferro\Laravel\FerroServiceProvider;
use Ferro\Laravel\Http\FerroHttpFactory;
use Ferro\Laravel\Http\Retry;
use Ferro\Laravel\Tests\Support\RecordingDispatcher;
use Ferro\Tests\Live\HttpLiveTestCase;
use GuzzleHttp\Middleware;
use Illuminate\Container\Container;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Http\Client\Events\RequestSending;
use Illuminate\Http\Client\Events\ResponseReceived;
use Illuminate\Http\Client\Pool;
use Illuminate\Support\Facades\Facade;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Fluent;

/**
 * M6-F9: Laravel's `Http` facade through the package's own provider (SPEC §23.11.6) against a REAL
 * `ferrod` and the recording loopback upstream. `Http::fake()` is proven by the upstream's log:
 * a faked request is never received.
 */
final class HttpFacadeLiveTest extends HttpLiveTestCase
{
    private RecordingDispatcher $events;
    private string $deadOrigin = '';

    protected function setUp(): void
    {
        parent::setUp();
        $app = new Container();
        $this->events = new RecordingDispatcher();
        $app->instance(Dispatcher::class, $this->events);
        $app->instance('config', new Fluent(['http' => ['ferro' => [
            'socket' => $this->socketPath,
            'upstreams' => [$this->origin() => 'up', $this->deadOrigin => 'dead'],
        ]]]));
        (new FerroServiceProvider($app))->register();
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication($app);
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
        gc_collect_cycles();
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

    public function testHttpGetReachesTheEngineWithEventsAsStock(): void
    {
        $this->assertInstanceOf(FerroHttpFactory::class, Http::getFacadeRoot());
        $res = Http::withHeaders(['X-App' => 'demo'])->post($this->origin() . '/echo?l=1', ['a' => 1]);
        $this->assertTrue($res->ok());
        $this->assertSame(['a' => 1], json_decode(base64_decode((string) $res->json('body')), true));
        $this->assertSame(1, $this->received('/echo?l=1'), 'the contact assertion');
        $this->assertSame([RequestSending::class, ResponseReceived::class], $this->events->classes());
    }

    public function testHttpFakeStillWinsAndTheEngineNeverSeesTheRequest(): void
    {
        Http::fake([$this->origin() . '/*' => Http::response(['faked' => true], 202)]);
        $res = Http::post($this->origin() . '/echo?l=fake', ['a' => 1]);
        $this->assertSame(202, $res->status());
        $this->assertTrue($res->json('faked'));
        Http::assertSent(fn ($r): bool => $r->url() === $this->origin() . '/echo?l=fake');
        Http::assertSentCount(1);
        usleep(200_000);
        $this->assertSame(0, $this->received('/echo'), 'a faked request never reaches the upstream');
        $this->assertSame([RequestSending::class, ResponseReceived::class], $this->events->classes(), 'events as stock under fake too');
    }

    public function testAnIndeterminatePostSurfacesAsAConnectionExceptionAndRetryWhenDoesNotResendIt(): void
    {
        try {
            Http::timeout(1)->retry(3, 0, Retry::when())->post($this->origin() . '/hold?l=ind', ['a' => 1]);
            $this->fail('expected the timeout');
        } catch (ConnectionException $e) {
            $this->assertInstanceOf(IndeterminateConnectException::class, $e->getPrevious());
        }
        usleep(300_000);
        $this->assertSame(1, $this->received('/hold?l=ind'), 'Retry::when(): sent exactly once');
    }

    public function testRetryWhenResendsWhatWasNeverSent(): void
    {
        $attempts = 0;
        try {
            Http::withMiddleware(Middleware::tap(static function () use (&$attempts): void { ++$attempts; }))
                ->retry(3, 0, Retry::when())
                ->post($this->deadOrigin . '/x', ['a' => 1]);
            $this->fail('expected the dial failure');
        } catch (ConnectionException) {
        }
        $this->assertSame(3, $attempts, 'connect_refused: never sent, so retried');
    }

    /**
     * Found by this slice: Laravel's `Http::pool()` hands the factory Guzzle's stock curl handler for
     * every pooled request, which bypassed Ferro. Pooled requests now go through the engine, and run
     * concurrently in it.
     */
    public function testHttpPoolGoesThroughFerroAndRunsConcurrently(): void
    {
        $start = microtime(true);
        $responses = Http::pool(fn (Pool $pool) => [
            $pool->get($this->origin() . '/delay/500?l=p1'),
            $pool->get($this->origin() . '/delay/500?l=p2'),
            $pool->get($this->origin() . '/delay/500?l=p3'),
        ]);
        $elapsed = microtime(true) - $start;
        foreach ($responses as $r) {
            $this->assertSame('delayed', $r->body());
        }
        $this->assertLessThan(1.2, $elapsed, 'three 500 ms calls in about 0.5 s');
        $this->assertSame(3, $this->received('/delay/500?l=p'), 'all three reached the upstream through Ferro');
    }
}
