<?php // /php/laravel/tests/Unit/HttpFactoryTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Unit;

use Ferro\Client\Connection;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Guzzle\FerroHandler;
use Ferro\Guzzle\IndeterminateConnectException;
use Ferro\Guzzle\IndeterminateRequestException;
use Ferro\Laravel\FerroServiceProvider;
use Ferro\Laravel\Http\FerroHttpFactory;
use Ferro\Laravel\Http\HttpWiring;
use Ferro\Laravel\Http\Retry;
use Ferro\Laravel\Tests\Support\RecordingDispatcher;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeTransport;
use Ferro\Tests\Support\HttpFrames as F;
use Illuminate\Container\Container;
use Illuminate\Http\Client\ConnectionException;
use Illuminate\Http\Client\Events\ConnectionFailed;
use Illuminate\Http\Client\Events\RequestSending;
use Illuminate\Http\Client\Events\ResponseReceived;
use Illuminate\Http\Client\Factory;
use Illuminate\Http\Client\Request;
use Illuminate\Support\Facades\Facade;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Fluent;
use PHPUnit\Framework\TestCase;

/**
 * M6-F9: the Laravel `Http` factory rebinding (SPEC §23.11.6) against `illuminate/http` v11, over
 * `ferro/client`'s in-memory transport: what reached "the engine" is the REQUEST frames written.
 */
final class HttpFactoryTest extends TestCase
{
    private const ORIGIN = 'https://api.example.com';

    private int $connects = 0;

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
        parent::tearDown();
    }

    private function factory(FakeTransport $t, ?RecordingDispatcher $events = null): FerroHttpFactory
    {
        $t->feed(F::helloAck(C::FEATURE_ENGINE_HTTP));
        return new FerroHttpFactory(function () use ($t): FerroHandler {
            return new FerroHandler(function () use ($t): Connection {
                ++$this->connects;
                $session = new Session($t, new RequestIdAllocator(0));
                $session->hello();
                return new Connection($session, 'default');
            }, [self::ORIGIN => 'up']);
        }, $events);
    }

    public function testHttpGetReachesTheEngineThroughTheStockPendingRequest(): void
    {
        $t = new FakeTransport();
        $events = new RecordingDispatcher();
        $http = $this->factory($t, $events);
        $t->feed(F::head(1, 200, [['content-type', 'application/json']]) . F::body(1, '{"ok":true}') . F::done(1));

        $res = $http->withToken('t0k')->get(self::ORIGIN . '/v1/x', ['q' => 'a b']);
        $this->assertTrue($res->ok());
        $this->assertSame(['ok' => true], $res->json());
        $req = F::requests($t->written)[1];
        $this->assertSame('/v1/x?q=a%20b', $req['target']);
        $this->assertContains(['Authorization', 'Bearer t0k'], $req['headers']);
        $this->assertSame([RequestSending::class, ResponseReceived::class], $events->classes(), 'events as stock');
        $this->assertSame(1, $this->connects);
    }

    public function testHttpFakeStillWinsAndNothingReachesTheEngine(): void
    {
        $t = new FakeTransport();
        $events = new RecordingDispatcher();
        $http = $this->factory($t, $events);
        $http->fake([self::ORIGIN . '/*' => Factory::response(['faked' => true], 201)]);

        $res = $http->post(self::ORIGIN . '/v1/charge', ['amount' => 5]);
        $this->assertSame(201, $res->status());
        $this->assertSame(['faked' => true], $res->json());
        $this->assertSame(0, $this->connects, 'no connection to the engine was ever opened');
        $this->assertSame([], F::requests($t->written));
        $http->assertSent(static fn (Request $r): bool => $r->url() === self::ORIGIN . '/v1/charge' && $r['amount'] === 5);
        $http->assertSentCount(1);
        $this->assertSame([RequestSending::class, ResponseReceived::class], $events->classes(), 'events and recording as stock');
    }

    public function testPreventStrayRequestsStillRefusesBeforeTheHandler(): void
    {
        $t = new FakeTransport();
        $http = $this->factory($t);
        $http->preventStrayRequests();
        try {
            $http->get(self::ORIGIN . '/x');
            $this->fail('a stray request must be refused');
        } catch (\RuntimeException $e) {
            $this->assertStringContainsString('without a matching fake', $e->getMessage());
        }
        $this->assertSame(0, $this->connects);
    }

    public function testAnIndeterminateSurfacesAsLaravelWrapsCurlsClass(): void
    {
        // timeout: curl 28 → ConnectException → Laravel's ConnectionException, the Ferro exception as previous.
        $t = new FakeTransport();
        $events = new RecordingDispatcher();
        $http = $this->factory($t, $events);
        $t->feed(F::error(1, C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'timeout'));
        try {
            $http->post(self::ORIGIN . '/charge', ['a' => 1]);
            $this->fail('expected the exception');
        } catch (ConnectionException $e) {
            $this->assertInstanceOf(IndeterminateConnectException::class, $e->getPrevious());
        }
        $this->assertContains(ConnectionFailed::class, $events->classes());

        // reset after send: curl 56 → RequestException, which Laravel does NOT wrap.
        $t = new FakeTransport();
        $http = $this->factory($t);
        $t->feed(F::error(1, C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'reset'));
        try {
            $http->post(self::ORIGIN . '/charge', ['a' => 1]);
            $this->fail('expected the exception');
        } catch (IndeterminateRequestException $e) {
            $this->assertInstanceOf(\GuzzleHttp\Exception\RequestException::class, $e, 'escapes Http::send() raw, as curl\'s 56 does');
        }
    }

    public function testRetryWhenNeverResendsAnIndeterminateAndDoesResendARetryable(): void
    {
        $t = new FakeTransport();
        $http = $this->factory($t);
        $t->feed(F::error(1, C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'timeout'));
        try {
            $http->retry(3, 0, Retry::when())->post(self::ORIGIN . '/charge', ['a' => 1]);
            $this->fail('expected the exception');
        } catch (ConnectionException) {
        }
        $this->assertCount(1, F::requests($t->written), 'sent exactly once');

        // The control: Http::retry(3) with no `when` re-sends the very same Indeterminate POST.
        $t = new FakeTransport();
        $http = $this->factory($t);
        $t->feed(F::error(1, C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'timeout'));
        $t->feed(F::head(2, 200) . F::done(2));
        $this->assertTrue($http->retry(3, 0)->post(self::ORIGIN . '/charge', ['a' => 1])->ok());
        $this->assertCount(2, F::requests($t->written), 'stock retry() re-sent the POST');

        $t = new FakeTransport();
        $http = $this->factory($t);
        $t->feed(F::error(1, C::ERR_UPSTREAM_UNAVAILABLE, C::BRANCH_RETRYABLE, 'connect_refused'));
        $t->feed(F::error(2, C::ERR_RATE_LIMITED, C::BRANCH_RETRYABLE, 'rate_limited', 1));
        $t->feed(F::head(3, 201) . F::done(3));
        $this->assertSame(201, $http->retry(3, 0, Retry::when())->post(self::ORIGIN . '/charge', ['a' => 1])->status());
        $this->assertCount(3, F::requests($t->written), 'Retryable: retried');
    }

    public function testRetryWhenReadsAFailedResponsesFate(): void
    {
        foreach ([[500, false, 1], [500, true, 2], [503, false, 1], [429, false, 2]] as [$status, $idempotent, $sent]) {
            $t = new FakeTransport();
            $http = $this->factory($t);
            $t->feed(F::head(1, $status, [], $idempotent) . F::done(1));
            $t->feed(F::head(2, 200, [], $idempotent) . F::done(2));
            $res = $http->retry(2, 0, Retry::when(), throw: false)->post(self::ORIGIN . '/s');
            $this->assertCount($sent, F::requests($t->written), "{$status} idempotent=" . var_export($idempotent, true));
            $this->assertSame($sent === 2 ? 200 : $status, $res->status());
        }
    }

    /**
     * The review's F-A: on a 3xx that is not `successful()` (a 304, or any 3xx under
     * `withoutRedirecting()`), Laravel hands `when` the NULL `toException()`; a callback typed
     * `\Throwable` threw a TypeError there. Sync and pool, with the stock-closure control the review
     * used to show the trap is Laravel's.
     */
    public function testRetryWhenAcceptsTheNullLaravelPassesOnA3xx(): void
    {
        foreach ([[304, []], [302, [['location', '/elsewhere']]]] as [$status, $headers]) {
            $t = new FakeTransport();
            $http = $this->factory($t);
            $t->feed(F::head(1, $status, $headers) . F::done(1));
            $res = $http->withoutRedirecting()->retry(3, 0, Retry::when(), throw: false)->get(self::ORIGIN . '/etag');
            $this->assertSame($status, $res->status());
            $this->assertCount(1, F::requests($t->written), 'a 3xx is not retried');
        }

        $t = new FakeTransport();
        $http = $this->factory($t);
        $t->feed(F::head(1, 304) . F::done(1));
        // throw: false — with Laravel's default `throw: true` a non-failed 3xx pool slot is
        // `toException()`, i.e. null, which is stock Laravel's own behaviour for any `when`.
        $r = $http->pool(static fn ($p) => [$p->withoutRedirecting()->retry(3, 0, Retry::when(), throw: false)->get(self::ORIGIN . '/etag')]);
        $this->assertInstanceOf(\Illuminate\Http\Client\Response::class, $r[0], 'the pool slot is a response, not a TypeError');
        $this->assertSame(304, $r[0]->status());

        $this->assertFalse(Retry::when()(null));
    }

    /**
     * The review's MAA: an UNREADABLE fate is never retried — here an `Http::fake()` 500, a stock psr7
     * response with no Ferro fate on a POST.
     */
    public function testRetryWhenNeverRetriesAnUnreadableFate(): void
    {
        $http = new FerroHttpFactory(static fn () => new FerroHandler(static fn () => throw new \LogicException('no engine'), [self::ORIGIN => 'up']));
        $http->fake(['*' => $http->sequence()->push('boom', 500)->push('ok', 200)]);
        $res = $http->retry(3, 0, Retry::when(), throw: false)->post(self::ORIGIN . '/charge');
        $this->assertSame(500, $res->status(), 'a fate-less 500 POST is not re-sent');
        $http->assertSentCount(1);
    }

    public function testThePoolRunsConcurrentlyThroughOneHandler(): void
    {
        $t = new FakeTransport();
        $http = $this->factory($t);
        $t->feed(F::head(1, 200) . F::body(1, 'a') . F::done(1) . F::head(2, 200) . F::body(2, 'b') . F::done(2));
        $responses = $http->pool(static fn ($pool) => [
            $pool->get(self::ORIGIN . '/a'),
            $pool->get(self::ORIGIN . '/b'),
        ]);
        $this->assertSame(['a', 'b'], [$responses[0]->body(), $responses[1]->body()]);
        $this->assertSame(['/a', '/b'], array_column(F::requests($t->written), 'target'), 'both through Ferro');
        $this->assertSame(1, $this->connects, 'one connection, one session');
    }

    /**
     * Found by this slice's first pool test: Laravel's `Pool` hands the factory
     * `GuzzleHttp\Utils::chooseHandler()` for every pooled request, which replaced the Ferro handler
     * — the pool went to curl (and the environment's proxy), silently. A stock transport now keeps
     * Ferro; an application's own handler is still honoured.
     */
    public function testAStockTransportHandedToTheFactoryKeepsFerroAnApplicationsHandlerDoesNot(): void
    {
        $this->assertTrue(FerroHttpFactory::isStockTransport(\GuzzleHttp\Utils::chooseHandler()));
        $this->assertTrue(FerroHttpFactory::isStockTransport(new \GuzzleHttp\Handler\StreamHandler()));
        // What chooseHandler() returns when there is no curl_multi_exec and no allow_url_fopen (MJ).
        $this->assertTrue(FerroHttpFactory::isStockTransport(new \GuzzleHttp\Handler\CurlHandler()));
        $this->assertTrue(FerroHttpFactory::isStockTransport(new \GuzzleHttp\Handler\CurlMultiHandler()));
        $this->assertFalse(FerroHttpFactory::isStockTransport(new \GuzzleHttp\Handler\MockHandler()));
        $this->assertFalse(FerroHttpFactory::isStockTransport(static fn () => null));

        $t = new FakeTransport();
        $http = $this->factory($t);
        $t->feed(F::head(1, 200) . F::body(1, 'ferro') . F::done(1));
        $this->assertSame('ferro', $http->setHandler(\GuzzleHttp\Utils::chooseHandler())->get(self::ORIGIN . '/x')->body());
        $mock = new \GuzzleHttp\Handler\MockHandler([new \GuzzleHttp\Psr7\Response(200, [], 'mocked')]);
        $this->assertSame('mocked', $http->setHandler($mock)->get(self::ORIGIN . '/x')->body());
        $this->assertCount(1, F::requests($t->written));
    }

    // ---- the provider -----------------------------------------------------------------------------

    public function testWithoutAnHttpFerroBlockTheFactoryIsUntouched(): void
    {
        $app = new Container();
        $app->instance('config', new Fluent(['database' => []]));
        (new FerroServiceProvider($app))->register();
        $this->assertFalse($app->bound(Factory::class));
    }

    public function testAnHttpFerroBlockRebindsTheFactoryTheFacadeResolves(): void
    {
        $app = new Container();
        $app->instance('config', new Fluent(['http' => ['ferro' => [
            'socket' => '/nonexistent/ferro.sock',
            'upstreams' => [self::ORIGIN => 'up'],
        ]]]));
        (new FerroServiceProvider($app))->register();
        $this->assertTrue($app->bound(Factory::class));
        Facade::setFacadeApplication($app);
        $root = Http::getFacadeRoot();
        $this->assertInstanceOf(FerroHttpFactory::class, $root);
        $this->assertSame($root, $app->make(Factory::class), 'a singleton');

        // Faked through the FACADE: answered without the socket ever being dialled (it does not exist).
        Http::fake(['*' => Http::response('stubbed')]);
        $this->assertSame('stubbed', Http::get(self::ORIGIN . '/x')->body());
        Http::assertSentCount(1);
    }

    public function testAMisconfiguredBlockIsLoud(): void
    {
        $app = new Container();
        $app->instance('config', new Fluent(['http' => ['ferro' => ['upstreams' => [self::ORIGIN => 'up']]]]));
        $this->expectException(\InvalidArgumentException::class);
        HttpWiring::register($app);
    }

    /**
     * The review's MB: the refusal itself. A Laravel whose factory no longer DECLARES
     * `newPendingRequest()` (renamed, or only inherited) must refuse to wire, loudly.
     */
    public function testAFactoryWithoutTheSeamIsRefusedLoudly(): void
    {
        foreach ([\stdClass::class, SeamInherited::class] as $factory) {
            try {
                HttpWiring::assertSeam($factory);
                $this->fail("{$factory}: expected the refusal");
            } catch (\LogicException $e) {
                $this->assertStringContainsString('newPendingRequest()', $e->getMessage());
                $this->assertStringContainsString('refusing to wire', $e->getMessage());
            }
        }
    }

    /**
     * The review's MR/MS: the `http.ferro` block's `pool` and timeouts reach `Ferro::connect()` under
     * the right parameter, and the defaults are `Ferro::connect()`'s own.
     */
    public function testTheConnectArgumentsAreTheBlocksByParameterName(): void
    {
        $args = HttpWiring::connectArguments(HttpWiring::settings([
            'socket' => '/run/ferro/app.sock',
            'upstreams' => [self::ORIGIN => 'up'],
            'pool' => 'reports',
            'io_timeout' => 9,
            'connect_timeout' => 0.5,
        ]));
        $this->assertSame(['socketPath' => '/run/ferro/app.sock', 'pool' => 'reports', 'connectTimeout' => 0.5, 'ioTimeout' => 9.0], $args);
        $params = array_map(static fn (\ReflectionParameter $p): string => $p->getName(), (new \ReflectionMethod(\Ferro\Ferro::class, 'connect'))->getParameters());
        foreach (array_keys($args) as $name) {
            $this->assertContains($name, $params, "{$name} is a Ferro::connect() parameter");
        }
        $defaults = HttpWiring::connectArguments(HttpWiring::settings(['socket' => '/s', 'upstreams' => [self::ORIGIN => 'up']]));
        $this->assertSame(['socketPath' => '/s', 'pool' => 'default', 'connectTimeout' => 2.0, 'ioTimeout' => 5.0], $defaults);
    }

    public function testTheSeamIsAsserted(): void
    {
        HttpWiring::assertSeam(); // illuminate/http v11: newPendingRequest() is declared on Factory
        $m = new \ReflectionMethod(Factory::class, 'newPendingRequest');
        $this->assertTrue($m->isProtected(), 'the seam is protected, which is why it is pinned (§23.11.6)');
        $this->assertSame(FerroHttpFactory::class, (new \ReflectionMethod(FerroHttpFactory::class, 'newPendingRequest'))->getDeclaringClass()->getName());
    }
}

/** A factory that INHERITS `newPendingRequest()` instead of declaring it: not the seam's owner. */
final class SeamInherited extends \Illuminate\Http\Client\Factory
{
}
