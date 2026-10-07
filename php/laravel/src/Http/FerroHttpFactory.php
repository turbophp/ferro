<?php // /php/laravel/src/Http/FerroHttpFactory.php
declare(strict_types=1);
namespace Ferro\Laravel\Http;

use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Http\Client\Factory;
use Illuminate\Http\Client\PendingRequest;

/**
 * Laravel's `Http` client factory with Ferro HTTP as its transport (SPEC §23.11.6): it overrides
 * ONLY `newPendingRequest()`, to hand every pending request the Ferro Guzzle handler.
 *
 * `PendingRequest::buildHandlerStack()` then builds `HandlerStack::create($handler)` and pushes
 * Laravel's own middleware, before-sending callbacks, recorder and STUB handlers ABOVE it — so
 * `Http::fake()` still wins: a faked request is answered by the stub and never reaches the handler
 * (no engine contact at all), and recording, `Http::assertSent()` and the request/response events
 * behave exactly as stock. Everything else — `Http::get()`, `Http::pool()`, `retry()`, `withToken()`
 * — is the stock factory's.
 *
 * Rejected alternatives (§23.11.6): `globalOptions(['handler' => …])` replaces the whole client stack,
 * bypassing the stub and the recorder (and Guzzle 7.12+ deprecates a request-level handler); a global
 * middleware is pushed outermost, so a short-circuit there would skip `Http::fake()`.
 *
 * The handler is built on first use, so an application that never makes an HTTP call — or fakes
 * every one — never connects to the engine for it.
 */
final class FerroHttpFactory extends Factory
{
    private ?\Closure $handler = null;

    /**
     * @param \Closure(): callable $handlerFactory builds the Ferro handler (`Ferro\Guzzle\FerroHandler`)
     */
    public function __construct(private readonly \Closure $handlerFactory, ?Dispatcher $dispatcher = null)
    {
        parent::__construct($dispatcher);
    }

    /**
     * @return PendingRequest
     */
    protected function newPendingRequest()
    {
        return parent::newPendingRequest()->setHandler($this->handler());
    }

    /**
     * **`Http::pool()` would otherwise bypass Ferro, silently.** Laravel's `Http\Client\Pool` builds
     * every pooled request with `$factory->setHandler(GuzzleHttp\Utils::chooseHandler())` — Guzzle's
     * stock curl/stream transport — which, through `Factory::__call`, replaced the Ferro handler on each
     * of them (measured: a pooled request went to curl and the environment's proxy, not the engine).
     * So a STOCK Guzzle transport handed to the factory keeps the Ferro handler: that is the pool's
     * default, not a choice the application made. Any other handler — an application's own
     * `MockHandler`, say — is honoured as stock Laravel honours it. A pending request's own
     * `setHandler()` (`Http::withOptions(…)->setHandler(…)` reaches the PendingRequest directly) is
     * untouched.
     *
     * @param callable $handler
     * @return PendingRequest
     */
    public function setHandler($handler)
    {
        $pending = $this->createPendingRequest();
        return self::isStockTransport($handler) ? $pending : $pending->setHandler($handler);
    }

    /** Whether `$handler` is one of Guzzle's own transports (curl, stream, or `Utils::chooseHandler()`'s proxy). */
    public static function isStockTransport(mixed $handler): bool
    {
        if ($handler instanceof \GuzzleHttp\Handler\CurlHandler
            || $handler instanceof \GuzzleHttp\Handler\CurlMultiHandler
            || $handler instanceof \GuzzleHttp\Handler\StreamHandler) {
            return true;
        }
        return $handler instanceof \Closure
            && (new \ReflectionFunction($handler))->getClosureScopeClass()?->getName() === \GuzzleHttp\Handler\Proxy::class;
    }

    /** The one Ferro handler every pending request of this factory shares (one wait loop). */
    public function handler(): \Closure
    {
        return $this->handler ??= \Closure::fromCallable(($this->handlerFactory)());
    }
}
