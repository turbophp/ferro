<?php // /php/laravel/src/Http/Retry.php
declare(strict_types=1);
namespace Ferro\Laravel\Http;

use Ferro\Http\Fate;
use Ferro\Http\FateClass;

/**
 * Ferro's retry rules for Laravel's `Http::retry()` (SPEC §23.11.4) — the same rules as
 * `Ferro\Guzzle\Retry`, read through Laravel's wrapping:
 *
 *     Http::retry(3, 100, when: Ferro\Laravel\Http\Retry::when())->post('https://api.example.com/charge', $body);
 *
 * Laravel hands the callback a `ConnectionException` (wrapping a `ConnectException`, which
 * {@see Fate::of()} reads through `getPrevious()`), a raw Guzzle `RequestException` (Laravel does not
 * wrap it), or — for a 4xx/5xx response — `$response->toException()`, an Illuminate
 * `RequestException` whose `->response->toPsrResponse()` is the Ferro response with its fate.
 *
 * It returns true ONLY for a Retryable fate. **An Indeterminate failure is never retried** — a POST
 * whose connection died after it was sent, which `Http::retry(3)` with no `when` WOULD re-send, as
 * it would under curl — nor an Indeterminate status (a 5xx of a non-idempotent request), nor
 * anything whose fate cannot be read (a stray non-Ferro exception), which counts as non-idempotent.
 */
final class Retry
{
    private function __construct() {}

    /**
     * The `when` callback. Its argument is NULLABLE: on a response that is not `successful()` but not
     * a failure either — a 304, or any 3xx under `withoutRedirecting()` — Laravel passes
     * `$response->toException()`, which is null below 4xx, so a callback typed `\Throwable` threw a
     * TypeError on every such response (M6-F9 review F-A). Null is not a failure: not retried.
     *
     * @return \Closure(?\Throwable, mixed=): bool
     */
    public static function when(): \Closure
    {
        return static fn (?\Throwable $exception, mixed $request = null): bool
            => $exception !== null && Fate::of($exception)?->fate === FateClass::Retryable;
    }
}
