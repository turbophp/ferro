<?php // /php/guzzle/src/Guzzle/Retry.php
declare(strict_types=1);
namespace Ferro\Guzzle;

use Ferro\Http\Fate;
use Ferro\Http\FateClass;
use GuzzleHttp\Middleware;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * **Ferro's retry decider for Guzzle's `Middleware::retry()`** (SPEC §23.11.4) — the policy layer
 * the engine deliberately is not (charter rule 3):
 *
 *     $stack->push(Ferro\Guzzle\Retry::middleware(3));
 *     // or, with a delay function of your own:
 *     $stack->push(Middleware::retry(Ferro\Guzzle\Retry::decider(3), $myDelay));
 *
 * It reads every outcome through {@see Fate::of()} and:
 *
 *  - **retries** a failure marked {@see \Ferro\Http\Fate\Retryable} — not sent (a dial failure, an
 *    admission refusal, "dispatched, not sent"), or sent but declared idempotent;
 *  - **retries** a response whose status {@see \Ferro\Http\StatusFate} calls Retryable (408, 425,
 *    429, 503 with `Retry-After`, and a 5xx of an idempotent request), honouring `Retry-After` — and
 *    the engine's own `retry_after_ms` on `RateLimited` — with exponential backoff otherwise;
 *  - **never** retries an {@see \Ferro\Http\Fate\Indeterminate} failure (a POST whose connection died
 *    after it was sent), an Indeterminate status (a 5xx of a non-idempotent request), a
 *    NonRetryable one, or anything whose fate it cannot read (`null`: a response a middleware
 *    rebuilt, a curl exception) — which is treated as non-idempotent.
 *
 * Whatever the CLASS of the exception: an Indeterminate POST rejected as a `ConnectException` (curl
 * would have reported 28 or 52) is refused here, where a naive `instanceof ConnectException`
 * decider would re-send it.
 */
final class Retry
{
    /** @var \WeakMap<RequestInterface, int> the `Retry-After` the decider saw for a request it chose to retry */
    private \WeakMap $retryAfter;

    public function __construct(
        private readonly int $maxRetries,
        /** The first backoff; each further retry doubles it. */
        private readonly int $baseDelayMs = 1000,
        /** The ceiling on any delay, `Retry-After` included. */
        private readonly int $maxDelayMs = 60_000,
    ) {
        if ($maxRetries < 0 || $baseDelayMs < 0 || $maxDelayMs < 0) {
            throw new \InvalidArgumentException('retry counts and delays must not be negative');
        }
        $this->retryAfter = new \WeakMap();
    }

    /**
     * The decider alone, for `Middleware::retry($decider, $delay)` with a delay function of the
     * caller's own (`Retry-After` is then the caller's to honour).
     *
     * @return \Closure(int, RequestInterface, ?ResponseInterface=, mixed=): bool
     */
    public static function decider(int $maxRetries): \Closure
    {
        return (new self($maxRetries))->deciderFn();
    }

    /** `Middleware::retry()` with this decider and a delay that honours `Retry-After`. */
    public static function middleware(int $maxRetries, int $baseDelayMs = 1000, int $maxDelayMs = 60_000): callable
    {
        $retry = new self($maxRetries, $baseDelayMs, $maxDelayMs);
        return Middleware::retry($retry->deciderFn(), $retry->delayFn());
    }

    /** @return \Closure(int, RequestInterface, ?ResponseInterface=, mixed=): bool */
    public function deciderFn(): \Closure
    {
        return fn (int $retries, RequestInterface $request, ?ResponseInterface $response = null, mixed $exception = null): bool
            => $this->shouldRetry($retries, $request, $response, $exception);
    }

    /** @return \Closure(int, ?ResponseInterface=, ?RequestInterface=): int */
    public function delayFn(): \Closure
    {
        return fn (int $retries, ?ResponseInterface $response = null, ?RequestInterface $request = null): int
            => $this->delayMs($retries, $request);
    }

    public function shouldRetry(int $retries, RequestInterface $request, ?ResponseInterface $response = null, mixed $exception = null): bool
    {
        if ($retries >= $this->maxRetries) {
            return false;
        }
        $subject = $exception instanceof \Throwable ? $exception : $response;
        if ($subject === null) {
            return false;
        }
        $fate = Fate::of($subject);
        if ($fate === null || $fate->fate !== FateClass::Retryable) {
            return false; // Indeterminate, NonRetryable, not a failure, or unreadable: never re-sent
        }
        if ($fate->retryAfterMs !== null) {
            $this->retryAfter[$request] = $fate->retryAfterMs;
        } else {
            unset($this->retryAfter[$request]);
        }
        return true;
    }

    /** The delay before retry number `$retries` (1-based) of `$request`. */
    public function delayMs(int $retries, ?RequestInterface $request = null): int
    {
        if ($request !== null && isset($this->retryAfter[$request])) {
            return min($this->maxDelayMs, $this->retryAfter[$request]);
        }
        $delay = $this->baseDelayMs * (2 ** max(0, $retries - 1));
        return (int) min($this->maxDelayMs, $delay);
    }
}
