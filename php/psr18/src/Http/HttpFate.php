<?php // /php/psr18/src/Http/HttpFate.php
declare(strict_types=1);
namespace Ferro\Http;

use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\RetryableException;
use Ferro\Http\Error\HttpCancelledException;
use Ferro\Http\Error\HttpException;
use Ferro\Http\Error\ResponseIncompleteException;

/**
 * What a Ferro HTTP outcome means for a retry (SPEC §23.11.2, §23.11.4): the one value
 * {@see Fate::of()} returns and every Ferro response and Ferro adapter exception carries
 * ({@see Fate\Carrier}).
 *
 *  - {@see $fate}: the verdict — for a response, {@see StatusFate}'s advisory reading of its status;
 *    for a failure, the transport's fate (§23.7.1), combined with the head's status where a head
 *    arrived (`ResponseIncomplete`, §23.7.1's combined fate);
 *  - {@see $idempotent}: the engine's EFFECTIVE idempotency (`HttpHead.idempotent`) when a head
 *    arrived, else null — before a head the client knows only the request's own declaration;
 *  - {@see $status}: the head's status when one arrived;
 *  - {@see $retryAfterMs}: the delay the engine (`RateLimited`, a breaker) or the status's
 *    `Retry-After` asked for;
 *  - {@see $cause}: the `[http.causes]` token of a failure (`link_lost` when the client classified
 *    it, §23.7.3), null for a response.
 *
 * Nothing in Ferro retries on it. A caller's policy MAY retry {@see FateClass::Retryable}, and must
 * never retry {@see FateClass::Indeterminate} unless it holds its own licence (charter rule 3).
 */
final class HttpFate
{
    public function __construct(
        public readonly FateClass $fate,
        public readonly ?int $retryAfterMs = null,
        public readonly ?bool $idempotent = null,
        public readonly ?int $status = null,
        public readonly ?string $cause = null,
        public readonly bool $clientSynthesised = false,
    ) {}

    /** The advisory verdict on a completed exchange's head ({@see StatusFate}). */
    public static function ofHead(ResponseHead $head): self
    {
        $verdict = $head->statusFate();
        return new self($verdict->fate, $verdict->retryAfterMs, $head->idempotent, $head->status);
    }

    /**
     * The fate of a `ferro/client` failure, or null for anything else (an exception this layer
     * cannot vouch for — which callers treat as non-idempotent, §23.11.4).
     *
     * An {@see HttpException} reports its own fate ({@see HttpException::fate()}, the combined fate
     * on `ResponseIncomplete`). Any other `ferro/client` exception is read by its taxonomy base:
     * {@see RetryableException} (the request was not sent, as `InFlightLimitException`),
     * {@see IndeterminateException}, and everything else — `NonRetryable`, `Cancelled`, a protocol
     * or usage error — NonRetryable, never Retryable.
     */
    public static function ofFailure(\Throwable $e): ?self
    {
        if (!$e instanceof FerroException) {
            return null;
        }
        $retryAfter = method_exists($e, 'retryAfterMs') ? $e->retryAfterMs() : null;
        $retryAfter = is_int($retryAfter) ? $retryAfter : null;
        if ($e instanceof HttpException) {
            $head = $e instanceof ResponseIncompleteException ? $e->head() : null;
            if ($head === null && $e instanceof HttpCancelledException) {
                $head = $e->head();
            }
            return new self(
                $e->fate(),
                $retryAfter,
                $head?->idempotent,
                $head?->status,
                $e->cause(),
                $e->clientSynthesised(),
            );
        }
        $fate = match (true) {
            $e instanceof RetryableException => FateClass::Retryable,
            $e instanceof IndeterminateException => FateClass::Indeterminate,
            default => FateClass::NonRetryable,
        };
        return new self($fate, $retryAfter);
    }

    public function isRetryable(): bool
    {
        return $this->fate === FateClass::Retryable;
    }

    public function isIndeterminate(): bool
    {
        return $this->fate === FateClass::Indeterminate;
    }
}
