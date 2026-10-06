<?php // /php/client/src/Http/Error/HttpRetryableException.php
declare(strict_types=1);
namespace Ferro\Http\Error;

use Ferro\Client\Error\RetryableException;
use Ferro\Http\FateClass;
use Ferro\Protocol\ErrorPayload;

/**
 * A Retryable Ferro HTTP failure (SPEC §23.7.1, §23.7.3): the request was not sent (a policy or
 * admission refusal, a dial failure, a lost link before the frame left the client), or it was
 * declared idempotent. `UpstreamUnavailable` and `RateLimited` carry {@see retryAfterMs} where the
 * engine set one. Nothing in this client re-sends it.
 */
final class HttpRetryableException extends RetryableException implements HttpException
{
    public function __construct(
        ErrorPayload $errorPayload,
        private readonly string $httpCause,
        private readonly bool $clientSynthesised = false,
    ) {
        parent::__construct($errorPayload);
    }

    public function cause(): string { return $this->httpCause; }

    public function clientSynthesised(): bool { return $this->clientSynthesised; }

    public function fate(): FateClass { return FateClass::Retryable; }
}
