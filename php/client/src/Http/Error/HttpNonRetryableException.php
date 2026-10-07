<?php // /php/client/src/Http/Error/HttpNonRetryableException.php
declare(strict_types=1);
namespace Ferro\Http\Error;

use Ferro\Client\Error\NonRetryableException;
use Ferro\Http\FateClass;
use Ferro\Protocol\ErrorPayload;

/**
 * A NonRetryable Ferro HTTP failure (SPEC §23.7.1): a policy refusal (`Forbidden`, a `forbidden_*`
 * cause), a refused certificate (`TlsRefused`), or a timeout of a request declared idempotent
 * (`QueryTimeout`, §9.2's read rule). A truncated response body is the dedicated
 * {@see ResponseIncompleteException}.
 */
final class HttpNonRetryableException extends NonRetryableException implements HttpException
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

    public function fate(): FateClass { return FateClass::NonRetryable; }
}
