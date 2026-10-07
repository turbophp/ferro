<?php // /php/psr18/src/Psr18/UnrepresentableResponseException.php
declare(strict_types=1);
namespace Ferro\Psr18;

use Ferro\Http\Fate\Carrier;
use Ferro\Http\FateClass;
use Ferro\Http\HttpFate;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Message\RequestInterface;

/**
 * The upstream ANSWERED, but the response factory refused to build what it sent (SPEC §23.5.2,
 * §23.7.4): a status of 600..=999, which `ferrod` passes through and `guzzlehttp/psr7` refuses
 * ("Status code must be an integer value between 1xx and 5xx"), or a header value the factory
 * rejects. PSR-18 requires a `ClientExceptionInterface` when "the HTTP response could not be parsed
 * into a PSR-7 response object", and that is this.
 *
 * **The fate is the status's** ({@see \Ferro\Http\StatusFate}, which reads 600..=999 as a 5xx per
 * RFC 9110 §15): Retryable for an idempotent request, **Indeterminate** for a non-idempotent one —
 * the upstream received it, and an invalid status promises nothing about what it did. `getPrevious()`
 * is the factory's exception; {@see status()} is what the upstream sent.
 */
abstract class UnrepresentableResponseException extends \RuntimeException implements ClientExceptionInterface, Carrier
{
    final public function __construct(
        string $message,
        private readonly RequestInterface $request,
        private readonly HttpFate $ferroFate,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
    }

    public static function create(string $message, RequestInterface $request, HttpFate $fate, ?\Throwable $previous = null): self
    {
        return match ($fate->fate) {
            FateClass::Retryable => new RetryableUnrepresentableResponseException($message, $request, $fate, $previous),
            FateClass::Indeterminate => new IndeterminateUnrepresentableResponseException($message, $request, $fate, $previous),
            default => new NonRetryableUnrepresentableResponseException($message, $request, $fate, $previous),
        };
    }

    public function getRequest(): RequestInterface
    {
        return $this->request;
    }

    /** The status the upstream sent. */
    public function status(): ?int
    {
        return $this->ferroFate->status;
    }

    public function ferroFate(): HttpFate
    {
        return $this->ferroFate;
    }
}
