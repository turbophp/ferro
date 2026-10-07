<?php // /php/psr18/src/Http/Exception/BodyReadException.php
declare(strict_types=1);
namespace Ferro\Http\Exception;

use Ferro\Http\Fate\Carrier;
use Ferro\Http\FateClass;
use Ferro\Http\HttpFate;

/**
 * A STREAMED response body failed after its head (SPEC §23.11.3, "after HEAD, streamed body
 * failed"): thrown from `StreamInterface::read()` (and everything built on it) by
 * {@see \Ferro\Http\Adapter\BodyStream}, the lazy body of Guzzle's `stream => true` and of every
 * `ferro/psr18` response. A `\RuntimeException`, as PSR-7 says a failed read is, carrying the fate:
 * {@see create} picks the concrete class whose marker is that fate, and `getPrevious()` is the
 * `ferro/client` exception (a `ResponseIncompleteException` for a non-idempotent request, whose
 * `wasApplied()` says whether a 2xx head means the request was applied).
 */
abstract class BodyReadException extends \RuntimeException implements Carrier
{
    final public function __construct(string $message, private readonly HttpFate $ferroFate, ?\Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }

    public function ferroFate(): HttpFate
    {
        return $this->ferroFate;
    }

    public static function create(string $message, HttpFate $fate, ?\Throwable $previous = null): self
    {
        return match ($fate->fate) {
            FateClass::Retryable => new RetryableBodyReadException($message, $fate, $previous),
            FateClass::Indeterminate => new IndeterminateBodyReadException($message, $fate, $previous),
            default => new NonRetryableBodyReadException($message, $fate, $previous),
        };
    }
}
