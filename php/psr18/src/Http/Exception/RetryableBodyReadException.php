<?php // /php/psr18/src/Http/Exception/RetryableBodyReadException.php
declare(strict_types=1);
namespace Ferro\Http\Exception;

use Ferro\Http\Fate\Retryable;

/** A streamed body failed after its head on a request the engine counted idempotent. */
final class RetryableBodyReadException extends BodyReadException implements Retryable
{
}
