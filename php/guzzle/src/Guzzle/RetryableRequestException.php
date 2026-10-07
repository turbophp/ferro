<?php // /php/guzzle/src/Guzzle/RetryableRequestException.php
declare(strict_types=1);
namespace Ferro\Guzzle;

use Ferro\Http\Fate\Retryable;

/**
 * A request-class failure that is safe to retry: "dispatched, not sent" (`unsent_write`,
 * `unsent_closed`), or a reset after sending on a request the engine counted idempotent.
 */
final class RetryableRequestException extends RequestException implements Retryable
{
}
