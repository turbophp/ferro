<?php // /php/guzzle/src/Guzzle/RetryableConnectException.php
declare(strict_types=1);
namespace Ferro\Guzzle;

use Ferro\Http\Fate\Retryable;

/** A connect-class failure of a request that was not sent, or was declared idempotent. */
final class RetryableConnectException extends ConnectException implements Retryable
{
}
