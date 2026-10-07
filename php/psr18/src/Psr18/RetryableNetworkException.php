<?php // /php/psr18/src/Psr18/RetryableNetworkException.php
declare(strict_types=1);
namespace Ferro\Psr18;

use Ferro\Http\Fate\Retryable;

/** The request was not sent, or it was declared idempotent: a caller's policy MAY retry it. */
final class RetryableNetworkException extends NetworkException implements Retryable
{
}
