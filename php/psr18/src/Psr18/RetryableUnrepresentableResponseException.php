<?php // /php/psr18/src/Psr18/RetryableUnrepresentableResponseException.php
declare(strict_types=1);
namespace Ferro\Psr18;

use Ferro\Http\Fate\Retryable;

/** An unrepresentable response to an idempotent request. */
final class RetryableUnrepresentableResponseException extends UnrepresentableResponseException implements Retryable
{
}
