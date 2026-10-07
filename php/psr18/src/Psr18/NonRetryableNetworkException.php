<?php // /php/psr18/src/Psr18/NonRetryableNetworkException.php
declare(strict_types=1);
namespace Ferro\Psr18;

use Ferro\Http\Fate\NonRetryable;

/** Retrying as-is will not help, or the request was applied (a body that failed after a 2xx head). */
final class NonRetryableNetworkException extends NetworkException implements NonRetryable
{
}
