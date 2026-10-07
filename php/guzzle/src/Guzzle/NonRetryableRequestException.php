<?php // /php/guzzle/src/Guzzle/NonRetryableRequestException.php
declare(strict_types=1);
namespace Ferro\Guzzle;

use Ferro\Http\Fate\NonRetryable;

/**
 * A request-class failure that retrying will not fix: a refusal of the request itself, or a body
 * that failed after a head on a request that was applied (`ResponseIncomplete` after a 2xx).
 */
class NonRetryableRequestException extends RequestException implements NonRetryable
{
}
