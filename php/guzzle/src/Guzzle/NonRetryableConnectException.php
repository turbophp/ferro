<?php // /php/guzzle/src/Guzzle/NonRetryableConnectException.php
declare(strict_types=1);
namespace Ferro\Guzzle;

use Ferro\Http\Fate\NonRetryable;

/**
 * A connect-class failure that retrying will not fix: the engine's `timeout` of a declared-idempotent
 * request (§9.2's read rule, `QueryTimeout`), a refused dial the engine classified NonRetryable.
 */
final class NonRetryableConnectException extends ConnectException implements NonRetryable
{
}
