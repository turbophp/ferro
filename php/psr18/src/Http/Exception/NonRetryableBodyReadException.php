<?php // /php/psr18/src/Http/Exception/NonRetryableBodyReadException.php
declare(strict_types=1);
namespace Ferro\Http\Exception;

use Ferro\Http\Fate\NonRetryable;

/** A streamed body failed after its head, and retrying will not help (or the request was applied). */
final class NonRetryableBodyReadException extends BodyReadException implements NonRetryable
{
}
