<?php // /php/psr18/src/Http/Exception/IndeterminateBodyReadException.php
declare(strict_types=1);
namespace Ferro\Http\Exception;

use Ferro\Http\Fate\Indeterminate;

/**
 * A streamed body failed after a 5xx (or 600–999) head on a non-idempotent request: §23.7.1's
 * combined fate is Indeterminate. Never retried by anything in Ferro.
 */
final class IndeterminateBodyReadException extends BodyReadException implements Indeterminate
{
}
