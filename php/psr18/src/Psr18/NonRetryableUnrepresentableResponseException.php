<?php // /php/psr18/src/Psr18/NonRetryableUnrepresentableResponseException.php
declare(strict_types=1);
namespace Ferro\Psr18;

use Ferro\Http\Fate\NonRetryable;

/** An unrepresentable response whose status does not call for a retry. */
final class NonRetryableUnrepresentableResponseException extends UnrepresentableResponseException implements NonRetryable
{
}
