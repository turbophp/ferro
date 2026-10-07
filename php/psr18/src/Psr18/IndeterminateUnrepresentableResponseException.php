<?php // /php/psr18/src/Psr18/IndeterminateUnrepresentableResponseException.php
declare(strict_types=1);
namespace Ferro\Psr18;

use Ferro\Http\Fate\Indeterminate;

/** An unrepresentable (600–999) response to a non-idempotent request: never retried by Ferro. */
final class IndeterminateUnrepresentableResponseException extends UnrepresentableResponseException implements Indeterminate
{
}
