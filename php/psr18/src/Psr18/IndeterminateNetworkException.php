<?php // /php/psr18/src/Psr18/IndeterminateNetworkException.php
declare(strict_types=1);
namespace Ferro\Psr18;

use Ferro\Http\Fate\Indeterminate;

/** A non-idempotent request was sent and its fate is unknown (SPEC §9.2). Never retried by Ferro. */
final class IndeterminateNetworkException extends NetworkException implements Indeterminate
{
}
