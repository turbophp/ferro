<?php // /php/guzzle/src/Guzzle/IndeterminateRequestException.php
declare(strict_types=1);
namespace Ferro\Guzzle;

use Ferro\Http\Fate\Indeterminate;

/**
 * A non-idempotent request was sent and its connection failed with no usable answer — a reset after
 * sending (curl 56), a malformed or partial head, or a body truncated after a 5xx head (§23.7.1's
 * combined fate). Never re-sent by {@see Retry} or `Ferro\Laravel\Http\Retry`.
 */
final class IndeterminateRequestException extends RequestException implements Indeterminate
{
}
