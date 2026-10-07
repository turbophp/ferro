<?php // /php/guzzle/src/Guzzle/IndeterminateConnectException.php
declare(strict_types=1);
namespace Ferro\Guzzle;

use Ferro\Http\Fate\Indeterminate;

/**
 * A non-idempotent request was SENT and its connection gave no response — a `timeout` (curl 28) or
 * `eof_empty` (curl 52). Still a `ConnectException`, as curl's would be, so a naive decider re-sends
 * it exactly as it would under curl; Ferro's deciders read this marker and never do.
 */
final class IndeterminateConnectException extends ConnectException implements Indeterminate
{
}
