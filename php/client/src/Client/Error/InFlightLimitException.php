<?php // /php/client/src/Client/Error/InFlightLimitException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;

/**
 * The session's in-flight limit is reached and no slot can free, because every in-flight request
 * is a Ferro HTTP stream parked on its credit window — which only its holder can replenish
 * (M6-F8 review F2). The request was NOT written, so it cannot have run: Retryable, with the code
 * the engine uses for "no capacity" (`PoolTimeout`). Nothing retries it.
 *
 * Deliberately not a {@see TransportException}: the link is healthy, and a lost-link classification
 * would make a read path reconnect — closing the session and every stream open on it.
 */
final class InFlightLimitException extends RetryableException
{
    public function __construct(string $message)
    {
        parent::__construct(new ErrorPayload(C::ERR_POOL_TIMEOUT, C::BRANCH_RETRYABLE, null, null, $message, null, null));
    }
}
