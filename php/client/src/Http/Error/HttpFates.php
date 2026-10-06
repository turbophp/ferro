<?php // /php/client/src/Http/Error/HttpFates.php
declare(strict_types=1);
namespace Ferro\Http\Error;

use Ferro\Client\Error\ErrorMapper;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Http\ResponseHead;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Outcome;

/**
 * The ONE place a Ferro HTTP failure becomes an exception (SPEC §23.7.1, §23.7.3, §23.11.1), for
 * both its sources:
 *
 *  - {@see fromOutcome}: the ENGINE classified the exchange (§23.7.1's table) and said so in its
 *    terminal. The class is chosen from the wire `branch` byte alone — the {@see ErrorMapper} rule
 *    (W-3), so an unknown or garbled branch is NonRetryable, never Retryable — and `detail` carries
 *    the `[http.causes]` token;
 *  - {@see linkLost}: the link to the engine died first, so the CLIENT classifies, by §23.7.3's
 *    table. It cannot see the upstream's configuration, so before `HEAD` a request counts as
 *    idempotent only if the request itself declared `idempotent = true`; after `HEAD` it uses the
 *    engine's `HttpHead.idempotent`.
 *
 * @internal
 */
final class HttpFates
{
    /**
     * The exception for a non-`Ok` HTTP terminal. `$head` is the head already read, if any — a
     * `ResponseIncomplete` carries it ({@see ResponseIncompleteException}).
     */
    public static function fromOutcome(Outcome $outcome, ?ResponseHead $head): FerroException
    {
        if ($outcome->isCancelled()) {
            return new HttpCancelledException($head);
        }
        if (!$outcome->isError()) {
            return new ProtocolException('HttpFates::fromOutcome called on an Ok outcome');
        }
        $ep = $outcome->errorPayload();
        $cause = $ep->detail;
        if ($cause === null) {
            // `Protocol` and `Unsupported` (an `https` upstream before slice F5, a method with no
            // route): the two terminals that are not an exchange's fate carry no cause (§23.5.6
            // as amended at F2), so they map as any engine error does.
            return ErrorMapper::fromErrorPayload($ep);
        }
        return match ($ep->branch) {
            C::BRANCH_RETRYABLE => new HttpRetryableException($ep, $cause),
            C::BRANCH_INDETERMINATE => new HttpIndeterminateException($ep, $cause),
            default => $ep->code === C::ERR_RESPONSE_INCOMPLETE && $head !== null
                ? new ResponseIncompleteException($ep, $cause, $head)
                : new HttpNonRetryableException($ep, $cause),
        };
    }

    /**
     * §23.7.3: the link to the engine died with the request in flight.
     *
     * | In flight when the link died | client-idempotent | otherwise |
     * |---|---|---|
     * | `REQUEST` not completely written (`$sent` false) | Retryable `ConnectionLost` | same |
     * | written, no `HEAD` | Retryable `ConnectionLost` | **Indeterminate `WriteUnconfirmed`** |
     * | `HEAD` received | Retryable `ConnectionLost` | NonRetryable `ResponseIncomplete` |
     *
     * @param bool $declaredIdempotent the request's OWN `idempotent = true` (before `HEAD`)
     */
    public static function linkLost(bool $sent, bool $declaredIdempotent, ?ResponseHead $head, string $reason): FerroException
    {
        $cause = HttpException::CLIENT_LINK_LOST;
        if (!$sent) {
            return new HttpRetryableException(self::payload(
                C::ERR_CONNECTION_LOST,
                C::BRANCH_RETRYABLE,
                'HTTP request not sent — the link to the engine failed before its frame was completely '
                    . 'written, so it cannot have reached the upstream (Retryable): ' . $reason,
            ), $cause, true);
        }
        $idempotent = $head !== null ? $head->idempotent : $declaredIdempotent;
        if ($idempotent) {
            return new HttpRetryableException(self::payload(
                C::ERR_CONNECTION_LOST,
                C::BRANCH_RETRYABLE,
                'link to the engine lost with an idempotent HTTP request in flight (Retryable): ' . $reason,
            ), $cause, true);
        }
        if ($head === null) {
            return new HttpIndeterminateException(self::payload(
                C::ERR_WRITE_UNCONFIRMED,
                C::BRANCH_INDETERMINATE,
                'link to the engine lost after a non-idempotent HTTP request was sent and before its '
                    . 'response head arrived — the upstream may or may not have applied it (Indeterminate): '
                    . $reason,
            ), $cause, true);
        }
        return new ResponseIncompleteException(self::payload(
            C::ERR_RESPONSE_INCOMPLETE,
            C::BRANCH_NON_RETRYABLE,
            sprintf(
                'link to the engine lost mid-body after a %d head on a non-idempotent HTTP request '
                    . '(ResponseIncomplete): %s',
                $head->status,
                $reason,
            ),
        ), $cause, $head, true);
    }

    private static function payload(int $code, int $branch, string $message): ErrorPayload
    {
        return new ErrorPayload($code, $branch, null, null, $message, null, null);
    }
}
