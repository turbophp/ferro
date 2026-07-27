<?php // /php/client/src/Client/Error/ErrorMapper.php
declare(strict_types=1);
namespace Ferro\Client\Error;

use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Outcome;

/**
 * Turns a non-`Ok` {@see Outcome} into the {@see FerroException} the query/tx path throws.
 *
 * The three-branch taxonomy is chosen from the WIRE `branch` byte ALONE (decision W-3), never from
 * the error `code`'s range — so an unknown/future `code` still classifies correctly:
 *   - {@see C::BRANCH_RETRYABLE} (1)     → {@see RetryableException}
 *   - {@see C::BRANCH_INDETERMINATE} (2) → {@see IndeterminateException}
 *   - {@see C::BRANCH_NON_RETRYABLE} (3) → {@see NonRetryableException}
 *
 * **A branch byte NOT in {1,2,3} maps to {@see NonRetryableException}, NEVER a retryable default**
 * (§19.3): a garbled branch defaulting to Retryable would let the client silently replay a statement
 * whose true fate is unknown — a breach of the engine's never-retry safety property. `Cancelled`
 * (status 2) becomes a {@see CancelledException}.
 */
final class ErrorMapper
{
    /** Map any non-`Ok` outcome to its exception. (An `Ok` outcome is a caller bug → protocol fault.) */
    public static function fromOutcome(Outcome $outcome): FerroException
    {
        if ($outcome->isCancelled()) {
            return new CancelledException();
        }
        if (!$outcome->isError()) {
            return new ProtocolException('ErrorMapper::fromOutcome called on an Ok outcome');
        }
        return self::fromErrorPayload($outcome->errorPayload());
    }

    /** Classify a decoded {@see ErrorPayload} on its wire `branch` byte. */
    public static function fromErrorPayload(ErrorPayload $ep): FerroException
    {
        return match ($ep->branch) {
            C::BRANCH_RETRYABLE     => new RetryableException($ep),
            C::BRANCH_INDETERMINATE => new IndeterminateException($ep),
            C::BRANCH_NON_RETRYABLE => new NonRetryableException($ep),
            // Unknown/garbled branch: the SAFE fate is non-retryable. Defaulting to Retryable would
            // breach §19.3 (silent replay of a possibly-applied statement). Payload carried verbatim.
            default => new NonRetryableException($ep),
        };
    }
}
