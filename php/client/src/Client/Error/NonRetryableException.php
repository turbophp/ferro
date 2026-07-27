<?php // /php/client/src/Client/Error/NonRetryableException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * Branch 3 ({@see \Ferro\Protocol\Generated\Constants::BRANCH_NON_RETRYABLE}) — a deterministic
 * failure that WILL recur if replayed as-is: a SQL syntax error, a unique/foreign-key/not-null/check
 * violation, auth, unsupported. Retrying is pointless and hides the bug, so it propagates.
 *
 * This class is ALSO the safe home for an unknown/garbled `branch` byte (any value not in {1,2,3}):
 * defaulting a garbled branch to Retryable would breach the never-retry property (§19.3), so
 * {@see ErrorMapper} maps the unknown case here — the strictly safe (non-retryable) fate. Carries the
 * decoded `ErrorPayload`.
 */
final class NonRetryableException extends FerroException
{
    use CarriesErrorPayload;
}
