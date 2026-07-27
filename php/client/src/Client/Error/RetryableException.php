<?php // /php/client/src/Client/Error/RetryableException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * Branch 1 ({@see \Ferro\Protocol\Generated\Constants::BRANCH_RETRYABLE}) of the three-branch
 * taxonomy: a transient failure whose statement provably did NOT apply (a checkout-time connect
 * failure, a pool timeout, a deadlock/serialization abort, a lost READ). The Task-4 resilience loop
 * MAY transparently re-issue a Retryable READ (`retry_reads`) — it MUST NOT retry a Retryable write
 * unless a manifest declares it idempotent (M3). Carries the decoded `ErrorPayload`.
 */
final class RetryableException extends FerroException
{
    use CarriesErrorPayload;
}
