<?php // /php/psr18/src/Http/Fate/Indeterminate.php
declare(strict_types=1);
namespace Ferro\Http\Fate;

/**
 * Fate marker (SPEC §9.2, §23.7.1, §23.11.3): a non-idempotent request that was sent and whose fate
 * is unknown — the upstream may or may not have applied it. Never retried by anything in Ferro,
 * including {@see \Ferro\Guzzle\Retry} and `Ferro\Laravel\Http\Retry`; only the caller's own licence
 * (an idempotency key the upstream honours, a read-back) may justify re-sending it.
 *
 * See {@see Retryable} for how the three markers are used.
 */
interface Indeterminate extends Carrier
{
}
