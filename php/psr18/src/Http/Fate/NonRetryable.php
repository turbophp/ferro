<?php // /php/psr18/src/Http/Fate/NonRetryable.php
declare(strict_types=1);
namespace Ferro\Http\Fate;

/**
 * Fate marker (SPEC §23.11.3): retrying the request as-is will not help (a policy refusal, a
 * configuration error, a timeout of a declared read), or it was applied and must not be repeated
 * (`ResponseIncomplete` after a 2xx head).
 *
 * See {@see Retryable} for how the three markers are used.
 */
interface NonRetryable extends Carrier
{
}
