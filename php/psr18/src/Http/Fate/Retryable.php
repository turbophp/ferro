<?php // /php/psr18/src/Http/Fate/Retryable.php
declare(strict_types=1);
namespace Ferro\Http\Fate;

/**
 * Fate marker (SPEC §23.11.3): the request provably did not reach the upstream, or it was declared
 * idempotent, so a caller's policy MAY retry it. Nothing in Ferro does.
 *
 * Every Ferro adapter exception implements exactly one of {@see Retryable}, {@see NonRetryable} and
 * {@see Indeterminate}, chosen from the failure's fate, so a decider branches on `instanceof`;
 * {@see Carrier::ferroFate()} carries the detail.
 */
interface Retryable extends Carrier
{
}
