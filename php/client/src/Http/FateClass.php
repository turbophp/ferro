<?php // /php/client/src/Http/FateClass.php
declare(strict_types=1);
namespace Ferro\Http;

/**
 * The four answers to "what does this HTTP outcome mean for a retry?" (SPEC §23.7.1, §23.7.4).
 *
 *  - {@see NotAFailure}: a response that is not a failure (1xx–3xx, or a 4xx/5xx the caller treats
 *    as data — {@see StatusFate} decides);
 *  - {@see Retryable}: the request provably did not apply, or applying it again is harmless (it was
 *    declared idempotent). A caller's policy MAY retry it;
 *  - {@see NonRetryable}: retrying as-is will not help (or the request was applied and must not be
 *    repeated);
 *  - {@see Indeterminate}: a non-idempotent request whose fate is unknown. Never retried by anything
 *    in Ferro; the caller decides (§9.2).
 *
 * Advice for the caller's policy layer: nothing in `ferro/client` retries an HTTP request.
 */
enum FateClass: string
{
    case NotAFailure = 'not_a_failure';
    case Retryable = 'retryable';
    case NonRetryable = 'non_retryable';
    case Indeterminate = 'indeterminate';

    /**
     * The more cautious of two fates — how §23.7.1's "combined fate" joins a transport fate with a
     * status verdict: Indeterminate wins over everything, then NonRetryable, then Retryable. A
     * truncated body after a 500 on a non-idempotent request is therefore Indeterminate even though
     * the transport fate alone (ResponseIncomplete) is NonRetryable.
     */
    public static function combine(self $a, self $b): self
    {
        foreach ([self::Indeterminate, self::NonRetryable, self::Retryable] as $fate) {
            if ($a === $fate || $b === $fate) {
                return $fate;
            }
        }
        return self::NotAFailure;
    }
}
