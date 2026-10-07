<?php // /php/client/src/Http/Error/HttpException.php
declare(strict_types=1);
namespace Ferro\Http\Error;

use Ferro\Http\FateClass;

/**
 * Every failed Ferro HTTP exchange (SPEC §23.11.1). Each implementation also extends the
 * `ferro/client` taxonomy base of its fate — {@see \Ferro\Client\Error\RetryableException},
 * {@see \Ferro\Client\Error\NonRetryableException}, {@see \Ferro\Client\Error\IndeterminateException}
 * or {@see \Ferro\Client\Error\CancelledException} — so code that catches by fate needs no HTTP case.
 *
 * Nothing in `ferro/client` re-sends an HTTP request, whatever its fate (§23.7.3). A `Retryable`
 * licenses the caller's own policy, never this client.
 */
interface HttpException extends \Throwable
{
    /**
     * Client-synthesised cause: the UDS link to the engine died (or the request never completely
     * left the client). Not an `[http.causes]` token — the engine never classified the request.
     */
    public const CLIENT_LINK_LOST = 'link_lost';

    /**
     * Why. When the ENGINE classified the exchange this is exactly its `[http.causes]` token
     * (`ErrorPayload.detail`, §23.5.6) — one of `Constants::HTTP_CAUSES`. When the CLIENT did,
     * because the link to the engine died (§23.7.3), it is {@see CLIENT_LINK_LOST}, which is not a
     * registry token; {@see clientSynthesised} says which.
     */
    public function cause(): string;

    /** Whether the client classified this failure itself (the link died, §23.7.3), not the engine. */
    public function clientSynthesised(): bool;

    /**
     * The fate a retry policy should read. For every class but {@see ResponseIncompleteException}
     * it is the class's own branch; there it is §23.7.1's COMBINED fate of the transport (applied
     * and answered, so NonRetryable) and the head's status ({@see \Ferro\Http\StatusFate}).
     */
    public function fate(): FateClass;
}
