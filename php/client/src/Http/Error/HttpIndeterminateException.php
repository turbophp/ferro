<?php // /php/client/src/Http/Error/HttpIndeterminateException.php
declare(strict_types=1);
namespace Ferro\Http\Error;

use Ferro\Client\Error\IndeterminateException;
use Ferro\Http\FateClass;
use Ferro\Protocol\ErrorPayload;

/**
 * §9.2's `WriteUnconfirmed` on Ferro HTTP: a NON-idempotent request that was sent, with no response
 * head (SPEC §23.7.1) — the upstream may or may not have applied it. Never re-sent by anything in
 * Ferro (charter rule 3, §23.7.3); the caller decides.
 *
 * **`cause()` is the `[http.causes]` token here**, not the inherited client inference: on HTTP the
 * wire carries why (`eof_empty`, `reset`, `timeout`, `cancelled`, …, §23.5.6). When the client
 * synthesised the fate because the link to the engine died, it is {@see HttpException::CLIENT_LINK_LOST}
 * — never `engine_restart`, because the HTTP path re-issues nothing and so never reconnects to learn
 * whether the epoch changed. {@see inferredCause} keeps {@see IndeterminateException}'s own label.
 */
final class HttpIndeterminateException extends IndeterminateException implements HttpException
{
    public function __construct(
        ErrorPayload $errorPayload,
        private readonly string $httpCause,
        private readonly bool $clientSynthesised = false,
    ) {
        parent::__construct(
            $errorPayload,
            $clientSynthesised ? IndeterminateException::CAUSE_LINK_LOST : IndeterminateException::CAUSE_ENGINE_REPORTED,
        );
    }

    public function cause(): string { return $this->httpCause; }

    /** {@see IndeterminateException::cause}'s label: `engine_reported`, or `link_lost` when client-synthesised. */
    public function inferredCause(): string { return parent::cause(); }

    public function clientSynthesised(): bool { return $this->clientSynthesised; }

    public function fate(): FateClass { return FateClass::Indeterminate; }
}
