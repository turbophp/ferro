<?php // /php/client/src/Http/Error/HttpCancelledException.php
declare(strict_types=1);
namespace Ferro\Http\Error;

use Ferro\Client\Error\CancelledException;
use Ferro\Http\FateClass;
use Ferro\Http\ResponseHead;
use Ferro\Protocol\Generated\Constants as C;

/**
 * The engine answered a `CANCEL` with `Outcome::Cancelled` (SPEC §23.7.1): the request was not sent,
 * was declared idempotent, or had its head already. A non-idempotent request that was sent with no
 * head is never `Cancelled` — the engine answers it `WriteUnconfirmed`
 * ({@see HttpIndeterminateException}, cause `cancelled`).
 *
 * The native API sends a `CANCEL` only for an abandoned exchange (whose terminal it discards) and for
 * a request whose client deadline passed with no answer from the engine (§23.11.0) — so this is what
 * that backstop surfaces as, when the engine's answer to it is `Cancelled`.
 */
final class HttpCancelledException extends CancelledException implements HttpException
{
    public function __construct(private readonly ?ResponseHead $head = null)
    {
        parent::__construct('HTTP request was cancelled by the engine (Outcome::Cancelled)');
    }

    /** The head that arrived before the cancel, if one did. */
    public function head(): ?ResponseHead { return $this->head; }

    /** `true` after a 2xx head (the request was applied), `null` otherwise — never `false`. */
    public function wasApplied(): ?bool
    {
        return $this->head !== null && $this->head->status >= 200 && $this->head->status < 300 ? true : null;
    }

    public function cause(): string { return C::HTTP_CAUSE_CANCELLED; }

    public function clientSynthesised(): bool { return false; }

    public function fate(): FateClass { return FateClass::NonRetryable; }
}
