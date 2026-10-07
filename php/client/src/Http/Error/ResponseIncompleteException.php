<?php // /php/client/src/Http/Error/ResponseIncompleteException.php
declare(strict_types=1);
namespace Ferro\Http\Error;

use Ferro\Client\Error\NonRetryableException;
use Ferro\Http\FateClass;
use Ferro\Http\ResponseHead;
use Ferro\Protocol\ErrorPayload;

/**
 * `ResponseIncomplete` (`0x300E`, SPEC §23.7.1): the upstream ANSWERED — a head arrived — and then
 * the body failed (`body_reset`, `body_eof`, `body_framing`, `decode`, `max_response_bytes`, the
 * drain cap), or the link to the engine died mid-body on a non-idempotent request (§23.7.3,
 * client-synthesised, cause {@see HttpException::CLIENT_LINK_LOST}).
 *
 * It does NOT mean "nothing happened": after a 2xx head the request was applied, which
 * {@see wasApplied} says. {@see fate} is the COMBINED fate of §23.7.1 — the transport's NonRetryable
 * joined with the head's {@see \Ferro\Http\StatusFate} — so a truncated body after a 500 on a
 * non-idempotent request is Indeterminate, agreeing with §23.7.4.
 */
final class ResponseIncompleteException extends NonRetryableException implements HttpException
{
    public function __construct(
        ErrorPayload $errorPayload,
        private readonly string $httpCause,
        private readonly ResponseHead $head,
        private readonly bool $clientSynthesised = false,
    ) {
        parent::__construct($errorPayload);
    }

    public function cause(): string { return $this->httpCause; }

    public function clientSynthesised(): bool { return $this->clientSynthesised; }

    /** The status of the head that arrived before the body failed. */
    public function status(): int { return $this->head->status; }

    /** @return array<string, list<string>> the head's headers, lowercase names. */
    public function headers(): array { return $this->head->headers; }

    /** The whole head the client holds. */
    public function head(): ResponseHead { return $this->head; }

    /** `true` after a 2xx head (the request was applied); `null` otherwise — never `false`. */
    public function wasApplied(): ?bool
    {
        return $this->head->status >= 200 && $this->head->status < 300 ? true : null;
    }

    public function fate(): FateClass
    {
        return FateClass::combine(FateClass::NonRetryable, $this->head->statusFate()->fate);
    }
}
