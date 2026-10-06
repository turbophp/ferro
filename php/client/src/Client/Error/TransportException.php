<?php // /php/client/src/Client/Error/TransportException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * A raw transport-layer failure: connect refused/timed out, a read that hit EOF or a read/write
 * timeout, a short write. Distinct from a protocol-level fault (a well-formed connection that
 * carried an unexpected frame) — this is the socket itself failing.
 *
 * **{@see requestUnsent} is the one fact about a transport failure that decides a write's fate.**
 * A request whose frame was never completely written was never dispatched: the engine cannot
 * decode a partial frame, and {@see \Ferro\Client\Session} closes the socket on the failure, so
 * the partial frame can never be completed by a later one. That is SPEC §19.3's engine-side
 * "not-yet-dispatched → Retryable" applied client-side, and it is what lets
 * {@see \Ferro\Client\FateClassifier::classifyLoss} report such a loss as `Retryable` instead of
 * `Indeterminate`. Only the session sets it, because only the session knows which frame was being
 * written (M2-C1e-3, SPEC §22.2 (bx)).
 */
final class TransportException extends FerroException
{
    private bool $requestUnsent = false;

    private bool $readTimedOut = false;

    /**
     * A read waited its whole timeout and received nothing more (M3-D1c). The transport KEEPS any
     * bytes it had already read of the frame, so the stream is still in step and the session may
     * keep reading: silence is a liveness question ({@see \Ferro\Client\Session}), not a failure.
     */
    public static function readTimedOut(string $message): self
    {
        $e = new self($message);
        $e->readTimedOut = true;
        return $e;
    }

    /** True for a read that timed out with the stream still in step — see {@see readTimedOut}. */
    public function isReadTimeout(): bool
    {
        return $this->readTimedOut;
    }

    /**
     * The transport failed while a request frame was being WRITTEN — or the session was already
     * poisoned, so nothing was written at all. Wraps the underlying failure.
     */
    public static function requestNotSent(string $message, ?\Throwable $previous = null): self
    {
        $e = new self($message, 0, $previous);
        $e->requestUnsent = true;
        return $e;
    }

    /** True only when no complete request frame reached the engine — see the class docblock. */
    public function requestUnsent(): bool
    {
        return $this->requestUnsent;
    }
}
