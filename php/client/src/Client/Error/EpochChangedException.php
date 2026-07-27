<?php // /php/client/src/Client/Error/EpochChangedException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * Surfaced when a reconnect observed a CHANGED `boot_epoch` (SPEC §19.1): the daemon restarted, so
 * ALL engine-side state — any open `tx_id`, streams, prepared handles — is void. A read is
 * transparently re-issued on the new epoch, but an in-flight/open transaction is dead: its `tx_id`
 * no longer exists on the restarted engine. The closure-`transaction` API re-runs the whole closure
 * on the new epoch under the caller's {@see \Ferro\Client\RetryPolicy}; when that budget is
 * exhausted this is what propagates, so the caller knows the tx must be restarted (never resumed).
 *
 * `epochChanged()` is always true here — the type IS the epoch-change signal — but the accessor is
 * kept explicit so call sites read intent, not the class name.
 */
final class EpochChangedException extends FerroException
{
    public function __construct(string $message, private readonly bool $epochChanged = true, ?\Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }

    public function epochChanged(): bool { return $this->epochChanged; }
}
