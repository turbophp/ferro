<?php // /php/client/src/Client/ReconnectLoop.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\HandshakeException;
use Ferro\Client\Error\TransportException;

/**
 * The epoch-aware reconnect mechanism (SPEC §19.1). On a `ConnectionLost`/transport failure the
 * runtime calls {@see reconnect}: it re-opens a fresh, handshaken session via the injected factory,
 * retrying with {@see Backoff} full-jitter waits up to `maxAttempts`, then compares the NEW
 * `boot_epoch` against the cached one and hands back whether it CHANGED.
 *
 * **Opaque epoch compare (§19.1/§19.3-CRITICAL).** The epoch is compared as an OPAQUE scalar with
 * strict `===` — NEVER `(int)`-coerced. A `u64 > PHP_INT_MAX` arrives as a decimal STRING; two
 * distinct such epochs would BOTH coerce to `PHP_INT_MAX` and compare equal, so a real restart would
 * look like "same epoch", the dead `tx_id` would never be voided, and the client could talk to a
 * fresh engine as if its transaction still lived. Strict `===` on the raw scalar makes a changed
 * epoch always detected.
 *
 * A changed epoch means all engine-side state is gone; the caller (read path re-issues; the tx
 * closure re-runs). A {@see HandshakeException} during reconnect (registry/version mismatch) is FATAL
 * and re-thrown immediately — reconnecting into the same rejection is pointless.
 */
final class ReconnectLoop
{
    /** @var \Closure(): SessionInterface builds a freshly connected + handshaken session */
    private readonly \Closure $factory;

    private SessionInterface $session;

    /** OPAQUE — int or decimal string; compared with === only. */
    private int|string $cachedEpoch;

    private bool $lastEpochChanged = false;
    private int $reconnectCount = 0;

    /**
     * @param SessionInterface           $initial a session whose handshake has already completed.
     * @param \Closure(): SessionInterface $factory returns a fresh connected + handshaken session.
     */
    public function __construct(
        SessionInterface $initial,
        \Closure $factory,
        private readonly Backoff $backoff,
        private readonly int $maxAttempts = 3,
    ) {
        if ($maxAttempts < 1) {
            throw new \InvalidArgumentException("maxAttempts must be >= 1, got {$maxAttempts}");
        }
        $this->session = $initial;
        $this->factory = $factory;
        $this->cachedEpoch = $initial->bootEpoch();
    }

    /** The live session (swapped in place by {@see reconnect}). */
    public function session(): SessionInterface { return $this->session; }

    /** The currently cached opaque `boot_epoch` (`int|string`). */
    public function currentEpoch(): int|string { return $this->cachedEpoch; }

    /** Whether the most recent {@see reconnect} observed a changed epoch. */
    public function lastEpochChanged(): bool { return $this->lastEpochChanged; }

    /** How many successful reconnects have happened over this loop's life. */
    public function reconnectCount(): int { return $this->reconnectCount; }

    /**
     * Re-open the session with bounded full-jitter backoff, swap it in, and return whether the new
     * `boot_epoch` DIFFERS from the cached one (`true` ⇒ engine restarted, all engine state void).
     * Throws the last transport failure if every attempt fails, or a {@see HandshakeException}
     * immediately (fatal, never retried).
     *
     * @throws FerroException when reconnection cannot be established.
     */
    public function reconnect(): bool
    {
        // The dead session's transport may already be gone; closing is best-effort.
        try {
            $this->session->close();
        } catch (\Throwable) {
            // ignore — we are replacing it anyway.
        }

        $lastError = null;
        for ($attempt = 0; $attempt < $this->maxAttempts; $attempt++) {
            $this->backoff->sleepFor($attempt);
            try {
                $fresh = ($this->factory)();
                $newEpoch = $fresh->bootEpoch();
                // OPAQUE compare — strict === on the raw int|string, NO (int) coercion (§19.1).
                $changed = $newEpoch !== $this->cachedEpoch;
                $this->cachedEpoch = $newEpoch;
                $this->session = $fresh;
                $this->lastEpochChanged = $changed;
                ++$this->reconnectCount;
                return $changed;
            } catch (HandshakeException $e) {
                // Registry/version mismatch is fatal — the client must regen/redeploy, not spin.
                throw $e;
            } catch (TransportException | ConnectionLostException $e) {
                $lastError = $e;
            }
        }

        throw $lastError ?? new ConnectionLostException(
            sprintf('reconnect exhausted after %d attempt(s)', $this->maxAttempts),
        );
    }
}
