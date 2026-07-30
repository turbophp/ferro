<?php // /php/client/src/Client/FateClassifier.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;

/**
 * The SINGLE chokepoint the reconnect loop consults before re-issuing anything — the client half of
 * the engine's §19.3 "never transparently retry" safety property. It answers two questions:
 *
 *  1. {@see mayRetry} — given `(branch, readonly, opKind, idempotent)`, MAY this op be re-issued?
 *     The never-retry set is absolute (returns false regardless of any RetryPolicy):
 *       - a lost/failed COMMIT (`opKind = TxCommit`)      — an already-committed tx must not re-apply;
 *       - an `Indeterminate` (branch 2)                   — a write whose fate is unknown;
 *       - a `NonRetryable`/garbled branch (branch 3/other) — deterministic, replay just repeats it;
 *       - any WRITE (`readonly=false`) unless `idempotent` — M0 has NO idempotent license (manifests
 *         are M3), so `idempotent` is always false here and a retryable write NEVER retries.
 *     The ONLY yes: a `Retryable` (branch 1) READ (`readonly=true`), and only when `retryReads`.
 *
 *  2. {@see classifyLoss} — turn a no-response / dead-transport failure into its fate exception per
 *     the §19.1 matrix. The lost-COMMIT carve-out lives here and is checked FIRST: a lost COMMIT is
 *     ALWAYS `Indeterminate`, never reclassified down to Retryable even by a (misbehaving) server hint.
 */
final class FateClassifier
{
    public function __construct(private readonly bool $retryReads = true) {}

    public function retryReads(): bool { return $this->retryReads; }

    /**
     * Retry-eligibility decision. See the class docblock for the full never-retry set. Pure and
     * total — every unknown/garbled branch falls through to `false` (the strictly safe fate).
     */
    public function mayRetry(int $branch, bool $readonly, OpKind $opKind, bool $idempotent = false): bool
    {
        // (1) A lost/failed COMMIT is the transactional Indeterminate — NEVER re-issue (§19.3 carve-out).
        if ($opKind === OpKind::TxCommit) {
            return false;
        }
        // (2) A write whose fate is unknown — NEVER re-issue.
        if ($branch === C::BRANCH_INDETERMINATE) {
            return false;
        }
        // (3) Only a Retryable (branch 1) failure is ever eligible; NonRetryable/garbled ⇒ false.
        if ($branch !== C::BRANCH_RETRYABLE) {
            return false;
        }
        // (4) A retryable WRITE needs an idempotent manifest license (M3) — false in M0.
        if (!$readonly) {
            return $idempotent;
        }
        // (5) A retryable READ: eligible iff reads are configured retryable.
        return $this->retryReads;
    }

    /**
     * Route a decoded/mapped exception through {@see mayRetry}, reading the wire branch off the
     * taxonomy exceptions and treating everything else (Cancelled/Protocol/Handshake/Transport/…) as
     * non-retryable. The reconnect + read-retry paths call THIS so the branch is never guessed.
     */
    public function mayRetryException(
        FerroException $ex,
        bool $readonly,
        OpKind $opKind,
        bool $idempotent = false,
    ): bool {
        $branch = match (true) {
            $ex instanceof RetryableException     => C::BRANCH_RETRYABLE,
            $ex instanceof IndeterminateException => C::BRANCH_INDETERMINATE,
            $ex instanceof NonRetryableException  => $ex->branch(),
            // Cancelled / Protocol / Handshake / Transport / ConnectionLost carry no taxonomy branch —
            // none of them is ever transparently re-issued.
            default => -1,
        };
        return $this->mayRetry($branch, $readonly, $opKind, $idempotent);
    }

    /**
     * Classify a no-response / dead-transport loss into its fate exception (§19.1 matrix). The
     * lost-COMMIT carve-out is checked FIRST and is unconditional. When the engine actually sent a
     * session-fatal `Outcome::Error` (`$server`), its branch is trusted for the non-COMMIT cases (the
     * engine already applied the dispatched-vs-not classification).
     *
     * `$epochChanged` is the client-side {@see IndeterminateException::cause} inference for the TWO
     * no-response branches below (lost COMMIT / lost autocommit write): pass whether the
     * {@see \Ferro\Client\ReconnectLoop} has (so far) observed a CHANGED `boot_epoch` — `true` labels
     * the built exception {@see IndeterminateException::CAUSE_ENGINE_RESTART}, `false` (the default —
     * no reconnect has happened yet, or none observed a change) labels it
     * {@see IndeterminateException::CAUSE_LINK_LOST}. This is honest best-effort inference, NOT a wire
     * signal — the wire carries no `cause` at all (§9.2/§19.3; M1-S4 verification MAJOR).
     */
    public function classifyLoss(
        OpKind $opKind,
        bool $readonly,
        string $reason,
        ?ErrorPayload $server = null,
        bool $epochChanged = false,
    ): FerroException {
        $noResponseCause = $epochChanged
            ? IndeterminateException::CAUSE_ENGINE_RESTART
            : IndeterminateException::CAUSE_LINK_LOST;

        // §19.3 carve-out: a COMMIT with no confirmed response is the ONE transactional Indeterminate.
        if ($opKind === OpKind::TxCommit) {
            return new IndeterminateException(self::payload(
                C::ERR_WRITE_UNCONFIRMED,
                C::BRANCH_INDETERMINATE,
                'COMMIT sent with no confirmed response — the transaction may or may not have applied '
                    . '(§19.3 Indeterminate; the client never re-runs it): ' . $reason,
            ), $noResponseCause);
        }

        // The engine spoke a definite fate before the link died — trust it (branch already classified).
        // This IS an engine-reported fate (just relayed via a session-fatal terminal rather than a
        // matched-id reply), so an Indeterminate here is CAUSE_ENGINE_REPORTED, never the no-response
        // inference above.
        if ($server !== null) {
            return match ($server->branch) {
                C::BRANCH_RETRYABLE     => new RetryableException($server),
                C::BRANCH_INDETERMINATE => new IndeterminateException($server, IndeterminateException::CAUSE_ENGINE_REPORTED),
                default                 => new NonRetryableException($server),
            };
        }

        // A dispatched autocommit write whose response never arrived is Indeterminate.
        if ($opKind === OpKind::Write) {
            return new IndeterminateException(self::payload(
                C::ERR_WRITE_UNCONFIRMED,
                C::BRANCH_INDETERMINATE,
                'autocommit write lost mid-flight — fate unknown (§19.3 Indeterminate): ' . $reason,
            ), $noResponseCause);
        }

        // Reads, BEGIN, mid-tx statements, ROLLBACK, savepoints: provably did not commit → Retryable.
        return new RetryableException(self::payload(
            C::ERR_CONNECTION_LOST,
            C::BRANCH_RETRYABLE,
            'connection lost with no write-fate to be unsure about (Retryable): ' . $reason,
        ));
    }

    private static function payload(int $code, int $branch, string $message): ErrorPayload
    {
        return new ErrorPayload($code, $branch, null, null, $message, null, null);
    }
}
