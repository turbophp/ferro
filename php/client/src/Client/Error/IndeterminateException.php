<?php // /php/client/src/Client/Error/IndeterminateException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

use Ferro\Protocol\ErrorPayload;

/**
 * Branch 2 ({@see \Ferro\Protocol\Generated\Constants::BRANCH_INDETERMINATE}) — the spec's defining
 * safety property (SPEC §9.2 / §19.3): a write that was TRANSMITTED but whose fate is UNKNOWN
 * (`WriteUnconfirmed`; a connection lost mid-flight on a non-readonly statement). The engine NEVER
 * transparently retries it and NEITHER DOES THE CLIENT — this exception propagates to the caller,
 * whose policy decides. Auto-retry is licensed ONLY by a manifest `idempotent: true` (M3), never by
 * a default. Carries the decoded `ErrorPayload`.
 *
 * **`cause()` is a CLIENT-SIDE INFERENCE, NEVER a wire field** (M1-S4 verification MAJOR): the wire
 * only ever carries `code=WRITE_UNCONFIRMED, branch=Indeterminate` (`/proto/errors.toml`) — it cannot
 * say WHY the write's fate is unknown. `cause()` is scoped to exactly the three things the client can
 * honestly tell apart from WHERE the exception was built:
 *
 *   - {@see CAUSE_LINK_LOST} — a transport-death write with NO response
 *     ({@see \Ferro\Client\FateClassifier::classifyLoss} with `$server === null`), and no reconnect has
 *     (yet) observed a changed `boot_epoch`.
 *   - {@see CAUSE_ENGINE_RESTART} — the SAME no-response path, but the {@see \Ferro\Client\ReconnectLoop}
 *     has already resolved a CHANGED `boot_epoch` — the daemon is known to have restarted.
 *   - {@see CAUSE_ENGINE_REPORTED} — the branch was decoded from an actual engine reply
 *     (`Outcome::Error{code: WRITE_UNCONFIRMED}` via {@see ErrorMapper}, or a trusted `$server` hint
 *     inside `classifyLoss`). This is the S4 timeout/cancel-Indeterminate case (a cancelled/timed-out
 *     autocommit write) — the wire CANNOT distinguish it from any OTHER engine-reported
 *     `WriteUnconfirmed`, so the label stays this honest generic, NEVER narrowed to `"timeout"`.
 *
 * Do not add a `"timeout"` cause, or any other label, that the wire cannot actually carry.
 */
final class IndeterminateException extends FerroException
{
    use CarriesErrorPayload {
        __construct as private fromErrorPayload;
    }

    public const CAUSE_LINK_LOST = 'link_lost';
    public const CAUSE_ENGINE_RESTART = 'engine_restart';
    public const CAUSE_ENGINE_REPORTED = 'engine_reported';

    /**
     * @param string $cause one of {@see CAUSE_LINK_LOST}|{@see CAUSE_ENGINE_RESTART}|
     *                      {@see CAUSE_ENGINE_REPORTED}. Defaults to the common case — an
     *                      engine-decoded reply — since every construction site EXCEPT the two
     *                      no-response inference branches in `classifyLoss` is exactly that.
     */
    public function __construct(ErrorPayload $errorPayload, private readonly string $cause = self::CAUSE_ENGINE_REPORTED)
    {
        $this->fromErrorPayload($errorPayload);
    }

    /** The client-side inferred cause. See the class docblock for what each label honestly means. */
    public function cause(): string
    {
        return $this->cause;
    }
}
