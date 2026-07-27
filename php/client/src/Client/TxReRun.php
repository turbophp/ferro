<?php // /php/client/src/Client/TxReRun.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * How {@see Connection::transaction} may re-run a closure whose transaction died (SPEC §19.1) — an
 * internal decision, never a lost-COMMIT (that carve-out is handled separately and is always
 * `Indeterminate`, never a re-run).
 */
enum TxReRun
{
    /** Not retryable — propagate the error (Indeterminate/NonRetryable/application error). */
    case No;

    /** The session died mid-tx: reconnect (epoch-aware) to a fresh session, then re-run the closure. */
    case Reconnect;

    /** The tx aborted retryably (deadlock/serialization) on a still-live session: re-run in place. */
    case SameSession;
}
