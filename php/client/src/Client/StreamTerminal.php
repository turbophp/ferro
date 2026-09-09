<?php // /php/client/src/Client/StreamTerminal.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * The settled state of ONE streamed request's terminal frame (M1-S9, B1a).
 *
 * **Why this exists.** The stream terminal has ALWAYS carried a real `affected` on the wire — the
 * engine reads the command tag post-drain (`RowStream::rows_affected()`, "never a hardcoded 0")
 * and encodes it into the same `ExecOk` body every EXEC terminal uses. What was missing is the
 * CLIENT surfacing it: {@see Connection::pumpRaw} decoded the Ok terminal only to check it for an
 * error, and dropped `affected`/`last_insert_id` on the floor — which is the actual reason the
 * Doctrine tier's prepared path buffers (§22.2 (ac) recorded the gap as a missing WIRE field; that
 * clause was measured FALSE in M1-S9 and amended — the field was there all along).
 *
 * **Why it is MUTABLE, and who writes it.** The value only exists once the generator reaches the
 * terminal, but {@see RawStream} is handed out at OPEN time — so the two must share a cell that
 * the pump writes exactly once and the handle reads. Exactly TWO writers exist, both inside
 * {@see Connection}: the `end`-at-open path (settled before the handle is even returned) and
 * `pumpRaw`'s terminal arm. Nothing else may write it; consumers read through
 * {@see RawStream::affected()} / {@see RawStream::lastInsertId()} / {@see RawStream::settled()}.
 *
 * **`settled` is the truth flag, not a nullable convention.** An Ok terminal ALWAYS carries an
 * `affected` int (0 included), so `affected === null ⇔ unsettled` happens to hold today — but
 * `last_insert_id` is legitimately null on a settled terminal, so a reader gating on a nullable
 * field would conflate "not yet drained" with "no generated key". Gate on `settled`.
 *
 * A stream that is ABANDONED ({@see RawStream::close} before the terminal) or dies on an error
 * terminal never settles: the command tag of a cancelled statement was never read, and inventing
 * a 0 would be exactly the "hardcoded 0" defect the engine side refuses.
 */
final class StreamTerminal
{
    /** Written exactly once, by {@see Connection}'s terminal decode. */
    public bool $settled = false;

    /**
     * The terminal's command-tag affected-row count. For PostgreSQL — the only streaming backend
     * today — a `SELECT` reports the number of rows returned, a DML its affected-row count.
     */
    public ?int $affected = null;

    /**
     * The terminal's generated key, `null` when the backend reported none — which is EVERY
     * streamed statement today (PG has no such protocol field; MySQL streaming is deferred,
     * §22.2 (n)). Carried anyway so a future streaming backend that reports one changes its
     * producer, not this plumbing — the same reasoning as `build_stream_terminal_body`'s
     * parameter on the engine side.
     */
    public int|string|null $lastInsertId = null;
}
