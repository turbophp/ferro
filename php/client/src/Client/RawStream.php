<?php // /php/client/src/Client/RawStream.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ProtocolException;

/**
 * An OPEN streamed read: the column names, a lazy Generator of POSITIONAL rows, and an explicit
 * {@see close}. Produced by {@see Connection::streamRaw}, consumed by the Doctrine tier's `Result`.
 *
 * **Why the columns are eager and the rows are lazy.** `Doctrine\DBAL\Result::columnCount()` and
 * `getColumnName()` are callable before any fetch, and `Doctrine\DBAL\Statement::execute()` is
 * expected to have RUN the statement by the time it returns — so the `HEAD` frame must already be
 * read. The DATA frames must not be: `Doctrine\DBAL\Result::iterateAssociative()` is literally
 * `while (($row = $this->fetchAssociative()) !== false) yield $row;`, so "never buffer" reduces
 * entirely to pulling one row at a time here.
 *
 * **Why {@see close} exists at all.** {@see Connection::stream} needs no such method: it opens the
 * stream INSIDE its generator, so a generator that is never started never opened anything, and a
 * generator that IS started runs its `finally` (a `CANCEL` + drain) when it is destroyed. This
 * handle opens EAGERLY, so a caller that builds one and drops it without iterating would leave the
 * engine-side stream open and the very next request would read its frames as its own reply — a wire
 * desync. `close()` closes that hole. It is safe to call unconditionally and repeatedly:
 * {@see Session::abandonStream} is idempotent by construction (it returns immediately when no
 * stream with that id is open), so calling it after a normal drain is a no-op.
 */
final class RawStream
{
    private bool $closed = false;

    /**
     * @param list<string> $cols the column names from the `HEAD` frame, in order.
     * @param \Generator<int, list<mixed>> $rows one POSITIONAL row per iteration.
     * @param ?StreamingSessionInterface $session null when the stream reached its terminal during
     *   the open (a known fate decided before any HEAD/DATA went out) — nothing to abandon, and
     *   {@see close} must NOT invent a wire operation for a request id that was never a stream.
     * @param StreamTerminal $terminal the shared settled-state cell {@see Connection}'s pump
     *   writes when the generator reaches the Ok terminal (M1-S9 B1a). Shared BY CONSTRUCTION:
     *   the pump exists before this handle does, so the cell is how the two meet.
     */
    public function __construct(
        private readonly array $cols,
        private readonly \Generator $rows,
        private readonly ?StreamingSessionInterface $session,
        private readonly int $requestId,
        private readonly StreamTerminal $terminal,
    ) {}

    /**
     * Whether the stream reached its Ok terminal — the gate for {@see affected} and
     * {@see lastInsertId}. `false` mid-iteration, after {@see close}-before-drain (an abandoned
     * statement's command tag was never read), and after an error terminal (the taxonomy
     * exception is the answer there). See {@see StreamTerminal} for why the flag is explicit
     * rather than a nullable-field convention.
     */
    public function settled(): bool
    {
        return $this->terminal->settled;
    }

    /**
     * The terminal's command-tag affected-row count, or `null` until {@see settled}. On the only
     * streaming backend today (PostgreSQL) a drained `SELECT` reports the number of rows it
     * returned and a DML its affected-row count — the value `Doctrine\DBAL\Result::rowCount()`
     * needs, and the reason the prepared path can stop buffering (M1-S9 B1a; §22.2 (ac) as
     * amended: the wire ALWAYS carried this, the client dropped it).
     */
    public function affected(): ?int
    {
        return $this->terminal->settled ? $this->terminal->affected : null;
    }

    /**
     * The terminal's generated key, `null` until {@see settled} — and null AFTER it on every
     * streamed statement today (PG reports none; MySQL streaming is deferred, §22.2 (n)). Gate on
     * {@see settled} to tell the two nulls apart.
     */
    public function lastInsertId(): int|string|null
    {
        return $this->terminal->settled ? $this->terminal->lastInsertId : null;
    }

    /** @return list<string> */
    public function columns(): array
    {
        return $this->cols;
    }

    /**
     * The row generator. Iterating it consumes DATA frames and replenishes the credit window; a
     * mid-stream error terminal throws the mapped taxonomy exception AFTER the rows that already
     * arrived.
     *
     * Refused after {@see close}: the engine has been told to cancel this request, so pulling from
     * the generator would either read nothing or — worse — read frames belonging to a LATER request
     * as if they were this one's.
     *
     * @return \Generator<int, list<mixed>>
     */
    public function rows(): \Generator
    {
        if ($this->closed) {
            throw new ProtocolException('RawStream::rows() after close()');
        }
        return $this->rows;
    }

    /** Whether {@see close} has been called. */
    public function isClosed(): bool
    {
        return $this->closed;
    }

    /**
     * Abandon whatever is left: `CANCEL` + drain to the ONE terminal (charter rule 4). Idempotent,
     * and a no-op when the stream already finished normally.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->session?->abandonStream($this->requestId);
    }
}
