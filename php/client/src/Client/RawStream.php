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
     * @param ?\Closure(): void $release HOW to abandon this stream, injected by
     *   {@see Connection::streamRaw} because the choice is not this class's to make: outside a
     *   transaction abandonment is a `CANCEL`, inside one it MUST be a drain, and only the
     *   Connection knows which ({@see Connection::releaseStream}). Null falls back to the bare
     *   `CANCEL` — the wire-less unit fixtures, and the terminal-at-open case where `$session` is
     *   null and there is nothing to abandon either way.
     */
    public function __construct(
        private readonly array $cols,
        private readonly \Generator $rows,
        private readonly ?StreamingSessionInterface $session,
        private readonly int $requestId,
        private readonly ?\Closure $release = null,
    ) {}

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
     * Abandon whatever is left, reaching the ONE terminal (charter rule 4) either way: a `CANCEL` +
     * drain outside a transaction, a plain drain inside one — see {@see $release} and
     * {@see Connection::releaseStream} for why cancelling inside a transaction destroys it.
     * Idempotent, and a no-op when the stream already finished normally.
     *
     * This is the abandonment site a `foreach ($s->rows() as $r) { break; }` actually reaches:
     * while this handle is alive it holds the pump generator, so breaking runs no `finally`.
     */
    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        if ($this->release !== null) {
            ($this->release)();
            return;
        }
        $this->session?->abandonStream($this->requestId);
    }
}
