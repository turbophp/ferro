<?php // /php/doctrine-dbal/src/Result.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\FetchUtils;
use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Exception\InvalidColumnIndex;
use Ferro\Client\Error\FerroException;
use Ferro\Client\RawStream;
use Ferro\DBAL\Exception\DriverException;

/**
 * A DBAL driver result over Ferro's `{cols, rows, affected}`.
 *
 * The nine SPI methods plus `getColumnName()`. Only `fetchNumeric()` and `fetchAssociative()` are
 * real cursors; the other four delegate to `Doctrine\DBAL\Driver\FetchUtils`, exactly as all eight
 * bundled driver results do, so the family stays consistent by construction and Task 12's streamed
 * mode inherits the whole family from ONE incremental `fetchNumeric()`.
 *
 * `rowCount()` is the TERMINAL's `affected`, never `count($this->rows)` — they are different
 * numbers, and conflating them reports 0 for an `UPDATE` that changed rows (the exact bug the
 * research spike shipped).
 *
 * **Two modes, one cursor.** `buffered()` holds the rows the terminal delivered; `streamed()` holds
 * an OPEN `RawStream` and pulls one row per `fetchNumeric()`, which is the whole of §14's
 * "`iterateAssociative()` never buffers" (`Doctrine\DBAL\Result::iterateAssociative()` is literally
 * `while (($row = $this->fetchAssociative()) !== false) yield $row;`, so there is no other hook).
 *
 * `getColumnName()` is declared on `Doctrine\DBAL\Driver\Result` only as a docblock `@method`, which
 * makes it look optional. It is not: `Doctrine\DBAL\Result::getColumnName()` (dbal 4.4.4,
 * `src/Result.php:267-281`) throws a `LogicException` through `method_exists` when it is missing,
 * `Connection::executeCacheQuery()` loops it to build the cache key, and
 * `Driver\Middleware\AbstractResultMiddleware` forwards it behind the same guard — so omitting it
 * would silently disable DBAL's result cache and break any middleware wrapping our result.
 *
 */
final class Result implements ResultInterface
{
    private ?RawStream $stream = null;

    /** @var ?\Generator<int, list<mixed>> */
    private ?\Generator $gen = null;

    /**
     * Whether {@see fetchNumeric} owes the generator a `next()`.
     *
     * The streamed cursor advances LAZILY: it parks the generator on the row it has just returned
     * and performs the pending `next()` at the START of the following pull. That is not a style
     * choice — `Ferro\Client\Connection::pumpRaw()` yields every row of a DATA frame and only then
     * reads the next frame, so a mid-stream error terminal throws AFTER the rows that already
     * arrived (the contract `RawStream::rows()` documents). Advancing eagerly would run that read
     * inside the very call that is already holding a row, and the exception would REPLACE the return
     * value — silently losing the last row before any mid-stream error. It also keeps the producer
     * exactly level with the consumer instead of permanently one row ahead.
     */
    private bool $pendingAdvance = false;

    /**
     * @param list<string> $cols
     * @param list<list<mixed>> $rows
     */
    private function __construct(
        private array $cols,
        private array $rows,
        private readonly int $affected,
        private int $cursor = 0,
    ) {}

    /**
     * @param list<string> $cols
     * @param list<list<mixed>> $rows
     */
    public static function buffered(array $cols, array $rows, int $affected): self
    {
        return new self($cols, $rows, $affected);
    }

    /**
     * A LAZY result over an OPEN streamed read. `fetchNumeric()` pulls exactly one row per call,
     * which is what makes `Doctrine\DBAL\Result::iterateAssociative()` — literally
     * `while (($row = $this->fetchAssociative()) !== false) yield $row;` — never buffer.
     *
     * `affected` is `0`: the HEAD/DATA/END producer carries no such field, which is exactly why the
     * PREPARED path does not stream ({@see \Ferro\DBAL\Connection::query}).
     */
    public static function streamed(RawStream $stream): self
    {
        $r = new self($stream->columns(), [], 0);
        $r->stream = $stream;
        $r->gen = $stream->rows();
        return $r;
    }

    /** Whether this result is still pulling from an open stream. */
    public function isStreaming(): bool
    {
        return $this->stream !== null;
    }

    /**
     * The ONE cursor. Everything else in the family is built on it (directly, or through
     * `FetchUtils`), so an exhausted result keeps answering `false` from every method for free —
     * and a streamed result gets the whole `fetch*` family incremental from this one method.
     *
     * @return list<mixed>|false
     */
    public function fetchNumeric(): array|false
    {
        $gen = $this->gen;
        if ($gen !== null) {
            if ($this->pendingAdvance) {
                $this->pendingAdvance = false;
                // This is where a mid-stream error terminal surfaces, i.e. after every row that
                // arrived before it ({@see $pendingAdvance}) — and where it has to change clothes.
                $this->advance($gen);
            }
            if (!$this->hasRow($gen)) {
                // The producer reached its own terminal, so there is nothing left to cancel and
                // this is no longer a stream. `free()` stays correct either way (it is idempotent
                // and so is `Session::abandonStream`), but keeping the handle here would make an
                // ENDED stream indistinguishable from an open one to `isStreaming()` and to
                // `Connection::settleOpenStream()`.
                $this->gen = null;
                $this->stream = null;
                return false;
            }
            $row = $gen->current();
            $this->pendingAdvance = true;
            return $row;
        }
        return $this->rows[$this->cursor++] ?? false;
    }

    /**
     * Drain whatever is left of the stream into memory and become an ordinary buffered result.
     * Returns the number of rows that cost — 0 when there was nothing to drain, which is also what
     * every call after the first returns.
     *
     * The escape hatch for the canonical Doctrine batch idiom
     * `foreach ($conn->iterateAssociative($sql) as $row) { $conn->executeStatement(…); }`. The Ferro
     * session is strictly SINGLE-IN-FLIGHT — `Session::assertNoOpenStream()` throws on any request
     * while a stream is open — so without this, that idiom would raise a `ProtocolException` that
     * every user would read as a driver bug. With it, pure iteration never buffers and interleaving
     * degrades to buffering, which is what PDO does unconditionally.
     *
     * **The drain is an explicit `valid()/current()/next()` loop and must stay one.** `foreach` over
     * a `Generator` calls `Generator::rewind()`, which THROWS `Cannot rewind a generator that was
     * already run` as soon as the generator has advanced past its first yield — and
     * {@see fetchNumeric} advances it on every call. A `foreach` here would therefore die on the
     * FIRST real use, from the first line of `exec()`/`runPrepared()`/`beginTransaction()`, i.e.
     * attributed to an innocent statement.
     *
     * Idempotent. Already-fetched rows stay consumed, and `$this->rows` is invariantly `[]` for a
     * streamed result — {@see streamed} builds it that way and the streamed branch of
     * {@see fetchNumeric} never appends — so there is no cursor arithmetic to do here.
     */
    public function materialize(): int
    {
        $gen = $this->gen;
        if ($gen === null) {
            return 0;
        }
        if ($this->pendingAdvance) {
            $this->pendingAdvance = false;
            $this->advance($gen);
        }
        $rest = [];
        while ($this->hasRow($gen)) {
            $rest[] = $gen->current();
            $this->advance($gen);
        }
        $this->rows = $rest;
        $this->cursor = 0;
        $this->gen = null;
        $this->stream = null;
        return count($rest);
    }

    /**
     * Advance the streamed generator, translating a mid-stream failure into something DBAL can
     * convert.
     *
     * A streamed statement's ERROR does not arrive at `execute()`/`query()` time — the open only
     * reads the `HEAD` — it arrives from the pump, on whichever fetch reaches the terminal. It
     * arrives as a `Ferro\Client\Error\*`, and hazard 9 is unforgiving there:
     * `Doctrine\DBAL\Result::fetchAssociative()` (and every sibling) catches exactly
     * `Doctrine\DBAL\Driver\Exception` before calling `Connection::convertException()`, so an
     * unwrapped client exception would sail past DBAL's conversion, past
     * {@see \Ferro\DBAL\ExceptionConverter}, and reach the application as a class it has never heard
     * of — losing the §9.2 branch, the `IndeterminateWriteException` mapping and every
     * `catch (Doctrine\DBAL\Exception)` an app or framework has. The buffered path wraps at
     * `Connection::runPrepared()`; this and {@see hasRow} are the streamed path's equivalent, and
     * between them they cover every pull that can reach the wire.
     *
     * @param \Generator<int, list<mixed>> $gen
     */
    private function advance(\Generator $gen): void
    {
        try {
            $gen->next();
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    /**
     * Is there a row parked on the generator? — with the SAME wrapping as {@see advance}, and it is
     * not symmetry for its own sake.
     *
     * `Generator::valid()` is what STARTS a generator, and starting `Connection::pumpRaw()` runs it
     * through `readStreamFrame()` up to the first `yield`. A statement that produces NO rows before
     * it fails — a server-side cancel, a `statement_timeout`, a constraint hit on the first row —
     * therefore throws out of `valid()` and never reaches `next()`.
     *
     * FOUND LIVE while running this task against Task 11's suite, not reasoned about in advance:
     * with only `next()` wrapped, `SELECT pg_cancel_backend(pg_backend_pid())` delivered a correct
     * §19.3 `IndeterminateException` that was UNCONVERTIBLE — it escaped
     * `Doctrine\DBAL\Result`'s `catch (Driver\Exception)` raw, so the application's
     * `catch (Doctrine\DBAL\Exception)` (and every framework's) missed it entirely.
     *
     * @param \Generator<int, list<mixed>> $gen
     */
    private function hasRow(\Generator $gen): bool
    {
        try {
            return $gen->valid();
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    /**
     * **The abandonment path.** When the consumer stops iterating early, the DBAL-side Generator is
     * destroyed, `Doctrine\DBAL\Result` (which has no `__destruct`) is released with it, and this
     * object becomes unreferenced — because {@see \Ferro\DBAL\Connection} holds only a
     * `\WeakReference` to it. Nothing in DBAL calls `free()` on that path (hazard 80), so this is
     * where the `CANCEL` comes from. Without it, `break`-ing out of a large `iterateAssociative()`
     * would leave the stream open and the NEXT statement would transfer the entire remaining result
     * set — invisible at 100 000 rows in a test, an OOM on a real table.
     *
     * `free()` is idempotent and `Session::abandonStream()` is idempotent by construction
     * (`Session.php:344-353`), so this is safe after a normal drain. The `catch` is not defensive
     * padding: at request shutdown the transport may already be gone, and an exception escaping a
     * destructor during shutdown is a fatal error that would mask whatever actually went wrong.
     */
    public function __destruct()
    {
        try {
            $this->free();
        } catch (\Throwable) {
            // nothing useful can be done from a destructor; the session's own state machine and the
            // engine's ONE-terminal rule (charter rule 4) are what guarantee the stream is closed.
        }
    }

    /**
     * @return array<string,mixed>|false
     *
     * DUPLICATE column names collapse here (the last wins) exactly as they do under PDO. That is
     * why {@see fetchNumeric} is built on POSITIONAL rows from the wire rather than on
     * `array_values()` of this — the numeric shape must not lose a column.
     *
     * The arity check turns a framing disagreement into a `Doctrine\DBAL\Driver\Exception`. Without
     * it `array_combine()` raises a bare `ValueError`, and `Doctrine\DBAL\Connection::executeQuery()`
     * catches exactly `Driver\Exception` — so the raw error would escape DBAL's conversion entirely
     * and reach the application past every `catch (Doctrine\DBAL\Exception)` it has.
     */
    public function fetchAssociative(): array|false
    {
        $row = $this->fetchNumeric();
        if ($row === false) {
            return false;
        }
        if (count($row) !== count($this->cols)) {
            throw DriverException::local(sprintf(
                'Ferro: result row has %d cells but the header declared %d columns.',
                count($row),
                count($this->cols),
            ));
        }
        return array_combine($this->cols, $row);
    }

    /**
     * `false` at end-of-result, and NEVER for a row whose first cell is `NULL` — the two are
     * different answers and `FetchUtils::fetchFirstColumn()` is `while (($v = fetchOne()) !== false)`,
     * so a `$row[0] ?? false` shortcut would silently TRUNCATE a column containing NULLs. Delegating
     * to `FetchUtils` (as every bundled driver does) is what keeps the distinction.
     */
    public function fetchOne(): mixed
    {
        return FetchUtils::fetchOne($this);
    }

    /** @return list<list<mixed>> the rows REMAINING from the cursor, not the whole buffer */
    public function fetchAllNumeric(): array
    {
        return FetchUtils::fetchAllNumeric($this);
    }

    /** @return list<array<string,mixed>> the rows REMAINING from the cursor, not the whole buffer */
    public function fetchAllAssociative(): array
    {
        return FetchUtils::fetchAllAssociative($this);
    }

    /** @return list<mixed> */
    public function fetchFirstColumn(): array
    {
        return FetchUtils::fetchFirstColumn($this);
    }

    /**
     * The TERMINAL's `affected` count — never `count($this->rows)`, which is a different number
     * (`Doctrine\DBAL\Connection::executeStatement()` returns exactly this value for a
     * parameterised statement, and a `SELECT` carries rows while affecting nothing).
     *
     * **A documented cross-backend divergence:** for a `SELECT`, PostgreSQL's command tag reports
     * the row count while MySQL reports `0`. DBAL treats `rowCount()` on a SELECT as
     * driver-specific and undefined, and every stock driver has the same divergence, so this is
     * reported as-is rather than normalised — normalising it would mean counting rows, which is
     * exactly the conflation above.
     */
    public function rowCount(): int
    {
        return $this->affected;
    }

    public function columnCount(): int
    {
        return count($this->cols);
    }

    public function getColumnName(int $index): string
    {
        return $this->cols[$index] ?? throw InvalidColumnIndex::new($index);
    }

    /**
     * Idempotent, and afterwards the result holds no rows and no columns: `fetchNumeric()` is
     * `false`, `fetchAll*()` are `[]`, `columnCount()` is `0` and `getColumnName()` throws
     * `InvalidColumnIndex` — the same post-free state the stock `Driver\PgSQL\Result` reaches by
     * nulling its `PgSql\Result` handle.
     *
     * `rowCount()` deliberately SURVIVES, because `affected` is a value the terminal already
     * delivered rather than a handle into a released result. That matches the stock
     * `Driver\SQLite3\Result`, whose `rowCount()` returns a `$changes` int captured at construction
     * and is unaffected by `free()`; `PgSQL\Result` is the one bundled driver that answers `0`
     * afterwards, because it reads `pg_affected_rows()` off the very handle it just released. Both
     * shapes exist upstream; ours is the one that cannot lose a number it was already told.
     *
     * On a STREAMED result it ALSO abandons the open stream (`CANCEL` + drain to the ONE terminal,
     * charter rule 4), without which the next request on this session would read the leftover DATA
     * frames as its own reply. A stream that already reached its terminal is no longer held here
     * ({@see fetchNumeric} releases it), so a completed iteration sends no needless `CANCEL`.
     */
    public function free(): void
    {
        try {
            $this->stream?->close();
        } catch (FerroException $e) {
            // Same boundary rule as {@see advance}: `close()` is a wire operation (CANCEL + drain)
            // and DBAL's `Result::free()` has no idea what a `Ferro\Client\Error\*` is. Clear the
            // state FIRST so a failed close cannot leave this result half-open.
            $this->stream = null;
            $this->gen = null;
            $this->rows = [];
            $this->cols = [];
            $this->cursor = 0;
            throw DriverException::fromFerro($e);
        }
        $this->stream = null;
        $this->gen = null;
        $this->pendingAdvance = false;
        $this->rows = [];
        $this->cols = [];
        $this->cursor = 0;
    }
}
