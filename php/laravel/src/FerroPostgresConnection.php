<?php // /php/laravel/src/FerroPostgresConnection.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Ferro\Client\Connection as FerroClient;
use Ferro\Client\Error\FerroException;
use Ferro\Client\RawStream;
use Ferro\Laravel\Exception\FerroQueryException;
use Illuminate\Database\PostgresConnection;

/**
 * An Illuminate PostgreSQL connection whose EXECUTION layer talks to `ferrod` (§15).
 *
 * It extends `PostgresConnection` rather than replacing it, so the stock Grammar, Processor and
 * Schema builder are inherited untouched — they only build SQL strings and post-process arrays, and
 * charter rule 6 says the drop-in tiers change execution and never SQL generation.
 *
 * **C1b–C1d: reads, writes, transactions and streaming.** The wider PDO surface (C1e) is still to
 * come; anything reaching for an unimplemented PDO method refuses BY NAME through
 * {@see FerroPdoShim::__call} rather than failing obscurely. See `docs/dev-loop/PHASE-C-SCOPE.md`.
 */
class FerroPostgresConnection extends PostgresConnection
{
    /**
     * @param array<string,mixed> $config
     */
    public function __construct(
        private readonly FerroClient $ferro,
        string $database = '',
        string $tablePrefix = '',
        array $config = [],
    ) {
        // `Connection::getPdo()` has NO return type — it hands back whatever `$this->pdo` holds —
        // so the shim need not extend `\PDO` (which cannot be constructed without a real DSN).
        // Passing it here is what lets `ManagesTransactions` run UNCHANGED: the transaction counter,
        // savepoint naming through the stock grammar, the connection events and the `attempts:`
        // retry loop are all inherited, and only the five PDO methods underneath them are ours.
        // Passed as a CLOSURE, which `getPdo()` resolves on first use, because Illuminate documents
        // the parameter as `\PDO|\Closure`. The closure form satisfies that contract exactly and
        // costs nothing — `getPdo()` memoises the result into `$this->pdo` on the first call.
        parent::__construct(static fn (): FerroPdoShim => new FerroPdoShim($ferro), $database, $tablePrefix, $config);
    }

    /** The underlying Ferro client — the handle a contact assertion checks. */
    public function getFerroConnection(): FerroClient
    {
        return $this->ferro;
    }

    /**
     * Run a select and return the rows.
     *
     * Routed through `run()` on purpose: that is what gives the tier Illuminate's query log, its
     * `QueryExecuted` event, and — load-bearing — its wrapping of a thrown driver exception into
     * `Illuminate\Database\QueryException`. Only the innermost execution changes.
     *
     * **Every statement here is fate-declared a WRITE (`readonly: false`), and that is deliberate
     * rather than lazy.** `select()` looks like a read and usually is, but it genuinely carries
     * writes: `PostgresProcessor::processInsertGetId` runs `insert … returning id` through
     * `Connection::selectFromWriteConnection()`, which is `select($query, $bindings, false)` —
     * verified in `illuminate/database` v11.51.0. Declaring reads would therefore mis-declare the
     * fate of the single most common Eloquent write on PostgreSQL, and §19.3's `Indeterminate`
     * branch exists precisely so a write whose outcome is unknown is never silently retried.
     *
     * `$useReadPdo === false` is the signal Laravel itself uses to mark that case, so a future
     * refinement could declare `readonly: true` only when it is `true`. It is NOT taken here
     * because that signal is an application-supplied hint, not a guarantee — `DB::select('INSERT …')`
     * passes `true` — and mis-declaring a write as retryable is the one error this branch must never
     * make. The cost is the §22.2 (ac) cry-wolf: a plain SELECT killed by a server-side
     * `statement_timeout` surfaces as an indeterminate write. Same trade the DBAL tier makes, made
     * for the same reason.
     *
     * @param  string  $query
     * @param  array<int|string,mixed>  $bindings
     * @param  bool  $useReadPdo
     * @return list<\stdClass>
     */
    public function select($query, $bindings = [], $useReadPdo = true)
    {
        $rows = $this->run($query, $bindings, function (string $query, array $bindings): array {
            if ($this->pretending()) {
                return [];
            }
            try {
                $result = $this->ferro->fetchRaw($query, array_values($bindings), readonly: false);
            } catch (FerroException $e) {
                // Mapped here, not at `run()`: Illuminate wraps whatever escapes into a
                // QueryException and COPIES ITS CODE, so the SQLSTATE has to be on the exception
                // BEFORE it leaves this closure or `attempts:` cannot see it.
                throw FerroQueryException::fromFerro($e);
            }
            return self::hydrate($result['cols'], $result['rows']);
        });
        return self::narrowRows($rows);
    }

    /**
     * Stream a result set one row at a time — the engine's windowed HEAD/DATA/END producer surfaced
     * as a Generator. `Model::lazy()`, `chunkById()` and `LazyCollection` all build on this, and the
     * point of the method is that a million-row table never materialises in PHP memory.
     *
     * **This method is itself a Generator, so its body is LAZY** — nothing reaches the engine until
     * the caller starts iterating. Stock Illuminate's `cursor()` has exactly the same property (it
     * `yield`s too), so preserving it is fidelity rather than an optimisation.
     *
     * **The `finally` is the load-bearing part.** If the caller stops early — the canonical
     * `foreach ($conn->cursor(...) as $r) { break; }`, or simply dropping the Generator — PHP runs
     * the `finally` on destruction, and {@see \Ferro\Client\RawStream::close} sends an outbound
     * `CANCEL` and drains to the one terminal. Without it the unread DATA frames would sit on the
     * session socket and the NEXT request would read them as its own reply. That failure does not
     * surface on the abandoned query at all — which is why `CursorLiveTest` asserts the NEXT query,
     * not this one.
     *
     * **Mutation-proven, and the failure is worse than a wrong answer.** Deleting this `finally`
     * does not corrupt the following query — it HANGS the session: the abandoned stream leaves
     * ~50 000 rows in flight and the next request waits behind frames nobody will read. The whole
     * suite stopped rather than failing, which is exactly the shape of bug that looks like an
     * infrastructure problem in CI and gets re-run instead of fixed.
     *
     * Fate: `readonly: false`, consistent with {@see select} and for the same reason — see that
     * method's note. A cursor is in practice always a read, but the tier does not infer read-vs-write
     * (charter rule 6), and the cost is the §22.2 (ac) trade rather than a safety risk.
     *
     * @param  string  $query
     * @param  array<int|string,mixed>  $bindings
     * @param  bool  $useReadPdo
     * @return \Generator<int,\stdClass>
     */
    public function cursor($query, $bindings = [], $useReadPdo = true)
    {
        $stream = $this->run($query, $bindings, function (string $query, array $bindings): ?RawStream {
            if ($this->pretending()) {
                return null;
            }
            try {
                return $this->ferro->streamRaw($query, array_values($bindings), readonly: false);
            } catch (FerroException $e) {
                throw FerroQueryException::fromFerro($e);
            }
        });

        if ($stream === null) {
            return;
        }
        if (!$stream instanceof RawStream) {
            throw new \LogicException(
                'Ferro: Illuminate\'s Connection::run() no longer returns its callback\'s value '
                . 'verbatim (expected RawStream, got ' . get_debug_type($stream) . ').',
            );
        }

        $cols = $stream->columns();
        try {
            foreach ($stream->rows() as $row) {
                yield self::hydrateOne($cols, $row);
            }
        } finally {
            $stream->close();
        }
    }

    /**
     * A write whose result the caller does not need. Illuminate's contract is a bare success bool.
     *
     * `fetch:none` rather than `fetch:rows`: the engine then never materialises a result set, which
     * is the whole point of the distinction Illuminate draws between `statement()` and `select()`.
     *
     * @param  string  $query
     * @param  array<int|string,mixed>  $bindings
     * @return bool
     */
    public function statement($query, $bindings = [])
    {
        return self::narrowBool($this->run($query, $bindings, function (string $query, array $bindings): bool {
            if ($this->pretending()) {
                return true;
            }
            $this->execWrite($query, $bindings);
            $this->recordsHaveBeenModified();
            return true;
        }));
    }

    /**
     * A write whose affected-row count the caller DOES need — `update()`, `delete()`, and
     * `executeStatement`-shaped calls.
     *
     * The count comes from the engine's own `affected`, never from counting rows: they are different
     * numbers, and an `UPDATE` that matched 10 rows and changed none still affected 10.
     *
     * @param  string  $query
     * @param  array<int|string,mixed>  $bindings
     * @return int
     */
    public function affectingStatement($query, $bindings = [])
    {
        return self::narrowInt($this->run($query, $bindings, function (string $query, array $bindings): int {
            if ($this->pretending()) {
                return 0;
            }
            $affected = $this->execWrite($query, $bindings);
            $this->recordsHaveBeenModified($affected > 0);
            return $affected;
        }));
    }

    /**
     * Raw SQL with no bindings — migrations and `DB::unprepared()`.
     *
     * @param  string  $query
     * @return bool
     */
    public function unprepared($query)
    {
        return self::narrowBool($this->run($query, [], function (string $query): bool {
            if ($this->pretending()) {
                return true;
            }
            $this->execWrite($query, []);
            $this->recordsHaveBeenModified(true);
            return true;
        }));
    }

    /**
     * `Connection::run()` is annotated `@return mixed` even though it hands back exactly what its
     * callback returned. These two narrow that back, and they CHECK rather than assert: a violation
     * can only mean Illuminate changed `run()`'s contract, which is worth a loud failure naming the
     * fact instead of a silently wrong return type.
     */
    /** @return list<\stdClass> */
    private static function narrowRows(mixed $v): array
    {
        if (!is_array($v)) {
            throw new \LogicException(
                'Ferro: Illuminate\'s Connection::run() no longer returns its callback\'s value '
                . 'verbatim (expected array, got ' . get_debug_type($v) . ').',
            );
        }
        /** @var list<\stdClass> $v */
        return $v;
    }

    private static function narrowBool(mixed $v): bool
    {
        return is_bool($v) ? $v : throw new \LogicException(
            'Ferro: Illuminate\'s Connection::run() no longer returns its callback\'s value verbatim '
            . '(expected bool, got ' . get_debug_type($v) . ').',
        );
    }

    private static function narrowInt(mixed $v): int
    {
        return is_int($v) ? $v : throw new \LogicException(
            'Ferro: Illuminate\'s Connection::run() no longer returns its callback\'s value verbatim '
            . '(expected int, got ' . get_debug_type($v) . ').',
        );
    }

    /**
     * The one place a write reaches the engine, so the fate declaration and the exception mapping
     * are stated once.
     *
     * `readonly: false` is unambiguous here (unlike on {@see select}, where it needed an argument):
     * every caller of this method is a write by Illuminate's own contract.
     *
     * @param array<int|string,mixed> $bindings
     */
    private function execWrite(string $query, array $bindings): int
    {
        try {
            return $this->ferro->exec($query, array_values($bindings), readonly: false);
        } catch (FerroException $e) {
            throw FerroQueryException::fromFerro($e);
        }
    }

    /**
     * Wire rows (positional cells + a separate column list) into the `stdClass` objects Illuminate's
     * Processor and Eloquent expect.
     *
     * `array_combine` is deliberately NOT used: it collapses duplicate column names, and
     * `select a.id, b.id from …` is ordinary SQL. The later name wins here — which is what PDO's
     * `FETCH_OBJ` does too — but building the object property-by-property keeps the row's arity
     * intact rather than silently returning a shorter row.
     *
     * @param list<string> $cols
     * @param list<list<mixed>> $rows
     * @return list<\stdClass>
     */
    private static function hydrate(array $cols, array $rows): array
    {
        $out = [];
        foreach ($rows as $row) {
            $out[] = self::hydrateOne($cols, $row);
        }
        return $out;
    }

    /**
     * One row. Shared by the buffered and streamed paths so they can never drift — a streamed row
     * that hydrated differently from a buffered one would be a difference no test asserts directly.
     *
     * @param list<string> $cols
     * @param list<mixed> $row
     */
    private static function hydrateOne(array $cols, array $row): \stdClass
    {
        $o = new \stdClass();
        foreach ($cols as $i => $name) {
            $o->{$name} = $row[$i] ?? null;
        }
        return $o;
    }
}
