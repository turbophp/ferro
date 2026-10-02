<?php // /php/laravel/src/FerroConnectionBody.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Ferro\Client\Connection as FerroClient;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\Error\TransportException;
use Ferro\Laravel\Exception\ConnectFailed;
use Ferro\Protocol\Generated\Constants;
use Ferro\Client\RawStream;
use Ferro\Laravel\Exception\FerroQueryException;

/**
 * The Ferro execution layer, shared by every family's Illuminate connection (§15).
 *
 * **This body is family-AGNOSTIC, and that was measured rather than hoped for.** When C3-6b came to
 * add a SQLite column, the whole of `FerroPostgresConnection` turned out to be PostgreSQL-specific
 * in exactly one character: `extends PostgresConnection`. `select`, `cursor`, `statement`,
 * `affectingStatement`, `unprepared`, the binding normalisation and the hydration are all written
 * against the Ferro client and Illuminate's own `run()`, neither of which knows what a family is.
 *
 * So it is a TRAIT rather than a shared base class, because the thing each family must NOT share is
 * its parent: `FerroPostgresConnection extends PostgresConnection` and
 * `FerroSQLiteConnection extends SQLiteConnection` inherit two different stock Grammars, Processors
 * and Schema builders, which is precisely what charter rule 6 keeps stock. A common ancestor could
 * only have been `Connection` itself, which would have thrown that inheritance away.
 *
 * `parent::__construct()` below resolves against the USING class's parent, which is what makes the
 * one shared constructor correct for both.
 */
trait FerroConnectionBody
{
    /**
     * @param \Closure(): FerroClient $dial opens ONE new Ferro client per call. It is called each
     *   time Illuminate resolves this connection's PDO — never at construction (see below).
     * @param array<string,mixed> $config
     */
    public function __construct(
        \Closure $dial,
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
        //
        // **The client is NOT kept on this connection — the shim owns it** (M2-C1e-2). See
        // {@see shim} for why that is a correctness property rather than a matter of taste.
        //
        // **And the client is DIALLED when the closure is resolved, never before — once per
        // resolution, exactly like the closure `ConnectionFactory::createPdoResolver()` hands a
        // stock connection.** The first cut captured a client dialled at construction, and the
        // adversarial review measured what that costs, because Illuminate passes this closure
        // AROUND: `refreshPdoConnections()` transplants it from a throwaway connection that sits
        // in a reference cycle with its grammar, so the throwaway kept the superseded client — and
        // its open transaction — alive after `DB::disconnect()` until the cycle collector ran (a
        // same-key insert blocked the engine's full 10 s idle-in-transaction deadline; 19 of 20
        // superseded clients survived 20 reconnects). And after `DB::purge()` the closure is
        // RESOLVED BY TWO CONNECTION OBJECTS, which then shared one session: one connection's
        // `rollBack()` discarded the other's autocommitted write. A closure that dials owns
        // nothing until it runs, and each run owns its own session, so neither can happen.
        parent::__construct(
            static function () use ($dial): FerroPdoShim {
                try {
                    $client = $dial();
                } catch (TransportException $e) {
                    // Nothing was sent: the transport failed before any statement existed. Typed,
                    // so the lost-connection guard can let Illuminate reconnect and retry, as it
                    // would after a PDO connect failure ({@see causedByLostConnection}).
                    throw ConnectFailed::from($e);
                }
                return new FerroPdoShim($client);
            },
            $database,
            $tablePrefix,
            $config,
        );
    }

    /**
     * The PDO shim, and through it the ONE Ferro client every path of this connection uses.
     *
     * **Why the client lives in the shim rather than on the connection.** Illuminate reconnects by
     * REPLACING THE PDO, never the connection: `DatabaseManager::refreshPdoConnections()` — behind
     * `DB::reconnect()`, behind `run()`'s `reconnectIfMissingConnection()` after `DB::disconnect()`,
     * and behind the lost-connection retry — builds a whole fresh connection and transplants its
     * `getRawPdo()` into the one the application holds (verified in v11.51.0). With PDO that is
     * complete, because the PDO object IS the session. The first cut of this tier kept the client
     * on the connection AND gave the shim its own reference, so after a transplant the shim (and
     * therefore every BEGIN/COMMIT/ROLLBACK, which `ManagesTransactions` routes through `getPdo()`)
     * moved to the new client while every statement stayed on the old one.
     *
     * **MEASURED, against a live engine, both ways it goes wrong — and both are silent:**
     * - after `DB::reconnect()`, `transaction(fn () => insert; throw)` rolled back an EMPTY
     *   transaction on the new client while the insert autocommitted on the old one, so a row the
     *   application was told was discarded was COMMITTED;
     * - after `DB::disconnect()` inside a transaction, the old client still held that transaction
     *   open, so the next ordinary insert ran INSIDE a transaction nothing would ever commit, and
     *   the write was LOST.
     *
     * Routing every path through `getPdo()` makes the shim the single source of truth, so a
     * transplant moves statements and transactions together — PDO's behaviour, by construction.
     * The statement paths resolve it INSIDE their `run()` callbacks, never before, because
     * Illuminate's lost-connection retry reconnects and then re-invokes the SAME callback: one that
     * captured the client up front would retry on the client that had just failed.
     *
     * `getPdo()` is never `null` here on the statement paths: `run()` calls
     * `reconnectIfMissingConnection()` before it invokes the callback.
     */
    private function shim(): FerroPdoShim
    {
        return self::asShim($this->getPdo());
    }

    /**
     * Narrow `getPdo()`'s result to the shim, at a `mixed` boundary on purpose.
     *
     * Illuminate DOCUMENTS `Connection::getPdo()` as returning `\PDO`, but the method has no native
     * return type and returns whatever `$this->pdo` holds — which, on this tier, is a
     * {@see FerroPdoShim} by construction (see the constructor). Static analysis believes the
     * docblock and would call this `instanceof` impossible; the runtime check is the honest one,
     * and it is what turns a foreign PDO set through `setPdo()` into a loud error instead of a
     * fatal call on the wrong object.
     */
    private static function asShim(mixed $pdo): FerroPdoShim
    {
        if (!$pdo instanceof FerroPdoShim) {
            throw new \LogicException(sprintf(
                'Ferro: this connection\'s PDO is %s, not the Ferro shim — something replaced it with '
                . 'setPdo(). A Ferro connection executes through its shim\'s client, so it cannot run '
                . 'on a foreign PDO (SPEC §15).',
                get_debug_type($pdo),
            ));
        }
        return $pdo;
    }

    /**
     * The underlying Ferro client — the handle a contact assertion checks. Reconnects first if
     * Illuminate has disconnected this connection, exactly as a statement would.
     */
    public function getFerroConnection(): FerroClient
    {
        $this->reconnectIfMissingConnection();
        return $this->shim()->ferro();
    }

    /**
     * Illuminate's lost-connection detector, answering by TYPE wherever a Ferro failure is involved.
     *
     * When a `QueryException` outside a transaction is "caused by lost connection",
     * `Connection::tryAgainIfCausedByLostConnection()` reconnects and RE-RUNS THE SAME STATEMENT.
     * The stock detector decides by MESSAGE SUBSTRING (`'Lost connection'`, `'Broken pipe'`,
     * `'No such file or directory'`, …). For a write whose fate is unknown that re-run is exactly
     * the transparent retry charter rule 3 and §19.3 forbid — the first attempt may have applied —
     * and whether a substring happens to match is an accident of wording. Measured at C1e-2: the
     * engine's link-loss text is a lower-case "connection lost during …", which misses
     * `'Connection lost'` by one letter; the engine also forwards backend messages verbatim, and the
     * client's own dial failure DOES match (`'No such file or directory'`).
     *
     * So, walking the exception chain, the FIRST Ferro-relevant link decides:
     * - {@see ConnectFailed} — the client could not be dialled, so nothing was sent: TRUE, the same
     *   reconnect-and-retry PDO gets after a connect failure;
     * - {@see RetryableException} carrying `ERR_CONNECTION_LOST` — the engine itself classified the
     *   loss as known-fate (the statement was never transmitted, or it was inside a transaction,
     *   where Illuminate does not retry anyway): TRUE, by type, whatever its text;
     * - any other {@see RetryableException} — the stock detector decides, as it would for PDO;
     * - ANY OTHER Ferro failure (`IndeterminateException`, a raw `ConnectionLostException` or
     *   `TransportException` — `cursor()`'s stream-open path can surface those unclassified —,
     *   `NonRetryableException`, …): FALSE, whatever its text. Its fate is not known-safe, and a
     *   wrong "yes" re-sends a write.
     *
     * A non-Ferro throwable is left entirely to the stock detector.
     *
     * The cost, stated: Illuminate's PDO-style retry of a SELECT lost mid-flight no longer happens,
     * because this tier declares every statement a write (§22.2 (ac)) and the client therefore
     * reports such a loss `Indeterminate`. The application sees the error instead of a silent retry.
     *
     * @param \Throwable $e
     * @return bool
     */
    protected function causedByLostConnection(\Throwable $e)
    {
        for ($t = $e; $t !== null; $t = $t->getPrevious()) {
            if ($t instanceof ConnectFailed) {
                return true;
            }
            if ($t instanceof RetryableException) {
                return $t->errorPayload()->code === Constants::ERR_CONNECTION_LOST
                    || parent::causedByLostConnection($e);
            }
            if ($t instanceof FerroException) {
                return false;
            }
        }
        return parent::causedByLostConnection($e);
    }

    /**
     * Illuminate's concurrency detector, refusing a write whose fate is unknown.
     *
     * A "yes" here makes `DB::transaction($fn, attempts: N)` RE-RUN THE WHOLE TRANSACTION — from
     * `handleCommitTransactionException()` too, i.e. after a COMMIT. A COMMIT whose reply was lost
     * is `Indeterminate` (§19.3's one transactional case) and must never be re-run, yet the stock
     * detector matches message substrings including SQLite's verbatim `'database is locked'`. No
     * Indeterminate failure carries such a message today; this makes that a property rather than a
     * fact about wording, the same way {@see causedByLostConnection} does.
     *
     * @param \Throwable $e
     * @return bool
     */
    protected function causedByConcurrencyError(\Throwable $e)
    {
        for ($t = $e; $t !== null; $t = $t->getPrevious()) {
            if ($t instanceof IndeterminateException) {
                return false;
            }
        }
        return parent::causedByConcurrencyError($e);
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
                $shim = $this->shim();
                $result = $shim->ferro()->fetchRaw($query, $this->ferroBindings($bindings), readonly: false);
                $shim->rememberInsertId();
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
                return $this->shim()->ferro()->streamRaw($query, $this->ferroBindings($bindings), readonly: false);
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
     * Illuminate's OWN binding normalisation, which the execution paths must not skip.
     *
     * `Connection::prepareBindings()` converts every `DateTimeInterface` to the query grammar's
     * date format and every bool to an int. Stock Illuminate calls it before binding on every path;
     * the first cut of this tier passed `array_values($bindings)` straight through and skipped it.
     *
     * **That was a real defect, and it took a full framework suite to surface it.** Eloquent binds
     * `Carbon` instances directly for datetime attributes, so a plain
     * `Model::create(['created_at' => $carbon])` reached the engine as a `DateTimeInterface`, was
     * tagged canonical `TIMESTAMPTZ`, and was refused against PostgreSQL's naive `timestamp` column
     * — which is what `$table->timestamps()` creates. Unit tests with scalar bindings could not see
     * it; C2's first real run failed on it immediately.
     *
     * Deliberately delegating rather than reimplementing: the grammar owns the date format, and
     * charter rule 6 keeps the grammar stock.
     *
     * @param array<int|string,mixed> $bindings
     * @return list<mixed>
     */
    private function ferroBindings(array $bindings): array
    {
        return array_values($this->prepareBindings($bindings));
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
            $shim = $this->shim();
            $affected = $shim->ferro()->exec($query, $this->ferroBindings($bindings), readonly: false);
            $shim->rememberInsertId();
            return $affected;
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
