<?php // /php/laravel/src/FerroPostgresConnection.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Ferro\Client\Connection as FerroClient;
use Illuminate\Database\PostgresConnection;

/**
 * An Illuminate PostgreSQL connection whose EXECUTION layer talks to `ferrod` (§15).
 *
 * It extends `PostgresConnection` rather than replacing it, so the stock Grammar, Processor and
 * Schema builder are inherited untouched — they only build SQL strings and post-process arrays, and
 * charter rule 6 says the drop-in tiers change execution and never SQL generation.
 *
 * **C1b implements `select()` and nothing else.** Writes, transactions, `cursor()` and the PDO shim
 * are later slices; until they land, every other execution method still runs Illuminate's stock PDO
 * path, which will fail for want of a real PDO — deliberately, and loudly, rather than appearing to
 * work. See `docs/dev-loop/PHASE-C-SCOPE.md`.
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
        // Illuminate's constructor accepts `\PDO|\Closure`. A closure is passed rather than a real
        // PDO because Ferro has none: `Connection::$pdo` is resolved lazily, so as long as nothing
        // in the implemented path asks for it, this is never invoked. When it IS invoked the
        // failure names the reason instead of surfacing as a TypeError deep inside Illuminate.
        parent::__construct(
            static function (): never {
                throw new \LogicException(
                    'Ferro: this connection has no PDO. The execution path that asked for one is '
                    . 'not implemented yet (C1b implements select() only) — or it is a caller that '
                    . 'reaches for the raw PDO, which the FerroPdoShim slice will address.',
                );
            },
            $database,
            $tablePrefix,
            $config,
        );
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
        /** @var list<\stdClass> */
        return $this->run($query, $bindings, function (string $query, array $bindings): array {
            if ($this->pretending()) {
                return [];
            }
            $result = $this->ferro->fetchRaw($query, array_values($bindings), readonly: false);
            return self::hydrate($result['cols'], $result['rows']);
        });
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
            $o = new \stdClass();
            foreach ($cols as $i => $name) {
                $o->{$name} = $row[$i] ?? null;
            }
            $out[] = $o;
        }
        return $out;
    }
}
