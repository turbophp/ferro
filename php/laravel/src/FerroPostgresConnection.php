<?php // /php/laravel/src/FerroPostgresConnection.php
declare(strict_types=1);
namespace Ferro\Laravel;

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
     * The entire execution layer lives in {@see FerroConnectionBody} and is shared verbatim with
     * {@see FerroSQLiteConnection}. What is PostgreSQL-specific about this class is the `extends`
     * clause and nothing else — the stock `PostgresGrammar`, `PostgresProcessor` and
     * `PostgresBuilder` come with it, untouched (charter rule 6).
     */
    use FerroConnectionBody;
}
