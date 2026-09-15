<?php // /php/laravel/src/Schema/FerroSQLiteBuilder.php
declare(strict_types=1);
namespace Ferro\Laravel\Schema;

use Illuminate\Database\Schema\Grammars\SQLiteGrammar;
use Illuminate\Database\Schema\SQLiteBuilder;

/**
 * The stock SQLite schema builder with its two schema-wiping methods made to work over a pooled,
 * transaction-mode connection (C3-6b).
 *
 * **Nothing here generates SQL.** Every statement below still comes from the stock
 * `SQLiteGrammar` — `compileEnableWriteableSchema()`, `compileDropAllTables()`,
 * `compileDisableWriteableSchema()`, `compileRebuild()` — and the only change is WHERE the statement
 * boundaries fall. Charter rule 6 intact: the drop-in tiers change execution, never SQL generation.
 *
 * Two independent defects, both MEASURED against laravel/framework v11.51.0 rather than reasoned
 * about, and `migrate:fresh` (which `DatabaseMigrations` drives before EVERY test) hits both.
 *
 * ## 1. `refreshDatabaseFile()` would truncate a file that is not the database
 *
 * Upstream's `dropAllTables()` takes the statement branch only for an in-memory database; for a file
 * one it calls `refreshDatabaseFile()`, which is
 * `file_put_contents($connection->getDatabaseName(), '')`. **Under Ferro `getDatabaseName()` is the
 * config LABEL, not a path** — SPEC §12/D8 keeps the path in the engine, so PHP never learns it, and
 * the engine refuses an in-memory DSN outright (§22.2 (bd)). So the file branch would create and
 * truncate a junk file named e.g. `laravel_tests` in the working directory, leave every real table
 * in place, and report success. The very next test then fails on `table "users" already exists` —
 * which is exactly what the framework suite reported before this class existed.
 *
 * The statement branch is therefore taken UNCONDITIONALLY, and `refreshDatabaseFile()` is overridden
 * to throw rather than left reachable: silently truncating an arbitrary path derived from a config
 * label is destructive, so any future caller should stop loudly instead.
 *
 * ## 2. The statement branch needs one transaction, and the `vacuum` needs to be outside it
 *
 * `PRAGMA writable_schema = 1` is CONNECTION-scoped, and on a transaction-mode pool each of those
 * four `select()` calls is its own checkout. Before §22.2 (bm) the pragma leaked between tenants and
 * the sequence appeared to work — which is how that cross-tenant leak was found. Now that hygiene
 * closes it properly, the sequence is REFUSED in autocommit with `table sqlite_master may not be
 * modified`, measured, and succeeds inside ONE transaction, which pins all of it to one connection.
 * That is the same remedy C3-6a verified for DBAL's `__temp__` table rebuild (§22.2 (bl)); SQLite is
 * simply the first family whose stock schema code depends on session state between statements.
 *
 * `compileRebuild()` is `vacuum`, and **SQLite cannot VACUUM inside a transaction**, so it runs after
 * the commit. It is not merely tidy-up: it is what rebuilds the database after `sqlite_master` was
 * edited by hand, so skipping it would leave the file inconsistent.
 */
class FerroSQLiteBuilder extends SQLiteBuilder
{
    /**
     * Drop every table. Always the statement branch — see the class docblock for why the file one
     * is not merely unnecessary here but actively wrong.
     *
     * @return void
     */
    public function dropAllTables()
    {
        $this->wipe($this->sqliteGrammar()->compileDropAllTables());
    }

    /**
     * Drop every view. Upstream has no file branch here, but the four statements have the same
     * connection-scoped-pragma problem and need the same transaction.
     *
     * @return void
     */
    public function dropAllViews()
    {
        $this->wipe($this->sqliteGrammar()->compileDropAllViews());
    }

    /**
     * @throws \LogicException always
     * @return void
     */
    public function refreshDatabaseFile()
    {
        throw new \LogicException(
            'Ferro: refreshDatabaseFile() would truncate the path in the connection\'s database '
            . 'NAME, and under Ferro that name is the config label — the real file lives in the '
            . 'engine (SPEC §12/D8), so PHP never learns it. Calling this would create and empty a '
            . 'junk file while leaving every table in place. Use dropAllTables(), which does the '
            . 'same job with the stock grammar.',
        );
    }

    /**
     * The shared body: arm `writable_schema`, run the caller's `delete from sqlite_master`, disarm
     * it — all in ONE transaction — then rebuild outside it.
     */
    private function wipe(string $deleteSql): void
    {
        $grammar = $this->sqliteGrammar();

        $this->connection->transaction(function () use ($grammar, $deleteSql): void {
            $this->connection->statement($grammar->compileEnableWriteableSchema());
            $this->connection->statement($deleteSql);
            $this->connection->statement($grammar->compileDisableWriteableSchema());
        });

        // OUTSIDE the transaction: SQLite refuses `VACUUM` inside one.
        $this->connection->statement($grammar->compileRebuild());
    }

    /**
     * `Builder::$grammar` is documented as the BASE `Grammar`, which has none of the four methods
     * above — they are `SQLiteGrammar`'s. This narrows it with a real check rather than an
     * assertion: the only thing that constructs this class is {@see \Ferro\Laravel\
     * FerroSQLiteConnection::getSchemaBuilder}, so a different grammar means someone called
     * `setSchemaGrammar()` with one that cannot compile these statements, and saying so beats
     * failing later on an undefined method.
     */
    private function sqliteGrammar(): SQLiteGrammar
    {
        $grammar = $this->grammar;
        if (!$grammar instanceof SQLiteGrammar) {
            throw new \LogicException(sprintf(
                'Ferro: FerroSQLiteBuilder needs Illuminate\'s SQLiteGrammar to compile the '
                . 'writable-schema statements, but this connection\'s schema grammar is a %s.',
                get_debug_type($grammar),
            ));
        }
        return $grammar;
    }
}
