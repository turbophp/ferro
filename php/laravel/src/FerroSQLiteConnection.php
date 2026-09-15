<?php // /php/laravel/src/FerroSQLiteConnection.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Ferro\Laravel\Schema\FerroSQLiteBuilder;
use Illuminate\Database\SQLiteConnection;

/**
 * An Illuminate SQLite connection whose EXECUTION layer talks to `ferrod` (§15, C3-6b).
 *
 * The execution layer itself is {@see FerroConnectionBody}, shared verbatim with
 * {@see FerroPostgresConnection} — see that trait for why the two families share a body but must
 * not share a parent. Everything SQLite-specific here comes from `extends SQLiteConnection`: the
 * stock `SQLiteGrammar`, `SQLiteProcessor` and `SQLiteBuilder`, untouched (charter rule 6).
 *
 * **Illuminate's own constructor issues four session PRAGMAs, and all four are §7.4 shapes that
 * cannot work through a pooled connection.** `SQLiteConnection::__construct` calls
 * `configureForeignKeyConstraints()`, `configureBusyTimeout()`, `configureJournalMode()` and
 * `configureSynchronous()`; each returns early when its config key is absent, so on an ordinary
 * Ferro config they are no-ops and nothing is overridden here. When one IS set the pragma is issued
 * on whatever connection the pool hands that statement and is then cleared by hygiene before the
 * next one — which after §22.2 (bm) is a close-and-reopen, so it cannot even leak. The setting
 * would therefore be silently ineffective rather than wrong.
 *
 * That is not a gap to fill at this tier: the engine already guarantees the two that matter
 * (`journal_mode=WAL` and `foreign_keys=ON` are verified at dial, §22.2 (bl)), and `busy_timeout` is
 * the pool's own `checkout_timeout` (§22.2 (bh)). A Ferro application configures them on the pool,
 * exactly as a MySQL one configures `time_zone` there. Left UNREFUSED for now because the framework
 * suite has not demanded a refusal — recorded here rather than guessed at.
 */
class FerroSQLiteConnection extends SQLiteConnection
{
    use FerroConnectionBody;

    /**
     * The one stock class this family cannot inherit unchanged: {@see FerroSQLiteBuilder} fixes
     * `dropAllTables()`/`dropAllViews()` for a pooled connection WITHOUT touching the grammar that
     * produces their SQL. See that class for the two measured defects.
     *
     * **It delegates the PRIMING to `parent::getSchemaBuilder()` rather than copying it.** Upstream's
     * body is `if (is_null($this->schemaGrammar)) { $this->useDefaultSchemaGrammar(); }` followed by
     * the `new` — and copying that condition is both a drift risk and, because the property is
     * documented `@var Grammar`, statically dead code. Letting the parent do it inherits whatever
     * priming Illuminate does now and later; the throwaway stock builder it returns costs one
     * object that stores two references.
     *
     * @return \Illuminate\Database\Schema\SQLiteBuilder
     */
    public function getSchemaBuilder()
    {
        parent::getSchemaBuilder();

        return new FerroSQLiteBuilder($this);
    }
}
