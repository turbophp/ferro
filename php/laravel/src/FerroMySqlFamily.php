<?php // /php/laravel/src/FerroMySqlFamily.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Exception;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;
use Ferro\Laravel\Exception\FerroQueryException;

/**
 * What the MySQL FAMILY needs beyond the shared execution layer (M2-C1f), used by both
 * {@see FerroMySqlConnection} (Illuminate's `mysql` driver) and {@see FerroMariaDbConnection}
 * (Laravel 11's `mariadb` driver) — which differ only in the stock parent each extends, and so in
 * the Grammar, Processor and Schema builder they inherit (charter rule 6).
 *
 * Every member here was named by the framework suite or by the slice's adversarial review, not
 * designed in advance; SPEC §22.2 (cb) records each.
 */
trait FerroMySqlFamily
{
    /** Once per connection object: the configured `database` label names the pool's database. */
    private bool $ferroDatabaseVerified = false;

    /**
     * Stock `MySqlConnection::insert()` executes through `getPdo()->prepare()` — which the shim
     * refuses, since there is no real PDO — and then stores `getPdo()->lastInsertId()` on the
     * connection for `MySqlProcessor::processInsertGetId()` to read back. The first framework-suite
     * run measured that: 443 of 456 errors were this one refusal.
     *
     * So this is the same method on the shared write path, keeping its load-bearing details: the
     * key is read INSIDE the `run()` callback, before `QueryExecuted` fires (a listener that runs a
     * query must not change the key `insertGetId()` returns — `AfterQueryTest` exercises exactly
     * that), and it is the STATEMENT's own key, read from the client — `pdo_mysql`'s contract,
     * measured: `"0"` after an insert that generated no key, a `SELECT` or an `UPDATE`. The engine
     * reports the OK packet's `last_insert_id` per statement (§6, `ExecOk.last_insert_id`) and `null`
     * for "none"; `"0"` is the string `pdo_mysql` answers in that case, and
     * `MySqlProcessor::processInsertGetId()` casts it exactly as it casts PDO's.
     *
     * `recordsHaveBeenModified()` runs BEFORE the statement, as in stock — a failed insert still
     * marks the connection modified there. `$sequence` is accepted and ignored, as `pdo_mysql`
     * ignores it.
     *
     * @param  string  $query
     * @param  array<int|string,mixed>  $bindings
     * @param  string|null  $sequence
     * @return bool
     */
    public function insert($query, $bindings = [], $sequence = null)
    {
        return self::narrowBool($this->run($query, $bindings, function (string $query, array $bindings): bool {
            if ($this->pretending()) {
                return true;
            }
            $this->recordsHaveBeenModified();
            $this->execWrite($query, $bindings);
            $id = $this->shim()->ferro()->lastInsertId();
            $this->lastInsertId = $id === null ? '0' : (string) $id;
            return true;
        }));
    }

    /**
     * MySQL's duplicate-key error is errno **1062**, decided from the error's STRUCTURE.
     *
     * Stock `MySqlConnection::isUniqueConstraintError()` matches the message text
     * `Integrity constraint violation: 1062` — `pdo_mysql`'s wording, which a Ferro error does not
     * carry (it carries the server's own message, with the errno and SQLSTATE as fields). Under the
     * stock detector `createOrFirst()` never recognised the duplicate it exists to catch and
     * re-threw it: 9 framework-suite errors. The same rule the tier applies to lost connections and
     * concurrency errors (§22.2 (bw)): what Illuminate decides by wording, the tier decides by type,
     * and the stock detector still answers for anything that is not a Ferro error. 1062 alone —
     * exactly the code the stock pattern names. MariaDB reports a duplicate as 1062 too.
     */
    protected function isUniqueConstraintError(Exception $exception)
    {
        $ferro = $exception instanceof FerroQueryException ? $exception->getPrevious() : null;
        if ($ferro instanceof NonRetryableException
            || $ferro instanceof RetryableException
            || $ferro instanceof IndeterminateException
        ) {
            return $ferro->errno() === 1062;
        }
        return parent::isUniqueConstraintError($exception);
    }

    /**
     * The stock schema builder's Ferro subclass — after checking, ONCE, that the configured
     * `database` names the database the pool actually dials.
     *
     * **On this family the label is a SELECTOR, not only a label** (C1f review F12): every
     * `MySqlBuilder` introspection — `hasTable`, `getTables`, `getColumns`, `getIndexes`,
     * `getForeignKeys`, and so `dropAllTables()` — passes `getDatabaseName()` as
     * `information_schema.table_schema`. Measured with a mismatched label: `hasTable()` answered
     * false for a table that exists, so `migrate` re-ran a migration and failed "already exists",
     * and `migrate:fresh` dropped nothing — or, with a label naming ANOTHER schema, would have
     * enumerated that schema's tables. The pool's DSN chooses the database (§12, D8), so the label
     * is compared against the server's own `database()` and a mismatch REFUSES, naming both.
     *
     * One round trip, outside `run()` (no query-log entry, no `QueryExecuted`), at most once per
     * connection object; statements never pay it.
     *
     * @return \Illuminate\Database\Schema\MySqlBuilder
     */
    public function getSchemaBuilder()
    {
        $this->assertDatabaseLabelIsThePoolsDatabase();

        // The parent sets up the schema grammar; its own builder is discarded — the
        // FerroSQLiteConnection shape.
        parent::getSchemaBuilder();

        return $this->newFerroSchemaBuilder();
    }

    private function assertDatabaseLabelIsThePoolsDatabase(): void
    {
        if ($this->ferroDatabaseVerified) {
            return;
        }
        try {
            $result = $this->shim()->ferro()->fetchRaw('select database()', [], readonly: true);
        } catch (FerroException $e) {
            throw FerroQueryException::fromFerro($e);
        }
        $actual = $result['rows'][0][0] ?? null;
        $label = $this->getDatabaseName();
        if (!is_string($actual) || $actual !== $label) {
            throw new \LogicException(sprintf(
                'Ferro: this connection\'s "database" is "%s", but its pool (%s) is connected to %s. '
                . 'On MySQL and MariaDB Illuminate\'s schema builder uses "database" to query '
                . 'information_schema, so it must name the database in the pool\'s DSN — otherwise '
                . 'hasTable()/migrate/migrate:fresh would inspect the wrong schema. Set "database" to '
                . 'that name (SPEC §15, §22.2 (cb)).',
                $label,
                is_string($pool = $this->getConfig('pool')) ? $pool : 'default',
                is_string($actual) ? '"' . $actual . '"' : 'NO database',
            ));
        }
        $this->ferroDatabaseVerified = true;
    }
}
