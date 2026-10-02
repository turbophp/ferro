<?php // /php/laravel/src/FerroMySqlConnection.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Exception;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;
use Ferro\Laravel\Exception\FerroQueryException;
use Illuminate\Database\MySqlConnection;

/**
 * An Illuminate MySQL / MariaDB connection whose EXECUTION layer talks to `ferrod` (§15, M2-C1f).
 *
 * Like its siblings it extends the stock family connection, so `MySqlGrammar`, `MySqlProcessor` and
 * `MySqlBuilder` are inherited untouched (charter rule 6), and the execution layer is the shared
 * {@see FerroConnectionBody}. `MySqlConnection::isMaria()` reads `PDO::ATTR_SERVER_VERSION`, which
 * {@see FerroPdoShim::getAttribute} passes through byte-identical on this family — `-MariaDB` is the
 * only thing that tells the two dialects apart.
 */
class FerroMySqlConnection extends MySqlConnection
{
    use FerroConnectionBody;

    /**
     * Stock `MySqlConnection::insert()` executes through `getPdo()->prepare()` — which the shim
     * refuses, since there is no real PDO — and then stores `getPdo()->lastInsertId()` on the
     * connection for `MySqlProcessor::processInsertGetId()` to read back. The first framework-suite
     * run measured that: 443 of 456 errors were this one refusal (M2-C1f).
     *
     * So this is the same method on the shared write path, keeping its two load-bearing details:
     * the key is read INSIDE the `run()` callback, before `QueryExecuted` fires (a listener that
     * runs a query must not be able to change the key `insertGetId()` returns — which is what
     * `AfterQueryTest` exercises); and it is the STATEMENT's own key, read from the client, not the
     * shim's handle-sticky one. That is `pdo_mysql`'s contract, measured: `lastInsertId()` answers
     * the statement just executed, and `"0"` after an insert that generated no key, a SELECT or an
     * UPDATE. The engine reports the OK packet's `last_insert_id` per statement (§6, `ExecOk`'s
     * `last_insert_id`) and `null` for "none"; `"0"` here is the string `pdo_mysql` answers in that case, and
     * `MySqlProcessor::processInsertGetId()` casts it exactly as it casts PDO's.
     *
     * `$sequence` is accepted and ignored, as `pdo_mysql` ignores it.
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
            $this->execWrite($query, $bindings);
            $this->recordsHaveBeenModified();
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
     * carry (it carries the server's own message, plus the errno and SQLSTATE as fields). So under
     * the stock detector `createOrFirst()` never recognised the duplicate it exists to catch and
     * re-threw it: 8 of the framework suite's 13 non-passes on the first measured MySQL column
     * (M2-C1f). This is the same rule the tier already applies to lost connections and concurrency
     * errors (§22.2 (bw)): a classification Illuminate makes by wording is made here by type, and
     * the stock detector still answers for anything that is not a Ferro error.
     *
     * 1062 alone, exactly the code the stock pattern names — not 1586 or 1022, which stock does not
     * treat as a unique violation either.
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
}
