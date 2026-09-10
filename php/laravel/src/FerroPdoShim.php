<?php // /php/laravel/src/FerroPdoShim.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Ferro\Client\Connection as FerroClient;
use Ferro\Client\Error\FerroException;
use Ferro\Laravel\Exception\FerroQueryException;

/**
 * The narrow PDO-shaped seam Illuminate's transaction handling is written against.
 *
 * **This exists earlier than §15's ordering implies, and the reason is structural rather than a
 * matter of convenience.** §15 frames the PDO shim as a compatibility layer for ecosystem packages
 * that reach for the raw PDO. But `Illuminate\Database\Concerns\ManagesTransactions` — the trait
 * that owns the transaction COUNTER, savepoint naming through the grammar, the `committing`/
 * `committed`/`rollingBack` events, the `transactionsManager` bookkeeping and the `attempts:` retry
 * loop — is itself written entirely against `getPdo()->beginTransaction()/commit()/rollBack()/
 * inTransaction()/exec()`. Supplying those five methods lets ALL of that bookkeeping be inherited
 * unchanged; the alternative is copying `ManagesTransactions`' body into a subclass and keeping it
 * in step with Laravel forever. Five delegating methods against a framework-internal reimplementation
 * is not a close call.
 *
 * It is possible at all because `Connection::getPdo()` has NO return type — it returns whatever
 * `$this->pdo` holds — so this need not extend `\PDO` (which cannot be constructed without a real
 * DSN) and is duck-typed instead. Verified against v11.51.0.
 *
 * **Everything not implemented refuses by name.** `__call` is what makes the boundary honest: a
 * caller reaching for `prepare()`, `quote()` or any other PDO method gets a message saying which
 * method and why, instead of PHP's bare "call to undefined method". `quote()` in particular is
 * deliberately absent — implementing it means owning dialect-specific SQL escaping, security-critical
 * code with no known caller (see `docs/dev-loop/PHASE-C-SCOPE.md`).
 */
final class FerroPdoShim
{
    public function __construct(private readonly FerroClient $ferro) {}

    /**
     * Illuminate calls this only at transaction level 0 — nested levels become savepoints via
     * {@see exec}. The isolation level is not passed here: Laravel has no per-transaction isolation
     * API, so a connection uses the pool's default until a slice adds one.
     */
    public function beginTransaction(): bool
    {
        return $this->guard(fn () => $this->ferro->begin());
    }

    public function commit(): bool
    {
        return $this->guard(fn () => $this->ferro->commit());
    }

    public function rollBack(): bool
    {
        return $this->guard(fn () => $this->ferro->rollBack());
    }

    /**
     * `ManagesTransactions::performRollBack()` checks this before rolling back at level 0, so it
     * must reflect the CLIENT's view of whether a transaction is open — not Illuminate's counter,
     * which is exactly the thing being reconciled.
     */
    public function inTransaction(): bool
    {
        return $this->ferro->inTransaction();
    }

    /**
     * The savepoint path. `createSavepoint()` and `performRollBack($toLevel > 0)` both route here
     * with SQL the STOCK grammar compiled (`SAVEPOINT trans2`, `ROLLBACK TO SAVEPOINT trans2`), so
     * no SQL is generated or rewritten here — charter rule 6 — and the engine admits it as the
     * savepoint passthrough SPEC §22.2 (r) describes, which is legal precisely because a transaction
     * is already open at this point.
     *
     * PDO's own `exec()` returns `int|false`; this one returns `int` and never `false`, because a
     * failure here THROWS ({@see guard}) rather than being signalled by a return value. Declaring
     * the narrower type is the truthful signature — and it is safe because the only callers are
     * Illuminate's two savepoint paths, which ignore the value. (Stock `unprepared()` DOES test
     * `exec(...) !== false`, but this connection overrides `unprepared()` and never routes it here.)
     */
    public function exec(string $statement): int
    {
        return $this->guard(fn (): int => $this->ferro->exec($statement));
    }

    /**
     * @param array<int,mixed> $arguments
     * @throws \LogicException always
     */
    public function __call(string $name, array $arguments): never
    {
        throw new \LogicException(sprintf(
            'Ferro: PDO::%s() is not implemented by FerroPdoShim. The Ferro connection has no real '
            . 'PDO — statements run through the engine, and only the transaction methods '
            . '(beginTransaction, commit, rollBack, inTransaction, exec) are shimmed. If a package '
            . 'needs %s(), that is a scope decision, not an oversight: see '
            . 'docs/dev-loop/PHASE-C-SCOPE.md.',
            $name,
            $name,
        ));
    }

    /**
     * Client exceptions become {@see FerroQueryException} here rather than at the connection, so a
     * transaction-control failure carries its SQLSTATE into Illuminate on the same terms a statement
     * failure does. `ManagesTransactions` catches `Throwable` on every one of these paths, so the
     * type matters for CLASSIFICATION (`causedByConcurrencyError`, `causedByLostConnection`), never
     * for whether it is caught.
     *
     * @template T
     * @param \Closure(): T $op
     * @return ($op is \Closure(): int ? int : bool)
     */
    private function guard(\Closure $op): mixed
    {
        try {
            $r = $op();
        } catch (FerroException $e) {
            throw FerroQueryException::fromFerro($e);
        }
        return is_int($r) ? $r : true;
    }
}
