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
     * `PDO::ATTR_SERVER_VERSION` only, and it is here because C2 MEASURED that stock framework code
     * needs it — not because §15 lists a shim surface.
     *
     * `Connection::getServerVersion()` is `getPdo()->getAttribute(PDO::ATTR_SERVER_VERSION)`, and
     * the stock `PostgresGrammar::compileColumns()` branches on it:
     * `version_compare($this->connection?->getServerVersion(), '12.0', '<')` decides whether the
     * introspection SQL selects `a.attgenerated`. So every `hasColumn()`/`getColumnListing()` — and
     * therefore a large part of any schema-touching test — goes through this.
     *
     * **A missing version is LOUD, never defaulted**, and that is the whole point: `version_compare`
     * against `null` evaluates as older-than-12 and would silently emit the wrong introspection SQL
     * for a modern PostgreSQL. The sibling Doctrine tier made the same call for the same reason —
     * a wrong version is a silently wrong dialect (§22.2, D-S8b-1).
     *
     * Every OTHER attribute still refuses by name. That is deliberate: the roster grows only as
     * real framework code is measured needing it, which is exactly how this one arrived.
     */
    public function getAttribute(int $attribute): string
    {
        if ($attribute !== \PDO::ATTR_SERVER_VERSION) {
            throw new \LogicException(sprintf(
                'Ferro: PDO::getAttribute(%d) is not implemented by FerroPdoShim. Only '
                . 'ATTR_SERVER_VERSION (%d) is, because that is the one stock Illuminate code was '
                . 'measured to need. If a package needs another, that is a scope decision — see '
                . 'docs/dev-loop/PHASE-C-SCOPE.md.',
                $attribute,
                \PDO::ATTR_SERVER_VERSION,
            ));
        }

        $info = $this->ferro->poolInfo();
        $version = $info?->serverVersion;
        if ($version === null || $version === '') {
            throw new \LogicException(sprintf(
                'Ferro: the engine advertises no server_version for pool "%s", and Illuminate needs '
                . 'one — PostgresGrammar::compileColumns() version_compares it against 12.0 to pick '
                . 'its introspection SQL, so guessing would silently emit the wrong query. Check '
                . 'that ferrod can reach the pool\'s backend.',
                $info->name ?? '(unknown)',
            ));
        }
        return $version;
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
