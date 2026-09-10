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
 * caller reaching for `prepare()` or any other PDO method gets a message saying which method and
 * why, instead of PHP's bare "call to undefined method". The roster grows only as real framework
 * code is MEASURED needing it — which is how both members arrived. `quote()` used to be named here
 * as deliberately absent, "security-critical code with no known caller"; M2-C2f found the caller
 * (see {@see quote}) and the security half of that sentence is why the implementation looks the way
 * it does, not a reason it does not exist.
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
     * Quote a string as a PostgreSQL literal — `'` doubled, wrapped in single quotes.
     *
     * **The caller is real and was MEASURED, which is what changed this from "deliberately absent".**
     * `Connection::escapeString()` is `getReadPdo()->quote($value)`, reached from `DB::escape()` and
     * — more commonly than that — from `Grammar::substituteBindingsIntoRawSql()`, i.e. every
     * `Builder::toRawSql()`, every `->dd()`/`->dump()` on a query builder, and the query strings
     * ecosystem debug tooling renders. Upstream's own `Postgres/EscapeTest::testEscapeString` fails
     * without it, and the control column (stock `pdo_pgsql`, same server) passes it — so it is a
     * Ferro gap rather than an environment one.
     *
     * **Illuminate has already done the dangerous half before this is called**, which is why so
     * little is left here: `Connection::escape()` rejects NUL bytes and invalid UTF-8 itself, and
     * routes `null`/`int`/`float`/`bool`/binary/array elsewhere entirely. What reaches `quote()` is
     * a valid UTF-8 string with no NULs.
     *
     * **The rule is doubling `'` and NOTHING else, and that is only correct while
     * `standard_conforming_strings` is `on`** — so it is VERIFIED rather than assumed
     * ({@see assertLiteralsAreStandard}). With it on, a backslash is an ordinary character;
     * with it off, PostgreSQL would read `\` as an escape and a value ending in a backslash could
     * consume the closing quote. MEASURED against `pdo_pgsql` on PostgreSQL 16 over eight cases
     * including `backslash-then-quote \'` and `quote-then-backslash '\`: this rule is
     * BYTE-IDENTICAL to PDO's output, and every case round-trips through `SELECT <literal>` back to
     * the original bytes.
     *
     * **SPEC §21 D5 is SATISFIED, and it briefly was not.** D5 reads "`quote()` implemented
     * client-side with per-platform tables; **no engine round trip**". The first version of this
     * method verified the rule with a cached `SHOW standard_conforming_strings` — one round trip,
     * which D5 forbids. It is now read from `HELLO_ACK`'s per-pool `literals_are_standard`
     * (§22.2 (at)), which the engine learns from a `ParameterStatus` and therefore costs nothing at
     * all. Do NOT "fix" a future concern here by deleting the check: an unverified premise under an
     * escaping function is the one option that was considered and rejected outright.
     *
     * **`PDO::PARAM_LOB` is refused rather than guessed at.** PostgreSQL's binary literal is
     * `'\x…'::bytea`, a different shape entirely, and Illuminate never asks this method for one —
     * `escape($value, binary: true)` goes to `PostgresConnection::escapeBinary()`, which builds that
     * form in PHP without touching PDO. Accepting the parameter and ignoring it would silently
     * produce a text literal for binary data.
     */
    public function quote(string $string, int $type = \PDO::PARAM_STR): string
    {
        if ($type !== \PDO::PARAM_STR) {
            throw new \LogicException(sprintf(
                'Ferro: FerroPdoShim::quote() supports only PDO::PARAM_STR (%d), not %d. '
                . "PostgreSQL's binary literal is a different shape (\\x…::bytea) and Illuminate "
                . 'builds it in PHP via PostgresConnection::escapeBinary(), never through here.',
                \PDO::PARAM_STR,
                $type,
            ));
        }
        $this->assertLiteralsAreStandard();

        return "'" . str_replace("'", "''", $string) . "'";
    }

    /**
     * Confirm the backend treats a backslash as an ordinary character inside a literal.
     *
     * **Read off the HANDSHAKE, not the connection — SPEC §21 D5 requires exactly that.** D5 says
     * `quote()` is client-side with NO ENGINE ROUND TRIP, and the first version of this method
     * violated it with a cached `SHOW standard_conforming_strings`. `HELLO_ACK` now advertises
     * `literals_are_standard` per pool (§22.2 (at)), which the engine learns for free — PostgreSQL
     * reports the GUC as a `ParameterStatus`, so nothing is ever asked. The value is also better
     * than the `SHOW` was, not merely cheaper: `ParameterStatus` tracks it LIVE, where a cached
     * `SHOW` was one checkout's snapshot.
     *
     * **Fail-closed, and `null` is the case that matters.** `null` means the engine has not learned
     * it — an unreachable backend, an expired cache, a backend family whose arm is unfilled — and a
     * client must REFUSE to build a literal on unknown. Never read it as false (that would claim
     * backslashes ARE escapes) and never as true. Only an unambiguous `true` proceeds.
     */
    private function assertLiteralsAreStandard(): void
    {
        $info = $this->guardValue(fn (): ?\Ferro\Protocol\PoolInfo => $this->ferro->poolInfo());

        if ($info?->literalsAreStandard === true) {
            return;
        }
        throw new \LogicException(sprintf(
            'Ferro: this pool (%s) does not advertise literals_are_standard=true (got %s), and '
            . 'quoting a string literal safely without it needs backslash escaping this driver does '
            . 'not implement. On PostgreSQL that means standard_conforming_strings is off (set it '
            . "on — it is PostgreSQL's own default since 9.1), or the engine has not learned it yet "
            . 'for this pool. Otherwise avoid DB::escape() / toRawSql() on this connection.',
            $info->name ?? '(unknown pool)',
            $info === null ? 'no pool metadata' : var_export($info->literalsAreStandard, true),
        ));
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
     * **A version that is PRESENT but unparseable is the same failure and much quieter, so the raw
     * wire string is normalised before it leaves here** ({@see ServerVersion}). The engine caches the
     * backend's `version()` output verbatim, and PostgreSQL's leads with the product name — which
     * `version_compare` reads as older than any number, so the guard above passed while the branch
     * it protects still went the wrong way. Measured, and caught only by upstream's own suite.
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
        // `$info` is non-null here BY CONSTRUCTION: `$version` came off it, and a null `$version`
        // already threw above. PHPStan agrees — a `?->` here is a reported error, not caution.
        return ServerVersion::normalise($info->kind, $version);
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
        $r = $this->guardValue($op);
        return is_int($r) ? $r : true;
    }

    /**
     * The error translation ALONE, with the result untouched.
     *
     * {@see guard} exists for the PDO methods whose contract is `bool`/`int`, and its coercion to
     * `int|true` is right for those and wrong for anything that returns data — a row set routed
     * through it becomes `true`, which is how the removed `SHOW`-based predecessor of
     * {@see assertLiteralsAreStandard} first read
     * `(unreadable)`. Splitting the two makes the coercion a deliberate choice at each call site
     * rather than something a new caller inherits by accident.
     *
     * @template T
     * @param \Closure(): T $op
     * @return T
     */
    private function guardValue(\Closure $op): mixed
    {
        try {
            return $op();
        } catch (FerroException $e) {
            throw FerroQueryException::fromFerro($e);
        }
    }
}
