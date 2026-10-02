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
    /**
     * The shim OWNS the connection's Ferro client — the owning connection reaches it only through
     * `getPdo()` ({@see FerroConnectionBody::shim}), so whatever Illuminate does to the PDO it does
     * to statements and transactions together. That is what makes `DB::reconnect()`,
     * `DB::disconnect()` and the lost-connection retry behave as they do on PDO (M2-C1e-2).
     */
    public function __construct(
        private readonly FerroClient $ferro,
    ) {}

    /**
     * The last key any write through THIS handle generated — PDO's `lastInsertId()` semantics.
     *
     * Held here, on the handle, because that is where PDO keeps it: a fresh handle after a
     * reconnect starts with none, exactly as a fresh `PDO` does. It used to live on the owning
     * connection and be read through a closure bound to whichever connection BUILT the shim — which,
     * after a `DatabaseManager` reconnect, is the throwaway connection the manager discards, so
     * `insertGetId()` after `DB::reconnect()` read a key nothing would ever write (C1e-2).
     */
    private int|string|null $lastInsertId = null;

    /**
     * The Ferro client behind this handle. Not part of the PDO surface: the owning connection's
     * execution paths use it, and nothing else should.
     *
     * @internal
     */
    public function ferro(): FerroClient
    {
        return $this->ferro;
    }

    /**
     * Called by the owning connection after every successful statement. Overwrites ONLY on a
     * non-null key, which is what makes the value sticky in exactly PDO's way: a SELECT, an UPDATE,
     * a failed statement or a rolled-back transaction all leave the previous key readable, because
     * `pdo_sqlite` leaves `last_insert_rowid()` alone in every one of those cases too.
     *
     * @internal
     */
    public function rememberInsertId(): void
    {
        $id = $this->ferro->lastInsertId();
        if ($id !== null) {
            $this->lastInsertId = $id;
        }
    }

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
     * Quote a string as a SQL string literal, by the rule the POOL's backend advertises — the rule
     * the family's own PDO driver applies.
     *
     * **The caller is real and was MEASURED, which is what changed this from "deliberately absent".**
     * `Connection::escapeString()` is `getReadPdo()->quote($value)`, reached from `DB::escape()` and
     * — more commonly than that — from `Grammar::substituteBindingsIntoRawSql()`, i.e. every
     * `Builder::toRawSql()`, every `->dd()`/`->dump()` on a query builder, every statement
     * `DB::pretend()` logs, and Laravel's own `castAsJson()` testing helper. Upstream's
     * `{Postgres,MySql,MariaDb,Sqlite}/EscapeTest` fail without it, and their controls pass.
     *
     * **Illuminate has already done the dangerous half before this is called**, which is why so
     * little is left here: `Connection::escape()` rejects NUL bytes and invalid UTF-8 itself, and
     * routes `null`/`int`/`float`/`bool`/binary/array elsewhere entirely. What reaches `quote()`
     * from Illuminate is a valid UTF-8 string with no NULs.
     *
     * **Two rules, chosen by the engine's advertised `literals_are_standard`, never assumed**
     * ({@see quotingRule}):
     *
     *  - **`true` — a backslash is an ordinary character — so the rule is doubling `'` and NOTHING
     *    else.** PostgreSQL with `standard_conforming_strings = on` (its default since 9.1), SQLite
     *    (always: it has no backslash escape mode at all), and MySQL/MariaDB with
     *    `NO_BACKSLASH_ESCAPES` in `sql_mode`. MEASURED against `pdo_pgsql` on PostgreSQL 16 over
     *    eight cases including `backslash-then-quote \'` and `quote-then-backslash '\`: this rule
     *    is BYTE-IDENTICAL to PDO's output, and every case round-trips through `SELECT <literal>`.
     *    It is also exactly what `pdo_mysql` emits under `NO_BACKSLASH_ESCAPES`
     *    (`mysql_real_escape_string_quote()` doubles the quote and nothing else there).
     *  - **`false` on a MySQL-family pool — backslashes ARE escapes, MySQL's default — so the rule
     *    is `mysql_real_escape_string()`'s with ONE deliberate change**: `\` `"` NUL LF CR and
     *    Ctrl-Z are escaped with a backslash, as `pdo_mysql` does, but `'` is DOUBLED rather than
     *    backslash-escaped (M2-C1g, §22.2 (cc)). The reason is that this rule is the POOL's default,
     *    not the live session's: `pdo_mysql` reads `SERVER_STATUS_NO_BACKSLASH_ESCAPES` off the
     *    connection it is about to send on, while the shim reads the bit the engine advertised for a
     *    freshly reset session. An application that turns `NO_BACKSLASH_ESCAPES` ON inside its own
     *    transaction and then quotes would, under `\'`, get a literal whose backslash is ordinary
     *    and whose quote ENDS the string — an injection. `''` means `'` in BOTH modes, and every
     *    other escape can then only mis-render (`\\` reads as two backslashes there), never break
     *    out. The cost is byte-identity with `pdo_mysql` for a string containing `'`
     *    (`'O''Brien'` where it emits `'O\'Brien'`); MySQL reads the two identically in the
     *    default mode, and upstream's `MySql/EscapeTest::testEscapeString`, which compares the
     *    bytes, records it. Escaping BYTE-WISE is correct because
     *    every Ferro MySQL session is `utf8mb4`: `mysql_async` sends `utf8mb4_general_ci` in its
     *    handshake (not configurable from the DSN), and no UTF-8 multi-byte sequence contains a byte
     *    below 0x80 — the GBK/Big5/SJIS class, where a trailing `0x5c` makes byte-wise escaping
     *    unsafe, cannot be negotiated. A tenant that issues `SET NAMES gbk` is tracked as a session
     *    mutation and reset before the next tenant; inside its own session it has the same exposure
     *    `pdo_mysql` has after a `SET NAMES` (the client-side charset does not follow it either).
     *    Measured against `pdo_mysql` on the same server: byte-identical for every input without a
     *    `'`, and every literal round-trips through Ferro in BOTH modes (`MySqlEscapeLiveTest`).
     *
     * `false` on any OTHER family refuses: on PostgreSQL it means `standard_conforming_strings =
     * off`, whose escaping (`E''` semantics, encoding-dependent) this driver does not implement.
     * `null` — the engine has not learned it — refuses on every family.
     *
     * **SPEC §21 D5 is SATISFIED, and it briefly was not.** D5 reads "`quote()` implemented
     * client-side with per-platform tables; **no engine round trip**". The first version of this
     * method verified the rule with a cached `SHOW standard_conforming_strings` — one round trip,
     * which D5 forbids. It is now read from `HELLO_ACK`'s per-pool `literals_are_standard`
     * (§22.2 (at)), which the engine learns for free on every family (a `ParameterStatus` on
     * PostgreSQL, the OK packet's `SERVER_STATUS_NO_BACKSLASH_ESCAPES` on MySQL, a constant on
     * SQLite). Do NOT "fix" a future concern here by deleting the check: an unverified premise
     * under an escaping function is the one option that was considered and rejected outright.
     *
     * **`PDO::PARAM_LOB` is refused rather than guessed at.** Each family's binary literal is a
     * different shape (`'\x…'::bytea`, `x'…'`), and Illuminate never asks this method for one —
     * `escape($value, binary: true)` goes to the connection's own `escapeBinary()`, which builds
     * that form in PHP without touching PDO. Accepting the parameter and ignoring it would silently
     * produce a text literal for binary data.
     */
    public function quote(string $string, int $type = \PDO::PARAM_STR): string
    {
        if ($type !== \PDO::PARAM_STR) {
            throw new \LogicException(sprintf(
                'Ferro: FerroPdoShim::quote() supports only PDO::PARAM_STR (%d), not %d. '
                . "A binary literal is a different shape on every family (PostgreSQL's \\x…::bytea, "
                . "MySQL's and SQLite's x'…'), and Illuminate builds it in PHP via the connection's "
                . 'escapeBinary(), never through here.',
                \PDO::PARAM_STR,
                $type,
            ));
        }

        return match ($this->quotingRule()) {
            'standard' => "'" . str_replace("'", "''", $string) . "'",
            'mysql-backslash' => "'" . strtr($string, self::MYSQL_ESCAPES) . "'",
        };
    }

    /**
     * `mysql_real_escape_string()`'s table for a `utf8mb4` connection, except that `'` is DOUBLED —
     * the one spelling that means `'` whether or not the session has `NO_BACKSLASH_ESCAPES` (see
     * {@see quote}). Single-byte keys, so `strtr()` applies each once and never re-escapes its own
     * output.
     */
    private const MYSQL_ESCAPES = [
        "\\" => "\\\\",
        "'" => "''",
        '"' => '\\"',
        "\0" => '\\0',
        "\n" => '\\n',
        "\r" => '\\r',
        "\x1a" => '\\Z',
    ];

    /**
     * Which escaping rule the backend has CONFIRMED for this pool — or a refusal.
     *
     * **Read off the HANDSHAKE, not the connection — SPEC §21 D5 requires exactly that.** D5 says
     * `quote()` is client-side with NO ENGINE ROUND TRIP, and the first version of this method
     * violated it with a cached `SHOW standard_conforming_strings`. `HELLO_ACK` now advertises
     * `literals_are_standard` per pool (§22.2 (at)), which the engine learns for free.
     *
     * **Fail-closed, and `null` is the case that matters.** `null` means the engine has not learned
     * it — an unreachable backend, an expired cache — and a client must REFUSE to build a literal on
     * unknown. `false` is a real answer only where this driver implements the rule it implies, which
     * is the MySQL family (§22.2 (cc)).
     *
     * @return 'standard'|'mysql-backslash'
     */
    private function quotingRule(): string
    {
        $info = $this->guardValue(fn (): ?\Ferro\Protocol\PoolInfo => $this->ferro->poolInfo());

        if ($info?->literalsAreStandard === true) {
            return 'standard';
        }
        if ($info?->literalsAreStandard === false && $info->kind === 'mysql') {
            return 'mysql-backslash';
        }
        throw new \LogicException(sprintf(
            'Ferro: this pool (%s) does not advertise a quoting rule this driver implements '
            . '(literals_are_standard=%s on a %s pool), so it refuses to build a string literal '
            . 'rather than guess. On PostgreSQL, false means standard_conforming_strings is off — '
            . "set it on, PostgreSQL's own default since 9.1. NULL means the engine has not learned "
            . 'it for this pool (an unreachable backend). Otherwise avoid DB::escape() / toRawSql() '
            . 'on this connection.',
            $info->name ?? '(unknown pool)',
            $info === null ? 'no pool metadata' : var_export($info->literalsAreStandard, true),
            $info->kind ?? 'unknown',
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
     * The generated key of the last successful statement — PDO's `lastInsertId()`.
     *
     * **Demanded by the SQLite column, unreachable on PostgreSQL, and that asymmetry is Illuminate's
     * rather than ours.** `PostgresProcessor::processInsertGetId` runs `insert … returning id` and
     * reads the value out of the RESULT, so the PostgreSQL tier never touches this method — which
     * is why it did not exist until C3-6b. SQLite has no such override: its inserts go through the
     * BASE `Processor::processInsertGetId`, which calls `$connection->getPdo()->lastInsertId()`
     * directly. The framework suite is what surfaced that (11 of 11 `EloquentWhereTest` cases died
     * on the `__call` refusal), which is the whole reason the measurement is built before the tier
     * surface (lesson: do not build what the suite has not demanded).
     *
     * It THROWS where `pdo_sqlite` answers the string `"0"` (measured on a fresh handle — not
     * `false`, as an earlier version of this note said), deliberately and on the sibling Doctrine
     * tier's reasoning: a caller cannot tell `"0"` from a key, and Illuminate's
     * `Processor::processInsertGetId` would hand it straight back as the model's primary key.
     * Loud beats silently wrong — §22.2 (m) already records a WRONG key as strictly worse than none.
     *
     * **It reads the HANDLE's remembered key, not the client's, and that difference is the whole
     * substance of this method.** `Ferro\Client\Connection::lastInsertId()` is deliberately a
     * PER-STATEMENT value: it is cleared on the way in to every request, so a statement that
     * generates no key leaves it `null` rather than carrying a stale one over (M1-S8a, §22.2 (bf)).
     * That is right for the client — on a transaction-mode pool the next statement can land on a
     * different backend connection, so the ENGINE must never carry a rowid forward — and it is
     * exactly wrong for PDO, whose `lastInsertId()` is a property of the HANDLE and keeps answering
     * until another insert replaces it.
     *
     * MEASURED, not reasoned: the framework suite reported 182 of 225 errors on this one line.
     * `Processor::processInsertGetId()` is `insert(); getPdo()->lastInsertId();` with nothing in
     * between — but `Connection::insert()` fires `QueryExecuted`, and any listener that runs a query
     * (which is what `AfterQueryTest`'s whole subject matter is) clears the client's value before
     * Illuminate reads it. Under `pdo_sqlite` the same code is fine.
     *
     * So the PDO SEMANTICS LIVE IN THE PDO SHIM, which is what this class is for: the handle
     * remembers each non-null key from its own writes and never clears it, and the remembered value
     * is byte-for-byte the one `pdo_sqlite` would return — including after a rollback, where PDO
     * also still answers the rolled-back rowid. Nothing in the engine or the client changed, so the
     * Doctrine tier's stricter "read it immediately" contract is untouched.
     *
     * The `$sequence` argument is accepted and IGNORED, which is exactly what `pdo_sqlite` and
     * `pdo_mysql` do. PostgreSQL never reaches here: `PostgresProcessor` uses `insert … returning`.
     */
    public function lastInsertId(?string $sequence = null): string
    {
        // **The MySQL family follows `pdo_mysql`, which is PER-STATEMENT** (M2-C1f review F1):
        // `mysql_insert_id()` answers the statement just executed and "0" after one that generated
        // no key — measured, after a key-less insert, `INSERT IGNORE`, a `SELECT`, an `UPDATE` and a
        // rollback. The client's value has exactly that shape (cleared on the way in to every
        // request), so on this family the handle-sticky key below — `pdo_sqlite`'s semantics —
        // would answer a STALE key where PDO answers "0". Read from the handshake's pool metadata:
        // no round trip.
        $info = $this->guardValue(fn (): ?\Ferro\Protocol\PoolInfo => $this->ferro->poolInfo());
        if ($info?->kind === 'mysql') {
            $current = $this->ferro->lastInsertId();
            return $current === null ? '0' : (string) $current;
        }

        $id = $this->lastInsertId;
        if ($id === null) {
            // `null` here means NO write through this handle has ever generated a key — the handle
            // remembers every one it sees and never clears it, so an intervening statement, a
            // failed one or a rolled-back transaction cannot produce this.
            throw new \LogicException(
                'Ferro: no statement on this connection has generated a key, so lastInsertId() has '
                . 'nothing to return. On SQLite a key comes from last_insert_rowid(), which the '
                . 'engine reports only when the statement actually moved it — an INSERT that '
                . 'explicitly reuses the current rowid reports none (SPEC §22.2 (bf)). On '
                . 'PostgreSQL the wire carries no such field at all and Illuminate does not need '
                . 'it: PostgresProcessor uses `insert … returning id`.',
            );
        }
        return (string) $id;
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
