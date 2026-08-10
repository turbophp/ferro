<?php // /php/doctrine-dbal/src/Connection.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Ferro\Client\Connection as FerroConnection;
use Ferro\Client\Error\FerroException;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\Exception\NoIdentityValue;
use Ferro\DBAL\Exception\ServerVersionUnavailable;

/**
 * The EXECUTION layer. Everything above it — Grammar, the platforms, the schema managers, the
 * migrations runner — stays stock (charter rule 6); this class only decides HOW a statement reaches
 * the engine.
 *
 * **Every statement is declared a WRITE for §19.3 fate purposes** unless the whole connection was
 * configured `driverOptions.readonly`. The DBAL 4 SPI carries no read/write signal — `executeQuery()`
 * with no parameters reaches `query()`, `executeStatement()` with no parameters reaches `exec()`,
 * and BOTH use the same `prepare()`+`execute()` path when parameters are present, so
 * `executeQuery('INSERT … RETURNING id')` is indistinguishable from a SELECT — and charter rule 6
 * forbids inferring one from the SQL text. Declaring "write" costs a lost READ its retryability
 * (it is reported `Indeterminate` rather than `Retryable`); declaring "read" would cost a lost
 * WRITE its honesty, which is the failure this project exists to refuse.
 */
final class Connection implements DriverConnection
{
    /**
     * **The pool NAME is here from Task 5 on, not added later.** Nothing in this task reads it, but
     * Task 6's `ServerVersionUnavailable` message must name the pool (a driver may serve several)
     * and Tasks 7-13 all construct this class. Threading a parameter through afterwards would mean
     * editing every call site those tasks wrote — and a 4-argument call against a 3-argument
     * constructor does not fail where you would expect: PHP binds the first three and DISCARDS the
     * fourth, so under `strict_types` it surfaces as a `TypeError` naming the WRONG parameter
     * (hazard 81).
     */
    public function __construct(
        private readonly FerroConnection $ferro,
        private readonly string $poolName,
        private readonly string $poolKind,
        private readonly bool $readonly,
    ) {}

    /**
     * The resolved backend version, cached for the life of THIS connection — see
     * {@see getServerVersion} for why it is an instance field and not a static.
     */
    private ?string $serverVersion = null;

    /** The underlying Ferro client connection — also what {@see getNativeConnection} returns. */
    public function ferro(): FerroConnection
    {
        return $this->ferro;
    }

    /** The `driverOptions.pool` this connection was opened against. */
    public function poolName(): string
    {
        return $this->poolName;
    }

    /** `postgres` or `mysql`, from `HELLO_ACK`. Never nil. */
    public function poolKind(): string
    {
        return $this->poolKind;
    }

    public function prepare(string $sql): StatementInterface
    {
        return new Statement($this, $sql);
    }

    public function query(string $sql): ResultInterface
    {
        return $this->runPrepared($sql, []);
    }

    /**
     * The parameterless statement path — and, measured rather than assumed, **the one Doctrine's
     * savepoints actually take**: `Doctrine\DBAL\Connection::executeStatement()` calls the driver's
     * `exec()` whenever `count($params) === 0`, and `createSavepoint()`/`rollbackSavepoint()` pass
     * no parameters. So the invariant documented on {@see runPrepared} is load-bearing HERE first.
     */
    public function exec(string $sql): int
    {
        try {
            return $this->ferro->fetchRaw($sql, [], $this->readonly, false)['affected'];
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    /**
     * The ONE place a statement WITH PARAMETERS reaches the engine (`Statement::execute()` and
     * {@see query} both land here; {@see exec} is the parameterless twin). Both call
     * `Ferro\Client\Connection::fetchRaw()`, which is what keeps the fate declaration and the
     * pinned-transaction routing in a single place.
     *
     * **THE INVARIANT: while a transaction is open, this rides its pinned `tx_id`.** It does so
     * because `Ferro\Client\Connection::dispatch()` — which `fetchRaw()` shares with every other
     * statement method — forks on its own open transaction handle. That is not an optimisation
     * detail: Doctrine nests transactions CLIENT-SIDE, so a nested `beginTransaction()` is an
     * ordinary `executeStatement($platform->createSavePoint($name))` arriving right here — at
     * {@see exec}, since it carries no parameters. A statement that did not carry the `tx_id` would
     * be checked out onto a DIFFERENT backend connection, and Doctrine would hold a rollback point
     * that exists in no session.
     *
     * Two guards at two vantage points, so neither can rot into decoration:
     * `TransactionLiveTest::testDbalNestedTransactionsUseSavepointsOnThePinnedTransaction` drives
     * Doctrine's REAL nesting API against both live backends and proves the CONSEQUENCE (the inner
     * rollback undoes only the inner write); `TransactionRoutingTest` proves the MECHANISM by
     * reading the `tx_id` back off the ENCODED `ExecRequest` that carried the stock platform's own
     * `SAVEPOINT …` text.
     *
     * @param list<mixed> $params
     */
    public function runPrepared(string $sql, array $params): ResultInterface
    {
        try {
            $raw = $this->ferro->fetchRaw($sql, $params, $this->readonly, true);
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
        return Result::buffered($raw['cols'], $raw['rows'], $raw['affected']);
    }

    /**
     * D5: present for compatibility, discouraged — parameters are the supported path.
     *
     * **It is per-FAMILY, and that is not cosmetic.** `AbstractPlatform::quoteStringLiteral()`
     * doubles the single quote, but `AbstractMySQLPlatform` overrides it to escape BACKSLASHES
     * first, because MySQL treats `\` as an escape character inside a string literal. Emitting the
     * PostgreSQL form on a MySQL connection would mangle every value containing a backslash. The
     * family is always known (`PoolInfo.kind` is never nil), so this needs no platform and
     * therefore no server version — which matters, because `quote()` must keep working on a pool
     * whose version is unknown. `DriverQuoteTest` locks both branches against the stock platform
     * accessors, so a DBAL change to either goes red here.
     */
    public function quote(string $value): string
    {
        if ($this->poolKind === PlatformVersion::KIND_MYSQL) {
            $value = str_replace('\\', '\\\\', $value);
        }
        return "'" . str_replace("'", "''", $value) . "'";
    }

    /**
     * The generated key of the MOST RECENT statement — never a stale one.
     *
     * DBAL 4's SPI is `lastInsertId(): int|string` with **no sequence-name argument** (that overload
     * was removed in 4.0, which is why SPEC §14's "sequence-name argument supported for PG" is
     * unimplementable), and it must THROW when there is no identity value rather than return a
     * falsy placeholder — a caller cannot tell `0`/`''` from a key.
     *
     * On **PostgreSQL it always throws**: the wire carries no such field, and the client refuses to
     * emulate it with a follow-up `lastval()` because on a transaction-mode pool that lands on a
     * DIFFERENT connection and returns a silently wrong key. {@see NoIdentityValue} names both
     * working answers (`INSERT … RETURNING`, or the ORM's SEQUENCE identity strategy — D-S8b-5).
     *
     * It is read from the CONNECTION, not from a `Result`, and it survives a statement run inside a
     * transaction because `Ferro\Client\Connection::dispatch()` propagates the tx path's
     * `last_insert_id` up to the connection (M1-S8a) — which is where nearly every real INSERT
     * happens. `LastInsertIdLiveTest` pins all three: the MySQL key, the PG throw with its message,
     * and the in-transaction read.
     */
    public function lastInsertId(): int|string
    {
        $id = $this->ferro->lastInsertId();
        if ($id === null) {
            throw NoIdentityValue::forKind($this->poolKind);
        }
        return $id;
    }

    public function beginTransaction(): void
    {
        try {
            $this->ferro->begin($this->readonly);
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    public function commit(): void
    {
        try {
            $this->ferro->commit();
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    public function rollBack(): void
    {
        try {
            $this->ferro->rollBack();
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    /**
     * The backend's own `version()` string, VERBATIM — normalisation is {@see PlatformVersion}'s
     * job, and it is asymmetric (mandatory on PostgreSQL, forbidden on the MySQL family, where the
     * `-MariaDB` suffix is the ONLY thing separating two different SQL dialects).
     *
     * **The SPEC §14 nil-version decision, implemented: DEFER, resolve ONCE, then FAIL LOUDLY.**
     * The return type is a non-nullable `string`, so "unknown" cannot be represented — the only
     * honest options are to resolve it or to throw. `HELLO_ACK` carries `server_version` as
     * `str | nil`, and `nil` is a NORMAL recurring value on a healthy system (a TTL expiry racing a
     * re-probe, a probe failure inside its 5 s backoff, a backend that is down at connect), so it
     * must never be treated as an error state by itself — failing at connect would turn a routine
     * few-second window into an outage for every worker reconnecting during it (§19.1 boot_epoch
     * storms make that concrete).
     *
     * Deferral is free: nothing here runs at connect. Doctrine resolves the platform lazily on
     * first demand ({@see \Doctrine\DBAL\Connection::getDatabasePlatform}), which is typically well
     * after connect — by which time the engine's detached probe has usually landed a value.
     *
     * When it has not, resolution is ONE `SELECT version()` through the ordinary SQL path. That is
     * the same statement `ferrod`'s own probe issues (`ferrod/src/pools.rs`'s `VERSION_SQL`); it is
     * a leading `SELECT`, so the assist lexer's safe-list leaves the connection unpinned and
     * untainted; and it is the ONLY mechanism that can produce a NEW answer — re-reading
     * `poolInfo()` cannot, because that is a snapshot taken once during this session's handshake.
     * It is declared `readonly = true` because it is the DRIVER'S OWN statement: the
     * connection-wide "declare write for everything" rule exists because the DBAL SPI hides the
     * CALLER's intent, and here there is no caller to hide.
     *
     * The result is cached PER CONNECTION for the life of that connection: one round trip, ever.
     * Per connection and not per process — two pools in one worker are two different backends, and
     * a shared cache would hand one pool's version to the other, i.e. possibly MySQL's dialect to
     * PostgreSQL.
     *
     * Note for the streaming task: this reaches the wire, so it must not be attempted while a
     * streamed result is open (the session is strictly single-in-flight). In practice it cannot be:
     * DBAL resolves the platform through this method before any statement runs, and the value is
     * cached from then on.
     *
     * @throws ServerVersionUnavailable when the version is neither advertised nor resolvable. Never
     *   a default platform: a wrong platform is a wrong SQL dialect for every statement that follows.
     */
    public function getServerVersion(): string
    {
        if ($this->serverVersion !== null) {
            return $this->serverVersion;
        }

        $advertised = $this->ferro->poolInfo()?->serverVersion;
        if ($advertised !== null && $advertised !== '') {
            return $this->serverVersion = $advertised;
        }

        try {
            $raw = $this->ferro->fetchRaw('SELECT version()', [], true);
        } catch (FerroException $e) {
            throw ServerVersionUnavailable::forPool($this->poolName, $this->poolKind, $e);
        }

        $v = $raw['rows'][0][0] ?? null;
        if (!is_string($v) || $v === '') {
            throw ServerVersionUnavailable::forPool($this->poolName, $this->poolKind, null);
        }
        return $this->serverVersion = $v;
    }

    /**
     * SPEC §14's documented break: this is a `Ferro\Client\Connection`, not a `PDO`. Anything doing
     * `pg_escape_string($native, …)` or `$native->real_escape_string()` will fatal — that is the
     * incompatibility, and it is listed in `docs/known-incompatibilities.md`.
     */
    public function getNativeConnection(): FerroConnection
    {
        return $this->ferro;
    }
}
