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

    public function exec(string $sql): int
    {
        try {
            return $this->ferro->fetchRaw($sql, [], $this->readonly, false)['affected'];
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    /**
     * The ONE place a statement with parameters reaches the engine. `Statement::execute()` and
     * {@see query} both land here, which is what keeps the fate declaration and (from Task 10) the
     * pinned-transaction routing in a single place.
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
     * Walking-skeleton form. Task 6 replaces it with the §14 decision — defer, resolve once through
     * one ordinary `SELECT version()`, then FAIL LOUDLY naming the pool — because an empty string
     * here reaches `PlatformVersion::platformFor()` and becomes `InvalidPlatformVersion`, which is
     * loud but says nothing about which pool could not be identified.
     *
     * Written long-hand deliberately: the plan's `poolInfo()?->serverVersion ?? ''` is REJECTED by
     * PHPStan level 9 (`nullsafe.neverNull` — a nullsafe fetch on the left of `??` is redundant,
     * since `??` already suppresses the null read), and level 9 clean is a charter DoD gate.
     */
    public function getServerVersion(): string
    {
        $info = $this->ferro->poolInfo();
        if ($info === null) {
            return '';
        }
        return $info->serverVersion ?? '';
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
