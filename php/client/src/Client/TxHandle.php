<?php // /php/client/src/Client/TxHandle.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ErrorMapper;
use Ferro\Client\Error\ProtocolException;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\SavepointRequest;
use Ferro\Protocol\TxControl;

/**
 * The tx-scoped statement handle passed to a {@see Connection::transaction} closure. Every read/write
 * it issues carries this transaction's `tx_id`, so the engine routes it to the owning tx actor
 * (SPEC §6/§7); savepoint control goes over `SERVICE_TX`. The handle is bound to ONE session and
 * NEVER transparently reconnects mid-statement — a mid-tx connection loss propagates to
 * {@see Connection::transaction}, which (per §19.1) rolls back, reconnects, and re-runs the WHOLE
 * closure on the new epoch under the caller's {@see RetryPolicy}. Retrying an individual in-tx
 * statement would be meaningless: the transaction it belonged to is already dead.
 *
 * `commit`/`rollback` are the closure runner's to call ({@see Connection::transaction}); a closure
 * returns normally to commit or throws to roll back — it does not call them directly. Savepoints
 * ({@see savepoint}/{@see release}/{@see rollbackTo}) are exposed for nested-scope use.
 */
final class TxHandle
{
    /** The auto-generated key the LAST statement in this transaction reported, or null. */
    private int|string|null $lastInsertId = null;

    /** §19.3: reads declare readonly=true (a lost read is Retryable); writes default readonly=false. */
    public function __construct(
        private readonly SessionInterface $session,
        private readonly ExecCodec $codec,
        private readonly string $pool,
        private readonly int $txId,
        private readonly PackerInterface $encodePacker,
    ) {}

    /** This transaction's engine-assigned id (monotonic, never reused; native int, < 2^63). */
    public function txId(): int { return $this->txId; }

    /**
     * The auto-generated key produced by the most recent statement IN THIS TRANSACTION, or `null`.
     * Same contract as {@see Connection::lastInsertId} — it rides the statement's own terminal
     * frame (MySQL's OK packet), is `null` on PostgreSQL, and is never emulated with a follow-up
     * query, which on a transaction-mode pool would read another connection's session state.
     */
    public function lastInsertId(): int|string|null
    {
        return $this->lastInsertId;
    }

    /**
     * Execute a write inside the transaction. `readonly` defaults false (the write fate); a lost
     * connection here dies with the tx (rolled back), so the closure re-runs — never a silent replay.
     *
     * @param list<mixed> $params
     */
    public function exec(string $sql, array $params = [], bool $readonly = false): int
    {
        return $this->run($sql, $params, $readonly, ExecCodec::FETCH_NONE)['affected'];
    }

    /**
     * @template T of object
     * @param list<mixed> $params
     * @param class-string<T>|null $dto
     * @return ($dto is null ? list<array<string,mixed>> : list<T>)
     */
    public function query(string $sql, array $params = [], ?string $dto = null): array
    {
        $res = $this->run($sql, $params, true, ExecCodec::FETCH_ROWS);
        if ($dto === null) {
            return $this->codec->assocRows($res);
        }
        $out = [];
        foreach ($res['rows'] as $row) {
            $out[] = $this->codec->hydrateDto($dto, $res['cols'], $row);
        }
        return $out;
    }

    /**
     * @template T of object
     * @param list<mixed> $params
     * @param class-string<T>|null $dto
     * @return ($dto is null ? array<string,mixed>|null : T|null)
     */
    public function queryOne(string $sql, array $params = [], ?string $dto = null): array|object|null
    {
        $res = $this->run($sql, $params, true, ExecCodec::FETCH_ROWS);
        $firstRow = $res['rows'][0] ?? null;
        if ($firstRow === null) {
            return null;
        }
        return $dto === null
            ? $this->codec->assocRow($res['cols'], $firstRow)
            : $this->codec->hydrateDto($dto, $res['cols'], $firstRow);
    }

    /** @param list<mixed> $params */
    public function scalar(string $sql, array $params = []): mixed
    {
        $res = $this->run($sql, $params, true, ExecCodec::FETCH_ROWS);
        $firstRow = $res['rows'][0] ?? null;
        return $firstRow === null ? null : ($firstRow[0] ?? null);
    }

    /**
     * @param list<mixed> $params
     * @return list<array<string,mixed>>
     */
    public function rows(string $sql, array $params = []): array
    {
        return $this->codec->assocRows($this->run($sql, $params, true, ExecCodec::FETCH_ROWS));
    }

    /** Open a savepoint (engine-named when `$name` is null: an `sp_<n>` stack). */
    public function savepoint(?string $name = null): void
    {
        $this->control(C::METHOD_TX_SAVEPOINT, SavepointRequest::encode(
            ['tx_id' => $this->txId, 'name' => $name],
            $this->encodePacker,
        ));
    }

    /** Release a previously-opened savepoint. */
    public function release(?string $name = null): void
    {
        $this->control(C::METHOD_TX_RELEASE, SavepointRequest::encode(
            ['tx_id' => $this->txId, 'name' => $name],
            $this->encodePacker,
        ));
    }

    /** Roll back to a savepoint (keeping the outer transaction open). */
    public function rollbackTo(?string $name = null): void
    {
        $this->control(C::METHOD_TX_ROLLBACK_TO, SavepointRequest::encode(
            ['tx_id' => $this->txId, 'name' => $name],
            $this->encodePacker,
        ));
    }

    /**
     * COMMIT this transaction. Called by {@see Connection::transaction} when the closure returns
     * normally. A lost/failed COMMIT is the §19.3 Indeterminate carve-out — handled by the caller,
     * which is why this stays a bare send-and-classify with no retry.
     */
    public function commit(): void
    {
        $this->control(C::METHOD_TX_COMMIT, TxControl::encode(['tx_id' => $this->txId], $this->encodePacker));
    }

    /** ROLLBACK this transaction. Called best-effort by {@see Connection::transaction} on closure failure. */
    public function rollback(): void
    {
        $this->control(C::METHOD_TX_ROLLBACK, TxControl::encode(['tx_id' => $this->txId], $this->encodePacker));
    }

    /**
     * Send a tx-scoped EXEC and decode its terminal. A non-`Ok` outcome throws the mapped taxonomy
     * exception (it propagates out of the closure to the tx runner); a garbled body → ProtocolException.
     *
     * @param list<mixed> $params
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}
     */
    private function run(string $sql, array $params, bool $readonly, int $fetch): array
    {
        $payload = $this->codec->encode($this->pool, $sql, $params, $readonly, $fetch, $this->txId);
        try {
            $outcome = $this->session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, $payload);
        } catch (CodecException $e) {
            throw new ProtocolException('failed to decode tx EXEC terminal: ' . $e->getMessage(), 0, $e);
        }
        if (!$outcome->isOk()) {
            throw ErrorMapper::fromOutcome($outcome);
        }
        $decoded = $this->codec->decode($outcome);
        $this->lastInsertId = $decoded['last_insert_id'];
        return $decoded;
    }

    /** Send a `SERVICE_TX` control frame; a non-`Ok` terminal throws the mapped taxonomy exception. */
    private function control(int $method, string $payload): void
    {
        try {
            $outcome = $this->session->sendRequest(C::SERVICE_TX, $method, $payload);
        } catch (CodecException $e) {
            throw new ProtocolException('failed to decode TX control terminal: ' . $e->getMessage(), 0, $e);
        }
        if (!$outcome->isOk()) {
            throw ErrorMapper::fromOutcome($outcome);
        }
        // A control op's Ok body is empty (declare_ctl ⇒ empty Outcome::Ok); nothing to decode.
    }
}
