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
 *
 * The same class backs {@see Connection}'s IMPERATIVE trio ({@see Connection::begin} …
 * {@see Connection::commit}/{@see Connection::rollBack}), which holds one privately and routes every
 * statement through {@see runForConnection} — that is how the imperative path inherits these
 * semantics instead of re-implementing them. There is deliberately **no `stream()` here**: a
 * streamed read is many frames per request and lives on {@see Connection::stream}, which carries
 * {@see txId} and rides {@see session} when a transaction is open, so a tx-scoped stream is reached
 * through the Connection rather than duplicated onto this handle. Consequence worth knowing: calling
 * `Connection::stream()` from inside a `transaction()` CLOSURE streams in AUTOCOMMIT — the closure
 * form's handle is not the Connection's `$tx`, so the Connection has no way to know a transaction is
 * open. A caller that needs a tx-scoped stream uses the imperative form.
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
        private readonly ?\Ferro\Manifest $manifest = null,
    ) {}

    /** This transaction's engine-assigned id (monotonic, never reused; native int, < 2^63). */
    public function txId(): int { return $this->txId; }

    /**
     * The session this transaction lives on.
     *
     * A `tx_id` is only meaningful to the session that opened it — the engine's tx registry is
     * per-session and refuses a foreign id ({@see \Ferro\Client\Error\NonRetryableException},
     * `NotFoundOrForbidden`). Exposed so {@see Connection::stream} can send a tx-scoped streamed
     * fetch on the RIGHT session rather than on `Connection::session()`, which is the reconnect
     * loop's CURRENT one and is only incidentally the same object.
     */
    public function session(): SessionInterface { return $this->session; }

    /**
     * The auto-generated key produced by the most recent statement IN THIS TRANSACTION, or `null`.
     * Same contract as {@see Connection::lastInsertId} — it rides the statement's own terminal
     * frame (MySQL's OK packet), is `null` on PostgreSQL, and is never emulated with a follow-up
     * query, which on a transaction-mode pool would read another connection's session state.
     *
     * "Same contract" INCLUDES the failure rule: a statement that throws leaves this `null`, never
     * the previous statement's key ({@see run} clears it on the way in). The two must not diverge —
     * the imperative trio on {@see Connection} routes through this very object.
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

    // ---- checked queries by manifest id, inside the transaction (M3-D2e review F3) ----------
    //
    // The same contract as `Connection::…ById`, minus the idempotent licence: inside a transaction
    // a lost statement is the TRANSACTION's fate (it will never commit), which the closure runner
    // already handles, so nothing here ever re-sends.

    /** @param list<mixed> $params */
    public function execById(string $id, array $params = []): int
    {
        return $this->runById($id, $params, ExecCodec::FETCH_NONE)['affected'];
    }

    /**
     * @template T of object
     * @param list<mixed> $params
     * @param class-string<T>|null $dto
     * @return ($dto is null ? list<array<string,mixed>> : list<T>)
     */
    public function queryById(string $id, array $params = [], ?string $dto = null): array
    {
        $res = $this->runById($id, $params, ExecCodec::FETCH_ROWS);
        if ($dto === null) {
            return $this->codec->assocRows($res);
        }
        $out = [];
        foreach ($res['rows'] as $row) {
            $out[] = $this->codec->hydrateDto($dto, $res['cols'], $row);
        }
        return $out;
    }

    /** @param list<mixed> $params */
    public function scalarById(string $id, array $params = []): mixed
    {
        $res = $this->runById($id, $params, ExecCodec::FETCH_ROWS);
        $firstRow = $res['rows'][0] ?? null;
        return $firstRow === null ? null : ($firstRow[0] ?? null);
    }

    /**
     * @param list<mixed> $params
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}
     */
    private function runById(string $id, array $params, int $fetch): array
    {
        if ($this->manifest === null) {
            throw new \Ferro\Client\Error\ManifestException(
                'a query by id needs a manifest: connect with Ferro::connect(manifest: Manifest::fromFile(…))',
            );
        }
        $q = $this->manifest->query($id);
        if ($q->pool !== $this->pool) {
            throw new \Ferro\Client\Error\ManifestException(
                "query `{$id}` is declared for pool `{$q->pool}`, and this transaction runs on `{$this->pool}`",
            );
        }
        return $this->run($q->sql, $params, $q->readonly, $fetch, $q->id);
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
     * {@see run}, for {@see Connection}'s IMPERATIVE transaction path only (M1-S8a Task 9) — same
     * body, same return shape.
     *
     * It exists so `Connection::begin()`…`commit()` reuses these semantics VERBATIM instead of
     * growing a second in-transaction statement path: one bare send-and-classify, no transparent
     * reconnect, no re-issue (charter rule 3). A statement issued between `begin()` and
     * `commit()`/`rollBack()` is this method.
     *
     * @param list<mixed> $params
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}
     */
    public function runForConnection(string $sql, array $params, bool $readonly, int $fetch, ?string $queryId = null): array
    {
        return $this->run($sql, $params, $readonly, $fetch, $queryId);
    }

    /**
     * Send a tx-scoped EXEC and decode its terminal. A non-`Ok` outcome throws the mapped taxonomy
     * exception (it propagates out of the closure to the tx runner); a garbled body → ProtocolException.
     *
     * @param list<mixed> $params
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}
     */
    private function run(string $sql, array $params, bool $readonly, int $fetch, ?string $queryId = null): array
    {
        // CLEAR FIRST, exactly as `Connection::dispatch` does: {@see lastInsertId} promises the
        // SAME contract as the Connection's, so a statement that fails here must not leave the
        // previous statement's key readable either.
        $this->lastInsertId = null;

        $payload = $this->codec->encode($this->pool, $sql, $params, $readonly, $fetch, $this->txId, $queryId);
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

    /**
     * `COPY … FROM STDIN` inside this transaction (M3-D4) — {@see Connection::copyIn}'s contract,
     * with the transaction's rules: a ROLLBACK undoes it, a server error leaves the transaction
     * failed for the caller to roll back, and a COPY stopped or lost mid-way ends the transaction.
     *
     * @param iterable<mixed, string> $data
     */
    public function copyIn(string $sql, iterable $data): int
    {
        $this->lastInsertId = null;
        return $this->copyRunner()->in(
            Connection::copySession($this->session),
            $this->copyPayload($sql, false),
            $data,
        );
    }

    /**
     * `COPY … TO STDOUT` inside this transaction — {@see Connection::copyOut}'s contract; it sees the
     * transaction's own uncommitted writes.
     *
     * @return \Generator<int, string, mixed, int>
     */
    public function copyOut(string $sql, bool $readonly = false): \Generator
    {
        $this->lastInsertId = null;
        return $this->copyRunner()->out(
            Connection::copySession($this->session),
            $this->copyPayload($sql, $readonly),
            $readonly,
        );
    }

    private function copyRunner(): CopyRunner
    {
        // In a transaction every loss is the transaction's: `TxStatement`, Retryable, whatever the
        // classifier's read-retry setting — so a default classifier decides exactly as the
        // Connection's would.
        return new CopyRunner(new FateClassifier(), $this->codec, true, static fn (): bool => false);
    }

    private function copyPayload(string $sql, bool $readonly): string
    {
        return \Ferro\Protocol\CopyRequest::encode(
            ['pool' => $this->pool, 'sql' => $sql, 'readonly' => $readonly, 'timeout_ms' => null, 'tx_id' => $this->txId],
            $this->encodePacker,
        );
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
