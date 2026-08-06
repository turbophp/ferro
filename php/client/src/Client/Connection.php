<?php // /php/client/src/Client/Connection.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\EpochChangedException;
use Ferro\Client\Error\ErrorMapper;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M0ValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Client\Value\ValuePolicy;
use Ferro\Protocol\BeginRequest;
use Ferro\Protocol\BeginResponse;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\Outcome;

/**
 * The query + transaction surface a PHP app actually calls (the M0 "pool" is one session). Reads
 * (`query`/`queryOne`/`scalar`/`rows`) declare `readonly=true`; `exec` is the write path
 * (`readonly=false`); `transaction(closure)` is the recovery surface (SPEC §19.1).
 *
 * **§19.3-CRITICAL — the engine gates the Indeterminate split on the client-declared `readonly` flag
 * ALONE** (no SQL inference): a statement `sent && !readonly` whose connection dies mid-flight is
 * `WriteUnconfirmed{Indeterminate}`, which the client NEVER retries. So a read has no write-fate — a
 * lost read is `ConnectionLost{Retryable}`, safely re-issuable — and every read here sends
 * `readonly=true`; only {@see exec} defaults `readonly=false`.
 *
 * **Resilience.** When constructed with a {@see ReconnectLoop} + {@see RetryPolicy} (as
 * {@see \Ferro\Ferro::connect} does), a `Retryable` READ that hits a lost connection transparently
 * reconnects (epoch-aware) and re-issues, bounded by the policy. A lost WRITE / an `Indeterminate` /
 * a lost COMMIT is NEVER auto-retried — it propagates. Every retry decision is routed through the
 * single {@see FateClassifier} chokepoint. Without a loop (bare unit construction) no path reconnects.
 */
final class Connection
{
    private readonly ExecCodec $codec;
    private readonly RetryPolicy $policy;
    private readonly FateClassifier $fate;
    private readonly PackerInterface $encodePacker;
    private readonly PackerInterface $decodePacker;
    private readonly TypePolicyOptions $types;

    /**
     * `codec:` and the `values:`/`plans:`/`types:` PARTS are mutually exclusive, and so are
     * `values:` and `types:` — see the constructor body for why (each combination used to, or would,
     * silently discard an argument).
     *
     * @param ?TypePolicyOptions $types the SPEC §9.1 policy knobs this connection decodes with
     *   (client-side in M1 — see {@see TypePolicyOptions}). Defaults to the safe object forms.
     */
    public function __construct(
        private readonly SessionInterface $session,
        private readonly string $pool = 'default',
        ?ExecCodec $codec = null,
        private readonly ?ReconnectLoop $reconnect = null,
        ?RetryPolicy $policy = null,
        ?FateClassifier $fate = null,
        ?ValuePolicy $values = null,
        ?PlanCache $plans = null,
        ?PackerInterface $encodePacker = null,
        ?PackerInterface $decodePacker = null,
        ?TypePolicyOptions $types = null,
    ) {
        // A supplied ExecCodec already carries its own ValuePolicy and PlanCache, so `values:`,
        // `plans:` and `types:` have nowhere to go — they used to be accepted and DROPPED, which
        // meant an app that configured a §9.1 policy silently got the default decoding instead.
        // Reject the combination loudly rather than pick a winner.
        if ($codec !== null && ($values !== null || $plans !== null || $types !== null)) {
            throw new \InvalidArgumentException(
                'Connection: `codec:` is mutually exclusive with `values:`, `plans:` and `types:` — '
                . 'a supplied ExecCodec already carries its own ValuePolicy and PlanCache, so those '
                . 'arguments would be SILENTLY DISCARDED. Pass a fully-built `codec:`, or the '
                . '`values:`/`plans:`/`types:` parts, never both.',
            );
        }
        // Same trap one layer down: a ready-made ValuePolicy already embeds whichever §9.1 options
        // it was built with, so a `types:` alongside it would be discarded just as quietly.
        if ($values !== null && $types !== null) {
            throw new \InvalidArgumentException(
                'Connection: `values:` is mutually exclusive with `types:` — a supplied ValuePolicy '
                . 'already embeds its own §9.1 TypePolicyOptions, so `types:` would be SILENTLY '
                . 'DISCARDED. Pass a ready ValuePolicy (`values:`) or the policy options (`types:`).',
            );
        }

        $this->types = $types ?? new TypePolicyOptions();
        $this->encodePacker = $encodePacker ?? PackerFactory::forEncode();
        $this->decodePacker = $decodePacker ?? PackerFactory::forDecode();
        // The default-policy site. M1-S7 Task 7 swaps `M0ValuePolicy` for the M1 policy built from
        // `$this->types`; until then a configured `types:` governs only tags M0 already refuses
        // LOUDLY (DECIMAL/U64/TIMESTAMP/UUID all raise), so it can never mask a silent miscast.
        $this->codec = $codec ?? new ExecCodec(
            $values ?? new M0ValuePolicy(),
            $plans ?? new PlanCache(),
            $this->encodePacker,
            $this->decodePacker,
        );
        $this->policy = $policy ?? RetryPolicy::default();
        $this->fate = $fate ?? new FateClassifier($this->policy->retryReads);
    }

    /** The SPEC §9.1 type policy this connection decodes with (client-side in M1). */
    public function typePolicy(): TypePolicyOptions
    {
        return $this->types;
    }

    /** The live session (the reconnect loop's current one when resilient). */
    public function session(): SessionInterface
    {
        return $this->reconnect?->session() ?? $this->session;
    }

    /** The currently cached opaque `boot_epoch` (`int|string`). */
    public function currentEpoch(): int|string
    {
        return $this->session()->bootEpoch();
    }

    /** Whether the most recent transparent reconnect observed a changed epoch (false if none). */
    public function lastReconnectEpochChanged(): bool
    {
        return $this->reconnect?->lastEpochChanged() ?? false;
    }

    /** How many transparent reconnects have happened (0 without a reconnect loop). */
    public function reconnectCount(): int
    {
        return $this->reconnect?->reconnectCount() ?? 0;
    }

    // ---- autocommit query API -------------------------------------------------------------------

    /**
     * Execute a statement. Defaults to the WRITE fate (`readonly=false`, `fetch=none`) — a caller
     * running a read-only statement through `exec` must pass `readonly=true` so a lost connection is
     * classified Retryable, not Indeterminate. Returns the affected-row count.
     *
     * @param list<mixed> $params positional bind values (`?` → `$n`).
     */
    public function exec(string $sql, array $params = [], bool $readonly = false): int
    {
        return $this->dispatchAutocommit($sql, $params, $readonly, ExecCodec::FETCH_NONE)['affected'];
    }

    /**
     * @template T of object
     * @param list<mixed> $params
     * @param class-string<T>|null $dto
     * @return ($dto is null ? list<array<string,mixed>> : list<T>)
     */
    public function query(string $sql, array $params = [], ?string $dto = null): array
    {
        $res = $this->dispatchAutocommit($sql, $params, true, ExecCodec::FETCH_ROWS);
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
        $res = $this->dispatchAutocommit($sql, $params, true, ExecCodec::FETCH_ROWS);
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
        $res = $this->dispatchAutocommit($sql, $params, true, ExecCodec::FETCH_ROWS);
        $firstRow = $res['rows'][0] ?? null;
        return $firstRow === null ? null : ($firstRow[0] ?? null);
    }

    /**
     * @param list<mixed> $params
     * @return list<array<string,mixed>>
     */
    public function rows(string $sql, array $params = []): array
    {
        return $this->codec->assocRows($this->dispatchAutocommit($sql, $params, true, ExecCodec::FETCH_ROWS));
    }

    /**
     * Lazily stream a read-only query's rows over the engine's windowed HEAD/DATA/END producer
     * (`fetch:FETCH_STREAM`, SPEC §7.2, M1-S5) — the Doctrine/Eloquent `iterate*()`-never-buffers
     * contract: each row is hydrated and yielded as its DATA batch arrives, never the whole result
     * set at once. Sends a replenishing `WINDOW_UPDATE` after every consumed DATA frame so the
     * server's per-request credit window stays healthy.
     *
     * **Abandonment safety.** If the caller stops iterating before the terminal `END` — a
     * `foreach (...) { break; }`, or simply letting the returned Generator get garbage-collected —
     * the Generator's `finally` sends an outbound `CANCEL` and drains the remaining DATA/END frames
     * to the ONE terminal. Without this, the un-read frames would sit on the session's socket and
     * the NEXT request would read them as its own reply (a wire desync) — the reason this method
     * needs its own streamed read path rather than reusing {@see query}'s buffered one.
     *
     * A mid-stream error terminal throws the mapped {@see \Ferro\Client\Error\FerroException} (via
     * {@see \Ferro\Client\Error\ErrorMapper}) AFTER the rows that arrived before it — they are
     * already handed to the caller and are never rewound.
     *
     * Requires the active session to implement {@see StreamingSessionInterface} (the concrete
     * {@see Session}); throws {@see ProtocolException} otherwise rather than mis-reading frames.
     *
     * @template T of object
     * @param list<mixed> $params
     * @param class-string<T>|null $dto
     * @return ($dto is null ? iterable<array<string,mixed>> : iterable<T>)
     */
    public function stream(string $sql, array $params = [], ?string $dto = null): iterable
    {
        $session = $this->session();
        if (!$session instanceof StreamingSessionInterface) {
            throw new ProtocolException(
                'stream() requires a session implementing StreamingSessionInterface (the concrete Session)',
            );
        }
        $payload = $this->codec->encode($this->pool, $sql, $params, true, ExecCodec::FETCH_STREAM, null);

        $opened = $session->openStream(C::SERVICE_SQL, C::METHOD_SQL_EXEC, $payload);
        if ($opened['type'] === 'end') {
            // A known fate decided before any HEAD/DATA went out (e.g. a checkout failure) — no
            // stream was ever really opened, so there's nothing to cancel/drain.
            $this->throwIfError($opened['outcome']);
            return;
        }
        $rid = $opened['requestId'];
        $colNames = array_map(static fn (array $c): string => $c['name'], $opened['cols']);

        $reachedTerminal = false;
        // Set on a failure of a WIRE operation itself (a read/write against `$session`) so the
        // `finally` below never attempts a second wire operation (the cancel+drain) on a
        // connection we already know is broken — that would either mask the real failure with a
        // confusing secondary one, or (worse) silently replace it (a `finally` that throws
        // discards whatever exception was already propagating). A hydration failure (bad DTO
        // arity, etc.) does NOT set this — the wire is fine, so draining the still-unread
        // DATA/END frames is exactly the right cleanup for the NEXT request's sake.
        $wireFailed = false;
        try {
            while (true) {
                try {
                    $frame = $session->readStreamFrame($rid);
                } catch (\Throwable $e) {
                    $wireFailed = true;
                    throw $e;
                }
                if ($frame['type'] === 'end') {
                    $reachedTerminal = true;
                    $this->throwIfError($frame['outcome']);
                    return;
                }

                foreach ($frame['rows'] as $rawRow) {
                    $row = $this->codec->decodeRow($rawRow);
                    yield $dto === null
                        ? $this->codec->assocRow($colNames, $row)
                        : $this->codec->hydrateDto($dto, $colNames, $row);
                }

                try {
                    $session->sendWindowUpdate($rid, 1, $frame['bytes']);
                } catch (\Throwable $e) {
                    $wireFailed = true;
                    throw $e;
                }
            }
        } finally {
            if (!$reachedTerminal && !$wireFailed) {
                $session->abandonStream($rid);
            }
        }
    }

    // ---- transaction ----------------------------------------------------------------------------

    /**
     * Run `$fn($tx)` inside a transaction (SPEC §19.1 recovery surface): `BEGIN` → the closure runs
     * against a tx-scoped {@see TxHandle} → normal return `COMMIT`s, a thrown exception `ROLLBACK`s
     * (best-effort) and rethrows.
     *
     * **§19.3-CRITICAL — the lost-COMMIT carve-out.** A `COMMIT` with no confirmed response is the
     * ONE transactional `Indeterminate`: surfaced as {@see IndeterminateException} and the closure is
     * NEVER re-run (an already-committed tx must not re-apply). The `RetryPolicy` may re-run the whole
     * closure ONLY when the tx is provably dead — a lost/failed `BEGIN`, or a mid-tx statement whose
     * connection died (the engine rolled the tx back) — never for a lost `COMMIT`. A changed epoch on
     * reconnect voids the open `tx_id`; the closure re-runs on the new epoch, or, once the budget is
     * spent, an {@see EpochChangedException} propagates.
     *
     * @template R
     * @param callable(TxHandle): R $fn
     * @return R
     */
    public function transaction(callable $fn, ?RetryPolicy $policy = null): mixed
    {
        $policy ??= $this->policy;
        $attempt = 0;

        while (true) {
            $session = $this->session();

            // ---- 1. BEGIN ----
            try {
                $begin = BeginRequest::encode(
                    ['pool' => $this->pool, 'isolation' => null, 'readonly' => false],
                    $this->encodePacker,
                );
                $outcome = $session->sendRequest(C::SERVICE_TX, C::METHOD_TX_BEGIN, $begin);
            } catch (ConnectionLostException | TransportException $e) {
                // A lost BEGIN never opened the tx (nothing applied) → Retryable, safe to re-run.
                $fate = $this->fate->classifyLoss(
                    OpKind::TxBegin,
                    true,
                    'BEGIN lost: ' . $e->getMessage(),
                    $e instanceof ConnectionLostException ? $e->errorPayload() : null,
                );
                if ($this->reconnect !== null && $attempt + 1 < $policy->maxAttempts) {
                    $this->reconnect->reconnect();
                    ++$attempt;
                    continue;
                }
                throw $fate;
            } catch (CodecException $e) {
                throw new ProtocolException('failed to decode BEGIN terminal: ' . $e->getMessage(), 0, $e);
            }
            if (!$outcome->isOk()) {
                $ex = ErrorMapper::fromOutcome($outcome);
                if ($ex instanceof RetryableException && $attempt + 1 < $policy->maxAttempts) {
                    ++$attempt;
                    continue; // BEGIN rejected retryably; nothing opened — re-run on the same session.
                }
                throw $ex;
            }
            $txId = $this->decodeTxId($outcome);
            $tx = new TxHandle($session, $this->codec, $this->pool, $txId, $this->encodePacker);

            // ---- 2. run the closure ----
            try {
                $result = $fn($tx);
            } catch (\Throwable $closureError) {
                // Best-effort rollback; the original error is what matters.
                try {
                    $tx->rollback();
                } catch (\Throwable) {
                    // The connection may already be gone; swallow — never mask the closure error.
                }

                $decision = $this->txReRunDecision($closureError);
                if ($decision !== TxReRun::No && $attempt + 1 < $policy->maxAttempts) {
                    if ($decision === TxReRun::Reconnect) {
                        // The session died mid-tx: get a fresh, handshaken one. A changed epoch just
                        // confirms the tx is void — we re-run the whole closure either way.
                        $this->reconnect?->reconnect();
                    }
                    ++$attempt;
                    continue;
                }

                // Out of budget (or non-retryable): if a mid-tx epoch change voided the tx, say so.
                if ($this->reconnect?->lastEpochChanged() === true
                    && ($closureError instanceof ConnectionLostException
                        || $closureError instanceof TransportException)) {
                    throw new EpochChangedException(
                        'the engine restarted mid-transaction (boot_epoch changed); the transaction '
                            . 'is dead and must be restarted (§19.1)',
                        true,
                        $closureError,
                    );
                }
                throw $closureError;
            }

            // ---- 3. COMMIT ----
            try {
                $tx->commit();
                return $result;
            } catch (ConnectionLostException | TransportException $e) {
                // §19.3 carve-out: a lost COMMIT is the ONE transactional Indeterminate. The session's
                // last in-flight frame is TX/COMMIT (the frame we just failed on) — NEVER re-run the
                // closure. classifyLoss(TxCommit, …) is unconditional Indeterminate regardless. The
                // `cause()` on that exception is CAUSE_ENGINE_RESTART iff a reconnect (any earlier one
                // in this connection's life) has already observed a changed `boot_epoch`, else the
                // honest generic CAUSE_LINK_LOST — a client-side inference only, never a wire signal.
                throw $this->fate->classifyLoss(
                    OpKind::TxCommit,
                    false,
                    'COMMIT lost: ' . $e->getMessage(),
                    null,
                    $this->reconnect?->lastEpochChanged() ?? false,
                );
            } catch (IndeterminateException | NonRetryableException $e) {
                throw $e; // server gave a definite fate — propagate (Indeterminate is never retried).
            } catch (RetryableException $e) {
                // Server rejected COMMIT retryably (e.g. deadline/rollback before commit) — the tx did
                // NOT apply, so re-running the closure is safe when the budget allows.
                if ($attempt + 1 < $policy->maxAttempts) {
                    ++$attempt;
                    continue;
                }
                throw $e;
            } catch (CodecException $e) {
                throw new ProtocolException('failed to decode COMMIT terminal: ' . $e->getMessage(), 0, $e);
            }
        }
    }

    // ---- internals ------------------------------------------------------------------------------

    /** Throw the mapped taxonomy exception for a non-`Ok` stream terminal {@see Outcome}; a no-op on `Ok`. */
    private function throwIfError(Outcome $outcome): void
    {
        if (!$outcome->isOk()) {
            throw ErrorMapper::fromOutcome($outcome);
        }
    }

    /**
     * Send one autocommit EXEC and decode it, transparently reconnecting + re-issuing a Retryable
     * READ (bounded by the policy). A lost WRITE / Indeterminate / exhausted read propagates.
     *
     * @param list<mixed> $params
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int}
     */
    private function dispatchAutocommit(string $sql, array $params, bool $readonly, int $fetch): array
    {
        $opKind = $readonly ? OpKind::Read : OpKind::Write;
        $payload = $this->codec->encode($this->pool, $sql, $params, $readonly, $fetch, null);
        $attempt = 0;

        while (true) {
            try {
                $outcome = $this->session()->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, $payload);
            } catch (ConnectionLostException | TransportException $e) {
                // No response / dead transport → classify per §19.1 (a lost write is Indeterminate).
                // The `cause()` this yields (when Indeterminate) is CAUSE_ENGINE_RESTART iff a
                // reconnect has already observed a changed `boot_epoch`, else CAUSE_LINK_LOST — a
                // client-side inference only (the wire carries no `cause`).
                $server = $e instanceof ConnectionLostException ? $e->errorPayload() : null;
                $fate = $this->fate->classifyLoss(
                    $opKind,
                    $readonly,
                    $e->getMessage(),
                    $server,
                    $this->reconnect?->lastEpochChanged() ?? false,
                );
                if ($this->reconnect !== null
                    && $attempt + 1 < $this->policy->maxAttempts
                    && $this->fate->mayRetryException($fate, $readonly, $opKind)
                ) {
                    $this->reconnect->reconnect();
                    ++$attempt;
                    continue;
                }
                throw $fate;
            } catch (CodecException $e) {
                throw new ProtocolException('failed to decode SQL terminal: ' . $e->getMessage(), 0, $e);
            }

            if ($outcome->isOk()) {
                return $this->codec->decode($outcome);
            }

            // Server responded with a definite error.
            $ex = ErrorMapper::fromOutcome($outcome);
            if ($this->reconnect !== null
                && $attempt + 1 < $this->policy->maxAttempts
                && $this->fate->mayRetryException($ex, $readonly, $opKind)
            ) {
                // A server-declared Retryable READ: the session is alive → re-issue, no reconnect.
                ++$attempt;
                continue;
            }
            throw $ex;
        }
    }

    /** Whether — and how — a closure failure lets the WHOLE transaction re-run (§19.1). */
    private function txReRunDecision(\Throwable $closureError): TxReRun
    {
        // A mid-tx connection loss killed the tx (the engine rolled it back). Re-running the closure
        // is safe (nothing committed) — but only if we can get a fresh session.
        if ($closureError instanceof ConnectionLostException || $closureError instanceof TransportException) {
            return $this->reconnect !== null ? TxReRun::Reconnect : TxReRun::No;
        }
        // A server-declared Retryable inside the tx (deadlock/serialization): the tx aborted, so
        // re-run the whole closure on the SAME live session — no reconnect needed.
        if ($closureError instanceof RetryableException) {
            return TxReRun::SameSession;
        }
        // Indeterminate (should not occur mid-tx), NonRetryable, Cancelled, Protocol, or a non-Ferro
        // application error: never silently re-run — propagate.
        return TxReRun::No;
    }

    private function decodeTxId(Outcome $outcome): int
    {
        try {
            $off = 0;
            $w = $this->decodePacker->unpack($outcome->body(), $off);
            if (!is_array($w)) {
                throw new CodecException('BeginResponse body is not an array');
            }
            return BeginResponse::mapFromWire(array_values($w))['tx_id'];
        } catch (CodecException $e) {
            throw new ProtocolException('failed to decode BEGIN response: ' . $e->getMessage(), 0, $e);
        }
    }
}
