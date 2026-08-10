<?php // /php/client/src/Client/Connection.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\EpochChangedException;
use Ferro\Client\Error\ErrorMapper;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\InvalidTransactionStateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Client\Value\ValuePolicy;
use Ferro\Protocol\BeginRequest;
use Ferro\Protocol\BeginResponse;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\PoolInfo;

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
    /**
     * The wire error codes that mean "the transaction you named is not here any more", which
     * {@see rollBack} swallows alongside a lost link. Codes, not messages: a message check would be
     * source-text matching against engine strings that are free to change.
     *
     * `ERR_TX_DEADLINE` is `ferrod`'s tombstone arm (the tx was rolled back + released by a deadline
     * or an in-tx cancel); `ERR_PROTOCOL` is its unknown-or-forbidden `tx_id` and its
     * "transaction is no longer active" (the actor is already gone). Both are engine-side facts
     * about a transaction that has ALREADY ended — see {@see rollBack}'s docblock.
     *
     * @var list<int>
     */
    private const TX_ALREADY_GONE = [C::ERR_TX_DEADLINE, C::ERR_PROTOCOL];

    private readonly ExecCodec $codec;
    private readonly RetryPolicy $policy;
    private readonly FateClassifier $fate;
    private readonly PackerInterface $encodePacker;
    private readonly PackerInterface $decodePacker;
    private readonly TypePolicyOptions $types;

    /** The auto-generated key the LAST statement on this connection reported, or null. */
    private int|string|null $lastInsertId = null;

    /**
     * The handle for an IMPERATIVE transaction opened by {@see begin}, or null.
     *
     * Non-null makes every statement method on this Connection route through it, so a statement
     * issued between `begin()` and `commit()`/`rollBack()` carries the transaction's `tx_id`. It is
     * a real {@see TxHandle} — the SAME object the closure form uses — precisely so the imperative
     * path inherits {@see TxHandle::runForConnection}'s bare send-and-classify semantics: NO
     * transparent reconnect, NO re-issue (charter rule 3). Reconnecting mid-transaction would void
     * the `tx_id` silently and the next statement would land on a tombstoned id.
     *
     * It is cleared on EVERY exit — commit, rollback, and the failure of either — because a handle
     * left behind after a dead transaction would make {@see inTransaction} report `true` forever and
     * every later statement would target a `tx_id` the engine has already tombstoned, with no way
     * back short of discarding the Connection.
     */
    private ?TxHandle $tx = null;

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
        // The default-policy site, and the ONLY place `types:` becomes behaviour: an M1 policy is
        // BUILT from `$this->types` here (M1-S7 Task 7, superseding M0's scalar-only policy). Revert
        // this to `new M0ValuePolicy()` and `Ferro::connect(types: ...)` silently becomes an inert
        // public knob — every DECIMAL/TIMESTAMP/UUID/U64 read throws "not supported in M0" while the
        // configured policy is never consulted. `ConnectionTypePolicyWiringTest` is the guard.
        $this->codec = $codec ?? new ExecCodec(
            $values ?? new M1ValuePolicy($this->types),
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

    /**
     * This connection's OWN pool metadata from `HELLO_ACK` — or null if the engine does not
     * advertise a pool by that name.
     *
     * Resolved LIVE off {@see session} on every call and deliberately NOT cached: the
     * {@see ReconnectLoop} replaces the Session object on a reconnect, and a restarted engine can
     * advertise a different `server_version`. That value is what the Doctrine tier turns into a
     * PLATFORM, i.e. into which SQL dialect it emits, so a stale copy is a silently wrong dialect.
     *
     * Null means "this engine does not have that pool", which is a configuration error worth
     * reporting as itself — never a reason to guess a backend family.
     */
    public function poolInfo(): ?PoolInfo
    {
        foreach ($this->session()->poolInfo() as $info) {
            if ($info->name === $this->pool) {
                return $info;
            }
        }
        return null;
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

    /**
     * The auto-generated key produced by the most recent statement issued **through this
     * Connection**, or `null` when the backend reported none.
     *
     * "Through this Connection" is the whole contract, and it covers exactly two paths (M1-S8a
     * Task 9 widened it from autocommit-only, which is what Task 2's docblock promised):
     *
     *  * an **autocommit** statement ({@see dispatchAutocommit}); and
     *  * a statement issued while an **imperative** transaction is open ({@see begin} …
     *    {@see commit}/{@see rollBack}), which routes through {@see $tx} and propagates the key
     *    back here — so a DBAL driver's `lastInsertId()` is correct for a transactional INSERT,
     *    which is where nearly every real one happens.
     *
     * **It is still NOT updated by the CLOSURE form's statements.** `transaction(fn ($tx) => …)`
     * hands the closure its own {@see TxHandle}; that object records the key and exposes it as
     * {@see TxHandle::lastInsertId()}, and nothing propagates it back to the Connection (the handle
     * is the closure's, and a value read after the closure returned would be from a transaction
     * that no longer exists). So after a closure transaction this still reports the last key from an
     * autocommit or imperative statement — measured at Task 2: autocommit INSERT → 1, in-closure
     * INSERT → 2 via the handle, and this accessor still → 1. Read the key inside the closure, off
     * the handle. Pinned by `ConnectionImperativeTxTest`
     * (`testAnInsertInsideAnImperativeTransactionPropagatesItsKey` and
     * `testTheClosureFormStillDoesNotPropagateTheKey`).
     *
     * **A statement that FAILS clears it — a DELIBERATE divergence from PDO.** Every statement path
     * ({@see dispatch}) nulls this field on the way IN and rewrites it only on success, so after a
     * failed / cancelled / Indeterminate statement this reports `null`, never the key an EARLIER
     * insert generated. PDO does not do that (a failed statement leaves `mysql_insert_id` alone, so
     * PDO keeps serving the stale key), and the divergence was chosen anyway:
     *
     *  * the alternative is handing a caller ANOTHER row's generated key for a statement that
     *    inserted nothing — the silent-wrong-answer class this engine exists to refuse. It is worst
     *    exactly where it matters most: an `Indeterminate` write (SPEC §19.3) has an UNKNOWN key by
     *    definition, so a stale int there is a fabricated answer where `null` is the truth.
     *  * it is not observable through DBAL or the ORM, which read `lastInsertId()` immediately after
     *    a SUCCESSFUL `executeStatement()` and never after a throw. The only shape where PDO and
     *    Ferro differ is code that catches the exception and reads the key anyway.
     *  * the rest of this contract already diverges from PDO deliberately (PG reports `null` rather
     *    than `lastval()`; a plain read clears the key rather than leaving it standing), so "match
     *    PDO" was never the governing rule — "the last statement's key, or nothing" is.
     *
     * Pinned by `ConnectionLastInsertIdTest` (`testAFailedStatementClearsItRatherThanServingAStale…`
     * and its in-transaction / connection-loss siblings).
     *
     * MySQL/MariaDB report it on the OK packet of an `INSERT` into an `AUTO_INCREMENT` table.
     * **PostgreSQL always reports `null`** — it has no such protocol field; the idiomatic form is
     * `INSERT … RETURNING id`, which comes back as an ordinary row.
     *
     * This is NOT emulated with a follow-up query, and that is a correctness rule rather than an
     * optimization: on a transaction-mode pool the follow-up lands on a DIFFERENT connection, where
     * MySQL's `SELECT LAST_INSERT_ID()` returns `0` and PG's `SELECT lastval()` either throws
     * `55000` or — once that session has itself used any sequence — returns ITS value, a silently
     * WRONG key. The value rides the statement's own terminal frame or it does not exist.
     */
    public function lastInsertId(): int|string|null
    {
        return $this->lastInsertId;
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
        return $this->dispatch($sql, $params, $readonly, ExecCodec::FETCH_NONE)['affected'];
    }

    /**
     * @template T of object
     * @param list<mixed> $params
     * @param class-string<T>|null $dto
     * @return ($dto is null ? list<array<string,mixed>> : list<T>)
     */
    public function query(string $sql, array $params = [], ?string $dto = null): array
    {
        $res = $this->dispatch($sql, $params, true, ExecCodec::FETCH_ROWS);
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
        $res = $this->dispatch($sql, $params, true, ExecCodec::FETCH_ROWS);
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
        $res = $this->dispatch($sql, $params, true, ExecCodec::FETCH_ROWS);
        $firstRow = $res['rows'][0] ?? null;
        return $firstRow === null ? null : ($firstRow[0] ?? null);
    }

    /**
     * @param list<mixed> $params
     * @return list<array<string,mixed>>
     */
    public function rows(string $sql, array $params = []): array
    {
        return $this->codec->assocRows($this->dispatch($sql, $params, true, ExecCodec::FETCH_ROWS));
    }

    /**
     * The RAW statement entry point: positional rows, the terminal's own `affected` count, the
     * generated key — and, uniquely on this class, a `readonly` fate flag the CALLER chooses.
     *
     * **Why this exists (M1-S8b).** Every other result-producing method here hard-codes
     * `readonly = true` ({@see query}, {@see queryOne}, {@see scalar}, {@see rows}, {@see stream}),
     * and the engine gates the §19.3 Indeterminate split on that flag ALONE — it never infers a
     * read from the SQL. That is correct for the native API, where the method name IS the
     * declaration. It is wrong for a driver: the Doctrine DBAL 4 SPI carries no read/write signal
     * at all (`Connection::executeQuery('INSERT … RETURNING id')` with no parameters reaches the
     * driver's `query()`, and the prepared path serves `executeQuery` and `executeStatement`
     * alike), and charter rule 6 forbids inferring one from the statement text. A driver built on
     * {@see query} would therefore hand the application `Retryable` — "provably did not apply" —
     * for a write whose fate is genuinely unknown. Here the caller says which it is, and a caller
     * that cannot tell says `false` and gets the conservative answer.
     *
     * Two secondary gaps close with it: the rows come back POSITIONAL (so a driver's
     * `fetchNumeric()` is possible at all, and duplicate column names do not collapse the way
     * {@see rows}' `array_combine` collapses them), and `affected` arrives ALONGSIDE the rows
     * rather than being inferred from `count($rows)` — the two are different numbers.
     *
     * Inside an imperative transaction ({@see begin}) this routes through the pinned `tx_id` like
     * every other statement method, because it shares {@see dispatch}.
     *
     * @param list<mixed> $params positional bind values (`?` → `$n`).
     * @param bool $readonly the §19.3 fate declaration: `false` (the default) means a lost
     *   statement is `Indeterminate`, `true` means it is `Retryable`. Declaring `true` for a
     *   statement that writes is UNSAFE; declaring `false` for a read is merely conservative.
     * @param bool $wantRows `fetch=rows` when true, `fetch=none` when false.
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}
     */
    public function fetchRaw(
        string $sql,
        array $params = [],
        bool $readonly = false,
        bool $wantRows = true,
    ): array {
        return $this->dispatch(
            $sql,
            $params,
            $readonly,
            $wantRows ? ExecCodec::FETCH_ROWS : ExecCodec::FETCH_NONE,
        );
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
     * **Inside an imperative transaction the stream is tx-SCOPED** ({@see begin}): it carries the
     * open transaction's `tx_id` and rides that transaction's OWN session. Either half missing is a
     * silent wrong answer — an autocommit stream runs OUTSIDE the open transaction and sees none of
     * its uncommitted writes, and a `tx_id` sent on a different session than the one that owns it is
     * `NotFoundOrForbidden`. The engine routes a tx-scoped streamed fetch to the owning actor
     * (`ferrod/src/services/sql.rs`), so this is a supported shape, not a workaround.
     *
     * Note that this method is a GENERATOR: nothing below runs — and no frame is written — until the
     * caller starts iterating. That is why the `lastInsertId` reset lives in the body rather than at
     * call time: a stream that is never iterated never ran, so it must not clear anything.
     *
     * @template T of object
     * @param list<mixed> $params
     * @param class-string<T>|null $dto
     * @return ($dto is null ? iterable<array<string,mixed>> : iterable<T>)
     */
    public function stream(string $sql, array $params = [], ?string $dto = null): iterable
    {
        // A tx-scoped stream MUST go out on the transaction's own session, not on
        // `session()` (the reconnect loop's CURRENT one). They are the same object today because
        // nothing reconnects while a transaction is open — but "today" is not an invariant, and the
        // failure if they ever diverge is the engine refusing a tx_id it does not own.
        $session = $this->tx?->session() ?? $this->session();
        if (!$session instanceof StreamingSessionInterface) {
            throw new ProtocolException(
                'stream() requires a session implementing StreamingSessionInterface (the concrete Session)',
            );
        }
        $payload = $this->codec->encode(
            $this->pool,
            $sql,
            $params,
            true,
            ExecCodec::FETCH_STREAM,
            $this->tx?->txId(),
        );
        // A streamed read reports no generated key, and — like every other statement — it CLEARS the
        // previous one rather than letting it linger (the `lastInsertId()` contract is "the last
        // statement's key", never a stale carry-over from two statements ago). The HEAD/DATA/END
        // producer carries no `last_insert_id` field at all, so `null` is the honest value.
        $this->lastInsertId = null;

        $opened = $session->openStream(C::SERVICE_SQL, C::METHOD_SQL_EXEC, $payload);
        if ($opened['type'] === 'end') {
            // A known fate decided before any HEAD/DATA went out (e.g. a checkout failure) — no
            // stream was ever really opened, so there's nothing to cancel/drain.
            $this->throwIfError($opened['outcome']);
            return;
        }
        $rid = $opened['requestId'];
        // `$colNames` is deliberately `list<string>` — the `ColMeta` TAG is dropped here ON PURPOSE
        // (F25/hazard 47). The decode authority is the PER-CELL tag ({@see ExecCodec::decodeRow},
        // which reads `$cell['tag']`), not the column metadata, and the BUFFERED path drops the tag
        // too ({@see ExecCodec::decode}), so the two paths must agree on this shape. Widening it to
        // carry the tag would break `assocRow`, `hydrateDto` and `PlanCache::planFor` for ZERO
        // behavioural gain — a streamed row and a buffered row of the same data already decode to
        // equal values (`ConnectionStreamTest::testStreamedAndBufferedRowsDecodeIdentically`).
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

    // ---- imperative transaction (the DBAL shape) -------------------------------------------------

    /** Whether an imperative transaction opened by {@see begin} is currently open. */
    public function inTransaction(): bool
    {
        return $this->tx !== null;
    }

    /**
     * Open a transaction IMPERATIVELY and leave it open until {@see commit} or {@see rollBack}.
     *
     * This is the shape a Doctrine DBAL driver needs: DBAL's `Connection::beginTransaction()`,
     * `commit()` and `rollBack()` are three unrelated calls with the caller's code in between, it
     * owns its own nesting counter (implemented with `SAVEPOINT` SQL), and its `transactional()`
     * helper is built ON TOP of the trio — DBAL never hands a closure to a driver.
     *
     * **Retry is the CALLER's.** Unlike {@see transaction}, nothing here re-runs anything: there is
     * no closure to re-run, and re-issuing an individual in-transaction statement would be
     * meaningless (the transaction it belonged to is already dead). What the caller IS given is a
     * FATE — every failure below arrives as a taxonomy exception, so the caller can tell a lost
     * BEGIN (nothing opened, safe to re-run) from a lost COMMIT (§19.3 Indeterminate, never
     * re-runnable). A driver should construct its Connection with {@see RetryPolicy::none()} so the
     * autocommit read-retry does not double up with the caller's own policy.
     *
     * Nesting is not supported — call `SAVEPOINT` SQL instead, which passes through inside an open
     * transaction (M1-S8a Task 7). Attempting to nest, or to {@see transaction} while one is open,
     * throws {@see InvalidTransactionStateException}.
     *
     * Isolation is deliberately NOT a parameter: named isolation constants would mean hand-written
     * protocol numbers on the PHP side (charter rule 2), and Doctrine sets isolation with a
     * `SET SESSION TRANSACTION ISOLATION LEVEL …` statement, not a driver flag.
     */
    public function begin(bool $readonly = false): void
    {
        if ($this->tx !== null) {
            throw new InvalidTransactionStateException(
                'a transaction is already open on this connection; Ferro does not nest transactions '
                    . '(use SAVEPOINT SQL, which passes through inside an open transaction)',
            );
        }
        $session = $this->session();
        $payload = BeginRequest::encode(
            ['pool' => $this->pool, 'isolation' => null, 'readonly' => $readonly],
            $this->encodePacker,
        );
        try {
            $outcome = $session->sendRequest(C::SERVICE_TX, C::METHOD_TX_BEGIN, $payload);
        } catch (ConnectionLostException | TransportException $e) {
            // A LOST BEGIN must be handed to the caller as a FATE, not as a raw transport error.
            // "The caller owns retry" only means anything if the caller is told what it is ALLOWED
            // to retry — and a lost BEGIN opened nothing, so it is Retryable. The closure form
            // already routes this through the same classifier ({@see transaction}'s BEGIN arm); the
            // imperative form must not be the one path that leaks an untyped TransportException.
            //
            // NOTE the deliberate difference from the closure form: no reconnect and no re-issue
            // happen here (charter rule 3). The typed exception is the whole answer.
            throw $this->fate->classifyLoss(
                OpKind::TxBegin,
                true,
                'BEGIN lost: ' . $e->getMessage(),
                $e instanceof ConnectionLostException ? $e->errorPayload() : null,
                $this->reconnect?->lastEpochChanged() ?? false,
            );
        } catch (CodecException $e) {
            throw new ProtocolException('failed to decode BEGIN terminal: ' . $e->getMessage(), 0, $e);
        }
        if (!$outcome->isOk()) {
            // A REJECTED BEGIN opened nothing either, so nothing is left dangling. The taxonomy
            // exception propagates verbatim and the CALLER decides whether to retry.
            throw ErrorMapper::fromOutcome($outcome);
        }
        // `$this->tx` is assigned LAST: every throw above leaves this Connection transaction-free,
        // so a failed begin() can never wedge `inTransaction()` at true.
        $this->tx = new TxHandle(
            $session,
            $this->codec,
            $this->pool,
            $this->decodeTxId($outcome),
            $this->encodePacker,
        );
    }

    /**
     * COMMIT the transaction opened by {@see begin}.
     *
     * A lost COMMIT is the §19.3 Indeterminate carve-out and propagates as
     * {@see IndeterminateException} — it is NEVER retried, here or anywhere. The handle is cleared
     * BEFORE the exception escapes so a failed commit cannot leave this Connection wedged in a
     * transaction that no longer exists engine-side.
     */
    public function commit(): void
    {
        $tx = $this->requireTx('commit');
        $this->tx = null;
        try {
            $tx->commit();
        } catch (ConnectionLostException | TransportException $e) {
            throw $this->fate->classifyLoss(
                OpKind::TxCommit,
                false,
                'COMMIT lost: ' . $e->getMessage(),
                null,
                $this->reconnect?->lastEpochChanged() ?? false,
            );
        }
    }

    /**
     * ROLLBACK the transaction opened by {@see begin}. The handle is cleared either way.
     *
     * **A lost ROLLBACK does not throw.** This is a deliberate asymmetry with {@see commit}, and it
     * exists because of how the caller uses it: `Doctrine\DBAL\Connection::transactional()` — and
     * essentially every hand-written `try { … } catch { $conn->rollBack(); throw; }` — calls this
     * from a `catch`/`finally` block, where the caller is ALREADY carrying the error that matters.
     * A raw throw from here would replace that error with a transport failure and the real cause
     * would never be seen.
     *
     * It is also harmless, which is what makes it correct rather than merely convenient: a rollback
     * whose response was lost has the same OUTCOME as one that succeeded. The transaction is dead
     * either way — the engine rolls back and tombstones the `tx_id` on session death, on deadline
     * and on drop (§19.3; `OpKind::TxRollback` classifies Retryable precisely because "a lost
     * rollback is not a lost write"). There is nothing for the caller to decide, so there is nothing
     * to report.
     *
     * **The same reasoning covers "that transaction is already gone", which is NOT a link failure**
     * and is in fact the COMMONEST way a mid-transaction failure lands. When an in-transaction
     * statement hits `idle_in_tx` / `max_tx` / an in-tx cancel, the engine rolls the transaction
     * back, releases the pinned connection and TOMBSTONES the `tx_id`; the caller's `finally` then
     * sends `ROLLBACK` on that dead id and gets back a well-formed `Outcome::Error` —
     * {@see C::ERR_TX_DEADLINE} (`resolve_active`'s tombstone arm) or {@see C::ERR_PROTOCOL}
     * ("unknown or forbidden tx_id", "transaction is no longer active"). Letting those throw
     * defeated the exact property this swallow exists to protect: the `finally` replaced the
     * caller's real error with a `RetryableException` about a transaction that was already dead.
     * They are swallowed for the identical reason a lost ROLLBACK is — there is nothing left to
     * decide. (Reported in the M1-S8a Task 8/9 review, F4.)
     *
     * **It is NOT blanket**, and deliberately so. Any OTHER server-side rejection still throws — a
     * backend failure during the rollback itself ({@see C::ERR_CONNECTION_LOST} out of the fate
     * matrix) is the engine reporting something that HAPPENED, not "that transaction does not
     * exist", and swallowing every class here would leave `rollBack()` unable to report anything at
     * all. The closure form's rollback arm is a blanket `catch (\Throwable)`; this one is scoped.
     *
     * Known cost of keying on the wire code: `ERR_PROTOCOL` on a ROLLBACK has a third producer — a
     * malformed `TxControl` body, i.e. a CLIENT codec defect — which is indistinguishable on the
     * wire from the two "tx is gone" cases and is therefore swallowed here too. It stays loud on
     * COMMIT and on every savepoint op, which do not swallow. A dedicated `TxNotFound` error code in
     * `/proto` would remove the ambiguity; that is a registry change and is left to the tier that
     * owns `/proto`.
     */
    public function rollBack(): void
    {
        $tx = $this->requireTx('rollBack');
        $this->tx = null;
        try {
            $tx->rollback();
        } catch (ConnectionLostException | TransportException) {
            // Intentionally swallowed — see the docblock. The transaction is dead either way, and
            // this is almost always called from a `finally` that is carrying the real error.
        } catch (RetryableException | NonRetryableException $e) {
            // Both classes are reachable for the same fact: the wire `branch` byte picks the class
            // (tombstone → Retryable, unknown-id → NonRetryable), so the decision is made on the
            // `code`, which is what actually identifies "that transaction is gone".
            if (!in_array($e->errorCode(), self::TX_ALREADY_GONE, true)) {
                throw $e;
            }
        }
    }

    /**
     * The open imperative transaction, or the loud misuse error naming the method that wanted one.
     *
     * A statement that FAILS mid-transaction deliberately does NOT clear `$this->tx`: the caller's
     * `finally` must still be able to call {@see rollBack} and have it stay quiet. Clearing there
     * would turn every such `finally` into an {@see InvalidTransactionStateException} that MASKS the
     * error the caller was already carrying — the exact failure {@see rollBack}'s swallowing arm
     * exists to prevent.
     */
    private function requireTx(string $method): TxHandle
    {
        return $this->tx ?? throw new InvalidTransactionStateException(
            $method . '() with no open transaction (call begin() first)',
        );
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
        // Refuse to nest inside an imperative transaction (M1-S8a Task 9). Running the closure form
        // here would BEGIN a second, unrelated transaction on the same Connection while `$this->tx`
        // still points at the first — and, worse, its §19.1 re-run loop could reconnect underneath
        // the open imperative tx and silently void its `tx_id`. Everything below this guard is
        // unchanged.
        if ($this->tx !== null) {
            throw new InvalidTransactionStateException(
                'transaction() cannot be called while an imperative transaction is open '
                    . '(commit() or rollBack() first); Ferro does not nest transactions',
            );
        }
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
     * The ONE fork every statement method takes: an open imperative transaction routes through its
     * {@see TxHandle}, everything else takes the autocommit path.
     *
     * It is deliberately NOT folded into {@see dispatchAutocommit} — that method's transparent
     * reconnect + re-issue loop is precisely what an in-transaction statement must bypass (a
     * mid-transaction reconnect would void the `tx_id` silently, and re-issuing a statement whose
     * transaction is already dead is meaningless). {@see TxHandle::runForConnection} is the bare
     * send-and-classify the closure form has always used, reused verbatim rather than duplicated.
     *
     * Being ONE fork rather than five copy-pasted ones is the point: a statement method that missed
     * the delegation would silently run OUTSIDE the caller's open transaction, which is unobservable
     * until a rollback fails to undo it.
     *
     * @param list<mixed> $params
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}
     */
    private function dispatch(string $sql, array $params, bool $readonly, int $fetch): array
    {
        // CLEAR FIRST, on the way IN — this is what makes `lastInsertId()`'s "never a stale key"
        // invariant true on the FAILURE path, and it is a deliberate divergence from PDO. See
        // {@see lastInsertId} for the whole argument. Both statement paths below overwrite it on
        // success, so the only state this can leave behind is `null`, which is the honest answer for
        // a statement that errored, was cancelled, or whose fate is Indeterminate.
        $this->lastInsertId = null;

        if ($this->tx === null) {
            return $this->dispatchAutocommit($sql, $params, $readonly, $fetch);
        }
        $res = $this->tx->runForConnection($sql, $params, $readonly, $fetch);
        // Propagate the generated key to the connection level (M1-S8a Task 9): a driver's
        // `lastInsertId()` is read off the Connection, and nearly every real INSERT happens inside a
        // transaction. The closure form deliberately does NOT propagate — see {@see lastInsertId}.
        $this->lastInsertId = $res['last_insert_id'];
        return $res;
    }

    /**
     * Send one autocommit EXEC and decode it, transparently reconnecting + re-issuing a Retryable
     * READ (bounded by the policy). A lost WRITE / Indeterminate / exhausted read propagates.
     *
     * @param list<mixed> $params
     * @return array{cols: list<string>, rows: list<list<mixed>>, affected: int, last_insert_id: int|string|null}
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
                $decoded = $this->codec->decode($outcome);
                // Record the generated key BEFORE returning: this is the ONLY moment it exists.
                // The "never a stale key" half of the contract is enforced by {@see dispatch}, which
                // cleared the field on the way in — including for the statements that never reach
                // this line because they threw.
                $this->lastInsertId = $decoded['last_insert_id'];
                return $decoded;
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
