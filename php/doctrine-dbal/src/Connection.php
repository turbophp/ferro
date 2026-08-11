<?php // /php/doctrine-dbal/src/Connection.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Exception\NoIdentityValue;
use Doctrine\DBAL\Driver\Result as ResultInterface;
use Doctrine\DBAL\Driver\Statement as StatementInterface;
use Ferro\Client\Connection as FerroConnection;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\RetryableException;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\Exception\ServerVersionUnavailable;
use Ferro\DBAL\Exception\UnsupportedStatement;
// Aliased: `FerroConnection` is already taken above by the CLIENT connection this class wraps.
// The plan's Task 13 snippet imports the wrapper under its bare name, which would collide.
use Ferro\DBAL\Wrapper\FerroConnection as FerroWrapper;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Isolation;

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

    /**
     * A WEAK reference, and that is the entire abandonment design.
     *
     * A STRONG reference here would be the only thing keeping an abandoned stream alive:
     * `Doctrine\DBAL\Connection::iterateAssociative()` returns
     * `$this->executeQuery(…)->iterateAssociative()`, so the only other reference to the
     * `Doctrine\DBAL\Result` — and through it to this driver `Result` — is the returned Generator's
     * bound `$this`. `Doctrine\DBAL\Result` has no `__destruct` and DBAL never calls the driver's
     * `free()` (hazard 80). So with a strong reference the driver can NEVER tell "the caller
     * abandoned this" from "the caller may still fetch from this", and both end up draining the
     * whole remainder on the next statement.
     *
     * Weakly: when the consumer stops iterating, the Generator dies, the DBAL `Result` dies, the
     * driver `Result` becomes unreferenced, PHP frees it by refcount THERE AND THEN, and its
     * `__destruct` sends the `CANCEL`. `get()` then returns null and {@see settleOpenStream} has
     * nothing to do. When the consumer is still iterating, `get()` returns the live result and it
     * materialises.
     *
     * **MEASURED LIMIT (PHP 8.4.18 + doctrine/dbal 4.4.4).** That is true when the generator is a
     * TEMPORARY — `foreach ($conn->iterateAssociative($sql) as $row) { … break; }`, the canonical
     * idiom — and NOT when it was bound first (`$it = $conn->iterateAssociative($sql); foreach ($it
     * …) { break; }`), where `$it` keeps the result alive until it leaves scope or is `unset()`. A
     * live reference is indistinguishable from a caller who may still fetch, so that shape
     * materialises the remainder instead. It is a PHP refcount fact rather than a design choice;
     * `StreamingLiveTest` pins BOTH shapes so nobody reads this as "abandonment always cancels".
     *
     * @var ?\WeakReference<Result> PHPStan level 9 will not infer the generic parameter.
     */
    private ?\WeakReference $openStream = null;

    /** @see settledRowCount */
    private int $settledRows = 0;

    /** @see abandonDrainedRowCount */
    private int $abandonDrainedRows = 0;

    /**
     * How many rows an ABANDONED streamed result has had to drain because cancelling it would have
     * destroyed an open transaction ({@see \Ferro\DBAL\Result::releaseStream}).
     *
     * **0 outside a transaction** — there the abandonment is a `CANCEL` and nothing is transferred —
     * and non-zero inside one, where it is the remainder the caller never read. It is the counterpart
     * to {@see settledRowCount} (which counts the OTHER drain, the one an interleaved statement
     * forces), and the two must not be conflated: they are different paths with different costs, and
     * a guard that could not tell them apart would bless either one silently replacing the other.
     *
     * It exists because the alternative — asserting only that the transaction survived — CANNOT FAIL
     * below one `StreamBatch::DEFAULT` frame (1024 rows / 256 KiB): at that size the producer has
     * already finished, so even the destructive `CANCEL` lands on an idle backend and the transaction
     * lives. That is exactly the size a test fixture reaches for.
     */
    public function abandonDrainedRowCount(): int
    {
        return $this->abandonDrainedRows;
    }

    /**
     * How many rows this connection has had to drain because a streamed result was still open when
     * another statement was issued.
     *
     * **0 for pure iteration and 0 for a properly abandoned iteration**; non-zero only for the
     * interleave idiom, where it is the size of the remainder that had to be buffered. It is what
     * makes the two abandonment cases observable from a test — and it answers a real operator
     * question, which is why it is a public accessor rather than test scaffolding.
     */
    public function settledRowCount(): int
    {
        return $this->settledRows;
    }

    /**
     * Bring any open streamed `Result` into memory before this connection issues anything else.
     *
     * The Ferro session is strictly single-in-flight: `Session::assertNoOpenStream()` throws on any
     * request while a stream is open. Rather than surface that as a `ProtocolException` — which
     * would break `foreach ($conn->iterateAssociative(…)) { $conn->executeStatement(…); }`, an idiom
     * every Doctrine codebase uses — the open result drains its remainder here. Pure iteration
     * still never buffers; interleaving degrades to what PDO does unconditionally.
     *
     * A result whose caller is GONE is not drained: it has already cancelled itself on destruction.
     */
    private function settleOpenStream(): void
    {
        $ref = $this->openStream;
        $this->openStream = null;
        $open = $ref?->get();
        if ($open instanceof Result) {
            $this->settledRows += $open->materialize();
        }
    }

    private ?Isolation $pendingIsolation = null;

    /**
     * The isolation level the NEXT {@see beginTransaction} will carry, set by
     * {@see \Ferro\DBAL\Wrapper\FerroConnection::setTransactionIsolation}. Null means the pool
     * default.
     *
     * It is sticky, matching Doctrine's own semantics: `setTransactionIsolation()` applies to every
     * subsequent transaction, not just the next one.
     */
    public function setIsolation(?Isolation $isolation): void
    {
        $this->pendingIsolation = $isolation;
    }

    /**
     * The refusal, raised from every statement entry point.
     *
     * Refused, not ignored and not rewritten. Left alone this statement SUCCEEDS and does nothing:
     * it lands on an arbitrary pooled connection, taints it, and hygiene wipes the level before the
     * next BEGIN — so the application asks for SERIALIZABLE and silently gets the pool default
     * (SPEC §22.2 (s), which also records that the obvious "did the next tenant inherit it" test
     * cannot fail, because hygiene masks it either way). The message names the one-line
     * configuration fix.
     *
     * It sits in a helper called from THREE places rather than inline in two, because
     * {@see query}'s PostgreSQL branch reaches the wire through `streamRaw()` and never touches
     * {@see runPrepared} — so a guard on `exec()`/`runPrepared()` alone leaves
     * `executeQuery('SET SESSION CHARACTERISTICS AS …')` unrefused on exactly one of the two
     * families. Measured, not assumed: `IsolationLiveTest`'s
     * `testTheRefusalAlsoCoversTheZeroParameterQueryPath` is RED against the two-site form.
     */
    private function refuseIsolationStatement(string $sql): void
    {
        if (FerroWrapper::isIsolationStatement($sql)) {
            throw UnsupportedStatement::isolation($sql);
        }
    }

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

    /**
     * The ZERO-PARAMETER read path, and the ONE place this driver streams.
     * `Doctrine\DBAL\Connection::executeQuery()` calls it directly when there are no parameters and
     * — crucially — never asks the result for a row count, so nothing here can be made wrong by a
     * terminal that carries no `affected`.
     *
     * **Why the prepared path does not stream.** `executeStatement()` with parameters is
     * `$stmt->execute()->rowCount()`, and a streamed request's terminal carries no `affected` field
     * (the HEAD/DATA/END producer has none), so streaming there would make every parameterized
     * write return 0 — a silently wrong value, which is worse than buffering. Adding `affected` to
     * the stream terminal is a `/proto` change (registry + golden vectors + both codecs) and is
     * DEFERRED, not smuggled in here.
     *
     * **Why MySQL buffers.** `PoolBackend::supports_row_streaming()` is false for MySQL/MariaDB
     * (SPEC §22.2 (n), controller decision D-S8b-2), where `streamRaw()` would come back as a clean
     * `Unsupported` — paying a round trip to discover that on every query is not worth it when the
     * pool kind is already known from `HELLO_ACK`.
     *
     * The returned result is the CALLER's alone; this connection keeps only a `\WeakReference`
     * ({@see $openStream}). A caller that discards it — `$conn->query($sql);` in statement position —
     * destroys it at the end of that statement and the stream is abandoned there, which is correct
     * and is what the buffered path already does with its rows.
     *
     * **Streaming is safe INSIDE a transaction only because abandonment is told what it is inside.**
     * The two closures below are that: abandoning a stream normally sends a `CANCEL`, which `ferrod`
     * turns into a real backend `CancelRequest`, which aborts the statement, which puts an open
     * `BEGIN` block into PG's ABORTED state — so the engine rolls the transaction back and tombstones
     * it, and the caller's earlier writes are lost (`review/wb-xslice.md`, BLOCKER: three ordinary
     * lines, a silently discarded `INSERT`). Inside a transaction the result drains to its terminal
     * instead. `inTransaction()` is asked at ABANDONMENT time rather than captured here: it is the
     * state at the moment the decision is made, and it is free to ask.
     */
    public function query(string $sql): ResultInterface
    {
        $this->settleOpenStream();
        $this->refuseIsolationStatement($sql);
        $this->refuseWhileNestingIsDesynced($sql);
        if ($this->poolKind !== PlatformVersion::KIND_POSTGRES) {
            return $this->runPrepared($sql, []);
        }
        try {
            $stream = $this->ferro->streamRaw($sql, [], $this->readonly);
        } catch (FerroException $e) {
            throw $this->statementException($e);
        }
        $result = Result::streamed(
            $stream,
            fn (): bool => $this->ferro->inTransaction(),
            function (int $rows): void { $this->abandonDrainedRows += $rows; },
        );
        // WEAK on purpose — see the field's docblock. The caller's own reference (via
        // `Doctrine\DBAL\Result`) is the one that decides whether this result is still alive.
        $this->openStream = \WeakReference::create($result);
        return $result;
    }

    /**
     * The parameterless statement path — and, measured rather than assumed, **the one Doctrine's
     * savepoints actually take**: `Doctrine\DBAL\Connection::executeStatement()` calls the driver's
     * `exec()` whenever `count($params) === 0`, and `createSavepoint()`/`rollbackSavepoint()` pass
     * no parameters. So the invariant documented on {@see runPrepared} is load-bearing HERE first.
     *
     * **It is also the ONE write intent the DBAL 4 SPI makes visible**, which is why
     * {@see refuseAutocommitWriteEntryPoint} lives here — see that method for the exact scope of
     * what `driverOptions.readonly` does and does not refuse.
     */
    public function exec(string $sql): int
    {
        $this->settleOpenStream();
        $this->refuseIsolationStatement($sql);
        $this->refuseWhileNestingIsDesynced($sql);
        $this->refuseAutocommitWriteEntryPoint($sql);
        try {
            return $this->ferro->fetchRaw($sql, [], $this->readonly, false)['affected'];
        } catch (FerroException $e) {
            throw $this->statementException($e);
        }
    }

    /**
     * `driverOptions.readonly`, ENFORCED on the autocommit path — as far as the SPI allows, and no
     * further than that.
     *
     * **The defect this closes.** Inside a transaction the engine really does enforce the flag
     * (`BEGIN READ ONLY` / `START TRANSACTION READ ONLY`, and the server then refuses the write with
     * `25006`). On the autocommit path NOTHING enforced it: MEASURED live on PG 17, an `INSERT`
     * through a `['readonly' => true]` connection returned `affected = 1` and the row was there. So
     * an operator who validated the flag the way anyone would — by trying a write INSIDE a
     * transaction, watching the server refuse it, and concluding "enforced" — had a connection on
     * which `executeStatement('UPDATE …')` ran and committed, while every §19.3 fate on it was being
     * computed from the claim that it only reads.
     *
     * **Why THIS entry point and not "all writes".** Charter rule 6 forbids inferring read-vs-write
     * from SQL text, and the DBAL 4 SPI carries no read/write signal — `executeQuery('INSERT …
     * RETURNING id')` is indistinguishable from a `SELECT`. What the SPI DOES carry is which method
     * the application called: `Doctrine\DBAL\Connection::executeStatement()` is the write API (it
     * returns affected rows and is never used for a read), and with no parameters it lands exactly
     * here. That is a fact about the CALLER's declared intent, not about the SQL, so refusing it is
     * charter-legal and free.
     *
     * **The limit, stated so this cannot be read as completeness.** A PARAMETERIZED
     * `executeStatement('UPDATE … WHERE id = ?', [1])` reaches {@see runPrepared}, which
     * `executeQuery()` reaches too — the driver cannot tell them apart and does NOT refuse there,
     * because refusing would break every parameterized READ. Those writes still execute. What
     * protects at-most-once for them is the second half of this fix,
     * {@see statementException}: the readonly declaration can no longer turn a LOST autocommit
     * statement into a "safe to retry" verdict. Real enforcement for the remaining shapes needs the
     * engine to run a `readonly` autocommit statement inside a server-enforced read-only transaction
     * (no client round trips, but a `ferrod` change) — recorded as a follow-up, not smuggled in here.
     *
     * Inside a transaction this passes through untouched: the server is already enforcing, and
     * Doctrine's own `SAVEPOINT`/`RELEASE`/`ROLLBACK TO` statements arrive at `exec()` (they carry no
     * parameters). Refusing them would break nested transactions on every readonly connection.
     */
    private function refuseAutocommitWriteEntryPoint(string $sql): void
    {
        if ($this->readonly && !$this->ferro->inTransaction()) {
            throw UnsupportedStatement::readonlyAutocommitWrite($sql);
        }
    }

    /**
     * The ONE place a statement's `FerroException` becomes a driver exception — and the ONE place the
     * unverifiable half of `driverOptions.readonly` is taken back.
     *
     * **The cell.** `fate.rs` reads `readonly` twice. In the 57014 override it buys the honest
     * `Cancelled{NonRetryable}` answer instead of `WriteUnconfirmed{Indeterminate}` — that is the
     * §22.2 (ac) relief, it is NOT retryable, and it is left exactly as it is. In the
     * `PoolError::ConnectionLost` arm it does something else entirely: `sent && !readonly && !in_tx`
     * is `Indeterminate`, and the same event with `readonly = true` is
     * `ConnectionLost{Retryable}` — which this driver's converter turns into
     * `RetryableDriverException implements Doctrine\DBAL\Exception\RetryableException`, i.e. the
     * marker Symfony Messenger and every hand-rolled retry loop key on. On a connection wrongly
     * marked readonly that is an invitation to replay a write that may already have landed: the
     * at-most-once violation the branch exists to prevent, reachable through one boolean in config.
     *
     * **The rule.** For an AUTOCOMMIT statement on a `readonly` connection, a `Retryable` verdict
     * whose code is `ERR_CONNECTION_LOST` — the one cell whose retryability is contingent on the
     * declaration — is re-minted as §19.3 `Indeterminate`, which is precisely what the same event
     * yields on the default write-declared connection. Nothing else is touched: a pool checkout
     * timeout, a deadlock, a serialization failure and a cancel are all classified without reference
     * to `readonly`, so they keep their verdicts.
     *
     * **Two deliberate limits.**
     *  * It is scoped to STATEMENTS. `beginTransaction()`/`commit()`/`rollBack()` keep
     *    {@see \Ferro\DBAL\Exception\DriverException::fromFerro} — a lost BEGIN opened nothing and a
     *    lost ROLLBACK changes nothing whatever the connection declared, and `fate.rs` treats those
     *    control boundaries as `in_tx: false` for the same reason. Re-minting there would destroy
     *    true information.
     *  * The engine emits ONE `ERR_CONNECTION_LOST` for two situations — "never transmitted" (a
     *    known did-not-apply) and "transmitted but declared a read" — and the wire does not separate
     *    them. So on a readonly connection a checkout-time loss is over-reported as `Indeterminate`.
     *    That is the safe direction of an unavoidable approximation, and it is the reason the fix is
     *    an upgrade of the FATE rather than a silent strip of the retry marker: an application that
     *    audits indeterminate writes must still see this one.
     */
    private function statementException(FerroException $e): DriverException
    {
        if ($this->readonly
            && !$this->ferro->inTransaction()
            && $e instanceof RetryableException
            && $e->errorCode() === C::ERR_CONNECTION_LOST
        ) {
            return DriverException::unbackedReadonlyLoss($e);
        }
        return DriverException::fromFerro($e);
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
        $this->settleOpenStream();
        $this->refuseIsolationStatement($sql);
        $this->refuseWhileNestingIsDesynced($sql);
        try {
            $raw = $this->ferro->fetchRaw($sql, $params, $this->readonly, true);
        } catch (FerroException $e) {
            throw $this->statementException($e);
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
     * DIFFERENT connection and returns a silently wrong key. The message names both working answers
     * (`INSERT … RETURNING`, or the ORM's SEQUENCE identity strategy — D-S8b-5).
     *
     * The thrown class is `Doctrine\DBAL\Driver\Exception\NoIdentityValue` — the SPI's own signal,
     * which all six bundled drivers (PDO, PgSQL, Mysqli, SQLite3, SQLSrv, IBMDB2) throw here. S8b
     * originally shipped a Ferro-owned class instead, and the upstream acceptance run measured the
     * cost: `tests/Functional/WriteTest::testLastInsertIdNewConnection` asserts on the class of
     * `getPrevious()`. Nothing in DBAL's `src/` CATCHES it, so the application-visible behaviour is
     * identical either way — but there is no reason to invent a second class for a signal the SPI
     * already defines, and the message (the part that is actually ours) is preserved verbatim.
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
            throw new NoIdentityValue($this->poolKind === PlatformVersion::KIND_POSTGRES
                ? 'Ferro: PostgreSQL reports no generated key on the wire, and Ferro will not '
                    . 'emulate lastInsertId() with a follow-up query — on a transaction-mode pool '
                    . 'that runs on a different connection and returns a wrong key. Use '
                    . '`INSERT … RETURNING id`, or configure Doctrine ORM to use the SEQUENCE '
                    . 'identity strategy on PostgreSQL.'
                : 'Ferro: the last statement reported no generated key. lastInsertId() reflects the '
                    . 'MOST RECENT statement and is cleared by a statement that fails, so read it '
                    . 'immediately after a successful INSERT into an AUTO_INCREMENT column.');
        }
        return $id;
    }

    /**
     * True while `Doctrine\DBAL\Connection`'s nesting counter is KNOWN to be out of step with this
     * connection: a `beginTransaction()` was REJECTED, and DBAL does not undo the increment it made
     * before calling us.
     *
     * **Why this state exists at all, i.e. why a PDO driver never needs it.**
     * `Doctrine\DBAL\Connection::beginTransaction()` is `++$this->transactionNestingLevel;` and only
     * THEN `$connection->beginTransaction()`, with no decrement on failure
     * (`vendor/doctrine/dbal/src/Connection.php:1047-1062`). Under PDO that is nearly harmless,
     * because a failed BEGIN there means a dead connection. Ferro rejects a BEGIN **retryably on a
     * perfectly healthy client session** — `PoolTimeout` under load, a `ConnectionLost` that was
     * never sent, a replica that is unavailable — so the shape is ROUTINE here. After it, DBAL
     * reports `isTransactionActive() === true` at nesting 1 while this connection has no transaction
     * at all, and the review MEASURED what follows: ordinary statements run AUTOCOMMIT (a durability
     * lie — the caller believes they are transactional), the next `beginTransaction()` becomes
     * `SAVEPOINT DOCTRINE_2` which the engine correctly refuses ("savepoint statement outside a
     * transaction"), naming a savepoint the application never wrote, and the counter never comes
     * back down. A worker was wedged for the rest of its life.
     *
     * The counter itself can only be repaired ABOVE the SPI, which is what
     * {@see \Ferro\DBAL\Wrapper\ResyncsNestingOnARejectedBegin} does. What THIS flag does is make the
     * un-wrapped case honest and recoverable rather than silent and terminal:
     * {@see refuseWhileNestingIsDesynced} refuses statements in the window (so nothing can be written
     * outside the transaction the caller believes in — which is also what makes the rollBack below
     * TRUE rather than a lie), and {@see rollBack} is the resync.
     */
    private bool $rejectedBegin = false;

    /**
     * Refuse a statement while the nesting counter is desynced ({@see $rejectedBegin}).
     *
     * Two things are wrong with running it, and they are different: it would execute in AUTOCOMMIT
     * while the caller believes a transaction is open (durable writes that no rollback can undo), and
     * if it is one of Doctrine's savepoint statements it would fail against the engine's own guard
     * with a message about a savepoint the application never wrote. The refusal names the real cause
     * and both ways out.
     *
     * `inTransaction()` is asked every time rather than trusted from the flag: the flag only says a
     * BEGIN was rejected, and a later successful one clears it.
     */
    private function refuseWhileNestingIsDesynced(string $sql): void
    {
        if ($this->rejectedBegin && !$this->ferro->inTransaction()) {
            throw UnsupportedStatement::afterARejectedBegin($sql);
        }
    }

    public function beginTransaction(): void
    {
        $this->settleOpenStream();
        try {
            $this->ferro->begin($this->readonly, $this->pendingIsolation);
        } catch (FerroException $e) {
            // DBAL has ALREADY counted this transaction as open and will not uncount it. Record
            // that, so the statements that follow are refused instead of silently running in
            // autocommit — see {@see $rejectedBegin}.
            $this->rejectedBegin = true;
            throw DriverException::fromFerro($e);
        }
        $this->rejectedBegin = false;
    }

    public function commit(): void
    {
        $this->settleOpenStream();
        if ($this->rejectedBegin && !$this->ferro->inTransaction()) {
            // DBAL's own `finally` decrements its counter around this call, so clearing the flag
            // here leaves both sides consistent — but the caller must NOT be told this committed:
            // the transaction it is committing was never opened.
            $this->rejectedBegin = false;
            throw DriverException::local(
                'Ferro: commit() with no open transaction — this connection\'s beginTransaction() '
                . 'was REJECTED and Doctrine kept counting the transaction as open. Nothing was '
                . 'committed because nothing ran: every statement since was refused.',
            );
        }
        try {
            $this->ferro->commit();
        } catch (FerroException $e) {
            throw DriverException::fromFerro($e);
        }
    }

    /**
     * **THE RESYNC.** A `rollBack()` while the nesting counter is desynced
     * ({@see $rejectedBegin}) is a no-op that clears the flag, and that is honest rather than
     * convenient: the transaction being rolled back was never opened, and every statement since was
     * REFUSED ({@see refuseWhileNestingIsDesynced}), so there is provably nothing to undo. It is also
     * the only recovery an un-wrapped `Doctrine\DBAL\Connection` has — DBAL zeroes its own counter
     * before calling us here (`rollBack()` at level 1 sets `transactionNestingLevel = 0` FIRST), so
     * after this call both sides agree again and the connection is usable.
     *
     * Without it the client raises `InvalidTransactionStateException` ("rollBack() with no open
     * transaction") and the connection stays wedged: the counter is only reset on the path that just
     * threw.
     */
    public function rollBack(): void
    {
        $this->settleOpenStream();
        if ($this->rejectedBegin && !$this->ferro->inTransaction()) {
            $this->rejectedBegin = false;
            return;
        }
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

        // The resolution below reaches the WIRE, so an open streamed result has to be settled
        // first — the session is single-in-flight. In practice DBAL resolves the platform before
        // any statement runs and the answer is cached from then on, but "in practice" is not an
        // invariant and this method is public.
        $this->settleOpenStream();
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
