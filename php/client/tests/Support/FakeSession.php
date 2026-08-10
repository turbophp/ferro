<?php // /php/client/tests/Support/FakeSession.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\SessionInterface;
use Ferro\Client\StreamingSessionInterface;
use Ferro\Protocol\BeginResponse;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\PoolInfo;

/**
 * A scripted {@see SessionInterface} for the Task-4 unit tests: no socket, no ferrod. Each queued
 * step is either an {@see Outcome} to return or a {@see \Throwable} to raise on the next
 * {@see sendRequest}. Every send is recorded (`$sent` as `[service, method, payload]`) and
 * `lastInFlight()` reports the last `(service, method)` — so tests can assert the ROLLBACK/COMMIT
 * frame was written and that the §19.3 carve-out reads the right in-flight op.
 *
 * Once a step throws a {@see ConnectionLostException}/{@see TransportException} the session goes
 * DEAD: every subsequent send throws too (modelling a real broken socket), so a best-effort ROLLBACK
 * after a mid-tx loss naturally fails and is swallowed. `bootEpoch()` is fixed at construction (the
 * reconnect loop reads it), opaque `int|string`.
 *
 * It also implements {@see StreamingSessionInterface} (M1-S8a Task 9) — but ONLY the
 * "terminal-before-any-HEAD" stream shape, which is all the imperative-transaction tests need: they
 * assert what the streamed REQUEST carried (its `tx_id`, its `fetch` mode, which session it went out
 * on), not how rows come back. The multi-frame HEAD/DATA path is exercised by
 * `ConnectionStreamTest` against a real {@see \Ferro\Client\Session} over a {@see FakeTransport};
 * anything else queued here fails loudly rather than pretending.
 */
final class FakeSession implements SessionInterface, StreamingSessionInterface
{
    /**
     * Each step is the reply (an {@see Outcome} to return, or a {@see \Throwable} to raise) plus an
     * OPTIONAL `(service, method)` the send is expected to carry — so a fixture named
     * `thenThrowOnCommit()` cannot silently fire on some other frame.
     *
     * @var list<array{0:Outcome|\Throwable,1:array{0:int,1:int}|null}>
     */
    private array $script = [];
    private int $pos = 0;

    /** @var list<array{0:int,1:int,2:string}> every (service, method, payload) sent, in order */
    public array $sent = [];

    /** @var array{0:int,1:int}|null */
    private ?array $lastInFlight = null;

    public bool $closed = false;
    private bool $dead = false;

    /** @var list<Outcome> queued immediate stream terminals, one per {@see openStream} */
    private array $streamScript = [];
    private int $streamPos = 0;
    private int $streamRid = 100;

    public function __construct(private readonly int|string $epoch = 1) {}

    /**
     * Queue one step (an Outcome to return, or a Throwable to raise). Fluent.
     *
     * @param array{0:int,1:int}|null $expect the `(service, method)` this step must receive; a
     *   mismatch is a LOUD LogicException rather than a step quietly firing on the wrong frame.
     */
    public function push(Outcome|\Throwable $step, ?array $expect = null): self
    {
        $this->script[] = [$step, $expect];
        return $this;
    }

    public function sendRequest(int $service, int $method, string $payload): Outcome
    {
        if ($this->dead) {
            throw new ConnectionLostException('fake session is dead (a prior send lost the connection)');
        }
        $this->sent[] = [$service, $method, $payload];
        $this->lastInFlight = [$service, $method];

        if ($this->pos >= count($this->script)) {
            throw new \LogicException(sprintf(
                'FakeSession: no scripted step for send #%d (service=%d method=%d)',
                $this->pos + 1,
                $service,
                $method,
            ));
        }
        [$step, $expect] = $this->script[$this->pos++];
        if ($expect !== null && $expect !== [$service, $method]) {
            throw new \LogicException(sprintf(
                'FakeSession: step #%d expected (service=%d, method=%d) but got (service=%d, method=%d)',
                $this->pos,
                $expect[0],
                $expect[1],
                $service,
                $method,
            ));
        }
        if ($step instanceof \Throwable) {
            if ($step instanceof ConnectionLostException || $step instanceof TransportException) {
                $this->dead = true;
            }
            throw $step;
        }
        return $step;
    }

    public function bootEpoch(): int|string { return $this->epoch; }

    /** @return array{0:int,1:int}|null */
    public function lastInFlight(): ?array { return $this->lastInFlight; }

    /**
     * What this fake's `HELLO_ACK` "advertised". Public so a test states the pool topology it is
     * asserting about in one line, next to the assertion, instead of through a builder.
     *
     * @var list<PoolInfo>
     */
    public array $poolInfo = [];

    /** @return list<PoolInfo> */
    public function poolInfo(): array { return $this->poolInfo; }

    public function close(): void { $this->closed = true; }

    /** Count of sends recorded so far. */
    public function sendCount(): int { return count($this->sent); }

    /**
     * The most recent send, named rather than positional — so a test can decode the PAYLOAD that
     * actually went on the wire instead of trusting a getter on the object under test.
     *
     * @return array{service:int, method:int, payload:string}
     */
    public function lastRequest(): array
    {
        $last = $this->sent[count($this->sent) - 1] ?? throw new \LogicException('FakeSession: nothing sent yet');
        return ['service' => $last[0], 'method' => $last[1], 'payload' => $last[2]];
    }

    // ---- named constructors for the imperative-transaction tests (M1-S8a Task 9) ----------------

    /** A session whose FIRST send is a `TX/BEGIN` answered with `tx_id = $txId`. */
    public static function withTxBegin(int $txId): self
    {
        return (new self())->push(self::beginOk($txId), [C::SERVICE_TX, C::METHOD_TX_BEGIN]);
    }

    /** A session whose FIRST send is a `SQL/EXEC` answered Ok. */
    public static function withExecOk(?int $lastInsertId = null): self
    {
        return (new self())->thenExecOk($lastInsertId);
    }

    /** A session whose `TX/BEGIN` dies on the wire (what {@see \Ferro\Client\Transport} raises). */
    public static function thatThrowsTransportOnBegin(): self
    {
        return (new self())->push(
            new TransportException('link died writing BEGIN'),
            [C::SERVICE_TX, C::METHOD_TX_BEGIN],
        );
    }

    /** Queue an Ok `SQL/EXEC` reply carrying `$lastInsertId` (a bare affected-rows terminal). */
    public function thenExecOk(?int $lastInsertId = null): self
    {
        return $this->push(
            self::execOk([
                'cols' => [],
                'rows' => [],
                'affected' => 1,
                'last_insert_id' => $lastInsertId === null
                    ? null
                    : ['tag' => C::TAG_I64, 'data' => $lastInsertId],
                'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
            ]),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
    }

    /** Queue an Ok `SQL/EXEC` reply for a READ that returns no rows and reports no generated key. */
    public function thenEmptyRows(): self
    {
        return $this->push(
            self::execOk([
                'cols' => [['name' => 'v', 'tag' => C::TAG_I64]],
                'rows' => [],
                'affected' => 0,
                'last_insert_id' => null,
                'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
            ]),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
    }

    /**
     * Queue a stream whose very first reply is the terminal `END` — the real "known fate decided
     * before any HEAD/DATA went out" shape (`ferrod` decides it pre-checkout). Enough to pin what
     * the streamed REQUEST carried; see the class docblock for what this fixture does NOT model.
     */
    public function thenStreamEnd(): self
    {
        $this->streamScript[] = Outcome::ok('');
        return $this;
    }

    /** Queue a `TX/COMMIT` that dies on the wire. */
    public function thenThrowOnCommit(): self
    {
        return $this->push(
            new TransportException('link died writing COMMIT'),
            [C::SERVICE_TX, C::METHOD_TX_COMMIT],
        );
    }

    /** Queue a `TX/ROLLBACK` that dies on the wire. */
    public function thenThrowOnRollback(): self
    {
        return $this->push(
            new TransportException('link died writing ROLLBACK'),
            [C::SERVICE_TX, C::METHOD_TX_ROLLBACK],
        );
    }

    /** Queue a `TX/COMMIT` (or ROLLBACK) ack. */
    public function thenControlOk(): self
    {
        return $this->push(self::controlOk());
    }

    // ---- StreamingSessionInterface (immediate-terminal shape only) -------------------------------

    /** @return array{type:'head', requestId:int, cols:list<array{name:string,tag:int}>}|array{type:'end', requestId:int, outcome:Outcome} */
    public function openStream(int $service, int $method, string $payload): array
    {
        if ($this->dead) {
            throw new ConnectionLostException('fake session is dead (a prior send lost the connection)');
        }
        $this->sent[] = [$service, $method, $payload];
        $this->lastInFlight = [$service, $method];
        if ($this->streamPos >= count($this->streamScript)) {
            throw new \LogicException('FakeSession: no scripted stream for openStream()');
        }
        return [
            'type' => 'end',
            'requestId' => ++$this->streamRid,
            'outcome' => $this->streamScript[$this->streamPos++],
        ];
    }

    /** @return array{type:'data', rows:list<list<array{tag:int,data:mixed}>>, bytes:int}|array{type:'end', outcome:Outcome} */
    public function readStreamFrame(int $requestId): array
    {
        // Unreachable for the immediate-terminal shape this fixture models: `Connection::stream`
        // returns at the `openStream` terminal and never asks for another frame. Loud on purpose —
        // a silent empty frame here would let a broken stream path look green.
        throw new \LogicException('FakeSession models only the immediate-terminal stream shape');
    }

    public function sendWindowUpdate(int $requestId, int $frames, int $bytes): void
    {
        throw new \LogicException('FakeSession models only the immediate-terminal stream shape');
    }

    public function sendCancel(int $requestId): void
    {
        throw new \LogicException('FakeSession models only the immediate-terminal stream shape');
    }

    public function abandonStream(int $requestId): void
    {
        // A no-op: the immediate terminal already closed the stream, so there is nothing to drain.
        // (`Connection::stream` calls this only when it did NOT reach a terminal.)
    }

    // ---- Outcome builders (kept here so tests stay concise) -------------------------------------

    public static function beginOk(int $txId): Outcome
    {
        return Outcome::ok(BeginResponse::encode(['tx_id' => $txId], PackerFactory::forEncode()));
    }

    /** A control-op (COMMIT/ROLLBACK/SAVEPOINT) ack: an empty Outcome::Ok body. */
    public static function controlOk(): Outcome
    {
        return Outcome::ok('');
    }

    /** @param array<string,mixed> $execOk */
    public static function execOk(array $execOk): Outcome
    {
        return Outcome::ok(ExecOk::encode($execOk, PackerFactory::forEncode()));
    }

    public static function errorOutcome(ErrorPayload $ep): Outcome
    {
        return Outcome::error($ep);
    }

    /**
     * A one-row, one-column i64 ExecOk (e.g. `SELECT 1`), for read-path tests.
     *
     * @param int $value the single cell value
     */
    public static function scalarRow(int $value): Outcome
    {
        return self::execOk([
            'cols' => [['name' => 'n', 'tag' => \Ferro\Protocol\Generated\Constants::TAG_I64]],
            'rows' => [[['tag' => \Ferro\Protocol\Generated\Constants::TAG_I64, 'data' => $value]]],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ]);
    }
}
