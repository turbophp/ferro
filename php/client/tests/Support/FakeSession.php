<?php // /php/client/tests/Support/FakeSession.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\SessionInterface;
use Ferro\Protocol\BeginResponse;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;

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
 */
final class FakeSession implements SessionInterface
{
    /** @var list<Outcome|\Throwable> */
    private array $script = [];
    private int $pos = 0;

    /** @var list<array{0:int,1:int,2:string}> every (service, method, payload) sent, in order */
    public array $sent = [];

    /** @var array{0:int,1:int}|null */
    private ?array $lastInFlight = null;

    public bool $closed = false;
    private bool $dead = false;

    public function __construct(private readonly int|string $epoch = 1) {}

    /** Queue one step (an Outcome to return, or a Throwable to raise). Fluent. */
    public function push(Outcome|\Throwable $step): self
    {
        $this->script[] = $step;
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
        $step = $this->script[$this->pos++];
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

    public function close(): void { $this->closed = true; }

    /** Count of sends recorded so far. */
    public function sendCount(): int { return count($this->sent); }

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
