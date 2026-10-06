<?php // /php/client/tests/Client/CopyRunnerTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\CopyRunner;
use Ferro\Client\CopySessionInterface;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\ExecCodec;
use Ferro\Client\FateClassifier;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Pg\Copy;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\Outcome;
use PHPUnit\Framework\TestCase;

/**
 * {@see CopyRunner} against a scripted session (M3-D4): the client's half of the COPY_IN credit
 * contract (never a byte beyond the grant, chunks split to fit, grants read only when out), the
 * abandonment rules, and the §19.3 fate of a COPY lost before and after its end-of-data.
 */
final class CopyRunnerTest extends TestCase
{
    private static function codec(): ExecCodec
    {
        return new ExecCodec(new M1ValuePolicy(new TypePolicyOptions()), new PlanCache(), new PurePacker(), new PurePacker());
    }

    private static function runner(bool $inTx = false): CopyRunner
    {
        return new CopyRunner(new FateClassifier(), self::codec(), $inTx, static fn (): bool => false);
    }

    public static function okOutcome(int $affected): Outcome
    {
        return Outcome::ok(ExecOk::encode([
            'cols' => [], 'rows' => [], 'affected' => $affected, 'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
        ], new PurePacker()));
    }

    public function testNeverSendsBeyondTheGrantAndSplitsChunksToFit(): void
    {
        $s = new ScriptedCopySession(grant: [2, 10]);
        $s->events = [
            ['type' => 'grant', 'frames' => 1, 'bytes' => 4],
            ['type' => 'grant', 'frames' => 5, 'bytes' => 100],
            ['type' => 'end', 'outcome' => self::okOutcome(3)],
        ];
        $n = self::runner()->in($s, 'payload', ['abcdefgh', 'ijklmnop']);
        $this->assertSame(3, $n);
        $this->assertSame('abcdefghijklmnop', implode('', $s->sent));
        // Credit replayed: never negative in frames or bytes at any send.
        $frames = 2;
        $bytes = 10;
        $grants = [[1, 4], [5, 100]];
        foreach ($s->sent as $chunk) {
            while ($frames === 0 || $bytes === 0) {
                [$f, $b] = array_shift($grants) ?? [0, 0];
                $frames += $f;
                $bytes += $b;
            }
            $this->assertLessThanOrEqual($bytes, strlen($chunk), 'a chunk beyond the byte grant');
            $frames--;
            $bytes -= strlen($chunk);
        }
        $this->assertSame([10, 4, 2], array_map('strlen', $s->sent), 'split to fit, in order');
        $this->assertTrue($s->doneSent);
        $this->assertFalse($s->abandoned);
    }

    public function testAnIterableThatThrowsAbandonsTheCopyAndItsErrorPropagates(): void
    {
        $s = new ScriptedCopySession(grant: [64, 1 << 20]);
        $source = (static function (): \Generator {
            yield str_repeat('x', CopyRunner::CHUNK_BYTES);
            throw new \DomainException('producer');
        })();
        try {
            self::runner()->in($s, 'payload', $source);
            $this->fail('must throw');
        } catch (\DomainException $e) {
            $this->assertSame('producer', $e->getMessage());
        }
        $this->assertTrue($s->abandoned, 'CANCEL + drain');
        $this->assertFalse($s->doneSent, 'no end-of-data: nothing can apply');
    }

    public function testANonStringPieceIsRefusedAndTheCopyAbandoned(): void
    {
        $s = new ScriptedCopySession(grant: [64, 1 << 20]);
        try {
            self::runner()->in($s, 'payload', ["1\n", 2]);
            $this->fail('must throw');
        } catch (\InvalidArgumentException) {
            $this->addToAssertionCount(1);
        }
        $this->assertTrue($s->abandoned);
        $this->assertFalse($s->doneSent);
    }

    public function testALossBeforeTheEndOfDataIsAKnownRetryableAndIsNotFollowedByAWireOperation(): void
    {
        $s = new ScriptedCopySession(grant: [8, 64]);
        $s->failSendAt = 0; // the chunk's write fails: the session is poisoned
        try {
            self::runner()->in($s, 'payload', ['abcd', 'efgh']);
            $this->fail('must throw');
        } catch (RetryableException $e) {
            $this->assertSame(C::ERR_CONNECTION_LOST, $e->errorPayload()->code);
            $this->assertStringContainsString('end-of-data', $e->getMessage());
        }
        $this->assertFalse($s->abandoned, 'no second wire operation on a failed session');
    }

    public function testALossAfterTheEndOfDataIsIndeterminateInAutocommitAndRetryableInATransaction(): void
    {
        foreach ([false => IndeterminateException::class, true => RetryableException::class] as $inTx => $class) {
            $s = new ScriptedCopySession(grant: [8, 64]);
            $s->events = [];
            $s->failReadAfterDone = true;
            try {
                self::runner((bool) $inTx)->in($s, 'payload', ["1\n"]);
                $this->fail('must throw');
            } catch (\Throwable $e) {
                $this->assertInstanceOf($class, $e, 'inTx=' . var_export((bool) $inTx, true));
            }
            $this->assertTrue($s->doneSent);
        }
    }

    public function testAnEarlyErrorTerminalWhileWaitingForCreditIsThrownWithoutAbandoning(): void
    {
        $s = new ScriptedCopySession(grant: [1, 4]);
        $s->events = [['type' => 'end', 'outcome' => Outcome::error(new ErrorPayload(
            C::ERR_PROTOCOL, C::BRANCH_NON_RETRYABLE, '22P02', null, 'invalid input syntax', null, null,
        ))]];
        try {
            self::runner()->in($s, 'payload', ['abcd', 'efgh', 'ijkl']);
            $this->fail('must throw');
        } catch (NonRetryableException $e) {
            $this->assertSame('22P02', $e->errorPayload()->sqlstate);
        }
        $this->assertSame(['abcd'], $s->sent, 'stopped at the first chunk the credit did not cover');
        $this->assertFalse($s->abandoned, 'the terminal already arrived');
        $this->assertFalse($s->doneSent);
    }

    public function testCopyOutYieldsLazilyReplenishesPerChunkAndReturnsTheRowCount(): void
    {
        $s = new ScriptedCopySession(grant: [0, 0]);
        $s->events = [
            ['type' => 'data', 'data' => "1\n", 'bytes' => 5],
            ['type' => 'data', 'data' => "2\n", 'bytes' => 5],
            ['type' => 'end', 'outcome' => self::okOutcome(2)],
        ];
        $gen = self::runner()->out($s, 'payload', true);
        $this->assertSame([], $s->windowUpdates, 'nothing happens before iteration');
        $got = [];
        foreach ($gen as $chunk) {
            $got[] = $chunk;
            $this->assertCount(count($got) - 1, $s->windowUpdates, 'replenished only after the caller took it');
        }
        $this->assertSame(["1\n", "2\n"], $got);
        $this->assertSame(2, $gen->getReturn());
        $this->assertSame([[1, 5], [1, 5]], $s->windowUpdates);
        $this->assertFalse($s->abandoned);
    }

    public function testBreakingOutOfCopyOutAbandonsIt(): void
    {
        $s = new ScriptedCopySession(grant: [0, 0]);
        $s->events = [
            ['type' => 'data', 'data' => 'a', 'bytes' => 4],
            ['type' => 'data', 'data' => 'b', 'bytes' => 4],
        ];
        foreach (self::runner()->out($s, 'payload', true) as $chunk) {
            break;
        }
        $this->assertTrue($s->abandoned);
    }

    public function testTextRowEscapesExactlyWhatTheFormatRequires(): void
    {
        $this->assertSame("1\tt\tf\t\\N\n", Copy::textRow([1, true, false, null]));
        $this->assertSame("a\\\\b\\tc\\nd\\re\n", Copy::textRow(["a\\b\tc\nd\re"]));
        $this->assertSame("\\\\.\n", Copy::textRow(['\\.']), 'the end-of-data marker cannot be forged');
        $this->assertSame("\n", Copy::textRow(['']), 'an empty string is not NULL');
        $this->assertSame("-9223372036854775808\n", Copy::textRow([PHP_INT_MIN]));
        try {
            Copy::textRow([1.5]);
            $this->fail('a float is refused');
        } catch (\InvalidArgumentException) {
            $this->addToAssertionCount(1);
        }
        $rows = Copy::textRows([[1], [2]]);
        $this->assertSame(["1\n", "2\n"], iterator_to_array($rows, false));
    }
}

/** A scripted {@see CopySessionInterface}: records what the runner sends, replays engine events. */
final class ScriptedCopySession implements CopySessionInterface
{
    /** @var list<string> */
    public array $sent = [];
    public bool $doneSent = false;
    public bool $abandoned = false;
    /** @var list<array{0:int,1:int}> */
    public array $windowUpdates = [];
    /** @var list<array<string,mixed>> */
    public array $events = [];
    public ?int $failSendAt = null;
    public bool $failReadAfterDone = false;

    /** @param array{0:int,1:int} $grant */
    public function __construct(private readonly array $grant) {}

    public function openCopyIn(string $payload): array
    {
        return ['type' => 'grant', 'requestId' => 7, 'frames' => $this->grant[0], 'bytes' => $this->grant[1]];
    }

    public function openCopyOut(string $payload): int
    {
        return 7;
    }

    public function readCopyEvent(int $requestId): array
    {
        if ($this->failReadAfterDone && $this->doneSent) {
            throw new TransportException('link lost');
        }
        $e = array_shift($this->events);
        if ($e === null) {
            throw new \LogicException('the script ran out of events');
        }
        /** @var array{type:'grant', frames:int, bytes:int}|array{type:'data', data:string, bytes:int}|array{type:'end', outcome:Outcome} $e */
        return $e;
    }

    public function sendCopyData(int $requestId, string $data): void
    {
        if ($this->failSendAt === count($this->sent)) {
            throw new TransportException('write failed');
        }
        $this->sent[] = $data;
    }

    public function sendCopyDone(int $requestId): void
    {
        $this->doneSent = true;
    }

    public function abandonCopy(int $requestId): void
    {
        $this->abandoned = true;
    }

    public function sendWindowUpdate(int $requestId, int $frames, int $bytes): void
    {
        $this->windowUpdates[] = [$frames, $bytes];
    }

    public function openStream(int $service, int $method, string $payload): array
    {
        throw new \LogicException('not a stream');
    }

    public function readStreamFrame(int $requestId): array
    {
        throw new \LogicException('not a stream');
    }

    public function sendCancel(int $requestId): void {}

    public function abandonStream(int $requestId): void {}
}
