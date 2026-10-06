<?php // /php/client/tests/Protocol/QueueCodecTest.php
declare(strict_types=1);
namespace Ferro\Tests\Protocol;

use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\QueueCodec;
use PHPUnit\Framework\TestCase;

/**
 * The QUEUE codec's STRICTNESS (M7-G1a, SPEC §24.4) — what the golden vectors cannot show, because a
 * vector is a value the codec accepts. ENCODE refuses, before a byte is written, what the engine's
 * decoder would refuse (so a client bug is a local {@see CodecException}, never a `Protocol` terminal
 * that costs a round trip); DECODE refuses a body of the wrong type, width or bound instead of
 * inventing a value.
 */
final class QueueCodecTest extends TestCase
{
    private const COMMON = ['tx_id' => null, 'timeout_ms' => null, 'traceparent' => null];

    /** @return array<string,mixed> */
    private static function fenced(string $jobId, string $token): array
    {
        return ['store' => 'jobs', 'job_id' => $jobId, 'token' => $token, 'common' => self::COMMON];
    }

    public function testAHandleIsWrittenAsBinNeverAsStr(): void
    {
        $p = new PurePacker();
        // "42" as a str would be a2 34 32; as a bin it is c4 02 34 32.
        $hex = bin2hex(QueueCodec::encodeFencedRequest(self::fenced('42', str_repeat("\x01", 8)), $p));
        $this->assertStringContainsString('c4023432', $hex);
        $this->assertStringNotContainsString('a23432', $hex);
    }

    /** @return iterable<string, array{0:int}> */
    public static function outOfBoundsLengths(): iterable
    {
        yield 'empty' => [0];
        yield 'one past the registry bound' => [C::QUEUE_HANDLE_MAX_BYTES + 1];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('outOfBoundsLengths')]
    public function testEveryHandlePositionRefusesAnOutOfBoundsHandleOnEncode(int $len): void
    {
        $p = new PurePacker();
        $bad = str_repeat("\x07", $len);
        $ok = str_repeat("\x01", 8);
        $stats = ['queue_us' => 1, 'exec_us' => 2];
        $cases = [
            'ack job_id' => fn () => QueueCodec::encodeFencedRequest(self::fenced($bad, $ok), $p),
            'ack token' => fn () => QueueCodec::encodeFencedRequest(self::fenced('1', $bad), $p),
            'release job_id' => fn () => QueueCodec::encodeReleaseRequest(['delay_s' => 0] + self::fenced($bad, $ok), $p),
            'release token' => fn () => QueueCodec::encodeReleaseRequest(['delay_s' => 0] + self::fenced('1', $bad), $p),
            'enqueue response job_id' => fn () => QueueCodec::encodeEnqueueResponse(
                ['job_id' => $bad, 'inserted' => 1, 'deduplicated' => false, 'stats' => $stats], $p),
            'release response new_job_id' => fn () => QueueCodec::encodeReleaseResponse(['new_job_id' => $bad, 'stats' => $stats], $p),
            'reserve response token' => fn () => QueueCodec::encodeReserveResponse(['jobs' => [[
                'job_id' => '1', 'token' => $bad, 'attempts' => 1, 'queue' => 'q', 'payload' => '',
                'created_at' => 1, 'lease_deadline' => 2]], 'stats' => $stats], $p),
        ];
        foreach ($cases as $what => $encode) {
            try {
                $encode();
                $this->fail("{$what}: a {$len}-byte handle was encoded");
            } catch (CodecException $e) {
                $this->assertStringContainsString("{$len} bytes", $e->getMessage(), $what);
            }
        }
        // The control: both inclusive bounds encode.
        foreach ([1, C::QUEUE_HANDLE_MAX_BYTES] as $n) {
            $this->assertNotSame('', QueueCodec::encodeFencedRequest(self::fenced(str_repeat('x', $n), str_repeat('y', $n)), $p));
        }
    }

    public function testCountsAreBoundedOnEncode(): void
    {
        $p = new PurePacker();
        $job = ['queue' => 'q', 'payload' => '{}', 'delay_s' => 0];
        foreach ([0, C::QUEUE_ENQUEUE_MAX_JOBS + 1] as $n) {
            try {
                QueueCodec::encodeEnqueueRequest(['store' => 'jobs', 'jobs' => array_fill(0, $n, $job),
                    'dedup_key' => null, 'common' => self::COMMON], $p);
                $this->fail("{$n} jobs were encoded");
            } catch (CodecException $e) {
                $this->assertStringContainsString("{$n} entries", $e->getMessage());
            }
        }
        foreach ([0, C::QUEUE_RESERVE_MAX_QUEUES + 1] as $n) {
            try {
                QueueCodec::encodeReserveRequest(['store' => 'jobs', 'queues' => array_fill(0, $n, 'q'),
                    'max_jobs' => 1, 'wait_ms' => 0, 'liveness' => false, 'common' => self::COMMON], $p);
                $this->fail("{$n} queues were encoded");
            } catch (CodecException $e) {
                $this->assertStringContainsString("{$n} entries", $e->getMessage());
            }
        }
    }

    public function testANonUtf8PayloadOrQueueIsRefusedBeforeSending(): void
    {
        $p = new PurePacker();
        $this->expectException(CodecException::class);
        $this->expectExceptionMessageMatches('/payload is not UTF-8/');
        QueueCodec::encodeEnqueueRequest(['store' => 'jobs', 'jobs' => [['queue' => 'q', 'payload' => "\xff", 'delay_s' => 0]],
            'dedup_key' => null, 'common' => self::COMMON], $p);
    }

    public function testMaxJobsAndTimeoutsMustFitTheirWidth(): void
    {
        $p = new PurePacker();
        $base = ['store' => 'jobs', 'queues' => ['q'], 'max_jobs' => 1, 'wait_ms' => 0, 'liveness' => false, 'common' => self::COMMON];
        foreach ([['max_jobs' => 0x10000], ['wait_ms' => 0x100000000], ['common' => ['timeout_ms' => 0x100000000] + self::COMMON]] as $bad) {
            try {
                QueueCodec::encodeReserveRequest($bad + $base, $p);
                $this->fail('encoded ' . json_encode($bad));
            } catch (CodecException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testAnAckOutcomeOutsideTheRegistryIsRefusedBothWays(): void
    {
        $p = new PurePacker();
        $stats = $p->packArrayLen(2) . $p->packUint(1) . $p->packUint(2);
        foreach ([0, 3] as $bad) {
            try {
                QueueCodec::decodeAckResponse($p->packArrayLen(2) . $p->packUint($bad) . $stats, $p);
                $this->fail("decoded outcome {$bad}");
            } catch (CodecException) {
                $this->addToAssertionCount(1);
            }
            try {
                QueueCodec::encodeAckResponse(['outcome' => $bad, 'stats' => ['queue_us' => 1, 'exec_us' => 2]], $p);
                $this->fail("encoded outcome {$bad}");
            } catch (CodecException) {
                $this->addToAssertionCount(1);
            }
        }
        $this->assertSame(C::ACK_OUTCOME_GONE,
            QueueCodec::decodeAckResponse($p->packArrayLen(2) . $p->packUint(C::ACK_OUTCOME_GONE) . $stats, $p)['outcome']);
    }

    public function testACountPastPhpIntMaxIsRefusedNotInvented(): void
    {
        $p = new PurePacker();
        // uint64 2^63 unpacks as a decimal STRING in the pure decoder; a count is bounded < 2^63.
        $body = $p->packArrayLen(2) . $p->packUint('9223372036854775808')
            . $p->packArrayLen(2) . $p->packUint(1) . $p->packUint(2);
        $this->expectException(CodecException::class);
        QueueCodec::decodeClearResponse($body, $p);
    }

    public function testNegativeTimesSurviveBothWays(): void
    {
        $p = new PurePacker();
        $m = ['lease_deadline' => -1, 'stats' => ['queue_us' => 0, 'exec_us' => 0]];
        $this->assertSame($m, QueueCodec::decodeExtendResponse(QueueCodec::encodeExtendResponse($m, $p), $p));
    }

    public function testTrailingBytesAndAWrongArityAreRefused(): void
    {
        $p = new PurePacker();
        $ok = QueueCodec::encodeFencedRequest(self::fenced('1', str_repeat("\x01", 8)), $p);
        try {
            QueueCodec::decodeFencedRequest($ok . "\xc0", $p);
            $this->fail('trailing bytes accepted');
        } catch (CodecException $e) {
            $this->assertStringContainsString('trailing', $e->getMessage());
        }
        $this->expectException(CodecException::class);
        QueueCodec::decodeFencedRequest("\x93" . substr($ok, 1), $p);
    }
}
