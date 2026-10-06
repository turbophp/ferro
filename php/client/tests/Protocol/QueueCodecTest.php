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

    /**
     * The count bounds are INCLUSIVE (review M3: a decoder whose upper bound became exclusive, or used
     * the other shape's bound, passed every other test). Positive controls at exactly the maximum,
     * both directions; the golden vectors `queue_enqueue_request_max_jobs` and
     * `queue_reserve_request_max_queues` lock the same against the Rust bytes.
     */
    public function testCountsAtTheMaximumEncodeAndDecode(): void
    {
        $p = new PurePacker();
        $jobs = array_fill(0, C::QUEUE_ENQUEUE_MAX_JOBS, ['queue' => 'q', 'payload' => '{}', 'delay_s' => 0]);
        $enq = QueueCodec::encodeEnqueueRequest(['store' => 'jobs', 'jobs' => $jobs, 'dedup_key' => null,
            'common' => self::COMMON], $p);
        $this->assertCount(1000, QueueCodec::decodeEnqueueRequest($enq, $p)['jobs']);
        $queues = array_map(static fn (int $i): string => "q{$i}", range(1, C::QUEUE_RESERVE_MAX_QUEUES));
        $res = QueueCodec::encodeReserveRequest(['store' => 'jobs', 'queues' => $queues, 'max_jobs' => 1,
            'wait_ms' => 0, 'liveness' => false, 'common' => self::COMMON], $p);
        $this->assertCount(16, QueueCodec::decodeReserveRequest($res, $p)['queues']);
        $this->assertSame([1000, 16], [C::QUEUE_ENQUEUE_MAX_JOBS, C::QUEUE_RESERVE_MAX_QUEUES], 'SPEC §24.4, literally');
    }

    /**
     * Review L1: a `str` the Rust decoder refuses unless it is UTF-8 is refused by this decoder too —
     * here a reserved job's payload (the engine never sends one, which is why it is a LOW).
     */
    public function testANonUtf8StringIsRefusedOnDecode(): void
    {
        $p = new PurePacker();
        $body = $p->packArrayLen(2) . $p->packArrayLen(1) . $p->packArrayLen(7)
            . $p->packBin('1') . $p->packBin(str_repeat("\x01", 8)) . $p->packUint(1)
            . $p->packStr('q') . $p->packStr("\xff") . $p->packInt(1) . $p->packInt(2)
            . $p->packArrayLen(2) . $p->packUint(1) . $p->packUint(2);
        try {
            QueueCodec::decodeReserveResponse($body, $p);
            $this->fail('a non-UTF-8 payload was decoded');
        } catch (CodecException $e) {
            $this->assertStringContainsString('payload is not UTF-8', $e->getMessage());
        }
        // The control: the same body with a UTF-8 payload decodes.
        $ok = str_replace($p->packStr("\xff"), $p->packStr('{}'), $body);
        $this->assertSame('{}', QueueCodec::decodeReserveResponse($ok, $p)['jobs'][0]['payload']);
    }

    /**
     * Review L1, at EVERY `str` position the PHP side decodes (the test above pins one): each field
     * carries a distinct 4-byte ASCII marker, the frame is encoded, and ONE marker at a time is
     * replaced by a same-length non-UTF-8 string, so framing is intact and only that field is wrong.
     * `traceparent` is the control — decoded as it arrives, as the Rust decoder decodes it lossily.
     */
    public function testEveryStrPositionRefusesNonUtf8OnDecodeButTraceparent(): void
    {
        $p = new PurePacker();
        $tok = str_repeat("\x01", 8);
        $common = ['tx_id' => null, 'timeout_ms' => null, 'traceparent' => 'Mtp0'];
        $stats = ['queue_us' => 1, 'exec_us' => 2];
        $cases = [
            'EnqueueRequest' => [QueueCodec::encodeEnqueueRequest(['store' => 'Mst0',
                'jobs' => [['queue' => 'Mqu0', 'payload' => 'Mpl0', 'delay_s' => 0]],
                'dedup_key' => 'Mdk0', 'common' => $common], $p),
                QueueCodec::decodeEnqueueRequest(...), ['Mst0', 'Mqu0', 'Mpl0', 'Mdk0']],
            'ReserveRequest' => [QueueCodec::encodeReserveRequest(['store' => 'Mst1', 'queues' => ['Mqa1', 'Mqb1'],
                'max_jobs' => 1, 'wait_ms' => 0, 'liveness' => false, 'common' => $common], $p),
                QueueCodec::decodeReserveRequest(...), ['Mst1', 'Mqa1', 'Mqb1']],
            'ReserveResponse' => [QueueCodec::encodeReserveResponse(['jobs' => [['job_id' => '1', 'token' => $tok,
                'attempts' => 1, 'queue' => 'Mqu2', 'payload' => 'Mpl2', 'created_at' => 1, 'lease_deadline' => 2]],
                'stats' => $stats], $p),
                QueueCodec::decodeReserveResponse(...), ['Mqu2', 'Mpl2']],
            'FencedRequest' => [QueueCodec::encodeFencedRequest(['store' => 'Mst3', 'job_id' => '1', 'token' => $tok,
                'common' => $common], $p),
                QueueCodec::decodeFencedRequest(...), ['Mst3']],
            'ReleaseRequest' => [QueueCodec::encodeReleaseRequest(['store' => 'Mst4', 'job_id' => '1', 'token' => $tok,
                'delay_s' => 0, 'common' => $common], $p),
                QueueCodec::decodeReleaseRequest(...), ['Mst4']],
            'ScopeRequest' => [QueueCodec::encodeScopeRequest(['store' => 'Mst5', 'queue' => 'Mqu5',
                'common' => $common], $p),
                QueueCodec::decodeScopeRequest(...), ['Mst5', 'Mqu5']],
        ];
        foreach ($cases as $name => [$frame, $decode, $markers]) {
            $decode($frame, $p); // the control: every marker is valid UTF-8
            foreach ($markers as $m) {
                $needle = $p->packStr($m);
                $this->assertSame(1, substr_count($frame, $needle), "{$name} {$m} is unique");
                try {
                    $decode(str_replace($needle, $p->packStr("\xff" . substr($m, 1)), $frame), $p);
                    $this->fail("{$name}: a non-UTF-8 {$m} was decoded");
                } catch (CodecException $e) {
                    $this->assertStringContainsString('not UTF-8', $e->getMessage(), "{$name} {$m}");
                }
            }
            if ($name !== 'ReserveResponse') {
                $bad = str_replace($p->packStr('Mtp0'), $p->packStr("\xfftp0"), $frame);
                $decoded = $decode($bad, $p);
                $this->assertSame("\xfftp0", $decoded['common']['traceparent'], "{$name} traceparent");
            }
        }
    }

    public function testOldestPendingAtIsNullableBothWays(): void
    {
        $p = new PurePacker();
        foreach ([null, 1790000000, -1] as $oldest) {
            $m = ['pending' => 1, 'delayed' => 2, 'reserved' => 3, 'oldest_pending_at' => $oldest,
                'stats' => ['queue_us' => 4, 'exec_us' => 5]];
            $this->assertSame($m, QueueCodec::decodeSizeResponse(QueueCodec::encodeSizeResponse($m, $p), $p));
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
