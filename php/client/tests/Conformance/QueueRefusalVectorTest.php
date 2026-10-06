<?php // /php/client/tests/Conformance/QueueRefusalVectorTest.php
declare(strict_types=1);
namespace Ferro\Tests\Conformance;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\QueueCodec;
use PHPUnit\Framework\TestCase;

/**
 * SPEC §24.3's G1 prerequisite (b), the PHP half: every QUEUE refusal vector in
 * `/proto/vectors/refusal/` — a frame with a valid header whose payload is well-formed EXCEPT one
 * field out of its shape bound (a handle of 0 or QUEUE_HANDLE_MAX_BYTES + 1 bytes, or 0 / max + 1
 * jobs or queues) — is refused by {@see QueueCodec}'s decoder FOR ITS OWN REASON: the exception names
 * the field and the length, so a refusal for some unrelated reason cannot pass. The Rust half is
 * `ferro-proto/tests/golden_vectors.rs::queue_refusal_vectors_are_refused_for_their_own_reason`.
 */
final class QueueRefusalVectorTest extends TestCase
{
    private const DIR = __DIR__ . '/../../../../proto/vectors/refusal';

    /** @return iterable<string, array{0:array<string,mixed>}> */
    public static function refusals(): iterable
    {
        foreach (glob(self::DIR . '/queue_*.json') ?: [] as $f) {
            /** @var array<string,mixed> $v */
            $v = json_decode((string) file_get_contents($f), true, 512, JSON_THROW_ON_ERROR);
            yield basename($f) => [$v];
        }
    }

    /** @param array<string,mixed> $v */
    #[\PHPUnit\Framework\Attributes\DataProvider('refusals')]
    public function testEachRefusalVectorIsRefusedForItsOwnReason(array $v): void
    {
        $frame = (string) hex2bin((string) $v['frame_hex']);
        $h = Header::decode($frame);
        $this->assertSame(C::SERVICE_QUEUE, $h->service);
        $payload = substr($frame, 16);
        $p = new PurePacker();
        $end = ($h->flags & C::FLAG_END) !== 0;
        if ($end) {
            $outcome = Outcome::decode($payload, $p);
            $this->assertTrue($outcome->isOk(), 'the envelope itself is valid');
            $payload = $outcome->body();
        }
        $decode = match (true) {
            !$end && $h->method === C::METHOD_QUEUE_ENQUEUE => QueueCodec::decodeEnqueueRequest(...),
            !$end && $h->method === C::METHOD_QUEUE_RESERVE => QueueCodec::decodeReserveRequest(...),
            !$end && ($h->method === C::METHOD_QUEUE_ACK || $h->method === C::METHOD_QUEUE_EXTEND) => QueueCodec::decodeFencedRequest(...),
            !$end && $h->method === C::METHOD_QUEUE_RELEASE => QueueCodec::decodeReleaseRequest(...),
            $end && $h->method === C::METHOD_QUEUE_ENQUEUE => QueueCodec::decodeEnqueueResponse(...),
            $end && $h->method === C::METHOD_QUEUE_RESERVE => QueueCodec::decodeReserveResponse(...),
            $end && $h->method === C::METHOD_QUEUE_RELEASE => QueueCodec::decodeReleaseResponse(...),
            default => $this->fail("no decoder for method {$h->method}"),
        };
        $field = (string) $v['field'];
        $len = (string) $v['len'];
        try {
            $decode($payload, $p);
            $this->fail("{$v['name']} was accepted");
        } catch (CodecException $e) {
            $this->assertStringContainsString($field, $e->getMessage(), "{$v['name']}: refused, but not for its own field");
            $this->assertStringContainsString("{$len} ", $e->getMessage(), "{$v['name']}: refused, but not for its own length");
        }
    }

    /** The set on disk is exactly the required set, so a deleted vector cannot pass vacuously. */
    public function testTheRefusalSetIsComplete(): void
    {
        $over = C::QUEUE_HANDLE_MAX_BYTES + 1;
        $required = [];
        foreach ([0, $over] as $n) {
            foreach (['enqueue_response_job_id', 'reserve_response_job_id', 'reserve_response_token',
                'ack_request_job_id', 'ack_request_token', 'extend_request_job_id', 'extend_request_token',
                'release_request_job_id', 'release_request_token', 'release_response_new_job_id'] as $pos) {
                $required[] = "queue_{$pos}_{$n}";
            }
        }
        foreach ([0, C::QUEUE_ENQUEUE_MAX_JOBS + 1] as $n) { $required[] = "queue_enqueue_request_jobs_{$n}"; }
        foreach ([0, C::QUEUE_RESERVE_MAX_QUEUES + 1] as $n) { $required[] = "queue_reserve_request_queues_{$n}"; }
        $seen = [];
        foreach (self::refusals() as [$v]) { $seen[] = (string) $v['name']; }
        sort($required);
        sort($seen);
        $this->assertSame($required, $seen);
    }
}
