<?php // /php/client/src/Protocol/QueueCodec.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codecs for the QUEUE service (service `QUEUE` = 7, M7-G1a; SPEC §24.4,
 * /proto/PROTOCOL.md §14). Mirrors the Rust `messages::queue` BYTES, one encode/decode pair per
 * shape:
 *
 *   ENQUEUE  request  `[store, jobs: [[queue, payload, delay_s]] (1..=1000), dedup_key|nil, common]`
 *            response `[job_id: bin|nil, inserted: u32, deduplicated: bool, stats]`
 *   RESERVE  request  `[store, queues: [str] (1..=16), max_jobs: u16, wait_ms: u32, liveness: bool, common]`
 *            response `[jobs: [[job_id: bin, token: bin, attempts: u32, queue, payload, created_at: i64,
 *                       lease_deadline: i64]], stats]`
 *   ACK / EXTEND request `[store, job_id: bin, token: bin, common]` (one shape, "fenced")
 *            ACK response `[outcome: u8 (ACK_OUTCOME_*), stats]`; EXTEND response `[lease_deadline: i64, stats]`
 *   RELEASE  request  `[store, job_id: bin, token: bin, delay_s: u32, common]`
 *            response `[new_job_id: bin|nil, stats]`
 *   SIZE / CLEAR request `[store, queue, common]` (one shape, "scope")
 *            SIZE response `[pending, delayed, reserved, oldest_pending_at: i64|nil, stats]`; CLEAR response `[deleted, stats]`
 *
 * where `common = [tx_id: u64|nil, timeout_ms: u32|nil, traceparent: str|nil]` and
 * `stats = [queue_us: u64, exec_us: u64]`. Response bodies are the `Outcome::Ok` body only; the caller
 * wraps/unwraps the envelope.
 *
 * **Handles are opaque binary strings** (`job_id`, `new_job_id`, `token`; SPEC D22 (b), D24). This
 * codec never interprets one: it checks the length (1..={@see C::QUEUE_HANDLE_MAX_BYTES}) and always
 * writes msgpack `bin`. Writing `str` would change the wire type and could fail the engine's UTF-8
 * check. PHP cannot tell a `str` from a `bin` after unpack (both arrive as strings, see
 * {@see HttpWire}), so on decode the length is the check this side can make; the vectors lock the
 * family byte for byte.
 *
 * **STRICT in both directions**, like {@see HttpWire}: a value the engine's decoder would refuse (a
 * handle of 0 or 1 025 bytes, 0 or 1 001 jobs, a non-UTF-8 payload, a timeout past u32) is refused
 * here before a byte is written, as a {@see CodecException}; a malformed body from the engine is
 * refused on decode rather than coerced — every `str` field included, which must be UTF-8 on decode
 * exactly as the Rust decoder requires (M7-G1a review L1), `traceparent` alone excepted (decoded
 * lossily by the engine, carried as given here). What a value MEANS (a payload with U+0000, a `job_id` the
 * store cannot decode) is the engine's business, never this codec's.
 */
final class QueueCodec
{
    public const U16_MAX = 0xFFFF;

    // ---- ENQUEUE ----

    /**
     * @param array<string,mixed> $m `store`, `jobs` (list of `{queue, payload, delay_s}`), `dedup_key`, `common`
     */
    public static function encodeEnqueueRequest(array $m, PackerInterface $p): string
    {
        $jobs = $m['jobs'] ?? null;
        if (!is_array($jobs) || !array_is_list($jobs)) { throw new CodecException('EnqueueRequest jobs must be a list'); }
        self::count(count($jobs), C::QUEUE_ENQUEUE_MAX_JOBS, 'EnqueueRequest jobs');
        $out = $p->packArrayLen(4) . $p->packStr(HttpWire::utf8($m['store'] ?? null, 'EnqueueRequest store'))
            . $p->packArrayLen(count($jobs));
        foreach ($jobs as $i => $j) {
            if (!is_array($j)) { throw new CodecException("EnqueueRequest job {$i} is not an array"); }
            $out .= $p->packArrayLen(3)
                . $p->packStr(HttpWire::utf8($j['queue'] ?? null, "EnqueueRequest job {$i} queue"))
                . $p->packStr(HttpWire::utf8($j['payload'] ?? null, "EnqueueRequest job {$i} payload"))
                . $p->packUint(HttpWire::uint($j['delay_s'] ?? null, HttpWire::U32_MAX, "EnqueueRequest job {$i} delay_s"));
        }
        $dedup = HttpWire::nullableUtf8($m['dedup_key'] ?? null, 'EnqueueRequest dedup_key');
        return $out . ($dedup === null ? $p->packNil() : $p->packStr($dedup))
            . self::packCommon($p, $m['common'] ?? [], 'EnqueueRequest');
    }

    /**
     * @return array{store:string,jobs:list<array{queue:string,payload:string,delay_s:int}>,dedup_key:?string,common:array{tx_id:?int,timeout_ms:?int,traceparent:?string}}
     */
    public static function decodeEnqueueRequest(string $payload, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($payload, $p, 4, 'EnqueueRequest');
        $jobsW = $w[1];
        if (!is_array($jobsW) || !array_is_list($jobsW)) { throw new CodecException('EnqueueRequest jobs is not an array'); }
        self::count(count($jobsW), C::QUEUE_ENQUEUE_MAX_JOBS, 'EnqueueRequest jobs');
        $jobs = [];
        foreach ($jobsW as $i => $j) {
            $j = HttpWire::arity($j, 3, "EnqueueJob {$i}");
            $jobs[] = [
                'queue' => HttpWire::utf8($j[0], 'EnqueueJob queue'),
                'payload' => HttpWire::utf8($j[1], 'EnqueueJob payload'),
                'delay_s' => HttpWire::uint($j[2], HttpWire::U32_MAX, 'EnqueueJob delay_s'),
            ];
        }
        return [
            'store' => HttpWire::utf8($w[0], 'EnqueueRequest store'),
            'jobs' => $jobs,
            'dedup_key' => HttpWire::nullableUtf8($w[2], 'EnqueueRequest dedup_key'),
            'common' => self::common($w[3], 'EnqueueRequest'),
        ];
    }

    /** @param array<string,mixed> $m `job_id` (?binary string), `inserted`, `deduplicated`, `stats` */
    public static function encodeEnqueueResponse(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(4)
            . self::packOptHandle($p, $m['job_id'] ?? null, 'EnqueueResponse job_id')
            . $p->packUint(HttpWire::uint($m['inserted'] ?? null, HttpWire::U32_MAX, 'EnqueueResponse inserted'))
            . $p->packBool(HttpWire::bool($m['deduplicated'] ?? null, 'EnqueueResponse deduplicated'))
            . self::packStats($p, $m['stats'] ?? null, 'EnqueueResponse');
    }

    /** @return array{job_id:?string,inserted:int,deduplicated:bool,stats:array{queue_us:int,exec_us:int}} */
    public static function decodeEnqueueResponse(string $body, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($body, $p, 4, 'EnqueueResponse');
        return [
            'job_id' => self::optHandle($w[0], 'EnqueueResponse job_id'),
            'inserted' => HttpWire::uint($w[1], HttpWire::U32_MAX, 'EnqueueResponse inserted'),
            'deduplicated' => HttpWire::bool($w[2], 'EnqueueResponse deduplicated'),
            'stats' => self::stats($w[3], 'EnqueueResponse'),
        ];
    }

    // ---- RESERVE ----

    /** @param array<string,mixed> $m `store`, `queues` (list of strings), `max_jobs`, `wait_ms`, `liveness`, `common` */
    public static function encodeReserveRequest(array $m, PackerInterface $p): string
    {
        $queues = $m['queues'] ?? null;
        if (!is_array($queues) || !array_is_list($queues)) { throw new CodecException('ReserveRequest queues must be a list'); }
        self::count(count($queues), C::QUEUE_RESERVE_MAX_QUEUES, 'ReserveRequest queues');
        $out = $p->packArrayLen(6) . $p->packStr(HttpWire::utf8($m['store'] ?? null, 'ReserveRequest store'))
            . $p->packArrayLen(count($queues));
        foreach ($queues as $i => $q) { $out .= $p->packStr(HttpWire::utf8($q, "ReserveRequest queue {$i}")); }
        return $out
            . $p->packUint(HttpWire::uint($m['max_jobs'] ?? null, self::U16_MAX, 'ReserveRequest max_jobs'))
            . $p->packUint(HttpWire::uint($m['wait_ms'] ?? null, HttpWire::U32_MAX, 'ReserveRequest wait_ms'))
            . $p->packBool(HttpWire::bool($m['liveness'] ?? null, 'ReserveRequest liveness'))
            . self::packCommon($p, $m['common'] ?? [], 'ReserveRequest');
    }

    /**
     * @return array{store:string,queues:list<string>,max_jobs:int,wait_ms:int,liveness:bool,common:array{tx_id:?int,timeout_ms:?int,traceparent:?string}}
     */
    public static function decodeReserveRequest(string $payload, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($payload, $p, 6, 'ReserveRequest');
        $qs = $w[1];
        if (!is_array($qs) || !array_is_list($qs)) { throw new CodecException('ReserveRequest queues is not an array'); }
        self::count(count($qs), C::QUEUE_RESERVE_MAX_QUEUES, 'ReserveRequest queues');
        $queues = [];
        foreach ($qs as $q) { $queues[] = HttpWire::utf8($q, 'ReserveRequest queue'); }
        return [
            'store' => HttpWire::utf8($w[0], 'ReserveRequest store'),
            'queues' => $queues,
            'max_jobs' => HttpWire::uint($w[2], self::U16_MAX, 'ReserveRequest max_jobs'),
            'wait_ms' => HttpWire::uint($w[3], HttpWire::U32_MAX, 'ReserveRequest wait_ms'),
            'liveness' => HttpWire::bool($w[4], 'ReserveRequest liveness'),
            'common' => self::common($w[5], 'ReserveRequest'),
        ];
    }

    /** @param array<string,mixed> $m `jobs` (list of reserved-job arrays), `stats` */
    public static function encodeReserveResponse(array $m, PackerInterface $p): string
    {
        $jobs = $m['jobs'] ?? null;
        if (!is_array($jobs) || !array_is_list($jobs)) { throw new CodecException('ReserveResponse jobs must be a list'); }
        $out = $p->packArrayLen(2) . $p->packArrayLen(count($jobs));
        foreach ($jobs as $i => $j) {
            if (!is_array($j)) { throw new CodecException("ReserveResponse job {$i} is not an array"); }
            $out .= $p->packArrayLen(7)
                . $p->packBin(self::handle($j['job_id'] ?? null, 'ReservedJob job_id'))
                . $p->packBin(self::handle($j['token'] ?? null, 'ReservedJob token'))
                . $p->packUint(HttpWire::uint($j['attempts'] ?? null, HttpWire::U32_MAX, 'ReservedJob attempts'))
                . $p->packStr(HttpWire::utf8($j['queue'] ?? null, 'ReservedJob queue'))
                . $p->packStr(HttpWire::utf8($j['payload'] ?? null, 'ReservedJob payload'))
                . $p->packInt(self::int($j['created_at'] ?? null, 'ReservedJob created_at'))
                . $p->packInt(self::int($j['lease_deadline'] ?? null, 'ReservedJob lease_deadline'));
        }
        return $out . self::packStats($p, $m['stats'] ?? null, 'ReserveResponse');
    }

    /**
     * @return array{jobs:list<array{job_id:string,token:string,attempts:int,queue:string,payload:string,created_at:int,lease_deadline:int}>,stats:array{queue_us:int,exec_us:int}}
     */
    public static function decodeReserveResponse(string $body, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($body, $p, 2, 'ReserveResponse');
        $jobsW = $w[0];
        if (!is_array($jobsW) || !array_is_list($jobsW)) { throw new CodecException('ReserveResponse jobs is not an array'); }
        $jobs = [];
        foreach ($jobsW as $i => $j) {
            $j = HttpWire::arity($j, 7, "ReservedJob {$i}");
            $jobs[] = [
                'job_id' => self::handle($j[0], 'ReservedJob job_id'),
                'token' => self::handle($j[1], 'ReservedJob token'),
                'attempts' => HttpWire::uint($j[2], HttpWire::U32_MAX, 'ReservedJob attempts'),
                'queue' => HttpWire::utf8($j[3], 'ReservedJob queue'),
                'payload' => HttpWire::utf8($j[4], 'ReservedJob payload'),
                'created_at' => self::int($j[5], 'ReservedJob created_at'),
                'lease_deadline' => self::int($j[6], 'ReservedJob lease_deadline'),
            ];
        }
        return ['jobs' => $jobs, 'stats' => self::stats($w[1], 'ReserveResponse')];
    }

    // ---- ACK / EXTEND (fenced) ----

    /** @param array<string,mixed> $m `store`, `job_id`, `token` (binary strings), `common` */
    public static function encodeFencedRequest(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(4)
            . $p->packStr(HttpWire::utf8($m['store'] ?? null, 'FencedRequest store'))
            . $p->packBin(self::handle($m['job_id'] ?? null, 'FencedRequest job_id'))
            . $p->packBin(self::handle($m['token'] ?? null, 'FencedRequest token'))
            . self::packCommon($p, $m['common'] ?? [], 'FencedRequest');
    }

    /** @return array{store:string,job_id:string,token:string,common:array{tx_id:?int,timeout_ms:?int,traceparent:?string}} */
    public static function decodeFencedRequest(string $payload, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($payload, $p, 4, 'FencedRequest');
        return [
            'store' => HttpWire::utf8($w[0], 'FencedRequest store'),
            'job_id' => self::handle($w[1], 'FencedRequest job_id'),
            'token' => self::handle($w[2], 'FencedRequest token'),
            'common' => self::common($w[3], 'FencedRequest'),
        ];
    }

    /** @param array<string,mixed> $m `outcome` (an `ACK_OUTCOME_*`), `stats` */
    public static function encodeAckResponse(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(2)
            . $p->packUint(self::ackOutcome($m['outcome'] ?? null))
            . self::packStats($p, $m['stats'] ?? null, 'AckResponse');
    }

    /** @return array{outcome:int,stats:array{queue_us:int,exec_us:int}} */
    public static function decodeAckResponse(string $body, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($body, $p, 2, 'AckResponse');
        return ['outcome' => self::ackOutcome($w[0]), 'stats' => self::stats($w[1], 'AckResponse')];
    }

    /** @param array<string,mixed> $m `lease_deadline`, `stats` */
    public static function encodeExtendResponse(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(2)
            . $p->packInt(self::int($m['lease_deadline'] ?? null, 'ExtendResponse lease_deadline'))
            . self::packStats($p, $m['stats'] ?? null, 'ExtendResponse');
    }

    /** @return array{lease_deadline:int,stats:array{queue_us:int,exec_us:int}} */
    public static function decodeExtendResponse(string $body, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($body, $p, 2, 'ExtendResponse');
        return [
            'lease_deadline' => self::int($w[0], 'ExtendResponse lease_deadline'),
            'stats' => self::stats($w[1], 'ExtendResponse'),
        ];
    }

    // ---- RELEASE ----

    /** @param array<string,mixed> $m `store`, `job_id`, `token`, `delay_s`, `common` */
    public static function encodeReleaseRequest(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(5)
            . $p->packStr(HttpWire::utf8($m['store'] ?? null, 'ReleaseRequest store'))
            . $p->packBin(self::handle($m['job_id'] ?? null, 'ReleaseRequest job_id'))
            . $p->packBin(self::handle($m['token'] ?? null, 'ReleaseRequest token'))
            . $p->packUint(HttpWire::uint($m['delay_s'] ?? null, HttpWire::U32_MAX, 'ReleaseRequest delay_s'))
            . self::packCommon($p, $m['common'] ?? [], 'ReleaseRequest');
    }

    /** @return array{store:string,job_id:string,token:string,delay_s:int,common:array{tx_id:?int,timeout_ms:?int,traceparent:?string}} */
    public static function decodeReleaseRequest(string $payload, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($payload, $p, 5, 'ReleaseRequest');
        return [
            'store' => HttpWire::utf8($w[0], 'ReleaseRequest store'),
            'job_id' => self::handle($w[1], 'ReleaseRequest job_id'),
            'token' => self::handle($w[2], 'ReleaseRequest token'),
            'delay_s' => HttpWire::uint($w[3], HttpWire::U32_MAX, 'ReleaseRequest delay_s'),
            'common' => self::common($w[4], 'ReleaseRequest'),
        ];
    }

    /** @param array<string,mixed> $m `new_job_id` (?binary string; `null` = gone), `stats` */
    public static function encodeReleaseResponse(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(2)
            . self::packOptHandle($p, $m['new_job_id'] ?? null, 'ReleaseResponse new_job_id')
            . self::packStats($p, $m['stats'] ?? null, 'ReleaseResponse');
    }

    /** @return array{new_job_id:?string,stats:array{queue_us:int,exec_us:int}} */
    public static function decodeReleaseResponse(string $body, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($body, $p, 2, 'ReleaseResponse');
        return [
            'new_job_id' => self::optHandle($w[0], 'ReleaseResponse new_job_id'),
            'stats' => self::stats($w[1], 'ReleaseResponse'),
        ];
    }

    // ---- SIZE / CLEAR (scope) ----

    /** @param array<string,mixed> $m `store`, `queue`, `common` */
    public static function encodeScopeRequest(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(3)
            . $p->packStr(HttpWire::utf8($m['store'] ?? null, 'QueueScopeRequest store'))
            . $p->packStr(HttpWire::utf8($m['queue'] ?? null, 'QueueScopeRequest queue'))
            . self::packCommon($p, $m['common'] ?? [], 'QueueScopeRequest');
    }

    /** @return array{store:string,queue:string,common:array{tx_id:?int,timeout_ms:?int,traceparent:?string}} */
    public static function decodeScopeRequest(string $payload, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($payload, $p, 3, 'QueueScopeRequest');
        return [
            'store' => HttpWire::utf8($w[0], 'QueueScopeRequest store'),
            'queue' => HttpWire::utf8($w[1], 'QueueScopeRequest queue'),
            'common' => self::common($w[2], 'QueueScopeRequest'),
        ];
    }

    /**
     * `oldest_pending_at` is the earliest `available_at` among the queue's pending jobs (Unix seconds),
     * or `null` when none is pending — what Laravel 12's `creationTimeOfOldestPendingJob()` returns.
     * @param array<string,mixed> $m `pending`, `delayed`, `reserved`, `oldest_pending_at`, `stats`
     */
    public static function encodeSizeResponse(array $m, PackerInterface $p): string
    {
        $oldest = $m['oldest_pending_at'] ?? null;
        return $p->packArrayLen(5)
            . $p->packUint(HttpWire::uint($m['pending'] ?? null, PHP_INT_MAX, 'SizeResponse pending'))
            . $p->packUint(HttpWire::uint($m['delayed'] ?? null, PHP_INT_MAX, 'SizeResponse delayed'))
            . $p->packUint(HttpWire::uint($m['reserved'] ?? null, PHP_INT_MAX, 'SizeResponse reserved'))
            . ($oldest === null ? $p->packNil() : $p->packInt(self::int($oldest, 'SizeResponse oldest_pending_at')))
            . self::packStats($p, $m['stats'] ?? null, 'SizeResponse');
    }

    /** @return array{pending:int,delayed:int,reserved:int,oldest_pending_at:?int,stats:array{queue_us:int,exec_us:int}} */
    public static function decodeSizeResponse(string $body, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($body, $p, 5, 'SizeResponse');
        return [
            'pending' => HttpWire::uint($w[0], PHP_INT_MAX, 'SizeResponse pending'),
            'delayed' => HttpWire::uint($w[1], PHP_INT_MAX, 'SizeResponse delayed'),
            'reserved' => HttpWire::uint($w[2], PHP_INT_MAX, 'SizeResponse reserved'),
            'oldest_pending_at' => $w[3] === null ? null : self::int($w[3], 'SizeResponse oldest_pending_at'),
            'stats' => self::stats($w[4], 'SizeResponse'),
        ];
    }

    /** @param array<string,mixed> $m `deleted`, `stats` */
    public static function encodeClearResponse(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(2)
            . $p->packUint(HttpWire::uint($m['deleted'] ?? null, PHP_INT_MAX, 'ClearResponse deleted'))
            . self::packStats($p, $m['stats'] ?? null, 'ClearResponse');
    }

    /** @return array{deleted:int,stats:array{queue_us:int,exec_us:int}} */
    public static function decodeClearResponse(string $body, PackerInterface $p): array
    {
        $w = HttpWire::unpackArray($body, $p, 2, 'ClearResponse');
        return [
            'deleted' => HttpWire::uint($w[0], PHP_INT_MAX, 'ClearResponse deleted'),
            'stats' => self::stats($w[1], 'ClearResponse'),
        ];
    }

    // ---- shared field rules (one each, mirrored in the Rust `messages::queue`) ----

    /** An opaque handle: a string of 1..=QUEUE_HANDLE_MAX_BYTES bytes, never interpreted. */
    public static function handle(mixed $v, string $what): string
    {
        if (!is_string($v)) { throw new CodecException("{$what} is not a binary string"); }
        $n = strlen($v);
        if ($n < 1 || $n > C::QUEUE_HANDLE_MAX_BYTES) {
            throw new CodecException("{$what}: {$n} bytes, outside 1.." . C::QUEUE_HANDLE_MAX_BYTES);
        }
        return $v;
    }

    private static function optHandle(mixed $v, string $what): ?string
    {
        return $v === null ? null : self::handle($v, $what);
    }

    private static function packOptHandle(PackerInterface $p, mixed $v, string $what): string
    {
        return $v === null ? $p->packNil() : $p->packBin(self::handle($v, $what));
    }

    private static function count(int $n, int $max, string $what): void
    {
        if ($n < 1 || $n > $max) { throw new CodecException("{$what}: {$n} entries, outside 1..{$max}"); }
    }

    private static function int(mixed $v, string $what): int
    {
        if (!is_int($v)) { throw new CodecException("{$what} is not an integer"); }
        return $v;
    }

    private static function ackOutcome(mixed $v): int
    {
        if ($v !== C::ACK_OUTCOME_ACKED && $v !== C::ACK_OUTCOME_GONE) {
            throw new CodecException('AckResponse outcome is not an ACK_OUTCOME_* value');
        }
        return $v;
    }

    private static function packCommon(PackerInterface $p, mixed $c, string $what): string
    {
        if (!is_array($c)) { throw new CodecException("{$what} common is not an array"); }
        $tx = HttpWire::nullableUint($c['tx_id'] ?? null, PHP_INT_MAX, "{$what} tx_id");
        $timeout = HttpWire::nullableUint($c['timeout_ms'] ?? null, HttpWire::U32_MAX, "{$what} timeout_ms");
        $trace = HttpWire::nullableString($c['traceparent'] ?? null, "{$what} traceparent");
        return $p->packArrayLen(3)
            . ($tx === null ? $p->packNil() : $p->packUint($tx))
            . ($timeout === null ? $p->packNil() : $p->packUint($timeout))
            . ($trace === null ? $p->packNil() : $p->packStr($trace));
    }

    /** @return array{tx_id:?int,timeout_ms:?int,traceparent:?string} */
    private static function common(mixed $w, string $what): array
    {
        $c = HttpWire::arity($w, 3, "{$what} common");
        return [
            'tx_id' => HttpWire::nullableUint($c[0], PHP_INT_MAX, "{$what} tx_id"),
            'timeout_ms' => HttpWire::nullableUint($c[1], HttpWire::U32_MAX, "{$what} timeout_ms"),
            'traceparent' => HttpWire::nullableString($c[2], "{$what} traceparent"),
        ];
    }

    private static function packStats(PackerInterface $p, mixed $s, string $what): string
    {
        if (!is_array($s)) { throw new CodecException("{$what} stats is not an array"); }
        return $p->packArrayLen(2)
            . $p->packUint(HttpWire::uint($s['queue_us'] ?? null, PHP_INT_MAX, "{$what} queue_us"))
            . $p->packUint(HttpWire::uint($s['exec_us'] ?? null, PHP_INT_MAX, "{$what} exec_us"));
    }

    /** @return array{queue_us:int,exec_us:int} */
    private static function stats(mixed $w, string $what): array
    {
        $s = HttpWire::arity($w, 2, "{$what} stats");
        return [
            'queue_us' => HttpWire::uint($s[0], PHP_INT_MAX, "{$what} queue_us"),
            'exec_us' => HttpWire::uint($s[1], PHP_INT_MAX, "{$what} exec_us"),
        ];
    }
}
