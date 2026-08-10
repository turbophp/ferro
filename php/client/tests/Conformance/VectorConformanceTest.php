<?php // /php/client/tests/Conformance/VectorConformanceTest.php
declare(strict_types=1);
namespace Ferro\Tests\Conformance;
use Ferro\Protocol\BeginRequest;
use Ferro\Protocol\BeginResponse;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\{PurePacker, ExtPacker};
use Ferro\Protocol\Outcome;
use Ferro\Protocol\PoolInfo;
use Ferro\Protocol\SavepointRequest;
use Ferro\Protocol\StreamData;
use Ferro\Protocol\StreamHead;
use Ferro\Protocol\TxControl;
use PHPUnit\Framework\TestCase;

final class VectorConformanceTest extends TestCase
{
    private const DIR = __DIR__ . '/../../../../proto/vectors';

    /**
     * The core messages the client actually ENCODES — the single source for both the byte-lock
     * provider filter and the accounting in {@see testEveryCommittedVectorIsByteLocked}. It used to
     * be written out twice by hand, so deleting a name from the byte-lock filter silently dropped
     * that message's cross-language lock while the accounting guard, reading its own copy, stayed
     * green. One const, two readers: a drop is now RED at the accounting guard.
     *
     * `error_protocol` is deliberately absent: it is an Outcome the client only ever decodes.
     */
    private const CLIENT_ENCODED_MESSAGES = ['hello', 'hello_ack', 'ping', 'pong', 'goodbye', 'window_update'];

    /** @return iterable<string, array{0:array<string,mixed>}> */
    public static function vectors(): iterable
    {
        foreach (glob(self::DIR . '/*.json') ?: [] as $f) {
            /** @var array<string,mixed> $v */
            $v = json_decode((string) file_get_contents($f), true, 512, JSON_THROW_ON_ERROR);
            yield basename($f) => [$v];
        }
    }

    /** @param array<string,mixed> $v */
    #[\PHPUnit\Framework\Attributes\DataProvider('vectors')]
    public function testHeaderDecodesToVectorFields(array $v): void
    {
        $frame = (string) hex2bin((string) $v['frame_hex']);
        $h = Header::decode($frame);
        $this->assertSame($v['header']['service'], $h->service, "service for {$v['name']}");
        $this->assertSame($v['header']['method'], $h->method, "method for {$v['name']}");
        $this->assertSame(strlen($frame) - 16, $h->payloadLen, "payload_len for {$v['name']}");
        $this->assertSame($v['header']['flags'], $h->flags, "flags for {$v['name']}");
        $this->assertSame($v['header']['request_id'], $h->requestId, "request_id for {$v['name']}");
    }

    /** @param array<string,mixed> $v */
    #[\PHPUnit\Framework\Attributes\DataProvider('vectors')]
    public function testPurePackerDecodesPayloadToLogicalMessage(array $v): void
    {
        $frame = (string) hex2bin((string) $v['frame_hex']);
        $payload = substr($frame, 16);
        $p = new PurePacker();
        $off = 0;
        $decoded = $p->unpack($payload, $off);
        $this->assertSame(strlen($payload), $off, "consumed all payload bytes for {$v['name']}");
        $this->assertIsArray($decoded, "every S1 message payload is a positional array for {$v['name']}");
    }

    /**
     * THE cross-language byte lock: PurePacker must re-encode each client-sent message to the EXACT
     * bytes the Rust codec produced. hello_ack is included (encoding boot_epoch from its decimal
     * string yields the exact uint64 bytes); error_protocol is an Outcome the client never sends,
     * so it is decode-only and skipped here. A rmp-serde map-vs-array default mismatch, a field-order
     * bug, or an integer-width divergence all fail HERE rather than silently in S5.
     * @param array<string,mixed> $v
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('vectors')]
    public function testPurePackerEncodesMessageToExactVectorBytes(array $v): void
    {
        $name = (string) $v['name'];
        if (!in_array($name, self::CLIENT_ENCODED_MESSAGES, true)) {
            $this->markTestSkipped("{$name} is decode-only for the client in S1 (no message encoder)");
        }
        $fields = is_array($v['message']) ? $v['message'] : [];
        $payload = Message::encode($name, $fields, new PurePacker());
        $expected = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $this->assertSame(bin2hex($expected), bin2hex($payload),
            "PHP-encoded {$name} payload must byte-match the Rust-generated vector");
    }

    /**
     * The `hello_ack` byte lock above feeds {@see Message::encode} the vector's DECODED JSON, whose
     * `pools` are assoc arrays — so `Message::poolInfoArray` takes its `$entry['name']` KEY branch
     * and {@see PoolInfo::toWire} is never called on that path. `toWire()` has no other caller in
     * `src/`, so the OBJECT branch — the shape a real producer hands in — carried NO byte lock at
     * all, while the docblock on `poolInfoArray` claimed the vector exercised exactly it.
     *
     * This runs the SAME vector through the OTHER branch: rebuild `message.pools` as {@see PoolInfo}
     * instances and re-encode. A reorder inside `toWire()` (or a field dropped from it) now moves
     * the bytes and fails HERE, against the Rust-generated frame rather than against another PHP
     * assertion.
     */
    public function testHelloAckPoolsByteMatchTheVectorThroughThePoolInfoObjectBranch(): void
    {
        $v = self::loadVector('hello_ack.json');
        $message = is_array($v['message'] ?? null) ? $v['message'] : [];

        $pools = [];
        foreach (is_array($message['pools'] ?? null) ? $message['pools'] : [] as $entry) {
            $this->assertIsArray($entry, 'the hello_ack vector must carry assoc pool entries');
            $version = $entry['server_version'] ?? null;
            $pools[] = new PoolInfo(
                (string) ($entry['name'] ?? ''),
                (string) ($entry['kind'] ?? ''),
                $version === null ? null : (string) $version,
            );
        }
        // Without this the test would pass VACUOUSLY on an empty pool list, proving nothing about
        // toWire(). The vector carries two triples, the second with a nil version.
        $this->assertCount(2, $pools, 'the hello_ack vector must carry two pool triples for this lock');
        $this->assertNull($pools[1]->serverVersion, 'the nil-version arm must ride this lock too');

        $message['pools'] = $pools;
        $expected = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $this->assertSame(
            bin2hex($expected),
            bin2hex(Message::encode('hello_ack', $message, new PurePacker())),
            'hello_ack encoded through the PoolInfo OBJECT branch must byte-match the Rust vector',
        );
    }

    /** @param array<string,mixed> $v */
    #[\PHPUnit\Framework\Attributes\DataProvider('vectors')]
    public function testExtPackerDecodeMatchesPureWhenLoaded(array $v): void
    {
        if (!\extension_loaded('msgpack')) { $this->markTestSkipped('ext-msgpack not loaded (CI provisions it)'); }
        $payload = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $off = 0;
        $pure = (new PurePacker())->unpack($payload, $off);
        if (self::hasBigUint($pure)) {
            // ext-msgpack decodes a uint64 > PHP_INT_MAX to a LOSSY float; PurePacker returns the
            // exact decimal string and is authoritative. The two are not comparable here — pure-only
            // coverage lives in PurePackerTest::testUint64BeyondPhpIntDecodesToString.
            $this->markTestSkipped("vector {$v['name']} carries a uint64 > PHP_INT_MAX (ext-msgpack lossy)");
        }
        $off = 0;
        $ext = (new ExtPacker())->unpack($payload, $off);
        $this->assertEquals(json_encode($pure), json_encode($ext), "ext vs pure decode for {$v['name']}");
    }

    /** @return iterable<string, array{0:array<string,mixed>}> only the SQL EXEC vectors (Task S5). */
    public static function sqlVectors(): iterable
    {
        foreach (self::vectors() as $name => [$v]) {
            if (str_starts_with((string) ($v['name'] ?? ''), 'sql_exec_')) {
                yield $name => [$v];
            }
        }
    }

    /**
     * THE SQL cross-language byte lock (bespoke Value-splicing codec). For every sql_exec_* vector,
     * PHP must (a) re-encode the "message" JSON to the EXACT Rust-produced payload bytes, (b) decode
     * those bytes back to the message value, and (c) round-trip decode->encode to the same bytes.
     * Requests are the ExecRequest payload directly; responses are the terminal Outcome::Ok(ExecOk)
     * envelope, so PHP wraps the ExecOk body in `[OUTCOME_OK, body]`.
     * @param array<string,mixed> $v
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('sqlVectors')]
    public function testSqlVectorByteMatchesBothDirections(array $v): void
    {
        $name = (string) $v['name'];
        $payload = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $message = is_array($v['message']) ? $v['message'] : [];
        $p = new PurePacker();

        if (str_starts_with($name, 'sql_exec_request')) {
            // (a) encode direction
            $this->assertSame(bin2hex($payload), bin2hex(ExecRequest::encode($message, $p)),
                "PHP ExecRequest encode must byte-match {$name}");
            // (b) decode direction + full consumption
            $off = 0;
            $wire = $p->unpack($payload, $off);
            $this->assertSame(strlen($payload), $off, "consumed all payload bytes for {$name}");
            $this->assertIsArray($wire);
            $decoded = ExecRequest::mapFromWire($wire);
            $this->assertEquals($message, $decoded, "PHP ExecRequest decode==value for {$name}");
            // (c) fixpoint
            $this->assertSame(bin2hex($payload), bin2hex(ExecRequest::encode($decoded, $p)),
                "ExecRequest decode->encode fixpoint for {$name}");
            return;
        }

        // sql_exec_response*: the terminal Outcome::Ok(ExecOk) envelope.
        $this->assertSame(bin2hex($payload), bin2hex(self::wrapOk($p, ExecOk::encode($message, $p))),
            "PHP Outcome::Ok(ExecOk) encode must byte-match {$name}");
        $off = 0;
        $outcome = $p->unpack($payload, $off);
        $this->assertSame(strlen($payload), $off, "consumed all payload bytes for {$name}");
        $this->assertIsArray($outcome);
        $outcome = array_values($outcome);
        $this->assertSame(C::OUTCOME_OK, (int) $outcome[0], "terminal status is Ok for {$name}");
        $this->assertIsArray($outcome[1]);
        $decoded = ExecOk::mapFromWire($outcome[1]);
        $this->assertEquals($message, $decoded, "PHP ExecOk decode==value for {$name}");
        $this->assertSame(bin2hex($payload), bin2hex(self::wrapOk($p, ExecOk::encode($decoded, $p))),
            "ExecOk decode->encode fixpoint for {$name}");
    }

    /** @return iterable<string, array{0:array<string,mixed>}> the STREAM HEAD/DATA vectors (M1-S5 Task 1). */
    public static function streamVectors(): iterable
    {
        foreach (self::vectors() as $name => [$v]) {
            if (str_starts_with((string) ($v['name'] ?? ''), 'stream_head_')
                || str_starts_with((string) ($v['name'] ?? ''), 'stream_data_')
            ) {
                yield $name => [$v];
            }
        }
    }

    /**
     * THE STREAM cross-language byte lock (bespoke Value-splicing codec, mirrors the SQL EXEC lock
     * above). Neither `HEAD` nor `DATA` is wrapped in the `Outcome` envelope — they are plain
     * message payloads, exactly like an `ExecRequest` vector (see /proto/PROTOCOL.md §10). For
     * every stream_* vector, PHP must (a) re-encode the "message" JSON to the EXACT Rust-produced
     * payload bytes, (b) decode those bytes back to the message value with full consumption, and
     * (c) round-trip decode->encode to the same bytes.
     * @param array<string,mixed> $v
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('streamVectors')]
    public function testStreamVectorByteMatchesBothDirections(array $v): void
    {
        $name = (string) $v['name'];
        $payload = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $message = is_array($v['message']) ? $v['message'] : [];
        $p = new PurePacker();
        $isHead = str_starts_with($name, 'stream_head_');

        // (a) encode direction
        $encoded = $isHead ? StreamHead::encode($message, $p) : StreamData::encode($message, $p);
        $this->assertSame(bin2hex($payload), bin2hex($encoded),
            "PHP " . ($isHead ? 'StreamHead' : 'StreamData') . " encode must byte-match {$name}");

        // (b) decode direction + full consumption
        $off = 0;
        $wire = $p->unpack($payload, $off);
        $this->assertSame(strlen($payload), $off, "consumed all payload bytes for {$name}");
        $this->assertIsArray($wire);
        $decoded = $isHead ? StreamHead::mapFromWire($wire) : StreamData::mapFromWire($wire);
        $this->assertEquals($message, $decoded, "PHP " . ($isHead ? 'StreamHead' : 'StreamData') . " decode==value for {$name}");

        // (c) fixpoint
        $reencoded = $isHead ? StreamHead::encode($decoded, $p) : StreamData::encode($decoded, $p);
        $this->assertSame(bin2hex($payload), bin2hex($reencoded), "{$name} decode->encode fixpoint");
    }

    /** @return iterable<string, array{0:array<string,mixed>}> the request-bearing TX vectors (Task S7). */
    public static function txRequestVectors(): iterable
    {
        foreach (self::vectors() as $name => [$v]) {
            if (in_array((string) ($v['name'] ?? ''), ['tx_begin_request', 'tx_commit', 'tx_savepoint'], true)) {
                yield $name => [$v];
            }
        }
    }

    /**
     * THE TX cross-language byte lock (Value-free rmp-serde positional layout). For each request-bearing
     * TX vector, PHP must (a) re-encode the "message" JSON to the EXACT Rust-produced payload bytes,
     * (b) decode those bytes back to the message value with full consumption, and (c) round-trip
     * decode->encode to the same bytes. Closes the S6 deferral: PHP is now an independent arbiter of the
     * TX request wire.
     * @param array<string,mixed> $v
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('txRequestVectors')]
    public function testTxRequestVectorByteMatchesBothDirections(array $v): void
    {
        $name = (string) $v['name'];
        $payload = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $message = is_array($v['message']) ? $v['message'] : [];
        $p = new PurePacker();

        // (a) encode direction
        $this->assertSame(bin2hex($payload), bin2hex($this->encodeTxRequest($name, $message, $p)),
            "PHP {$name} encode must byte-match the Rust vector");

        // (b) decode direction + full consumption
        $off = 0;
        $wire = $p->unpack($payload, $off);
        $this->assertSame(strlen($payload), $off, "consumed all payload bytes for {$name}");
        $this->assertIsArray($wire);
        $decoded = $this->decodeTxRequest($name, $wire);
        $this->assertEquals($message, $decoded, "PHP {$name} decode==value");

        // (c) fixpoint
        $this->assertSame(bin2hex($payload), bin2hex($this->encodeTxRequest($name, $decoded, $p)),
            "{$name} decode->encode fixpoint");
    }

    /**
     * `tx_begin_response`: the terminal `Outcome::Ok(BeginResponse)` envelope. Encode by wrapping the
     * one-field BeginResponse body in `[OUTCOME_OK, body]`; decode by cracking the outer Outcome (raw
     * opaque body) then the inner BeginResponse. `tx_id` is a bounded native int (< 2^63), never a
     * decimal string.
     */
    public function testTxBeginResponseVectorByteMatchesBothDirections(): void
    {
        $v = self::loadVector('tx_begin_response.json');
        $message = is_array($v['message']) ? $v['message'] : [];
        $payload = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $p = new PurePacker();

        // (a) encode: wrap the BeginResponse body in Outcome::Ok.
        $encoded = Outcome::ok(BeginResponse::encode($message, $p))->encode($p);
        $this->assertSame(bin2hex($payload), bin2hex($encoded),
            'PHP Outcome::Ok(BeginResponse) encode must byte-match tx_begin_response');

        // (b) decode: crack the outer Outcome, then the inner BeginResponse body.
        $outcome = Outcome::decode($payload, $p);
        $this->assertTrue($outcome->isOk(), 'tx_begin_response is an Outcome::Ok');
        $this->assertSame(C::OUTCOME_OK, $outcome->status);
        $this->assertSame($message['status'], $outcome->status, 'status matches vector');
        $off = 0;
        $bodyWire = $p->unpack($outcome->body(), $off);
        $this->assertSame(strlen($outcome->body()), $off, 'consumed all BeginResponse body bytes');
        $this->assertIsArray($bodyWire);
        $begin = BeginResponse::mapFromWire($bodyWire);
        $this->assertSame($message['tx_id'], $begin['tx_id'], 'decoded tx_id matches vector');
        $this->assertIsInt($begin['tx_id'], 'tx_id is a bounded native int (< 2^63), not a decimal string');

        // (c) fixpoint
        $this->assertSame(bin2hex($payload), bin2hex(Outcome::ok(BeginResponse::encode($begin, $p))->encode($p)),
            'tx_begin_response decode->encode fixpoint');
    }

    /**
     * `error_protocol`: the terminal `Outcome::Error(ErrorPayload)` wire. Encode by wrapping the 7-field
     * ErrorPayload in `[OUTCOME_ERROR, body]`; decode by cracking the outer Outcome then the inner
     * ErrorPayload — exposing `branch` (`ErrorPayload[1]`), the byte the Task-3 taxonomy classifies on.
     * Grounds the three-branch error mapping in a real vector and closes the S6 error-wire boundary.
     */
    public function testErrorProtocolVectorByteMatchesBothDirections(): void
    {
        $v = self::loadVector('error_protocol.json');
        $message = is_array($v['message']) ? $v['message'] : [];
        $errorFields = is_array($message['error'] ?? null) ? $message['error'] : [];
        $payload = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $p = new PurePacker();

        // (a) encode: wrap the ErrorPayload in Outcome::Error.
        $encoded = Outcome::error(ErrorPayload::fromArray($errorFields))->encode($p);
        $this->assertSame(bin2hex($payload), bin2hex($encoded),
            'PHP Outcome::Error(ErrorPayload) encode must byte-match error_protocol');

        // (b) decode: crack the outer Outcome, then read the ErrorPayload fields.
        $outcome = Outcome::decode($payload, $p);
        $this->assertTrue($outcome->isError(), 'error_protocol is an Outcome::Error');
        $this->assertSame(C::OUTCOME_ERROR, $outcome->status);
        $this->assertSame($message['status'], $outcome->status, 'status matches vector');
        $err = $outcome->errorPayload();
        // `branch` is the wire byte the ErrorMapper classifies on (ErrorPayload[1]).
        $this->assertSame($errorFields['branch'], $err->branch, 'decoded branch matches vector (ErrorPayload[1])');
        $this->assertSame(C::BRANCH_NON_RETRYABLE, $err->branch, 'error_protocol is a NonRetryable branch');
        $this->assertSame($errorFields['code'], $err->code, 'decoded code matches vector');
        $this->assertSame($errorFields['message'], $err->message, 'decoded message matches vector');
        $this->assertEquals($errorFields, $err->toArray(), 'PHP ErrorPayload decode==value for error_protocol');

        // (c) fixpoint
        $this->assertSame(bin2hex($payload), bin2hex(Outcome::error($err)->encode($p)),
            'error_protocol decode->encode fixpoint');
    }

    /**
     * `error_mysql_errno`: the first vector carrying a NON-NULL `errno` alongside a SQLSTATE. Locks
     * that PHP's `ErrorPayload` moves BOTH fields, independently, in both directions — the pair a
     * Doctrine MySQL ExceptionConverter keys on (it matches the errno EXCLUSIVELY; MySQL's `23000`
     * covers both a duplicate key and a NOT NULL violation, so the SQLSTATE cannot substitute).
     */
    public function testErrorMysqlErrnoVectorByteMatchesBothDirections(): void
    {
        $v = self::loadVector('error_mysql_errno.json');
        $message = is_array($v['message']) ? $v['message'] : [];
        $errorFields = is_array($message['error'] ?? null) ? $message['error'] : [];
        $payload = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $p = new PurePacker();

        $encoded = Outcome::error(ErrorPayload::fromArray($errorFields))->encode($p);
        $this->assertSame(bin2hex($payload), bin2hex($encoded),
            'PHP Outcome::Error(ErrorPayload) encode must byte-match error_mysql_errno');

        $err = Outcome::decode($payload, $p)->errorPayload();
        $this->assertSame(1062, $err->errno, 'the vendor errno must survive the round trip');
        $this->assertSame('23000', $err->sqlstate, 'the SQLSTATE is carried independently');
        $this->assertEquals($errorFields, $err->toArray());
        $this->assertSame(bin2hex($payload), bin2hex(Outcome::error($err)->encode($p)),
            'error_mysql_errno decode->encode fixpoint');
    }

    /**
     * @param array<string,mixed> $message
     */
    private function encodeTxRequest(string $name, array $message, PurePacker $p): string
    {
        return match ($name) {
            'tx_begin_request' => BeginRequest::encode($message, $p),
            'tx_commit' => TxControl::encode($message, $p),
            'tx_savepoint' => SavepointRequest::encode($message, $p),
            default => throw new \LogicException("unexpected tx request vector {$name}"),
        };
    }

    /**
     * @param array<int,mixed> $wire
     * @return array<string,mixed>
     */
    private function decodeTxRequest(string $name, array $wire): array
    {
        return match ($name) {
            'tx_begin_request' => BeginRequest::mapFromWire($wire),
            'tx_commit' => TxControl::mapFromWire($wire),
            'tx_savepoint' => SavepointRequest::mapFromWire($wire),
            default => throw new \LogicException("unexpected tx request vector {$name}"),
        };
    }

    /**
     * COVERAGE GUARD (M1-S7). A golden vector earns its cross-language byte lock ONLY through its
     * NAME PREFIX — `sqlVectors()` keys on `sql_exec_`, `streamVectors()` on `stream_head_`/
     * `stream_data_`. A vector named outside those prefixes still passes the generic header/unpack
     * tests above, so it LOOKS covered while getting no byte lock at all. This asserts (a) the four
     * M1-S7 canonical-tag vectors landed inside the byte-locked providers, and (b) every committed
     * vector is accounted for by some byte-lock test — a derived accounting, not a hardcoded count,
     * so adding a well-named vector never needs an edit here but a misnamed one fails immediately.
     */
    public function testEveryCommittedVectorIsByteLocked(): void
    {
        $prefixLocked = [];
        foreach (self::sqlVectors() as [$v]) { $prefixLocked[] = (string) $v['name']; }
        foreach (self::streamVectors() as [$v]) { $prefixLocked[] = (string) $v['name']; }

        // The four vectors this task added — each must be inside a PREFIX-keyed provider.
        foreach ([
            'sql_exec_response_types_scalars',
            'sql_exec_response_types_edge',
            'sql_exec_response_types_u64',
            'stream_data_types',
        ] as $name) {
            $this->assertContains($name, $prefixLocked,
                "{$name} must match the sql_exec_*/stream_data_* prefix rule or it gets NO byte lock");
        }

        // Vectors byte-locked by a name-keyed test rather than a prefix provider.
        // DERIVED, not a parallel list (charter: a completeness check must be able to fail).
        // Every byte-lock test in this class reaches its fixture through `self::loadVector('X.json')`,
        // so the set of name-locked vectors IS the set of names that call appears with. Adding a
        // name to a hand-written array would have made this guard certify a vector that has NO
        // byte-lock test at all — which is precisely the failure mode it exists to catch.
        $namedLocked = self::namesLoadedByAByteLockTest();
        foreach (self::txRequestVectors() as [$v]) { $namedLocked[] = (string) $v['name']; }
        // The core messages the client encodes (testPurePackerEncodesMessageToExactVectorBytes) —
        // read from the SAME const that test filters on, so the two can never disagree.
        $coreLocked = self::CLIENT_ENCODED_MESSAGES;

        $locked = array_merge($prefixLocked, $namedLocked, $coreLocked);
        $all = [];
        foreach (self::vectors() as [$v]) { $all[] = (string) $v['name']; }
        $unlocked = array_values(array_diff($all, $locked));
        $this->assertSame([], $unlocked,
            'these committed vectors have NO cross-language byte lock (wrong name prefix?): '
            . implode(', ', $unlocked));
    }

    /**
     * The vector names some test in THIS file loads by name, scraped from its own source.
     *
     * Deliberately a source scan and not a constant: the thing being asserted is "a byte-lock test
     * exists for this vector", and the only honest evidence of that is a call site. `__FILE__` keeps
     * it self-referential, so the scan cannot drift from the file it is scanning.
     *
     * @return list<string>
     */
    private static function namesLoadedByAByteLockTest(): array
    {
        $src = (string) file_get_contents(__FILE__);
        $m = [];
        // Anchored to an ASSIGNMENT, not a bare mention: the evidence here is textual, so an
        // unanchored pattern lets a COMMENT count as a call site. Proven in the M1-S8a Task 3
        // review — with the byte-lock test deleted and a comment reading
        // `// TODO: someday write self::loadVector('error_mysql_errno.json') here.`, this guard
        // went GREEN over an unlocked vector. Every real call site is `$x = self::loadVector(...)`,
        // which this matches and no comment does.
        preg_match_all(
            "/\\\$\\w+\\s*=\\s*self::loadVector\\(\\s*'([A-Za-z0-9_]+)\\.json'\\s*\\)/",
            $src,
            $m,
        );
        /** @var list<string> $names */
        $names = array_values(array_unique(is_array($m[1] ?? null) ? $m[1] : []));
        return $names;
    }

    /** @return array<string,mixed> */
    private static function loadVector(string $file): array
    {
        /** @var array<string,mixed> $v */
        $v = json_decode((string) file_get_contents(self::DIR . '/' . $file), true, 512, JSON_THROW_ON_ERROR);
        return $v;
    }

    private static function wrapOk(PurePacker $p, string $body): string
    {
        return $p->packArrayLen(2) . $p->packUint(C::OUTCOME_OK) . $body;
    }

    /** True if $v (recursively) contains a decimal string that exceeds PHP_INT_MAX — PurePacker's
     *  representation of a uint64 the msgpack extension cannot decode losslessly. */
    private static function hasBigUint(mixed $v): bool
    {
        if (is_array($v)) {
            foreach ($v as $x) { if (self::hasBigUint($x)) { return true; } }
            return false;
        }
        if (!is_string($v) || !preg_match('/^\d+$/', $v)) { return false; }
        $s = ltrim($v, '0');
        if ($s === '') { $s = '0'; }
        $max = '9223372036854775807';
        return strlen($s) > strlen($max) || (strlen($s) === strlen($max) && strcmp($s, $max) > 0);
    }
}
