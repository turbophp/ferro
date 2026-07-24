<?php // /php/client/tests/Conformance/VectorConformanceTest.php
declare(strict_types=1);
namespace Ferro\Tests\Conformance;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\{PurePacker, ExtPacker};
use PHPUnit\Framework\TestCase;

final class VectorConformanceTest extends TestCase
{
    private const DIR = __DIR__ . '/../../../../proto/vectors';

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
        if (!in_array($name, ['hello', 'hello_ack', 'ping', 'pong', 'goodbye', 'window_update'], true)) {
            $this->markTestSkipped("{$name} is decode-only for the client in S1 (no message encoder)");
        }
        $fields = is_array($v['message']) ? $v['message'] : [];
        $payload = Message::encode($name, $fields, new PurePacker());
        $expected = substr((string) hex2bin((string) $v['frame_hex']), 16);
        $this->assertSame(bin2hex($expected), bin2hex($payload),
            "PHP-encoded {$name} payload must byte-match the Rust-generated vector");
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
