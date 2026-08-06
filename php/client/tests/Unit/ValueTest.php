<?php // /php/client/tests/Unit/ValueTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Value;
use Ferro\Protocol\Msgpack\PurePacker;
use PHPUnit\Framework\TestCase;

final class ValueTest extends TestCase
{
    public function testCanonicalBytesMatchRust(): void
    {
        $p = new PurePacker();
        $this->assertSame("\x92\x00\xc0", Value::null()->encode($p));
        $this->assertSame("\x92\x01\xc3", Value::bool(true)->encode($p));
        $this->assertSame("\x92\x02\x01", Value::i64(1)->encode($p));
        $this->assertSame("\x92\x02\xcc\xc8", Value::i64(200)->encode($p)); // uint8, matches Rust
        $this->assertSame("\x92\x02\xd1\xff\x38", Value::i64(-200)->encode($p)); // int16
        $this->assertSame("\x92\x06\xa2hi", Value::text('hi')->encode($p));
        $this->assertSame("\x92\x07\xc4\x03\x01\x02\x03", Value::bytes("\x01\x02\x03")->encode($p));
    }

    /**
     * M1-S7 canonical tags (/proto/PROTOCOL.md §3.2): seven text-canonical payloads on the msgpack
     * `str` family, plus `U64` on the uint family. The payloads here are the exact ones the golden
     * vectors carry, so this unit test and the cross-language byte lock cannot disagree.
     */
    public function testS7CanonicalTagBytesMatchRust(): void
    {
        $p = new PurePacker();
        $this->assertSame("\x92\x05\xab-12345.6700", Value::decimal('-12345.6700')->encode($p));
        $this->assertSame("\x92\x08\xaa2026-08-05", Value::date('2026-08-05')->encode($p));
        $this->assertSame("\x92\x09\xa813:45:07", Value::time('13:45:07')->encode($p));
        $this->assertSame("\x92\x0a\xba2026-08-05 13:45:07.250000",
            Value::timestamp('2026-08-05 13:45:07.250000')->encode($p));
        $this->assertSame("\x92\x0b\xbb2026-08-05T13:45:07.250000Z",
            Value::timestamptz('2026-08-05T13:45:07.250000Z')->encode($p));
        // 36 chars > fixstr's 31 => str8 (0xd9 0x24).
        $this->assertSame("\x92\x0c\xd9\x246ba7b810-9dad-11d1-80b4-00c04fd430c8",
            Value::uuid('6ba7b810-9dad-11d1-80b4-00c04fd430c8')->encode($p));
        $json = '{"a":[1,2,{"b":null}],"n":"caf' . "\xc3\xa9" . '"}';
        $this->assertSame("\x92\x0d\xd9\x22" . $json, Value::json($json)->encode($p),
            'the raw UTF-8 JSON document text rides `str` verbatim (34 bytes => str8)');
    }

    /**
     * `U64` uses the CANONICAL NARROWING LADDER, not a fixed 0xcf — byte-identical to rmp's
     * `write_uint`. A marker-strict reader on either side is a defect (PROTOCOL.md §3.2).
     */
    public function testU64UsesTheCanonicalNarrowingLadder(): void
    {
        $p = new PurePacker();
        $this->assertSame("\x92\x03\x00", Value::u64(0)->encode($p));          // positive fixint
        $this->assertSame("\x92\x03\x05", Value::u64(5)->encode($p));          // positive fixint
        $this->assertSame("\x92\x03\xcc\xc8", Value::u64(200)->encode($p));    // uint8
        $this->assertSame("\x92\x03\xcd\x01\x00", Value::u64(256)->encode($p)); // uint16
        $this->assertSame("\x92\x03\xce\xff\xff\xff\xff", Value::u64(0xffffffff)->encode($p)); // uint32
    }

    /**
     * A `U64` above PHP_INT_MAX arrives from the wire as a DECIMAL STRING (PurePacker's uint64
     * representation) and must round-trip byte-exactly. This is the regression net for hazard 29:
     * a `packInt(self::toInt(...))` arm cannot emit above PHP_INT_MAX, so it would silently
     * saturate `u64::MAX` to `7fffffffffffffff` — same 0xcf marker, corrupt value.
     */
    public function testU64AbovePhpIntMaxRoundTripsExactly(): void
    {
        $p = new PurePacker();
        $bytes = Value::u64('18446744073709551615')->encode($p);
        $this->assertSame("\x92\x03\xcf\xff\xff\xff\xff\xff\xff\xff\xff", $bytes,
            'u64::MAX must encode to the exact uint64 bytes, not a PHP_INT_MAX saturation');
        // The explicit marker check the brief asks for: above u32::MAX the payload IS a uint64.
        $this->assertSame(0xcf, ord($bytes[2]), 'TAG_U64 payload marker above u32::MAX is 0xcf');

        $off = 0;
        $decoded = $p->unpack($bytes, $off);
        $this->assertSame(strlen($bytes), $off);
        $this->assertSame([3, '18446744073709551615'], $decoded,
            'decode returns the exact decimal string, not a lossy int/float');
    }

    /**
     * The BIND-path guards (hazard 30/F18). A `Value` reconstructed from the wire can carry any
     * payload shape; re-encoding a malformed one must THROW, never coerce. `self::toStr` would
     * make it an empty-string WRITE; `self::toInt` would saturate a u64.
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('s7StrTags')]
    public function testNonStringPayloadForATextCanonicalTagThrows(int $tag, string $name): void
    {
        $p = new PurePacker();
        // Build [tag, payload] with a NON-string payload and decode it into a Value (the only way
        // such a Value can exist — the factories are `string`-typed).
        $wire = $p->packArrayLen(2) . $p->packInt($tag) . $p->packInt(42);
        $off = 0;
        $v = Value::decode($p, $wire, $off);
        $this->expectException(CodecException::class);
        $this->expectExceptionMessage("TypedValue tag {$tag}: expected a canonical-text string payload");
        $v->encode($p);
        $this->fail("tag {$name} accepted a non-string payload");
    }

    /** @return array<string, array{0:int, 1:string}> */
    public static function s7StrTags(): array
    {
        return [
            'DECIMAL' => [C::TAG_DECIMAL, 'DECIMAL'],
            'DATE' => [C::TAG_DATE, 'DATE'],
            'TIME' => [C::TAG_TIME, 'TIME'],
            'TIMESTAMP' => [C::TAG_TIMESTAMP, 'TIMESTAMP'],
            'TIMESTAMPTZ' => [C::TAG_TIMESTAMPTZ, 'TIMESTAMPTZ'],
            'UUID' => [C::TAG_UUID, 'UUID'],
            'JSON' => [C::TAG_JSON, 'JSON'],
        ];
    }

    /** The U64 arm's own guard: neither a negative int nor a non-numeric string is a uint. */
    #[\PHPUnit\Framework\Attributes\DataProvider('badU64Payloads')]
    public function testU64RejectsANonUintPayload(int|string $bad): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessage('TypedValue tag 3: expected a non-negative int or decimal string');
        Value::u64($bad)->encode(new PurePacker());
    }

    /** @return array<string, array{0:int|string}> */
    public static function badU64Payloads(): array
    {
        return ['negative int' => [-1], 'non-numeric string' => ['12a'], 'signed string' => ['-1']];
    }
}
