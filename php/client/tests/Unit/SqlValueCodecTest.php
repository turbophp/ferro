<?php // /php/client/tests/Unit/SqlValueCodecTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Error\ProtocolException;
use Ferro\Client\ExecCodec;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\SqlValueCodec;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * `SqlValueCodec` is the {tag, data} <-> wire chokepoint the SQL/STREAM message codecs share, and
 * `encode()` is ALSO the bind path — so a malformed payload here becomes a corrupt WRITE, not just
 * a bad test fixture. These tests pin the M1-S7 arms' refusal behaviour (hazard 30/F18): the arms
 * must NOT route through the `toStr`/`toInt` narrowing helpers, whose ''/0 fallbacks are exactly
 * the silent miscast SPEC §9.1 exists to prevent.
 */
final class SqlValueCodecTest extends TestCase
{
    /** Round-trip fromWire(encode(x)) === x for every S7 canonical payload. */
    #[DataProvider('s7Values')]
    public function testS7TagRoundTripsThroughTheWireShape(int $tag, mixed $data): void
    {
        $p = new PurePacker();
        $bytes = SqlValueCodec::encode($p, ['tag' => $tag, 'data' => $data]);
        $off = 0;
        $pair = $p->unpack($bytes, $off);
        $this->assertSame(strlen($bytes), $off, 'consumed all bytes');
        $this->assertSame(['tag' => $tag, 'data' => $data], SqlValueCodec::fromWire($pair));
    }

    /** @return array<string, array{0:int, 1:mixed}> */
    public static function s7Values(): array
    {
        return [
            'DECIMAL' => [C::TAG_DECIMAL, '-12345.6700'],
            'DECIMAL NaN' => [C::TAG_DECIMAL, 'NaN'],
            'DATE' => [C::TAG_DATE, '2026-08-05'],
            'DATE infinity' => [C::TAG_DATE, 'infinity'],
            'DATE zero' => [C::TAG_DATE, '0000-00-00'],
            'TIME' => [C::TAG_TIME, '13:45:07'],
            'TIME 24h' => [C::TAG_TIME, '24:00:00'],
            'TIME negative' => [C::TAG_TIME, '-838:59:58.000001'],
            'TIMESTAMP' => [C::TAG_TIMESTAMP, '2026-08-05 13:45:07.250000'],
            'TIMESTAMP whole second' => [C::TAG_TIMESTAMP, '2026-08-05 13:45:07'],
            'TIMESTAMP zero' => [C::TAG_TIMESTAMP, '0000-00-00 00:00:00'],
            'TIMESTAMPTZ' => [C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07.250000Z'],
            'TIMESTAMPTZ -infinity' => [C::TAG_TIMESTAMPTZ, '-infinity'],
            'UUID' => [C::TAG_UUID, '6ba7b810-9dad-11d1-80b4-00c04fd430c8'],
            'JSON' => [C::TAG_JSON, '{"a":[1,2,{"b":null}],"n":"caf' . "\xc3\xa9" . '"}'],
            'U64 small' => [C::TAG_U64, 5],
            'U64 at u32::MAX' => [C::TAG_U64, 0xffffffff],
        ];
    }

    /**
     * A `> PHP_INT_MAX` U64 is a decimal STRING in both directions — the value's PHP type follows
     * its MAGNITUDE, not its tag (PROTOCOL.md §3.2). `toInt` would saturate it to PHP_INT_MAX.
     */
    public function testU64AbovePhpIntMaxStaysADecimalString(): void
    {
        $p = new PurePacker();
        $bytes = SqlValueCodec::encode($p, ['tag' => C::TAG_U64, 'data' => '18446744073709551615']);
        $this->assertSame("\x92\x03\xcf\xff\xff\xff\xff\xff\xff\xff\xff", $bytes);
        $off = 0;
        $this->assertSame(
            ['tag' => C::TAG_U64, 'data' => '18446744073709551615'],
            SqlValueCodec::fromWire($p->unpack($bytes, $off))
        );
    }

    /** One per text-canonical tag: a non-string payload is REFUSED, never coerced to ''. */
    #[DataProvider('s7StrTags')]
    public function testNonStringPayloadThrowsNamingTheTag(int $tag): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessage("TypedValue tag {$tag}: expected a canonical-text string payload");
        SqlValueCodec::encode(new PurePacker(), ['tag' => $tag, 'data' => ['not', 'a', 'string']]);
    }

    /** @return array<string, array{0:int}> */
    public static function s7StrTags(): array
    {
        return [
            'DECIMAL' => [C::TAG_DECIMAL],
            'DATE' => [C::TAG_DATE],
            'TIME' => [C::TAG_TIME],
            'TIMESTAMP' => [C::TAG_TIMESTAMP],
            'TIMESTAMPTZ' => [C::TAG_TIMESTAMPTZ],
            'UUID' => [C::TAG_UUID],
            'JSON' => [C::TAG_JSON],
        ];
    }

    /** The same refusal for a `null` payload — the empty-string-write case specifically. */
    #[DataProvider('s7StrTags')]
    public function testNullPayloadThrowsRatherThanWritingAnEmptyString(int $tag): void
    {
        $this->expectException(CodecException::class);
        SqlValueCodec::encode(new PurePacker(), ['tag' => $tag, 'data' => null]);
    }

    public function testU64RejectsANonScalarPayload(): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessage('TypedValue tag 3: expected a non-negative int or decimal string');
        SqlValueCodec::encode(new PurePacker(), ['tag' => C::TAG_U64, 'data' => [1, 2]]);
    }

    public function testU64RejectsANegativeIntPayload(): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessage('TypedValue tag 3: expected a non-negative int or decimal string');
        SqlValueCodec::encode(new PurePacker(), ['tag' => C::TAG_U64, 'data' => -1]);
    }

    /** A tag outside the implemented set stays a loud refusal, not a codec crash. */
    public function testDeferredTagIsStillRejected(): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessage('unsupported TypedValue tag ' . C::TAG_ARRAY);
        SqlValueCodec::encode(new PurePacker(), ['tag' => C::TAG_ARRAY, 'data' => []]);
    }

    // ---- F6: the READ-side mirror of the TAG_BYTES refusal -----------------------------------

    /**
     * **The decode-side mirror of {@see BytesBindTest::testANonStringNonListBytesPayloadIsRefused}.**
     *
     * M1-S8a closed the `TAG_BYTES` coercion on the ENCODE side (a non-string payload used to emit
     * `c400`, an empty bin) and argued the case in that method's docblock: "a silently-empty blob is
     * exactly the silent corrupt WRITE §9.1 exists to prevent". The READ arm was left as
     * `is_string($data) ? $data : ''`, so ANY non-string payload on a `bytea`/`BLOB` column decoded
     * to an empty blob with no exception — the read silently lost the data, and a read -> write-back
     * round trip then PERSISTED the emptiness. One direction of the pair is not a policy.
     *
     * The positive control below is what makes this more than "some input throws": the SAME shape
     * with a real `bin` payload still decodes to the exact bytes.
     */
    #[DataProvider('nonBinBytesPayloads')]
    public function testANonBinBytesPayloadIsRefusedOnTheReadPath(mixed $payload, string $expectedType): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessage(
            'TypedValue tag ' . C::TAG_BYTES . ': expected a bin payload (a byte string), got ' . $expectedType,
        );
        SqlValueCodec::fromWire([C::TAG_BYTES, $payload]);
    }

    /** @return array<string, array{0:mixed, 1:string}> every non-`bin` msgpack family, by decoded type */
    public static function nonBinBytesPayloads(): array
    {
        return [
            'nil'   => [null, 'null'],
            'int'   => [7, 'int'],
            'float' => [1.5, 'float'],
            'bool'  => [true, 'bool'],
            'array' => [[1, 2, 3], 'array'],
        ];
    }

    /** POSITIVE CONTROL: a real `bin` payload still decodes, so the refusal above is not blanket. */
    public function testARealBinBytesPayloadStillDecodesToItsBytes(): void
    {
        $this->assertSame(
            ['tag' => C::TAG_BYTES, 'data' => [0x00, 0x01, 0xff]],
            SqlValueCodec::fromWire([C::TAG_BYTES, "\x00\x01\xff"]),
        );
        // …and the empty bin is a LEGITIMATE empty blob, distinguishable from the fault above only
        // because the fault now throws. This assertion is why the refusal had to be an exception
        // rather than a sentinel.
        $this->assertSame(
            ['tag' => C::TAG_BYTES, 'data' => []],
            SqlValueCodec::fromWire([C::TAG_BYTES, '']),
        );
    }

    /**
     * The fault must reach the CALLER as a §9.2 protocol fault on the real read path, not die inside
     * the codec — so this drives a whole hand-built `ExecOk` terminal (one `bytea` column, one row,
     * a `nil` where the `bin` belongs) through {@see ExecCodec::decode}, the exact route a row takes
     * off the wire. Asserted from the user's vantage point: `ProtocolException`, never a row
     * carrying an empty blob.
     */
    public function testAMalformedBytesCellSurfacesAsAProtocolExceptionFromExecCodec(): void
    {
        $p = new PurePacker();
        $codec = new ExecCodec(
            new M1ValuePolicy(new TypePolicyOptions()),
            new PlanCache(),
            $p,
            $p,
        );

        // Positive control FIRST: the same terminal with a real bin decodes to the blob's bytes.
        $ok = $codec->decode(Outcome::ok(self::oneBytesCellExecOk($p, $p->packBin("\x00\xff"))));
        $this->assertSame([['blob' => "\x00\xff"]], $codec->assocRows($ok));

        $this->expectException(ProtocolException::class);
        $this->expectExceptionMessage('expected a bin payload (a byte string), got null');
        $codec->decode(Outcome::ok(self::oneBytesCellExecOk($p, $p->packNil())));
    }

    /** A 5-field `ExecOk` body: one `blob` BYTES column, one row whose only cell carries `$payload`. */
    private static function oneBytesCellExecOk(PurePacker $p, string $payload): string
    {
        $cols = $p->packArrayLen(1) . $p->packArrayLen(2) . $p->packStr('blob') . $p->packUint(C::TAG_BYTES);
        $cell = $p->packArrayLen(2) . $p->packUint(C::TAG_BYTES) . $payload;
        $rows = $p->packArrayLen(1) . $p->packArrayLen(1) . $cell;
        $stats = $p->packArrayLen(4) . $p->packUint(0) . $p->packUint(0) . $p->packUint(0) . $p->packUint(0);

        return $p->packArrayLen(5) . $cols . $rows . $p->packUint(0) . $p->packNil() . $stats;
    }
}
