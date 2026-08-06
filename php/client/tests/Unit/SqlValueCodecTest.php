<?php // /php/client/tests/Unit/SqlValueCodecTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
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
}
