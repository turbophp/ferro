<?php // /php/client/tests/Protocol/F64SpecialsByteIdentityTest.php
declare(strict_types=1);
namespace Ferro\Tests\Protocol;

use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\Value;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * F64 specials byte-identity (T1-review #3, the PHP half). JSON can't carry NaN/±Inf/-0.0, so a
 * golden vector can't lock them — this asserts PHP's `pack('E', $f)` (double, big-endian) emits the
 * SAME 8 IEEE-754 bytes the Rust codec's `f64::to_be_bytes()` does (see the Rust `f64_specials_byte_
 * identity` test). The expected byte strings below are the canonical IEEE-754 big-endian encodings —
 * they are the shared contract both languages must hit.
 */
final class F64SpecialsByteIdentityTest extends TestCase
{
    /** @return array<string, array{0:float, 1:string}> name => [value, expected 8 BE bytes] */
    public static function specials(): array
    {
        return [
            // canonical quiet NaN
            '+NaN'  => [NAN,  "\x7f\xf8\x00\x00\x00\x00\x00\x00"],
            '+Inf'  => [INF,  "\x7f\xf0\x00\x00\x00\x00\x00\x00"],
            '-Inf'  => [-INF, "\xff\xf0\x00\x00\x00\x00\x00\x00"],
            '-0.0'  => [-0.0, "\x80\x00\x00\x00\x00\x00\x00\x00"],
        ];
    }

    #[DataProvider('specials')]
    public function testPackFloat64MatchesCanonicalBigEndianBytes(float $f, string $expected): void
    {
        $p = new PurePacker();
        // msgpack float64 = marker 0xcb + 8 big-endian IEEE-754 bytes.
        $this->assertSame("\xcb" . $expected, $p->packFloat64($f), 'float64 marker + BE bytes');
        // pack('E', ...) is the big-endian double PHP uses; confirm it is what we asserted.
        $this->assertSame($expected, pack('E', $f), "pack('E') is big-endian IEEE-754");
    }

    #[DataProvider('specials')]
    public function testValueF64WireShape(float $f, string $expected): void
    {
        // A full Value F64 is the wire 2-array [tag, payload]: fixarray(2) 0x92, the F64 tag as a
        // positive fixint, then the float64. Mirrors the Rust `Value::F64(f).encode()` byte layout.
        $p = new PurePacker();
        $wire = Value::f64($f)->encode($p);
        $this->assertSame("\x92" . chr(C::TAG_F64) . "\xcb" . $expected, $wire);
    }
}
