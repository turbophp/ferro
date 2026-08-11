<?php // /php/client/tests/Unit/I64BoundaryDecodeTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Value\CanonicalText;
use Ferro\Client\Value\M0ValuePolicy;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Client\Value\ValuePolicy;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\SqlValueCodec;
use Ferro\Protocol\Value;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * **M1-S8c — the `I64` range guard.** Every `int64` the engine can put on the wire must come back as
 * the IDENTICAL PHP `int`, on every value policy.
 *
 * THE DEFECT THIS PINS (measured live against PG 17 through `Client\Connection`, no DBAL anywhere,
 * before the fix): `4294967295` decoded to an `int` and `4294967296` THREW
 * `ProtocolException: value tag 2: expected a int payload, got string`. So every `bigint` past
 * 4.29e9 — ordinary application data, every large auto-increment key, every epoch-millis column —
 * was unreadable on every backend and every policy.
 *
 * MECHANISM, which is why the cases below are chosen the way they are: the canonical narrowing
 * ladder ({@see PurePacker::packInt}, mirroring `rmp`'s `write_sint`) sends every NON-NEGATIVE
 * integer out under an UNSIGNED marker, so `2^32` rides `0xcf` — an 8-byte *unsigned* limb — exactly
 * like a `u64` does, while `2^32-1` still fits `0xce`. `PurePacker::be()` handed the whole `0xcf`
 * family back as a decimal STRING and `CanonicalText::requireInt` is (rightly) `is_int()`-strict.
 * **The boundary was therefore exactly 2^32 — not `PHP_INT_MAX`, not the 2^53 float cliff, and not
 * anywhere on the negative side** (negatives ride `0xd0..0xd3` and always decoded to `int`). A test
 * written against a midpoint like `4294967296` alone would pass over the marker turnover at
 * `0xce`/`0xcf`, the `PHP_INT_MAX` turnover this fix MOVED the string form to, and every negative.
 *
 * Each case asserts THREE things — the exact wire bytes, the packer's decoded PHP type, and the
 * policy-decoded value — so a regression in the encoder ladder, in `be()`, or in a policy arm is a
 * different failure rather than the same one.
 */
final class I64BoundaryDecodeTest extends TestCase
{
    /**
     * The boundaries, each with the msgpack marker the canonical ladder MUST choose for it.
     *
     * @return array<string, array{0: int, 1: string}>
     */
    public static function boundaries(): array
    {
        return [
            // --- the negative side: signed markers, never affected by the defect, still pinned ---
            'PHP_INT_MIN'      => [PHP_INT_MIN,      'd38000000000000000'],
            '-(2^53)-1'        => [-9007199254740993, 'd3ffdfffffffffffff'],
            '-(2^53)'          => [-9007199254740992, 'd3ffe0000000000000'],
            '-(2^32)'          => [-4294967296,      'd3ffffffff00000000'],
            '-(2^32)+1'        => [-4294967295,      'd3ffffffff00000001'],
            '-(2^31)-1'        => [-2147483649,      'd3ffffffff7fffffff'],
            '-(2^31)'          => [-2147483648,      'd280000000'],
            'int -1'           => [-1,               'ff'],
            // --- zero and the small end ---
            'int 0'            => [0,                '00'],
            'int 1'            => [1,                '01'],
            // --- the 32-bit boundaries ---
            '2^31-1'           => [2147483647,       'ce7fffffff'],
            '2^31'             => [2147483648,       'ce80000000'],
            // --- THE DEFECT BOUNDARY: the last uint32 and the first uint64 ---
            '2^32-1'           => [4294967295,       'ceffffffff'],
            '2^32'             => [4294967296,       'cf0000000100000000'],
            '2^32+1'           => [4294967297,       'cf0000000100000001'],
            // --- the float-precision cliff: (float) is lossy from here up, (int) must not be ---
            '2^53-1'           => [9007199254740991, 'cf001fffffffffffff'],
            '2^53'             => [9007199254740992, 'cf0020000000000000'],
            '2^53+1'           => [9007199254740993, 'cf0020000000000001'],
            // --- the top of int64 ---
            'PHP_INT_MAX-1'    => [9223372036854775806, 'cf7ffffffffffffffe'],
            'PHP_INT_MAX'      => [PHP_INT_MAX,      'cf7fffffffffffffff'],
        ];
    }

    /** @return list<array{0: string, 1: ValuePolicy}> */
    public static function policies(): array
    {
        return [
            ['M1ValuePolicy (the native default)', new M1ValuePolicy(new TypePolicyOptions())],
            ['RawStringValuePolicy (the DBAL hand-off)', new RawStringValuePolicy()],
            ['M0ValuePolicy', new M0ValuePolicy()],
        ];
    }

    /**
     * THE GUARD. The full read path — `Value::i64` → the canonical ladder → `PurePacker::unpack` →
     * `SqlValueCodec::fromWire` → every {@see ValuePolicy} — must hand back the identical `int`.
     */
    #[DataProvider('boundaries')]
    public function testEveryInt64BoundaryDecodesToTheIdenticalInt(int $n, string $markerHex): void
    {
        $p = new PurePacker();
        $frame = Value::i64($n)->encode($p);

        // 1. the ENCODER's ladder: `92` fixarray(2) + `02` TAG_I64 + the marker under test.
        $this->assertSame(
            '9202' . $markerHex,
            bin2hex($frame),
            'the canonical narrowing ladder must not change silently — the marker IS the case',
        );

        // 2. the DECODER's PHP type: an int64 is always an `int`, never the decimal-string form.
        $off = 0;
        $pair = $p->unpack($frame, $off);
        $this->assertSame(strlen($frame), $off, 'unpack must consume exactly the frame');
        $cell = SqlValueCodec::fromWire($pair);
        $this->assertSame(C::TAG_I64, $cell['tag']);
        $this->assertIsInt(
            $cell['data'],
            'PurePacker::be() handed back a decimal STRING for this limb — that IS the S8c defect '
            . '(every 0xcf-marked value was unreadable, from 2^32 up)',
        );

        // 3. every POLICY, since a row reaches the user through one of them.
        foreach (self::policies() as [$label, $policy]) {
            $this->assertSame($n, $policy->decode($cell['tag'], $cell['data']), $label);
        }
    }

    /**
     * The `(float)` cliff, stated as a property rather than trusted: from 2^53 up, a `float` round
     * trip loses integers, so "it decoded to something numerically close" is NOT the assertion.
     * `2^53+1` is the smallest positive integer a float cannot represent.
     */
    public function testTheFloatCliffIsRealSoTheIntPathIsLoadBearing(): void
    {
        $this->assertSame(9007199254740992, (int) (float) 9007199254740993, 'sanity: float loses it');
        $this->assertNotSame(9007199254740993, (int) (float) 9007199254740993);

        $p = new PurePacker();
        $off = 0;
        $cell = SqlValueCodec::fromWire($p->unpack(Value::i64(9007199254740993)->encode($p), $off));
        $this->assertSame(
            9007199254740993,
            (new M1ValuePolicy(new TypePolicyOptions()))->decode($cell['tag'], $cell['data']),
            'the int path must survive what a float cannot',
        );
    }

    /**
     * The `int|string` turnover is at `PHP_INT_MAX`, and the string form is still EXACT above it —
     * this is the other half of the fix, and the mutation guard for "just `(int)`-cast everything".
     * `u64::MAX` and `PHP_INT_MAX + 1` are both outside `int64`, so they can only arrive under
     * `TAG_U64`; `(int)` on either SATURATES at `PHP_INT_MAX` (measured below), which is exactly the
     * silent wrong answer the string form exists to prevent.
     */
    public function testAboveIntMaxStaysAnExactDecimalString(): void
    {
        $this->assertSame(PHP_INT_MAX, (int) '9223372036854775808', 'sanity: (int) saturates');

        $p = new PurePacker();
        foreach (['9223372036854775808', '18446744073709551615'] as $dec) {
            $off = 0;
            $cell = SqlValueCodec::fromWire($p->unpack(Value::u64($dec)->encode($p), $off));
            $this->assertSame(C::TAG_U64, $cell['tag']);
            $this->assertIsString($cell['data'], "a value above PHP_INT_MAX must not become an int ({$dec})");
            $this->assertSame($dec, $cell['data']);
            $this->assertSame($dec, (new RawStringValuePolicy())->decode($cell['tag'], $cell['data']));
        }
    }

    /**
     * A `U64` at or below `PHP_INT_MAX` now arrives as an `int` (the same `be()` change), and the
     * `U64` arm normalizes BOTH forms to the canonical decimal string — so a `BIGINT UNSIGNED`
     * column does not change PHP type between rows (hazard 28). This is the collateral surface of
     * the fix and it is asserted rather than assumed.
     */
    public function testU64NormalizesBothPackerFormsAcrossTheTurnover(): void
    {
        // dec => [the marker the ladder must pick, the PHP type `be()` must hand back].
        // The 8-byte rows are the ones that matter: `cf` is the SAME limb for an in-range `u64` and
        // an out-of-range one, and which PHP type comes out of it is the whole subject of this
        // slice. The `05`/`ce` rows are here so the fixint and uint32 ladders are not silently
        // standing in for the 8-byte fast path (that substitution is how the defect hid).
        $cases = [
            '5'                    => ['05',                 'int'],
            '4294967295'           => ['ceffffffff',         'int'],
            '4294967296'           => ['cf0000000100000000', 'int'],
            '9223372036854775807'  => ['cf7fffffffffffffff', 'int'],
            '9223372036854775808'  => ['cf8000000000000000', 'string'],
            '18446744073709551615' => ['cfffffffffffffffff', 'string'],
        ];
        $p = new PurePacker();
        foreach ($cases as $dec => [$markerHex, $phpType]) {
            $frame = Value::u64((string) $dec)->encode($p);
            $this->assertSame('9203' . $markerHex, bin2hex($frame), "u64 ladder for {$dec}");
            $off = 0;
            $cell = SqlValueCodec::fromWire($p->unpack($frame, $off));
            $this->assertSame($phpType, get_debug_type($cell['data']), "be() form for {$dec}");
            // Whichever form it took, the column must not change PHP type between rows: both
            // normalize to the canonical decimal string (hazard 28).
            $this->assertSame((string) $dec, (new RawStringValuePolicy())->decode(C::TAG_U64, $cell['data']));
        }
    }

    /**
     * `requireInt` is and stays `is_int()`-STRICT — nothing here coerces. What S8c added is the
     * DIAGNOSIS for the one string a conformant decoder can hand it: a legal wire value this PHP
     * BUILD cannot hold, which on a 64-bit build is only `> PHP_INT_MAX` and on a 32-bit build is
     * everything past ±2^31. It is refused, never `(int)`-truncated.
     *
     * This is the 32-bit contract, asserted from the only vantage point available on a 64-bit host:
     * the unit boundary. What it proves is `requireInt`'s verdict and message; what it CANNOT prove
     * is that a 32-bit build routes smaller values here (that is `be()`'s `PHP_INT_MAX` keying).
     */
    public function testAnUnrepresentableIntegerPayloadIsRefusedByName(): void
    {
        try {
            CanonicalText::requireInt('9223372036854775808'); // PHP_INT_MAX + 1
            $this->fail('an integer payload this build cannot hold must be refused, not truncated');
        } catch (ProtocolException $e) {
            $this->assertStringContainsString('does not fit this PHP build', $e->getMessage());
            $this->assertStringContainsString('PHP_INT_SIZE=' . PHP_INT_SIZE, $e->getMessage());
            $this->assertStringContainsString('refused rather than truncated', $e->getMessage());
            // SPEC §12: the message names the BUILD, never the cell's contents.
            $this->assertStringNotContainsString('9223372036854775808', $e->getMessage());
        }
    }

    /**
     * The strictness that must NOT have been traded away for the above: every non-`int` payload
     * still throws, including a numeric string that WOULD fit. `M1ValuePolicyTest::testM0Scalar
     * ArmsAreStrictAsWell` pins `[TAG_I64, '7']`; this states the same rule at the guard itself and
     * across the shapes a wrong-family payload actually takes.
     *
     * @return list<array{0: mixed}>
     */
    public static function nonIntegerPayloads(): array
    {
        return [['7'], ['0'], [''], ['-1'], ['+1'], ['1.0'], ['0x10'], ['1e3'], [' 7'], ['7 '],
            [7.0], [true], [false], [null], [[7]], [['a' => 7]]];
    }

    #[DataProvider('nonIntegerPayloads')]
    public function testRequireIntNeverCoerces(mixed $payload): void
    {
        $this->expectException(ProtocolException::class);
        CanonicalText::requireInt($payload);
    }
}
