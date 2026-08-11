<?php // /php/client/tests/Conformance/PackerConformanceTest.php
declare(strict_types=1);
namespace Ferro\Tests\Conformance;

use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\ExtPacker;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\Msgpack\PurePacker;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

/**
 * **The ext-vs-pure packer conformance gate** (M1-S8c; a carry open since M1-S7).
 *
 * Charter rule 7 makes `ext-msgpack` OPTIONAL and runtime-detected, which means the client ships TWO
 * encoders — {@see PurePacker} (the default, and the spec-authoritative one) and {@see ExtPacker} —
 * and a host merely having the extension installed must not change a single byte on the wire or a
 * single decoded value. Nothing asserted that until now, and the gap has already shipped a defect:
 * `ExtPacker::packBin` was `\msgpack_pack($s)`, which emits the msgpack **`str`** family (measured on
 * pecl msgpack 3.0.0: `msgpack_pack("ab")` is `a26162`) where the `TAG_BYTES` contract and the
 * engine's marker-strict `read_bin` demand **`bin`** — a silent corrupt WRITE visible only by running
 * BOTH arms over the SAME table, which is precisely what no single-path test can do. `packUint` had
 * the sibling defect (`\msgpack_pack((int) $n)` saturating every `u64` above `PHP_INT_MAX`).
 *
 * So the table below is driven from ONE fixture set, three ways:
 *   1. `PurePacker` must emit the exact canonical bytes (golden hex — this arm never skips);
 *   2. `ExtPacker` must emit the IDENTICAL bytes for every one of them; and
 *   3. both must DECODE every one of them to the identical PHP value.
 *
 * The per-tag fixtures are DERIVED from `proto/registry.lock.json`'s `implemented` list, so a newly
 * implemented tag with no fixture fails here rather than shipping unproven (the S7 rule: a coverage
 * claim must not be able to outrun the code).
 *
 * **THE SKIP IS LOUD BY DESIGN.** Arms 2 and 3 need the extension actually loaded; when it is not,
 * they skip with a message that says so in those words, they carry the `msgpack-ext` group so a gate
 * can run `phpunit --group msgpack-ext --fail-on-skipped`, and `FERRO_REQUIRE_EXT_MSGPACK=1` turns
 * the skip into a FAILURE. GitHub CI's `php` job installs `extensions: msgpack`, so they run there.
 */
#[Group('packer-conformance')]
final class PackerConformanceTest extends TestCase
{
    /**
     * Every canonical limb, as `[packer method, argument, expected canonical hex]`.
     *
     * The integer rungs are the S8c boundaries: the canonical ladder sends every NON-NEGATIVE value
     * out under an UNSIGNED marker, so 2^32 turns `0xce` into `0xcf` — the turnover that made every
     * `bigint` past 4.29e9 unreadable — while `PHP_INT_MAX`+1 is where the decoded PHP type has to
     * turn over from `int` to the exact decimal string. Both are here, on both encoders.
     *
     * @return array<string, array{0: string, 1: mixed, 2: string}>
     */
    public static function limbs(): array
    {
        $cases = [
            'nil'                  => ['packNil',     null,  'c0'],
            'bool true'            => ['packBool',    true,  'c3'],
            'bool false'           => ['packBool',    false, 'c2'],

            // ---- packInt: the full int64 ladder, boundary by boundary --------------------------
            'int PHP_INT_MIN'      => ['packInt', PHP_INT_MIN,          'd38000000000000000'],
            'int -(2^53)-1'        => ['packInt', -9007199254740993,    'd3ffdfffffffffffff'],
            'int -(2^32)'          => ['packInt', -4294967296,          'd3ffffffff00000000'],
            'int -(2^31)-1'        => ['packInt', -2147483649,          'd3ffffffff7fffffff'],
            'int -(2^31)'          => ['packInt', -2147483648,          'd280000000'],
            'int -32769'           => ['packInt', -32769,               'd2ffff7fff'],
            'int -32768'           => ['packInt', -32768,               'd18000'],
            'int -129'             => ['packInt', -129,                 'd1ff7f'],
            'int -128'             => ['packInt', -128,                 'd080'],
            'int -33'              => ['packInt', -33,                  'd0df'],
            'int -32'              => ['packInt', -32,                  'e0'],
            'int -1'               => ['packInt', -1,                   'ff'],
            'int 0'                => ['packInt', 0,                    '00'],
            'int 127'              => ['packInt', 127,                  '7f'],
            'int 128'              => ['packInt', 128,                  'cc80'],
            'int 255'              => ['packInt', 255,                  'ccff'],
            'int 256'              => ['packInt', 256,                  'cd0100'],
            'int 65535'            => ['packInt', 65535,                'cdffff'],
            'int 65536'            => ['packInt', 65536,                'ce00010000'],
            'int 2^31-1'           => ['packInt', 2147483647,           'ce7fffffff'],
            'int 2^31'             => ['packInt', 2147483648,           'ce80000000'],
            'int 2^32-1'           => ['packInt', 4294967295,           'ceffffffff'],
            'int 2^32'             => ['packInt', 4294967296,           'cf0000000100000000'],
            'int 2^53-1'           => ['packInt', 9007199254740991,     'cf001fffffffffffff'],
            'int 2^53'             => ['packInt', 9007199254740992,     'cf0020000000000000'],
            'int 2^53+1'           => ['packInt', 9007199254740993,     'cf0020000000000001'],
            'int PHP_INT_MAX'      => ['packInt', PHP_INT_MAX,          'cf7fffffffffffffff'],

            // ---- packUint: the same ladder plus the two rungs no PHP int can express -----------
            'uint int 0'           => ['packUint', 0,                     '00'],
            'uint int 2^32'        => ['packUint', 4294967296,            'cf0000000100000000'],
            'uint str "0"'         => ['packUint', '0',                   '00'],
            'uint str "007"'       => ['packUint', '007',                 '07'],
            'uint str 2^32-1'      => ['packUint', '4294967295',          'ceffffffff'],
            'uint str 2^32'        => ['packUint', '4294967296',          'cf0000000100000000'],
            'uint str INT_MAX'     => ['packUint', '9223372036854775807', 'cf7fffffffffffffff'],
            'uint str INT_MAX+1'   => ['packUint', '9223372036854775808', 'cf8000000000000000'],
            'uint str u64::MAX'    => ['packUint', '18446744073709551615', 'cfffffffffffffffff'],

            // ---- packFloat64: IEEE-754 big-endian, specials included ---------------------------
            'f64 0.0'              => ['packFloat64', 0.0,   'cb0000000000000000'],
            'f64 -0.0'             => ['packFloat64', -0.0,  'cb8000000000000000'],
            'f64 1.5'              => ['packFloat64', 1.5,   'cb3ff8000000000000'],
            'f64 NaN'              => ['packFloat64', NAN,   'cb7ff8000000000000'],
            'f64 +Inf'             => ['packFloat64', INF,   'cb7ff0000000000000'],
            'f64 -Inf'             => ['packFloat64', -INF,  'cbfff0000000000000'],
            // 2^53+1 as a float is the value the F64 tag CAN carry where I64 must not be used.
            'f64 2^53'             => ['packFloat64', 9007199254740992.0, 'cb4340000000000000'],

            // ---- packStr / packBin: every length rung, including the empty one -----------------
            'str empty'            => ['packStr', '',        'a0'],
            'str 1'                => ['packStr', 'a',       'a161'],
            'str utf8'             => ['packStr', "\xc3\xa9", 'a2c3a9'],
            'str 31 (fixstr max)'  => ['packStr', null,      ''], // filled below
            'str 32 (str8)'        => ['packStr', null,      ''],
            'str 255 (str8 max)'   => ['packStr', null,      ''],
            'str 256 (str16)'      => ['packStr', null,      ''],
            'str 65536 (str32)'    => ['packStr', null,      ''],
            // THE HISTORICAL DEFECT: a raw byte string must ride `bin`, never `str`, and must never
            // come out as an EMPTY bin (`c400`).
            'bin empty'            => ['packBin', '',                'c400'],
            'bin 3 non-utf8'       => ['packBin', "\x00\x01\xff",    'c40300 01ff'],
            'bin 255 (bin8 max)'   => ['packBin', null,      ''],
            'bin 256 (bin16)'      => ['packBin', null,      ''],
            'bin 65536 (bin32)'    => ['packBin', null,      ''],
        ];
        $cases['bin 3 non-utf8'][2] = 'c4030001ff'; // (the literal above keeps the bytes readable)

        // The long rungs are built rather than pasted: a 64 KiB hex literal in a fixture table is
        // unreadable and unreviewable, and the marker+length prefix is the whole content of the
        // assertion anyway.
        $x31 = str_repeat('x', 31);
        $x32 = str_repeat('x', 32);
        $x255 = str_repeat('x', 255);
        $x256 = str_repeat('x', 256);
        $x65536 = str_repeat('x', 65536);
        $b255 = str_repeat("\xfe", 255);
        $b256 = str_repeat("\xfe", 256);
        $b65536 = str_repeat("\xfe", 65536);
        $cases['str 31 (fixstr max)'] = ['packStr', $x31, 'bf' . bin2hex($x31)];
        $cases['str 32 (str8)'] = ['packStr', $x32, 'd920' . bin2hex($x32)];
        $cases['str 255 (str8 max)'] = ['packStr', $x255, 'd9ff' . bin2hex($x255)];
        $cases['str 256 (str16)'] = ['packStr', $x256, 'da0100' . bin2hex($x256)];
        $cases['str 65536 (str32)'] = ['packStr', $x65536, 'db00010000' . bin2hex($x65536)];
        $cases['bin 255 (bin8 max)'] = ['packBin', $b255, 'c4ff' . bin2hex($b255)];
        $cases['bin 256 (bin16)'] = ['packBin', $b256, 'c50100' . bin2hex($b256)];
        $cases['bin 65536 (bin32)'] = ['packBin', $b65536, 'c600010000' . bin2hex($b65536)];

        return $cases;
    }

    /**
     * The canonical PAYLOAD of every implemented tag, as `[packer method, argument]` — the limb the
     * tag actually rides per {@see \Ferro\Protocol\Value::encode}. Keyed by the registry's tag NAME
     * so {@see testEveryImplementedTagHasAPayloadFixture} can diff it against
     * `proto/registry.lock.json`.
     *
     * @return array<string, array{0: string, 1: mixed}>
     */
    public static function tagPayloads(): array
    {
        return [
            'NULL'        => ['packNil', null],
            'BOOL'        => ['packBool', true],
            'I64'         => ['packInt', 4294967296],                    // the S8c boundary
            'U64'         => ['packUint', '18446744073709551615'],
            'F64'         => ['packFloat64', -0.0],
            'DECIMAL'     => ['packStr', '-1.10'],                       // display scale preserved
            'TEXT'        => ['packStr', "a\u{00e9}\u{1f600}"],
            'BYTES'       => ['packBin', "\x00\x7f\x80\xff"],            // non-UTF8 on purpose
            'DATE'        => ['packStr', '0001-01-01'],
            'TIME'        => ['packStr', '-838:59:59.000000'],           // MySQL's negative extreme
            'TIMESTAMP'   => ['packStr', '0000-00-00 00:00:00'],         // the zero-datetime sentinel
            'TIMESTAMPTZ' => ['packStr', '2026-08-11T13:45:07.250000Z'],
            'UUID'        => ['packStr', '00000000-0000-0000-0000-000000000000'],
            'JSON'        => ['packStr', '{"a":[1,2],"b":null}'],
        ];
    }

    // ---- arm 0: the coverage lock, and the charter-rule-7 default -----------------------------

    /**
     * A tag the registry calls implemented but that has no fixture here would be a silent coverage
     * hole in BOTH arms — the shape S7 closed for the live type gate and this closes for the codec.
     */
    public function testEveryImplementedTagHasAPayloadFixture(): void
    {
        $lockPath = dirname(__DIR__, 4) . '/proto/registry.lock.json';
        $this->assertFileExists($lockPath);
        /** @var mixed $lock */
        $lock = json_decode((string) file_get_contents($lockPath), true);
        $this->assertIsArray($lock);
        $implemented = $lock['implemented'] ?? null;
        $this->assertIsArray($implemented, 'registry.lock.json must carry the implemented tag list');
        $names = [];
        foreach ($implemented as $n) {
            $this->assertIsString($n);
            $names[] = $n;
        }
        sort($names);
        $covered = array_keys(self::tagPayloads());
        sort($covered);
        $this->assertSame(
            $names,
            $covered,
            'the ext-vs-pure fixture table and /proto/registry.lock.json disagree about which tags '
            . 'are implemented — add the fixture in the same change as the tag',
        );
    }

    /**
     * Charter rule 7: the extension is OPTIONAL, so the default paths must be the pure implementation
     * **even on a host that has it loaded**. The ext-loaded run is the load-bearing one; in the
     * pure-only run this is trivially true, which is why it is asserted in both.
     */
    public function testTheDefaultCodecPathsAreThePureImplementation(): void
    {
        $this->assertInstanceOf(PurePacker::class, PackerFactory::forEncode());
        $this->assertInstanceOf(PurePacker::class, PackerFactory::forDecode());
    }

    // ---- arm 1: PurePacker emits the canonical bytes (never skips) -----------------------------

    #[DataProvider('limbs')]
    public function testPurePackerEmitsTheCanonicalBytes(string $method, mixed $arg, string $hex): void
    {
        $this->assertSame($hex, bin2hex(self::emit(new PurePacker(), $method, $arg)));
    }

    #[DataProvider('tagPayloads')]
    public function testPurePackerEmitsEveryTagPayload(string $method, mixed $arg): void
    {
        // No golden hex here (the per-limb table above owns that); what this pins is that every
        // implemented tag's canonical payload is encodable at all, which is the precondition for the
        // ext comparison below to be meaningful rather than vacuous.
        $this->assertNotSame('', self::emit(new PurePacker(), $method, $arg));
    }

    // ---- arm 2: ExtPacker is byte-identical ----------------------------------------------------

    /**
     * Deliberately NOT a `#[DataProvider]` test. Arm 1 already gives every fixture its own named
     * case; fanning the ext arms out too would put 135 `S`s in the default offline lane, and a lane
     * whose skip count is that large is one where a NEW, unintended skip is invisible. So this walks
     * the whole table in one case, COLLECTS every mismatch (rather than stopping at the first, which
     * a loop normally would) and reports them together.
     */
    #[Group('msgpack-ext')]
    public function testExtPackerIsByteIdenticalToPure(): void
    {
        $ext = $this->extPackerOrLoudSkip();
        $pure = new PurePacker();
        $diffs = [];
        $checked = 0;
        foreach ([self::limbs(), self::tagPayloads()] as $table) {
            foreach ($table as $label => $row) {
                $method = $row[0];
                $arg = $row[1];
                $pureHex = bin2hex(self::emit($pure, $method, $arg));
                $extHex = bin2hex(self::emit($ext, $method, $arg));
                ++$checked;
                if ($pureHex !== $extHex) {
                    $diffs[] = sprintf(
                        '%s (%s): pure=%s ext=%s',
                        $label,
                        $method,
                        self::abbreviate($pureHex),
                        self::abbreviate($extHex),
                    );
                }
            }
        }
        $this->assertSame(count(self::limbs()) + count(self::tagPayloads()), $checked);
        $this->assertSame(
            [],
            $diffs,
            'ExtPacker does not byte-match PurePacker — a host that merely has ext-msgpack installed '
            . "would put different bytes on the wire:\n  " . implode("\n  ", $diffs),
        );
    }

    // ---- arm 3: ExtPacker decodes identically ---------------------------------------------------

    /** Same one-case, collect-everything shape as arm 2, and for the same reason. */
    #[Group('msgpack-ext')]
    public function testExtPackerDecodesToTheIdenticalValue(): void
    {
        $ext = $this->extPackerOrLoudSkip();
        $pure = new PurePacker();
        $diffs = [];
        $checked = 0;
        foreach ([self::limbs(), self::tagPayloads()] as $table) {
            foreach ($table as $label => $row) {
                $bytes = self::emit($pure, $row[0], $row[1]);
                $o1 = 0;
                $o2 = 0;
                $a = $pure->unpack($bytes, $o1);
                $b = $ext->unpack($bytes, $o2);
                ++$checked;
                $same = is_float($a) && is_nan($a)
                    ? is_float($b) && is_nan($b) // NaN !== NaN, so it needs its own predicate
                    : $a === $b;
                if (!$same) {
                    $diffs[] = sprintf(
                        '%s: pure=%s(%s) ext=%s(%s)',
                        $label,
                        self::abbreviate((string) json_encode($a)),
                        get_debug_type($a),
                        self::abbreviate((string) json_encode($b)),
                        get_debug_type($b),
                    );
                }
            }
        }
        $this->assertSame(count(self::limbs()) + count(self::tagPayloads()), $checked);
        $this->assertSame(
            [],
            $diffs,
            "the two decoders disagree — PackerFactory::forDecode() picks one of them:\n  "
            . implode("\n  ", $diffs),
        );
    }

    /**
     * The decode comparison at FRAME scale: a nested `[[tag, payload], …]` body, i.e. the shape a
     * real `ExecOk.rows` has, so a disagreement about container framing (not just a scalar limb) is
     * caught too.
     */
    #[Group('msgpack-ext')]
    public function testExtPackerDecodesAWholeRowFrameIdentically(): void
    {
        $ext = $this->extPackerOrLoudSkip();
        $pure = new PurePacker();

        $cells = '';
        $n = 0;
        foreach (self::tagPayloads() as $name => [$method, $arg]) {
            $tag = self::tagConstant($name);
            $cells .= $pure->packArrayLen(2) . $pure->packInt($tag) . self::emit($pure, $method, $arg);
            ++$n;
        }
        $frame = $pure->packArrayLen($n) . $cells;

        $o1 = 0;
        $o2 = 0;
        $a = $pure->unpack($frame, $o1);
        $b = $ext->unpack($frame, $o2);
        $this->assertSame(strlen($frame), $o1, 'pure must consume the frame exactly');
        $this->assertIsArray($a);
        $this->assertIsArray($b);
        $this->assertCount($n, $a);
        // Compare through JSON so the F64 -0.0 cell (which `assertSame` on nested arrays handles,
        // but which is easy to lose) and every string payload are compared as encoded bytes.
        $this->assertSame(
            json_encode($a, JSON_PARTIAL_OUTPUT_ON_ERROR),
            json_encode($b, JSON_PARTIAL_OUTPUT_ON_ERROR),
            'the two decoders disagree on a whole row frame',
        );
    }

    /**
     * **The ONE divergence, asserted so `PackerFactory::forDecode`'s reasoning is falsifiable rather
     * than folklore.** `ExtPacker::unpack` is `\msgpack_unpack($buf)` + `$offset = strlen($buf)`: it
     * cannot decode ONE value out of a buffer that holds several, and it cannot resume at a caller's
     * offset. Every framed decode in this client (`Codec`, `Value::decode`, `ExecOk`) does exactly
     * that. So the extension is unusable as the decoder no matter how conformant its scalars are —
     * which arm 3 above shows they now are, on pecl msgpack 3.0.0.
     */
    #[Group('msgpack-ext')]
    public function testExtPackerCannotHonourAnOffsetWhichIsWhyItIsNotTheDecoder(): void
    {
        $ext = $this->extPackerOrLoudSkip();
        $pure = new PurePacker();
        $two = $pure->packInt(1) . $pure->packInt(2);

        $o = 0;
        $this->assertSame(1, $pure->unpack($two, $o));
        $this->assertSame(1, $o, 'pure advances by exactly the value it read');
        $this->assertSame(2, $pure->unpack($two, $o), 'so the second value is reachable');

        // The extension emits its own "[msgpack] Extra bytes" PHP warning here — that IS the
        // divergence, so it is captured and discarded rather than left to redden the suite. It is not
        // ASSERTED on: `msgpack.error_display` is an ini knob a host may have Off, so the warning's
        // presence is not a property of the contract; the offset below is.
        $o = 0;
        set_error_handler(static fn (): bool => true);
        try {
            $ext->unpack($two, $o);
        } finally {
            restore_error_handler();
        }
        $this->assertSame(
            strlen($two),
            $o,
            'ExtPacker consumes the whole buffer regardless of what it read — if this ever changes, '
            . 're-read PackerFactory::forDecode() before making the extension the decoder',
        );
        $this->assertInstanceOf(PurePacker::class, PackerFactory::forDecode());
    }

    /**
     * `packArrayLen` is the other structural reason the extension can never be the ENCODER: it packs
     * whole values, so it has no way to emit a bare container header, and every Ferro frame is one.
     */
    #[Group('msgpack-ext')]
    public function testExtPackerRefusesToEmitAContainerHeader(): void
    {
        $ext = $this->extPackerOrLoudSkip();
        $this->expectException(CodecException::class);
        $ext->packArrayLen(2);
    }

    // ---- helpers -------------------------------------------------------------------------------

    /**
     * The extension, or a LOUD skip. Silent skips are how a coverage hole becomes a false green, so
     * this one names itself as a hole, carries the `msgpack-ext` group (run
     * `phpunit --group msgpack-ext --fail-on-skipped` on a host that has it), and can be made FATAL
     * with `FERRO_REQUIRE_EXT_MSGPACK=1`.
     */
    private function extPackerOrLoudSkip(): ExtPacker
    {
        if (\extension_loaded('msgpack')) {
            return new ExtPacker();
        }
        $msg = 'ext-msgpack IS NOT LOADED, so the ext arm of the packer conformance gate DID NOT RUN. '
            . 'This is a COVERAGE HOLE, not a pass: the two encoders are unverified against each '
            . 'other on this host, and that gap has already shipped a silent corrupt write '
            . '(ExtPacker::packBin emitting `str` instead of `bin`). Install it '
            . '(`pecl install msgpack`, or `apt install php8.4-msgpack`) and re-run; GitHub CI\'s '
            . '`php` job installs it. Set FERRO_REQUIRE_EXT_MSGPACK=1 to make this a FAILURE.';
        if (getenv('FERRO_REQUIRE_EXT_MSGPACK') === '1') {
            $this->fail($msg);
        }
        $this->markTestSkipped($msg);
    }

    /** Keep a 64 KiB hex blob out of a failure message while leaving both ends comparable. */
    private static function abbreviate(string $hex): string
    {
        return strlen($hex) <= 80 ? $hex : substr($hex, 0, 40) . '…' . substr($hex, -20);
    }

    /** Dispatch one fixture row onto a packer. */
    private static function emit(PackerInterface $p, string $method, mixed $arg): string
    {
        return match ($method) {
            'packNil' => $p->packNil(),
            'packBool' => $p->packBool((bool) $arg),
            'packInt' => $p->packInt(is_int($arg) ? $arg : 0),
            'packUint' => $p->packUint(is_int($arg) || is_string($arg) ? $arg : 0),
            'packFloat64' => $p->packFloat64(is_float($arg) ? $arg : 0.0),
            'packStr' => $p->packStr(is_string($arg) ? $arg : ''),
            'packBin' => $p->packBin(is_string($arg) ? $arg : ''),
            default => throw new \LogicException("unknown fixture method {$method}"),
        };
    }

    /** A registry tag NAME → its `/proto` constant, so the frame test uses the real tag ids. */
    private static function tagConstant(string $name): int
    {
        $const = C::class . '::TAG_' . $name;
        if (!defined($const)) {
            throw new \LogicException("no generated constant for tag {$name}");
        }
        /** @var int $v */
        $v = constant($const);
        return $v;
    }
}
