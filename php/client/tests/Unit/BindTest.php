<?php // /php/client/tests/Unit/BindTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TypePolicyException;
use Ferro\Client\ExecCodec;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Date;
use Ferro\Decimal;
use Ferro\Json;
use Ferro\NaiveTimestamp;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\ExtPacker;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\SqlValueCodec;
use Ferro\Time;
use Ferro\U64;
use Ferro\Uuid;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * **The BIND (write) path for the M1-S7 canonical tags** — `ExecCodec::bindOne`, the chokepoint
 * every positional parameter passes through (hazard 31: the `ValuePolicy` seam is DECODE-only, so
 * without these arms the whole slice is read-only and a DBAL suite, which binds `DateTime`s and
 * decimals constantly, is still broken).
 *
 * Three failure classes are locked here, each of which is a SILENT data corruption if it regresses:
 *
 *  1. **Subclass ordering (F14).** `NaiveTimestamp` IS a `DateTimeImmutable`, so a `match(true)`
 *     that tests `\DateTimeInterface` first swallows it and every naive value re-binds as a UTC
 *     instant — shifted by the session offset, with no error anywhere.
 *  2. **`U64` above `PHP_INT_MAX` (hazard 29/48).** It must ride `packUint`, never `packInt` (which
 *     physically cannot emit it) and never a `(int)` cast (which SATURATES at `PHP_INT_MAX`).
 *  3. **Coercion (hazard 30/F18).** Nothing on this path may route through the `toStr`/`toInt`
 *     narrowing helpers, whose `''`/`0` fallbacks would turn a malformed payload into a corrupt
 *     WRITE rather than an error.
 *
 * The acceptance test is {@see testEveryCanonicalTagRoundTripsByteIdenticallyThroughTheWire}: read a
 * canonical payload, bind the hydrated value straight back, and assert the emitted bytes are
 * IDENTICAL to encoding the original text. Live cross-engine round trips are M1-S7 Task 9.
 */
final class BindTest extends TestCase
{
    /** `ExecCodec` takes FOUR required args (`ExecCodec.php:38-43`) — one factory for the class. */
    private function codec(?TypePolicyOptions $options = null): ExecCodec
    {
        return new ExecCodec(
            new M1ValuePolicy($options ?? new TypePolicyOptions()),
            new PlanCache(),
            new PurePacker(),
            new PurePacker(),
        );
    }

    // ---- the U64 range (hazard 29) ---------------------------------------------------------------

    /** A `U64` above `PHP_INT_MAX` MUST go through `packUint`, never `packInt`. */
    public function testU64BindsViaPackUintAndSurvivesTheFullRange(): void
    {
        $big = '18446744073709551615';
        $p = new PurePacker();
        // SqlValueCodec::encode(PackerInterface $p, mixed $vj) — PACKER FIRST (SqlValueCodec.php:16).
        $wire = SqlValueCodec::encode($p, ['tag' => C::TAG_U64, 'data' => $big]);
        // unpack is an INSTANCE method with a BY-REF offset (Msgpack/PurePacker.php:81).
        $off = 0;
        self::assertSame($big, SqlValueCodec::fromWire($p->unpack($wire, $off))['data']);
        // Byte-level: [0x92, tag 0x03, payload]. A regression to packInt cannot silently pass this.
        self::assertSame(0xcf, ord($wire[2]), 'TAG_U64 payload must carry the uint64 marker');
    }

    /** `bindOne` must reach that path from a `Ferro\U64`, not just from a hand-built cell. */
    public function testU64ValueObjectBindsAsAUint64OnTheWire(): void
    {
        $bound = $this->codec()->bindOne(new U64('18446744073709551615'));
        self::assertSame(C::TAG_U64, $bound['tag']);
        $wire = SqlValueCodec::encode(new PurePacker(), $bound);
        self::assertSame(0xcf, ord($wire[2]), 'a Ferro\U64 must reach the uint64 marker end-to-end');
    }

    // ---- the value-object arms -------------------------------------------------------------------

    public function testValueObjectsBindToTheirCanonicalText(): void
    {
        foreach ([
            [new Decimal('1.10'),                              C::TAG_DECIMAL, '1.10'],
            [new Decimal('NaN'),                               C::TAG_DECIMAL, 'NaN'],
            [new Date('2026-08-05'),                           C::TAG_DATE,    '2026-08-05'],
            [new Time('24:00:00'),                             C::TAG_TIME,    '24:00:00'],
            [new Time('-838:59:58.000001'),                    C::TAG_TIME,    '-838:59:58.000001'],
            [new Uuid('3f2b8c1a-0000-4fff-8000-abcdefabcdef'), C::TAG_UUID,    '3f2b8c1a-0000-4fff-8000-abcdefabcdef'],
            [new Json('{"a":1}'),                              C::TAG_JSON,    '{"a":1}'],
            [new U64('18446744073709551615'),                  C::TAG_U64,     '18446744073709551615'],
            [new U64(5),                                       C::TAG_U64,     '5'],
        ] as [$obj, $tag, $text]) {
            $bound = $this->codec()->bindOne($obj);
            self::assertSame($tag, $bound['tag'], get_debug_type($obj) . ' binds its own tag');
            self::assertSame($text, $bound['data'], get_debug_type($obj) . ' re-emits canonical text');
        }
    }

    /** The M0 scalars are unchanged — this path is additive, not a rewrite. */
    public function testM0ScalarsStillBindUnchanged(): void
    {
        $c = $this->codec();
        self::assertSame(['tag' => C::TAG_NULL, 'data' => null], $c->bindOne(null));
        self::assertSame(['tag' => C::TAG_BOOL, 'data' => true], $c->bindOne(true));
        self::assertSame(['tag' => C::TAG_I64,  'data' => -7], $c->bindOne(-7));
        self::assertSame(['tag' => C::TAG_F64,  'data' => 1.5], $c->bindOne(1.5));
        self::assertSame(['tag' => C::TAG_TEXT, 'data' => 'x'], $c->bindOne('x'));
    }

    public function testUnbindableValueStillThrows(): void
    {
        $this->expectException(ProtocolException::class);
        $this->codec()->bindOne(new \stdClass());
    }

    // ---- the naive/instant rule (F14) — the ordering guard ---------------------------------------

    /** A plain `DateTimeImmutable` is an INSTANT → `TIMESTAMPTZ`, UTC-normalized. */
    public function testDateTimeImmutableBindsAsTimestampTz(): void
    {
        $dt = new \DateTimeImmutable('2026-08-05 13:45:07.250000', new \DateTimeZone('+02:00'));
        $bound = $this->codec()->bindOne($dt);
        self::assertSame(C::TAG_TIMESTAMPTZ, $bound['tag']);
        self::assertSame('2026-08-05T11:45:07.250000Z', $bound['data']);
    }

    /**
     * **The reordering guard.** `NaiveTimestamp` is a SUBCLASS of `DateTimeImmutable`, so it must be
     * matched FIRST — swap the two `match` arms and this value silently becomes a UTC instant on
     * write-back, shifted by whatever zone the object carries.
     */
    public function testNaiveTimestampBindsBackAsTimestampNotTimestampTz(): void
    {
        $naive = (new M1ValuePolicy(new TypePolicyOptions()))
            ->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07.250000');
        self::assertInstanceOf(NaiveTimestamp::class, $naive);
        // The reason the arm order is load-bearing, asserted rather than assumed:
        self::assertInstanceOf(\DateTimeImmutable::class, $naive, 'a NaiveTimestamp IS a DateTimeImmutable');

        $bound = $this->codec()->bindOne($naive);
        self::assertSame(C::TAG_TIMESTAMP, $bound['tag'], 'the subclass arm must precede DateTimeInterface');
        self::assertSame('2026-08-05 13:45:07.250000', $bound['data'], 'byte-stable read->write round trip');
    }

    /**
     * The same guard from the other side: a `NaiveTimestamp` built in a NON-UTC zone must still bind
     * its wall clock verbatim. If the `\DateTimeInterface` arm caught it, this would come back as
     * `2026-08-05T11:45:07Z` — the exact silent shift F14 settles.
     */
    public function testNaiveTimestampInANonUtcZoneStillBindsItsWallClock(): void
    {
        $naive = new NaiveTimestamp('2026-08-05 13:45:07', new \DateTimeZone('+02:00'));
        $bound = $this->codec()->bindOne($naive);
        self::assertSame(C::TAG_TIMESTAMP, $bound['tag']);
        self::assertSame('2026-08-05 13:45:07', $bound['data'], 'a naive value has no zone to convert');
    }

    /** A MUTABLE `\DateTime` binds as an instant too, and binding must not mutate the caller's object. */
    public function testMutableDateTimeBindsAsTimestampTzWithoutMutatingIt(): void
    {
        $dt = new \DateTime('2026-08-05 13:45:07', new \DateTimeZone('+02:00'));
        $bound = $this->codec()->bindOne($dt);
        self::assertSame(C::TAG_TIMESTAMPTZ, $bound['tag']);
        self::assertSame('2026-08-05T11:45:07Z', $bound['data']);
        self::assertSame('+02:00', $dt->getTimezone()->getName(), 'the caller\'s DateTime is untouched');
    }

    /**
     * The §3.2 fraction rule, on BOTH temporal tags: no `.ffffff` group at all when the sub-second
     * part is zero, otherwise EXACTLY six digits — never a trailing-zero-trimmed variant.
     */
    public function testFractionRuleIsExactlySixDigitsOrAbsent(): void
    {
        $c = $this->codec();
        self::assertSame(
            '2026-08-05T13:45:07Z',
            $c->bindOne(new \DateTimeImmutable('2026-08-05 13:45:07', new \DateTimeZone('UTC')))['data'],
        );
        self::assertSame(
            '2026-08-05T13:45:07.100000Z',
            $c->bindOne(new \DateTimeImmutable('2026-08-05 13:45:07.1', new \DateTimeZone('UTC')))['data'],
        );
        self::assertSame(
            '2026-08-05 13:45:07',
            $c->bindOne(NaiveTimestamp::fromCanonicalText('2026-08-05 13:45:07'))['data'],
        );
        self::assertSame(
            '2026-08-05 13:45:07.100000',
            $c->bindOne(NaiveTimestamp::fromCanonicalText('2026-08-05 13:45:07.100000'))['data'],
        );
    }

    /**
     * `naive_datetime_zone=error` is an operator's "this application does not deal in naive
     * timestamps". It refuses the READ (M1ValuePolicy) and must refuse the WRITE symmetrically —
     * otherwise the knob silently permits exactly the value it was set to eliminate. It stays a
     * {@see TypePolicyException} (a configuration refusal), never a `ProtocolException` (a wire fault).
     */
    public function testNaiveTimestampBindIsRefusedUnderNaiveDatetimeZoneError(): void
    {
        $codec = $this->codec(new TypePolicyOptions(naiveDatetimeZone: 'error'));
        $this->expectException(TypePolicyException::class);
        $codec->bindOne(NaiveTimestamp::fromCanonicalText('2026-08-05 13:45:07'));
    }

    /** …and under that same policy a plain instant is untouched — the knob is scoped to TAG_TIMESTAMP. */
    public function testTimestampTzBindIsUnaffectedByNaiveDatetimeZoneError(): void
    {
        $bound = $this->codec(new TypePolicyOptions(naiveDatetimeZone: 'error'))
            ->bindOne(new \DateTimeImmutable('2026-08-05 13:45:07', new \DateTimeZone('UTC')));
        self::assertSame(C::TAG_TIMESTAMPTZ, $bound['tag']);
    }

    // ---- the acceptance: read -> bind -> read, byte-identical ------------------------------------

    /**
     * **The slice's PHP-layer acceptance.** For every one of the eight canonical tags: decode the
     * canonical payload, bind the hydrated value straight back, push it through the real codec, and
     * assert (a) the tag survives, (b) the payload is byte-identical, (c) the ENCODED BYTES are
     * identical to encoding the original text directly — i.e. the bind path cannot diverge from the
     * read path or from the golden vectors — and (d) decoding again yields an equal PHP value.
     */
    #[DataProvider('canonicalPayloads')]
    public function testEveryCanonicalTagRoundTripsByteIdenticallyThroughTheWire(int $tag, string $text): void
    {
        $policy = new M1ValuePolicy(new TypePolicyOptions());
        $codec = $this->codec();
        $p = new PurePacker();

        $php = $policy->decode($tag, $text);                 // read
        $bound = $codec->bindOne($php);                      // bind it straight back
        self::assertSame($tag, $bound['tag'], 'the tag must survive a read -> bind round trip');
        self::assertSame($text, $bound['data'], 'the canonical text must survive verbatim');

        $wire = SqlValueCodec::encode($p, $bound);
        self::assertSame(
            SqlValueCodec::encode($p, ['tag' => $tag, 'data' => $text]),
            $wire,
            'bind-path bytes must be identical to read-path/golden-vector bytes',
        );

        $off = 0;
        $cell = SqlValueCodec::fromWire($p->unpack($wire, $off));
        self::assertSame(strlen($wire), $off, 'the cell consumed all its bytes');
        self::assertSame(['tag' => $tag, 'data' => $text], $cell);
        self::assertEquals($php, $policy->decode($cell['tag'], $cell['data']), 'read -> bind -> read is stable');
    }

    /** @return array<string, array{0:int, 1:string}> */
    public static function canonicalPayloads(): array
    {
        return [
            'U64 max' => [C::TAG_U64, '18446744073709551615'],
            'U64 above 2^63' => [C::TAG_U64, '9223372036854775808'],
            'DECIMAL scale preserved' => [C::TAG_DECIMAL, '-12345.6700'],
            'DECIMAL NaN' => [C::TAG_DECIMAL, 'NaN'],
            'DECIMAL 30 digits' => [C::TAG_DECIMAL, '123456789012345678901234567890'],
            'DATE' => [C::TAG_DATE, '2026-08-05'],
            'TIME 24h' => [C::TAG_TIME, '24:00:00'],
            'TIME negative' => [C::TAG_TIME, '-838:59:58.000001'],
            'TIMESTAMP fractional' => [C::TAG_TIMESTAMP, '2026-08-05 13:45:07.250000'],
            'TIMESTAMP whole second' => [C::TAG_TIMESTAMP, '2026-08-05 13:45:07'],
            'TIMESTAMPTZ fractional' => [C::TAG_TIMESTAMPTZ, '2026-08-05T11:45:07.250000Z'],
            'TIMESTAMPTZ whole second' => [C::TAG_TIMESTAMPTZ, '2026-08-05T11:45:07Z'],
            'UUID' => [C::TAG_UUID, '3f2b8c1a-0000-4fff-8000-abcdefabcdef'],
            'JSON non-ascii' => [C::TAG_JSON, '{"a":1,"b":[1,2,{"c":"é"}]}'],
        ];
    }

    /**
     * A `U64` that FITS a PHP int decodes to a plain `int` (SPEC §9: "int, or Ferro\U64 if >
     * PHP_INT_MAX"), so re-binding it yields `TAG_I64` — the VALUE is preserved exactly, the tag
     * widens down. Documented here rather than discovered later: every backend that produces a `U64`
     * also accepts an `I64` for the same column, and the alternative (sniffing a PHP int's magnitude
     * to guess `U64`) would retag ordinary integers.
     */
    public function testSmallU64DegradesToI64ButPreservesTheValue(): void
    {
        $php = (new M1ValuePolicy(new TypePolicyOptions()))->decode(C::TAG_U64, 5);
        self::assertSame(5, $php);
        self::assertSame(['tag' => C::TAG_I64, 'data' => 5], $this->codec()->bindOne($php));
    }

    // ---- sentinels (C20) -------------------------------------------------------------------------

    /**
     * A `DATE` sentinel round-trips **with its tag intact**: `Ferro\Date` carries `infinity` /
     * `-infinity` / `0000-00-00` verbatim, so the bind arm re-emits exactly what was read.
     */
    #[DataProvider('dateSentinels')]
    public function testDateSentinelRoundTripsWithItsTagIntact(string $text): void
    {
        $php = (new M1ValuePolicy(new TypePolicyOptions()))->decode(C::TAG_DATE, $text);
        self::assertInstanceOf(Date::class, $php);
        self::assertTrue($php->isSentinel());
        self::assertSame(['tag' => C::TAG_DATE, 'data' => $text], $this->codec()->bindOne($php));
    }

    /** @return array<string, array{0:string}> */
    public static function dateSentinels(): array
    {
        return [
            'PG infinity' => ['infinity'],
            'PG -infinity' => ['-infinity'],
            'MySQL zero date' => ['0000-00-00'],
            'MySQL zero-in-date' => ['2026-00-05'],
        ];
    }

    /**
     * **C20, decided and pinned.** A `TIMESTAMP`/`TIMESTAMPTZ` sentinel is not a wall-clock value, so
     * the policy hands it back as canonical TEXT (throwing would make a legal column unreadable). On
     * the way back out it therefore binds as `TAG_TEXT` — carrying the bytes VERBATIM, never parsed,
     * never rewritten.
     *
     * That tag widening is deliberate: `bindOne` must not sniff a bare PHP string's CONTENTS to guess
     * a temporal tag, because `'infinity'` and `'0000-00-00 00:00:00'` are perfectly ordinary values
     * in a `varchar` column and retagging them would be the silent miscast §9.1 forbids. The
     * consequence is loud, never silent: PG's `bind::accepts` refuses a `TEXT` param against a
     * `timestamp` slot as a §19.3 KNOWN-FATE, pre-send rejection. A tag-preserving sentinel write is
     * the same gap `TAG_BYTES` has (every PHP string binds `TEXT`), and carries the same S8 fix
     * shape — an explicit bind marker — recorded in SPEC §22.2.
     */
    #[DataProvider('timestampSentinels')]
    public function testTimestampSentinelsRebindVerbatimAsText(int $tag, string $text): void
    {
        $php = (new M1ValuePolicy(new TypePolicyOptions()))->decode($tag, $text);
        self::assertSame($text, $php, 'a sentinel decodes to its canonical text, never a date object');
        $bound = $this->codec()->bindOne($php);
        self::assertSame(C::TAG_TEXT, $bound['tag'], 'a bare string binds TEXT — contents are never sniffed');
        self::assertSame($text, $bound['data'], 'the sentinel bytes survive verbatim');
    }

    /** @return array<string, array{0:int, 1:string}> */
    public static function timestampSentinels(): array
    {
        return [
            'TIMESTAMP infinity' => [C::TAG_TIMESTAMP, 'infinity'],
            'TIMESTAMP -infinity' => [C::TAG_TIMESTAMP, '-infinity'],
            'MySQL zero datetime' => [C::TAG_TIMESTAMP, '0000-00-00 00:00:00'],
            'TIMESTAMPTZ infinity' => [C::TAG_TIMESTAMPTZ, 'infinity'],
            'MySQL zero TIMESTAMP' => [C::TAG_TIMESTAMPTZ, '0000-00-00 00:00:00'],
        ];
    }

    // ---- the call site ---------------------------------------------------------------------------

    /**
     * `bindParams` must actually route through the promoted INSTANCE method — a leftover
     * `self::bindOne($v)` would compile and pass every direct-call test above while every real
     * statement still rejected a value object.
     */
    public function testEncodeBindsValueObjectsThroughTheRealCallSite(): void
    {
        $payload = $this->codec()->encode(
            'default',
            'INSERT INTO t (a, b) VALUES ($1, $2)',
            [new Decimal('1.10'), NaiveTimestamp::fromCanonicalText('2026-08-05 13:45:07')],
            false,
            ExecCodec::FETCH_NONE,
            null,
        );
        self::assertStringContainsString('1.10', $payload);
        self::assertStringContainsString('2026-08-05 13:45:07', $payload);
        // The tag bytes ride next to the payloads: [0x92, tag, str...] for each param.
        self::assertStringContainsString("\x92" . chr(C::TAG_DECIMAL), $payload);
        self::assertStringContainsString("\x92" . chr(C::TAG_TIMESTAMP), $payload);
    }

    // ---- ExtPacker::packUint (hazard 48 / F19) ----------------------------------------------------

    /**
     * `ExtPacker::packUint` used to be `\msgpack_pack(is_string($n) ? (int) $n : $n)` — a silent
     * `(int)` SATURATION at `PHP_INT_MAX` for every `u64` above it. It was unreachable only because
     * nothing bound a `TAG_U64` string; the bind path above creates that call path, so the cast is
     * now a live corruption. The fix must be byte-identical to the canonical limb encoder, because a
     * host that happens to load ext-msgpack must not write different bytes than one that does not.
     */
    #[DataProvider('uintStrings')]
    public function testExtPackerPackUintMatchesTheCanonicalLimbEncoder(string $decimal): void
    {
        if (!\extension_loaded('msgpack')) {
            $this->markTestSkipped('ext-msgpack not loaded');
        }
        $pure = (new PurePacker())->packUint($decimal);
        self::assertSame(
            bin2hex($pure),
            bin2hex((new ExtPacker())->packUint($decimal)),
            'ExtPacker must not (int)-cast a uint string',
        );
    }

    /** @return array<string, array{0:string}> */
    public static function uintStrings(): array
    {
        return [
            'zero' => ['0'],
            'fixint' => ['5'],
            'uint16' => ['300'],
            'uint32 boundary' => ['4294967296'],
            'PHP_INT_MAX' => ['9223372036854775807'],
            'above PHP_INT_MAX' => ['9223372036854775808'],
            'u64 max' => ['18446744073709551615'],
        ];
    }

    /** …and the top of the range really is a uint64 marker, not a truncated int. */
    public function testExtPackerEncodesU64MaxAsARealUint64(): void
    {
        if (!\extension_loaded('msgpack')) {
            $this->markTestSkipped('ext-msgpack not loaded');
        }
        $out = (new ExtPacker())->packUint('18446744073709551615');
        self::assertSame('cfffffffffffffffff', bin2hex($out));
    }

    /** A non-numeric or out-of-range uint string is a loud CodecException, never a silent 0. */
    public function testExtPackerRefusesAnUnrepresentableUintString(): void
    {
        if (!\extension_loaded('msgpack')) {
            $this->markTestSkipped('ext-msgpack not loaded');
        }
        $p = new ExtPacker();
        try {
            $p->packUint('not-a-number');
            self::fail('a non-numeric uint string must throw, not (int)-cast to 0');
        } catch (CodecException) {
            self::assertTrue(true);
        }
        try {
            $p->packUint('18446744073709551616'); // 2^64, one past u64::MAX
            self::fail('a value past u64::MAX must throw');
        } catch (CodecException) {
            self::assertTrue(true);
        }
    }
}
