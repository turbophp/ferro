<?php // /php/client/tests/Unit/M1ValuePolicyTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TypePolicyException;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Date;
use Ferro\Decimal;
use Ferro\Json;
use Ferro\NaiveTimestamp;
use Ferro\Time;
use Ferro\U64;
use Ferro\Uuid;
use Ferro\Protocol\Generated\Constants as C;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The M1-S7 read path: canonical wire text (`/proto/PROTOCOL.md` §3.2) → the SPEC §9 PHP types,
 * under the four §9.1 policy knobs.
 *
 * Two properties are load-bearing here and are the reason the class runs under a NON-UTC default
 * timezone (F26): `format('Y-m-d H:i:s.u')` returns the same string in ANY zone, so a naive
 * assertion made under a UTC default is satisfied by a WRONGLY-zoned object which then shifts on
 * write-back. Every datetime assertion therefore checks `getTimezone()` + `getOffset()` too.
 */
final class M1ValuePolicyTest extends TestCase
{
    private string $prevTz = 'UTC';

    protected function setUp(): void
    {
        $this->prevTz = date_default_timezone_get();
        date_default_timezone_set('America/New_York');
    }

    protected function tearDown(): void
    {
        date_default_timezone_set($this->prevTz);
    }

    // ---- U64: the dual wire form (hazard 28) ------------------------------------------------------

    /** Hazard 28: a U64 arrives as int OR decimal-string depending on MAGNITUDE, not on its tag. */
    public function testU64AcceptsBothWireForms(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions(u64Overflow: 'string'));
        self::assertSame('5', $p->decode(C::TAG_U64, 5));                       // small: PHP int
        self::assertSame('4294967296', $p->decode(C::TAG_U64, '4294967296'));   // >2^32: decimal string
        self::assertSame('18446744073709551615', $p->decode(C::TAG_U64, '18446744073709551615'));
    }

    public function testU64OverflowPolicies(): void
    {
        $big = '18446744073709551615';
        self::assertInstanceOf(U64::class, (new M1ValuePolicy(new TypePolicyOptions()))->decode(C::TAG_U64, $big));
        self::assertSame($big, (new M1ValuePolicy(new TypePolicyOptions(u64Overflow: 'string')))->decode(C::TAG_U64, $big));
        // A POLICY REFUSAL, not a wire fault (F30).
        $this->expectException(TypePolicyException::class);
        (new M1ValuePolicy(new TypePolicyOptions(u64Overflow: 'error')))->decode(C::TAG_U64, $big);
    }

    /** A value that FITS PHP_INT_MAX must come back as a plain int regardless of wire form. */
    public function testU64WithinIntRangeIsAnInt(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions());
        self::assertSame(4294967296, $p->decode(C::TAG_U64, '4294967296'));
        self::assertSame(5, $p->decode(C::TAG_U64, 5));
        self::assertSame(PHP_INT_MAX, $p->decode(C::TAG_U64, (string) PHP_INT_MAX));
    }

    /** `u64_overflow=error` governs the OVERFLOW alone — a value that fits still decodes. */
    public function testU64OverflowErrorOnlyRefusesAboveIntMax(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions(u64Overflow: 'error'));
        self::assertSame(PHP_INT_MAX, $p->decode(C::TAG_U64, (string) PHP_INT_MAX));
        $this->expectException(TypePolicyException::class);
        $p->decode(C::TAG_U64, '9223372036854775808'); // PHP_INT_MAX + 1
    }

    /** A payload above `u64::MAX` cannot have come off a `uint64` wire — that is a WIRE fault. */
    public function testU64AboveTheUnsignedRangeIsAWireFault(): void
    {
        $this->expectException(ProtocolException::class);
        (new M1ValuePolicy(new TypePolicyOptions()))->decode(C::TAG_U64, '18446744073709551616');
    }

    // ---- DECIMAL ---------------------------------------------------------------------------------

    /** §9: DECIMAL is string-backed and EXACT — display scale survives. */
    public function testDecimalPreservesDisplayScale(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions());
        self::assertSame('1.10', (string) $p->decode(C::TAG_DECIMAL, '1.10'));
        self::assertSame('1.1', (string) $p->decode(C::TAG_DECIMAL, '1.1'));
        self::assertSame('NaN', (string) $p->decode(C::TAG_DECIMAL, 'NaN'));
        self::assertSame('Infinity', (string) $p->decode(C::TAG_DECIMAL, 'Infinity'));
        self::assertSame('-Infinity', (string) $p->decode(C::TAG_DECIMAL, '-Infinity'));
        self::assertInstanceOf(Decimal::class, $p->decode(C::TAG_DECIMAL, '-12345.6700'));
    }

    /** F31: the string policies must be exercised, not just declared. */
    public function testStringPoliciesReturnCanonicalTextVerbatim(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions(decimal: 'string', uuid: 'string'));
        self::assertSame('1.10', $p->decode(C::TAG_DECIMAL, '1.10'));
        self::assertSame(
            '3f2b8c1a-0000-4fff-8000-abcdefabcdef',
            $p->decode(C::TAG_UUID, '3f2b8c1a-0000-4fff-8000-abcdefabcdef'),
        );
    }

    /** The string forms still REFUSE a malformed payload — they are a representation, not a bypass. */
    public function testStringPoliciesStillValidate(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions(decimal: 'string', uuid: 'string'));
        $this->expectException(ProtocolException::class);
        $p->decode(C::TAG_UUID, '3F2B8C1A-0000-4FFF-8000-ABCDEFABCDEF'); // uppercase is not canonical
    }

    // ---- the naive-vs-instant split (F14/F26) ----------------------------------------------------

    public function testTimestampTzIsAUtcInstantAndTimestampIsNaive(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions());
        $tz = $p->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07.250000Z');
        self::assertInstanceOf(\DateTimeImmutable::class, $tz);
        self::assertNotInstanceOf(NaiveTimestamp::class, $tz);
        self::assertSame('UTC', $tz->getTimezone()->getName());
        self::assertSame(0, $tz->getOffset());
        self::assertSame('2026-08-05 13:45:07.250000', $tz->format('Y-m-d H:i:s.u'));

        $naive = $p->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07.250000');
        self::assertInstanceOf(NaiveTimestamp::class, $naive);     // binds back as TAG_TIMESTAMP
        self::assertInstanceOf(\DateTimeImmutable::class, $naive); // §9's PHP column stays true
        self::assertSame('2026-08-05 13:45:07.250000', $naive->format('Y-m-d H:i:s.u'));
        // F26: under naive_datetime_zone=utc the object is EXPLICITLY UTC — these two assertions are
        // what make the test able to fail while date_default_timezone_set('America/New_York').
        self::assertSame('UTC', $naive->getTimezone()->getName());
        self::assertSame(0, $naive->getOffset());
        // and the canonical text survives the round trip byte-for-byte (Task 8a's bind path).
        self::assertSame('2026-08-05 13:45:07.250000', $naive->toCanonicalText());
    }

    /** A zero sub-second part carries NO `.ffffff` group, in either direction (PROTOCOL.md §3.2). */
    public function testWholeSecondTimestampsOmitTheFraction(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions());
        $naive = $p->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07');
        self::assertInstanceOf(NaiveTimestamp::class, $naive);
        self::assertSame('2026-08-05 13:45:07', $naive->toCanonicalText());
        self::assertSame('UTC', $naive->getTimezone()->getName());

        $tz = $p->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07Z');
        self::assertInstanceOf(\DateTimeImmutable::class, $tz);
        self::assertSame(1785937507, $tz->getTimestamp());
    }

    /** F30: `error` is scoped to TAG_TIMESTAMP ONLY — the other date/time tags decode normally. */
    public function testNaiveDatetimeZoneErrorIsScopedToTimestampOnly(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions(naiveDatetimeZone: 'error'));
        self::assertInstanceOf(\DateTimeImmutable::class, $p->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07Z'));
        self::assertInstanceOf(Date::class, $p->decode(C::TAG_DATE, '2026-08-05'));
        self::assertInstanceOf(Time::class, $p->decode(C::TAG_TIME, '13:45:07'));
        $this->expectException(TypePolicyException::class); // a policy refusal, not a wire fault
        $p->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07');
    }

    // ---- sentinels (PROTOCOL.md §3.2: literal, deliberately NOT parseable) ------------------------

    /**
     * The four sentinels are carried VERBATIM. Feeding one to a date parser yields an exception or a
     * nonsense date — both silent-corruption classes — so the decoder branches on them first.
     *
     * @return list<array{int,string}>
     */
    public static function sentinelProvider(): array
    {
        return [
            [C::TAG_DATE, 'infinity'],
            [C::TAG_DATE, '-infinity'],
            [C::TAG_DATE, '0000-00-00'],
            [C::TAG_TIMESTAMP, 'infinity'],
            [C::TAG_TIMESTAMP, '-infinity'],
            [C::TAG_TIMESTAMP, '0000-00-00 00:00:00'],
            [C::TAG_TIMESTAMPTZ, 'infinity'],
            [C::TAG_TIMESTAMPTZ, '-infinity'],
            [C::TAG_TIMESTAMPTZ, '0000-00-00 00:00:00'],
        ];
    }

    #[DataProvider('sentinelProvider')]
    public function testSentinelsAreCarriedVerbatimAndNeverParsed(int $tag, string $text): void
    {
        $decoded = (new M1ValuePolicy(new TypePolicyOptions()))->decode($tag, $text);
        if ($tag === C::TAG_DATE) {
            self::assertInstanceOf(Date::class, $decoded);
            self::assertSame($text, (string) $decoded);
            self::assertTrue($decoded->isSentinel());
            return;
        }
        // A TIMESTAMP/TIMESTAMPTZ sentinel is not an instant: it comes back as the canonical text.
        self::assertSame($text, $decoded);
    }

    /** MySQL zero-IN-date components (`2026-00-05`) are legal and equally un-constructible. */
    public function testZeroInDateComponentsAreCarriedVerbatim(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions());
        self::assertSame('2026-00-05', (string) $p->decode(C::TAG_DATE, '2026-00-05'));
        self::assertSame('2026-08-00 01:02:03', $p->decode(C::TAG_TIMESTAMP, '2026-08-00 01:02:03'));
    }

    // ---- DATE / TIME -----------------------------------------------------------------------------

    public function testDateAndTimeCarryTheirCanonicalText(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions());
        $d = $p->decode(C::TAG_DATE, '2026-08-05');
        self::assertInstanceOf(Date::class, $d);
        self::assertSame('2026-08-05', (string) $d);
        self::assertFalse($d->isSentinel());

        // PG `time '24:00:00'` does NOT wrap, and a MySQL TIME may be negative / exceed 24 h.
        foreach (['13:45:07', '13:45:07.250000', '24:00:00', '838:59:59', '-838:59:59'] as $t) {
            $v = $p->decode(C::TAG_TIME, $t);
            self::assertInstanceOf(Time::class, $v);
            self::assertSame($t, (string) $v);
        }
        self::assertTrue($p->decode(C::TAG_TIME, '-01:00:00')->isNegative());
        self::assertFalse($p->decode(C::TAG_TIME, '01:00:00')->isNegative());
    }

    // ---- malformed payloads throw, always --------------------------------------------------------

    /** Hazard 30: MALFORMED payloads throw ProtocolException — never a silent zero/empty coercion. */
    public function testMalformedPayloadsThrowAndNeverCoerce(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions());
        $refused = 0;
        $cases = [[C::TAG_DECIMAL, 'not-a-number'], [C::TAG_DATE, '2026-13-99'],
            [C::TAG_UUID, 'nope'], [C::TAG_TIMESTAMP, ''], [C::TAG_U64, 'x1'],
            [C::TAG_DECIMAL, 42], [C::TAG_JSON, ['a']], [C::TAG_DATE, '2026-02-30'],
            [C::TAG_TIME, '13:45'], [C::TAG_TIME, '13:60:00'], [C::TAG_TIMESTAMPTZ, '2026-08-05 13:45:07'],
            [C::TAG_TIMESTAMP, '2026-08-05T13:45:07Z'], [C::TAG_DECIMAL, '1.2.3'],
            [C::TAG_U64, -1], [C::TAG_TIME, null], [C::TAG_DATE, 20260805],
            [C::TAG_TIMESTAMP, '2026-08-05 13:45:07.25'],
        ];
        foreach ($cases as [$tag, $bad]) {
            try {
                $p->decode($tag, $bad);
                self::fail("tag $tag accepted a malformed payload: " . var_export($bad, true));
            } catch (ProtocolException) {
                ++$refused;
            }
        }
        self::assertCount($refused, $cases);
    }

    /** The M0 scalar arms are strict too — no `(int) 'abc' === 0` / `(string) [] === ''` coercion. */
    public function testM0ScalarArmsAreStrictAsWell(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions());
        self::assertNull($p->decode(C::TAG_NULL, null));
        self::assertTrue($p->decode(C::TAG_BOOL, true));
        self::assertSame(42, $p->decode(C::TAG_I64, 42));
        self::assertSame(1.5, $p->decode(C::TAG_F64, 1.5));
        self::assertSame('hi', $p->decode(C::TAG_TEXT, 'hi'));
        self::assertSame("\x01\x02\xff", $p->decode(C::TAG_BYTES, [0x01, 0x02, 0xff]));

        $refused = 0;
        $cases = [
            [C::TAG_BOOL, 1], [C::TAG_I64, '7'], [C::TAG_F64, 'x'], [C::TAG_TEXT, 7],
            // §3.1 pins F64's family to `float64`; an int would cast lossily above 2^53.
            [C::TAG_F64, 1], [C::TAG_NULL, 'x'], [C::TAG_BYTES, [1, 'x']], [C::TAG_BYTES, [256]],
        ];
        foreach ($cases as [$tag, $bad]) {
            try {
                $p->decode($tag, $bad);
                self::fail("tag $tag coerced " . var_export($bad, true));
            } catch (ProtocolException) {
                ++$refused;
            }
        }
        self::assertCount($refused, $cases);
    }

    // ---- JSON ------------------------------------------------------------------------------------

    /** JSON is LAZY: no decode cost until access, and invalid JSON fails on access, not on row read. */
    public function testJsonIsLazy(): void
    {
        $j = (new M1ValuePolicy(new TypePolicyOptions()))->decode(C::TAG_JSON, '{"a":[1,2]}');
        self::assertInstanceOf(Json::class, $j);
        self::assertSame('{"a":[1,2]}', (string) $j);
        $decoded = $j->decoded();
        self::assertIsArray($decoded);
        self::assertSame([1, 2], $decoded['a']);
        $bad = (new M1ValuePolicy(new TypePolicyOptions()))->decode(C::TAG_JSON, '{oops');
        self::assertInstanceOf(Json::class, $bad); // construction succeeded — laziness is the point
        $this->expectException(ProtocolException::class);
        $bad->decoded();
    }

    // ---- the deferred tags -----------------------------------------------------------------------

    /** The deferred tags must still be a loud, named failure. */
    public function testDeferredTagsStillThrowNamingTheTag(): void
    {
        $p = new M1ValuePolicy(new TypePolicyOptions());
        foreach ([C::TAG_ARRAY, C::TAG_INTERVAL, C::TAG_INET, C::TAG_VECTOR] as $tag) {
            try {
                $p->decode($tag, null);
                self::fail("tag $tag must be unsupported");
            } catch (ProtocolException $e) {
                self::assertStringContainsString((string) $tag, $e->getMessage());
            }
        }
    }

    // ---- the S8 hand-off -------------------------------------------------------------------------

    /** F31: the S8 DBAL hand-off — a whole row of driver-native strings, no value objects. */
    public function testRawStringPolicyReturnsCanonicalTextForEveryTag(): void
    {
        $p = new RawStringValuePolicy();
        self::assertSame('1.10', $p->decode(C::TAG_DECIMAL, '1.10'));
        self::assertSame('2026-08-05', $p->decode(C::TAG_DATE, '2026-08-05'));
        self::assertSame('13:45:07', $p->decode(C::TAG_TIME, '13:45:07'));
        self::assertSame('2026-08-05 13:45:07.250000', $p->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07.250000'));
        self::assertSame('2026-08-05T13:45:07.250000Z', $p->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07.250000Z'));
        self::assertSame('3f2b8c1a-0000-4fff-8000-abcdefabcdef', $p->decode(C::TAG_UUID, '3f2b8c1a-0000-4fff-8000-abcdefabcdef'));
        self::assertSame('{"a":1}', $p->decode(C::TAG_JSON, '{"a":1}'));
        self::assertSame('18446744073709551615', $p->decode(C::TAG_U64, '18446744073709551615'));
        // The M0 scalars keep their natural PHP types — only the 8 new tags become strings.
        self::assertSame(7, $p->decode(C::TAG_I64, 7));
        self::assertTrue($p->decode(C::TAG_BOOL, true));
        self::assertNull($p->decode(C::TAG_NULL, null));
        self::assertSame("\x01\xff", $p->decode(C::TAG_BYTES, [0x01, 0xff]));
    }

    /** A whole row decodes to pure strings for the eight canonical tags (the DBAL driver contract). */
    public function testRawStringPolicyDecodesAWholeRowToStrings(): void
    {
        $p = new RawStringValuePolicy();
        $row = [
            [C::TAG_DECIMAL, '-12345.6700'],
            [C::TAG_DATE, 'infinity'],
            [C::TAG_TIME, '-838:59:59'],
            [C::TAG_TIMESTAMP, '0000-00-00 00:00:00'],
            [C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07Z'],
            [C::TAG_UUID, '3f2b8c1a-0000-4fff-8000-abcdefabcdef'],
            [C::TAG_JSON, '{"a":1}'],
            [C::TAG_U64, 5],
        ];
        foreach ($row as [$tag, $data]) {
            self::assertIsString($p->decode($tag, $data), "tag $tag must decode to a string");
        }
        self::assertSame('5', $p->decode(C::TAG_U64, 5)); // the int wire form normalizes too
    }

    /** Even the identity policy refuses a non-string payload rather than coercing it (hazard 30). */
    public function testRawStringPolicyRefusesANonStringPayload(): void
    {
        $this->expectException(ProtocolException::class);
        (new RawStringValuePolicy())->decode(C::TAG_DECIMAL, 42);
    }

    public function testRawStringPolicyStillRefusesTheDeferredTags(): void
    {
        $this->expectException(ProtocolException::class);
        (new RawStringValuePolicy())->decode(C::TAG_ARRAY, null);
    }

    /** An unknown tag number is never silently swallowed. */
    public function testUnknownTagIsALoudFailure(): void
    {
        $this->expectException(ProtocolException::class);
        (new M1ValuePolicy(new TypePolicyOptions()))->decode(99, 'whatever');
    }

    /** The bare constructor is the §9.1 default set (safe object forms). */
    public function testDefaultConstructorUsesTheSafeObjectForms(): void
    {
        $p = new M1ValuePolicy();
        self::assertInstanceOf(Decimal::class, $p->decode(C::TAG_DECIMAL, '1.10'));
        self::assertInstanceOf(Uuid::class, $p->decode(C::TAG_UUID, '3f2b8c1a-0000-4fff-8000-abcdefabcdef'));
    }
}
