<?php // /php/client/tests/Unit/ValueObjectsTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Error\ProtocolException;
use Ferro\Date;
use Ferro\Decimal;
use Ferro\Json;
use Ferro\NaiveTimestamp;
use Ferro\Time;
use Ferro\U64;
use Ferro\Uuid;
use PHPUnit\Framework\TestCase;

/**
 * The SPEC §9 value objects, standalone: each one is CANONICAL-TEXT backed (so a read → write-back
 * round trip is byte-stable), each VALIDATES in its constructor and each THROWS rather than coercing
 * (hazard 30 — the M0 `toInt`/`toStr` idiom would turn a malformed DECIMAL into `Decimal('')`).
 *
 * They live at the `src/` ROOT (`Ferro\Decimal` → `src/Decimal.php`) because `composer.json` maps
 * `Ferro\ => src/`: a file at `src/Value/Decimal.php` declaring `namespace Ferro;` cannot autoload
 * (hazard 40). This test would fatal on class-not-found if that ever regressed.
 */
final class ValueObjectsTest extends TestCase
{
    // ---- Decimal ---------------------------------------------------------------------------------

    public function testDecimalIsExactAndNeverNormalizes(): void
    {
        self::assertSame('1.10', (string) new Decimal('1.10'));
        self::assertSame('1.1', (string) new Decimal('1.1'));
        self::assertNotEquals(new Decimal('1.1'), new Decimal('1.10'));
        self::assertSame('-12345.6700', (new Decimal('-12345.6700'))->value);
        self::assertSame('+7', (string) new Decimal('+7'));
        // 200 integral digits + 50 fractional: PG NUMERIC goes to 131 072 integral digits, so no
        // fixed-width numeric type may ever sit in this path.
        $huge = str_repeat('9', 200) . '.' . str_repeat('1', 50);
        self::assertSame($huge, (string) new Decimal($huge));
    }

    public function testDecimalSpecials(): void
    {
        foreach (['NaN', 'Infinity', '-Infinity'] as $s) {
            $d = new Decimal($s);
            self::assertSame($s, (string) $d);
            self::assertFalse($d->isFinite());
        }
        self::assertTrue((new Decimal('0.00'))->isFinite());
    }

    public function testDecimalRejectsNonDecimalText(): void
    {
        $bad_ = ['', 'not-a-number', '1.2.3', '1e5', '1.', '.5', ' 1', '1 ', 'nan', 'INF'];
        $refused = 0;
        foreach ($bad_ as $bad) {
            try {
                new Decimal($bad);
                self::fail('Decimal accepted ' . var_export($bad, true));
            } catch (ProtocolException) {
                ++$refused;
            }
        }
        self::assertCount($refused, $bad_);
    }

    // ---- Date ------------------------------------------------------------------------------------

    public function testDateCarriesCanonicalTextAndSentinels(): void
    {
        self::assertSame('2026-08-05', (string) new Date('2026-08-05'));
        self::assertFalse((new Date('2026-08-05'))->isSentinel());
        foreach (['infinity', '-infinity', '0000-00-00', '2026-00-05', '2026-08-00'] as $s) {
            $d = new Date($s);
            self::assertSame($s, (string) $d);
            self::assertTrue($d->isSentinel(), "$s is not constructible as a calendar date");
        }
    }

    public function testDateRejectsImpossibleCalendarValues(): void
    {
        $bad_ = ['2026-13-01', '2026-01-32', '2026-02-30', '2025-02-29', '2026-8-5', '20260805',
            '2026-08-05 00:00:00', 'INFINITY', ''];
        $refused = 0;
        foreach ($bad_ as $bad) {
            try {
                new Date($bad);
                self::fail('Date accepted ' . var_export($bad, true));
            } catch (ProtocolException) {
                ++$refused;
            }
        }
        self::assertCount($refused, $bad_);
        self::assertSame('2024-02-29', (string) new Date('2024-02-29')); // a real leap day
    }

    // ---- Time ------------------------------------------------------------------------------------

    public function testTimeSpansTheFullBackendRange(): void
    {
        foreach (['00:00:00', '13:45:07', '13:45:07.250000', '24:00:00', '838:59:59', '-838:59:59'] as $t) {
            self::assertSame($t, (string) new Time($t));
        }
        self::assertTrue((new Time('-00:00:01'))->isNegative());
        self::assertFalse((new Time('00:00:01'))->isNegative());
    }

    public function testTimeRejectsMalformedText(): void
    {
        $bad_ = ['13:45', '1:45:07', '13:60:00', '13:45:60', '13:45:07.25', '13:45:07.2500000',
            '', 'infinity', '13:45:07Z'];
        $refused = 0;
        foreach ($bad_ as $bad) {
            try {
                new Time($bad);
                self::fail('Time accepted ' . var_export($bad, true));
            } catch (ProtocolException) {
                ++$refused;
            }
        }
        self::assertCount($refused, $bad_);
    }

    // ---- Uuid ------------------------------------------------------------------------------------

    public function testUuidIsCanonicalLowercaseHyphenated(): void
    {
        $u = new Uuid('3f2b8c1a-0000-4fff-8000-abcdefabcdef');
        self::assertSame('3f2b8c1a-0000-4fff-8000-abcdefabcdef', (string) $u);
        self::assertSame('3f2b8c1a-0000-4fff-8000-abcdefabcdef', $u->value);
    }

    public function testUuidRejectsEveryNonCanonicalForm(): void
    {
        $bad_ = [
            '3F2B8C1A-0000-4FFF-8000-ABCDEFABCDEF',   // uppercase
            '3f2b8c1a00004fff8000abcdefabcdef',       // unhyphenated
            '{3f2b8c1a-0000-4fff-8000-abcdefabcdef}', // braced
            '3f2b8c1a-0000-4fff-8000-abcdefabcde',    // short
            'nope', '',
        ];
        $refused = 0;
        foreach ($bad_ as $bad) {
            try {
                new Uuid($bad);
                self::fail('Uuid accepted ' . var_export($bad, true));
            } catch (ProtocolException) {
                ++$refused;
            }
        }
        self::assertCount($refused, $bad_);
    }

    // ---- Json ------------------------------------------------------------------------------------

    public function testJsonDecodesLazilyAndCaches(): void
    {
        $j = new Json('{"a":[1,2],"b":null}');
        self::assertSame('{"a":[1,2],"b":null}', (string) $j);
        self::assertSame('{"a":[1,2],"b":null}', $j->raw);
        $first = $j->decoded();
        self::assertIsArray($first);
        self::assertSame([1, 2], $first['a']);
        self::assertNull($first['b']);
        self::assertSame($first, $j->decoded()); // cached: decoded once, not per access
    }

    public function testJsonCachesANullDocumentToo(): void
    {
        // `null` is a valid JSON document AND the "failure" return of json_decode — a cache keyed on
        // `$cache !== null` would re-decode it forever, so the cache is keyed on a flag.
        $j = new Json('null');
        self::assertNull($j->decoded());
        self::assertNull($j->decoded());
    }

    public function testJsonConstructionNeverParsesAndInvalidJsonFailsOnAccess(): void
    {
        $bad = new Json('{oops');           // construction MUST succeed — laziness is the contract
        self::assertSame('{oops', (string) $bad);
        $this->expectException(ProtocolException::class);
        $bad->decoded();
    }

    // ---- U64 -------------------------------------------------------------------------------------

    public function testU64CarriesTheFullUnsignedRange(): void
    {
        $max = new U64('18446744073709551615');
        self::assertSame('18446744073709551615', (string) $max);
        self::assertFalse($max->fitsInt());

        $small = new U64(5);
        self::assertSame('5', (string) $small);
        self::assertTrue($small->fitsInt());
        self::assertSame(5, $small->toInt());

        self::assertSame('0', (string) new U64('0000')); // leading zeros normalize away
        self::assertTrue((new U64((string) PHP_INT_MAX))->fitsInt());
        self::assertFalse((new U64('9223372036854775808'))->fitsInt());
    }

    public function testU64ToIntRefusesToTruncate(): void
    {
        $this->expectException(\RangeException::class);
        (new U64('18446744073709551615'))->toInt();
    }

    public function testU64RejectsOutOfRangeAndMalformedText(): void
    {
        $bad_ = ['18446744073709551616', '-1', '1.0', '', 'x1', '1e3'];
        $refused = 0;
        foreach ($bad_ as $bad) {
            try {
                new U64($bad);
                self::fail('U64 accepted ' . var_export($bad, true));
            } catch (ProtocolException) {
                ++$refused;
            }
        }
        self::assertCount($refused, $bad_);
        try {
            new U64(-1);
            self::fail('U64 accepted a negative int');
        } catch (ProtocolException) { /* expected */ }
    }

    // ---- NaiveTimestamp --------------------------------------------------------------------------

    public function testNaiveTimestampIsADateTimeImmutablePinnedToUtc(): void
    {
        $prev = date_default_timezone_get();
        date_default_timezone_set('America/New_York');
        try {
            $n = NaiveTimestamp::fromCanonicalText('2026-08-05 13:45:07.250000');
            self::assertInstanceOf(\DateTimeImmutable::class, $n);
            self::assertInstanceOf(\DateTimeInterface::class, $n);
            // The zone assertions are the ones that can actually FAIL under a non-UTC default —
            // format('Y-m-d H:i:s.u') alone is satisfied by a wrongly-zoned object (F26).
            self::assertSame('UTC', $n->getTimezone()->getName());
            self::assertSame(0, $n->getOffset());
            self::assertSame('2026-08-05 13:45:07.250000', $n->format('Y-m-d H:i:s.u'));
            self::assertSame('2026-08-05 13:45:07.250000', $n->toCanonicalText());
            self::assertSame('2026-08-05 13:45:07', NaiveTimestamp::fromCanonicalText('2026-08-05 13:45:07')->toCanonicalText());
        } finally {
            date_default_timezone_set($prev);
        }
    }

    public function testNaiveTimestampRejectsNonCanonicalText(): void
    {
        $bad_ = ['2026-08-05T13:45:07Z', '2026-08-05', 'infinity', '0000-00-00 00:00:00',
            '2026-13-01 00:00:00', '2026-08-05 13:45:07.25', ''];
        $refused = 0;
        foreach ($bad_ as $bad) {
            try {
                NaiveTimestamp::fromCanonicalText($bad);
                self::fail('NaiveTimestamp accepted ' . var_export($bad, true));
            } catch (ProtocolException) {
                ++$refused;
            }
        }
        self::assertCount($refused, $bad_);
    }
}
