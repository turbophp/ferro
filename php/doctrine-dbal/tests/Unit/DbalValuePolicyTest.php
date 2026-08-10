<?php // /php/doctrine-dbal/tests/Unit/DbalValuePolicyTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Doctrine\DBAL\Types\Type;
use Ferro\Client\Error\ProtocolException;
use Ferro\DBAL\Exception\NonRepresentableValue;
use Ferro\DBAL\PlatformVersion;
use Ferro\DBAL\Value\DbalValuePolicy;
use Ferro\Protocol\Generated\Constants as C;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 9 — the type boundary.
 *
 * Two opposite jobs. **(a) Make `TIMESTAMPTZ` readable at all**: our canonical text is RFC3339
 * (`2026-08-05T13:45:07Z`) and DBAL's `DateTimeTzType` has NO fallback, so on PostgreSQL it accepts
 * only `Y-m-d H:i:sO` and on MySQL only `Y-m-d H:i:s`. Measured: every canonical form throws on
 * every platform. **(b) Refuse the values DBAL's parser would corrupt SILENTLY.** Measured on
 * 4.4.4, with NO exception raised:
 *     date     '2026-00-05'           -> DateTime(2025-12-05)
 *     datetime '0000-00-00 00:00:00'  -> DateTime(-0001-11-30)
 *     time     '24:00:00'             -> 00:00:00
 * `proto/PROTOCOL.md` §3.2 warns about that parser class in prose; the measurement is why this
 * policy refuses rather than hoping.
 *
 * The second test below is the load-bearing one: it drives the refused values through the STOCK
 * DBAL type layer to prove the corruption is real, and then through the policy to prove we stop it.
 */
final class DbalValuePolicyTest extends TestCase
{
    private function pg(): DbalValuePolicy
    {
        $p = new DbalValuePolicy();
        $p->bindBackend(PlatformVersion::KIND_POSTGRES);
        return $p;
    }

    private function mysql(): DbalValuePolicy
    {
        $p = new DbalValuePolicy();
        $p->bindBackend(PlatformVersion::KIND_MYSQL);
        return $p;
    }

    /** A whole-second TIMESTAMPTZ is re-rendered into the platform's own format, per family. */
    public function testTimestampTzIsRerenderedPerFamilyAndParsesBack(): void
    {
        $pg = $this->pg()->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07Z');
        self::assertSame('2026-08-05 13:45:07+0000', $pg);
        $back = Type::getType('datetimetz')->convertToPHPValue($pg, new PostgreSQL120Platform());
        self::assertInstanceOf(\DateTimeInterface::class, $back);
        self::assertSame('2026-08-05T13:45:07+00:00', $back->format('Y-m-d\TH:i:sP'));

        self::assertSame('2026-08-05 13:45:07', $this->mysql()->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07Z'));
    }

    /**
     * THE SILENT-CORRUPTION SET. Each row is first driven through the STOCK type layer to
     * demonstrate that it converts WITHOUT an exception to the WRONG value, then through the policy
     * to prove we refuse it. Without the first half this test would only be asserting our own
     * behaviour; with it, it is a standing proof that the refusal is load-bearing.
     *
     * The per-row FORMAT is a deliberate strengthening of the plan's single `Y-m-d H:i:s` +
     * `assertStringContainsString`: `'00:00:00'` (the corrupted `24:00:00`) is a substring of the
     * formatted *midnight* of any row, so the containment form could not tell the TIME row's
     * corruption from an unrelated one. `assertSame` against the row's own format pins the exact
     * wrong value.
     *
     * @return array<string, array{0: int, 1: string, 2: string, 3: string, 4: string}>
     *   tag, wire text, DBAL type name, the WRONG value stock DBAL produces, the format that shows it
     */
    public static function corrupting(): array
    {
        return [
            'MySQL zero-in-date'  => [C::TAG_DATE, '2026-00-05', 'date', '2025-12-05', 'Y-m-d'],
            'MySQL zero date'     => [C::TAG_TIMESTAMP, '0000-00-00 00:00:00', 'datetime', '-0001-11-30 00:00:00', 'Y-m-d H:i:s'],
            'PG legal 24:00:00'   => [C::TAG_TIME, '24:00:00', 'time', '00:00:00', 'H:i:s'],
        ];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('corrupting')]
    public function testTheSilentlyCorruptingValuesAreRefused(
        int $tag,
        string $wire,
        string $dbalType,
        string $wrong,
        string $format,
    ): void {
        $stock = Type::getType($dbalType)->convertToPHPValue($wire, new PostgreSQL120Platform());
        self::assertInstanceOf(
            \DateTimeInterface::class,
            $stock,
            "stock DBAL must still SILENTLY convert $wire — if it started throwing, this refusal "
            . 'could be reconsidered',
        );
        self::assertSame($wrong, $stock->format($format), 'and to the wrong value');
        self::assertNotSame($wire, $stock->format($format), 'the round trip does not survive stock DBAL');

        $this->expectException(NonRepresentableValue::class);
        $this->pg()->decode($tag, $wire);
    }

    /** PG's `infinity` sentinels are refused too — loudly, naming the native API. */
    public function testSentinelsAreRefusedWithAnActionableMessage(): void
    {
        foreach ([[C::TAG_DATE, 'infinity'], [C::TAG_TIMESTAMP, '-infinity'], [C::TAG_TIMESTAMPTZ, 'infinity']] as [$tag, $v]) {
            try {
                $this->pg()->decode($tag, $v);
                self::fail("$v must be refused");
            } catch (NonRepresentableValue $e) {
                self::assertStringContainsString('Ferro\\Client\\Connection', $e->getMessage());
            }
        }
    }

    /**
     * A sub-second TIMESTAMPTZ has NO representation DBAL can parse (measured: every microsecond
     * form throws on every platform), so it is refused rather than TRUNCATED. Truncating would be a
     * silent precision loss, which is the same class of defect as the corruption above.
     */
    public function testASubSecondTimestampTzIsRefusedRatherThanTruncated(): void
    {
        $this->expectException(NonRepresentableValue::class);
        $this->pg()->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07.250000Z');
    }

    /**
     * A sub-second TIME has no representation either — measured, `'13:45:07.250000'` THROWS
     * `InvalidFormat` on all three platforms, so it is the one member of the refused set that DBAL
     * would report rather than corrupt. It is still refused here so the failure names the VALUE and
     * the native escape route instead of DBAL's generic "could not convert database value".
     */
    public function testASubSecondTimeIsRefused(): void
    {
        $this->expectException(NonRepresentableValue::class);
        $this->pg()->decode(C::TAG_TIME, '13:45:07.250000');
    }

    /** A MySQL negative TIME interval has no Doctrine representation at all. */
    public function testANegativeTimeIsRefused(): void
    {
        $this->expectException(NonRepresentableValue::class);
        $this->mysql()->decode(C::TAG_TIME, '-838:59:59');
    }

    /**
     * ADDED beyond the plan, and measured: `date '2026-13-45'` -> `2027-02-14` and `'2026-02-30'` ->
     * `2026-03-02`, both SILENTLY. That is the same corruption class as the zero-in-date, reached by
     * a different route — a payload that is not canonical text at all — so it is reported as the
     * WIRE fault it is ({@see ProtocolException}, the client's own vocabulary for a non-canonical
     * payload) rather than as a representable-value refusal. The policy gets this by validating
     * through `CanonicalText` before it classifies, which `RawStringValuePolicy` deliberately does
     * NOT do — its stated reason ("DBAL's own converters report anything else in their own
     * vocabulary") is exactly the assumption these measurements falsify.
     *
     * @return array<string, array{0: string, 1: string}> wire text, the WRONG date stock DBAL yields
     */
    public static function nonCalendar(): array
    {
        return ['month 13' => ['2026-13-45', '2027-02-14'], 'February 30' => ['2026-02-30', '2026-03-02']];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('nonCalendar')]
    public function testACalendarImpossibleDateIsRefusedRatherThanRolledOver(string $wire, string $wrong): void
    {
        $stock = Type::getType('date')->convertToPHPValue($wire, new PostgreSQL120Platform());
        self::assertInstanceOf(\DateTimeInterface::class, $stock);
        self::assertSame($wrong, $stock->format('Y-m-d'), 'stock DBAL rolls it over, silently');

        $this->expectException(ProtocolException::class);
        $this->pg()->decode(C::TAG_DATE, $wire);
    }

    /** Everything else passes through as canonical text — the tags DBAL already handles correctly. */
    public function testTheUnaffectedTagsAreVerbatim(): void
    {
        $p = $this->pg();
        self::assertNull($p->decode(C::TAG_NULL, null));
        self::assertTrue($p->decode(C::TAG_BOOL, true));
        self::assertSame(7, $p->decode(C::TAG_I64, 7));
        self::assertSame(1.5, $p->decode(C::TAG_F64, 1.5));
        self::assertSame('hi', $p->decode(C::TAG_TEXT, 'hi'));
        self::assertSame("\x00\xff", $p->decode(C::TAG_BYTES, [0, 255]), 'BYTES arrives as a list<int>');
        self::assertSame('8589934592', $p->decode(C::TAG_U64, '8589934592'), 'U64 normalises to a decimal string');
        self::assertSame('1.2500', $p->decode(C::TAG_DECIMAL, '1.2500'), 'display scale survives');
        self::assertSame('NaN', $p->decode(C::TAG_DECIMAL, 'NaN'), 'DecimalType passes NaN through');
        self::assertSame('{"a":1}', $p->decode(C::TAG_JSON, '{"a":1}'));
        self::assertSame('2026-08-05', $p->decode(C::TAG_DATE, '2026-08-05'));
        self::assertSame('13:45:07', $p->decode(C::TAG_TIME, '13:45:07'));
        self::assertSame(
            '2026-08-05 13:45:07.250000',
            $p->decode(C::TAG_TIMESTAMP, '2026-08-05 13:45:07.250000'),
            'a NAIVE timestamp keeps its microseconds — DateTimeType has a new DateTime() fallback',
        );
    }

    /** A temporal cell before the backend is known is a LOUD driver error, never a guess. */
    public function testDecodingATemporalTagBeforeBindBackendThrows(): void
    {
        $this->expectException(\Ferro\DBAL\Exception\DriverException::class);
        (new DbalValuePolicy())->decode(C::TAG_TIMESTAMPTZ, '2026-08-05T13:45:07Z');
    }

    /**
     * ADDED beyond the plan: the one-shot guard is written in the implementation but nothing else
     * observes it, and "which dialect am I decoding for" changing under a live connection is a
     * silent per-row format switch.
     */
    public function testBindBackendIsOneShot(): void
    {
        $p = $this->pg();
        $this->expectException(\Ferro\DBAL\Exception\DriverException::class);
        $p->bindBackend(PlatformVersion::KIND_MYSQL);
    }
}
