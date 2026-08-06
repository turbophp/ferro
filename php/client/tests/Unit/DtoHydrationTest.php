<?php // /php/client/tests/Unit/DtoHydrationTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\HydrationException;
use Ferro\Client\ExecCodec;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Client\Value\ValuePolicy;
use Ferro\Decimal;
use Ferro\NaiveTimestamp;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Tests\Support\InvoiceDto;
use Ferro\Tests\Support\StringAmountInvoiceDto;
use PHPUnit\Framework\TestCase;

/**
 * **M1-S7 Task 9 Step 3 — the native-API DTO path (hazard 35).**
 *
 * Through M1-S6 every decoded cell was a PHP scalar, so `ReflectionClass::newInstanceArgs` could
 * only fail on a genuinely broken DTO. As of M1-S7 a `DECIMAL` column hydrates to a {@see Decimal}
 * and a `TIMESTAMP` to a {@see NaiveTimestamp}, which makes DTO hydration a real type boundary. The
 * three measured behaviours pinned here:
 *
 * 1. A DTO typed with the §9 value objects hydrates.
 * 2. A `string`-typed parameter STILL hydrates, because every §9 value object is `Stringable` and a
 *    reflection call runs in WEAK typing mode (`strict_types` does not propagate into calls made by
 *    internal functions) — so the object coerces to its canonical text. `\DateTimeImmutable` is the
 *    exception: it is not `Stringable`, so a date/time column cannot feed a `string` property.
 * 3. Where hydration genuinely cannot happen it fails INSIDE the {@see FerroException} contract as
 *    a {@see HydrationException} — never as a bare `\TypeError` leaking engine internals. The
 *    reachable case is a **sentinel** row: a `TIMESTAMP`/`TIMESTAMPTZ` column hands back a
 *    `NaiveTimestamp` for ordinary values and the canonical TEXT for `infinity` / a MySQL zero
 *    datetime, so a `\DateTimeImmutable`-typed DTO hydrates for most rows and fails on that one.
 *    That failure being LOUD is the whole point (SPEC §9).
 *
 * Every test drives the full decode → hydrate path (canonical wire cells in, DTO out), not a
 * hand-built argument array, so the value objects under test are the ones the policy really makes.
 */
final class DtoHydrationTest extends TestCase
{
    private function codec(?ValuePolicy $values = null): ExecCodec
    {
        return new ExecCodec(
            $values ?? new M1ValuePolicy(),
            new PlanCache(),
            PackerFactory::forEncode(),
            PackerFactory::forDecode(),
        );
    }

    /**
     * One canonical wire row for `id, amount, at` as the engine would frame it.
     *
     * @return list<array{tag:int,data:mixed}>
     */
    private static function wireRow(mixed $at = '2026-08-05T11:45:07.250000Z', int $atTag = C::TAG_TIMESTAMPTZ): array
    {
        return [
            ['tag' => C::TAG_I64, 'data' => 7],
            ['tag' => C::TAG_DECIMAL, 'data' => '-12345.6700000000'],
            ['tag' => $atTag, 'data' => $at],
        ];
    }

    /** @return list<string> */
    private static function cols(): array
    {
        return ['id', 'amount', 'at'];
    }

    public function testDtoWithValueObjectTypedParametersHydrates(): void
    {
        $codec = $this->codec();
        $row = $codec->decodeRow(self::wireRow());

        $dto = $codec->hydrateDto(InvoiceDto::class, self::cols(), $row);

        $this->assertInstanceOf(InvoiceDto::class, $dto);
        $this->assertSame(7, $dto->id);
        $this->assertInstanceOf(Decimal::class, $dto->amount);
        // The display scale survives the whole path: a Decimal is string-backed, never re-rendered.
        $this->assertSame('-12345.6700000000', $dto->amount->value);
        $this->assertSame('2026-08-05T11:45:07.250000+00:00', $dto->at->format('Y-m-d\TH:i:s.uP'));
        $this->assertSame('UTC', $dto->at->getTimezone()->getName());
    }

    /** A naive TIMESTAMP hydrates into the same `\DateTimeImmutable` slot — NaiveTimestamp IS one. */
    public function testNaiveTimestampHydratesIntoADateTimeImmutableParameter(): void
    {
        $codec = $this->codec();
        $row = $codec->decodeRow(self::wireRow('2026-08-05 13:45:07.250000', C::TAG_TIMESTAMP));

        $dto = $codec->hydrateDto(InvoiceDto::class, self::cols(), $row);

        $this->assertInstanceOf(NaiveTimestamp::class, $dto->at, 'a naive column stays discriminable');
        $this->assertSame('2026-08-05 13:45:07.250000', $dto->at->format('Y-m-d H:i:s.u'));
    }

    /**
     * MEASURED, and pinned so S8 can rely on it: a `string`-typed DTO property still works, because
     * the §9 value objects are `Stringable` and a reflection call is weakly typed. The coerced text
     * is the CANONICAL text, so the DTO carries exactly what the wire carried.
     */
    public function testStringableValueObjectsCoerceIntoAStringTypedParameter(): void
    {
        $codec = $this->codec();
        $row = $codec->decodeRow(self::wireRow());

        $dto = $codec->hydrateDto(StringAmountInvoiceDto::class, self::cols(), $row);

        $this->assertSame('-12345.6700000000', $dto->amount);
    }

    /**
     * THE CONTRACT TEST, on the reachable case: a `TIMESTAMPTZ` **sentinel** decodes to canonical
     * TEXT (it is not an instant), so a `\DateTimeImmutable`-typed DTO cannot take it. It must fail
     * as a {@see HydrationException}, not a bare `\TypeError`.
     */
    public function testSentinelRowFailsLoudlyInsideTheFerroExceptionContract(): void
    {
        $codec = $this->codec();
        $row = $codec->decodeRow(self::wireRow('infinity'));
        $this->assertSame('infinity', $row[2], 'a sentinel is canonical TEXT, never a parsed date');

        try {
            $codec->hydrateDto(InvoiceDto::class, self::cols(), $row);
            $this->fail('expected a HydrationException for a sentinel in a \DateTimeImmutable slot');
        } catch (\TypeError $e) {
            $this->fail('a bare \TypeError escaped the FerroException contract: ' . $e->getMessage());
        } catch (HydrationException $e) {
            $this->assertInstanceOf(FerroException::class, $e, 'HydrationException is in the FerroException tree');
            $this->assertStringContainsString(InvoiceDto::class, $e->getMessage());
            // The diagnosis names the offending column AND the value's actual PHP type.
            $this->assertStringContainsString('at: string', $e->getMessage());
            // ...and the way out (SPEC §9.1), so the message is actionable.
            $this->assertStringContainsString('§9.1', $e->getMessage());
            $this->assertInstanceOf(\TypeError::class, $e->getPrevious(), 'the original \TypeError is preserved');
        }
    }

    /** The same contract for the other direction: a plain TEXT cell into a `Decimal` parameter. */
    public function testAWrongColumnTypeIsAlsoAHydrationException(): void
    {
        $codec = $this->codec();
        $row = $codec->decodeRow([
            ['tag' => C::TAG_I64, 'data' => 7],
            ['tag' => C::TAG_TEXT, 'data' => 'not a decimal column'],
            ['tag' => C::TAG_TIMESTAMPTZ, 'data' => '2026-08-05T11:45:07.250000Z'],
        ]);

        $this->expectException(HydrationException::class);
        $this->expectExceptionMessageMatches('/amount: string/');
        $codec->hydrateDto(InvoiceDto::class, self::cols(), $row);
    }

    /**
     * The escape hatch the message advertises actually works: decoding the SAME row with a
     * string-valued policy makes a string-typed DTO hydrate. Without this the exception message
     * would be advice the client cannot honour.
     */
    public function testTheDocumentedStringPolicyEscapeHatchIsReal(): void
    {
        $codec = $this->codec(new M1ValuePolicy(new TypePolicyOptions(decimal: 'string')));
        $row = $codec->decodeRow(self::wireRow());

        $dto = $codec->hydrateDto(StringAmountInvoiceDto::class, self::cols(), $row);
        $this->assertSame('-12345.6700000000', $dto->amount);
    }

    /** The S8 DBAL hand-off policy leaves every canonical cell a raw string — the same hatch, one step further. */
    public function testRawStringPolicyAlsoFeedsAStringTypedDto(): void
    {
        $codec = $this->codec(new RawStringValuePolicy());
        $row = $codec->decodeRow(self::wireRow());
        $this->assertSame('-12345.6700000000', $row[1]);
        $this->assertSame('2026-08-05T11:45:07.250000Z', $row[2]);
    }

    /** A missing column stays the SAME exception class — one contract for the whole DTO path. */
    public function testMissingColumnIsAlsoAHydrationException(): void
    {
        $codec = $this->codec();
        $this->expectException(HydrationException::class);
        $codec->hydrateDto(InvoiceDto::class, ['id', 'amount'], [1, new Decimal('1.00')]);
    }
}
