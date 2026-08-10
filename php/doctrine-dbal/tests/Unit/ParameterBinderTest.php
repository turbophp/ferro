<?php // /php/doctrine-dbal/tests/Unit/ParameterBinderTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\ParameterType;
use Ferro\Bytes;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\ParameterBinder;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 7 — the bind mapping keys on the PAIR `(ParameterType, PHP type)`, never on the PHP
 * type alone. Measured against `doctrine/dbal 4.4.4`'s own type layer:
 *   `BooleanType::convertToDatabaseValue(true)` → `int(1)` with `ParameterType::BOOLEAN`
 *   `FloatType` / `DecimalType` / `BigIntType`  → `ParameterType::STRING` carrying a float or a
 *                                                 numeric string
 *   `BlobType`                                  → `ParameterType::LARGE_OBJECT` carrying a string
 * A binder keyed on the PHP type alone would send that `int(1)` as `TAG_I64`, which PostgreSQL's
 * narrow per-tag pre-flight refuses against a `boolean` column.
 *
 * **Assertion identity is load-bearing here and `assertEquals` is NOT enough.** MEASURED on
 * PHPUnit 11.5.56: `assertEquals(true, 1)` PASSES and `assertEquals(42, '42')` PASSES
 * (`ScalarComparator` compares loosely unless both sides are strings). The two rows that carry the
 * whole point of this class — `int(1)` under `BOOLEAN` becoming a real `bool`, and `'42'` under
 * `INTEGER` becoming a real `int` — are therefore invisible to `assertEquals`: with the `BOOLEAN`
 * arm collapsed into the string path this file was GREEN. Every scalar expectation below is
 * asserted with `assertSame`; only the `Bytes` rows use `assertEquals`, which for an object
 * compares type + properties and still refuses a bare string.
 */
final class ParameterBinderTest extends TestCase
{
    /**
     * Every `ParameterType` case, exercised. The provider is DERIVED from `ParameterType::cases()`
     * so an eighth case added by a future DBAL release makes this test fail (the row is missing)
     * rather than silently going unmapped.
     *
     * @return array<string, array{0: ParameterType, 1: mixed, 2: mixed}>
     */
    public static function pairs(): array
    {
        $expected = [
            'NULL' => [null, null],
            'BOOLEAN' => [1, true],
            'INTEGER' => ['42', 42],
            'STRING' => ['hello', 'hello'],
            'ASCII' => ['hello', 'hello'],
            'BINARY' => ["\x00\xff", new Bytes("\x00\xff")],
            'LARGE_OBJECT' => ["\x00\xff", new Bytes("\x00\xff")],
        ];
        $out = [];
        foreach (ParameterType::cases() as $case) {
            self::assertArrayHasKey($case->name, $expected, "unmapped ParameterType::{$case->name}");
            $out[$case->name] = [$case, $expected[$case->name][0], $expected[$case->name][1]];
        }
        return $out;
    }

    #[DataProvider('pairs')]
    public function testEveryParameterTypeMapsToACanonicalValue(ParameterType $type, mixed $in, mixed $out): void
    {
        $actual = ParameterBinder::toCanonical($in, $type);
        if (is_object($out)) {
            // An object expectation: `assertEquals` compares class + properties and REFUSES a bare
            // string, which is what makes the "wrapped in Bytes" claim falsifiable.
            self::assertEquals($out, $actual);
            self::assertInstanceOf($out::class, $actual);
            return;
        }
        // IDENTITY, deliberately: see the class docblock — `assertEquals(true, 1)` passes.
        self::assertSame($out, $actual);
    }

    /** Under `STRING`, the PHP type decides — that is what makes floats and ints work at all. */
    public function testStringTypeDispatchesOnThePhpType(): void
    {
        self::assertSame(1.5, ParameterBinder::toCanonical(1.5, ParameterType::STRING));
        self::assertSame(7, ParameterBinder::toCanonical(7, ParameterType::STRING));
        self::assertTrue(ParameterBinder::toCanonical(true, ParameterType::STRING));
        self::assertNull(ParameterBinder::toCanonical(null, ParameterType::STRING));
        self::assertSame('1.2500', ParameterBinder::toCanonical('1.2500', ParameterType::STRING));
    }

    /** NULL survives every type — DBAL binds a null with whatever type the column implies. */
    public function testNullSurvivesEveryParameterType(): void
    {
        foreach (ParameterType::cases() as $case) {
            self::assertNull(ParameterBinder::toCanonical(null, $case), "null under {$case->name}");
        }
    }

    /** A stream (what `BlobType` may hand us) is materialised, not stringified into "Resource id #N". */
    public function testALargeObjectStreamIsMaterialised(): void
    {
        $h = fopen('php://memory', 'r+');
        self::assertNotFalse($h);
        fwrite($h, "\x01\x02\x03");
        rewind($h);
        $out = ParameterBinder::toCanonical($h, ParameterType::LARGE_OBJECT);
        self::assertInstanceOf(Bytes::class, $out);
        self::assertSame("\x01\x02\x03", $out->value);
    }

    /** An object with no canonical shape is a LOUD driver error, never a silent cast. */
    public function testAnUnbindableValueThrows(): void
    {
        $this->expectException(DriverException::class);
        ParameterBinder::toCanonical(new \stdClass(), ParameterType::STRING);
    }

    // -----------------------------------------------------------------------------------------
    // ADDED beyond the plan. Each closes a guard that the plan's five tests leave unobservable.
    // -----------------------------------------------------------------------------------------

    /**
     * THE PAIR, stated as a mirror: the SAME PHP value under two `ParameterType`s must produce two
     * DIFFERENT canonical values. A one-sided row ("`1` under `BOOLEAN` is `true`") is satisfiable
     * by a binder that answers `true` for `1` everywhere; this row is not.
     *
     * `'1'` under `STRING` staying the string `'1'` is not academic — it is a `varchar` column
     * holding the character `1`, and turning it into a boolean would be the silent miscast SPEC
     * §9.1 exists to prevent.
     *
     * @return array<string, array{0: mixed, 1: ParameterType, 2: mixed, 3: ParameterType, 4: mixed}>
     */
    public static function mirrors(): array
    {
        return [
            'int 1: BOOLEAN vs STRING' => [1, ParameterType::BOOLEAN, true, ParameterType::STRING, 1],
            "string '1': BOOLEAN vs STRING" => ['1', ParameterType::BOOLEAN, true, ParameterType::STRING, '1'],
            "string '0': BOOLEAN vs STRING" => ['0', ParameterType::BOOLEAN, false, ParameterType::STRING, '0'],
            "string '42': INTEGER vs STRING" => ['42', ParameterType::INTEGER, 42, ParameterType::STRING, '42'],
            'int 0: BOOLEAN vs INTEGER' => [0, ParameterType::BOOLEAN, false, ParameterType::INTEGER, 0],
        ];
    }

    #[DataProvider('mirrors')]
    public function testOneValueUnderTwoParameterTypesProducesTwoCanonicalValues(
        mixed $value,
        ParameterType $a,
        mixed $expectedA,
        ParameterType $b,
        mixed $expectedB,
    ): void {
        self::assertSame($expectedA, ParameterBinder::toCanonical($value, $a), "under {$a->name}");
        self::assertSame($expectedB, ParameterBinder::toCanonical($value, $b), "under {$b->name}");
    }

    /**
     * The `asInt` overflow refusal, which the plan writes but never reaches. `(int) '9223372036854775808'`
     * SATURATES to `PHP_INT_MAX` in PHP — it does not wrap and it does not warn — so without the
     * round-trip check a `bigint` key one above the range would be written as a DIFFERENT, valid,
     * silently wrong number. The positive mirror (`PHP_INT_MAX` itself) is there so "refuse
     * everything long" cannot pass for the right reason.
     */
    public function testAnIntegerThatDoesNotFitAPhpIntIsRefusedRatherThanSaturated(): void
    {
        self::assertSame(PHP_INT_MAX, ParameterBinder::toCanonical((string) PHP_INT_MAX, ParameterType::INTEGER));
        self::assertSame(PHP_INT_MIN, ParameterBinder::toCanonical((string) PHP_INT_MIN, ParameterType::INTEGER));
        // Proof the hazard is real on this runtime, not merely asserted:
        self::assertSame(PHP_INT_MAX, (int) '9223372036854775808', 'the cast saturates; it does not wrap');

        $this->expectException(DriverException::class);
        $this->expectExceptionMessage('does not fit a PHP int');
        ParameterBinder::toCanonical('9223372036854775808', ParameterType::INTEGER);
    }

    /**
     * The typed refusals, one row per narrowing branch, each with the branch's own message fragment.
     * Without these, `asBool`/`asInt`/`asBinary` could return a silent cast for a value they cannot
     * represent and only the (PG-only) live test would notice.
     *
     * @return array<string, array{0: mixed, 1: ParameterType, 2: string}>
     */
    public static function refusals(): array
    {
        return [
            'a word is not a boolean' => ['yes', ParameterType::BOOLEAN, 'as ParameterType::BOOLEAN'],
            'a float is not a boolean' => [1.0, ParameterType::BOOLEAN, 'as ParameterType::BOOLEAN'],
            'a non-numeric string is not an integer' => ['12a', ParameterType::INTEGER, 'as ParameterType::INTEGER'],
            'a float is not an integer' => [4.0, ParameterType::INTEGER, 'as ParameterType::INTEGER'],
            'an int is not binary' => [7, ParameterType::BINARY, 'expected a string or a stream resource'],
            'an array has no canonical shape' => [[1, 2], ParameterType::STRING, 'cannot bind a value of type array'],
        ];
    }

    #[DataProvider('refusals')]
    public function testAValueTheTypeCannotRepresentIsRefusedLoudly(mixed $value, ParameterType $type, string $fragment): void
    {
        $this->expectException(DriverException::class);
        $this->expectExceptionMessage($fragment);
        ParameterBinder::toCanonical($value, $type);
    }

    /**
     * A `Stringable` (the shape a custom `Type::convertToDatabaseValue()` most often returns) is
     * accepted under `STRING` — and it is the ONE object arm, so the refusal row above and this row
     * together pin the boundary from both sides.
     */
    public function testAStringableIsAcceptedUnderString(): void
    {
        $v = new class implements \Stringable {
            public function __toString(): string
            {
                return '1.2500';
            }
        };
        self::assertSame('1.2500', ParameterBinder::toCanonical($v, ParameterType::STRING));
    }
}
