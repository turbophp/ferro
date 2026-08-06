<?php // /php/client/tests/Unit/TypePolicyOptionsTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Protocol\Generated\Constants as C;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The SPEC §9.1 "policies over guesses" knobs — `decimal`, `naive_datetime_zone`, `u64_overflow`,
 * `uuid` — as a validated value object with the SAFE OBJECT FORMS as defaults.
 *
 * Two properties are load-bearing and asserted here rather than left to the M1 policy (Task 7):
 *   1. `naive_datetime_zone=server` is REJECTED as deferred (nothing on the wire carries the
 *      backend session timezone — `HelloAck` has no pool metadata yet), never silently downgraded.
 *   2. `naive_datetime_zone=error` is scoped to `TAG_TIMESTAMP` ALONE. An undefined scope would
 *      make TIMESTAMPTZ/DATE/TIME columns unreadable with no escape hatch, so the scope lives in
 *      the options object where it is testable against every tag at once.
 */
final class TypePolicyOptionsTest extends TestCase
{
    public function testDefaultsAreTheSafeObjectForms(): void
    {
        $p = new TypePolicyOptions();
        self::assertSame('object', $p->decimal);
        self::assertSame('object', $p->uuid);
        self::assertSame('object', $p->u64Overflow);
        self::assertSame('utc', $p->naiveDatetimeZone);
    }

    public function testDefaultsFactoryMatchesTheBareConstructor(): void
    {
        self::assertEquals(new TypePolicyOptions(), TypePolicyOptions::defaults());
    }

    public function testServerZoneIsRejectedAsDeferred(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/naive_datetime_zone=server .*deferred/i');
        new TypePolicyOptions(naiveDatetimeZone: 'server');
    }

    public function testUnknownPolicyValueIsRejectedLoudly(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new TypePolicyOptions(decimal: 'float');   // never a lossy default
    }

    /** @return list<array{string, string}> knob name → an illegal value for it */
    public static function badKnobProvider(): array
    {
        return [
            ['decimal', 'float'],
            ['decimal', 'error'],          // §9.1 gives `decimal` no error form
            ['naiveDatetimeZone', 'local'],
            ['naiveDatetimeZone', 'UTC'],  // case-sensitive: no silent normalization
            ['u64Overflow', 'float'],
            ['uuid', 'bytes'],
            ['uuid', ''],
        ];
    }

    #[DataProvider('badKnobProvider')]
    public function testEveryKnobRejectsAnUnknownValueNamingItself(string $knob, string $bad): void
    {
        try {
            new TypePolicyOptions(...[$knob => $bad]);
            self::fail("{$knob}={$bad} must be rejected");
        } catch (\InvalidArgumentException $e) {
            self::assertStringContainsString($bad === '' ? "''" : $bad, $e->getMessage());
        }
    }

    public function testEveryLegalFormIsAccepted(): void
    {
        foreach (['object', 'string'] as $d) {
            foreach (['utc', 'error'] as $z) {
                foreach (['object', 'string', 'error'] as $u) {
                    foreach (['object', 'string'] as $x) {
                        $p = new TypePolicyOptions(decimal: $d, naiveDatetimeZone: $z, u64Overflow: $u, uuid: $x);
                        self::assertSame([$d, $z, $u, $x], [$p->decimal, $p->naiveDatetimeZone, $p->u64Overflow, $p->uuid]);
                    }
                }
            }
        }
    }

    // ---- the pinned `naive_datetime_zone=error` scope --------------------------------------------

    /**
     * Every canonical tag EXCEPT `TAG_TIMESTAMP`, DERIVED from the generated registry constants
     * rather than hand-listed.
     *
     * A hand-written list is an unfalsifiable guard: it is complete the day it is written, and a
     * future `TAG_18` added to `/proto/types.toml` would silently escape the scope proof below —
     * the tests would stay green while an entire new tag's behaviour under
     * `naive_datetime_zone=error` went unasserted. Reflecting over `Constants` means a new tag joins
     * this set the moment `gen-php.php` regenerates.
     *
     * @return array<string,int> tag NAME (without the `TAG_` prefix) → tag value
     */
    private static function everyOtherTag(): array
    {
        $out = [];
        /** @var array<string,mixed> $constants */
        $constants = (new \ReflectionClass(C::class))->getConstants();
        foreach ($constants as $name => $value) {
            if (!str_starts_with($name, 'TAG_') || !is_int($value) || $value === C::TAG_TIMESTAMP) {
                continue;
            }
            $out[substr($name, 4)] = $value;
        }
        self::assertNotEmpty($out, 'no TAG_* constants found — the registry generator changed shape');
        return $out;
    }

    public function testErrorZoneRefusesTheNaiveTimestampTag(): void
    {
        self::assertTrue((new TypePolicyOptions(naiveDatetimeZone: 'error'))->refusesNaiveTimestamp(C::TAG_TIMESTAMP));
    }

    public function testErrorZoneLeavesEveryOtherTagDecodable(): void
    {
        $p = new TypePolicyOptions(naiveDatetimeZone: 'error');
        foreach (self::everyOtherTag() as $name => $tag) {
            self::assertFalse(
                $p->refusesNaiveTimestamp($tag),
                "{$name} must decode normally under naive_datetime_zone=error (the refusal is scoped to TIMESTAMP alone)",
            );
        }
    }

    public function testUtcZoneRefusesNothingAtAll(): void
    {
        $p = new TypePolicyOptions();   // the default
        self::assertFalse($p->refusesNaiveTimestamp(C::TAG_TIMESTAMP));
        foreach (self::everyOtherTag() as $name => $tag) {
            self::assertFalse($p->refusesNaiveTimestamp($tag), "{$name} under naive_datetime_zone=utc");
        }
    }

    // ---- `u64_overflow` -------------------------------------------------------------------------

    public function testOnlyTheErrorFormRefusesAU64Overflow(): void
    {
        self::assertTrue((new TypePolicyOptions(u64Overflow: 'error'))->refusesU64Overflow());
        self::assertFalse((new TypePolicyOptions(u64Overflow: 'object'))->refusesU64Overflow());
        self::assertFalse((new TypePolicyOptions(u64Overflow: 'string'))->refusesU64Overflow());
    }
}
