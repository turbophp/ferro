<?php // /php/client/tests/Live/ValuePolicyFacadeLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\RetryPolicy;
use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\Decimal;
use Ferro\Ferro;

/**
 * M1-S8b Task 3(b) — `Ferro::connect(values:)` is not an inert knob.
 *
 * Asserted through the OBSERVABLE — what a DECIMAL cell decodes to — because the failure mode this
 * guards against is precisely a facade that accepts the argument and drops it on the floor.
 * `Ferro::assemble()`'s own docblock records that exact thing happening once already with `$types`:
 * "dropping $types from either assemble(...) call left PHPUnit green AND PHPStan level 9 clean while
 * Ferro::connect(types: …) became an inert public knob". A reflection parameter COUNT — which is
 * what plan v1 asserted here — passes over that bug.
 *
 * It is a LIVE test because there is no offline route: `assemble()` is private and `connect()` needs
 * a real socket, so the only honest way to observe the wiring is to decode a real cell.
 */
final class ValuePolicyFacadeLiveTest extends LiveTestCase
{
    public function testTheFacadeForwardsAValuePolicyAllTheWayToTheDecoder(): void
    {
        $raw = Ferro::connect(
            $this->socketPath,
            'default',
            2.0,
            5.0,
            RetryPolicy::none(),
            null,
            new RawStringValuePolicy(),
        );
        $got = $raw->scalar("SELECT CAST('1.50' AS numeric)");
        self::assertIsString($got, 'RawStringValuePolicy hands up the canonical wire text verbatim');
        self::assertSame('1.50', $got, 'the display scale survives — 1.50, not 1.5');

        // …and the SAME query on a connection built WITHOUT the argument still gets the §9.1 default,
        // which is what makes the assertion above a discriminator rather than a description of the
        // default behaviour.
        $def = Ferro::connect($this->socketPath, 'default', 2.0, 5.0, RetryPolicy::none());
        $obj = $def->scalar("SELECT CAST('1.50' AS numeric)");
        self::assertInstanceOf(Decimal::class, $obj, 'the default policy still decodes DECIMAL to an object');
        self::assertSame('1.50', (string) $obj);
    }
}
