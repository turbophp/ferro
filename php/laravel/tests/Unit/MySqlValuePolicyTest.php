<?php // /php/laravel/tests/Unit/MySqlValuePolicyTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Unit;

use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\Laravel\Value\MySqlValuePolicy;
use Ferro\Protocol\Generated\Constants as C;
use PHPUnit\Framework\TestCase;

/**
 * M2-C1f: on a MySQL pool a `TIMESTAMPTZ` comes back as the naive UTC wall clock — the string
 * `pdo_mysql` returns from a `+00:00` session — and NOTHING else differs from the raw policy.
 */
final class MySqlValuePolicyTest extends TestCase
{
    public function testATimestampTzBecomesTheNaiveUtcWallClock(): void
    {
        $p = new MySqlValuePolicy();
        self::assertSame('2017-11-12 13:14:15', $p->decode(C::TAG_TIMESTAMPTZ, '2017-11-12T13:14:15Z'));
        self::assertSame('2017-11-12 13:14:15.250000', $p->decode(C::TAG_TIMESTAMPTZ, '2017-11-12T13:14:15.250000Z'));
        self::assertSame('0001-01-01 00:00:00', $p->decode(C::TAG_TIMESTAMPTZ, '0001-01-01T00:00:00Z'));
    }

    /** The MySQL zero sentinel is already naive; anything that is not canonical RFC3339 passes through. */
    public function testSentinelsAndNonCanonicalTextPassThrough(): void
    {
        $p = new MySqlValuePolicy();
        self::assertSame('0000-00-00 00:00:00', $p->decode(C::TAG_TIMESTAMPTZ, '0000-00-00 00:00:00'));
        self::assertSame('2017-11-12T13:14:15+02:00', $p->decode(C::TAG_TIMESTAMPTZ, '2017-11-12T13:14:15+02:00'));
    }

    /** Every OTHER tag is the raw policy's answer, byte for byte — the CONTROL for the one change. */
    public function testEveryOtherTagIsTheRawPolicysAnswer(): void
    {
        $p = new MySqlValuePolicy();
        $raw = new RawStringValuePolicy();
        $cases = [
            [C::TAG_TIMESTAMP, '2017-11-12 13:14:15'],
            [C::TAG_DATE, '2017-11-12'],
            [C::TAG_TIME, '13:14:15'],
            [C::TAG_TEXT, '2017-11-12T13:14:15Z'],
            [C::TAG_DECIMAL, '-12.3400'],
            [C::TAG_I64, 42],
            [C::TAG_NULL, null],
        ];
        foreach ($cases as [$tag, $data]) {
            self::assertSame($raw->decode($tag, $data), $p->decode($tag, $data), "tag $tag");
        }
    }
}
