<?php // /php/laravel/tests/Live/ScalarHydrationLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

/**
 * The tier hands Illuminate **PDO-shaped scalars**, never the SPEC §9 value objects.
 *
 * This is a hard contract, not a preference, and it was MEASURED as a defect before it was a test.
 * The client's default {@see \Ferro\Client\Value\M1ValuePolicy} decodes the S7 tags into value
 * objects ({@see \Ferro\NaiveTimestamp}, {@see \Ferro\Decimal}, {@see \Ferro\Uuid}, …). Illuminate
 * is written against PDO, which hands up a driver-native string for every one of them, and stock
 * framework code puts those values straight into positions where only a scalar works. Both shapes
 * below come from laravel/framework v11.51.0's own `QueryBuilderTest::testPluck`, which failed on
 * this tier with `TypeError: Cannot access offset of type Ferro\NaiveTimestamp on array` before
 * {@see \Ferro\Laravel\FerroConnections} started passing
 * {@see \Ferro\Client\Value\RawStringValuePolicy}.
 *
 * Mutation check for anyone changing this: drop the `$values` argument in `FerroConnections::
 * client()` and every assertion here goes red — the first two on the type, `testPluckKeyedByA
 * TimestampColumn` on the `TypeError` itself.
 */
final class ScalarHydrationLiveTest extends LaravelLiveTestCase
{
    /**
     * The four tags an ordinary schema reaches on the very first query. Each is asserted as a
     * STRING with its canonical text, because "stringifies when you ask it to" is not the same
     * property: `substr()`, an array key and a `===` comparison all need the value to BE the string.
     */
    public function testTemporalAndNumericColumnsHydrateAsStrings(): void
    {
        $conn = $this->connection();

        $row = $conn->select(
            "select timestamp '2017-11-12 13:14:15' as ts,"
            . " date '2017-11-12' as d,"
            . " time '13:14:15' as t,"
            . " numeric '10.50' as num,"
            . " uuid '0a591bd0-5c6f-4a3d-8c3e-6f1c1b7b9a01' as u,"
            . " json '{\"a\":1}' as j",
        )[0];

        self::assertSame('2017-11-12 13:14:15', $row->ts);
        self::assertSame('2017-11-12', $row->d);
        self::assertSame('13:14:15', $row->t);
        self::assertSame('10.50', $row->num, 'the display scale survives — PDO_pgsql keeps it too');
        self::assertSame('0a591bd0-5c6f-4a3d-8c3e-6f1c1b7b9a01', $row->u);
        self::assertSame('{"a":1}', $row->j);
    }

    /**
     * `TIMESTAMPTZ` is the ONE deliberate divergence from PDO_pgsql, and it is deliberate because it
     * is safer: canonical RFC3339 rather than PDO's `Y-m-d H:i:s+00`. `Model::asDateTime()` runs
     * `Date::createFromFormat('Y-m-d H:i:s', $value)` and PHP's format parser IGNORES trailing data,
     * so PDO's offset form parses with the `+00` silently discarded and the instant reinterpreted in
     * the application timezone; the `T` separator here fails that parse and falls through to
     * Illuminate's own `Date::parse()`, which reads the `Z`. Locked so a future "make it look like
     * PDO" edit has to argue with this comment first.
     */
    public function testTimestamptzIsCanonicalRfc3339NotPdosOffsetForm(): void
    {
        $conn = $this->connection();

        $v = $conn->select("select timestamptz '2017-11-12 13:14:15+00' as ts")[0]->ts;

        self::assertSame('2017-11-12T13:14:15Z', $v);
        self::assertSame(
            1510492455,
            \Illuminate\Support\Facades\Date::parse($v)->getTimestamp(),
            "Illuminate's own fallback parser must read this form, and read it as UTC",
        );
    }

    /**
     * The exact upstream shape that caught it: `pluck($column, $key)` uses the key column's value as
     * an ARRAY KEY, which a value object cannot be.
     */
    public function testPluckKeyedByATimestampColumn(): void
    {
        $conn = $this->connection();

        $rows = $conn->select(
            "select 'Foo Post' as title, timestamp '2017-11-12 13:14:15' as created_at"
            . " union all select 'Bar Post', timestamp '2018-01-02 03:04:05' order by created_at",
        );

        $plucked = [];
        foreach ($rows as $r) {
            $plucked[$r->created_at] = $r->title;
        }

        self::assertSame([
            '2017-11-12 13:14:15' => 'Foo Post',
            '2018-01-02 03:04:05' => 'Bar Post',
        ], $plucked);
    }
}
