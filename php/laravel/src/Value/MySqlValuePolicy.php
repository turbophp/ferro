<?php // /php/laravel/src/Value/MySqlValuePolicy.php
declare(strict_types=1);
namespace Ferro\Laravel\Value;

use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\Client\Value\ValuePolicy;
use Ferro\Protocol\Generated\Constants as C;

/**
 * The Eloquent tier's value hand-off on a MySQL / MariaDB pool (M2-C1f): {@see RawStringValuePolicy}
 * — PDO-shaped driver-native strings — with ONE family-specific difference, for `TIMESTAMPTZ`.
 *
 * A MySQL `TIMESTAMP` column is what `$table->timestamp()` and `$table->timestamps()` create, so
 * this is the type of every Eloquent `created_at`/`updated_at` on this family. Ferro reads it as
 * `TIMESTAMPTZ`, whose canonical wire text is RFC3339 UTC (`2017-11-12T13:14:15Z`). `pdo_mysql`
 * hands it up as a NAIVE wall clock in the SESSION's timezone (`2017-11-12 13:14:15`) — and every
 * Ferro MySQL session is pinned to `+00:00` (M1-S7). So the PDO-identical string is the UTC wall
 * clock with the `T` and the `Z` removed, and that is what this returns.
 *
 * **It is the opposite call from the PostgreSQL tier's, and for a reason that is about the WRITE
 * path, not taste.** On PostgreSQL the tier keeps RFC3339 because `pdo_pgsql`'s own `timestamptz`
 * string carries an offset (SPEC §22.2 (al)). On MySQL nothing carries one: Eloquent WRITES a naive
 * `Y-m-d H:i:s` string, which the server reads in the (UTC) session, so the read must come back
 * naive in that same session for a write → read round trip to be byte-stable. Handing up `…Z`
 * instead makes Illuminate parse the value as a UTC INSTANT while it wrote a wall clock in the
 * application timezone — a silent shift of the UTC offset for any app not running in UTC. Measured
 * in upstream's own suite: `QueryBuilderTest::testPluck` and
 * `EloquentBelongsToManyTest::testCustomPivotClassUpdatesTimestamps` compare the raw string and
 * failed on the `T` (§22.2 (cb)).
 *
 * The MySQL zero sentinel `0000-00-00 00:00:00` is already naive and passes through, as does every
 * other tag. Precision is kept: a fraction the wire carries is kept.
 */
final class MySqlValuePolicy implements ValuePolicy
{
    private readonly RawStringValuePolicy $raw;

    public function __construct()
    {
        $this->raw = new RawStringValuePolicy();
    }

    public function decode(int $tag, mixed $data): mixed
    {
        $value = $this->raw->decode($tag, $data);
        if ($tag === C::TAG_TIMESTAMPTZ
            && is_string($value)
            && preg_match('/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)Z$/', $value, $m) === 1
        ) {
            return $m[1] . ' ' . $m[2];
        }
        return $value;
    }
}
