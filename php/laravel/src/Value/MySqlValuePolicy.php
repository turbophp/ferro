<?php // /php/laravel/src/Value/MySqlValuePolicy.php
declare(strict_types=1);
namespace Ferro\Laravel\Value;

use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Value\CanonicalText;
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
 * other tag. Precision is kept: a fraction the wire carries is kept — **but it is not `pdo_mysql`'s
 * rendering of it.** The canonical wire text has no fraction when the sub-second part is zero and
 * exactly six digits otherwise (PROTOCOL.md §3.2), while `pdo_mysql` renders the column's own
 * precision: a `TIMESTAMP(3)` holding `.25` is `.250` there and `.250000` here, and one holding a
 * whole second is `.000` there and nothing here. The INSTANT is identical and Illuminate's date
 * casts parse both; a consumer comparing the raw STRING of a fractional column (`pluck()` keys,
 * `getRawOriginal()`) sees the difference. The column's precision is not on the wire for any tag,
 * so matching it would need a protocol change; the divergence is pinned by a live test instead
 * (SPEC §22.2 (cb)).
 *
 * **It fails CLOSED.** A `TIMESTAMPTZ` payload that is neither a canonical instant nor a canonical
 * sentinel is a wire fault, and is raised as one (the client's own validator, the same one the
 * typed policy uses) rather than handed up unconverted: the unconverted RFC3339 text is precisely
 * the shape the paragraph above shows Illuminate shifting silently.
 */
final class MySqlValuePolicy implements ValuePolicy
{
    private readonly RawStringValuePolicy $raw;

    public function __construct()
    {
        $this->raw = new RawStringValuePolicy();
    }

    /** @throws ProtocolException a `TIMESTAMPTZ` payload that is not canonical */
    public function decode(int $tag, mixed $data): mixed
    {
        $value = $this->raw->decode($tag, $data);
        if ($tag !== C::TAG_TIMESTAMPTZ || !is_string($value)) {
            return $value;
        }
        CanonicalText::timestamptz($value);   // throws ProtocolException on a non-canonical payload
        if (!CanonicalText::timestamptzIsInstant($value)) {
            return $value;                     // a sentinel: carried verbatim, never parsed
        }
        // Validated above, so the instant form is exactly `YYYY-MM-DDTHH:MM:SS[.ffffff]Z`.
        return str_replace('T', ' ', substr($value, 0, -1));
    }
}
