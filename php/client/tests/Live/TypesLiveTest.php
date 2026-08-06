<?php // /php/client/tests/Live/TypesLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\Error\HydrationException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\TypePolicyException;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Date;
use Ferro\Decimal;
use Ferro\Json;
use Ferro\NaiveTimestamp;
use Ferro\Tests\Support\InvoiceDto;
use Ferro\Tests\Support\StringAmountInvoiceDto;
use Ferro\Time;
use Ferro\Uuid;

/**
 * **M1-S7 Task 9 — the PHP half of the slice acceptance gate.** The FULL path through the real
 * client: PHP value object → `ExecCodec::bindOne` → wire → `ferrod` → Postgres → back → the §9.1
 * policy → a PHP value object, asserted for exact equality with what was bound.
 *
 * The Rust `types_e2e_it.rs` proves the engine's half against all three engines; this file proves
 * the client's half — which is a different code path entirely (the PHP codec, the value objects,
 * the §9.1 knobs and the DTO hydration), and the one a Doctrine/Eloquent tier will stand on.
 *
 * Skips cleanly (via {@see LiveTestCase}) when `FERRO_TEST_PG_URL` or the `ferrod` binary is absent.
 */
final class TypesLiveTest extends LiveTestCase
{
    private const TABLE = 'ferro_s7_php_types';

    private function connection(?TypePolicyOptions $types = null): Connection
    {
        $session = $this->connect();
        $session->hello();
        return new Connection($session, 'default', types: $types);
    }

    /** Recreate the fixture table (persistent — the pool may hand out a different conn per EXEC). */
    private function createTable(Connection $conn): void
    {
        $conn->exec('DROP TABLE IF EXISTS ' . self::TABLE);
        $conn->exec(
            'CREATE TABLE ' . self::TABLE . ' (
               id int8 PRIMARY KEY, c_decimal numeric, c_date date, c_time time,
               c_ts timestamp, c_tstz timestamptz, c_uuid uuid, c_json jsonb, c_text text)',
        );
    }

    /**
     * THE ROUND TRIP: every canonical tag PG can carry, bound from PHP as its §9 value object and
     * read back as an equal one. Nothing here is a literal in SQL — every value crosses the wire in
     * both directions.
     */
    public function testEveryCanonicalTagRoundTripsFromPhpAndBack(): void
    {
        $conn = $this->connection();
        try {
            $this->createTable($conn);

            $decimal = new Decimal('-12345.6700000000'); // trailing zeros are part of the value
            $date = new Date('2026-08-05');
            $time = new Time('13:45:07.250000');
            $naive = NaiveTimestamp::fromCanonicalText('2026-08-05 13:45:07.250000');
            $instant = new \DateTimeImmutable('2026-08-05 13:45:07.250000', new \DateTimeZone('Europe/Berlin'));
            $uuid = new Uuid('a1b2c3d4-0000-4fff-8000-abcdefabcdef');
            $json = new Json('{"a": [1, 2], "n": "héllo"}');

            $conn->exec(
                'INSERT INTO ' . self::TABLE . ' (id, c_decimal, c_date, c_time, c_ts, c_tstz, c_uuid, c_json, c_text)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [1, $decimal, $date, $time, $naive, $instant, $uuid, $json, 'héllo'],
            );

            $row = $conn->queryOne('SELECT * FROM ' . self::TABLE . ' WHERE id = ?', [1]);
            $this->assertIsArray($row);

            // DECIMAL: string-backed and exact — the display scale survives the whole path.
            $this->assertInstanceOf(Decimal::class, $row['c_decimal']);
            $this->assertSame('-12345.6700000000', $row['c_decimal']->value);

            $this->assertInstanceOf(Date::class, $row['c_date']);
            $this->assertSame('2026-08-05', $row['c_date']->value);

            $this->assertInstanceOf(Time::class, $row['c_time']);
            $this->assertSame('13:45:07.250000', $row['c_time']->value);

            // TIMESTAMP is NAIVE: the wall clock comes back unshifted, as a NaiveTimestamp.
            $this->assertInstanceOf(NaiveTimestamp::class, $row['c_ts']);
            $this->assertSame('2026-08-05 13:45:07.250000', $row['c_ts']->format('Y-m-d H:i:s.u'));

            // TIMESTAMPTZ is an INSTANT: bound from a Europe/Berlin object (UTC+2 on this date),
            // it comes back as the same MOMENT, normalized to UTC. A naive/instant swap on either
            // side of the wire would show up here as a two-hour shift and nowhere else.
            $this->assertInstanceOf(\DateTimeImmutable::class, $row['c_tstz']);
            $this->assertNotInstanceOf(NaiveTimestamp::class, $row['c_tstz']);
            $this->assertSame('UTC', $row['c_tstz']->getTimezone()->getName());
            $this->assertSame('2026-08-05 11:45:07.250000', $row['c_tstz']->format('Y-m-d H:i:s.u'));
            $this->assertSame($instant->getTimestamp(), $row['c_tstz']->getTimestamp(), 'same instant');

            $this->assertInstanceOf(Uuid::class, $row['c_uuid']);
            $this->assertSame('a1b2c3d4-0000-4fff-8000-abcdefabcdef', $row['c_uuid']->value);

            // JSON is lazy: the raw document is carried verbatim and decoded only on access.
            $this->assertInstanceOf(Json::class, $row['c_json']);
            $this->assertSame(['a' => [1, 2], 'n' => 'héllo'], $row['c_json']->decoded());

            $this->assertSame('héllo', $row['c_text']);
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * READ → RE-BIND → READ, from PHP. The hydrated objects are written straight back into a second
     * row and re-read; every canonical payload must be byte-identical. This is the shape that
     * catches a naive/instant corruption, because a `NaiveTimestamp` re-bound as an instant would
     * shift by the process timezone — which is deliberately NOT UTC here.
     */
    public function testReadRebindReadIsByteStableUnderANonUtcPhpTimezone(): void
    {
        $previous = date_default_timezone_get();
        date_default_timezone_set('America/New_York'); // never UTC, and it observes DST
        $conn = $this->connection();
        try {
            $this->createTable($conn);
            $conn->exec(
                "INSERT INTO " . self::TABLE . " (id, c_decimal, c_date, c_time, c_ts, c_tstz, c_uuid, c_json, c_text)
                 VALUES (1, '-12345.6700000000', DATE '2026-08-05', TIME '13:45:07.250000',
                         TIMESTAMP '2026-08-05 13:45:07.250000', TIMESTAMPTZ '2026-08-05 13:45:07.25+02',
                         'A1B2C3D4-0000-4FFF-8000-ABCDEFABCDEF', '{\"a\": 1}', 'x')",
            );

            $first = $conn->queryOne('SELECT * FROM ' . self::TABLE . ' WHERE id = 1');
            $this->assertIsArray($first);

            $conn->exec(
                'INSERT INTO ' . self::TABLE . ' (id, c_decimal, c_date, c_time, c_ts, c_tstz, c_uuid, c_json, c_text)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [
                    2, $first['c_decimal'], $first['c_date'], $first['c_time'], $first['c_ts'],
                    $first['c_tstz'], $first['c_uuid'], $first['c_json'], $first['c_text'],
                ],
            );

            $second = $conn->queryOne('SELECT * FROM ' . self::TABLE . ' WHERE id = 2');
            $this->assertIsArray($second);

            $this->assertSame($first['c_decimal']->value, $second['c_decimal']->value);
            $this->assertSame($first['c_date']->value, $second['c_date']->value);
            $this->assertSame($first['c_time']->value, $second['c_time']->value);
            $this->assertSame(
                $first['c_ts']->format('Y-m-d H:i:s.u'),
                $second['c_ts']->format('Y-m-d H:i:s.u'),
                'a naive TIMESTAMP re-bound as an instant would shift by the process timezone',
            );
            $this->assertSame($first['c_tstz']->getTimestamp(), $second['c_tstz']->getTimestamp());
            $this->assertSame($first['c_uuid']->value, $second['c_uuid']->value);
            $this->assertSame($first['c_json']->raw, $second['c_json']->raw);
        } finally {
            $conn->session()->close();
            date_default_timezone_set($previous);
        }
    }

    /** The §9.1 knobs, live: `decimal`/`uuid` = `string` hand back the canonical text, still validated. */
    public function testTypePolicyStringKnobsApplyLive(): void
    {
        $conn = $this->connection(new TypePolicyOptions(decimal: 'string', uuid: 'string'));
        try {
            $row = $conn->queryOne(
                "SELECT '-12345.6700000000'::numeric AS d,
                        'A1B2C3D4-0000-4FFF-8000-ABCDEFABCDEF'::uuid AS u",
            );
            $this->assertIsArray($row);
            $this->assertSame('-12345.6700000000', $row['d']);
            $this->assertSame('a1b2c3d4-0000-4fff-8000-abcdefabcdef', $row['u']);
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * `naive_datetime_zone: error` refuses a naive `TIMESTAMP` — and it is a
     * {@see TypePolicyException} (a configuration refusal), never the `ProtocolException` a wire
     * fault raises. Its scope is `TIMESTAMP` alone: a `TIMESTAMPTZ` still decodes.
     */
    public function testNaiveDatetimeZoneErrorRefusesOnlyTheNaiveColumnLive(): void
    {
        $conn = $this->connection(new TypePolicyOptions(naiveDatetimeZone: 'error'));
        try {
            $ok = $conn->scalar("SELECT TIMESTAMPTZ '2026-08-05 13:45:07.25+02'");
            $this->assertInstanceOf(\DateTimeImmutable::class, $ok, 'the instant column still decodes');

            $this->expectException(TypePolicyException::class);
            $conn->scalar("SELECT TIMESTAMP '2026-08-05 13:45:07.250000'");
        } finally {
            $conn->session()->close();
        }
    }

    /** A sentinel is canonical TEXT, never an invented date — and a `DATE` sentinel keeps its tag. */
    public function testSentinelsSurfaceAsCanonicalTextLive(): void
    {
        $conn = $this->connection();
        try {
            $row = $conn->queryOne(
                "SELECT 'infinity'::date AS d, 'infinity'::timestamp AS t,
                        '-infinity'::timestamptz AS z",
            );
            $this->assertIsArray($row);
            $this->assertInstanceOf(Date::class, $row['d']);
            $this->assertTrue($row['d']->isSentinel());
            $this->assertSame('infinity', $row['d']->value);
            // A TIMESTAMP/TIMESTAMPTZ sentinel is not a wall clock, so the column's PHP type CHANGES
            // between rows (a DateTimeImmutable normally, canonical text here) — SPEC §9.
            $this->assertSame('infinity', $row['t']);
            $this->assertSame('-infinity', $row['z']);
        } finally {
            $conn->session()->close();
        }
    }

    /** The deferred tags stay loud all the way to PHP: a NonRetryable `Unsupported` naming the column. */
    public function testDeferredPgTypesStayLoudThroughTheClient(): void
    {
        $conn = $this->connection();
        try {
            $conn->query("SELECT '1 day'::interval AS c_interval");
            $this->fail('expected a NonRetryableException for a deferred PG type');
        } catch (NonRetryableException $e) {
            $this->assertStringContainsString('c_interval', $e->getMessage());
            $this->assertStringContainsString('interval', $e->getMessage());
        } finally {
            $conn->session()->close();
        }
    }

    /** The DTO path, live: value-object-typed constructor parameters hydrate straight from the wire. */
    public function testDtoWithValueObjectParametersHydratesLive(): void
    {
        $conn = $this->connection();
        try {
            $dto = $conn->queryOne(
                "SELECT 7 AS id, '-12345.6700000000'::numeric AS amount,
                        TIMESTAMPTZ '2026-08-05 13:45:07.25+02' AS at",
                [],
                InvoiceDto::class,
            );
            $this->assertInstanceOf(InvoiceDto::class, $dto);
            $this->assertSame(7, $dto->id);
            $this->assertSame('-12345.6700000000', $dto->amount->value);
            $this->assertSame('2026-08-05 11:45:07.250000', $dto->at->format('Y-m-d H:i:s.u'));

            // A string-typed property still works (the value objects are Stringable and a
            // reflection call is weakly typed) — the canonical text lands verbatim.
            $legacy = $conn->queryOne(
                "SELECT 7 AS id, '-12345.6700000000'::numeric AS amount,
                        TIMESTAMPTZ '2026-08-05 13:45:07.25+02' AS at",
                [],
                StringAmountInvoiceDto::class,
            );
            $this->assertInstanceOf(StringAmountInvoiceDto::class, $legacy);
            $this->assertSame('-12345.6700000000', $legacy->amount);
        } finally {
            $conn->session()->close();
        }
    }

    /**
     * The reachable DTO failure, live: a `TIMESTAMP` **sentinel** hands back canonical text, which a
     * `\DateTimeImmutable`-typed parameter cannot take. It must surface as a
     * {@see HydrationException} inside the `FerroException` contract — not a bare `\TypeError`.
     */
    public function testDtoTypeMismatchIsAHydrationExceptionLive(): void
    {
        $conn = $this->connection();
        try {
            $conn->queryOne(
                "SELECT 7 AS id, '1.00'::numeric AS amount, 'infinity'::timestamp AS at",
                [],
                InvoiceDto::class,
            );
            $this->fail('expected a HydrationException for a sentinel in a \DateTimeImmutable slot');
        } catch (\TypeError $e) {
            $this->fail('a bare \TypeError escaped the FerroException contract: ' . $e->getMessage());
        } catch (HydrationException $e) {
            $this->assertStringContainsString(InvoiceDto::class, $e->getMessage());
            $this->assertStringContainsString('at: string', $e->getMessage());
        } finally {
            $conn->session()->close();
        }
    }
}
