<?php // /php/doctrine-dbal/tests/Unit/CanonicalValueObjectBindTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\ParameterType;
use Ferro\Bytes;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\Date;
use Ferro\DBAL\Connection;
use Ferro\DBAL\Exception\DriverException;
use Ferro\DBAL\ParameterBinder;
use Ferro\DBAL\PlatformVersion;
use Ferro\Decimal;
use Ferro\Json;
use Ferro\NaiveTimestamp;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Tests\Support\FakeSession;
use Ferro\Time;
use Ferro\U64;
use Ferro\Uuid;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The whole-branch review's cross-slice MAJOR: `ParameterBinder::natural()` SILENTLY downgraded every
 * SPEC §9 canonical value object to a bare string.
 *
 * Six of the eight implement `\Stringable`, DBAL binds every untyped parameter
 * `ParameterType::STRING`, and the `\Stringable` arm did `(string) $v` — so the canonical TAG was
 * dropped and the value travelled as `TAG_TEXT`. That matters because the S7 pre-send guarantee is
 * TAG-KEYED: `ferro-backend-mysql` refuses `Value::Decimal("NaN")` / `Value::Time("839:00:00")`
 * before they are sent, while `Value::Text` has NO pre-flight — and under a permissive `sql_mode`
 * the server coerces. MEASURED live through this tier before the fix: `NaN` stored `0.00`,
 * `839:00:00` stored `838:59:59`. On PostgreSQL it made the engine's own sentinel-gate advice
 * ("send it with its own canonical tag instead") impossible to follow through the driver.
 *
 * **The vantage point is the ENCODED `ExecRequest`**, not the binder's return value, and that is
 * deliberate: `ParameterBinder::toCanonical()` returning the object is only half the fix — the tag is
 * decided later, by `ExecCodec::bindOne()`. Asserting the tag that LEAVES THE PROCESS is what makes
 * this test unable to pass for the wrong reason (the same idiom as `StatementBindWireTest`, and the
 * same hazards: `lastRequest()['payload']` associative, `PurePacker` never `ExtPacker`).
 *
 * The MIRRORS below are what keep the new arm NARROW: an ordinary application `\Stringable` must
 * still stringify (that is the documented custom-`Type` shape), and a raw `\DateTimeImmutable` must
 * still be refused — `NaiveTimestamp` EXTENDS it, and letting the parent through here would bind
 * every naive value as an instant.
 */
final class CanonicalValueObjectBindTest extends TestCase
{
    /**
     * @param list<mixed> $binds
     * @return list<array{tag: int, data: mixed}>
     */
    private static function sendAndDecodeParams(array $binds): array
    {
        $session = (new FakeSession())->thenExecOk(null);
        $conn = new Connection(
            new FerroClientConnection($session, 'default'),
            'default',
            PlatformVersion::KIND_POSTGRES,
            false,
        );

        $stmt = $conn->prepare('INSERT INTO t VALUES (' . implode(', ', array_fill(0, count($binds), '?')) . ')');
        foreach ($binds as $i => $value) {
            // The SPI DEFAULT type, which is what `executeStatement($sql, [$v])` with no $types uses
            // and what every stock DBAL Type that is not integer/boolean/binary ends up with.
            $stmt->bindValue($i + 1, $value, ParameterType::STRING);
        }
        $stmt->execute();

        $off = 0;
        $req = ExecRequest::mapFromWire(
            array_values((array) (new PurePacker())->unpack($session->lastRequest()['payload'], $off)),
        );
        /** @var list<array{tag: int, data: mixed}> $params */
        $params = $req['params'];
        return $params;
    }

    /**
     * One row per canonical value object: the object bound under `ParameterType::STRING`, the tag it
     * MUST carry on the wire, and the canonical payload it must carry verbatim.
     *
     * The values are chosen so the downgrade is not merely detectable but DANGEROUS: `NaN` and
     * `839:00:00` are exactly the two the review measured being silently coerced by MySQL, and
     * `infinity` is the PostgreSQL sentinel whose refusal message names these classes.
     *
     * @return array<string, array{0: mixed, 1: int, 2: mixed}>
     */
    public static function canonicalCells(): array
    {
        return [
            'Decimal — the MySQL silent-coercion case (stored 0.00 before the fix)' =>
                [new Decimal('NaN'), C::TAG_DECIMAL, 'NaN'],
            'Decimal — display scale, which TEXT would also carry but untyped' =>
                [new Decimal('1.10'), C::TAG_DECIMAL, '1.10'],
            'Date — the PG sentinel the gate tells you to send this way' =>
                [new Date('infinity'), C::TAG_DATE, 'infinity'],
            'Time — MySQL clamped this to 838:59:59 before the fix' =>
                [new Time('839:00:00'), C::TAG_TIME, '839:00:00'],
            'Uuid' =>
                [new Uuid('0f8fad5b-d9cb-469f-a165-70867728950e'), C::TAG_UUID, '0f8fad5b-d9cb-469f-a165-70867728950e'],
            'Json' =>
                [new Json('{"a":1}'), C::TAG_JSON, '{"a":1}'],
            'U64 above PHP_INT_MAX — a cast would saturate' =>
                [new U64('18446744073709551615'), C::TAG_U64, '18446744073709551615'],
            'NaiveTimestamp — a wall clock, never an instant' =>
                [new NaiveTimestamp('2026-08-05 13:45:07', new \DateTimeZone('UTC')), C::TAG_TIMESTAMP, '2026-08-05 13:45:07'],
            'Bytes — the only way to reach TAG_BYTES from PHP' =>
                [new Bytes("\x00\xffraw"), C::TAG_BYTES, "\x00\xffraw"],
        ];
    }

    #[DataProvider('canonicalCells')]
    public function testACanonicalValueObjectKeepsItsTagThroughTheDbalTier(mixed $value, int $tag, mixed $data): void
    {
        $params = self::sendAndDecodeParams([$value]);

        self::assertSame(
            $tag,
            $params[0]['tag'],
            'the canonical TAG must reach the wire — TAG_TEXT here means the value object was '
            . 'stringified and the S7 tag-keyed pre-send refusal has been routed around',
        );
        self::assertSame($data, self::payloadOf($params[0]), 'and its canonical payload, byte for byte');
    }

    /**
     * The decoded payload, with the ONE representation difference `SqlValueCodec::fromWire()`
     * deliberately introduces: a `TAG_BYTES` cell comes back as a `list<int>` of bytes (so a golden
     * vector can carry arbitrary bytes through JSON), while every other tag comes back as its text
     * or scalar. Re-joining it here keeps the assertion on the BYTES rather than on that encoding.
     *
     * @param array{tag: int, data: mixed} $param
     */
    private static function payloadOf(array $param): mixed
    {
        if ($param['tag'] !== C::TAG_BYTES || !is_array($param['data'])) {
            return $param['data'];
        }
        $out = '';
        foreach ($param['data'] as $b) {
            $out .= chr((int) $b & 0xff);
        }
        return $out;
    }

    /**
     * **MIRROR 1 — the arm stays narrow.** An application `\Stringable` (the shape a custom
     * `Type::convertToDatabaseValue()` returns) must still stringify into `TAG_TEXT`. A fix written
     * as "pass every object through" would go red here — `ExecCodec::bindOne()` has no arm for a
     * foreign class and would raise a `ProtocolException` instead of binding anything.
     */
    public function testAnOrdinaryStringableStillTravelsAsCanonicalText(): void
    {
        $v = new class implements \Stringable {
            public function __toString(): string
            {
                return '1.2500';
            }
        };

        $params = self::sendAndDecodeParams([$v]);

        self::assertSame(C::TAG_TEXT, $params[0]['tag']);
        self::assertSame('1.2500', $params[0]['data']);
    }

    /**
     * **MIRROR 2 — the parent class is NOT on the list.** `NaiveTimestamp extends
     * \DateTimeImmutable`; admitting `\DateTimeInterface` here would make every naive wall clock
     * arrive as an instant. Stock DBAL stringifies dates before the driver sees them, so a raw one
     * means the type layer was bypassed and the refusal names that.
     */
    public function testARawDateTimeIsStillRefusedWithTheTypeLayerMessage(): void
    {
        $this->expectException(DriverException::class);
        $this->expectExceptionMessage('Doctrine\'s type layer converts values');
        ParameterBinder::toCanonical(new \DateTimeImmutable('2026-08-05 13:45:07'), ParameterType::STRING);
    }

    /**
     * The binder's own return value, for the two classes that are NOT `\Stringable` and therefore
     * used to THROW here rather than downgrade. They are on the list so the rule is "the canonical
     * value objects survive" and not "the ones that happened to be Stringable survive".
     */
    public function testTheTwoNonStringableValueObjectsAreNoLongerRefused(): void
    {
        $naive = new NaiveTimestamp('2026-08-05 13:45:07', new \DateTimeZone('UTC'));
        $bytes = new Bytes("\x00\xff");

        self::assertSame($naive, ParameterBinder::toCanonical($naive, ParameterType::STRING));
        self::assertSame($bytes, ParameterBinder::toCanonical($bytes, ParameterType::STRING));
    }
}
