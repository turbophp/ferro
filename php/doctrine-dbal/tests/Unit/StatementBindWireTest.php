<?php // /php/doctrine-dbal/tests/Unit/StatementBindWireTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\ParameterType;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\DBAL\Connection;
use Ferro\DBAL\PlatformVersion;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 7, ADDED beyond the plan — the mapping asserted where it is actually CONSUMED: the
 * canonical TAG on the encoded `ExecRequest`.
 *
 * `ParameterBinderTest` proves `toCanonical()` in isolation, and the live PG test proves the whole
 * path against a real database. Between them sits a step nothing offline observes: that
 * `Statement::bindValue()` calls the binder at all. With the binder returning perfect values and
 * `bindValue()` still storing `$value` raw (its walking-skeleton body), every unit test in this
 * package stays green — only the PostgreSQL leg of the live tier notices, and only because PG has a
 * narrow per-tag bind pre-flight that MySQL does not have at all (hazard 32).
 *
 * The tag is also the only place the mapping's CONSEQUENCE is visible without a database: a PHP
 * `int(1)` reaches the engine as `TAG_BOOL` or as `TAG_I64` depending purely on the
 * `ParameterType` it was bound with, and PostgreSQL refuses the second against a `boolean` column
 * pre-send (§19.3 known fate). Asserting the tag is asserting exactly that difference.
 *
 * Idiom per hazards 86/87: `lastRequest()['payload']` (an ASSOCIATIVE array, never a list
 * destructure) and `PurePacker` (never `ExtPacker`, which consumes the whole buffer regardless of
 * the offset it is handed).
 */
final class StatementBindWireTest extends TestCase
{
    /**
     * @param list<array{0: mixed, 1: ParameterType}> $binds
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

        $stmt = $conn->prepare('INSERT INTO t VALUES (' . implode(', ', array_fill(0, max(1, count($binds)), '?')) . ')');
        foreach ($binds as $i => [$value, $type]) {
            $stmt->bindValue($i + 1, $value, $type);
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
     * One row per `(ParameterType, PHP type)` shape DBAL's own type layer produces, asserted as the
     * tag that leaves the process.
     *
     * @return array<string, array{0: mixed, 1: ParameterType, 2: int, 3: mixed}>
     */
    public static function cells(): array
    {
        return [
            // BooleanType::convertToDatabaseValue(true) === int(1), bound BOOLEAN. TAG_I64 here is
            // the measured failure: PG refuses an integer against a `boolean` column pre-send.
            'BooleanType hands us int(1)' => [1, ParameterType::BOOLEAN, C::TAG_BOOL, true],
            'BooleanType hands us int(0)' => [0, ParameterType::BOOLEAN, C::TAG_BOOL, false],
            // The mirror: the same int under STRING is an ordinary integer and must stay one.
            'the same int(1) under STRING' => [1, ParameterType::STRING, C::TAG_I64, 1],
            // BigIntType binds STRING carrying an int; IntegerType binds INTEGER, sometimes carrying
            // a numeric STRING (a PDO-shaped app, or a value that came back out of a result set).
            "IntegerType with a numeric string '42'" => ['42', ParameterType::INTEGER, C::TAG_I64, 42],
            // FloatType binds STRING carrying a float.
            'FloatType hands us a float' => [1.5, ParameterType::STRING, C::TAG_F64, 1.5],
            // DecimalType / DateType / DateTimeType / JsonType / GuidType all bind STRING carrying
            // canonical text. This is the tag Task 4 widened PG's bind pre-flight to accept.
            'DecimalType hands us canonical text' => ['1.2500', ParameterType::STRING, C::TAG_TEXT, '1.2500'],
            'DateType hands us canonical text' => ['2026-08-05', ParameterType::STRING, C::TAG_TEXT, '2026-08-05'],
            'a NULL keeps its own tag' => [null, ParameterType::STRING, C::TAG_NULL, null],
        ];
    }

    #[DataProvider('cells')]
    public function testTheBoundValueReachesTheWireWithTheTagThePairImplies(
        mixed $value,
        ParameterType $type,
        int $expectedTag,
        mixed $expectedData,
    ): void {
        $params = self::sendAndDecodeParams([[$value, $type]]);

        self::assertCount(1, $params);
        self::assertSame($expectedTag, $params[0]['tag'], 'the canonical TAG is what the engine binds on');
        self::assertSame($expectedData, $params[0]['data']);
    }

    /**
     * `BINARY`/`LARGE_OBJECT` must ride the msgpack `bin` family (`TAG_BYTES`). A bare PHP string
     * binds `TAG_TEXT`, whose `str` payload the engine's reader ends in `String::from_utf8` — so a
     * blob sent as a string is rejected as a malformed ExecRequest rather than as a bind error.
     *
     * The decoded payload is a `list<int>` because `SqlValueCodec::fromWire` renders a `bin` cell in
     * its golden-vector shape; asserting the byte values is asserting the bytes that were written.
     */
    public function testABinaryParameterRidesTheBinFamilyWithItsBytesIntact(): void
    {
        foreach ([ParameterType::BINARY, ParameterType::LARGE_OBJECT] as $type) {
            $params = self::sendAndDecodeParams([["\x00\x01\xff", $type]]);
            self::assertSame(C::TAG_BYTES, $params[0]['tag'], "{$type->name} must reach the wire as TAG_BYTES");
            self::assertSame([0, 1, 255], $params[0]['data'], 'the bytes travel verbatim, non-UTF-8 included');
        }
    }

    /**
     * A stream bound as `LARGE_OBJECT` is materialised into the SAME `bin` cell — the shape
     * `BlobType::convertToPHPValue()` produces when a value read back out of a blob column is
     * written straight back.
     */
    public function testAStreamBoundAsALargeObjectReachesTheWireAsItsBytes(): void
    {
        $h = fopen('php://memory', 'r+');
        self::assertNotFalse($h);
        fwrite($h, "\x00\x01\xff");
        rewind($h);

        $params = self::sendAndDecodeParams([[$h, ParameterType::LARGE_OBJECT]]);
        self::assertSame(C::TAG_BYTES, $params[0]['tag']);
        self::assertSame([0, 1, 255], $params[0]['data']);
    }

    /**
     * Parameters reach the engine in POSITIONAL order regardless of the order `bindValue()` was
     * called in — DBAL's `Statement::bindValue()` is index-addressed and an application (or a
     * middleware) may bind out of order. Without `execute()`'s `ksort`, `array_values` would ship
     * the binds in CALL order, silently swapping two same-typed columns: a corrupt write with no
     * error anywhere.
     */
    public function testParametersAreSentInPositionalOrderNotInBindOrder(): void
    {
        $session = (new FakeSession())->thenExecOk(null);
        $conn = new Connection(
            new FerroClientConnection($session, 'default'),
            'default',
            PlatformVersion::KIND_POSTGRES,
            false,
        );

        $stmt = $conn->prepare('INSERT INTO t (a, b, c) VALUES (?, ?, ?)');
        $stmt->bindValue(3, 'third', ParameterType::STRING);
        $stmt->bindValue(1, 'first', ParameterType::STRING);
        $stmt->bindValue(2, 'second', ParameterType::STRING);
        $stmt->execute();

        $off = 0;
        $req = ExecRequest::mapFromWire(
            array_values((array) (new PurePacker())->unpack($session->lastRequest()['payload'], $off)),
        );
        /** @var list<array{tag: int, data: mixed}> $params */
        $params = $req['params'];
        self::assertSame(['first', 'second', 'third'], array_column($params, 'data'));
    }
}
