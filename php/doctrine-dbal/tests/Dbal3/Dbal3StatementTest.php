<?php // /php/doctrine-dbal/tests/Dbal3/Dbal3StatementTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Dbal3;

use Doctrine\DBAL\Driver\Exception as DriverExceptionInterface;
use Doctrine\DBAL\ParameterType;
use Ferro\Bytes;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\DBAL\Dbal3\Connection;
use Ferro\DBAL\Dbal3\ParameterBinder;
use Ferro\DBAL\BindKind;
use Ferro\DBAL\PlatformVersion;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M2-C5 — the DBAL 3 statement and binder, asserted where the mapping is CONSUMED: the canonical
 * TAG on the encoded `ExecRequest` (the DBAL 4 lane's `StatementBindWireTest` shape). DBAL 3's
 * `ParameterType` is int constants, so these are the cells where its binder could diverge from
 * DBAL 4's while every value-level test stayed green.
 */
final class Dbal3StatementTest extends TestCase
{
    /**
     * @param callable(\Doctrine\DBAL\Driver\Statement): void $bind
     * @return list<array{tag: int, data: mixed}>
     */
    private static function sent(callable $bind): array
    {
        $session = (new FakeSession())->thenStreamEnd();
        $conn = new Connection(new FerroClientConnection($session, 'default'), 'default', PlatformVersion::KIND_POSTGRES, false);
        $stmt = $conn->prepare('INSERT INTO t VALUES (?, ?)');
        $bind($stmt);
        $off = 0;
        $req = ExecRequest::mapFromWire(
            array_values((array) (new PurePacker())->unpack($session->lastRequest()['payload'], $off)),
        );
        /** @var list<array{tag: int, data: mixed}> $params */
        $params = $req['params'];
        return $params;
    }

    /** The same `(ParameterType, PHP type)` pairs DBAL 3's type layer produces as DBAL 4's does. */
    public function testDbalTypeLayerPairsReachTheWireAsTheRightTag(): void
    {
        $p = self::sent(static function ($s): void {
            $s->bindValue(1, 1, ParameterType::BOOLEAN);     // BooleanType → int(1) tagged BOOLEAN
            $s->bindValue(2, '7', ParameterType::INTEGER);   // a numeric string tagged INTEGER
            $s->execute();
        });
        self::assertSame(['tag' => C::TAG_BOOL, 'data' => true], $p[0]);
        self::assertSame(['tag' => C::TAG_I64, 'data' => 7], $p[1]);

        $p = self::sent(static function ($s): void {
            $s->bindValue(1, "\x00\xff", ParameterType::LARGE_OBJECT);
            $s->bindValue(2, 1.5, ParameterType::STRING);     // FloatType tags STRING carrying a float
            $s->execute();
        });
        self::assertSame(C::TAG_BYTES, $p[0]['tag']);
        self::assertSame(C::TAG_F64, $p[1]['tag']);
    }

    /** `bindParam` binds by REFERENCE: the value at execute() is what is sent. */
    public function testBindParamReadsTheVariableAtExecute(): void
    {
        $p = self::sent(static function ($s): void {
            $a = 1;
            $b = 'x';
            $s->bindParam(1, $a, ParameterType::INTEGER);
            $s->bindParam(2, $b);
            $a = 2;
            $b = 'y';
            $s->execute();
        });
        self::assertSame(['tag' => C::TAG_I64, 'data' => 2], $p[0]);
        self::assertSame(['tag' => C::TAG_TEXT, 'data' => 'y'], $p[1]);
    }

    /** A later `bindValue` on the same position replaces a `bindParam`, and the reverse. */
    public function testTheLastBindingOfAPositionWins(): void
    {
        $p = self::sent(static function ($s): void {
            $v = 'ref';
            $s->bindParam(1, $v);
            $s->bindValue(1, 'value');
            $s->bindValue(2, 'value');
            $s->bindParam(2, $v);
            $s->execute();
        });
        self::assertSame('value', $p[0]['data']);
        self::assertSame('ref', $p[1]['data']);
    }

    /**
     * `execute($params)` is PDO-style: 0-based, every value bound as STRING (so the PHP type
     * decides), and it REPLACES the earlier bindings rather than merging with them.
     */
    public function testExecuteWithParamsIsPdoStyle(): void
    {
        $p = self::sent(static function ($s): void {
            $s->bindValue(1, 'discarded');
            $s->execute([5, 'five']);
        });
        self::assertSame([['tag' => C::TAG_I64, 'data' => 5], ['tag' => C::TAG_TEXT, 'data' => 'five']], $p);
    }

    public function testNamedParametersAreRefused(): void
    {
        $conn = new Connection(new FerroClientConnection(new FakeSession(), 'default'), 'default', PlatformVersion::KIND_POSTGRES, false);
        $this->expectException(DriverExceptionInterface::class);
        $this->expectExceptionMessage('named parameters are not supported');
        $conn->prepare('SELECT :a')->bindValue(':a', 1);
    }

    /**
     * EVERY `ParameterType` constant DBAL 3 defines maps — derived by reflection, so a constant
     * added in a later 3.x release fails here rather than reaching the refusing `default` arm in
     * production. The expected kinds are DBAL 4's mapping, case for case.
     */
    public function testEveryDbal3ParameterTypeMapsLikeDbal4s(): void
    {
        $expected = [
            'NULL' => BindKind::Null,
            'INTEGER' => BindKind::Integer,
            'STRING' => BindKind::Natural,
            'LARGE_OBJECT' => BindKind::Binary,
            'BOOLEAN' => BindKind::Boolean,
            'BINARY' => BindKind::Binary,
            'ASCII' => BindKind::Natural,
        ];
        $constants = (new \ReflectionClass(ParameterType::class))->getConstants();
        self::assertSame(array_keys($expected), array_keys($constants), 'DBAL 3 changed its ParameterType set');
        foreach ($constants as $name => $value) {
            self::assertSame($expected[$name], ParameterBinder::kindOf($value), $name);
        }
    }

    public function testAnUnknownTypeIsRefusedNotFunnelledIntoTheStringPath(): void
    {
        $this->expectException(DriverExceptionInterface::class);
        $this->expectExceptionMessage('unknown DBAL 3 ParameterType 99');
        ParameterBinder::toCanonical('x', 99);
    }

    public function testNullIsNullUnderEveryType(): void
    {
        foreach ((new \ReflectionClass(ParameterType::class))->getConstants() as $value) {
            self::assertNull(ParameterBinder::toCanonical(null, $value));
        }
        self::assertInstanceOf(Bytes::class, ParameterBinder::toCanonical('b', ParameterType::BINARY));
    }

    /**
     * `execute($params)` REPLACES every earlier binding, as PDO's does (M2-C5 review F9a: the test
     * above could not tell replacing from merging, because it bound the same position it passed).
     */
    public function testExecuteWithParamsDropsEveryEarlierBinding(): void
    {
        $p = self::sent(static function ($s): void {
            $s->bindValue(2, 'bound at position two');
            $s->execute(['only']);
        });
        self::assertSame([['tag' => C::TAG_TEXT, 'data' => 'only']], $p);
    }

    /**
     * Parameters reach the wire in POSITION order, whatever order they were bound in, and whichever
     * of `bindValue`/`bindParam` bound them (F9b: nothing pinned the sort).
     */
    public function testParametersAreSentInPositionOrder(): void
    {
        // Two shapes that leave the merged array OUT of position order, so only the sort can put
        // them right. (A first draft bound position 2 by reference and 1 by value — which merges
        // into an already-sorted array, and the no-sort mutation survived it.)
        $p = self::sent(static function ($s): void {
            $s->bindValue(2, 'two');
            $s->bindValue(1, 'one');
            $s->execute();
        });
        self::assertSame(['one', 'two'], array_column($p, 'data'), 'bindValue out of order');

        $p = self::sent(static function ($s): void {
            $first = 'one';
            $s->bindValue(2, 'two');
            $s->bindParam(1, $first);
            $s->execute();
        });
        self::assertSame(['one', 'two'], array_column($p, 'data'), 'a reference merged after a later value');
    }

    /** `ParameterType::NULL` binds NULL whatever it carries, as on DBAL 4. */
    public function testTheNullTypeBindsNullWhateverItCarries(): void
    {
        self::assertNull(ParameterBinder::toCanonical(5, ParameterType::NULL));
    }
}
