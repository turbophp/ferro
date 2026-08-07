<?php // /php/client/tests/Unit/PoolInfoTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Protocol\CodecException;
use Ferro\Protocol\PoolInfo;
use PHPUnit\Framework\TestCase;

/**
 * `Ferro\Protocol\PoolInfo` — the decoded `HelloAck.pools` element (M1-S8a). The BYTE lock against
 * the Rust encoder lives in {@see \Ferro\Tests\Conformance\VectorConformanceTest} (the `hello_ack`
 * vector now carries two non-empty triples); this file covers the decoder's own contract: the
 * positional field ORDER, the `str|nil` version arm, and the malformed shapes it must refuse.
 */
final class PoolInfoTest extends TestCase
{
    /**
     * Field ORDER, not just field presence: the fixture's three values are pairwise distinct, so a
     * decoder that read them in the wrong order fails here rather than passing on symmetry.
     */
    public function testFromWireDecodesThePositionalTripleInOrder(): void
    {
        $p = PoolInfo::fromWire(['default', 'postgres', 'PostgreSQL 17.10']);
        $this->assertSame('default', $p->name);
        $this->assertSame('postgres', $p->kind);
        $this->assertSame('PostgreSQL 17.10', $p->serverVersion);
    }

    /** The `nil` arm: an engine that has not learned a pool's version sends `null`, not `""`. */
    public function testFromWireAcceptsANullServerVersion(): void
    {
        $p = PoolInfo::fromWire(['reporting', 'mysql', null]);
        $this->assertSame('reporting', $p->name);
        $this->assertSame('mysql', $p->kind);
        $this->assertNull($p->serverVersion, 'an unlearned version stays null, never coerced to ""');
    }

    /**
     * `toWire` is the exact inverse of `fromWire` — the MIRROR property (a one-sided assertion on a
     * value the test itself constructed could not fail). Both the `Some` and the `None` version arm
     * ride the round trip.
     *
     * @param array{0:string,1:string,2:string|null} $triple
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('triples')]
    public function testWireRoundTripIsAFixpoint(array $triple): void
    {
        $this->assertSame($triple, PoolInfo::fromWire($triple)->toWire());
    }

    /** @return iterable<string, array{0:array{0:string,1:string,2:string|null}}> */
    public static function triples(): iterable
    {
        yield 'pg with version' => [['default', 'postgres', 'PostgreSQL 17.10']];
        yield 'mysql with version' => [['reporting', 'mysql', '8.4.11']];
        yield 'version unlearned' => [['cold', 'mysql', null]];
        yield 'empty version string is NOT null' => [['odd', 'postgres', '']];
    }

    /**
     * @param mixed $malformed
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('malformed')]
    public function testFromWireRefusesAMalformedTriple(mixed $malformed, string $expectedMessage): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessage($expectedMessage);
        PoolInfo::fromWire($malformed);
    }

    /** @return iterable<string, array{0:mixed, 1:string}> */
    public static function malformed(): iterable
    {
        yield 'too short' => [['default', 'postgres'], 'expected a 3-element array'];
        yield 'too long' => [['default', 'postgres', null, 'extra'], 'expected a 3-element array'];
        yield 'not an array' => ['default', 'expected a 3-element array'];
        yield 'version is an int' => [['default', 'postgres', 17], 'server_version is not str|nil'];
        yield 'version is an array' => [['default', 'postgres', []], 'server_version is not str|nil'];
        yield 'name is an int' => [[7, 'postgres', null], 'name and kind must both be str'];
        yield 'kind is null' => [['default', null, null], 'name and kind must both be str'];
    }
}
