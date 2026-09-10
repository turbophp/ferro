<?php // /php/client/tests/Unit/PoolInfoTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Protocol\CodecException;
use Ferro\Protocol\PoolInfo;
use PHPUnit\Framework\TestCase;

/**
 * `Ferro\Protocol\PoolInfo` — the decoded `HelloAck.pools` element (M1-S8a; fourth field M2-C2g).
 * The BYTE lock against the Rust encoder lives in
 * {@see \Ferro\Tests\Conformance\VectorConformanceTest} (the `hello_ack` vector carries three
 * non-empty entries covering every optional arm); this file covers the decoder's own contract: the
 * positional field ORDER, the `str|nil` version arm, the `bool|nil` quoting-rule arm, and the
 * malformed shapes it must refuse.
 */
final class PoolInfoTest extends TestCase
{
    /**
     * Field ORDER, not just field presence: the fixture's values are pairwise distinct, so a
     * decoder that read them in the wrong order fails here rather than passing on symmetry.
     */
    public function testFromWireDecodesThePositionalEntryInOrder(): void
    {
        $p = PoolInfo::fromWire(['default', 'postgres', 'PostgreSQL 17.10', true]);
        $this->assertSame('default', $p->name);
        $this->assertSame('postgres', $p->kind);
        $this->assertSame('PostgreSQL 17.10', $p->serverVersion);
        $this->assertTrue($p->literalsAreStandard);
    }

    /** The `nil` arm: an engine that has not learned a pool's version sends `null`, not `""`. */
    public function testFromWireAcceptsANullServerVersion(): void
    {
        $p = PoolInfo::fromWire(['reporting', 'mysql', null, false]);
        $this->assertSame('reporting', $p->name);
        $this->assertSame('mysql', $p->kind);
        $this->assertNull($p->serverVersion, 'an unlearned version stays null, never coerced to ""');
        $this->assertFalse($p->literalsAreStandard, 'false is a LEARNED answer, not an unknown one');
    }

    /**
     * The quoting rule's own `nil` arm, and the one a caller must treat as REFUSE. It is separate
     * from the version's nil on purpose: the two are independent, and an engine can know one
     * without the other (a backend family whose arm is unfilled answers a version and no rule).
     */
    public function testFromWireAcceptsANullQuotingRuleIndependentlyOfTheVersion(): void
    {
        $p = PoolInfo::fromWire(['cold', 'mysql', 'MySQL 8.4.11', null]);
        $this->assertSame('MySQL 8.4.11', $p->serverVersion);
        $this->assertNull(
            $p->literalsAreStandard,
            'unknown must stay null — never coerced to false, which would CLAIM backslashes escape',
        );
    }

    /**
     * `toWire` is the exact inverse of `fromWire` — the MIRROR property (a one-sided assertion on a
     * value the test itself constructed could not fail). Both the `Some` and the `None` version arm
     * ride the round trip.
     *
     * @param array{0:string,1:string,2:string|null,3:bool|null} $entry
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('entries')]
    public function testWireRoundTripIsAFixpoint(array $entry): void
    {
        $this->assertSame($entry, PoolInfo::fromWire($entry)->toWire());
    }

    /** @return iterable<string, array{0:array{0:string,1:string,2:string|null,3:bool|null}}> */
    public static function entries(): iterable
    {
        yield 'pg with version' => [['default', 'postgres', 'PostgreSQL 17.10', true]];
        yield 'mysql with version' => [['reporting', 'mysql', '8.4.11', false]];
        yield 'version unlearned' => [['cold', 'mysql', null, true]];
        yield 'quoting rule unlearned' => [['warm', 'postgres', 'PostgreSQL 16.4', null]];
        yield 'neither learned' => [['icy', 'mysql', null, null]];
        yield 'empty version string is NOT null' => [['odd', 'postgres', '', false]];
    }

    /**
     * @param mixed $malformed
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('malformed')]
    public function testFromWireRefusesAMalformedEntry(mixed $malformed, string $expectedMessage): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessage($expectedMessage);
        PoolInfo::fromWire($malformed);
    }

    /** @return iterable<string, array{0:mixed, 1:string}> */
    public static function malformed(): iterable
    {
        yield 'too short' => [['default', 'postgres', null], 'expected a 4-element array'];
        yield 'too long' => [['default', 'postgres', null, true, 'extra'], 'expected a 4-element array'];
        yield 'not an array' => ['default', 'expected a 4-element array'];
        yield 'version is an int' => [['default', 'postgres', 17, true], 'server_version is not str|nil'];
        yield 'version is an array' => [['default', 'postgres', [], true], 'server_version is not str|nil'];
        yield 'name is an int' => [[7, 'postgres', null, true], 'name and kind must both be str'];
        yield 'kind is null' => [['default', null, null, true], 'name and kind must both be str'];
        // The quoting rule is STRICT for the same reason the two required strings are: it gates
        // whether a caller builds a SQL literal at all, so a truthy 1 or "on" must not become true
        // behind its back. Both a truthy and a falsy coercible value are covered, because a decoder
        // using a loose cast would pass a one-sided test on whichever direction it happened to get
        // right.
        yield 'quoting rule is an int' => [['default', 'postgres', null, 1], 'literals_are_standard is not bool|nil'];
        yield 'quoting rule is the string on' => [['default', 'postgres', null, 'on'], 'literals_are_standard is not bool|nil'];
        yield 'quoting rule is a falsy int' => [['default', 'postgres', null, 0], 'literals_are_standard is not bool|nil'];
    }
}
