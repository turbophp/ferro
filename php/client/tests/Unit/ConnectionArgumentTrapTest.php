<?php // /php/client/tests/Unit/ConnectionArgumentTrapTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Connection;
use Ferro\Client\ExecCodec;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M0ValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Ferro;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * The silent-discard trap: `new Connection(codec: …, values: …)` used to build the ExecCodec from
 * `codec:` and throw `values:` (and `plans:`) on the floor without a word — so an app that
 * configured a §9.1 policy would get M0 decoding and never know. Same class of bug for
 * `values:` + `types:`: a ready-made ValuePolicy already embeds its own options.
 *
 * These are now mutually exclusive, with one exception naming every discarded argument.
 */
final class ConnectionArgumentTrapTest extends TestCase
{
    private static function codec(): ExecCodec
    {
        return new ExecCodec(
            new M0ValuePolicy(),
            new PlanCache(),
            PackerFactory::forEncode(),
            PackerFactory::forDecode(),
        );
    }

    public function testCodecCannotBeCombinedWithValuesOrPlansOrTypes(): void
    {
        $s = new FakeSession();
        $codec = self::codec();

        $cases = [
            ['values' => new M0ValuePolicy()],
            ['plans' => new PlanCache()],
            ['types' => new TypePolicyOptions()],
        ];
        foreach ($cases as $extra) {
            try {
                new Connection(...array_merge(['session' => $s, 'pool' => 'p', 'codec' => $codec], $extra));
                self::fail('codec: plus ' . array_key_first($extra) . ': must be rejected');
            } catch (\InvalidArgumentException $e) {
                self::assertStringContainsString('values', $e->getMessage());
                self::assertStringContainsString('plans', $e->getMessage());
                self::assertStringContainsString('types', $e->getMessage());
            }
        }
    }

    public function testValuesCannotBeCombinedWithTypes(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/values.*types|types.*values/s');
        new Connection(
            session: new FakeSession(),
            pool: 'p',
            values: new M0ValuePolicy(),
            types: new TypePolicyOptions(),
        );
    }

    public function testEachArgumentAloneIsStillAccepted(): void
    {
        $s = new FakeSession();
        self::assertInstanceOf(Connection::class, new Connection(session: $s, pool: 'p', codec: self::codec()));
        self::assertInstanceOf(Connection::class, new Connection(session: $s, pool: 'p', values: new M0ValuePolicy()));
        self::assertInstanceOf(Connection::class, new Connection(session: $s, pool: 'p', plans: new PlanCache()));
        self::assertInstanceOf(Connection::class, new Connection(session: $s, pool: 'p', types: new TypePolicyOptions()));
        self::assertInstanceOf(
            Connection::class,
            new Connection(session: $s, pool: 'p', values: new M0ValuePolicy(), plans: new PlanCache()),
        );
    }

    public function testTypePolicyDefaultsToTheSafeObjectFormsAndIsCarried(): void
    {
        $s = new FakeSession();

        self::assertEquals(new TypePolicyOptions(), (new Connection(session: $s, pool: 'p'))->typePolicy());

        $opts = new TypePolicyOptions(decimal: 'string', naiveDatetimeZone: 'error', u64Overflow: 'string', uuid: 'string');
        self::assertSame($opts, (new Connection(session: $s, pool: 'p', types: $opts))->typePolicy());
    }

    /**
     * The facade must actually expose the knob, or nothing an app can call ever reaches
     * {@see Connection}. Checked by reflection because `Ferro::connect` needs a live socket.
     *
     * **This asserts the SIGNATURE ONLY — it is not the forwarding guard** (M1-S7 review, G4).
     * Dropping `$types` from both `self::assemble(...)` calls in {@see Ferro} left this test, all
     * 469 others, and PHPStan level 9 green while the knob went inert. Forwarding is guarded by
     * `Ferro::assemble`'s now-REQUIRED `$types` parameter (statically) and by
     * `tests/Live/TypesLiveTest::testFerroConnectForwardsTheTypePolicyLive` (behaviourally).
     */
    public function testFerroFacadeThreadsTheTypePolicyThrough(): void
    {
        foreach (['connect', 'connectTcp'] as $method) {
            $params = (new \ReflectionMethod(Ferro::class, $method))->getParameters();
            $byName = [];
            foreach ($params as $p) {
                $byName[$p->getName()] = $p;
            }
            self::assertArrayHasKey('types', $byName, "Ferro::{$method} must accept types:");
            $t = $byName['types']->getType();
            self::assertInstanceOf(\ReflectionNamedType::class, $t);
            self::assertSame(TypePolicyOptions::class, $t->getName());
            self::assertTrue($t->allowsNull(), "Ferro::{$method}(types:) must default to null");
            self::assertTrue($byName['types']->isOptional());
        }
    }
}
