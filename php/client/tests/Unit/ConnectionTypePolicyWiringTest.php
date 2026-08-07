<?php // /php/client/tests/Unit/ConnectionTypePolicyWiringTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Connection;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Client\Value\M0ValuePolicy;
use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Decimal;
use Ferro\NaiveTimestamp;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * **The wiring guard for `Connection`'s default value policy.**
 *
 * `Ferro::connect(types: …)` → `Connection(types: …)` → the ExecCodec's `ValuePolicy` is a THREE-hop
 * chain whose last hop is one line (`Connection::__construct`'s `$values ?? new M1ValuePolicy(...)`).
 * Every other test in the suite exercises the policies DIRECTLY, so dropping that line leaves the
 * whole suite green while `types:` silently becomes an inert public knob — a configured
 * `decimal: 'string'` would be ignored and every DECIMAL/TIMESTAMP/UUID/U64 read would throw
 * "value tag N is not supported in M0". That is the exact failure mode the slice killed the inert
 * `PoolConfig` knobs to avoid, so it gets an end-to-end assertion through the real decode path:
 * bytes on a fake socket → `Session` → `ExecCodec::decode` → the policy → the PHP value.
 *
 * **Scope, precisely (M1-S7 review, G4).** These tests go RED if the default-policy site reverts to
 * `M0ValuePolicy`, or if `types:` stops reaching it FROM `Connection`'s constructor — they build
 * `new Connection(types: …)` directly, so they never traverse the FIRST hop and an earlier version
 * of this docblock overclaimed by implying they did. Dropping `$types` from `Ferro::connect`'s
 * `self::assemble(...)` call left every test here green. That hop now has two guards of its own:
 * `Ferro::assemble`'s REQUIRED `$types` parameter (a PHPStan level 9 error if the forward is
 * dropped) and the behavioural
 * `tests/Live/TypesLiveTest::testFerroConnectForwardsTheTypePolicyLive`.
 */
final class ConnectionTypePolicyWiringTest extends TestCase
{
    /** Frame one single-cell `Ok` EXEC terminal onto a fake socket and return the Connection. */
    private function connectionReturning(int $tag, mixed $data, ?TypePolicyOptions $types): Connection
    {
        $packer = PackerFactory::forEncode();
        $t = new FakeTransport();
        $body = ExecOk::encode([
            'cols' => [['name' => 'v', 'tag' => $tag]],
            'rows' => [[['tag' => $tag, 'data' => $data]]],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ], $packer);
        $payload = Outcome::ok($body)->encode($packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, strlen($payload));
        $t->feed((new Codec())->encodeFrame($header, $payload));

        return new Connection(
            session: new Session($t, new RequestIdAllocator(0)),
            pool: 'default',
            types: $types,
        );
    }

    /** The DEFAULT (no `types:`) must be the M1 policy's safe object forms — not M0's refusal. */
    public function testTheDefaultConnectionDecodesTheM1CanonicalTags(): void
    {
        $v = $this->connectionReturning(C::TAG_DECIMAL, '1.10', null)->scalar('SELECT 1.10::numeric');
        self::assertInstanceOf(Decimal::class, $v);
        self::assertSame('1.10', (string) $v);

        $ts = $this->connectionReturning(C::TAG_TIMESTAMP, '2026-08-05 13:45:07', null)->scalar('SELECT now()');
        self::assertInstanceOf(NaiveTimestamp::class, $ts);
        self::assertSame('UTC', $ts->getTimezone()->getName());
    }

    /**
     * A CONFIGURED `types:` must reach the decode path. This is the assertion the T6 review found
     * missing: with the M0 default still in place, this throws instead of returning a string.
     */
    public function testAConfiguredTypePolicyReachesTheDecodePath(): void
    {
        $conn = $this->connectionReturning(
            C::TAG_DECIMAL,
            '1.10',
            new TypePolicyOptions(decimal: 'string'),
        );
        $v = $conn->scalar('SELECT 1.10::numeric');
        self::assertIsString($v, '`types: decimal=string` did not reach the ExecCodec value policy');
        self::assertSame('1.10', $v);
    }

    /** …and so must the other three knobs, each observably different from the default. */
    public function testEveryPolicyKnobIsObservableThroughTheConnection(): void
    {
        $uuid = '3f2b8c1a-0000-4fff-8000-abcdefabcdef';
        self::assertSame(
            $uuid,
            $this->connectionReturning(C::TAG_UUID, $uuid, new TypePolicyOptions(uuid: 'string'))
                ->scalar('SELECT id'),
        );
        self::assertSame(
            '18446744073709551615',
            $this->connectionReturning(C::TAG_U64, '18446744073709551615', new TypePolicyOptions(u64Overflow: 'string'))
                ->scalar('SELECT big'),
        );
        $this->expectException(\Ferro\Client\Error\TypePolicyException::class);
        $this->connectionReturning(C::TAG_TIMESTAMP, '2026-08-05 13:45:07', new TypePolicyOptions(naiveDatetimeZone: 'error'))
            ->scalar('SELECT ts');
    }

    /** An explicitly-passed `values:` policy still wins — the S8 DBAL tier depends on that seam. */
    public function testAnExplicitValuePolicyStillOverridesTheDefault(): void
    {
        $packer = PackerFactory::forEncode();
        $t = new FakeTransport();
        $body = ExecOk::encode([
            'cols' => [['name' => 'v', 'tag' => C::TAG_DECIMAL]],
            'rows' => [[['tag' => C::TAG_DECIMAL, 'data' => '1.10']]],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ], $packer);
        $payload = Outcome::ok($body)->encode($packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, strlen($payload));
        $t->feed((new Codec())->encodeFrame($header, $payload));

        $conn = new Connection(
            session: new Session($t, new RequestIdAllocator(0)),
            pool: 'default',
            values: new RawStringValuePolicy(),
        );
        self::assertSame('1.10', $conn->scalar('SELECT 1.10::numeric'));
    }

    /** T6's constructor traps stay shut: `values:` and `types:` may never be passed together. */
    public function testTheMutuallyExclusiveConstructorGuardsAreIntact(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Connection(
            session: new Session(new FakeTransport(), new RequestIdAllocator(0)),
            values: new M0ValuePolicy(),
            types: new TypePolicyOptions(),
        );
    }
}
