<?php // /php/client/tests/Client/ConnectionBeginIsolationTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\Value\RawStringValuePolicy;
use Ferro\Protocol\BeginRequest;
use Ferro\Protocol\BeginResponse;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Isolation;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 3 — the isolation byte finally travels.
 *
 * The engine half shipped in M1-S8a (`compose_begin_sql(dialect, isolation, readonly)`, unit-tested
 * per cell, with the batched `SET TRANSACTION …; START TRANSACTION …` form on MySQL that does NOT
 * leak onto the pooled connection). The wire field and the `Isolation` enum shipped with it. The
 * only missing link was this parameter — and until it existed, a Doctrine
 * `setTransactionIsolation(SERIALIZABLE)` was a SILENT no-op (SPEC §22.2 (s)).
 *
 * The provider walks EVERY enum case plus the absent case, derived from `Isolation::cases()`, so a
 * fourth case added to the enum makes this test fail rather than silently skipping the new value.
 */
final class ConnectionBeginIsolationTest extends TestCase
{
    /** @return array<string, array{0: ?Isolation, 1: ?int}> */
    public static function levels(): array
    {
        $out = ['pool default (absent)' => [null, null]];
        foreach (Isolation::cases() as $case) {
            $out[$case->name] = [$case, $case->value];
        }
        return $out;
    }

    /** @return array{0: FakeSession, 1: Connection} a session whose BEGIN is answered Ok(tx_id=7) */
    private function wired(): array
    {
        $session = (new FakeSession())->push(
            Outcome::ok(BeginResponse::encode(['tx_id' => 7], PackerFactory::forEncode())),
            [C::SERVICE_TX, C::METHOD_TX_BEGIN],
        );
        return [$session, new Connection($session, 'default')];
    }

    /** @return array{pool:string,isolation:?int,readonly:bool} */
    private function sentBegin(FakeSession $session): array
    {
        $payload = $session->lastRequest()['payload'];
        $off = 0;
        // `array_values` (not a bare cast): `mapFromWire` is declared `array<int,mixed>` and does
        // `array_values($w)` itself, so this is the same normalisation stated where PHPStan can see
        // it — level 9 rejects the `array<mixed,mixed>` an unpack cast produces.
        return BeginRequest::mapFromWire(array_values((array) PackerFactory::forEncode()->unpack($payload, $off)));
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('levels')]
    public function testTheIsolationByteReachesTheBeginRequest(?Isolation $iso, ?int $expected): void
    {
        [$session, $conn] = $this->wired();
        $conn->begin(false, $iso);
        self::assertSame($expected, $this->sentBegin($session)['isolation']);
    }

    /** Appended LAST: the pre-S8b one-argument call site must keep compiling and behaving. */
    public function testTheReadonlyOnlyCallShapeStillWorks(): void
    {
        [$session, $conn] = $this->wired();
        $conn->begin(true);
        $sent = $this->sentBegin($session);
        self::assertNull($sent['isolation'], 'no isolation was asked for');
        self::assertTrue($sent['readonly'], 'readonly still travels');
    }

    /**
     * The mutual exclusion `Ferro::connect(values:)` has to respect. **This pins a PRE-EXISTING
     * invariant** (`Connection::__construct` already rejects the pair at HEAD, `Connection.php:120-127`)
     * — it passes before any Task 3 edit and is here so the facade's new `values:` parameter cannot
     * be wired in a way that bypasses it, not as evidence that the parameter works.
     *
     * **The OBSERVABLE that `values:` is not an inert knob is the LIVE test in Step 6**, and it has
     * to be: `Ferro::assemble()` is private, `connect()` needs a real socket, and a reflection
     * parameter COUNT passes just as well for a `connect()` that accepts `$values` and then drops
     * it — which is exactly what `assemble()`'s own docblock records happening once already with
     * `$types` ("dropping $types from either assemble(...) call left PHPUnit green AND PHPStan
     * level 9 clean while Ferro::connect(types: …) became an inert public knob"). v1 asserted the
     * count here and called it an observable; it is not one, so it is gone.
     */
    public function testConnectionRefusesAValuePolicyAlongsideTypeOptions(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Connection(
            new FakeSession(),
            'default',
            values: new RawStringValuePolicy(),
            types: new \Ferro\Client\Value\TypePolicyOptions(),
        );
    }
}
