<?php // /php/client/tests/Live/HandshakeLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Protocol\PoolInfo;

/**
 * The first end-to-end proof of the S7 client against a real `ferrod`: the HELLO handshake carries
 * a `boot_epoch` and the advertised `pools`, and two connections to the same running instance
 * observe the IDENTICAL epoch (a running instance draws it once, SPEC §19.1) — so we assert
 * equality across connects, never `nonzero` (nondeterministic + opaquely typed).
 *
 * The `pools` assertion proves the HELLO_ACK pool advertising (commit 4d07554, reshaped into
 * per-pool METADATA by M1-S8a Task 11). It is compared against {@see LiveTestCase::launchedPools}
 * and {@see LiveTestCase::launchedPoolKinds} — what THIS run actually configured `ferrod` with —
 * rather than a hard-coded `['default']`, which silently pinned a one-pool harness and broke as soon
 * as M1-S8a added a second. Comparing the engine's advertisement to the harness's own config still
 * fails on a dropped, renamed or invented pool, and now also on a MISLABELLED backend family.
 *
 * `serverVersion` is deliberately asserted as `null` here: M1-S8a Task 11 emits `None`
 * unconditionally (the handshake must not depend on a backend being reachable). Task 12 is what
 * learns the real version, and moving this assertion is part of that task.
 */
final class HandshakeLiveTest extends LiveTestCase
{
    public function testHandshakeReturnsBootEpochAndPools(): void
    {
        $s1 = $this->connect();
        $ack1 = $s1->hello();

        // boot_epoch is present and OPAQUE (int, or a decimal string for a uint64-encoded epoch).
        $this->assertTrue(
            is_int($ack1->bootEpoch) || is_string($ack1->bootEpoch),
            'HELLO_ACK carries a boot_epoch (opaque int|string)',
        );
        $this->assertSame(
            $this->launchedPools(),
            self::names($ack1->pools),
            'HELLO_ACK advertises exactly the pool names this run configured, in order',
        );
        $this->assertSame(
            $this->launchedPoolKinds(),
            self::kinds($ack1->pools),
            'HELLO_ACK advertises each pool\'s backend family, inferred from the DSN scheme',
        );
        $this->assertSame(
            array_fill(0, count($this->launchedPools()), null),
            array_map(static fn (PoolInfo $p): ?string => $p->serverVersion, $ack1->pools),
            'server_version rides as nil in Task 11; Task 12 fills it',
        );
        // The name-only accessor still answers what `ExecRequest.pool` needs.
        $this->assertSame($this->launchedPools(), $s1->pools());
        $this->assertSame($ack1->pools, $s1->poolInfo());

        // A SECOND connection in the same run: the running instance drew its epoch once, so the two
        // must match exactly (same value AND same type — an === comparison of the opaque scalar).
        $s2 = $this->connect();
        $ack2 = $s2->hello();
        $this->assertSame($ack1->bootEpoch, $ack2->bootEpoch, 'one running instance -> one boot_epoch');
        $this->assertSame($this->launchedPools(), self::names($ack2->pools));
        $this->assertSame($this->launchedPoolKinds(), self::kinds($ack2->pools));

        $s1->close();
        $s2->close();
    }

    /**
     * @param list<PoolInfo> $pools
     * @return list<string>
     */
    private static function names(array $pools): array
    {
        return array_map(static fn (PoolInfo $p): string => $p->name, $pools);
    }

    /**
     * @param list<PoolInfo> $pools
     * @return list<string>
     */
    private static function kinds(array $pools): array
    {
        return array_map(static fn (PoolInfo $p): string => $p->kind, $pools);
    }
}
