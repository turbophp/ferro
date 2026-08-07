<?php // /php/client/tests/Live/HandshakeLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

/**
 * The first end-to-end proof of the S7 client against a real `ferrod`: the HELLO handshake carries
 * a `boot_epoch` and the advertised `pools`, and two connections to the same running instance
 * observe the IDENTICAL epoch (a running instance draws it once, SPEC §19.1) — so we assert
 * equality across connects, never `nonzero` (nondeterministic + opaquely typed). The `pools`
 * assertion proves the HELLO_ACK pool-name advertising (commit 4d07554); it is compared against
 * {@see LiveTestCase::launchedPools} — what THIS run actually configured `ferrod` with — rather than
 * a hard-coded `['default']`, which silently pinned a one-pool harness and broke as soon as M1-S8a
 * added a second. Comparing the engine's advertisement to the harness's own config still fails on a
 * dropped, renamed or invented pool.
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
            $ack1->pools,
            'HELLO_ACK advertises exactly the pool names this run configured, in order',
        );

        // A SECOND connection in the same run: the running instance drew its epoch once, so the two
        // must match exactly (same value AND same type — an === comparison of the opaque scalar).
        $s2 = $this->connect();
        $ack2 = $s2->hello();
        $this->assertSame($ack1->bootEpoch, $ack2->bootEpoch, 'one running instance -> one boot_epoch');
        $this->assertSame($this->launchedPools(), $ack2->pools);

        $s1->close();
        $s2->close();
    }
}
