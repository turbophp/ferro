<?php // /php/client/tests/Live/HandshakeLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

/**
 * The first end-to-end proof of the S7 client against a real `ferrod`: the HELLO handshake carries
 * a `boot_epoch` and the advertised `pools`, and two connections to the same running instance
 * observe the IDENTICAL epoch (a running instance draws it once, SPEC §19.1) — so we assert
 * equality across connects, never `nonzero` (nondeterministic + opaquely typed). `pools===['default']`
 * proves the HELLO_ACK pool-name advertising (commit 4d07554).
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
        $this->assertSame(['default'], $ack1->pools, 'HELLO_ACK advertises the configured pool names');

        // A SECOND connection in the same run: the running instance drew its epoch once, so the two
        // must match exactly (same value AND same type — an === comparison of the opaque scalar).
        $s2 = $this->connect();
        $ack2 = $s2->hello();
        $this->assertSame($ack1->bootEpoch, $ack2->bootEpoch, 'one running instance -> one boot_epoch');
        $this->assertSame(['default'], $ack2->pools);

        $s1->close();
        $s2->close();
    }
}
