<?php // /php/client/tests/Live/RestartLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

/**
 * The §19.1 restart proof against a real `ferrod`: do a call, SIGTERM+relaunch the daemon on the SAME
 * socket (a NEW random `boot_epoch`), then the next readonly call observes the lost connection,
 * transparently reconnects, sees the CHANGED epoch (`epochChanged:true` ⇒ engine state void), and
 * re-issues successfully against the restarted daemon.
 */
final class RestartLiveTest extends LiveTestCase
{
    public function testDaemonRestartTriggersEpochChangeAndTransparentReadRetry(): void
    {
        $conn = $this->connectConnection();
        try {
            // 1. A baseline call succeeds and caches the pre-restart epoch.
            $this->assertSame(1, $conn->scalar('SELECT 1'));
            $oldEpoch = $conn->currentEpoch();
            $this->assertSame(0, $conn->reconnectCount(), 'no reconnect yet');

            // 2. Restart the daemon on the same socket → a fresh boot_epoch.
            $this->restartFerrod();

            // 3. The next read hits the dead socket → ConnectionLost → epoch-aware reconnect → re-issue.
            $this->assertSame(1, $conn->scalar('SELECT 1'), 'the read transparently retried on the restarted daemon');

            $this->assertGreaterThanOrEqual(1, $conn->reconnectCount(), 'a reconnect happened');
            $this->assertTrue($conn->lastReconnectEpochChanged(), 'the boot_epoch changed across the restart (§19.1)');
            $this->assertNotSame($oldEpoch, $conn->currentEpoch(), 'the cached epoch was updated to the new one');
        } finally {
            $conn->session()->close();
        }
    }
}
