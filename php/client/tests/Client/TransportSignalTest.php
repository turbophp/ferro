<?php // /php/client/tests/Client/TransportSignalTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\TransportException;
use Ferro\Client\Transport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresFunction;
use PHPUnit\Framework\TestCase;

/**
 * SPEC §24.8 / §24.17 premise R4, reproduced on the REAL {@see Transport} (M7-G3): a signal delivered
 * while a read is blocked does not end the read early — PHP restarts the read with its FULL timeout,
 * and the async handler runs only after the read returns. So a signal can only LENGTHEN the
 * transport's tolerance, never make §24.8's wait bound unsafe: a worker SIGTERMed while its RESERVE
 * is parked keeps reading until the engine's terminal (bounded by `wait_ms + queue_wait_grace_ms`)
 * arrives.
 *
 * Measured when §24 was drafted (PHP 8.4.19, a bare socketpair): a 3 s timeout with SIGTERM at 1 s
 * returned at 4.00 s, timed out, and the handler had run. Both of the transport's read paths are
 * exercised — `fread` and, where ext-sockets offers it, `recvmsg` — in the default suite, so CI gates
 * both (its fread-only lane runs only `tests/Live`; review F5).
 */
final class TransportSignalTest extends TestCase
{
    private string $path = '';
    /** @var resource|null */
    private $server = null;

    protected function setUp(): void
    {
        $this->path = sys_get_temp_dir() . '/ferro-signal-' . getmypid() . '.sock';
        @unlink($this->path);
        $server = stream_socket_server('unix://' . $this->path, $errno, $errstr);
        $this->assertNotFalse($server, $errstr);
        $this->server = $server;
    }

    protected function tearDown(): void
    {
        if (is_resource($this->server)) {
            fclose($this->server);
        }
        @unlink($this->path);
        if (function_exists('pcntl_async_signals')) {
            pcntl_async_signals(false);
            pcntl_signal(SIGTERM, SIG_DFL);
        }
    }

    /** @return array<string, array{0: bool}> */
    public static function readPaths(): array
    {
        return ['fread' => [false], 'recvmsg' => [true]];
    }

    #[DataProvider('readPaths')]
    #[RequiresFunction('pcntl_fork')]
    #[RequiresFunction('pcntl_async_signals')]
    #[RequiresFunction('posix_kill')]
    public function testASignalDuringABlockedReadOnlyLengthensItsTimeout(bool $recvmsg): void
    {
        if ($recvmsg && !Transport::canReceiveFds()) {
            $this->markTestSkipped('the recvmsg read path needs Linux and ext-sockets');
        }
        $t = Transport::connectUnix($this->path, 1.0, 1.5, $recvmsg);
        $this->assertSame($recvmsg, $t->receivesFds(), 'the read path under test');
        $this->assertNotNull($this->server);
        $peer = stream_socket_accept($this->server, 1.0);
        $this->assertNotFalse($peer);

        $handled = null;
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, static function () use (&$handled): void {
            $handled = microtime(true);
        });
        $parent = getmypid();
        $pid = pcntl_fork();
        if ($pid === 0) {
            usleep(500_000);
            posix_kill($parent, SIGTERM);
            posix_kill(getmypid(), SIGKILL);
        }
        $started = microtime(true);
        $returned = null;
        try {
            $t->readExact(1);
            $this->fail('nothing was sent');
        } catch (TransportException $e) {
            $returned = microtime(true);
            $this->assertTrue($e->isReadTimeout(), $e->getMessage());
        } finally {
            pcntl_waitpid($pid, $status);
        }
        $took = $returned - $started;
        // Never SHORTER than the configured 1.5 s: the signal did not cut the read.
        $this->assertGreaterThanOrEqual(1.45, $took, sprintf('read returned after %.3f s', $took));
        // The read RESTARTED with its full timeout at the signal (0.5 s + 1.5 s = 2.0 s), as R4 measured
        // (asserted at ≥ 1.9 s for clock slack).
        $this->assertGreaterThanOrEqual(1.9, $took, sprintf('read returned after %.3f s', $took));
        $this->assertNotNull($handled, 'the async handler ran');
        if ($recvmsg) {
            // Measured (M7-G3 review round): on the `recvmsg` path the handler runs AT the signal —
            // the syscall returns EINTR, the client's loop ticks the VM and re-enters `recvmsg` — and
            // the read still is not cut short (asserted above). §24.8's premise is about the read's
            // tolerance, which holds on both paths; only the handler's timing differs.
            $this->assertGreaterThanOrEqual($started + 0.4, $handled, 'not before the signal');
            $this->assertLessThan($returned, $handled, 'during the read, which then resumed');
        } else {
            // On the `fread` path the handler runs only AFTER the blocked read returned, as R4 measured.
            $this->assertGreaterThanOrEqual($started + 1.9, $handled, 'the handler ran after the read returned');
        }
        fclose($peer);
    }
}
