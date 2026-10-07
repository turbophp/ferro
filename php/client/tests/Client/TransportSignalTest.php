<?php // /php/client/tests/Client/TransportSignalTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\TransportException;
use Ferro\Client\Transport;
use PHPUnit\Framework\Attributes\RequiresFunction;
use PHPUnit\Framework\TestCase;

/**
 * SPEC §24.8 / §24.17 premise R4, reproduced on the REAL {@see Transport} (M7-G3): a signal delivered
 * while a read is blocked does not end the read early — PHP restarts the read with its FULL timeout,
 * and an async handler runs after the read returns. So a signal can only LENGTHEN the transport's
 * tolerance, never make §24.8's wait bound unsafe: a worker SIGTERMed while its RESERVE is parked
 * keeps reading until the engine's terminal (bounded by `wait_ms + queue_wait_grace_ms`) arrives.
 *
 * Measured when §24 was drafted (PHP 8.4.19, a bare socketpair): a 3 s timeout with SIGTERM at 1 s
 * returned at 4.00 s, timed out, and the handler had run.
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
        pcntl_async_signals(false);
        pcntl_signal(SIGTERM, SIG_DFL);
    }

    #[RequiresFunction('pcntl_fork')]
    #[RequiresFunction('pcntl_async_signals')]
    #[RequiresFunction('posix_kill')]
    public function testASignalDuringABlockedReadOnlyLengthensItsTimeout(): void
    {
        $t = Transport::connectUnix($this->path, 1.0, 1.5);
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
        try {
            $t->readExact(1);
            $this->fail('nothing was sent');
        } catch (TransportException $e) {
            $this->assertTrue($e->isReadTimeout(), $e->getMessage());
        } finally {
            pcntl_waitpid($pid, $status);
        }
        $returned = microtime(true);
        $took = $returned - $started;
        // Never SHORTER than the configured timeout: the signal did not cut the read.
        $this->assertGreaterThanOrEqual(1.45, $took, sprintf('read returned after %.3f s', $took));
        // The read RESTARTED with its full timeout at the signal (0.5 s + 1.5 s), as R4 measured.
        $this->assertGreaterThanOrEqual(1.9, $took, sprintf('read returned after %.3f s', $took));
        $this->assertNotNull($handled, 'the async handler ran');
        $this->assertGreaterThanOrEqual($started + 0.4, $handled);
        fclose($peer);
    }
}
