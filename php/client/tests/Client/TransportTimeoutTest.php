<?php // /php/client/tests/Client/TransportTimeoutTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\TransportException;
use Ferro\Client\Transport;
use PHPUnit\Framework\Attributes\RequiresFunction;
use PHPUnit\Framework\TestCase;

/**
 * M3-D1c: the real {@see Transport}'s timeout behaviour on a real Unix socket — what the in-memory
 * fakes cannot show, because they neither keep bytes nor share one timeout between reads and writes.
 */
final class TransportTimeoutTest extends TestCase
{
    private string $path = '';
    /** @var resource|null */
    private $server = null;

    protected function setUp(): void
    {
        $this->path = sys_get_temp_dir() . '/ferro-transport-' . getmypid() . '.sock';
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
    }

    /** @return array{0: Transport, 1: resource} the client transport and the accepted peer */
    private function pair(float $readTimeout): array
    {
        $t = Transport::connectUnix($this->path, 1.0, $readTimeout);
        $this->assertNotNull($this->server);
        $peer = stream_socket_accept($this->server, 1.0);
        $this->assertNotFalse($peer);
        return [$t, $peer];
    }

    /**
     * Review F7 (M4): a read that times out part-way through keeps the bytes it had, so the next
     * read resumes in step. Dropping them would read the rest of a frame as a new header.
     */
    public function testATimedOutReadKeepsWhatItHadRead(): void
    {
        [$t, $peer] = $this->pair(0.1);
        fwrite($peer, 'ABCDEFGHIJ');
        try {
            $t->readExact(16);
            $this->fail('only 10 of 16 bytes were sent');
        } catch (TransportException $e) {
            $this->assertTrue($e->isReadTimeout(), $e->getMessage());
        }
        fwrite($peer, 'KLMNOP');
        $this->assertSame('ABCDEFGHIJKLMNOP', $t->readExact(16));
    }

    /**
     * Review F2: PHP's socket stream bounds WRITES with the read timeout, so a read wait a request
     * deadline shortened to milliseconds used to bound the next write too — a large request then
     * failed mid-frame, which closes the session. A write always gets the configured timeout.
     */
    #[RequiresFunction('pcntl_fork')]
    #[RequiresFunction('posix_kill')]
    public function testAShortenedReadWaitDoesNotBoundTheNextWrite(): void
    {
        [$t, $peer] = $this->pair(5.0);
        $size = 1 << 20;
        $pid = pcntl_fork();
        if ($pid === 0) {
            // A slow reader: 64 KiB every 50 ms, so the writer must wait on a full socket buffer.
            $got = 0;
            while ($got < $size) {
                usleep(50_000);
                $chunk = fread($peer, 65536);
                if ($chunk === false || $chunk === '') {
                    break;
                }
                $got += strlen($chunk);
            }
            posix_kill(getmypid(), SIGKILL);
        }
        try {
            $t->setReadWait(0.02); // what a near request deadline leaves behind
            $t->writeAll(str_repeat('x', $size));
            $this->addToAssertionCount(1);
        } finally {
            posix_kill($pid, SIGKILL);
            pcntl_waitpid($pid, $status);
        }
    }

    /**
     * After a read timed out, a write that fails because the peer is gone is reported as the
     * broken pipe it is, not as a "write timed out" left over from the read.
     */
    public function testABrokenPipeAfterAReadTimeoutIsNotReportedAsATimeout(): void
    {
        [$t, $peer] = $this->pair(0.05);
        try {
            $t->readExact(1);
            $this->fail('nothing was sent');
        } catch (TransportException $e) {
            $this->assertTrue($e->isReadTimeout());
        }
        fclose($peer);
        try {
            $t->writeAll(str_repeat('x', 16));
            $this->fail('the peer is gone');
        } catch (TransportException $e) {
            $this->assertStringContainsString('write failed', $e->getMessage());
            $this->assertStringNotContainsString('timed out', $e->getMessage());
        }
    }
}
