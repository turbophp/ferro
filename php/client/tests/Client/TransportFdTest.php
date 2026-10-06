<?php // /php/client/tests/Client/TransportFdTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\TransportException;
use Ferro\Client\Transport;
use PHPUnit\Framework\TestCase;

/**
 * The real {@see Transport}'s `recvmsg` read path (M3-D3), over a real Unix socket with no `ferrod`:
 * a server in this process sends bytes and fds with `socket_sendmsg`, and the transport must read
 * every byte, queue every fd in arrival order, and refuse what it cannot account for.
 *
 * The control test is the reason the `recvmsg` path exists: the SAME bytes and fd read through the
 * `fread` path arrive with the fd silently gone.
 */
final class TransportFdTest extends TestCase
{
    private string $path = '';
    /** @var resource|null */
    private $server = null;
    /** @var resource|null */
    private $peer = null;

    protected function setUp(): void
    {
        if (!Transport::canReceiveFds()) {
            $this->markTestSkipped('needs Linux and ext-sockets');
        }
        $this->path = sys_get_temp_dir() . '/ferro-fdt-' . bin2hex(random_bytes(6)) . '.sock';
        $server = stream_socket_server('unix://' . $this->path);
        $this->assertIsResource($server);
        $this->server = $server;
    }

    protected function tearDown(): void
    {
        if (is_resource($this->peer)) { fclose($this->peer); }
        if (is_resource($this->server)) { fclose($this->server); }
        if ($this->path !== '' && file_exists($this->path)) { @unlink($this->path); }
    }

    private function connect(bool $receiveFds = true, float $readTimeout = 2.0): Transport
    {
        $t = Transport::connectUnix($this->path, 1.0, $readTimeout, $receiveFds);
        $this->assertIsResource($this->server);
        $peer = stream_socket_accept($this->server, 1.0);
        $this->assertIsResource($peer);
        $this->peer = $peer;
        return $t;
    }

    /**
     * @param list<resource> $fds
     */
    private function send(string $bytes, array $fds = []): void
    {
        $this->assertIsResource($this->peer);
        $sock = socket_import_stream($this->peer);
        $this->assertInstanceOf(\Socket::class, $sock);
        $msg = ['iov' => [$bytes]];
        if ($fds !== []) {
            $msg['control'] = [['level' => SOL_SOCKET, 'type' => SCM_RIGHTS, 'data' => $fds]];
        }
        $this->assertSame(strlen($bytes), socket_sendmsg($sock, $msg, 0));
    }

    /** @return resource a temp file holding `$content` */
    private static function file(string $content)
    {
        $f = tmpfile();
        self::assertIsResource($f);
        fwrite($f, $content);
        return $f;
    }

    /** @param mixed $fd */
    private static function contentOf($fd): string
    {
        self::assertIsResource($fd);
        $s = stream_get_contents($fd, -1, 0);
        fclose($fd);
        return (string) $s;
    }

    /**
     * Three fds, each riding its own write, read back by ONE `readExact` that spans all of them:
     * every byte arrives, and the fds come out oldest first.
     */
    public function testEveryFdIsQueuedInArrivalOrder(): void
    {
        $t = $this->connect();
        $this->assertTrue($t->receivesFds());
        $this->send('aaaaa', [self::file('A')]);
        $this->send('bbbbb', [self::file('B')]);
        $this->send('ccccc', [self::file('C')]);
        $this->send('ddddd');

        $this->assertSame('aaaaabbbbbcccccddddd', $t->readExact(20));
        $this->assertSame(3, $t->fdsReceived());
        $this->assertSame('A', self::contentOf($t->takeFd()));
        $this->assertSame('B', self::contentOf($t->takeFd()));
        $this->assertSame('C', self::contentOf($t->takeFd()));
        $this->assertNull($t->takeFd());
        $t->close();
    }

    /** More fds in one message than the control buffer holds: the kernel drops some, and that is loud. */
    public function testATruncatedControlMessageIsRefused(): void
    {
        $t = $this->connect();
        $this->send('xxxx', [self::file('1'), self::file('2'), self::file('3'), self::file('4'), self::file('5')]);
        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('truncated');
        $t->readExact(4);
    }

    public function testASilentPeerTimesOutAndAClosedOneIsEof(): void
    {
        $t = $this->connect(readTimeout: 0.2);
        $start = microtime(true);
        try {
            $t->readExact(1);
            $this->fail('a silent peer must time out');
        } catch (TransportException $e) {
            $this->assertStringContainsString('timed out', $e->getMessage());
        }
        $this->assertLessThan(2.0, microtime(true) - $start, 'bounded by the read timeout');

        $this->send('ab');
        $this->assertIsResource($this->peer);
        fclose($this->peer);
        $this->assertSame('ab', $t->readExact(2));
        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('unexpected EOF');
        $t->readExact(1);
    }

    /**
     * M3-D1c's two read rules hold on the `recvmsg` path too: a deadline-shortened wait
     * ({@see Transport::setReadWait}) bounds `recvmsg` — which `stream_set_timeout` alone would not,
     * since this path waits on `SO_RCVTIMEO` — and a read that times out mid-frame keeps what it read
     * (and the fd that came with it), so the next read resumes in step.
     */
    public function testAShortenedWaitBoundsRecvmsgAndAPartialReadResumes(): void
    {
        $t = $this->connect(readTimeout: 5.0);
        $t->setReadWait(0.1);
        $this->send('abc', [self::file('F')]);
        $start = microtime(true);
        try {
            $t->readExact(5);
            $this->fail('a short wait must time out');
        } catch (TransportException $e) {
            $this->assertTrue($e->isReadTimeout(), $e->getMessage());
        }
        $this->assertLessThan(1.0, microtime(true) - $start, 'the 0.1 s wait applied, not the 5 s timeout');
        $this->assertSame(1, $t->fdsReceived(), 'the fd that arrived before the timeout is kept');

        $this->send('de');
        $this->assertSame('abcde', $t->readExact(5), 'no byte lost, none repeated');
        $this->assertSame('F', self::contentOf($t->takeFd()));
        $t->close();
    }

    /** The CONTROL: the `fread` path reads the same bytes and loses the fd without a trace. */
    public function testTheFreadPathSilentlyLosesAnFd(): void
    {
        $t = $this->connect(receiveFds: false);
        $this->assertFalse($t->receivesFds());
        $this->send('hello', [self::file('lost')]);
        $this->assertSame('hello', $t->readExact(5));
        $this->assertSame(0, $t->fdsReceived());
        $this->assertNull($t->takeFd());
        $t->close();
    }

    /** `close()` closes fds that arrived but were never taken. */
    public function testCloseClosesUntakenFds(): void
    {
        $t = $this->connect();
        $f = self::file('orphan');
        $this->send('z', [$f]);
        $this->assertSame('z', $t->readExact(1));
        $this->assertSame(1, $t->fdsReceived());
        $t->close();
        $this->assertNull($t->takeFd(), 'the queue is emptied');
    }
}
