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

    /**
     * Review F1: a process at its fd limit still receives the engine's fd, because the transport
     * keeps one slot in reserve and frees it for the read a frame starts with. Without the reserve
     * the kernel closes the fd and truncates the control message — a result that SUCCEEDED (a
     * committed write) would be lost, where the `fread` path would have read it inline.
     *
     * Run in a child PHP process, so lowering `RLIMIT_NOFILE` and filling the fd table cannot
     * disturb this one. The child fills the table completely, reads an inline frame (which must NOT
     * free the reserve — a header peek says no fd can ride it), then receives an `OOB_FD` frame with
     * an fd, closes it (the reserve retakes the slot, so the application still cannot open a file),
     * and receives a second.
     */
    public function testAProcessAtItsFdLimitStillReceivesTheFd(): void
    {
        if (!function_exists('posix_setrlimit')) {
            $this->markTestSkipped('needs ext-posix to lower RLIMIT_NOFILE in the child');
        }
        $autoload = dirname(__DIR__, 2) . '/vendor/autoload.php';
        $code = <<<'PHP'
            require $argv[1];
            $path = $argv[2];
            posix_setrlimit(POSIX_RLIMIT_NOFILE, 64, 64);
            $server = stream_socket_server('unix://' . $path);
            $t = \Ferro\Client\Transport::connectUnix($path, 1.0, 2.0, true);
            $peer = stream_socket_accept($server, 1.0);
            $sock = socket_import_stream($peer);
            $files = [];
            foreach (['ONE', 'TWO'] as $c) { $f = tmpfile(); fwrite($f, $c); $files[] = $f; }
            // Loaded now: with the table full the autoloader could not open a class file.
            class_exists(\Ferro\Protocol\Header::class);
            class_exists(\Ferro\Protocol\Generated\Constants::class);
            class_exists(\Ferro\Client\Error\TransportException::class);
            $out = ['reserve_at_connect' => $t->holdsFdReserve()];
            $hold = [];
            while (($h = @fopen('/dev/null', 'r')) !== false) { $hold[] = $h; }
            $out['table_full'] = @fopen('/dev/null', 'r') === false;
            $C = \Ferro\Protocol\Generated\Constants::class;
            // An inline frame first: no fd can ride it, so the reserve is not freed for it.
            fwrite($peer, (new \Ferro\Protocol\Header($C::FLAG_END, 2, 1, 9, 0))->encode());
            $t->beginFrame();
            $t->readExact(16);
            $out['releases_after_inline'] = $t->fdReserveReleases();
            foreach ($files as $i => $f) {
                $head = (new \Ferro\Protocol\Header($C::FLAG_END | $C::FLAG_OOB_FD, 2, 1, $i + 1, 0))->encode();
                socket_sendmsg($sock, ['iov' => [$head],
                    'control' => [['level' => SOL_SOCKET, 'type' => SCM_RIGHTS, 'data' => [$f]]]], 0);
                try {
                    $t->beginFrame();
                    $bytes = $t->readExact(16);
                    $fd = $t->takeFd();
                    $out["frame$i"] = ($bytes === $head ? 'header' : 'WRONG BYTES') . ':' . (is_resource($fd) ? stream_get_contents($fd, -1, 0) : 'NO FD');
                    if (is_resource($fd)) { fclose($fd); $t->fdClosed(); }
                } catch (\Throwable $e) {
                    $out["frame$i"] = 'ERROR ' . $e->getMessage();
                }
                $out["reserve_after_$i"] = $t->holdsFdReserve();
                $out["app_can_open_after_$i"] = @fopen('/dev/null', 'r') !== false;
            }
            $out['releases'] = $t->fdReserveReleases();
            echo json_encode($out);
            PHP;
        $proc = proc_open([PHP_BINARY, '-r', $code, $autoload, $this->path . '.child'], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        $this->assertIsResource($proc);
        $stdout = (string) stream_get_contents($pipes[1]);
        $stderr = (string) stream_get_contents($pipes[2]);
        proc_close($proc);
        @unlink($this->path . '.child');
        $out = json_decode($stdout, true);
        $this->assertIsArray($out, "child output: {$stdout} {$stderr}");
        $this->assertSame([
            'reserve_at_connect' => true,
            'table_full' => true,
            'releases_after_inline' => 0,
            'frame0' => 'header:ONE',
            'reserve_after_0' => true,
            'app_can_open_after_0' => false,
            'frame1' => 'header:TWO',
            'reserve_after_1' => true,
            'app_can_open_after_1' => false,
            'releases' => 2,
        ], $out);
    }

    /**
     * The header peek's UNSURE branch (round-2 review): an `OOB_FD` header whose first 2 bytes
     * arrive in their own write, carrying the fd, and the other 14 in a second. A peek stops at the
     * first fd-bearing chunk, so it sees 2 bytes — too few to read the flags — and must answer
     * "may carry an fd", freeing the reserve; answering "no" would leave a full fd table with no
     * slot, and the kernel would close the fd. Under a full table, in a child process.
     */
    public function testAHeaderSplitBeforeItsFlagsStillFreesTheReserve(): void
    {
        if (!function_exists('posix_setrlimit')) {
            $this->markTestSkipped('needs ext-posix to lower RLIMIT_NOFILE in the child');
        }
        $autoload = dirname(__DIR__, 2) . '/vendor/autoload.php';
        $code = <<<'PHP'
            require $argv[1];
            $path = $argv[2];
            posix_setrlimit(POSIX_RLIMIT_NOFILE, 64, 64);
            $server = stream_socket_server('unix://' . $path);
            $t = \Ferro\Client\Transport::connectUnix($path, 1.0, 2.0, true);
            $peer = stream_socket_accept($server, 1.0);
            $sock = socket_import_stream($peer);
            $C = \Ferro\Protocol\Generated\Constants::class;
            $head = (new \Ferro\Protocol\Header($C::FLAG_END | $C::FLAG_OOB_FD, 2, 1, 7, 0))->encode();
            $f = tmpfile(); fwrite($f, 'SPLIT');
            class_exists(\Ferro\Client\Error\TransportException::class);
            $hold = [];
            while (($h = @fopen('/dev/null', 'r')) !== false) { $hold[] = $h; }
            socket_sendmsg($sock, ['iov' => [substr($head, 0, 2)],
                'control' => [['level' => SOL_SOCKET, 'type' => SCM_RIGHTS, 'data' => [$f]]]], 0);
            fwrite($peer, substr($head, 2));
            try {
                $t->beginFrame();
                $bytes = $t->readExact(16);
                $fd = $t->takeFd();
                $out = ($bytes === $head ? 'header' : 'WRONG BYTES') . ':' . (is_resource($fd) ? stream_get_contents($fd, -1, 0) : 'NO FD');
            } catch (\Throwable $e) {
                $out = 'ERROR ' . $e->getMessage();
            }
            echo json_encode(['frame' => $out, 'releases' => $t->fdReserveReleases()]);
            PHP;
        $proc = proc_open([PHP_BINARY, '-r', $code, $autoload, $this->path . '.split'], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        $this->assertIsResource($proc);
        $stdout = (string) stream_get_contents($pipes[1]);
        $stderr = (string) stream_get_contents($pipes[2]);
        proc_close($proc);
        @unlink($this->path . '.split');
        $this->assertSame(['frame' => 'header:SPLIT', 'releases' => 1], json_decode($stdout, true), "child: {$stdout} {$stderr}");
    }

    /**
     * Review F4: a received fd is close-on-exec from the moment it exists (`MSG_CMSG_CLOEXEC`), so a
     * child process the application starts never inherits a result's memfd.
     */
    public function testAReceivedFdIsNotInheritedByAChildProcess(): void
    {
        $t = $this->connect();
        $f = self::file('secret');
        $this->send('w', [$f]);
        // The sender's copy closes, so the only descriptor for this file is the received one.
        fclose($f);
        $this->assertSame('w', $t->readExact(1));
        $fd = $t->takeFd();
        $this->assertIsResource($fd);
        $st = fstat($fd);
        $this->assertIsArray($st);
        $mine = $st['dev'] . ':' . $st['ino'];

        $code = 'foreach (scandir("/proc/self/fd") as $n) { if ($n[0] !== ".") { $s = @stat("/proc/self/fd/$n"); '
            . 'if ($s) { echo $s["dev"], ":", $s["ino"], "\n"; } } }';
        $proc = proc_open([PHP_BINARY, '-r', $code], [1 => ['pipe', 'w']], $pipes);
        $this->assertIsResource($proc);
        $childFds = array_filter(explode("\n", (string) stream_get_contents($pipes[1])));
        proc_close($proc);
        $this->assertNotEmpty($childFds, 'the child listed its fds');
        $this->assertNotContains($mine, $childFds, 'the child inherited the received fd');
        fclose($fd);
        $t->close();
    }

    /**
     * Review F6: in AUTO mode a failure to set up `recvmsg` (importing the socket, or setting its
     * receive timeout) falls back to the `fread` path instead of failing the connection; asked for
     * explicitly, it throws.
     */
    public function testAutoModeFallsBackToFreadWhenFdReceivingCannotBeSetUp(): void
    {
        Transport::$importStream = static fn ($s): bool => false;
        try {
            $t = Transport::connectUnix($this->path, 1.0, 2.0);
            $this->assertIsResource($this->server);
            $peer = stream_socket_accept($this->server, 1.0);
            $this->assertIsResource($peer);
            $this->assertFalse($t->receivesFds(), 'auto mode fell back to fread');
            fwrite($peer, 'ok');
            $this->assertSame('ok', $t->readExact(2));
            $t->close();
            fclose($peer);

            $this->expectException(TransportException::class);
            Transport::connectUnix($this->path, 1.0, 2.0, true);
        } finally {
            Transport::$importStream = null;
        }
    }

    /**
     * A frame-start read waits ONCE: the header peek that decides whether to free the fd reserve
     * waits on `SO_RCVTIMEO` like the read itself, and its timeout IS the read's timeout — not a
     * first wait followed by a second one in `recvmsg`. Nothing is consumed, so the frame that
     * arrives afterwards reads intact, with its fd.
     */
    public function testAFrameStartReadOnASilentPeerTimesOutOnce(): void
    {
        $t = $this->connect(readTimeout: 5.0);
        $t->setReadWait(0.3);
        $t->beginFrame();
        $start = microtime(true);
        try {
            $t->readExact(16);
            $this->fail('a silent peer must time out');
        } catch (TransportException $e) {
            $this->assertTrue($e->isReadTimeout(), $e->getMessage());
        }
        $waited = microtime(true) - $start;
        $this->assertLessThan(0.5, $waited, "one 0.3 s wait, not two ({$waited} s)");

        $head = (new \Ferro\Protocol\Header(\Ferro\Protocol\Generated\Constants::FLAG_END | \Ferro\Protocol\Generated\Constants::FLAG_OOB_FD, 2, 1, 7, 0))->encode();
        $this->send($head, [self::file('late')]);
        $t->beginFrame();
        $this->assertSame($head, $t->readExact(16));
        $this->assertSame('late', self::contentOf($t->takeFd()));
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
