<?php // /php/client/src/Client/Transport.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\TransportException;
use Ferro\Protocol\Generated\Constants as C;

/**
 * Dependency-free stream transport (charter rule 7): `stream_socket_client` over `unix://`
 * (primary, `/run/ferro/{schema_hash}.sock`) or `tcp://` (the `FERRO_ADDR` fallback). Blocking reads
 * and writes; {@see Session} routes several in-flight requests over it (M3-D1a).
 *
 * **Two read paths (M3-D3).** By default every byte is read with `fread`. When `ext-sockets` is
 * loaded, the OS is Linux and the transport is a Unix domain socket, the stream is also imported as
 * an `ext-sockets` socket and EVERY byte — from the handshake on — is read with `socket_recvmsg`
 * instead (its wait is `SO_RCVTIMEO`, which {@see setReadWait} moves together with the stream
 * timeout), so the engine can pass a sealed memfd with `SCM_RIGHTS` (SPEC §5.1;
 * {@see FdReceivingTransportInterface}). Every byte, because the kernel discards an fd attached to
 * bytes read with plain `read(2)`, and because PHP's stream layer may read ahead into a buffer that a
 * later `recvmsg` would never see. Reads ask for EXACTLY the bytes needed, never more, so there is no
 * userland buffer either: the socket's readability, which a scheduler `stream_select`s on, stays
 * the whole truth. Writes stay `fwrite` on the same descriptor in both modes. `ext-sockets` is
 * detected at runtime and never required: without it the `fread` path is the only one.
 *
 * The stream is held as `/** @var resource *​/ private $sock` (a stream resource has no native
 * property type, so this class is deliberately NOT `readonly`); both
 * `stream_socket_client`'s `resource|false` and `fread`'s `string|false` are handled explicitly so
 * the type stays a bare `resource` for PHPStan level 9.
 */
final class Transport implements SelectableTransportInterface, FdReceivingTransportInterface
{
    private const DEFAULT_CONNECT_TIMEOUT = 5.0;
    private const DEFAULT_READ_TIMEOUT = 30.0;

    /**
     * Control-buffer room for this many fds per `recvmsg`. The engine attaches one fd per `OOB_FD`
     * frame and the kernel never returns two fd-bearing writes in one read, so 1 would do; the slack
     * means a surprise surfaces as fds to account for rather than as a silently truncated control
     * message (which the kernel answers by CLOSING the fds that did not fit — refused below anyway).
     */
    private const FD_SLOTS = 4;

    /** @var resource the connected stream */
    private $sock;

    /** The same descriptor as an `ext-sockets` socket, when this transport reads with `recvmsg`. */
    private ?\Socket $fdSocket = null;

    /** @var list<resource> received fds not yet taken, oldest first */
    private array $fds = [];

    private int $fdsReceived = 0;

    /**
     * One fd-table slot held in reserve while this transport receives fds (review F1). When a
     * process is at its `RLIMIT_NOFILE`, the kernel cannot install a received fd: it CLOSES it and
     * reports a truncated control message, and a result the engine had already produced — a
     * committed write — is lost, where the `fread` path would have read it. The reserve is released
     * immediately before the one read an fd can arrive with ({@see beginFrame}) and retaken as soon
     * as a slot is free again ({@see fdClosed}). One PHP thread per fd table runs one thing at a
     * time, so nothing can take the freed slot between the release and the `recvmsg` — an
     * ASSUMPTION: on a thread-safe (ZTS) SAPI that runs several PHP threads in one process
     * (FrankenPHP, Octane on FrankenPHP) another thread can open a file in that window. Unverified
     * (no ZTS build was available); the cost would be F1's behaviour returning under fd exhaustion —
     * the fd closed by the kernel, a committed large write reported `Indeterminate` — and the
     * mitigation is `receiveFds: false`. An `ext-sockets` socket rather than an open file: one
     * descriptor, and not subject to `open_basedir`.
     */
    private ?\Socket $reserve = null;

    /** Set by {@see beginFrame}, consumed by the next {@see readExact}. */
    private bool $frameStart = false;

    /** How many reads freed the reserve ({@see fdReserveReleases}). */
    private int $reserveReleases = 0;

    /**
     * Bytes of a frame already read when a read timed out (M3-D1c). The next {@see readExact}
     * starts from them, so a timeout never drops bytes and never puts the stream out of step.
     */
    private string $pending = '';

    /** The wait currently applied with `stream_set_timeout`, to skip redundant calls. */
    private float $appliedWait;

    /** The wait currently applied as `SO_RCVTIMEO` on the `recvmsg` path (M3-D3); -1 before any. */
    private float $appliedRcvWait = -1.0;

    /**
     * @param resource $sock an already-connected, blocking stream
     */
    private function __construct($sock, private readonly float $readTimeout = self::DEFAULT_READ_TIMEOUT)
    {
        $this->sock = $sock;
        $this->appliedWait = $readTimeout;
    }

    public function setReadWait(float $seconds): void
    {
        $seconds = max($seconds, 0.001);
        if ($seconds === $this->appliedWait || !is_resource($this->sock)) {
            return;
        }
        $this->applyTimeout($seconds);
    }

    /**
     * `stream_set_timeout` bounds WRITES as well as reads, and it also clears the stream's
     * `timed_out` flag. Both matter to {@see writeAll} (M3-D1c review F2).
     */
    private function applyTimeout(float $seconds): void
    {
        $sec = (int) $seconds;
        stream_set_timeout($this->sock, $sec, (int) round(($seconds - $sec) * 1_000_000));
        $this->appliedWait = $seconds;
        // On the `recvmsg` path (M3-D3) the read wait is the socket's `SO_RCVTIMEO`, which
        // `stream_set_timeout` does not touch. Writes stay on `fwrite`, bounded above.
        if ($this->fdSocket !== null && $seconds !== $this->appliedRcvWait) {
            $this->setRcvTimeout($this->fdSocket, $seconds);
        }
    }

    /** `SO_RCVTIMEO` for the `recvmsg` path. `{0, 0}` would mean "forever"; callers pass >= 1 ms. */
    private function setRcvTimeout(\Socket $socket, float $seconds): bool
    {
        $sec = (int) $seconds;
        $usec = (int) round(($seconds - $sec) * 1_000_000);
        if ($sec === 0 && $usec === 0) {
            $usec = 1;
        }
        $ok = @socket_set_option($socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => $sec, 'usec' => $usec]);
        if ($ok) {
            $this->appliedRcvWait = $seconds;
        }
        return $ok;
    }

    public function readTimeout(): float
    {
        return $this->readTimeout;
    }

    /**
     * Whether this process can receive fds at all (M3-D3): Linux, with `ext-sockets` providing
     * `socket_recvmsg` and `SCM_RIGHTS`. A transport also has to be a Unix domain socket.
     */
    public static function canReceiveFds(): bool
    {
        return PHP_OS_FAMILY === 'Linux'
            && extension_loaded('sockets')
            && function_exists('socket_recvmsg')
            && function_exists('socket_import_stream')
            && defined('SCM_RIGHTS');
    }

    /**
     * Connect over a Unix domain socket at `$socketPath` (the primary transport).
     *
     * @param ?bool $receiveFds read with `recvmsg` so the engine may pass a memfd (M3-D3). `null`
     *   (the default) does so exactly when {@see canReceiveFds}; `false` keeps the `fread` path;
     *   `true` insists, and throws when this process cannot.
     */
    public static function connectUnix(
        string $socketPath,
        float $connectTimeout = self::DEFAULT_CONNECT_TIMEOUT,
        float $readTimeout = self::DEFAULT_READ_TIMEOUT,
        ?bool $receiveFds = null,
    ): self {
        $wantFds = $receiveFds ?? self::canReceiveFds();
        if ($wantFds && !self::canReceiveFds()) {
            throw new TransportException('receiving fds needs Linux and ext-sockets (socket_recvmsg, SCM_RIGHTS)');
        }
        $t = self::open('unix://' . $socketPath, $connectTimeout, $readTimeout);
        if ($wantFds && !$t->enableFdReceive()) {
            // Asked for explicitly, a failure is the caller's to see. In AUTO mode it is not a
            // reason to fail a connection the `fread` path serves perfectly well (review F6).
            if ($receiveFds === true) {
                $t->close();
                throw new TransportException('could not set up fd receiving on the Unix socket');
            }
        }
        return $t;
    }

    /** Connect over TCP (the `FERRO_ADDR` fallback), e.g. host `127.0.0.1`, port `7777`. Never receives fds. */
    public static function connectTcp(
        string $host,
        int $port,
        float $connectTimeout = self::DEFAULT_CONNECT_TIMEOUT,
        float $readTimeout = self::DEFAULT_READ_TIMEOUT,
    ): self {
        return self::open('tcp://' . $host . ':' . $port, $connectTimeout, $readTimeout);
    }

    private static function open(string $remote, float $connectTimeout, float $readTimeout): self
    {
        $errno = 0;
        $errstr = '';
        // STREAM_CLIENT_CONNECT is the default; being explicit keeps the intent clear.
        $sock = @stream_socket_client(
            $remote,
            $errno,
            $errstr,
            $connectTimeout,
            STREAM_CLIENT_CONNECT,
        );
        if ($sock === false) {
            throw new TransportException(sprintf(
                'connect failed to %s: %s (errno %d)',
                $remote,
                $errstr !== '' ? $errstr : 'unknown error',
                $errno,
            ));
        }
        stream_set_blocking($sock, true);
        // Split the read timeout into whole seconds + microseconds for stream_set_timeout.
        $sec = (int) $readTimeout;
        $usec = (int) round(($readTimeout - $sec) * 1_000_000);
        stream_set_timeout($sock, $sec, $usec);
        return new self($sock, $readTimeout);
    }

    /**
     * Switch to the `recvmsg` read path, before anything has been read. The read timeout becomes the
     * socket's `SO_RCVTIMEO`, which bounds each `recvmsg` the way `stream_set_timeout` bounds each
     * `fread`. `false` — the stream untouched, still on the `fread` path — when the socket cannot be
     * imported or its receive timeout set.
     */
    private function enableFdReceive(): bool
    {
        $socket = self::$importStream !== null ? (self::$importStream)($this->sock) : @socket_import_stream($this->sock);
        if (!$socket instanceof \Socket) {
            return false;
        }
        if (!$this->setRcvTimeout($socket, $this->appliedWait)) {
            return false;
        }
        $this->fdSocket = $socket;
        $this->acquireReserve();
        return true;
    }

    /**
     * Test seam (review F6): replaces `socket_import_stream` so a test can make the import fail.
     * `null` restores it.
     *
     * @internal
     * @var (\Closure(resource): (\Socket|false))|null
     */
    public static ?\Closure $importStream = null;

    private function acquireReserve(): void
    {
        if ($this->reserve !== null || $this->fdSocket === null) {
            return;
        }
        $reserve = @socket_create(AF_UNIX, SOCK_DGRAM, 0);
        $this->reserve = $reserve instanceof \Socket ? $reserve : null;
    }

    private function releaseReserve(): void
    {
        if ($this->reserve !== null) {
            socket_close($this->reserve);
            $this->reserve = null;
        }
    }

    /** Whether the fd-table reserve is currently held. Diagnostic, for the tests. */
    public function holdsFdReserve(): bool
    {
        return $this->reserve !== null;
    }

    /**
     * How many reads have freed the fd-table reserve. Diagnostic: only a frame that may carry an fd
     * should, since freeing and retaking it costs about 2 µs.
     */
    public function fdReserveReleases(): int
    {
        return $this->reserveReleases;
    }

    /**
     * Whether the frame at the head of the socket's queue is an `OOB_FD` frame. Its flags (header
     * bytes 2-3) are read with a blocking `MSG_PEEK` into NO control buffer: the kernel gives a peek
     * with no room for an fd no fd, so the fd stays queued for the real read (measured). It waits as
     * a read does, on `SO_RCVTIMEO`, and its timeout is that read's — nothing was consumed. Unsure
     * (fewer than 4 bytes queued yet, an unexpected error) answers yes: freeing the reserve costs
     * about 2 µs, a lost fd costs a result. The peek costs about 0.4 µs, so frames that carry no fd —
     * nearly all — skip the reserve swap.
     */
    private function frameMayCarryAnFd(\Socket $socket, int $n): bool
    {
        while (true) {
            $peek = '';
            socket_clear_error($socket);
            $got = @socket_recv($socket, $peek, 4, MSG_PEEK);
            if ($got === false) {
                $err = socket_last_error($socket);
                if ($err === SOCKET_EINTR) {
                    continue;
                }
                if ($err === SOCKET_EAGAIN) {
                    throw TransportException::readTimedOut(sprintf('read timed out after 0 of %d bytes', $n));
                }
                return true;
            }
            if ($got < 4 || !is_string($peek)) {
                return true;
            }
            $u = unpack('v', $peek, 2);
            return !is_array($u) || !is_int($u[1] ?? null) || ($u[1] & C::FLAG_OOB_FD) !== 0;
        }
    }

    public function beginFrame(): void
    {
        $this->frameStart = true;
    }

    public function fdClosed(): void
    {
        $this->acquireReserve();
    }

    public function receivesFds(): bool
    {
        return $this->fdSocket !== null;
    }

    public function takeFd(): mixed
    {
        return array_shift($this->fds);
    }

    public function fdsReceived(): int
    {
        return $this->fdsReceived;
    }

    public function readExact(int $n): string
    {
        if ($n < 0) { throw new TransportException("readExact: negative length {$n}"); }
        if ($n === 0) { return ''; }
        $this->assertOpen('read');
        $frameStart = $this->frameStart;
        $this->frameStart = false;

        // Resume from what an earlier timed-out read had already received.
        $buf = (string) substr($this->pending, 0, $n);
        $this->pending = (string) substr($this->pending, $n);
        $remaining = $n - strlen($buf);
        $fdSocket = $this->fdSocket;
        if ($fdSocket !== null) {
            // A frame's first byte is the only byte an fd rides; a read resuming a frame already
            // has it (and its fd).
            if (!$frameStart || $buf !== '' || !$this->frameMayCarryAnFd($fdSocket, $n)) {
                return $this->recvExact($fdSocket, $buf, $n);
            }
            $this->reserveReleases++;
            $this->releaseReserve();
            try {
                return $this->recvExact($fdSocket, $buf, $n);
            } finally {
                // Fails while the fd just received holds the slot; {@see fdClosed} retakes it.
                $this->acquireReserve();
            }
        }
        while ($remaining > 0) {
            // Suppress the PHP-level warning (a dead peer raises one): the return value + stream meta
            // below are the authoritative error signal, surfaced as a typed TransportException.
            $chunk = @fread($this->sock, $remaining);
            if ($chunk === false || $chunk === '') {
                $meta = stream_get_meta_data($this->sock);
                if ($meta['timed_out'] === true) {
                    // Keep what was read: the frame is still in step, and the caller may wait on.
                    $this->pending = $buf . $this->pending;
                    throw TransportException::readTimedOut(sprintf('read timed out after %d of %d bytes', $n - $remaining, $n));
                }
                if (feof($this->sock)) {
                    throw new TransportException(sprintf('unexpected EOF after %d of %d bytes', $n - $remaining, $n));
                }
                throw new TransportException(sprintf('read failed after %d of %d bytes', $n - $remaining, $n));
            }
            $buf .= $chunk;
            $remaining -= strlen($chunk);
        }
        return $buf;
    }

    /**
     * The `recvmsg` read path: complete `$buf` (what an earlier timed-out read left, M3-D1c) to
     * exactly `$n` bytes, queueing every fd that arrives with them. A timeout keeps what was read,
     * exactly as the `fread` path does; an fd that arrived before it stays queued.
     */
    private function recvExact(\Socket $socket, string $buf, int $n): string
    {
        $remaining = $n - strlen($buf);
        $controlLen = socket_cmsg_space(SOL_SOCKET, SCM_RIGHTS, self::FD_SLOTS) ?? 0;
        // Received fds are close-on-exec from the moment they exist, so a `proc_open` elsewhere in
        // the process cannot inherit one in the window before it is read and closed.
        $flags = defined('MSG_CMSG_CLOEXEC') ? (int) constant('MSG_CMSG_CLOEXEC') : 0;
        while ($remaining > 0) {
            $msg = ['name' => [], 'buffer_size' => $remaining, 'controllen' => $controlLen];
            socket_clear_error();
            $got = @socket_recvmsg($socket, $msg, $flags);
            if ($got === false) {
                // `socket_recvmsg` reports through the GLOBAL error slot, not the socket's (measured).
                $err = socket_last_error();
                if ($err === SOCKET_EINTR) {
                    continue;
                }
                if ($err === SOCKET_EAGAIN) { // == EWOULDBLOCK on Linux: SO_RCVTIMEO expired
                    // Keep what was read: the frame is still in step, and the caller may wait on.
                    $this->pending = $buf . $this->pending;
                    throw TransportException::readTimedOut(sprintf('read timed out after %d of %d bytes', $n - $remaining, $n));
                }
                throw new TransportException(sprintf(
                    'read failed after %d of %d bytes: %s',
                    $n - $remaining,
                    $n,
                    socket_strerror($err),
                ));
            }
            // Take the fds BEFORE judging the read: an fd that arrived must be owned (and so closed)
            // by this transport whatever happens next.
            $this->collectFds($msg);
            if ((((int) ($msg['flags'] ?? 0)) & MSG_CTRUNC) !== 0) {
                throw new TransportException('an fd was lost: the control message was truncated');
            }
            if ($got === 0) {
                throw new TransportException(sprintf('unexpected EOF after %d of %d bytes', $n - $remaining, $n));
            }
            $iov = $msg['iov'] ?? [];
            $chunk = is_array($iov) && isset($iov[0]) && is_string($iov[0]) ? $iov[0] : '';
            if (strlen($chunk) !== $got) {
                throw new TransportException('recvmsg returned a buffer that does not match its length');
            }
            $buf .= $chunk;
            $remaining -= $got;
        }
        return $buf;
    }

    /** @param array<mixed> $msg a `socket_recvmsg` result */
    private function collectFds(array $msg): void
    {
        $control = $msg['control'] ?? [];
        if (!is_array($control)) {
            return;
        }
        foreach ($control as $cmsg) {
            if (!is_array($cmsg) || ($cmsg['level'] ?? null) !== SOL_SOCKET || ($cmsg['type'] ?? null) !== SCM_RIGHTS) {
                continue;
            }
            $data = $cmsg['data'] ?? [];
            foreach (is_array($data) ? $data : [] as $fd) {
                $this->fdsReceived++;
                if (is_resource($fd)) {
                    $this->fds[] = $fd;
                } elseif ($fd instanceof \Socket) {
                    // `ext-sockets` wraps a received SOCKET as a Socket, anything else as a stream.
                    // The engine never passes a socket; close it and let the frame that expected an
                    // fd find none — a desync the session reports.
                    socket_close($fd);
                }
            }
        }
    }

    public function writeAll(string $bytes): void
    {
        $this->assertOpen('write');
        // A write is bounded by the configured timeout, never by a read wait that a request
        // deadline shortened (M3-D1c review F2): PHP's socket stream applies ONE timeout to both
        // directions, so a wait shortened to milliseconds would make the next large request fail
        // mid-frame — which closes the session. Re-applying it unconditionally also clears a
        // `timed_out` flag left by an earlier read, so a broken pipe below is not reported as a
        // timeout.
        $this->applyTimeout($this->readTimeout);
        $len = strlen($bytes);
        $written = 0;
        while ($written < $len) {
            // Suppress the PHP-level warning on a broken pipe (a restarted/dead ferrod): the false/0
            // return + stream meta are handled below and surfaced as a typed TransportException, which
            // the §19.1 reconnect loop acts on. Without @, every reconnect would emit a stray notice.
            $n = @fwrite($this->sock, substr($bytes, $written));
            if ($n === false || $n === 0) {
                $meta = stream_get_meta_data($this->sock);
                if ($meta['timed_out'] === true) {
                    throw new TransportException(sprintf('write timed out after %d of %d bytes', $written, $len));
                }
                throw new TransportException(sprintf('write failed after %d of %d bytes', $written, $len));
            }
            $written += $n;
        }
    }

    /**
     * A read or write on a closed stream is a {@see TransportException}, never PHP's `TypeError`,
     * which `@` cannot suppress and which would escape every caller's typed handling (M3-D1a
     * review F2/F3).
     */
    private function assertOpen(string $op): void
    {
        if (!is_resource($this->sock)) {
            throw new TransportException("{$op} on a closed transport");
        }
    }

    public function stream(): mixed
    {
        return is_resource($this->sock) ? $this->sock : null;
    }

    public function close(): void
    {
        foreach ($this->fds as $fd) {
            if (is_resource($fd)) {
                fclose($fd);
            }
        }
        $this->fds = [];
        $this->releaseReserve();
        // The imported socket shares the stream's descriptor and does not close it; dropping it
        // before the stream closes means nothing can read a descriptor number the OS may reuse.
        $this->fdSocket = null;
        if (is_resource($this->sock)) {
            fclose($this->sock);
        }
    }
}
