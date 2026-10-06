<?php // /php/client/tests/Support/FakeFdTransport.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Client\Error\TransportException;
use Ferro\Client\FdReceivingTransportInterface;

/**
 * An in-memory {@see FdReceivingTransportInterface} for the M3-D3 session tests: inbound bytes as
 * {@see FakeTransport} queues them, plus a FIFO of "received" fds — real stream resources (a temp
 * file stands in for the memfd) — that `queueFd()` adds, so the session's pairing and closing of fds
 * can be asserted without a socket.
 */
final class FakeFdTransport implements FdReceivingTransportInterface
{
    private string $inbound = '';
    private int $pos = 0;
    public string $written = '';
    public bool $closed = false;

    /** @var list<resource> */
    private array $fds = [];
    private int $received = 0;

    public function __construct(private readonly bool $receives = true)
    {
    }

    public function feed(string $bytes): void
    {
        $this->inbound .= $bytes;
    }

    /** @param resource $fd */
    public function queueFd($fd): void
    {
        $this->fds[] = $fd;
        $this->received++;
    }

    public function receivesFds(): bool
    {
        return $this->receives;
    }

    public function takeFd(): mixed
    {
        return array_shift($this->fds);
    }

    public function fdsReceived(): int
    {
        return $this->received;
    }

    /** How many times the session announced a frame start / a closed fd (asserted by the tests). */
    public int $frameStarts = 0;
    public int $fdsClosed = 0;

    public function beginFrame(): void
    {
        $this->frameStarts++;
    }

    public function fdClosed(): void
    {
        $this->fdsClosed++;
    }

    public function readExact(int $n): string
    {
        if ($n === 0) { return ''; }
        if ($this->pos + $n > strlen($this->inbound)) {
            throw new TransportException(sprintf('fake fd transport: need %d bytes at %d', $n, $this->pos));
        }
        $slice = substr($this->inbound, $this->pos, $n);
        $this->pos += $n;
        return $slice;
    }

    public function writeAll(string $bytes): void
    {
        $this->written .= $bytes;
    }

    public function close(): void
    {
        $this->closed = true;
    }
}
