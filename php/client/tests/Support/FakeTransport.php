<?php // /php/client/tests/Support/FakeTransport.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Client\Error\TransportException;
use Ferro\Client\TransportInterface;

/**
 * In-memory {@see TransportInterface} for session unit tests: `feed()` queues inbound bytes the
 * session will read back exactly (via `readExact`), and every byte the session writes is captured
 * in `$written`. No socket, no ferrod.
 */
final class FakeTransport implements TransportInterface
{
    private string $inbound = '';
    private int $pos = 0;
    public string $written = '';
    public bool $closed = false;

    public function feed(string $bytes): void
    {
        $this->inbound .= $bytes;
    }

    public function readExact(int $n): string
    {
        if ($n === 0) { return ''; }
        if ($this->pos + $n > strlen($this->inbound)) {
            throw new TransportException(sprintf(
                'fake transport: need %d bytes at %d, only %d queued',
                $n,
                $this->pos,
                strlen($this->inbound) - $this->pos,
            ));
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
