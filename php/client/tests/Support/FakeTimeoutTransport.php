<?php // /php/client/tests/Support/FakeTimeoutTransport.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Client\Error\TransportException;
use Ferro\Client\SelectableTransportInterface;

/**
 * A selectable fake whose reads TIME OUT instead of hitting EOF when nothing is queued (M3-D1c), so
 * a test can drive silence, liveness probes, a timeout in the middle of a frame, and deadlines
 * without a socket or a clock. A timed-out read consumes NOTHING — exactly the real
 * {@see \Ferro\Client\Transport}'s contract, which keeps the bytes it had read for the next call.
 *
 * `$onTimeout` runs on every timeout and may `feed()` more bytes or set `$eof`, which is how a test
 * makes the "engine" answer only after the session has done something (a PING, a CANCEL).
 */
final class FakeTimeoutTransport implements SelectableTransportInterface
{
    private string $inbound = '';
    public string $written = '';
    public bool $eof = false;
    public int $timeouts = 0;

    /** @var list<float> every wait the session asked for, in order */
    public array $waits = [];

    /** @var (\Closure(self): void)|null */
    public ?\Closure $onTimeout = null;

    public function __construct(private readonly float $readTimeout = 0.05) {}

    public function feed(string $bytes): void
    {
        $this->inbound .= $bytes;
    }

    public function readExact(int $n): string
    {
        if ($n === 0) {
            return '';
        }
        if (strlen($this->inbound) >= $n) {
            $slice = substr($this->inbound, 0, $n);
            $this->inbound = substr($this->inbound, $n);
            return $slice;
        }
        if ($this->eof) {
            throw new TransportException('fake: EOF');
        }
        ++$this->timeouts;
        if ($this->onTimeout !== null) {
            ($this->onTimeout)($this);
        }
        throw TransportException::readTimedOut(sprintf('fake: read timed out (%d of %d bytes)', strlen($this->inbound), $n));
    }

    /** When set, every write fails as a broken pipe would (nothing written). */
    public bool $failWrites = false;

    public function writeAll(string $bytes): void
    {
        if ($this->failWrites) {
            throw new TransportException('fake: write failed after 0 of ' . strlen($bytes) . ' bytes');
        }
        $this->written .= $bytes;
    }

    public function close(): void
    {
        $this->eof = true;
    }

    public function stream(): mixed
    {
        return null;
    }

    public function readTimeout(): float
    {
        return $this->readTimeout;
    }

    public function setReadWait(float $seconds): void
    {
        $this->waits[] = $seconds;
    }
}
