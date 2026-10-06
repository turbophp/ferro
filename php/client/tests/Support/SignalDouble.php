<?php // /php/client/tests/Support/SignalDouble.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Client\DuplexTransportInterface;
use Ferro\Protocol\Header;

/**
 * A duplex double whose READABILITY is a real socket the test signals, while its bytes come from a
 * {@see FakeTransport} (M6-F8 review round 3, adopted from the reviewer's probe). The session asks
 * "is anything readable?" with a zero-timeout select on {@see stream}; {@see signal} makes the
 * answer yes, {@see unsignal} no. {@see $onWrite} runs before each duplex write, with the frame's
 * header and the session's `$onReadable`.
 */
final class SignalDouble implements DuplexTransportInterface
{
    /** @var resource */
    public $r;

    /** @var resource */
    public $w;

    /** @var (\Closure(Header, \Closure(): void): void)|null */
    public ?\Closure $onWrite = null;

    public function __construct(public readonly FakeTransport $inner, private readonly float $readTimeout = 5.0)
    {
        $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        if ($pair === false) {
            throw new \RuntimeException('stream_socket_pair failed');
        }
        [$this->r, $this->w] = $pair;
    }

    public function signal(): void
    {
        fwrite($this->w, 'x');
    }

    public function unsignal(): void
    {
        stream_set_blocking($this->r, false);
        fread($this->r, 64);
        stream_set_blocking($this->r, true);
    }

    public function readExact(int $n): string
    {
        return $this->inner->readExact($n);
    }

    public function writeAll(string $bytes): void
    {
        $this->inner->writeAll($bytes);
    }

    public function writeAllReading(string $bytes, \Closure $onReadable): void
    {
        if ($this->onWrite !== null) {
            ($this->onWrite)(Header::decode(substr($bytes, 0, 16)), $onReadable);
        }
        $this->inner->writeAll($bytes);
    }

    public function close(): void
    {
        $this->inner->close();
    }

    public function stream(): mixed
    {
        return $this->r;
    }

    public function readTimeout(): float
    {
        return $this->readTimeout;
    }

    public function setReadWait(float $seconds): void {}
}
