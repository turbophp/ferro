<?php // /php/client/tests/Support/DuplexDouble.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Client\DuplexTransportInterface;
use Ferro\Protocol\Header;

/**
 * A {@see DuplexTransportInterface} over a {@see FakeTransport}, for driving the session's
 * read-during-write path on cue (M6-F8 review round 2): what a real socket does when it will not
 * take a frame's bytes and the engine has some for us.
 *
 *  - {@see $onWrite} runs for every frame written through the duplex path, with the frame's header
 *    and the session's `$onReadable`. It runs after the first `$splitAt` bytes of the frame are
 *    written (0 = before any), so a hook can model a write blocked part-way through a frame.
 *  - {@see $onPayloadRead} runs once, after the next payload read (a destructor run by the cycle
 *    collector in the middle of a read), then disarms.
 *
 * Not selectable: {@see stream} is null, so the session reads it as having nothing readable.
 */
final class DuplexDouble implements DuplexTransportInterface
{
    /** @var (\Closure(Header, \Closure(): void): void)|null */
    public ?\Closure $onWrite = null;

    /** @var (\Closure(): void)|null */
    public ?\Closure $onPayloadRead = null;

    /** How many bytes of a frame are written before {@see $onWrite} runs. */
    public int $splitAt = 0;

    /** Reads so far (each `readExact` call). */
    public int $reads = 0;

    public function __construct(public readonly FakeTransport $inner, private readonly float $readTimeout = 5.0) {}

    public function readExact(int $n): string
    {
        ++$this->reads;
        $bytes = $this->inner->readExact($n);
        if ($n !== 16 && $this->onPayloadRead !== null) {
            $hook = $this->onPayloadRead;
            $this->onPayloadRead = null;
            $hook();
        }
        return $bytes;
    }

    public function writeAll(string $bytes): void
    {
        $this->inner->writeAll($bytes);
    }

    public function writeAllReading(string $bytes, \Closure $onReadable): void
    {
        if ($this->onWrite === null) {
            $this->inner->writeAll($bytes);
            return;
        }
        $split = min($this->splitAt, strlen($bytes));
        if ($split > 0) {
            $this->inner->writeAll(substr($bytes, 0, $split));
        }
        ($this->onWrite)(Header::decode(substr($bytes, 0, 16)), $onReadable);
        if ($split < strlen($bytes)) {
            $this->inner->writeAll(substr($bytes, $split));
        }
    }

    public function close(): void
    {
        $this->inner->close();
    }

    public function stream(): mixed
    {
        return null;
    }

    public function readTimeout(): float
    {
        return $this->readTimeout;
    }

    public function setReadWait(float $seconds): void {}

    /**
     * Decode a run of written frames, stopping at the first header that does not decode — which
     * is what the engine would make of a frame spliced into another.
     *
     * @return list<string> `service/method/rid/len` per frame, or `UNDECODABLE@offset`
     */
    public static function frames(string $written): array
    {
        $off = 0;
        $out = [];
        while ($off + 16 <= strlen($written)) {
            try {
                $h = Header::decode(substr($written, $off, 16));
            } catch (\Throwable) {
                $out[] = 'UNDECODABLE@' . $off;
                break;
            }
            $out[] = "{$h->service}/{$h->method}/{$h->requestId}/{$h->payloadLen}";
            $off += 16 + $h->payloadLen;
        }
        return $out;
    }
}
