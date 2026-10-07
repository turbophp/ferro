<?php // /php/psr18/src/Http/Adapter/BodyStream.php
declare(strict_types=1);
namespace Ferro\Http\Adapter;

use Ferro\Http\Exception\BodyReadException;
use Ferro\Http\HttpStream;
use Psr\Http\Message\StreamInterface;

/**
 * A LAZY, read-once PSR-7 body over a Ferro HTTP stream (SPEC §23.11.2 `stream => true`,
 * §23.11.5): Guzzle's streamed response body and every `ferro/psr18` response body.
 *
 * **It never buffers more than one `BODY` chunk** (≤ 256 KiB). A chunk is pulled off the session only
 * when a `read()` needs bytes, and its credit returns to the engine (`WINDOW_UPDATE`) only when the
 * next one is pulled — so a consumer that stops reading stops the upstream, and PHP holds at most
 * one credit window per open body (§23.9.1). An SSE or LLM stream is therefore delivered as it
 * arrives.
 *
 * **Abandonment.** {@see close()} (and {@see detach()}) send `CANCEL` and drain the exchange to its
 * one terminal, so the engine stops reading the upstream and frees its connection; dropping the
 * body unread CANCELs without waiting (the native stream's destructor). Reading to the end needs
 * neither.
 *
 * **A failure after the head** is thrown from `read()` as a {@see BodyReadException} — a
 * `\RuntimeException` carrying the fate marker — after every byte that arrived before it
 * (§23.11.3).
 *
 * Not seekable and not writable. `getSize()` is null: a decoded or chunked body has no length the
 * head can promise.
 *
 * @internal shared by `ferro/guzzle` and `ferro/psr18`
 */
final class BodyStream implements StreamInterface
{
    private string $buffer = '';
    private int $position = 0;
    private bool $ended = false;
    private bool $closed = false;
    /** @var ?\Generator<int, string> */
    private ?\Generator $chunks = null;

    /**
     * @param ?\Closure(int): void $onBytes called with the total body bytes read so far, after each chunk
     * @param ?\Closure(?\Throwable): void $onEnd called once, when the body completed (null) or failed
     */
    public function __construct(
        private readonly HttpStream $stream,
        private readonly ?\Closure $onBytes = null,
        private readonly ?\Closure $onEnd = null,
    ) {}

    public function __toString(): string
    {
        return $this->getContents();
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        $this->buffer = '';
        $wasEnded = $this->ended;
        $this->ended = true;
        $this->stream->close(); // CANCEL + drain; a no-op once the body was read to its end
        if (!$wasEnded) {
            $this->end(null);
        }
    }

    public function detach()
    {
        $this->close();
        return null;
    }

    public function getSize(): ?int
    {
        return null;
    }

    public function tell(): int
    {
        return $this->position;
    }

    public function eof(): bool
    {
        return $this->ended && $this->buffer === '';
    }

    public function isSeekable(): bool
    {
        return false;
    }

    public function seek(int $offset, int $whence = SEEK_SET): void
    {
        throw new \RuntimeException('a Ferro HTTP response body is not seekable');
    }

    public function rewind(): void
    {
        throw new \RuntimeException('a Ferro HTTP response body is not seekable');
    }

    public function isWritable(): bool
    {
        return false;
    }

    public function write(string $string): int
    {
        throw new \RuntimeException('a Ferro HTTP response body is not writable');
    }

    public function isReadable(): bool
    {
        return !$this->closed;
    }

    public function read(int $length): string
    {
        if ($this->closed) {
            throw new \RuntimeException('the Ferro HTTP response body was closed');
        }
        if ($length < 1) {
            return '';
        }
        while ($this->buffer === '' && !$this->ended) {
            $this->pull();
        }
        $out = substr($this->buffer, 0, $length);
        $this->buffer = (string) substr($this->buffer, strlen($out));
        $this->position += strlen($out);
        return $out;
    }

    public function getContents(): string
    {
        $out = '';
        while (!$this->eof()) {
            $out .= $this->read(1 << 20);
        }
        return $out;
    }

    /** @return array<string, mixed>|mixed|null */
    public function getMetadata(?string $key = null)
    {
        return $key === null ? [] : null;
    }

    /** The native stream this body reads (its head, trailers and stats). */
    public function httpStream(): HttpStream
    {
        return $this->stream;
    }

    private function pull(): void
    {
        try {
            if ($this->chunks === null) {
                $this->chunks = $this->stream->getIterator();
                $this->chunks->current(); // starts the generator: reads the first chunk (or the end)
            } else {
                $this->chunks->next(); // returns the previous chunk's credit, then reads the next
            }
            if (!$this->chunks->valid()) {
                $this->ended = true;
                $this->end(null);
                return;
            }
            $this->buffer .= $this->chunks->current();
            if ($this->onBytes !== null) {
                ($this->onBytes)($this->position + strlen($this->buffer));
            }
        } catch (\Throwable $e) {
            $this->ended = true;
            $failure = Failure::of($e, $this->stream->head);
            $error = BodyReadException::create(
                'the Ferro HTTP response body failed after its head: ' . $e->getMessage(),
                $failure->fate,
                $e,
            );
            $this->end($error);
            throw $error;
        }
    }

    private function end(?\Throwable $error): void
    {
        if ($this->onEnd !== null) {
            $onEnd = $this->onEnd;
            ($onEnd)($error);
        }
    }
}
