<?php // /php/client/src/Http/HttpStream.php
declare(strict_types=1);
namespace Ferro\Http;

/**
 * A Ferro HTTP response whose HEAD has arrived and whose body is read LAZILY ({@see Upstream::stream},
 * SPEC §23.11.1):
 *
 *     $s = $http->stream('POST', '/v1/chat/completions', body: $json);
 *     foreach ($s as $chunk) { … }   // one BODY chunk (≤ 256 KiB) per iteration
 *
 * **Never buffers.** Each chunk is read off the session when the loop asks for it, and its credit
 * is returned (`WINDOW_UPDATE`) only once the loop has taken it — so a consumer that stops pulling
 * stops the engine reading the upstream, and holds at most one credit window (64 frames / 16 MiB)
 * of unread body in PHP (§23.9.1). That bound is per open stream: N streams open at once and not
 * consumed hold up to N × 16 MiB.
 *
 * **Abandonment.** Leaving the loop early (`break`, an exception, the generator destroyed) or
 * calling {@see close} sends `CANCEL` and reads the exchange to its ONE terminal, so the engine
 * stops the exchange and frees its upstream connection (the `RawStream` contract). A stream dropped
 * without either is CANCELled without waiting, and its frames are discarded as they arrive.
 *
 * **Not exclusive.** Unlike a SQL stream, an open HTTP stream does not stop the session carrying
 * other requests — SQL statements, other HTTP requests, other HTTP streams — at the same time
 * (§23.11.1 lifts (cj)'s exclusivity for HTTP).
 *
 * A body failure after the head is thrown from the iteration as its fate, after every chunk that
 * arrived before it (a {@see Error\ResponseIncompleteException} for a non-idempotent request).
 *
 * @implements \IteratorAggregate<int, string>
 */
final class HttpStream implements \IteratorAggregate
{
    public readonly int $status;
    public readonly int $version;
    public readonly ?string $reason;
    /** @var array<string, list<string>> */
    public readonly array $headers;
    /** @var list<array{0:string,1:string}> */
    public readonly array $headerLines;
    /** @var ?array{0:string,1:?int} */
    public readonly ?array $decoded;
    public readonly bool $idempotent;

    private bool $iterated = false;

    /** @internal built by {@see Upstream::stream} once the HEAD has arrived */
    public function __construct(
        public readonly ResponseHead $head,
        private readonly HttpExchange $exchange,
    ) {
        $this->status = $head->status;
        $this->version = $head->version;
        $this->reason = $head->reason;
        $this->headers = $head->headers;
        $this->headerLines = $head->headerLines;
        $this->decoded = $head->decoded;
        $this->idempotent = $head->idempotent;
    }

    public function __destruct()
    {
        $this->exchange->abandonNow();
    }

    /**
     * The body, one chunk per iteration. Single pass: a second iteration is refused, because the
     * chunks already read are gone.
     *
     * @return \Generator<int, string>
     */
    public function getIterator(): \Generator
    {
        if ($this->iterated) {
            throw new \LogicException('an HttpStream body can be iterated only once');
        }
        if ($this->exchange->isDone() && !$this->exchange->isComplete()) {
            // Closed before it was read: an empty loop here would pass for an empty body.
            throw new \LogicException('HttpStream iterated after close()');
        }
        $this->iterated = true;
        try {
            while (($chunk = $this->exchange->nextChunk()) !== null) {
                yield $chunk[0];
                // The caller has taken the chunk: only now is its credit returned.
                $this->exchange->ack($chunk[1]);
            }
        } finally {
            $this->exchange->abandon(); // a no-op once the terminal was read
        }
    }

    /** Read the rest of the body into one string (the buffered form, for a stream already open). */
    public function body(): string
    {
        $body = '';
        foreach ($this as $chunk) {
            $body .= $chunk;
        }
        return $body;
    }

    /** `CANCEL` and drain to the terminal. Idempotent; a no-op once the body was read to its end. */
    public function close(): void
    {
        $this->exchange->abandon();
    }

    /** Whether the body was read to a successful terminal. */
    public function isComplete(): bool
    {
        return $this->exchange->isComplete();
    }

    /** Whether the exchange is over (completed, failed, or closed). */
    public function isClosed(): bool
    {
        return $this->exchange->isDone();
    }

    /** @return ?list<array{0:string,1:string}> the trailers, once {@see isComplete} */
    public function trailers(): ?array
    {
        return $this->exchange->trailers();
    }

    /** @return ?array{queue_us:int,connect_us:int,tls_us:int,ttfb_us:int,total_us:int,bytes_sent:int,bytes_received:int,reused:bool} */
    public function stats(): ?array
    {
        return $this->exchange->stats();
    }

    /** The first value of header `$name` (case-insensitive), or null. */
    public function header(string $name): ?string
    {
        return $this->head->header($name);
    }

    /** {@see StatusFate}'s advisory verdict on this status. */
    public function statusFate(?float $now = null): StatusFate
    {
        return $this->head->statusFate($now);
    }
}
