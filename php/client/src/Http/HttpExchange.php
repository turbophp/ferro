<?php // /php/client/src/Http/HttpExchange.php
declare(strict_types=1);
namespace Ferro\Http;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\Session;
use Ferro\Client\Waiter;
use Ferro\Http\Error\HttpFates;
use Ferro\Loop;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\HttpDone;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * One Ferro HTTP exchange in flight on a {@see Session}: reads its `HEAD`, its `BODY` chunks and its
 * ONE terminal, replenishes its credit window as the caller consumes, and turns every failure into
 * its fate ({@see HttpFates}). {@see HttpResponse} (buffered) and {@see HttpStream} are both built
 * on it, so the two forms cannot classify differently.
 *
 * **Fiber-aware per frame.** Before EVERY read it waits through {@see Loop::waitFor} — which
 * suspends a Fiber {@see Loop} owns (or, with {@see \Ferro\Revolt} installed, one the Revolt loop
 * runs) until that frame has arrived, and returns at once anywhere else. So a long body read under a
 * scheduler lets the other Fibers run between chunks, not only before the head.
 *
 * **Credit.** The engine debits every `HEAD` and `BODY` frame from this request's window (64 frames
 * / 16 MiB) and stops reading the upstream when it is empty (§23.6 step 8). The window is replenished
 * frame for frame, by exactly what was debited: the `HEAD` as soon as it is read, each chunk by
 * {@see ack} once the caller has consumed it. A caller that stops consuming therefore stops the
 * upstream — and holds at most one window of unread body in PHP (§23.9.1).
 *
 * @internal
 */
final class HttpExchange
{
    private ?ResponseHead $head = null;

    /** Whether this exchange is over: its terminal was read, it was abandoned, or it failed. */
    private bool $done = false;

    /** @var ?array{trailers:list<array{0:string,1:string}>,stats:array{queue_us:int,connect_us:int,tls_us:int,ttfb_us:int,total_us:int,bytes_sent:int,bytes_received:int,reused:bool}} */
    private ?array $completed = null;

    public function __construct(
        private readonly Session $session,
        public readonly int $requestId,
        /** The request's OWN `idempotent = true` — all the client may count before `HEAD` (§23.7.3). */
        private readonly bool $declaredIdempotent,
        private readonly PackerInterface $decodePacker,
    ) {}

    /** Read up to the `HEAD`, or throw the fate of an exchange that ended without one. */
    public function awaitHead(): ResponseHead
    {
        if ($this->head !== null) {
            return $this->head;
        }
        $frame = $this->next();
        if ($frame['type'] === 'end') {
            $this->done = true;
            if ($frame['outcome']->isOk()) {
                // The engine always sends a HEAD before an Ok terminal (§23.6 step 7).
                throw new ProtocolException("HTTP request {$this->requestId} completed without a HEAD");
            }
            throw HttpFates::fromOutcome($frame['outcome'], null);
        }
        if ($frame['type'] !== 'head') {
            throw new ProtocolException("HTTP request {$this->requestId}: a BODY before its HEAD"); // unreachable: the session checks order
        }
        $head = ResponseHead::fromWire($frame['head']);
        $this->head = $head;
        $this->replenish($frame['bytes']);
        return $head;
    }

    /**
     * The next body chunk and the bytes its frame debited — pass them to {@see ack} once consumed —
     * or null once the exchange completed. A failure terminal is thrown as its fate, AFTER every
     * chunk that arrived before it.
     *
     * @return ?array{0:string,1:int}
     */
    public function nextChunk(): ?array
    {
        if ($this->done) {
            return null;
        }
        $this->awaitHead();
        $frame = $this->next();
        if ($frame['type'] === 'body') {
            return [$frame['chunk'], $frame['bytes']];
        }
        if ($frame['type'] === 'head') {
            throw new ProtocolException("HTTP request {$this->requestId}: a second HEAD"); // unreachable: the session checks order
        }
        $this->done = true;
        $outcome = $frame['outcome'];
        if (!$outcome->isOk()) {
            throw HttpFates::fromOutcome($outcome, $this->head);
        }
        try {
            $this->completed = HttpDone::decode($outcome->body(), $this->decodePacker);
        } catch (CodecException $e) {
            throw new ProtocolException('malformed HttpDone terminal: ' . $e->getMessage(), 0, $e);
        }
        return null;
    }

    /** Replenish the window by one consumed chunk's frame. */
    public function ack(int $bytes): void
    {
        if (!$this->done) {
            $this->replenish($bytes);
        }
    }

    /** @return ?list<array{0:string,1:string}> the trailers, once the exchange completed */
    public function trailers(): ?array
    {
        return $this->completed['trailers'] ?? null;
    }

    /** @return ?array{queue_us:int,connect_us:int,tls_us:int,ttfb_us:int,total_us:int,bytes_sent:int,bytes_received:int,reused:bool} */
    public function stats(): ?array
    {
        return $this->completed['stats'] ?? null;
    }

    /**
     * The decoded `HttpDone` of an exchange read to its Ok terminal.
     *
     * @return array{trailers:list<array{0:string,1:string}>,stats:array{queue_us:int,connect_us:int,tls_us:int,ttfb_us:int,total_us:int,bytes_sent:int,bytes_received:int,reused:bool}}
     * @throws \LogicException before {@see nextChunk} has returned null
     */
    public function completed(): array
    {
        return $this->completed ?? throw new \LogicException("HTTP request {$this->requestId} has not completed");
    }

    public function isDone(): bool
    {
        return $this->done;
    }

    public function isComplete(): bool
    {
        return $this->completed !== null;
    }

    /**
     * Stop the exchange: `CANCEL`, then read and discard to its ONE terminal ({@see Session::abandonHttp}).
     * A no-op once it is over. Never throws: it runs from `finally` blocks and `close()` calls that
     * may carry the real error, and a session that fails here has closed itself — the exchange is
     * over either way.
     */
    public function abandon(): void
    {
        if ($this->done) {
            return;
        }
        $this->done = true;
        try {
            $this->session->abandonHttp($this->requestId);
        } catch (FerroException) {
            // The session poisoned itself; nothing of this exchange can arrive any more.
        }
    }

    /** As {@see abandon}, without waiting for the terminal — for a destructor. */
    public function abandonNow(): void
    {
        if ($this->done) {
            return;
        }
        $this->done = true;
        $this->session->cancelAndDiscard($this->requestId);
    }

    /**
     * @return array{type:'head', head:array{status:int,version:int,reason:?string,headers:list<array{0:string,1:string}>,decoded:?array{0:string,1:?int},idempotent:bool}, bytes:int}
     *       | array{type:'body', chunk:string, bytes:int}
     *       | array{type:'end', outcome:\Ferro\Protocol\Outcome}
     */
    private function next(): array
    {
        Loop::waitFor(new Waiter($this->session, $this->requestId));
        try {
            return $this->session->readHttpFrame($this->requestId);
        } catch (TransportException | ConnectionLostException $e) {
            $this->done = true;
            throw $this->lost($e);
        } catch (ProtocolException $e) {
            $this->done = true;
            throw $e;
        }
    }

    private function replenish(int $bytes): void
    {
        try {
            $this->session->sendWindowUpdate($this->requestId, 1, $bytes);
        } catch (TransportException $e) {
            // A control frame: the request itself was sent long ago, so this is a sent-and-lost.
            $this->done = true;
            throw $this->lost($e);
        }
    }

    private function lost(TransportException|ConnectionLostException $e): FerroException
    {
        $sent = !($e instanceof TransportException && $e->requestUnsent());
        return HttpFates::linkLost($sent, $this->declaredIdempotent, $this->head, $e->getMessage());
    }
}
