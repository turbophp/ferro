<?php // /php/client/src/Client/Session.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\HandshakeException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TransportException;
use Ferro\Protocol\Codec;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Hello;
use Ferro\Protocol\HelloAck;
use Ferro\Protocol\Message;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\PoolInfo;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\OobRef;
use Ferro\Protocol\StreamData;
use Ferro\Protocol\StreamHead;

/**
 * The session over a {@see TransportInterface}: HELLO/HELLO_ACK, request -> exactly one terminal
 * `END`, PING/PONG liveness, GOODBYE on close.
 *
 * **Multiplexed (M3-D1, SPEC §10.1).** Several requests may be in flight on the one socket at once:
 * {@see submit} writes a request frame and returns its `request_id` without reading, and
 * {@see awaitTerminal} reads until THAT id's terminal arrives. Every frame read off the wire goes
 * through ONE router ({@see pump}), which files it under its `request_id`, so a terminal for one
 * request can arrive while another is being awaited and is kept, not misread. The engine has always
 * run each request on its own task (`ferrod` `session::mod`), so k requests submitted before any is
 * awaited cost about max(query) rather than the sum. {@see sendRequest} is `submit` +
 * `awaitTerminal`, so the synchronous surface every tier uses is unchanged.
 *
 * Three rules keep the routing safe:
 *  - a frame for an id this session has not got in flight is a desync, so it poisons the session;
 *  - a session-fatal terminal on `request_id=0` does NOT decide any pending request's fate. With
 *    several requests in flight it may concern any one of them, or a control frame, so its payload
 *    is never attributed. The session stops sending and keeps READING until EOF, because the engine
 *    drains every in-flight request's own terminal before it closes (`ferrod` `session::mod`:
 *    `cancel_all` + `drain_supervisors`). A request still unanswered at EOF fails with a
 *    {@see ConnectionLostException} carrying NO server payload, so the caller's classifier decides
 *    (a lost write is `Indeterminate`);
 *  - a transport failure fails every pending request as SENT (each frame was completely written),
 *    so its fate stays the caller's to classify. A request that never got written is
 *    {@see TransportException::requestNotSent}, including one refused because the session had
 *    already failed while it waited for an in-flight slot.
 *
 * Open streams are still exclusive: no request may be submitted while a stream is open on this
 * session (see {@see $streamOpen}).
 *
 * Handshake branching (SPEC §5): after sending HELLO the session reads ONE reply frame and routes
 * it by shape, NEVER by comparing hashes client-side (the registry check is SERVER-side and can
 * only ever fire there):
 *   - `CORE/HELLO_ACK` (flags=0, not END) -> decode {@see HelloAck}, cache `boot_epoch` + `pools`;
 *   - an `Outcome::Error` terminal on `request_id=0` with `flags::END`
 *     (`service=CORE, method=0` — `error.rs terminal_frame(0, ep)`) -> throw the FATAL
 *     {@see HandshakeException} (keyed on `ERR_UNSUPPORTED` for the registry/version mismatch).
 *
 * `boot_epoch` is stored OPAQUE (`int|string`) exactly as the packer yields it — never coerced, so
 * the Task-4 reconnect loop can detect an epoch change even for `u64 > PHP_INT_MAX` values.
 */
final class Session implements MultiplexingSessionInterface, StreamingSessionInterface
{
    private readonly Codec $codec;
    private readonly PackerInterface $encodePacker;
    private readonly PackerInterface $decodePacker;
    private readonly RequestIdAllocator $ids;

    /** Cached from HELLO_ACK; OPAQUE — int, or a decimal string for a uint64-encoded epoch. */
    private int|string|null $bootEpoch = null;
    /** @var list<PoolInfo> cached pool metadata from HELLO_ACK (M1-S8a: name + family + version) */
    private array $poolInfo = [];
    private bool $handshakeDone = false;

    /**
     * The `(service, method)` of the last request frame put on the wire, exposed through
     * {@see lastInFlight}. Diagnostic only: the fate rules do not read it — each call site passes its
     * own `OpKind` to {@see FateClassifier::classifyLoss}, which is what decides a lost COMMIT.
     *
     * @var array{0:int,1:int}|null
     */
    private ?array $lastInFlight = null;

    /**
     * Set between a successful {@see openStream} and the streamed read reaching its terminal (or
     * being {@see abandonStream}-ed). While set, {@see submit} refuses. The router would keep a
     * stream's frames apart from a buffered request's, so this is no longer about desync (M3-D1a):
     * a buffered statement on the same `tx_id` would queue in the engine behind a stream stalled on
     * its credit window, which nobody replenishes while that statement is awaited — a deadlock.
     */
    private bool $streamOpen = false;
    private ?int $streamRequestId = null;
    /**
     * The Fiber that opened the stream; null is the main program.
     *
     * @var \Fiber<mixed, mixed, mixed, mixed>|null
     */
    private ?\Fiber $streamFiber = null;

    /**
     * Set by the first transport failure; the session is unusable from then on (M2-C1e-3).
     *
     * After a failure the framing is unknown in BOTH directions: a failed write may have left a
     * PARTIAL frame on the wire, which the next frame's bytes would complete — so the engine could
     * decode a request built from two of them — and a failed read leaves an unknown amount unread,
     * which the next request would read as its own reply. So the first failure closes the socket
     * (the engine sees EOF and discards any partial frame) and every later frame is refused BEFORE
     * a byte is written, as {@see TransportException::requestNotSent}. That refusal is what makes
     * "an unsent request cannot have executed" true rather than hopeful.
     */
    private ?string $poisoned = null;

    /** Results received through a sealed memfd ({@see oobPayloadsReceived}, M3-D3). */
    private int $oobPayloads = 0;

    /**
     * Ids whose request frame was written and whose final frame (a terminal, or a PONG) has not yet
     * been read off the wire. Its size is what {@see $maxInFlight} bounds.
     *
     * @var array<int, true>
     */
    private array $inFlight = [];

    /**
     * Frames read off the wire, filed under their `request_id`, not yet consumed by whoever awaits
     * that id. A stream's DATA frames wait here while another request is awaited; the credit window
     * bounds how many there can be.
     *
     * @var array<int, list<array{0:Header,1:string}>>
     */
    private array $inbox = [];

    /**
     * The message of a session-fatal `request_id=0` terminal, once one has arrived. From then on the
     * session sends nothing and only reads, to collect the terminals the engine drains. Its payload
     * is deliberately not kept for the pending requests (see the class docblock).
     */
    private ?string $fatal = null;

    /**
     * Ids whose {@see \Ferro\Future} was dropped unawaited ({@see discard}). Their frames are thrown
     * away on arrival, so an abandoned Future cannot grow {@see $inbox} for the session's life.
     *
     * @var array<int, true>
     */
    private array $discarded = [];

    /**
     * M3-D1c liveness: the id of the PING sent when a read waited its whole timeout in silence, and
     * when it was sent. Silence is not failure — a statement may simply take longer than the read
     * timeout — so the session asks the engine whether it is still there instead of giving up. Only
     * a second full timeout with no reply to that PING closes the session.
     */
    private ?int $probeRid = null;

    private float $probeSentAt = 0.0;

    /** When the last whole frame was read off the wire (M3-D1c review F6), for {@see onSilence}. */
    private float $lastFrameAt = 0.0;

    /**
     * M3-D1c per-request deadlines, as absolute `microtime(true)` values ({@see setDeadline}). When
     * one passes, the request is CANCELled ({@see enforceDeadlines}) and its deadline moves out by
     * one read timeout of grace for the engine's terminal; if that passes too the session closes.
     *
     * @var array<int, float>
     */
    private array $deadlines = [];

    /** @var array<int, true> requests already CANCELled for their deadline */
    private array $deadlineCancelled = [];

    /**
     * The header of a frame whose payload read timed out (M3-D1c): the next read resumes with its
     * payload, so a timeout between the two reads never puts the stream out of step.
     */
    private ?Header $partialHeader = null;

    /** See {@see setRequestTimeout}. */
    private ?float $requestTimeout = null;

    /**
     * The scheduler watching this session, if any ({@see observe}, M3-D1d).
     *
     * @var (\Closure(?int): void)|null
     */
    private ?\Closure $observer = null;

    /**
     * @param int $maxInFlight the most requests this session keeps in flight at once. At the limit,
     *                         {@see submit} reads frames until a terminal frees a slot before it
     *                         writes. It must stay below the engine's own per-session limit
     *                         (`max_inflight`, default 1024), which answers an excess request with
     *                         an error instead of running it.
     */
    public function __construct(
        private readonly TransportInterface $transport,
        ?RequestIdAllocator $ids = null,
        ?Codec $codec = null,
        ?PackerInterface $encodePacker = null,
        ?PackerInterface $decodePacker = null,
        private readonly int $maxInFlight = 256,
    ) {
        if ($maxInFlight < 1) {
            throw new \InvalidArgumentException("maxInFlight must be >= 1, got {$maxInFlight}");
        }
        $this->ids = $ids ?? new RequestIdAllocator();
        $this->codec = $codec ?? new Codec();
        $this->encodePacker = $encodePacker ?? PackerFactory::forEncode();
        $this->decodePacker = $decodePacker ?? PackerFactory::forDecode();
    }

    /**
     * Send HELLO and process the single reply. On success caches `boot_epoch` + `pools` and returns
     * the decoded {@see HelloAck}; on a session-fatal handshake rejection throws
     * {@see HandshakeException}.
     */
    public function hello(?string $manifestHash = null): HelloAck
    {
        $hello = new Hello(
            clientVersion: 1,
            typeRegistryHash: C::TYPE_REGISTRY_HASH,
            // M3-D2e: the manifest this client was built against, or none. The engine refuses the
            // handshake if it differs from its own, and runs a query by id only on a session that
            // sent the matching hash.
            manifestHash: $manifestHash,
            pid: getmypid() ?: 0,
            // Informational: this client can multiplex requests over the session (M3-D1). The engine
            // has always served requests concurrently and does not read this bit. `MEMFD_RX`
            // (M3-D3) is NOT informational: it licenses the engine to send a large result as a
            // sealed memfd, so it is set only when this transport actually reads with `recvmsg`.
            features: C::FEATURE_CLIENT_FIBERS | ($this->receivesFds() ? C::FEATURE_CLIENT_MEMFD_RX : 0),
        );
        $payload = $hello->encode($this->encodePacker);
        $this->writeFrame(0, C::SERVICE_CORE, C::METHOD_CORE_HELLO, $payload, 0, true);

        [$header, $body] = $this->readFrame();
        $isEnd = ($header->flags & C::FLAG_END) !== 0;

        // The HELLO_ACK branch: a non-terminal CORE control frame (flags=0, NOT END).
        if ($header->service === C::SERVICE_CORE && $header->method === C::METHOD_CORE_HELLO_ACK) {
            if ($isEnd) {
                throw new ProtocolException('HELLO_ACK must not carry the END flag');
            }
            $ack = HelloAck::decode($body, $this->decodePacker);
            $this->bootEpoch = $ack->bootEpoch;
            $this->poolInfo = $ack->pools;
            $this->handshakeDone = true;
            return $ack;
        }

        // The rejection branch: a session-fatal terminal on request_id=0 (service=CORE, method=0,
        // END) carrying an Outcome::Error — the shape `validate_hello` emits on registry/version
        // mismatch. Do NOT compare the reply's request_id to the HELLO id.
        if ($header->requestId === 0 && $isEnd) {
            $outcome = Outcome::decode($body, $this->decodePacker);
            if ($outcome->isError()) {
                throw new HandshakeException($outcome->errorPayload());
            }
            throw new ProtocolException('handshake terminal on request_id=0 was not an Outcome::Error');
        }

        throw new ProtocolException(sprintf(
            'unexpected handshake reply: service=%d method=%d flags=%d request_id=%d',
            $header->service,
            $header->method,
            $header->flags,
            $header->requestId,
        ));
    }

    /**
     * Send one request-bearing frame (SQL/TX) and block-read its single terminal.
     *
     * Terminal scoping (charter rule 4): a terminal on `request_id=0` with `flags::END` is a
     * SESSION-FATAL signal (`Fatal` `SessionError`) -> surfaced as {@see ConnectionLostException}
     * carrying the decoded `Outcome::Error`, NOT a generic id-mismatch. Otherwise the terminal MUST
     * carry `flags::END` and echo the sent `request_id`; anything else is a {@see ProtocolException}.
     */
    public function sendRequest(int $service, int $method, string $payload): Outcome
    {
        $rid = $this->submit($service, $method, $payload);
        if ($service === C::SERVICE_SQL && $method === C::METHOD_SQL_EXEC) {
            $this->armRequestTimeout($rid);
        }
        return $this->awaitTerminal($rid);
    }

    /**
     * Give every buffered SQL EXEC a deadline this long after it is sent (M3-D1c), or none. Set by
     * `Ferro::connect(statementTimeout: …)` as the statement timeout plus a margin: the engine
     * enforces the statement timeout itself (`timeout_ms`, which bounds the wait for a pooled
     * connection as well as the statement) and answers; this is the client's backstop for an engine
     * that does not.
     *
     * **Only a buffered SQL EXEC is given one** (M3-D1c review F1), because only an EXEC carries
     * `timeout_ms` and has a handler that acts on a CANCEL. Transaction control (BEGIN, COMMIT,
     * ROLLBACK, savepoints) and admin requests carry no timeout and their handlers ignore a CANCEL,
     * so a backstop there could only end in closing the session — and a slow COMMIT that then
     * committed would have been reported `Indeterminate` with every other request on the socket.
     * Nor is a stream (review F5): it is not sent `timeout_ms` either, and is bounded only by the
     * transport's liveness rule while the caller consumes it.
     */
    public function setRequestTimeout(?float $seconds): void
    {
        $this->requestTimeout = $seconds;
    }

    /**
     * Arm {@see setRequestTimeout}'s deadline on a just-submitted request, if one is configured.
     * The caller vouches that the request is a buffered SQL EXEC (see {@see setRequestTimeout}).
     */
    public function armRequestTimeout(int $requestId): void
    {
        if ($this->requestTimeout !== null) {
            $this->setDeadline($requestId, microtime(true) + $this->requestTimeout);
        }
    }

    /**
     * Write one request-bearing frame and return its `request_id` WITHOUT reading its terminal
     * (M3-D1). The terminal is read by {@see awaitTerminal}, in any order relative to other
     * submitted requests.
     *
     * At {@see $maxInFlight} this reads frames first, until one request's terminal frees a slot.
     * A failure to write is {@see TransportException::requestNotSent}, exactly as for
     * {@see sendRequest}.
     */
    public function submit(int $service, int $method, string $payload): int
    {
        $this->assertNoOpenStream();
        $this->refuseIfDead(true);
        while (count($this->inFlight) >= $this->maxInFlight) {
            // This request has not been written: whatever goes wrong while waiting for a slot, it
            // cannot have executed, so every failure here is `requestNotSent` (the C1e-3 rule).
            try {
                $this->enforceDeadlines();
                if ($this->poisoned === null) {
                    $this->pump($this->nearestDeadline());
                }
            } catch (DeadlineSignal) {
                continue; // another request's deadline: acted on at the top of the next pass
            } catch (TransportException $e) {
                throw $e->requestUnsent() ? $e : TransportException::requestNotSent(
                    'not sent: the session failed while this request waited for an in-flight slot ('
                        . $e->getMessage() . ')',
                    $e,
                );
            } catch (ProtocolException $e) {
                throw TransportException::requestNotSent(
                    'not sent: the session desynchronised while this request waited for an in-flight slot ('
                        . $e->getMessage() . ')',
                    $e,
                );
            }
            $this->refuseIfDead(true);
        }
        $rid = $this->nextFreeId();
        // Record BEFORE the write, so it names the request even when the write itself dies.
        $this->lastInFlight = [$service, $method];
        $this->writeFrame(0, $service, $method, $payload, $rid, true);
        $this->inFlight[$rid] = true;
        return $rid;
    }

    /**
     * Read until the terminal for `$requestId` (a previously {@see submit}-ted id) arrives, and
     * return it. Frames for other ids that arrive meanwhile are kept for their own awaiters.
     *
     * A non-terminal frame for this id is a {@see ProtocolException}: only a stream's frames are
     * non-terminal, and a stream is read through {@see readStreamFrame}.
     */
    public function awaitTerminal(int $requestId): Outcome
    {
        [$header, $body] = $this->nextFrameFor($requestId);
        if (($header->flags & C::FLAG_END) === 0) {
            // Only a stream's frames are non-terminal, and this id was not opened as a stream: the
            // two ends disagree about what this request is, so its remaining frames cannot be read
            // safely, and neither can anyone else's.
            $error = new ProtocolException(sprintf('terminal for request %d did not carry the END flag', $requestId));
            $this->poison(new TransportException($error->getMessage()));
            throw $error;
        }
        return Outcome::decode($body, $this->decodePacker);
    }

    /**
     * Give up on a submitted request's terminal: it will be read and thrown away when it arrives
     * (M3-D1a review F6). Called when a {@see \Ferro\Future} is dropped without being awaited, so
     * its terminal does not sit in {@see $inbox} for the session's life. The request's fate is then
     * never observed, which is the caller's choice.
     */
    public function discard(int $requestId): void
    {
        unset($this->inbox[$requestId], $this->deadlines[$requestId], $this->deadlineCancelled[$requestId]);
        if (isset($this->inFlight[$requestId])) {
            $this->discarded[$requestId] = true;
        }
    }

    /**
     * Whether awaiting `$requestId` now would NOT need to read the wire (M3-D1b): its next frame has
     * arrived, or the session has failed so awaiting it fails at once. A scheduler resumes a Fiber
     * waiting on this id only once it is ready.
     */
    public function isReady(int $requestId): bool
    {
        return ($this->inbox[$requestId] ?? []) !== []
            || !isset($this->inFlight[$requestId])
            || $this->poisoned !== null;
    }

    /**
     * The stream a scheduler can select on, or null when the transport cannot be selected (a test
     * double) or the session is closed — the scheduler then reads with {@see pollOnce} directly.
     *
     * @return resource|null
     */
    public function selectableStream(): mixed
    {
        if ($this->poisoned !== null || !$this->transport instanceof SelectableTransportInterface) {
            return null;
        }
        return $this->transport->stream();
    }

    /** The transport's read timeout in seconds, or null when it is not selectable. */
    public function readTimeout(): ?float
    {
        return $this->transport instanceof SelectableTransportInterface ? $this->transport->readTimeout() : null;
    }

    /**
     * Read ONE frame and file it (M3-D1b), for a scheduler that has seen this session's stream
     * become readable or that cannot select on it. A failure is recorded on the session, never
     * thrown: each awaiter then meets it through its own {@see awaitTerminal}, with its own fate.
     */
    public function pollOnce(): void
    {
        if ($this->poisoned !== null || $this->inFlight === []) {
            return;
        }
        try {
            $this->enforceDeadlines();
            if ($this->poisoned === null) {
                $this->pump($this->nearestDeadline());
            }
        } catch (DeadlineSignal) {
            $this->enforceDeadlines();
        } catch (TransportException | ProtocolException) {
            // `pump` has already poisoned the session (or recorded a fatal before EOF): every
            // pending request is now ready, and fails at its own await.
        }
    }

    /**
     * Let ONE scheduler hear about everything that can make a {@see Waiter} on this session ready
     * (M3-D1d, {@see \Ferro\Revolt}), or stop it hearing (`null`). `$observer($rid)` runs after every
     * frame read off the wire, whoever read it — a frame read by another Fiber's blocking call can be
     * the one a suspended Fiber waits for — with that frame's `request_id`; `$observer(null)` runs
     * when the session fails, an open stream closes, or a deadline is set, any of which can change
     * what is ready or when the scheduler must next wake.
     *
     * The observer must not read or write the session; what it throws is ignored.
     *
     * @internal
     * @param (\Closure(?int): void)|null $observer
     */
    public function observe(?\Closure $observer): void
    {
        $this->observer = $observer;
    }

    private function notify(?int $requestId): void
    {
        if ($this->observer === null) {
            return;
        }
        try {
            ($this->observer)($requestId);
        } catch (\Throwable) {
            // The scheduler routes its own failures to its waiters; a frame is never lost to it.
        }
    }

    /**
     * Bytes already read off the socket into PHP's stream buffer and not yet consumed. A scheduler
     * watching the descriptor for readability must drain these first: the descriptor does not
     * report them (M3-D1d).
     */
    public function bufferedBytes(): int
    {
        $stream = $this->selectableStream();
        if (!is_resource($stream)) {
            return 0;
        }
        $meta = stream_get_meta_data($stream);
        return $meta['unread_bytes'];
    }

    /**
     * Whether any request's final frame is still to be read off the wire. When none is,
     * {@see pollOnce} reads nothing, so a scheduler must not keep asking it to (M3-D1d review F3).
     */
    public function hasRequestsInFlight(): bool
    {
        return $this->inFlight !== [];
    }

    /** Whether `$requestId` was submitted and its terminal has not been consumed yet. */
    public function isPending(int $requestId): bool
    {
        return (isset($this->inFlight[$requestId]) || isset($this->inbox[$requestId]))
            && !isset($this->discarded[$requestId]);
    }

    /**
     * Liveness: send PING and read the matching PONG. A PONG is a non-terminal CORE control frame
     * (flags=0, NOT END) on the same `request_id` — a distinct read path from a request terminal.
     */
    public function ping(int $token): void
    {
        $rid = $this->nextFreeId();
        $payload = Message::encode('ping', ['token' => $token], $this->encodePacker);
        $this->writeFrame(0, C::SERVICE_CORE, C::METHOD_CORE_PING, $payload, $rid);
        // A PONG is routed like any frame, so a ping can run while requests are in flight.
        $this->inFlight[$rid] = true;
        [$header, $body] = $this->nextFrameFor($rid);
        unset($this->inFlight[$rid]);
        if ($header->service !== C::SERVICE_CORE || $header->method !== C::METHOD_CORE_PONG) {
            throw new ProtocolException(sprintf(
                'expected PONG, got service=%d method=%d',
                $header->service,
                $header->method,
            ));
        }
        if (($header->flags & C::FLAG_END) !== 0) {
            throw new ProtocolException('PONG must not carry the END flag');
        }
        if ($header->requestId !== $rid) {
            throw new ProtocolException(sprintf('PONG request_id %d does not echo %d', $header->requestId, $rid));
        }
        $off = 0;
        $decoded = $this->decodePacker->unpack($body, $off);
        $echoed = is_array($decoded) ? (array_values($decoded)[0] ?? null) : null;
        if ((is_int($echoed) || is_string($echoed)) && (string) $echoed !== (string) $token) {
            throw new ProtocolException(sprintf('PONG token %s does not echo %d', (string) $echoed, $token));
        }
    }

    /** Best-effort GOODBYE, then close the transport. The engine treats GOODBYE as a drain break. */
    public function close(): void
    {
        try {
            $rid = $this->ids->next();
            $payload = Message::encode('goodbye', [], $this->encodePacker);
            $this->writeFrame(0, C::SERVICE_CORE, C::METHOD_CORE_GOODBYE, $payload, $rid);
        } catch (\Throwable) {
            // The connection may already be gone; closing the transport is what matters.
        }
        // Mark the session closed BEFORE anything can read it again: a Future still pending on it
        // then fails as a sent-and-lost request (`TransportException`, so a write is
        // `Indeterminate`) instead of reading a closed stream (M3-D1a review F3).
        $this->poison(new TransportException('the session was closed'));
    }

    /** The opaque `boot_epoch` cached at handshake (`int|string`). Throws if HELLO has not run. */
    public function bootEpoch(): int|string
    {
        if (!$this->handshakeDone || $this->bootEpoch === null) {
            throw new ProtocolException('bootEpoch() called before a successful HELLO');
        }
        return $this->bootEpoch;
    }

    /** @return array{0:int,1:int}|null the `(service, method)` of the last frame sent, or null. */
    public function lastInFlight(): ?array { return $this->lastInFlight; }

    /** @return list<string> the pool NAMES, for `ExecRequest.pool`. Unchanged surface. */
    public function pools(): array
    {
        return array_map(static fn (PoolInfo $p): string => $p->name, $this->poolInfo);
    }

    /** @return list<PoolInfo> the full advertised metadata (name + backend family + server version). */
    public function poolInfo(): array
    {
        return $this->poolInfo;
    }

    public function handshakeComplete(): bool { return $this->handshakeDone; }

    // ---- streamed read (M1-S5 Task 6, {@see StreamingSessionInterface}) --------------------------

    /** @return array{type:'head', requestId:int, cols:list<array{name:string,tag:int}>}|array{type:'end', requestId:int, outcome:Outcome} */
    public function openStream(int $service, int $method, string $payload): array
    {
        $rid = $this->submit($service, $method, $payload);

        [$header, $body] = $this->nextFrameFor($rid);
        $isEnd = ($header->flags & C::FLAG_END) !== 0;

        if ($isEnd) {
            // A known fate decided before any HEAD/DATA went out (e.g. a checkout failure) — no
            // stream was ever really opened, so there is nothing to guard or drain.
            return ['type' => 'end', 'requestId' => $rid, 'outcome' => Outcome::decode($body, $this->decodePacker)];
        }

        if ($header->service !== C::SERVICE_STREAM || $header->method !== C::METHOD_STREAM_HEAD) {
            throw new ProtocolException(sprintf(
                'expected STREAM/HEAD for request %d, got service=%d method=%d flags=%d request_id=%d',
                $rid,
                $header->service,
                $header->method,
                $header->flags,
                $header->requestId,
            ));
        }

        $this->streamOpen = true;
        $this->streamRequestId = $rid;
        $this->streamFiber = \Fiber::getCurrent();

        return ['type' => 'head', 'requestId' => $rid, 'cols' => $this->decodeStreamHead($body)];
    }

    /**
     * @return array{type:'data', rows:list<list<array{tag:int,data:mixed}>>, bytes:int}
     *       | array{type:'end', outcome:Outcome}
     */
    public function readStreamFrame(int $requestId): array
    {
        try {
            [$header, $body] = $this->nextFrameFor($requestId);
        } catch (ConnectionLostException | ProtocolException $e) {
            if ($this->streamRequestId === $requestId) {
                $this->streamOpen = false;
                $this->streamRequestId = null;
                $this->notify(null);
            }
            throw $e;
        }
        $isEnd = ($header->flags & C::FLAG_END) !== 0;

        if ($isEnd) {
            $this->streamOpen = false;
            $this->streamRequestId = null;
            // A Fiber waiting for this stream to close ({@see assertNoOpenStream}) can go on.
            $this->notify(null);
            return ['type' => 'end', 'outcome' => Outcome::decode($body, $this->decodePacker)];
        }

        if ($header->service !== C::SERVICE_STREAM || $header->method !== C::METHOD_STREAM_DATA) {
            throw new ProtocolException(sprintf(
                'expected STREAM/DATA for request %d, got service=%d method=%d flags=%d request_id=%d',
                $requestId,
                $header->service,
                $header->method,
                $header->flags,
                $header->requestId,
            ));
        }

        return ['type' => 'data', 'rows' => $this->decodeStreamData($body), 'bytes' => strlen($body)];
    }

    public function sendWindowUpdate(int $requestId, int $frames, int $bytes): void
    {
        $payload = Message::encode('window_update', ['frames' => $frames, 'bytes' => $bytes], $this->encodePacker);
        $this->writeFrame(0, C::SERVICE_CORE, C::METHOD_CORE_WINDOW_UPDATE, $payload, $requestId);
    }

    public function sendCancel(int $requestId): void
    {
        $this->writeFrame(C::FLAG_CANCEL, C::SERVICE_CORE, 0, '', $requestId);
    }

    public function abandonStream(int $requestId): void
    {
        // Also covers a POISONED session: `poison()` clears the guard, because a closed socket has
        // nothing to drain and nothing to CANCEL on (the engine tears the stream down at EOF). That
        // matters because abandonment usually runs from a `finally` carrying the REAL error, which a
        // throw here would replace (M2-C1e-3 review F5/F2; the dedicated early return this once had
        // was dead code under exactly this guard, and was removed when a mutation showed it).
        if (!$this->streamOpen || $this->streamRequestId !== $requestId) {
            return; // already closed (normal completion), poisoned, or not this stream.
        }
        $this->sendCancel($requestId);
        while ($this->streamOpen) {
            $this->readStreamFrame($requestId); // discards DATA batches; clears the guard on 'end'.
        }
    }

    /**
     * Write one frame. A failure here means the frame was NOT completely written — the transport
     * contract ({@see TransportInterface::writeAll}) — and the session is poisoned so nothing can
     * ever complete the partial frame.
     *
     * **Only a REQUEST frame (`$isRequest`: HELLO, a buffered request, a stream open) surfaces as
     * {@see TransportException::requestNotSent}**, because only for a request does "this frame
     * never arrived" mean "this statement never executed". A CONTROL frame — PING, GOODBYE,
     * WINDOW_UPDATE, CANCEL — is about a request that may well have run already (a WINDOW_UPDATE
     * mid-stream follows rows the engine has produced), so its failure is a plain
     * `TransportException`, and a caller that trusted the flag there would be told a lie (M2-C1e-3
     * review F2).
     */
    private function writeFrame(
        int $flags,
        int $service,
        int $method,
        string $payload,
        int $requestId = 0,
        bool $isRequest = false,
    ): void {
        $this->refuseIfDead($isRequest);
        $header = new Header($flags, $service, $method, $requestId, strlen($payload));
        try {
            $this->transport->writeAll($this->codec->encodeFrame($header, $payload));
        } catch (TransportException $e) {
            $this->poison($e);
            throw $isRequest ? TransportException::requestNotSent($e->getMessage(), $e) : $e;
        }
    }

    /**
     * The next frame filed under `$requestId`, reading off the wire (through {@see pump}) until one
     * is there.
     *
     * @return array{0:Header,1:string}
     */
    private function nextFrameFor(int $requestId): array
    {
        while (true) {
            $queue = $this->inbox[$requestId] ?? [];
            if ($queue !== []) {
                $frame = array_shift($queue);
                if ($queue === []) {
                    unset($this->inbox[$requestId]);
                } else {
                    $this->inbox[$requestId] = $queue;
                }
                return $frame;
            }
            if (!isset($this->inFlight[$requestId])) {
                throw new ProtocolException(sprintf('request %d is not in flight on this session', $requestId));
            }
            if ($this->poisoned !== null && $this->fatal !== null) {
                // The engine drained what it could after its fatal and then closed: this request got
                // no terminal. The fatal may have been about any frame, so it decides nothing here.
                unset($this->inFlight[$requestId]);
                throw new ConnectionLostException(
                    'the engine ended the session (' . $this->fatal . ') before this request\'s '
                        . 'terminal arrived; its fate is unknown',
                );
            }
            if ($this->poisoned !== null) {
                // Every frame this request needs was lost with the socket. The request WAS written
                // (it is in flight), so this is the sent-and-lost case, never `requestNotSent`.
                unset($this->inFlight[$requestId]);
                throw new TransportException('the session closed while this request was in flight ('
                    . $this->poisoned . ')');
            }
            $this->enforceDeadlines();
            if ($this->poisoned !== null) {
                continue; // a deadline's grace ran out: the branches above fail this request
            }
            try {
                $this->pump($this->nearestDeadline());
            } catch (DeadlineSignal) {
                continue; // a deadline was reached: act on it at the top of the loop
            } catch (TransportException $e) {
                if ($this->fatal === null) {
                    throw $e;
                }
                // The EOF that follows a fatal is expected: loop once more, and the branch above
                // fails this request without attributing the fatal to it.
            }
        }
    }

    /**
     * Read ONE frame off the wire and file it under its `request_id` — the only place a frame is
     * read after the handshake.
     *
     * A frame for an id not in flight means the two ends disagree about what is outstanding, so
     * nothing later can be trusted: the session is poisoned. A session-fatal `request_id=0`
     * terminal is recorded so that every pending request fails with it.
     */
    private function pump(?float $until = null): void
    {
        [$header, $body] = $this->readFrame($until);
        try {
            $this->route($header, $body);
        } finally {
            // After filing, so an observer that looks finds the frame where its awaiter will.
            $this->notify($header->requestId);
        }
    }

    /** File one frame read by {@see pump}. */
    private function route(Header $header, string $body): void
    {
        $rid = $header->requestId;
        $isEnd = ($header->flags & C::FLAG_END) !== 0;

        if ($this->probeRid !== null && $rid === $this->probeRid) {
            // The answer to a liveness PING (M3-D1c): the engine is there.
            if ($header->service !== C::SERVICE_CORE || $header->method !== C::METHOD_CORE_PONG || $isEnd) {
                $error = new ProtocolException(sprintf('expected the liveness PONG on request_id %d', $rid));
                $this->poison(new TransportException($error->getMessage()));
                throw $error;
            }
            $this->probeRid = null;
            return;
        }

        if ($rid === 0) {
            // Record it and keep reading: the engine drains every in-flight request's own terminal
            // after a fatal, then closes, and that EOF is what ends the session here.
            $this->fatal = $this->sessionFatal($body, $isEnd)->getMessage();
            return;
        }
        if (isset($this->discarded[$rid])) {
            if ($isEnd) {
                unset($this->discarded[$rid], $this->inFlight[$rid], $this->deadlines[$rid], $this->deadlineCancelled[$rid]);
            }
            return;
        }
        if (!isset($this->inFlight[$rid])) {
            $error = new ProtocolException(sprintf(
                'a frame arrived for request_id %d, which is not in flight on this session '
                    . '(service=%d method=%d flags=%d)',
                $rid,
                $header->service,
                $header->method,
                $header->flags,
            ));
            $this->poison(new TransportException($error->getMessage()));
            throw $error;
        }
        if ($isEnd) {
            unset($this->inFlight[$rid], $this->deadlines[$rid], $this->deadlineCancelled[$rid]);
        }
        $this->inbox[$rid][] = [$header, $body];
    }

    /**
     * Refuse to write on a session that has failed or been told it is over. A request refused here
     * was never sent, so it is {@see TransportException::requestNotSent}; a control frame's refusal
     * is a plain {@see TransportException} (see {@see writeFrame}).
     */
    private function refuseIfDead(bool $isRequest): void
    {
        $why = match (true) {
            $this->poisoned !== null => 'this session was closed after an earlier failure (' . $this->poisoned . ')',
            $this->fatal !== null => 'the engine ended this session (' . $this->fatal . ')',
            default => null,
        };
        if ($why === null) {
            return;
        }
        throw $isRequest ? TransportException::requestNotSent('not sent: ' . $why) : new TransportException($why);
    }

    /** The next `request_id` that is neither 0 nor still pending (the u32 space wraps). */
    private function nextFreeId(): int
    {
        do {
            $rid = $this->ids->next();
        } while ($this->isPending($rid) || $rid === $this->probeRid);
        return $rid;
    }

    /**
     * Whether a transport failure has closed this session ({@see poison}). A poisoned session can
     * carry no further request; {@see Connection} replaces it before a request when it has a
     * reconnect loop to do so (M2-C1e-3 review F4).
     */
    public function isPoisoned(): bool
    {
        return $this->poisoned !== null;
    }

    /**
     * Whether this session's transport can be sent fds, so it advertises `MEMFD_RX` (M3-D3).
     * Diagnostic as well: what `Ferro::connect(receiveFds: …)` and the tiers' opt-outs decided.
     */
    public function receivesFds(): bool
    {
        return $this->transport instanceof FdReceivingTransportInterface && $this->transport->receivesFds();
    }

    /**
     * How many results this session has received through a sealed memfd (M3-D3, SPEC §5.1).
     * Diagnostic — the two paths are otherwise indistinguishable above this class, which is the
     * point — so a test or an operator can tell the out-of-band path was actually taken.
     */
    public function oobPayloadsReceived(): int
    {
        return $this->oobPayloads;
    }

    /**
     * Read one frame. With a selectable transport, a read that waits its whole timeout in silence
     * does not fail the session (M3-D1c): the session probes liveness with a PING
     * ({@see onSilence}) and reads on, and only a second silent timeout after that PING closes it.
     * With `$until` (an absolute deadline), the wait is shortened to it, and reaching it raises
     * {@see DeadlineSignal} for the caller to act on — nothing has been lost, the stream is in step.
     *
     * @return array{0:Header,1:string} the decoded header + its exact-length payload. An `OOB_FD`
     *   frame (M3-D3) is returned as the frame it stands for: its payload read from the memfd, and a
     *   header carrying that payload's length and no `OOB_FD`, so nothing above this method can tell
     *   the two paths apart.
     */
    private function readFrame(?float $until = null): array
    {
        try {
            return $this->readFrameWithin($until);
        } finally {
            // A deadline-shortened wait never outlives this read (M3-D1c review F2): the transport
            // re-applies its full timeout before every write as well, because PHP's socket stream
            // bounds writes with the same timeout.
            if ($until !== null && $this->transport instanceof SelectableTransportInterface) {
                $this->transport->setReadWait($this->transport->readTimeout());
            }
        }
    }

    /** @return array{0:Header,1:string} */
    private function readFrameWithin(?float $until): array
    {
        while (true) {
            $live = $this->transport instanceof SelectableTransportInterface;
            if ($live) {
                $wait = $this->transport->readTimeout();
                if ($until !== null) {
                    $wait = min($wait, max($until - microtime(true), 0.001));
                }
                $this->transport->setReadWait($wait);
            }
            try {
                if ($this->partialHeader === null && $this->transport instanceof FdReceivingTransportInterface) {
                    $this->transport->beginFrame();
                }
                $this->partialHeader ??= Header::decode($this->transport->readExact(16));
                $header = $this->partialHeader;
                $payload = $header->payloadLen > 0 ? $this->transport->readExact($header->payloadLen) : '';
                $this->partialHeader = null;
                if (($header->flags & C::FLAG_OOB_FD) !== 0) {
                    // M3-D3: the frame it stands for. Inside this `try`, so a disagreement about the
                    // OOB frame is the desync below. Its fd was queued by the transport no later
                    // than the payload just read, even if an earlier read of this frame timed out.
                    [$header, $payload] = $this->resolveOob($header, $payload);
                }
                $this->lastFrameAt = microtime(true);
                return [$header, $payload];
            } catch (CodecException $e) {
                // An undecodable header (bad magic, version, oversized length) leaves the stream at an
                // unknown offset: every later read would be out of step. It is a desync, so the session
                // is poisoned and the fault is a ProtocolException, which the router's callers handle
                // (M3-D1b review F4: a raw CodecException escaped Ferro\Loop past every task's catch).
                $this->poison(new TransportException('undecodable frame: ' . $e->getMessage()));
                throw new ProtocolException('undecodable frame: ' . $e->getMessage(), 0, $e);
            } catch (TransportException $e) {
                if ($live && $e->isReadTimeout()) {
                    if ($until !== null && microtime(true) >= $until) {
                        throw new DeadlineSignal();
                    }
                    $this->onSilence(); // probes, or closes the session after an unanswered probe
                    continue;
                }
                // The request (if any) WAS fully written, so its fate is the caller's to classify; the
                // session is unusable either way, because what is left unread is unknown.
                $this->poison($e);
                throw $e;
            }
        }
    }

    /**
     * A read waited its whole timeout and nothing arrived (M3-D1c). The first time, send a liveness
     * PING and keep waiting; if a PING is already out and has gone unanswered for a whole read
     * timeout, the engine (or the link) is gone: close the session, which fails every pending
     * request as sent-and-lost.
     */
    private function onSilence(): void
    {
        $timeout = $this->transport instanceof SelectableTransportInterface ? $this->transport->readTimeout() : 0.0;
        if ($this->fatal !== null) {
            // The engine said the session is over and promised to close it; a PING cannot be sent
            // (nothing is, after a fatal) and silence this long means the close never came.
            $e = new TransportException('the engine ended the session (' . $this->fatal . ') and then went silent');
            $this->poison($e);
            throw $e;
        }
        if ($this->probeRid !== null) {
            // The engine is judged dead only if NOTHING arrived since the PING went out (M3-D1c
            // review F6): a frame read after it — the PONG may legally follow a slow request's
            // terminal — proves it alive, so the clock restarts from that frame.
            $since = max($this->probeSentAt, $this->lastFrameAt);
            if (microtime(true) - $since >= $timeout) {
                $e = new TransportException(sprintf(
                    'the engine sent nothing for %.1f s and did not answer a liveness PING',
                    microtime(true) - $since + $timeout,
                ));
                $this->poison($e);
                throw $e;
            }
            return;
        }
        $rid = $this->nextFreeId();
        $payload = Message::encode('ping', ['token' => $rid], $this->encodePacker);
        $this->writeFrame(0, C::SERVICE_CORE, C::METHOD_CORE_PING, $payload, $rid);
        $this->probeRid = $rid;
        $this->probeSentAt = microtime(true);
    }

    /**
     * Give request `$requestId` an absolute deadline (`microtime(true)`, M3-D1c). Past it the
     * request is CANCELled — only that request; the session and every other request carry on — and
     * the engine's terminal decides its fate (a cancelled write is `Indeterminate`, as the engine's
     * own §19.3 matrix says). If the engine does not answer within one more read timeout, the
     * session is closed.
     */
    public function setDeadline(int $requestId, float $at): void
    {
        if (isset($this->inFlight[$requestId])) {
            $this->deadlines[$requestId] = $at;
            $this->notify(null); // a scheduler sleeping past `$at` must wake for it
        }
    }

    /**
     * Non-blocking liveness probe for a scheduler that selects instead of reading (M3-D1c): the
     * session has been silent for a whole read timeout, so PING it — or, if a PING is already out
     * and unanswered for that long, close it. A failure is recorded on the session, never thrown,
     * like {@see pollOnce}.
     */
    public function probeLiveness(): void
    {
        if ($this->poisoned !== null || $this->inFlight === []) {
            return;
        }
        try {
            $this->onSilence();
        } catch (TransportException) {
            // poisoned: every pending request is now ready, and fails at its own await
        }
    }

    /** The nearest pending request deadline, or null. */
    public function nearestDeadline(): ?float
    {
        return $this->deadlines === [] ? null : min($this->deadlines);
    }

    /**
     * Act on every deadline that has passed: CANCEL once, then close the session after the grace.
     *
     * **Never throws** (M3-D1c review F4). A CANCEL that cannot be written means the link is gone:
     * the write failure has already closed the session, and every request in flight on it — each
     * one completely written — then fails at its own await as sent-and-lost, with its own fate. A
     * throw here used to escape {@see \Ferro\Loop::run} past every task's catch, abandoning every
     * other task mid-await. After a session-fatal terminal nothing is sent at all, so there is
     * nothing to CANCEL with: the engine is draining every request's own terminal before it closes.
     */
    public function enforceDeadlines(): void
    {
        if ($this->poisoned !== null || $this->fatal !== null) {
            return;
        }
        $now = microtime(true);
        foreach ($this->deadlines as $rid => $at) {
            if ($now < $at) {
                continue;
            }
            if (!isset($this->deadlineCancelled[$rid])) {
                $this->deadlineCancelled[$rid] = true;
                $grace = $this->transport instanceof SelectableTransportInterface ? $this->transport->readTimeout() : 0.0;
                $this->deadlines[$rid] = $now + $grace;
                try {
                    $this->sendCancel($rid);
                } catch (TransportException) {
                    return; // `writeFrame` closed the session: see the docblock
                }
                continue;
            }
            $e = new TransportException(sprintf(
                'request %d passed its deadline and the engine did not answer its CANCEL', $rid,
            ));
            $this->poison($e);
            return;
        }
    }

    /**
     * Turn an `OOB_FD` frame into the frame it stands for (M3-D3, SPEC §5.1): pair it with the
     * oldest received fd ({@see FdReceivingTransportInterface} for why the pairing is FIFO and never
     * positional), read the memfd's `len` bytes from offset 0 — one copy; PHP cannot mmap — and
     * close the fd. Any mismatch (an `OOB_FD` frame on a session that never advertised `MEMFD_RX`,
     * no fd queued, a malformed {@see OobRef}, a memfd shorter or longer than `len`) means the two ends
     * disagree about the byte stream: a {@see CodecException}, which poisons the session as a desync.
     *
     * @return array{0:Header,1:string}
     */
    private function resolveOob(Header $header, string $refPayload): array
    {
        if (!$this->receivesFds() || !$this->transport instanceof FdReceivingTransportInterface) {
            throw new CodecException('an OOB_FD frame arrived on a session that did not advertise MEMFD_RX');
        }
        $fd = $this->transport->takeFd();
        if (!is_resource($fd)) {
            throw new CodecException('an OOB_FD frame arrived without its fd');
        }
        try {
            $ref = OobRef::decode($refPayload, $this->decodePacker);
            $len = $ref['len'];
            $stat = fstat($fd);
            if ($stat === false || $stat['size'] !== $len) {
                throw new CodecException('the OOB memfd is not exactly the length its OobRef names');
            }
            // From offset 0, explicitly: an SCM_RIGHTS fd shares its file OFFSET with the sender's.
            $payload = $len === 0 ? '' : stream_get_contents($fd, $len, 0);
            if (!is_string($payload) || strlen($payload) !== $len) {
                throw new CodecException('could not read the whole OOB memfd');
            }
        } finally {
            fclose($fd);
            $this->transport->fdClosed();
        }
        $this->oobPayloads++;
        $flags = $header->flags & ~C::FLAG_OOB_FD;
        return [new Header($flags, $header->service, $header->method, $header->requestId, $len), $payload];
    }

    /** Close the socket on the first transport failure and remember why. Idempotent. */
    private function poison(TransportException $e): void
    {
        if ($this->poisoned !== null) {
            return;
        }
        $this->poisoned = $e->getMessage();
        // The stream guard exists to stop a request interleaving with an open stream's unread
        // frames. On a closed socket there are no frames left to interleave with, and leaving the
        // guard set made every later request fail `ProtocolException` ("a stream is open")
        // instead of `requestNotSent` — so nothing above could tell the session was dead and
        // reconnect (M2-C1e-3 review F5, reproduced live with a `cursor()` across a restart).
        $this->streamOpen = false;
        $this->streamRequestId = null;
        try {
            $this->transport->close();
        } catch (\Throwable) {
            // Best-effort: the point is that nothing more is WRITTEN, which the flag guarantees.
        }
        // Every pending request is ready now (it fails at its own await), and the socket a
        // scheduler may be watching is closed: it must stop watching it before it selects again.
        $this->notify(null);
    }

    /** Whether the open stream has ended, or the session has failed (either ends a wait for it). */
    private function streamClosedOrDead(): bool
    {
        return !$this->streamOpen || $this->poisoned !== null;
    }

    /**
     * @throws ProtocolException if a stream is currently open on this session.
     *
     * Under {@see \Ferro\Loop}, a Fiber that is NOT the stream's own waits for it to close
     * instead (M3-D1b review F5): the stream's Fiber keeps reading it whenever it runs, so the wait
     * ends. The stream's own Fiber, the main program and Fibers the loop did not start still get
     * the refusal, because for them waiting would never end.
     */
    private function assertNoOpenStream(): void
    {
        if ($this->streamOpen && \Fiber::getCurrent() !== $this->streamFiber) {
            \Ferro\Loop::waitFor(new Waiter(
                $this,
                $this->streamRequestId ?? 0,
                $this->streamClosedOrDead(...),
            ));
            $this->refuseIfDead(true);
        }
        if ($this->streamOpen) {
            throw new ProtocolException(sprintf(
                'a stream (request_id=%d) is open on this session; drive it to its terminal or call '
                    . 'abandonStream() before sending another request',
                $this->streamRequestId ?? -1,
            ));
        }
    }

    /** Build the {@see ConnectionLostException} for a `request_id=0` session-fatal terminal body. */
    private function sessionFatal(string $body, bool $isEnd): ConnectionLostException
    {
        $ep = null;
        if ($isEnd) {
            try {
                $outcome = Outcome::decode($body, $this->decodePacker);
            } catch (CodecException $e) {
                return new ConnectionLostException('session-fatal terminal with an undecodable body: ' . $e->getMessage());
            }
            if ($outcome->isError()) { $ep = $outcome->errorPayload(); }
        }
        return new ConnectionLostException(
            $ep !== null
                ? sprintf('session-fatal terminal: %s (code=%d)', $ep->message, $ep->code)
                : 'session-fatal terminal on request_id=0',
            $ep,
        );
    }

    /** @return list<array{name:string,tag:int}> */
    private function decodeStreamHead(string $body): array
    {
        $off = 0;
        $w = $this->decodePacker->unpack($body, $off);
        if (!is_array($w)) {
            throw new ProtocolException('StreamHead body is not an array');
        }
        return StreamHead::mapFromWire(array_values($w))['cols'];
    }

    /** @return list<list<array{tag:int,data:mixed}>> */
    private function decodeStreamData(string $body): array
    {
        $off = 0;
        $w = $this->decodePacker->unpack($body, $off);
        if (!is_array($w)) {
            throw new ProtocolException('StreamData body is not an array');
        }
        return StreamData::mapFromWire(array_values($w))['rows'];
    }
}
