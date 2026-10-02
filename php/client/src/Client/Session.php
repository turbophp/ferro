<?php // /php/client/src/Client/Session.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\HandshakeException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TransportException;
use Ferro\Protocol\Codec;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Hello;
use Ferro\Protocol\HelloAck;
use Ferro\Protocol\Message;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\PoolInfo;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\StreamData;
use Ferro\Protocol\StreamHead;

/**
 * The synchronous, single-in-flight session over a {@see TransportInterface}: HELLO/HELLO_ACK, one
 * request -> one terminal `END`, PING/PONG liveness, GOODBYE on close. The Fibers/multiplexing
 * feature is post-M0 — this client advertises `features=0`.
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
final class Session implements SessionInterface, StreamingSessionInterface
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
     * being {@see abandonStream}-ed). While set, {@see sendRequest} / {@see openStream} refuse —
     * the single-in-flight session cannot interleave a buffered request with an open stream's
     * un-read DATA/END frames without desyncing the wire (M1-S5 Task 6).
     */
    private bool $streamOpen = false;
    private ?int $streamRequestId = null;

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

    public function __construct(
        private readonly TransportInterface $transport,
        ?RequestIdAllocator $ids = null,
        ?Codec $codec = null,
        ?PackerInterface $encodePacker = null,
        ?PackerInterface $decodePacker = null,
    ) {
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
    public function hello(): HelloAck
    {
        $hello = new Hello(
            clientVersion: 1,
            typeRegistryHash: C::TYPE_REGISTRY_HASH,
            manifestHash: null,
            pid: getmypid() ?: 0,
            features: 0,
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
        $this->assertNoOpenStream();
        $rid = $this->ids->next();
        // Record BEFORE the write, so it names the request even when the write itself dies.
        $this->lastInFlight = [$service, $method];
        $this->writeFrame(0, $service, $method, $payload, $rid, true);

        [$header, $body] = $this->readFrame();
        $isEnd = ($header->flags & C::FLAG_END) !== 0;

        if ($header->requestId === 0) {
            // Session-context terminal. Decode its Outcome::Error (if any) so the fate is preserved.
            throw $this->sessionFatal($body, $isEnd);
        }

        if (!$isEnd) {
            throw new ProtocolException(sprintf('terminal for request %d did not carry the END flag', $rid));
        }
        if ($header->requestId !== $rid) {
            throw new ProtocolException(sprintf(
                'terminal request_id %d does not echo the sent id %d',
                $header->requestId,
                $rid,
            ));
        }

        return Outcome::decode($body, $this->decodePacker);
    }

    /**
     * Liveness: send PING and read the matching PONG. A PONG is a non-terminal CORE control frame
     * (flags=0, NOT END) on the same `request_id` — a distinct read path from a request terminal.
     */
    public function ping(int $token): void
    {
        $rid = $this->ids->next();
        $payload = Message::encode('ping', ['token' => $token], $this->encodePacker);
        $this->writeFrame(0, C::SERVICE_CORE, C::METHOD_CORE_PING, $payload, $rid);

        [$header, $body] = $this->readFrame();
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
        $this->transport->close();
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
        $this->assertNoOpenStream();
        $rid = $this->ids->next();
        $this->lastInFlight = [$service, $method];
        $this->writeFrame(0, $service, $method, $payload, $rid, true);

        [$header, $body] = $this->readFrame();
        $isEnd = ($header->flags & C::FLAG_END) !== 0;

        if ($header->requestId === 0) {
            throw $this->sessionFatal($body, $isEnd);
        }

        if ($isEnd) {
            // A known fate decided before any HEAD/DATA went out (e.g. a checkout failure) — no
            // stream was ever really opened, so there is nothing to guard or drain.
            if ($header->requestId !== $rid) {
                throw new ProtocolException(sprintf(
                    'stream-open terminal request_id %d does not echo the sent id %d',
                    $header->requestId,
                    $rid,
                ));
            }
            return ['type' => 'end', 'requestId' => $rid, 'outcome' => Outcome::decode($body, $this->decodePacker)];
        }

        if ($header->service !== C::SERVICE_STREAM || $header->method !== C::METHOD_STREAM_HEAD
            || $header->requestId !== $rid) {
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

        return ['type' => 'head', 'requestId' => $rid, 'cols' => $this->decodeStreamHead($body)];
    }

    /**
     * @return array{type:'data', rows:list<list<array{tag:int,data:mixed}>>, bytes:int}
     *       | array{type:'end', outcome:Outcome}
     */
    public function readStreamFrame(int $requestId): array
    {
        [$header, $body] = $this->readFrame();
        $isEnd = ($header->flags & C::FLAG_END) !== 0;

        if ($header->requestId === 0) {
            $this->streamOpen = false;
            $this->streamRequestId = null;
            throw $this->sessionFatal($body, $isEnd);
        }

        if ($isEnd) {
            if ($header->requestId !== $requestId) {
                throw new ProtocolException(sprintf(
                    'stream terminal request_id %d does not echo the open stream id %d',
                    $header->requestId,
                    $requestId,
                ));
            }
            $this->streamOpen = false;
            $this->streamRequestId = null;
            return ['type' => 'end', 'outcome' => Outcome::decode($body, $this->decodePacker)];
        }

        if ($header->service !== C::SERVICE_STREAM || $header->method !== C::METHOD_STREAM_DATA
            || $header->requestId !== $requestId) {
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
        if ($this->poisoned !== null) {
            $why = 'this session was closed after an earlier transport failure (' . $this->poisoned . ')';
            throw $isRequest
                ? TransportException::requestNotSent('not sent: ' . $why)
                : new TransportException($why);
        }
        $header = new Header($flags, $service, $method, $requestId, strlen($payload));
        try {
            $this->transport->writeAll($this->codec->encodeFrame($header, $payload));
        } catch (TransportException $e) {
            $this->poison($e);
            throw $isRequest ? TransportException::requestNotSent($e->getMessage(), $e) : $e;
        }
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

    /** @return array{0:Header,1:string} the decoded header + its exact-length payload. */
    private function readFrame(): array
    {
        try {
            $head = $this->transport->readExact(16);
            $header = Header::decode($head);
            $payload = $header->payloadLen > 0 ? $this->transport->readExact($header->payloadLen) : '';
        } catch (TransportException $e) {
            // The request (if any) WAS fully written, so its fate is the caller's to classify; the
            // session is unusable either way, because what is left unread is unknown.
            $this->poison($e);
            throw $e;
        }
        return [$header, $payload];
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
    }

    /** @throws ProtocolException if a stream is currently open on this session. */
    private function assertNoOpenStream(): void
    {
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
            $outcome = Outcome::decode($body, $this->decodePacker);
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
