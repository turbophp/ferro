<?php // /php/client/src/Http/Upstream.php
declare(strict_types=1);
namespace Ferro\Http;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\Session;
use Ferro\Client\SessionInterface;
use Ferro\Client\TraceContext;
use Ferro\Client\Waiter;
use Ferro\Ferro;
use Ferro\Future;
use Ferro\Http\Error\HttpFates;
use Ferro\Http\Error\RequestTooLargeException;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\HttpRequest;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * A named Ferro HTTP upstream on a {@see \Ferro\Client\Connection}'s session (SPEC §23.11.1):
 *
 *     $http = $conn->upstream('openai');
 *     $res  = $http->request('POST', '/v1/chat/completions', headers: [...], body: $json,
 *                            timeoutMs: 30_000, route: '/v1/chat/completions');
 *     $f    = $http->requestAsync('GET', '/v1/models');           // Ferro\Future<HttpResponse>
 *     [$user, $models] = Ferro\await([$db->queryOneAsync(…), $f]); // both in flight at once
 *     foreach ($http->stream('POST', '/v1/chat/completions', body: $json) as $chunk) { … }
 *
 * The request goes to the upstream the OPERATOR declared under that name (`FERRO_UPSTREAMS`): PHP
 * names it and sends an origin-form target (`/path?query`), never a URL (§23.4). Every request
 * shares the connection's one multiplexed session with its SQL, so a DB query and several API calls
 * are in flight on one socket together.
 *
 * **Fates** (§23.7): a failed exchange is thrown as an {@see Error\HttpException} that extends the
 * `ferro/client` taxonomy base of its fate. A completed exchange is a response WHATEVER its status
 * (§23.7.4). **Nothing here ever re-sends a request** (§23.7.3, charter rule 3) — not a Retryable
 * one, not one whose session was lost, not one declared idempotent: a Retryable licenses the
 * caller's policy, never this client.
 *
 * **Idempotency is a declaration** (§23.7.2): pass `idempotent: true` for a request that is safe to
 * repeat, `false` to forbid the operator's per-upstream declaration, and `null` (the default) to
 * leave it to the operator. The method alone never makes a request idempotent — an undeclared `GET`
 * that dies after it was sent is Indeterminate.
 *
 * **Deadlines** (§23.11.0): `timeoutMs` bounds the whole exchange in the ENGINE, which answers when
 * it passes. The client arms a backstop `timeoutMs` + {@see Ferro::DEADLINE_MARGIN} later; past it the
 * client CANCELs the request and waits one liveness interval for the engine's answer, which still
 * decides the fate — and only then, with no answer, closes the session. A request with no
 * `timeoutMs` gets no client deadline (the engine bounds it by the upstream's `TIMEOUT_MS`, which the
 * client cannot see); silence while it runs is probed by `PING`, never treated as failure.
 */
final class Upstream
{
    /**
     * @param \Closure(): SessionInterface $session the session a NEW request goes out on; throws a
     *   {@see TransportException}/{@see ConnectionLostException} when it cannot provide one, which
     *   means nothing was sent
     */
    public function __construct(
        public readonly string $name,
        /** The origin PHP believes this upstream has; the engine refuses a mismatch (`forbidden_origin`). */
        public readonly ?string $origin,
        private readonly \Closure $session,
        private readonly PackerInterface $encodePacker,
        private readonly PackerInterface $decodePacker,
    ) {}

    /**
     * Send a request and buffer its response.
     *
     * @param array<array-key, mixed> $headers `['Name' => 'value' | ['v1', 'v2']]`, or a list of
     *   `[name, value]` pairs (order and duplicates kept)
     * @param ?string $body the request body (bytes), or null for none — distinct from `''`
     * @param ?int $timeoutMs the total-exchange bound (the engine caps it at the upstream's
     *   `TIMEOUT_MS`); null for the upstream's
     * @param ?int $connectTimeoutMs DNS + TCP + TLS, capped by `CONNECT_TIMEOUT_MS`
     * @param ?int $readTimeoutMs the idle bound between body bytes
     * @param ?bool $idempotent the caller's declaration (§23.7.2)
     * @param bool $decode ask the engine to decode a `gzip`/`deflate` body (§23.9.2)
     * @param ?string $route a route TEMPLATE for observability (`/v1/users/{id}`); never sent upstream
     */
    public function request(
        string $method,
        string $target,
        array $headers = [],
        ?string $body = null,
        ?int $timeoutMs = null,
        ?int $connectTimeoutMs = null,
        ?int $readTimeoutMs = null,
        ?bool $idempotent = null,
        bool $decode = false,
        ?string $route = null,
    ): HttpResponse {
        return $this->requestAsync(
            $method, $target, $headers, $body, $timeoutMs, $connectTimeoutMs, $readTimeoutMs, $idempotent, $decode, $route,
        )->await();
    }

    /**
     * {@see request}, written now and awaited later: the request is on the wire when this returns,
     * so several of them — and SQL statements — run concurrently in the engine. Every error surfaces
     * at `await`, never here. A Future dropped unawaited CANCELs its request and discards its frames.
     *
     * @param array<array-key, mixed> $headers
     * @return Future<HttpResponse>
     */
    public function requestAsync(
        string $method,
        string $target,
        array $headers = [],
        ?string $body = null,
        ?int $timeoutMs = null,
        ?int $connectTimeoutMs = null,
        ?int $readTimeoutMs = null,
        ?bool $idempotent = null,
        bool $decode = false,
        ?string $route = null,
    ): Future {
        try {
            [$session, $exchange] = $this->send(
                $method, $target, $headers, $body, $timeoutMs, $connectTimeoutMs, $readTimeoutMs, $idempotent, $decode, $route,
                buffered: true,
            );
        } catch (\Throwable $e) {
            return Future::settleNow(static fn () => throw $e);
        }
        return new Future(
            static fn (): HttpResponse => self::buffer($exchange),
            static fn () => $exchange->abandonNow(),
            new Waiter($session, $exchange->requestId),
        );
    }

    /**
     * Send a request and return once its HEAD has arrived; the body is read lazily by iterating
     * the {@see HttpStream}. A failure before the head is thrown here.
     *
     * @param array<array-key, mixed> $headers
     */
    public function stream(
        string $method,
        string $target,
        array $headers = [],
        ?string $body = null,
        ?int $timeoutMs = null,
        ?int $connectTimeoutMs = null,
        ?int $readTimeoutMs = null,
        ?bool $idempotent = null,
        bool $decode = false,
        ?string $route = null,
    ): HttpStream {
        [, $exchange] = $this->send(
            $method, $target, $headers, $body, $timeoutMs, $connectTimeoutMs, $readTimeoutMs, $idempotent, $decode, $route,
        );
        try {
            $head = $exchange->awaitHead();
        } catch (\Throwable $e) {
            $exchange->abandon(); // a no-op when the failure already ended the exchange
            throw $e;
        }
        return new HttpStream($head, $exchange);
    }

    private static function buffer(HttpExchange $exchange): HttpResponse
    {
        try {
            $head = $exchange->awaitHead();
            $body = '';
            while (($chunk = $exchange->nextChunk()) !== null) {
                $body .= $chunk[0];
                $exchange->ack($chunk[1]);
            }
            $done = $exchange->completed(); // nextChunk() returns null only once the Ok terminal decoded
            return new HttpResponse($head, $body, $done['trailers'], $done['stats']);
        } finally {
            $exchange->abandon(); // a no-op unless something above threw mid-exchange
        }
    }

    /**
     * Encode and write the `REQUEST`; nothing is read.
     *
     * @param array<array-key, mixed> $headers
     * @return array{0:Session, 1:HttpExchange}
     */
    private function send(
        string $method,
        string $target,
        array $headers,
        ?string $body,
        ?int $timeoutMs,
        ?int $connectTimeoutMs,
        ?int $readTimeoutMs,
        ?bool $idempotent,
        bool $decode,
        ?string $route,
        bool $buffered = false,
    ): array {
        $declaredIdempotent = $idempotent === true;
        $request = [
            'upstream' => $this->name,
            'method' => $method,
            'target' => $target,
            'origin' => $this->origin,
            'headers' => self::headerList($headers),
            'body' => $body,
            'timeout_ms' => $timeoutMs,
            'connect_timeout_ms' => $connectTimeoutMs,
            'read_timeout_ms' => $readTimeoutMs,
            'idempotent' => $idempotent,
            'decode' => $decode,
            'route' => $route,
            'traceparent' => TraceContext::current(),
        ];
        $payload = $this->encode($request);

        try {
            $session = ($this->session)();
        } catch (TransportException | ConnectionLostException $e) {
            throw HttpFates::linkLost(false, $declaredIdempotent, null, 'no session to send on: ' . $e->getMessage());
        }
        if (!$session instanceof Session) {
            throw new ProtocolException('Ferro HTTP requires the concrete Ferro\\Client\\Session');
        }
        if (($session->engineFeatures() & C::FEATURE_ENGINE_HTTP) === 0) {
            // A build without the `http` feature has the same registry hash, so the bit is the one
            // signal (§23.5). Refused here rather than sent for an `Unsupported`, with the same code.
            throw new NonRetryableException(new ErrorPayload(
                C::ERR_UNSUPPORTED,
                C::BRANCH_NON_RETRYABLE,
                null,
                null,
                'this engine does not serve Ferro HTTP (HELLO_ACK has no HTTP feature bit: built without '
                    . 'the `http` feature, FERRO_UPSTREAMS unset, or a daemon-wide HTTP configuration error)',
                null,
                null,
            ));
        }
        try {
            $rid = $session->submitHttp($payload);
        } catch (TransportException $e) {
            throw HttpFates::linkLost(!$e->requestUnsent(), $declaredIdempotent, null, $e->getMessage());
        }
        if ($buffered) {
            // Its body is held whole anyway: its credit goes back as frames are filed, so an
            // unawaited Future still runs to its terminal and frees its slot (review F2).
            $session->creditOnReceipt($rid);
        }
        if ($timeoutMs !== null) {
            // Until its HEAD: from then on the engine bounds the exchange (review F1).
            $session->setDeadline($rid, microtime(true) + $timeoutMs / 1000 + Ferro::DEADLINE_MARGIN);
        }
        return [$session, new HttpExchange($session, $rid, $declaredIdempotent, $this->decodePacker, $buffered)];
    }

    /**
     * The `REQUEST` payload, refused BEFORE a byte is written when the frame would exceed the cap
     * (§23.9.3). A trace context that alone pushes it over is dropped, as on the SQL path.
     *
     * @param array<string, mixed> $request
     */
    private function encode(array $request): string
    {
        try {
            $payload = HttpRequest::encode($request, $this->encodePacker);
            if (strlen($payload) > C::MAX_FRAME_PAYLOAD && $request['traceparent'] !== null) {
                $request['traceparent'] = null;
                $payload = HttpRequest::encode($request, $this->encodePacker);
            }
        } catch (CodecException $e) {
            throw new \InvalidArgumentException('invalid HTTP request: ' . $e->getMessage(), 0, $e);
        }
        if (strlen($payload) > C::MAX_FRAME_PAYLOAD) {
            throw new RequestTooLargeException(sprintf(
                'the HTTP request to upstream "%s" would be a %d-byte REQUEST frame, above MAX_FRAME_PAYLOAD '
                    . '(%d bytes, body included); it was not sent. Ferro HTTP v1 has no large-request-body '
                    . 'path (SPEC §23.9.3)',
                $this->name,
                strlen($payload),
                C::MAX_FRAME_PAYLOAD,
            ));
        }
        return $payload;
    }

    /**
     * @param array<array-key, mixed> $headers
     * @return list<array{0:string,1:string}>
     */
    private static function headerList(array $headers): array
    {
        $out = [];
        foreach ($headers as $key => $value) {
            if (is_int($key) && is_array($value)) {
                // A `[name, value]` pair.
                if (count($value) !== 2 || !array_is_list($value) || !is_string($value[0])) {
                    throw new \InvalidArgumentException("header #{$key} is neither 'Name' => value nor a [name, value] pair");
                }
                $out[] = [$value[0], self::headerValue($value[0], $value[1])];
                continue;
            }
            // `'Name' => value | [values]` — PHP turns a numeric name into an int key; restore it.
            $name = (string) $key;
            foreach (is_array($value) ? $value : [$value] as $one) {
                $out[] = [$name, self::headerValue($name, $one)];
            }
        }
        return $out;
    }

    private static function headerValue(string $name, mixed $value): string
    {
        if (is_string($value)) {
            return $value;
        }
        if (is_int($value)) {
            return (string) $value;
        }
        throw new \InvalidArgumentException("header '{$name}' has a " . get_debug_type($value) . ' value; a string is required');
    }
}
