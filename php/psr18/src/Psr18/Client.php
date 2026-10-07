<?php // /php/psr18/src/Psr18/Client.php
declare(strict_types=1);
namespace Ferro\Psr18;

use Ferro\Client\Connection;
use Ferro\Http\Adapter\BodyStream;
use Ferro\Http\Adapter\ConnectionProvider;
use Ferro\Http\Adapter\Failure;
use Ferro\Http\Adapter\OriginMap;
use Ferro\Http\Adapter\OutboundRequest;
use Ferro\Http\Adapter\ResponseParts;
use Ferro\Http\Error\RequestTooLargeException;
use Ferro\Http\Exception\BodyReadException;
use Ferro\Http\FateClass;
use Ferro\Http\HttpFate;
use Ferro\Http\Upstream;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseFactoryInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\StreamFactoryInterface;

/**
 * **Ferro HTTP as a synchronous PSR-18 client** (SPEC §23.11.5) — for `openai-php/client` and every
 * library that takes a `Psr\Http\Client\ClientInterface`:
 *
 *     $client = new Ferro\Psr18\Client($conn, $psr17, $psr17, [
 *         'https://api.openai.com' => 'openai',
 *     ]);
 *     $openai = OpenAI::factory()->withHttpClient($client)->make();
 *
 * It depends only on the PSR interfaces: responses are built by the application's own PSR-17
 * factories and returned as a {@see FatedResponse}, which carries the Ferro fate for every factory.
 *
 * **Routing.** The request URI's normalised origin is looked up in the upstream map; an unmapped
 * origin is an {@see UnmappedOriginException} (`RequestExceptionInterface`) and NOTHING is sent —
 * there is no fallback transport (§23.11.2).
 *
 * **Bodies are lazy by default** (`stream: true`): `sendRequest()` returns as soon as the response
 * HEAD arrives, and the body is read off the engine as the caller reads it, never buffered
 * (a streamed chat completion arrives as it is produced). Closing or dropping the body unread
 * CANCELs the exchange. A failure after the head is then thrown from `read()` as a
 * {@see \Ferro\Http\Exception\BodyReadException}. With `stream: false` the body is read into a stream
 * from the factory before `sendRequest()` returns, and such a failure is a {@see NetworkException}.
 *
 * **Errors** follow §23.11.3's PSR-18 column: a refusal of the request itself (a `forbidden_*`
 * policy refusal, an unmapped origin, a body over the 16 MiB frame cap) is a
 * {@see RequestException}; every other failure is a {@see NetworkException}. A response the
 * factory cannot represent (a status of 600..=999) is an {@see UnrepresentableResponseException}.
 * Each carries a fate marker (`Ferro\Http\Fate\Retryable|NonRetryable|Indeterminate`); nothing here
 * retries — a non-idempotent request whose connection died after sending is Indeterminate.
 *
 * **Any status is a response** (PSR-18 forbids treating one as an error, §23.7.4).
 *
 * **Idempotency is a declaration** (§23.7.2): {@see withIdempotent()} returns a client whose requests
 * carry it; the default (null) leaves it to the operator's `IDEMPOTENT_METHODS`.
 */
final class Client implements ClientInterface
{
    private OriginMap $origins;
    /** @var \Closure(): Connection */
    private \Closure $connection;
    /** @var array<string, Upstream> */
    private array $upstreams = [];

    /**
     * @param Connection|\Closure(): Connection $connection the Ferro connection whose session carries
     *   the requests, or a closure that provides it on first use
     * @param array<string, string> $upstreams origin (`https://api.example.com[:port]`) => upstream name
     * @param ?int $timeoutMs the total-exchange bound sent with every request (null: the upstream's)
     * @param bool $decode ask the engine to decode `gzip`/`deflate` (the removed headers come back
     *   as `x-encoded-content-*`, as Guzzle's handlers rename them)
     * @param bool $stream return after the head with a lazy body (true), or read the body first
     */
    public function __construct(
        Connection|\Closure $connection,
        private ResponseFactoryInterface $responses,
        private StreamFactoryInterface $streams,
        array $upstreams,
        private ?int $timeoutMs = null,
        private ?int $connectTimeoutMs = null,
        private ?int $readTimeoutMs = null,
        private ?bool $idempotent = null,
        private bool $decode = true,
        private bool $stream = true,
    ) {
        $this->origins = new OriginMap($upstreams);
        $this->connection = ConnectionProvider::memoise($connection);
    }

    /** A client whose requests carry this idempotency declaration (§23.7.2). */
    public function withIdempotent(?bool $idempotent): self
    {
        $clone = clone $this;
        $clone->idempotent = $idempotent;
        return $clone;
    }

    /** A client with these total / connect / read bounds, in milliseconds (null: the upstream's). */
    public function withTimeouts(?int $timeoutMs, ?int $connectTimeoutMs = null, ?int $readTimeoutMs = null): self
    {
        $clone = clone $this;
        $clone->timeoutMs = $timeoutMs;
        $clone->connectTimeoutMs = $connectTimeoutMs;
        $clone->readTimeoutMs = $readTimeoutMs;
        return $clone;
    }

    public function sendRequest(RequestInterface $request): ResponseInterface
    {
        $resolved = $this->origins->resolve($request->getUri());
        if ($resolved === null) {
            throw new UnmappedOriginException(sprintf(
                'no Ferro upstream is mapped for origin %s; nothing was sent (map it, or use another client for it)',
                OriginMap::normaliseUri($request->getUri()) ?? '(none: the URI has no http(s) origin Ferro can map)',
            ), $request);
        }
        [$name, $origin] = $resolved;

        try {
            $out = OutboundRequest::from($request, $name);
        } catch (RequestTooLargeException $e) {
            throw new RequestException($e->getMessage(), $request, null, $e);
        }
        try {
            $upstream = $this->upstream($name, $origin);
        } catch (\Throwable $e) {
            throw NetworkException::create(
                'Ferro HTTP: no connection to the engine, nothing was sent: ' . $e->getMessage(),
                $request,
                Failure::noConnection()->fate,
                $e,
            );
        }
        try {
            $stream = $upstream->stream(
                $out->method,
                $out->target,
                $out->headers,
                $out->body,
                $this->timeoutMs,
                $this->connectTimeoutMs,
                $this->readTimeoutMs,
                $this->idempotent,
                $this->decode,
            );
        } catch (\Throwable $e) {
            throw $this->failure($request, $e);
        }

        $head = $stream->head;
        $fate = HttpFate::ofHead($head);
        try {
            $response = $this->responses->createResponse($head->status, ResponseParts::reason($head))
                ->withProtocolVersion(ResponseParts::version($head));
            foreach (ResponseParts::headers($head) as $headerName => $values) {
                $response = $response->withHeader($headerName, $values);
            }
        } catch (\InvalidArgumentException $e) {
            $stream->close();
            throw UnrepresentableResponseException::create(sprintf(
                'upstream "%s" answered with status %d, which the response factory cannot represent: %s',
                $name,
                $head->status,
                $e->getMessage(),
            ), $request, $fate, $e);
        }

        $body = new BodyStream($stream);
        if (!$this->stream) {
            try {
                $body = $this->streams->createStream($body->getContents());
            } catch (BodyReadException $e) {
                throw NetworkException::create($e->getMessage(), $request, $e->ferroFate(), $e->getPrevious());
            }
        }
        return new FatedResponse($response->withBody($body), $fate);
    }

    private function upstream(string $name, string $origin): Upstream
    {
        return $this->upstreams[$name . ' ' . $origin] ??= ($this->connection)()->upstream($name, $origin);
    }

    private function failure(RequestInterface $request, \Throwable $e): ClientExceptionInterface
    {
        $failure = Failure::of($e);
        $message = 'Ferro HTTP: ' . $e->getMessage();
        if ($failure->kind === Failure::REFUSED && $failure->fate->fate === FateClass::NonRetryable) {
            return new RequestException($message, $request, $failure->fate, $e);
        }
        return NetworkException::create($message, $request, $failure->fate, $e);
    }
}
