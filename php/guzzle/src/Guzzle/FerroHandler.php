<?php // /php/guzzle/src/Guzzle/FerroHandler.php
declare(strict_types=1);
namespace Ferro\Guzzle;

use Ferro\Client\Connection;
use Ferro\Future;
use Ferro\Http\Adapter\BodyStream;
use Ferro\Http\Adapter\ConnectionProvider;
use Ferro\Http\Adapter\Failure;
use Ferro\Http\Adapter\OriginMap;
use Ferro\Http\Adapter\OutboundRequest;
use Ferro\Http\Adapter\ResponseParts;
use Ferro\Http\Exception\BodyReadException;
use Ferro\Http\FateClass;
use Ferro\Http\FerroResponse;
use Ferro\Http\HttpFate;
use Ferro\Http\HttpResponse;
use Ferro\Http\HttpStream;
use Ferro\Http\ResponseHead;
use Ferro\Http\Upstream;
use GuzzleHttp\Multiplexing;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Promise\Promise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\LazyOpenStream;
use GuzzleHttp\Psr7\Utils as Psr7Utils;
use GuzzleHttp\TransferStats;
use GuzzleHttp\Utils;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\StreamInterface;

/**
 * **Ferro HTTP as a Guzzle handler** — the `HandlerStack` seam (SPEC §23.11.2):
 *
 *     $stack  = HandlerStack::create(new FerroHandler($conn, upstreams: [
 *         'https://api.openai.com' => 'openai',
 *     ]));
 *     $client = new GuzzleHttp\Client(['handler' => $stack]);
 *
 * Every middleware above it — redirects, cookies, `http_errors`, auth, retries, Laravel's recorder
 * and `Http::fake()` stubs — runs unchanged; only the transport is Ferro's. A request goes to the
 * upstream its origin is mapped to, over the connection's one multiplexed session, and is sent at
 * most once: nothing here re-sends anything (charter rule 3).
 *
 * **Routing.** The URI's normalised origin (lowercase ASCII host, default port removed) selects the
 * upstream; an unmapped origin is rejected with {@see UnmappedOriginException} and NOTHING is sent,
 * unless the handler was given an explicit `fallback:` handler — which then receives every
 * unmapped request and gives up the SSRF guarantee for them.
 *
 * **Three execution paths**, all of them sending exactly one `REQUEST`:
 *
 *  - **synchronous** (Guzzle's `synchronous` option, which `Client::request()`/`get()`/… set, as do
 *    Laravel's `Http::get()`/`post()`): run in `__invoke`, as Guzzle's own `CurlHandler` and
 *    `StreamHandler` run; the body is copied into the `sink` (by default `php://temp`, which spills to
 *    disk past 2 MB) chunk by chunk as it arrives, so PHP never holds more than one credit window of
 *    it;
 *  - **asynchronous, buffered** (`requestAsync()`, `Pool`, `Http::pool()`): the `REQUEST` is written in
 *    `__invoke`, so every request of a batch runs concurrently in the engine (P11), and the promise is
 *    settled by the handler's wait loop. The body is held in PHP memory until the promise settles —
 *    the native API's buffered form, which has no cap (§22.2 (dd); the cost is recorded in this
 *    slice's §22.2 entry);
 *  - **streamed** (`stream => true`): the response's body is a lazy {@see BodyStream} over the
 *    exchange, with `WINDOW_UPDATE` as it is read; `close()` or destruction CANCELs. An asynchronous
 *    streamed request is opened by the wait loop (as `CurlMultiHandler` attaches transfers when it
 *    ticks), which waits for its HEAD.
 *
 * **`delay`** is scheduled, never slept, on the asynchronous paths: a delayed request joins the
 * handler's queue and the wait loop submits it once due while other requests progress — so a `Pool`
 * of N requests each delayed d completes in about d, not N × d. A synchronous request sleeps first,
 * as `StreamHandler` does.
 *
 * **Errors** follow §23.11.3, cause by cause: curl's class for the same physical event
 * ({@see ConnectException} / {@see RequestException}), plus a fate marker. **Any status is a
 * fulfilled response** — `http_errors` above the handler throws on 4xx/5xx as stock. A status of
 * 600..=999, which `guzzlehttp/psr7` cannot represent, is rejected exactly as stock Guzzle rejects
 * it ("An error was encountered while creating the response"), with the status's fate as its marker.
 *
 * **Options** (§23.11.2's table): `timeout`/`connect_timeout`/`read_timeout` become the request's
 * bounds; `decode_content` asks the engine to decode and renames the removed headers
 * `x-encoded-content-*`; `sink`, `on_headers`, `progress`, `on_stats`, `on_trailers` behave as stock
 * (upload progress is reported once, when the head arrives; `on_stats`' handler stats use a subset of
 * curl's names); `ferro => ['idempotent' => bool, 'route' => string]` carries the declaration and the
 * observability route. `version`, `curl`, `stream_context`, `debug` and `expect` are ignored.
 * `verify => false`, `cert`, `ssl_key`, `crypto_method`, `force_ip_resolve`, a `proxy` other than
 * the one Guzzle derives from the environment, and a `multiplex` that REQUIRES multiplexing are
 * refused, each naming the daemon setting or the non-goal that replaces it (trust, client
 * certificates and the TLS floor are the operator's; v1 has no HTTP/2 and no outbound proxy).
 */
final class FerroHandler
{
    /** libcurl's option numbers, spelled here so the check needs no ext-curl (Guzzle does not require it). */
    private const CURLOPT_HTTPAUTH = 107;
    private const CURLOPT_USERPWD = 10005;

    private OriginMap $origins;
    /** @var \Closure(): Connection */
    private \Closure $connection;
    /** @var ?callable(RequestInterface, array<array-key, mixed>): PromiseInterface */
    private $fallback;
    /** @var array<string, Upstream> */
    private array $upstreams = [];
    private int $nextId = 0;
    /**
     * Requests the wait loop has yet to submit: delayed, or streamed (opened when waited).
     *
     * @var array<int, array{request: RequestInterface, options: array<array-key, mixed>, name: string, origin: string, due: float, promise: Promise}>
     */
    private array $queued = [];
    /**
     * Asynchronous buffered requests on the wire, settled by the wait loop.
     *
     * @var array<int, array{request: RequestInterface, options: array<array-key, mixed>, future: Future<HttpResponse>, promise: Promise, start: float, uploaded: int}>
     */
    private array $inFlight = [];

    /**
     * @param Connection|\Closure(): Connection $connection the Ferro connection whose session carries the
     *   requests, or a closure that provides it on first use
     * @param array<string, string> $upstreams origin (`https://api.example.com[:port]`) => upstream name
     * @param ?callable(RequestInterface, array<array-key, mixed>): PromiseInterface $fallback a handler for
     *   UNMAPPED origins (e.g. `Utils::chooseHandler()`); none by default, so an unmapped origin is
     *   refused — a fallback gives up the SSRF guarantee for every origin it serves
     */
    public function __construct(Connection|\Closure $connection, array $upstreams, ?callable $fallback = null)
    {
        $this->origins = new OriginMap($upstreams);
        $this->connection = ConnectionProvider::memoise($connection);
        $this->fallback = $fallback;
    }

    /** @param array<array-key, mixed> $options */
    public function __invoke(RequestInterface $request, array $options): PromiseInterface
    {
        $resolved = $this->origins->resolve($request->getUri());
        if ($resolved === null) {
            if ($this->fallback !== null) {
                return ($this->fallback)($request, $options);
            }
            return Create::rejectionFor(new UnmappedOriginException(sprintf(
                'no Ferro upstream is mapped for origin %s; nothing was sent (map it in the FerroHandler, '
                    . 'or give the handler an explicit fallback)',
                OriginMap::normaliseUri($request->getUri()) ?? '(none: the URI has no http(s) origin Ferro can map)',
            ), $request));
        }
        [$name, $origin] = $resolved;

        $refusal = self::refusedOption($options);
        if ($refusal !== null) {
            return Create::rejectionFor(new NonRetryableRequestException(
                $refusal . '; nothing was sent',
                $request,
                new HttpFate(FateClass::NonRetryable),
            ));
        }

        $delayMs = isset($options['delay']) && is_numeric($options['delay']) ? max(0, (int) $options['delay']) : 0;
        if (!empty($options['synchronous'])) {
            if ($delayMs > 0) {
                usleep($delayMs * 1000);
            }
            try {
                return Create::promiseFor($this->exchange($request, $options, $name, $origin));
            } catch (\Throwable $e) {
                return Create::rejectionFor($e);
            }
        }

        $id = ++$this->nextId;
        $promise = new Promise(
            function () use ($id): void { $this->waitFor($id); },
            function () use ($id): void { $this->cancel($id); },
        );
        if ($delayMs > 0 || !empty($options['stream'])) {
            $this->queued[$id] = [
                'request' => $request, 'options' => $options, 'name' => $name, 'origin' => $origin,
                'due' => microtime(true) + $delayMs / 1000, 'promise' => $promise,
            ];
        } else {
            $this->submit($id, $request, $options, $name, $origin, $promise);
        }
        return $promise;
    }

    /** How many asynchronous requests the wait loop has yet to submit or to settle. */
    public function pending(): int
    {
        return count($this->queued) + count($this->inFlight);
    }

    // ---- the wait loop ---------------------------------------------------------------------------

    /**
     * Run until request `$id` is settled: submit every queued request that has come due, and settle
     * the target. Another request's frames that arrive meanwhile are filed by the session for it,
     * never lost. A target still waiting for its delay sleeps only until the earliest queued request
     * is due, so a whole delayed batch goes out together.
     */
    private function waitFor(int $id): void
    {
        while (true) {
            $this->submitDue();
            if (isset($this->inFlight[$id])) {
                $this->settle($id);
                $this->submitDue();
                return;
            }
            if (!isset($this->queued[$id])) {
                return; // settled while submitting (a streamed request), or cancelled
            }
            $next = min(array_column($this->queued, 'due'));
            $sleep = $next - microtime(true);
            if ($sleep > 0) {
                usleep((int) ceil($sleep * 1_000_000));
            }
        }
    }

    private function submitDue(): void
    {
        $now = microtime(true);
        $due = array_keys(array_filter($this->queued, static fn (array $q): bool => $q['due'] <= $now));
        foreach ($due as $id) {
            // Taken one by one: opening a streamed request runs the caller's callbacks, which may
            // cancel another queued request meanwhile.
            $q = $this->take($id);
            if ($q === null) {
                continue;
            }
            if (!empty($q['options']['stream'])) {
                // A streamed request is opened here and its promise settled once its HEAD arrived.
                try {
                    $q['promise']->resolve($this->exchange($q['request'], $q['options'], $q['name'], $q['origin']));
                } catch (\Throwable $e) {
                    $q['promise']->reject($e);
                }
                continue;
            }
            $this->submit($id, $q['request'], $q['options'], $q['name'], $q['origin'], $q['promise']);
        }
    }

    /**
     * Remove and return queued request `$id`, if it is still queued.
     *
     * @return ?array{request: RequestInterface, options: array<array-key, mixed>, name: string, origin: string, due: float, promise: Promise}
     */
    private function take(int $id): ?array
    {
        $q = $this->queued[$id] ?? null;
        unset($this->queued[$id]);
        return $q;
    }

    /** @param array<array-key, mixed> $options */
    private function submit(int $id, RequestInterface $request, array $options, string $name, string $origin, Promise $promise): void
    {
        $start = microtime(true);
        try {
            $out = OutboundRequest::from($request, $name);
        } catch (\Throwable $e) {
            $promise->reject($this->rejection($request, $options, $start, $e));
            return;
        }
        try {
            $upstream = $this->upstream($name, $origin);
        } catch (\Throwable $e) {
            $promise->reject($this->rejection($request, $options, $start, $e, Failure::noConnection()));
            return;
        }
        $future = $upstream->requestAsync(
            $out->method,
            $out->target,
            $out->headers,
            $out->body,
            self::ms($options['timeout'] ?? null),
            self::ms($options['connect_timeout'] ?? null),
            self::ms($options['read_timeout'] ?? null),
            self::idempotent($options),
            self::decode($options),
            self::route($options),
        );
        $this->inFlight[$id] = [
            'request' => $request, 'options' => $options, 'future' => $future, 'promise' => $promise,
            'start' => $start, 'uploaded' => strlen($out->body ?? ''),
        ];
    }

    private function settle(int $id): void
    {
        $f = $this->inFlight[$id];
        unset($this->inFlight[$id]);
        try {
            $res = $f['future']->await();
        } catch (\Throwable $e) {
            $f['promise']->reject($this->rejection($f['request'], $f['options'], $f['start'], $e));
            return;
        }
        try {
            $f['promise']->resolve($this->buffered($f['request'], $f['options'], $res, $f['start'], $f['uploaded']));
        } catch (\Throwable $e) {
            $f['promise']->reject($e);
        }
    }

    private function cancel(int $id): void
    {
        // A queued request was never sent. An in-flight one's Future is dropped here, which CANCELs
        // it and discards its frames as they arrive (the native API's drop rule) — Guzzle then
        // rejects the promise with its own CancellationException.
        unset($this->queued[$id], $this->inFlight[$id]);
    }

    // ---- one exchange ----------------------------------------------------------------------------

    /**
     * The synchronous and the streamed path: send, wait for the HEAD, then either hand back a lazy
     * body (`stream`) or copy the body into the sink as it arrives.
     *
     * @param array<array-key, mixed> $options
     */
    private function exchange(RequestInterface $request, array $options, string $name, string $origin): ResponseInterface
    {
        $start = microtime(true);
        try {
            $out = OutboundRequest::from($request, $name);
        } catch (\Throwable $e) {
            throw $this->rejection($request, $options, $start, $e);
        }
        try {
            $upstream = $this->upstream($name, $origin);
        } catch (\Throwable $e) {
            throw $this->rejection($request, $options, $start, $e, Failure::noConnection());
        }
        try {
            $stream = $upstream->stream(
                $out->method,
                $out->target,
                $out->headers,
                $out->body,
                self::ms($options['timeout'] ?? null),
                self::ms($options['connect_timeout'] ?? null),
                self::ms($options['read_timeout'] ?? null),
                self::idempotent($options),
                self::decode($options),
                self::route($options),
            );
        } catch (\Throwable $e) {
            throw $this->rejection($request, $options, $start, $e);
        }
        $uploaded = strlen($out->body ?? '');
        $head = $stream->head;
        $progress = self::callable($options, 'progress');
        $total = self::contentLength($head);
        if ($progress !== null) {
            $progress(0, 0, $uploaded, $uploaded); // upload progress: once, the request is wholly sent
        }

        if (!empty($options['stream'])) {
            $response = null;
            $body = new BodyStream(
                $stream,
                $progress === null ? null : static function (int $read) use ($progress, $total, $uploaded): void {
                    $progress($total, $read, $uploaded, $uploaded);
                },
                function (?\Throwable $error) use ($request, $options, $start, $stream, &$response): void {
                    if ($error === null && $stream->isComplete()) {
                        $this->trailers($options, $stream->trailers() ?? [], $response);
                    }
                    $this->stats($options, $request, $response, $start, $stream->stats(), $error);
                },
            );
            $response = $this->response($request, $options, $start, $head, $body, $stream);
            $this->onHeaders($request, $options, $start, $response, $stream);
            return $response;
        }

        $sink = self::sink($options, $request);
        $response = $this->response($request, $options, $start, $head, $sink, $stream);
        $this->onHeaders($request, $options, $start, $response, $stream);
        $read = 0;
        $body = new BodyStream($stream);
        try {
            while (!$body->eof()) {
                $chunk = $body->read(1 << 20);
                if ($chunk === '') {
                    continue;
                }
                $sink->write($chunk);
                $read += strlen($chunk);
                if ($progress !== null) {
                    $progress($total, $read, $uploaded, $uploaded);
                }
            }
        } catch (BodyReadException $e) {
            if ($sink->isSeekable()) {
                $sink->seek(0);
            }
            $previous = $e->getPrevious() ?? $e;
            $error = RequestException::forFate(
                'Ferro HTTP: the response body failed after its head: ' . $previous->getMessage(),
                $request,
                $e->ferroFate(),
                $response,
                $previous,
                self::context($e->ferroFate()),
            );
            $this->stats($options, $request, $response, $start, $stream->stats(), $error);
            throw $error;
        }
        if ($sink->isSeekable()) {
            $sink->seek(0);
        }
        $response = self::decodedLength($response, $head, $read);
        $this->trailers($options, $stream->trailers() ?? [], $response);
        $this->stats($options, $request, $response, $start, $stream->stats(), null);
        return $response;
    }

    /**
     * The asynchronous buffered path's response, built once the whole exchange is in.
     *
     * @param array<array-key, mixed> $options
     */
    private function buffered(RequestInterface $request, array $options, HttpResponse $res, float $start, int $uploaded): ResponseInterface
    {
        $sink = self::sink($options, $request);
        $response = $this->response($request, $options, $start, $res->head, $sink, null);
        $this->onHeaders($request, $options, $start, $response, null);
        $progress = self::callable($options, 'progress');
        if ($progress !== null) {
            $progress(0, 0, $uploaded, $uploaded);
        }
        if ($res->body !== '') {
            $sink->write($res->body);
            if ($progress !== null) {
                $progress(self::contentLength($res->head) ?: strlen($res->body), strlen($res->body), $uploaded, $uploaded);
            }
        }
        if ($sink->isSeekable()) {
            $sink->seek(0);
        }
        $response = self::decodedLength($response, $res->head, strlen($res->body));
        $this->trailers($options, $res->trailers, $response);
        $this->stats($options, $request, $response, $start, $res->stats, null);
        return $response;
    }

    /**
     * The `FerroResponse` for a head, or the rejection stock Guzzle produces when `guzzlehttp/psr7`
     * cannot represent what the upstream sent — a status of 600..=999 above all (§23.5.2) — with
     * that status's fate as its marker: the upstream answered, so for a non-idempotent request it is
     * Indeterminate (RFC 9110 §15 reads it as a 5xx), never Retryable.
     *
     * @param array<array-key, mixed> $options
     */
    private function response(RequestInterface $request, array $options, float $start, ResponseHead $head, StreamInterface $body, ?HttpStream $stream): FerroResponse
    {
        $fate = HttpFate::ofHead($head);
        try {
            return new FerroResponse(
                $fate,
                $head->status,
                ResponseParts::headers($head),
                $body,
                ResponseParts::version($head),
                ResponseParts::reason($head),
            );
        } catch (\Throwable $e) {
            $stream?->close();
            $error = RequestException::forFate(
                'An error was encountered while creating the response',
                $request,
                self::markerFate($fate),
                null,
                $e,
                self::context($fate),
            );
            $this->stats($options, $request, null, $start, $stream?->stats(), $error);
            throw $error;
        }
    }

    /** @param array<array-key, mixed> $options */
    private function onHeaders(RequestInterface $request, array $options, float $start, ResponseInterface $response, ?HttpStream $stream): void
    {
        $onHeaders = self::callable($options, 'on_headers');
        if ($onHeaders === null) {
            return;
        }
        try {
            $onHeaders($response);
        } catch (\Throwable $e) {
            $stream?->close(); // CANCEL + drain: the caller does not want this body
            $fate = $response instanceof FerroResponse ? self::markerFate($response->ferroFate()) : new HttpFate(FateClass::NonRetryable);
            $error = RequestException::forFate('An error was encountered during the on_headers event', $request, $fate, $response, $e, self::context($fate));
            $this->stats($options, $request, $response, $start, $stream?->stats(), $error);
            throw $error;
        }
    }

    /**
     * A failure of the native API as the Guzzle rejection §23.11.3 assigns to its cause.
     *
     * @param array<array-key, mixed> $options
     */
    private function rejection(RequestInterface $request, array $options, float $start, \Throwable $e, ?Failure $failure = null): \Throwable
    {
        $failure ??= Failure::of($e);
        $response = null;
        if ($failure->head !== null) {
            try {
                $response = new FerroResponse(
                    HttpFate::ofHead($failure->head),
                    $failure->head->status,
                    ResponseParts::headers($failure->head),
                    '',
                    ResponseParts::version($failure->head),
                    ResponseParts::reason($failure->head),
                );
            } catch (\Throwable) {
                $response = null; // a head psr7 cannot represent: the rejection carries none
            }
        }
        $cause = $failure->fate->cause;
        $message = sprintf('Ferro HTTP%s: %s', $cause !== null ? " ({$cause})" : '', $e->getMessage());
        $error = $failure->kind === Failure::CONNECT
            ? ConnectException::forFate($message, $request, $failure->fate, $e, self::context($failure->fate))
            : RequestException::forFate($message, $request, $failure->fate, $response, $e, self::context($failure->fate));
        $this->stats($options, $request, $response, $start, null, $error);
        return $error;
    }

    // ---- stock callbacks ---------------------------------------------------------------------------

    /**
     * @param array<array-key, mixed> $options
     * @param list<array{0:string,1:string}> $trailers
     */
    private function trailers(array $options, array $trailers, ?ResponseInterface $response): void
    {
        $onTrailers = self::callable($options, 'on_trailers');
        if ($onTrailers === null || $response === null) {
            return;
        }
        $grouped = [];
        foreach ($trailers as [$name, $value]) {
            $grouped[strtolower($name)][] = $value;
        }
        $onTrailers($grouped, $response);
    }

    /**
     * @param array<array-key, mixed> $options
     * @param ?array{queue_us:int,connect_us:int,tls_us:int,ttfb_us:int,total_us:int,bytes_sent:int,bytes_received:int,reused:bool} $stats
     */
    private function stats(array $options, RequestInterface $request, ?ResponseInterface $response, float $start, ?array $stats, ?\Throwable $error): void
    {
        $onStats = self::callable($options, 'on_stats');
        if ($onStats === null) {
            return;
        }
        $handlerStats = ['url' => (string) $request->getUri(), 'http_code' => $response?->getStatusCode() ?? 0];
        $transfer = microtime(true) - $start;
        if ($stats !== null) {
            $s = static fn (int $us): float => $us / 1_000_000;
            $transfer = $s($stats['total_us']);
            $handlerStats += [
                'total_time' => $transfer,
                'connect_time' => $s($stats['queue_us'] + $stats['connect_us']),
                'appconnect_time' => $s($stats['queue_us'] + $stats['connect_us'] + $stats['tls_us']),
                'starttransfer_time' => $s($stats['ttfb_us']),
                'size_upload' => $stats['bytes_sent'],
                'size_download' => $stats['bytes_received'],
                'ferro' => $stats,
            ];
        }
        $onStats(new TransferStats($request, $response, $transfer, $error, $handlerStats));
    }

    // ---- options ---------------------------------------------------------------------------------

    /**
     * Why an option is refused (§23.11.2's table), or null.
     *
     * @param array<array-key, mixed> $options
     */
    private static function refusedOption(array $options): ?string
    {
        if (($options['verify'] ?? true) === false) {
            return 'the "verify" => false option is refused: certificate trust is the daemon\'s (set the upstream\'s CA_FILE, SPEC §23.3.1)';
        }
        foreach (['cert' => 'CLIENT_CERT_FILE', 'ssl_key' => 'CLIENT_KEY_FILE', 'crypto_method_max' => 'MIN_TLS'] as $option => $key) {
            if (isset($options[$option])) {
                return "the \"{$option}\" option is refused: it is the daemon's upstream setting {$key} (SPEC §23.3.1)";
            }
        }
        // `crypto_method` is a FLOOR (curl's CURL_SSLVERSION_*: "this version or later"), and Laravel's
        // PendingRequest sets TLS 1.2 on every request. The daemon's `MIN_TLS` is never below 1.2, so a
        // floor of 1.2 or lower is already met; only a TLS 1.3 floor, which the client cannot see the
        // upstream honouring, is refused.
        if (isset($options['crypto_method']) && $options['crypto_method'] === STREAM_CRYPTO_METHOD_TLSv1_3_CLIENT) {
            return 'the "crypto_method" option asks for TLS 1.3, which the client cannot verify: the TLS floor is the daemon\'s upstream setting MIN_TLS (SPEC §23.3.1)';
        }
        // `auth => [user, pass, 'digest'|'ntlm']`: GuzzleHttp\Client applies only Basic itself; any
        // other scheme rides the `curl` option (CURLOPT_HTTPAUTH/USERPWD), which no Ferro path can
        // honour. Ignoring it would send the request UNAUTHENTICATED — refused instead (review).
        $auth = $options['auth'] ?? null;
        if (is_array($auth) && $auth !== [] && isset($auth[2]) && is_string($auth[2]) && strtolower($auth[2]) !== 'basic') {
            return sprintf('the "auth" option\'s "%s" scheme is refused: Guzzle applies it through curl, which Ferro HTTP does not use, so the request would go out unauthenticated; send the Authorization header yourself, or attach it daemon-side (ATTACH_HEADERS_FILE, SPEC §23.3.1)', $auth[2]);
        }
        $curl = $options['curl'] ?? null;
        if (is_array($curl) && (array_key_exists(self::CURLOPT_HTTPAUTH, $curl) || array_key_exists(self::CURLOPT_USERPWD, $curl))) {
            return 'the "curl" option carries HTTP authentication (CURLOPT_HTTPAUTH/CURLOPT_USERPWD), which Ferro HTTP cannot apply, so the request would go out unauthenticated; refused';
        }
        if (isset($options['force_ip_resolve'])) {
            return 'the "force_ip_resolve" option is refused: the engine resolves the upstream and checks every address (SPEC §23.8.5)';
        }
        if (isset($options['proxy']) && $options['proxy'] !== self::environmentProxy()) {
            return 'the "proxy" option is refused: Ferro HTTP v1 sends to the upstream directly and has no outbound proxy (SPEC §23.2)';
        }
        $multiplex = $options['multiplex'] ?? null;
        if ($multiplex === Multiplexing::REQUIRE_EAGER || $multiplex === Multiplexing::REQUIRE_WAIT) {
            return 'the "multiplex" option requires multiplexing, which HTTP/2 provides and Ferro HTTP v1 does not offer (every request rides HTTP/1.1, SPEC §22.2 (de))';
        }
        return null;
    }

    /**
     * The `proxy` default `GuzzleHttp\Client` derives from `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY`
     * (its `configureDefaults`), which is ignored rather than refused: it is environment, not a
     * choice the application made, and Ferro does not honour the proxy environment (§23.16 C9 item 1).
     *
     * @return ?array<string, mixed>
     */
    private static function environmentProxy(): ?array
    {
        $proxy = [];
        if (\PHP_SAPI === 'cli' && ($http = Utils::getenv('HTTP_PROXY'))) {
            $proxy['http'] = $http;
        }
        if ($https = Utils::getenv('HTTPS_PROXY')) {
            $proxy['https'] = $https;
        }
        if ($no = Utils::getenv('NO_PROXY')) {
            $proxy['no'] = explode(',', str_replace(' ', '', $no));
        }
        return $proxy === [] ? null : $proxy;
    }

    /** Guzzle seconds (0 = none) as the wire's milliseconds (null = the upstream's). */
    private static function ms(mixed $seconds): ?int
    {
        if (!is_numeric($seconds) || (float) $seconds <= 0) {
            return null;
        }
        return (int) min(4_294_967_295, max(1, ceil((float) $seconds * 1000)));
    }

    /** @param array<array-key, mixed> $options */
    private static function idempotent(array $options): ?bool
    {
        $ferro = $options['ferro'] ?? null;
        $value = is_array($ferro) ? ($ferro['idempotent'] ?? null) : null;
        return is_bool($value) ? $value : null;
    }

    /** @param array<array-key, mixed> $options */
    private static function route(array $options): ?string
    {
        $ferro = $options['ferro'] ?? null;
        $value = is_array($ferro) ? ($ferro['route'] ?? null) : null;
        return is_string($value) ? $value : null;
    }

    /** @param array<array-key, mixed> $options */
    private static function decode(array $options): bool
    {
        return ($options['decode_content'] ?? true) !== false;
    }

    /** @param array<array-key, mixed> $options */
    private static function callable(array $options, string $key): ?callable
    {
        $value = $options[$key] ?? null;
        if ($value === null) {
            return null;
        }
        if (!is_callable($value)) {
            throw new \InvalidArgumentException("{$key} must be callable");
        }
        return $value;
    }

    /** @param array<array-key, mixed> $options */
    private static function sink(array $options, RequestInterface $request): StreamInterface
    {
        $sink = $options['sink'] ?? null;
        if ($sink === null) {
            return Psr7Utils::streamFor(Psr7Utils::tryFopen('php://temp', 'r+'));
        }
        if (is_string($sink)) {
            return new LazyOpenStream($sink, 'w+');
        }
        if ($sink instanceof StreamInterface || is_resource($sink)) {
            return Psr7Utils::streamFor($sink);
        }
        throw new \InvalidArgumentException('sink must be a file name, a resource or a StreamInterface');
    }

    private static function contentLength(ResponseHead $head): int
    {
        $value = $head->header('content-length');
        return $value !== null && ctype_digit($value) ? (int) $value : 0;
    }

    /**
     * `EasyHandle`'s rule for a body curl decoded: the encoded `Content-Length` moves to
     * `x-encoded-content-length` and `Content-Length` becomes the decoded size (or goes).
     */
    private static function decodedLength(ResponseInterface $response, ResponseHead $head, int $bodyLength): ResponseInterface
    {
        if ($head->decoded === null || $head->decoded[1] === null) {
            return $response;
        }
        return $bodyLength > 0 ? $response->withHeader('Content-Length', (string) $bodyLength) : $response->withoutHeader('Content-Length');
    }

    /** A status verdict as a MARKER: a response that is not a failure was applied, so NonRetryable. */
    private static function markerFate(HttpFate $fate): HttpFate
    {
        return $fate->fate === FateClass::NotAFailure
            ? new HttpFate(FateClass::NonRetryable, $fate->retryAfterMs, $fate->idempotent, $fate->status, $fate->cause, $fate->clientSynthesised)
            : $fate;
    }

    /** @return array<string, mixed> */
    private static function context(HttpFate $fate): array
    {
        return ['ferro_cause' => $fate->cause, 'ferro_fate' => $fate->fate->value];
    }

    private function upstream(string $name, string $origin): Upstream
    {
        return $this->upstreams[$name . ' ' . $origin] ??= ($this->connection)()->upstream($name, $origin);
    }
}
