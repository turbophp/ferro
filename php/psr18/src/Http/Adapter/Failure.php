<?php // /php/psr18/src/Http/Adapter/Failure.php
declare(strict_types=1);
namespace Ferro\Http\Adapter;

use Ferro\Client\Error\InFlightLimitException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Http\Error\HttpCancelledException;
use Ferro\Http\Error\HttpException;
use Ferro\Http\Error\RequestTooLargeException;
use Ferro\Http\Error\ResponseIncompleteException;
use Ferro\Http\FateClass;
use Ferro\Http\HttpFate;
use Ferro\Http\ResponseHead;
use Ferro\Protocol\Generated\Constants as C;

/**
 * **SPEC §23.11.3's table, once, for both adapters**: which CLASS of exception a Ferro HTTP failure
 * becomes, chosen by its cause token so that every cell gets the class curl would have produced for
 * the same physical event, and which fate marker it carries.
 *
 * Three kinds:
 *
 *  - {@see CONNECT} — Guzzle `ConnectException`, PSR-18 `NetworkExceptionInterface`. curl's
 *    `CurlFactory` makes a `ConnectException` for exactly 28 (timeout), 6 (resolve), 7 (connect),
 *    35 (TLS connect) and 52 (got nothing); Ferro's analogues are the dial causes, the TLS
 *    handshake causes other than `tls_verify`, `timeout`/`read_idle` (28), `eof_empty` (52), and the
 *    admission refusals, which never reached the server at all;
 *  - {@see REQUEST} — Guzzle `RequestException`, PSR-18 `NetworkExceptionInterface`: everything curl
 *    reports as 55/56/8/60/18 — a reset, a partial or malformed head, a body that failed after the
 *    head, `tls_verify` (60), and "dispatched, not sent";
 *  - {@see REFUSED} — Guzzle `RequestException`, PSR-18 `RequestExceptionInterface`: the request
 *    itself was refused before anything was sent — a `forbidden_*` policy refusal, a body over the
 *    frame cap, an engine without HTTP, an invalid request.
 *
 * **The marker is the fate, never the class.** So a naive `instanceof ConnectException` decider
 * re-sends exactly what it re-sends under curl (a timeout, got-nothing) and not a reset after
 * sending — while Ferro's own decider reads the marker and never re-sends an Indeterminate POST,
 * whichever class carries it.
 *
 * @internal shared by `ferro/guzzle` and `ferro/psr18`
 */
final class Failure
{
    public const CONNECT = 'connect';
    public const REQUEST = 'request';
    public const REFUSED = 'refused';

    /**
     * Every `[http.causes]` token and its kind. TOTAL over the registry — a test requires the key set
     * to equal `Constants::HTTP_CAUSES` — so a token added to `/proto` fails the build here until its
     * class is decided.
     */
    public const CAUSE_KIND = [
        // Not sent: policy.
        C::HTTP_CAUSE_FORBIDDEN_UPSTREAM => self::REFUSED,
        C::HTTP_CAUSE_FORBIDDEN_ORIGIN => self::REFUSED,
        C::HTTP_CAUSE_FORBIDDEN_TARGET => self::REFUSED,
        C::HTTP_CAUSE_FORBIDDEN_METHOD => self::REFUSED,
        C::HTTP_CAUSE_FORBIDDEN_HEADER => self::REFUSED,
        C::HTTP_CAUSE_FORBIDDEN_BODY => self::REFUSED,
        C::HTTP_CAUSE_FORBIDDEN_ADDRESS => self::REFUSED,
        // Not sent: admission. Never reached the server; no curl analogue (§23.11.3).
        C::HTTP_CAUSE_BREAKER_OPEN => self::CONNECT,
        C::HTTP_CAUSE_BREAKER_PROBE_BUSY => self::CONNECT,
        C::HTTP_CAUSE_RATE_LIMITED => self::CONNECT,
        C::HTTP_CAUSE_RETRY_AFTER_HOLD => self::CONNECT,
        C::HTTP_CAUSE_QUEUE_FULL => self::CONNECT,
        C::HTTP_CAUSE_QUEUE_TIMEOUT => self::CONNECT,
        C::HTTP_CAUSE_BODY_BUDGET => self::CONNECT,
        C::HTTP_CAUSE_DEADLINE => self::CONNECT,
        C::HTTP_CAUSE_DRAINING => self::CONNECT,
        // Not sent: dial. curl 6 / 7 / 28 / 35 — and 60 for verification.
        C::HTTP_CAUSE_DNS => self::CONNECT,
        C::HTTP_CAUSE_CONNECT_REFUSED => self::CONNECT,
        C::HTTP_CAUSE_CONNECT_UNREACHABLE => self::CONNECT,
        C::HTTP_CAUSE_CONNECT_TIMEOUT => self::CONNECT,
        C::HTTP_CAUSE_TLS_HANDSHAKE => self::CONNECT,
        C::HTTP_CAUSE_TLS_VERSION => self::CONNECT,
        C::HTTP_CAUSE_TLS_ALPN => self::CONNECT,
        C::HTTP_CAUSE_TLS_VERIFY => self::REQUEST,
        // Dispatched, not sent (§22.2 (ct)): curl 55 / 56.
        C::HTTP_CAUSE_UNSENT_WRITE => self::REQUEST,
        C::HTTP_CAUSE_UNSENT_CLOSED => self::REQUEST,
        // Sent, no head.
        C::HTTP_CAUSE_TIMEOUT => self::CONNECT, // 28
        C::HTTP_CAUSE_EOF_EMPTY => self::CONNECT, // 52
        C::HTTP_CAUSE_WRITE => self::REQUEST, // 55
        C::HTTP_CAUSE_RESET => self::REQUEST, // 56
        C::HTTP_CAUSE_EOF_PARTIAL_HEAD => self::REQUEST, // 8
        C::HTTP_CAUSE_MALFORMED_HEAD => self::REQUEST, // 8
        C::HTTP_CAUSE_OVERSIZE_HEAD => self::REQUEST,
        C::HTTP_CAUSE_INFORMATIONAL_101 => self::REQUEST,
        // A CANCEL the CALLER did not ask for (the D1c backstop; a user's own cancel never reaches a
        // decider — its promise rejects with Guzzle's CancellationException). Reported as the
        // timeout it stands in for (§23.7.1: "cause `timeout`, as a cancelled SQL autocommit write").
        C::HTTP_CAUSE_CANCELLED => self::CONNECT,
        // HTTP/2 (reserved; unreachable in v1, SPEC §22.2 (de)).
        C::HTTP_CAUSE_H2_REFUSED_STREAM => self::REQUEST,
        C::HTTP_CAUSE_H2_GOAWAY_ABOVE_LAST => self::REQUEST,
        C::HTTP_CAUSE_H2_STREAM_ERROR => self::REQUEST,
        C::HTTP_CAUSE_H2_CONNECTION_ERROR => self::REQUEST,
        // After the head: curl 18 / 56 — and 28 for an idle read.
        C::HTTP_CAUSE_BODY_RESET => self::REQUEST,
        C::HTTP_CAUSE_BODY_EOF => self::REQUEST,
        C::HTTP_CAUSE_BODY_FRAMING => self::REQUEST,
        C::HTTP_CAUSE_DECODE => self::REQUEST,
        C::HTTP_CAUSE_MAX_RESPONSE_BYTES => self::REQUEST,
        C::HTTP_CAUSE_READ_IDLE => self::CONNECT,
    ];

    private function __construct(
        /** {@see CONNECT}, {@see REQUEST} or {@see REFUSED}. */
        public readonly string $kind,
        public readonly HttpFate $fate,
        /** The head that arrived before the failure, if any: the adapter attaches a response. */
        public readonly ?ResponseHead $head,
    ) {}

    /**
     * The adapter could not obtain a Ferro connection at all (the socket refused, the handshake
     * failed): nothing was sent anywhere, so Retryable, and reported as a dial failure.
     */
    public static function noConnection(): self
    {
        return new self(self::CONNECT, new HttpFate(FateClass::Retryable, cause: HttpException::CLIENT_LINK_LOST, clientSynthesised: true), null);
    }

    /**
     * Classify a failure thrown by the native API. `$head` is a head the ADAPTER already holds
     * (the streamed paths read it before the body failed); a `ResponseIncomplete` carries its own.
     */
    public static function of(\Throwable $e, ?ResponseHead $head = null): self
    {
        $fate = HttpFate::ofFailure($e) ?? new HttpFate(FateClass::NonRetryable);
        if ($e instanceof ResponseIncompleteException) {
            $head = $e->head();
        } elseif ($e instanceof HttpCancelledException && $e->head() !== null) {
            $head = $e->head();
        }
        if ($head !== null) {
            // After the head, every failure is "the body failed": a RequestException carrying the
            // response (§23.11.3's after-HEAD row), whatever the cause. curl would report an idle
            // read as 28 (ConnectException, no response); keeping the response the upstream DID send
            // is worth more than that cell, and a `ConnectException` cannot carry one.
            return new self(self::REQUEST, $fate, $head);
        }
        if ($e instanceof HttpException) {
            $cause = $e->cause();
            if ($e->clientSynthesised()) {
                // §23.7.3, the link to the engine died. Not sent (or declared idempotent): as a dial
                // failure, which naive deciders retry. Sent with no head on a non-idempotent request:
                // as curl's 56, which they do not.
                $kind = $fate->fate === FateClass::Retryable ? self::CONNECT : self::REQUEST;
                return new self($kind, $fate, null);
            }
            return new self(self::CAUSE_KIND[$cause] ?? self::REQUEST, $fate, null);
        }
        if ($e instanceof InFlightLimitException) {
            return new self(self::CONNECT, $fate, null); // not sent; a local slot limit
        }
        if ($e instanceof RequestTooLargeException || $e instanceof \InvalidArgumentException) {
            return new self(self::REFUSED, $fate, null);
        }
        if ($e instanceof NonRetryableException && $e->errorCode() === C::ERR_UNSUPPORTED) {
            return new self(self::REFUSED, $fate, null); // this engine does not serve Ferro HTTP
        }
        // A protocol fault, a lost link classified elsewhere, a usage error: NonRetryable (or its own
        // taxonomy fate), reported as curl's catch-all.
        return new self(self::REQUEST, $fate, null);
    }
}
