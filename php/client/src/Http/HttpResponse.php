<?php // /php/client/src/Http/HttpResponse.php
declare(strict_types=1);
namespace Ferro\Http;

/**
 * A completed Ferro HTTP exchange, body buffered ({@see Upstream::request}, SPEC §23.11.1).
 *
 * **Any final status is a response, never an exception** (§23.7.4): a 404 or a 503 arrives here
 * like a 200. What a status means for a retry is {@see statusFate}'s advisory table, judged by
 * {@see $idempotent} — the engine's effective idempotency for this request.
 */
final class HttpResponse
{
    public readonly int $status;
    /** `10`, `11` or `20`. The upstream's `HTTP` setting decides it, never the request (§23.8.3). */
    public readonly int $version;
    public readonly ?string $reason;
    /** @var array<string, list<string>> values by lowercase name, in arrival order */
    public readonly array $headers;
    /** @var list<array{0:string,1:string}> every header line as received (lowercase names) */
    public readonly array $headerLines;
    /** @var ?array{0:string,1:?int} the removed `Content-Encoding`/`Content-Length` when the engine decoded the body */
    public readonly ?array $decoded;
    /** The engine's effective idempotency (§23.7.2). */
    public readonly bool $idempotent;

    /**
     * @param list<array{0:string,1:string}> $trailers
     * @param array{queue_us:int,connect_us:int,tls_us:int,ttfb_us:int,total_us:int,bytes_sent:int,bytes_received:int,reused:bool} $stats
     */
    public function __construct(
        public readonly ResponseHead $head,
        public readonly string $body,
        /** @var list<array{0:string,1:string}> */
        public readonly array $trailers,
        /** @var array{queue_us:int,connect_us:int,tls_us:int,ttfb_us:int,total_us:int,bytes_sent:int,bytes_received:int,reused:bool} the engine's `HttpStats` (§23.5.4) */
        public readonly array $stats,
    ) {
        $this->status = $head->status;
        $this->version = $head->version;
        $this->reason = $head->reason;
        $this->headers = $head->headers;
        $this->headerLines = $head->headerLines;
        $this->decoded = $head->decoded;
        $this->idempotent = $head->idempotent;
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
