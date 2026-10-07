<?php // /php/client/src/Http/ResponseHead.php
declare(strict_types=1);
namespace Ferro\Http;

/**
 * A response head as the engine delivered it (`HttpHead`, SPEC §23.5.2): the status line, the
 * headers and the engine's EFFECTIVE idempotency for the request. Shared by {@see HttpResponse},
 * {@see HttpStream} and {@see Error\ResponseIncompleteException}.
 *
 * Header names are lowercase, because the engine delivers them so (P6: `hyper` does not keep a
 * response name's original case). {@see $headers} groups values by name in arrival order;
 * {@see $headerLines} is the list as received, duplicates and order kept.
 */
final class ResponseHead
{
    /**
     * @param array<string, list<string>> $headers
     * @param list<array{0:string,1:string}> $headerLines
     * @param ?array{0:string,1:?int} $decoded the `Content-Encoding` and `Content-Length` the engine
     *   removed when it decoded the body (§23.9.2), as received; null when it decoded nothing
     */
    public function __construct(
        public readonly int $status,
        /** `10`, `11` or `20` (HTTP/1.0, 1.1, 2). */
        public readonly int $version,
        /** The HTTP/1.x reason phrase as received (bytes); null on HTTP/2. */
        public readonly ?string $reason,
        public readonly array $headers,
        public readonly array $headerLines,
        public readonly ?array $decoded,
        /** The engine's effective idempotency (§23.7.2): what a status or a failure is judged by. */
        public readonly bool $idempotent,
    ) {}

    /** @param array{status:int,version:int,reason:?string,headers:list<array{0:string,1:string}>,decoded:?array{0:string,1:?int},idempotent:bool} $head */
    public static function fromWire(array $head): self
    {
        $grouped = [];
        foreach ($head['headers'] as [$name, $value]) {
            $grouped[$name][] = $value;
        }
        return new self(
            $head['status'],
            $head['version'],
            $head['reason'],
            $grouped,
            $head['headers'],
            $head['decoded'],
            $head['idempotent'],
        );
    }

    /** The first value of header `$name` (case-insensitive), or null when it is absent. */
    public function header(string $name): ?string
    {
        return $this->headers[strtolower($name)][0] ?? null;
    }

    /** {@see StatusFate}'s advisory verdict on this head's status, with its `Retry-After`. */
    public function statusFate(?float $now = null): StatusFate
    {
        return StatusFate::of($this->status, $this->idempotent, $this->header('retry-after'), $now);
    }
}
