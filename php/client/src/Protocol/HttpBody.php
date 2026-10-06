<?php // /php/client/src/Protocol/HttpBody.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for the HTTP `BODY` message (service `HTTP`, method `BODY` = 3; M6-F2,
 * SPEC §23.5.3, /proto/PROTOCOL.md §12.3) — engine → client, in a frame carrying the `STREAM` flag.
 * Mirrors the Rust `messages::http::HttpBody` BYTES: `[chunk: bin]`. The chunk's size bound and
 * non-empty rule are the engine producer's contract, not a codec check.
 */
final class HttpBody
{
    public const ARITY = 1;

    /** @param array<string,mixed> $m @return string the encoded fixarray(1) payload */
    public static function encode(array $m, PackerInterface $p): string
    {
        return $p->packArrayLen(self::ARITY)
            . $p->packBin(HttpWire::string($m['chunk'] ?? null, 'HttpBody chunk'));
    }

    /** Decode a whole `BODY` frame payload to its chunk (a binary string). */
    public static function decode(string $payload, PackerInterface $p): string
    {
        $w = HttpWire::unpackArray($payload, $p, self::ARITY, 'HttpBody');
        return HttpWire::string($w[0], 'HttpBody chunk');
    }
}
