<?php // /php/client/src/Protocol/HttpHead.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for the HTTP `HEAD` message (service `HTTP`, method `HEAD` = 2; M6-F2,
 * SPEC §23.5.2, /proto/PROTOCOL.md §12.2) — engine → client, sent once per response before any
 * `BODY`, not terminal and not flagged `STREAM`. Mirrors the Rust `messages::http::HttpHead` BYTES:
 * a fixarray of 6 —
 *   [status: u16, version: u8, reason: bin|nil, headers: array<[str, bin]>,
 *    decoded: [str, u64|nil]|nil, idempotent: bool].
 *
 * Decoding is STRICT on type and width ({@see HttpWire}); it does NOT apply the producer's semantic
 * contract (status 200..=599, version 10/11/20), which the engine owns and the native API (F8) may
 * check — the codec moves what the wire carries. `idempotent` is the engine's EFFECTIVE idempotency
 * for the request (§23.7.2), the one authority a client classifies against after `HEAD` (§23.7.3).
 */
final class HttpHead
{
    public const ARITY = 6;

    /**
     * @param array<string,mixed> $m
     * @return string the encoded fixarray(6) payload
     */
    public static function encode(array $m, PackerInterface $p): string
    {
        $reason = HttpWire::nullableString($m['reason'] ?? null, 'HttpHead reason');
        $decoded = $m['decoded'] ?? null;
        if ($decoded === null) {
            $decodedBytes = $p->packNil();
        } else {
            $d = HttpWire::arity($decoded, 2, 'HttpHead decoded');
            $length = HttpWire::nullableUint($d[1], PHP_INT_MAX, 'HttpHead decoded content_length');
            $decodedBytes = $p->packArrayLen(2)
                . $p->packStr(HttpWire::utf8($d[0], 'HttpHead decoded content_encoding'))
                . ($length === null ? $p->packNil() : $p->packUint($length));
        }
        return $p->packArrayLen(self::ARITY)
            . $p->packUint(HttpWire::uint($m['status'] ?? null, 0xFFFF, 'HttpHead status'))
            . $p->packUint(HttpWire::uint($m['version'] ?? null, 0xFF, 'HttpHead version'))
            . ($reason === null ? $p->packNil() : $p->packBin($reason))
            . HttpWire::packHeaders($p, HttpWire::headersIn($m['headers'] ?? [], 'HttpHead'))
            . $decodedBytes
            . $p->packBool(HttpWire::bool($m['idempotent'] ?? null, 'HttpHead idempotent'));
    }

    /**
     * Decode a whole `HEAD` frame payload.
     * @return array{status:int,version:int,reason:?string,headers:list<array{0:string,1:string}>,decoded:?array{0:string,1:?int},idempotent:bool}
     */
    public static function decode(string $payload, PackerInterface $p): array
    {
        return self::mapFromWire(HttpWire::unpackArray($payload, $p, self::ARITY, 'HttpHead'));
    }

    /**
     * @param array<array-key,mixed> $w
     * @return array{status:int,version:int,reason:?string,headers:list<array{0:string,1:string}>,decoded:?array{0:string,1:?int},idempotent:bool}
     */
    public static function mapFromWire(array $w): array
    {
        $w = HttpWire::arity($w, self::ARITY, 'HttpHead');
        $decoded = null;
        if ($w[4] !== null) {
            $d = HttpWire::arity($w[4], 2, 'HttpHead decoded');
            $decoded = [
                HttpWire::string($d[0], 'HttpHead decoded content_encoding'),
                HttpWire::nullableUint($d[1], PHP_INT_MAX, 'HttpHead decoded content_length'),
            ];
        }
        return [
            'status' => HttpWire::uint($w[0], 0xFFFF, 'HttpHead status'),
            'version' => HttpWire::uint($w[1], 0xFF, 'HttpHead version'),
            'reason' => HttpWire::nullableString($w[2], 'HttpHead reason'),
            'headers' => HttpWire::headersOut($w[3], 'HttpHead'),
            'decoded' => $decoded,
            'idempotent' => HttpWire::bool($w[5], 'HttpHead idempotent'),
        ];
    }
}
