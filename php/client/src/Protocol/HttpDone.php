<?php // /php/client/src/Protocol/HttpDone.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for `HttpDone`, the terminal `Outcome::Ok` body of a completed HTTP exchange —
 * WHATEVER its status (SPEC §23.7.4; M6-F2, SPEC §23.5.4, /proto/PROTOCOL.md §12.4). Mirrors the Rust
 * `messages::http::HttpDone` BYTES: `[trailers: array<[str, bin]>, stats: HttpStats]`, where
 * `HttpStats = [queue_us, connect_us, tls_us, ttfb_us, total_us, bytes_sent, bytes_received,
 * reused: bool]` and each count is a u64 contractually bounded below 2^63, so a native PHP int.
 * Encodes the body only; the caller wraps it in the Outcome envelope.
 */
final class HttpDone
{
    public const ARITY = 2;
    public const STATS = ['queue_us', 'connect_us', 'tls_us', 'ttfb_us', 'total_us', 'bytes_sent', 'bytes_received'];

    /**
     * @param array<string,mixed> $m `trailers` (list of [name, value]) and `stats` (keyed by
     *   {@see self::STATS} plus `reused`)
     * @return string the encoded fixarray(2) body (no Outcome wrapper)
     */
    public static function encode(array $m, PackerInterface $p): string
    {
        $stats = $m['stats'] ?? null;
        if (!is_array($stats)) { throw new CodecException('HttpDone stats is not an array'); }
        $out = $p->packArrayLen(self::ARITY)
            . HttpWire::packHeaders($p, HttpWire::headersIn($m['trailers'] ?? [], 'HttpDone trailers'))
            . $p->packArrayLen(count(self::STATS) + 1);
        foreach (self::STATS as $field) {
            $out .= $p->packUint(HttpWire::uint($stats[$field] ?? null, PHP_INT_MAX, "HttpDone {$field}"));
        }
        return $out . $p->packBool(HttpWire::bool($stats['reused'] ?? null, 'HttpDone reused'));
    }

    /**
     * Map an already-unpacked 2-element `Outcome::Ok` body.
     * @param array<array-key,mixed> $w
     * @return array{trailers:list<array{0:string,1:string}>,stats:array{queue_us:int,connect_us:int,tls_us:int,ttfb_us:int,total_us:int,bytes_sent:int,bytes_received:int,reused:bool}}
     */
    public static function mapFromWire(array $w): array
    {
        $w = HttpWire::arity($w, self::ARITY, 'HttpDone');
        $s = HttpWire::arity($w[1], count(self::STATS) + 1, 'HttpDone stats');
        return [
            'trailers' => HttpWire::headersOut($w[0], 'HttpDone trailers'),
            'stats' => [
                'queue_us' => HttpWire::uint($s[0], PHP_INT_MAX, 'HttpDone queue_us'),
                'connect_us' => HttpWire::uint($s[1], PHP_INT_MAX, 'HttpDone connect_us'),
                'tls_us' => HttpWire::uint($s[2], PHP_INT_MAX, 'HttpDone tls_us'),
                'ttfb_us' => HttpWire::uint($s[3], PHP_INT_MAX, 'HttpDone ttfb_us'),
                'total_us' => HttpWire::uint($s[4], PHP_INT_MAX, 'HttpDone total_us'),
                'bytes_sent' => HttpWire::uint($s[5], PHP_INT_MAX, 'HttpDone bytes_sent'),
                'bytes_received' => HttpWire::uint($s[6], PHP_INT_MAX, 'HttpDone bytes_received'),
                'reused' => HttpWire::bool($s[7], 'HttpDone reused'),
            ],
        ];
    }

    /**
     * Decode an `Outcome::Ok` body's raw bytes.
     * @return array{trailers:list<array{0:string,1:string}>,stats:array{queue_us:int,connect_us:int,tls_us:int,ttfb_us:int,total_us:int,bytes_sent:int,bytes_received:int,reused:bool}}
     */
    public static function decode(string $body, PackerInterface $p): array
    {
        return self::mapFromWire(HttpWire::unpackArray($body, $p, self::ARITY, 'HttpDone'));
    }
}
