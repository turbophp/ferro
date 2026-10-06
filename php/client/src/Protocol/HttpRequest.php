<?php // /php/client/src/Protocol/HttpRequest.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for the HTTP `REQUEST` message (service `HTTP`, method `REQUEST` = 1; M6-F2,
 * SPEC §23.5.1, /proto/PROTOCOL.md §12.1) — client → engine. Mirrors the Rust
 * `messages::http::HttpRequest` BYTES: a fixarray of 13 —
 *   [upstream: str, method: str, target: str, origin: str|nil, headers: array<[str, bin]>,
 *    body: bin|nil, timeout_ms: u32|nil, connect_timeout_ms: u32|nil, read_timeout_ms: u32|nil,
 *    idempotent: bool|nil, decode: bool, route: str|nil, traceparent: str|nil].
 *
 * The logical shape is an array with those keys; `headers` is a list of `[name, value]` string
 * pairs and `body`/header values are BINARY strings (written as msgpack `bin`). `body` `null` means
 * "no body", `''` a present zero-length one — distinct on the wire.
 *
 * Encoding refuses what the engine's decoder would refuse (a non-UTF-8 `str` field, a timeout past
 * `u32`, a wrong type) BEFORE a byte is written, so a client bug is a local {@see CodecException}
 * rather than an engine `Protocol` terminal. `traceparent` is the exception the engine tolerates
 * (decoded lossily, `ExecRequest` field 9's rule); this codec still requires a string. It applies no
 * §23.4 rule (that is the engine's validator) and no `TraceContext` policy (that is the native API's,
 * slice F8).
 */
final class HttpRequest
{
    public const ARITY = 13;

    /**
     * @param array<string,mixed> $m
     * @return string the encoded fixarray(13) payload
     */
    public static function encode(array $m, PackerInterface $p): string
    {
        $body = HttpWire::nullableString($m['body'] ?? null, 'HttpRequest body');
        $idempotent = HttpWire::nullableBool($m['idempotent'] ?? null, 'HttpRequest idempotent');
        return $p->packArrayLen(self::ARITY)
            . $p->packStr(HttpWire::utf8($m['upstream'] ?? null, 'HttpRequest upstream'))
            . $p->packStr(HttpWire::utf8($m['method'] ?? null, 'HttpRequest method'))
            . $p->packStr(HttpWire::utf8($m['target'] ?? null, 'HttpRequest target'))
            . self::optStr($p, HttpWire::nullableUtf8($m['origin'] ?? null, 'HttpRequest origin'))
            . HttpWire::packHeaders($p, HttpWire::headersIn($m['headers'] ?? [], 'HttpRequest'))
            . ($body === null ? $p->packNil() : $p->packBin($body))
            . self::optU32($p, $m['timeout_ms'] ?? null, 'HttpRequest timeout_ms')
            . self::optU32($p, $m['connect_timeout_ms'] ?? null, 'HttpRequest connect_timeout_ms')
            . self::optU32($p, $m['read_timeout_ms'] ?? null, 'HttpRequest read_timeout_ms')
            . ($idempotent === null ? $p->packNil() : $p->packBool($idempotent))
            . $p->packBool(HttpWire::bool($m['decode'] ?? false, 'HttpRequest decode'))
            . self::optStr($p, HttpWire::nullableUtf8($m['route'] ?? null, 'HttpRequest route'))
            . self::optStr($p, HttpWire::nullableString($m['traceparent'] ?? null, 'HttpRequest traceparent'));
    }

    /**
     * Map an already-unpacked 13-element wire array back to the logical shape. The engine is the
     * production decoder of this message; this exists so the golden vector is locked in BOTH
     * directions.
     *
     * @param array<array-key,mixed> $w
     * @return array{upstream:string,method:string,target:string,origin:?string,headers:list<array{0:string,1:string}>,body:?string,timeout_ms:?int,connect_timeout_ms:?int,read_timeout_ms:?int,idempotent:?bool,decode:bool,route:?string,traceparent:?string}
     */
    public static function mapFromWire(array $w): array
    {
        $w = HttpWire::arity($w, self::ARITY, 'HttpRequest');
        return [
            'upstream' => HttpWire::string($w[0], 'HttpRequest upstream'),
            'method' => HttpWire::string($w[1], 'HttpRequest method'),
            'target' => HttpWire::string($w[2], 'HttpRequest target'),
            'origin' => HttpWire::nullableString($w[3], 'HttpRequest origin'),
            'headers' => HttpWire::headersOut($w[4], 'HttpRequest'),
            'body' => HttpWire::nullableString($w[5], 'HttpRequest body'),
            'timeout_ms' => HttpWire::nullableUint($w[6], HttpWire::U32_MAX, 'HttpRequest timeout_ms'),
            'connect_timeout_ms' => HttpWire::nullableUint($w[7], HttpWire::U32_MAX, 'HttpRequest connect_timeout_ms'),
            'read_timeout_ms' => HttpWire::nullableUint($w[8], HttpWire::U32_MAX, 'HttpRequest read_timeout_ms'),
            'idempotent' => HttpWire::nullableBool($w[9], 'HttpRequest idempotent'),
            'decode' => HttpWire::bool($w[10], 'HttpRequest decode'),
            'route' => HttpWire::nullableString($w[11], 'HttpRequest route'),
            'traceparent' => HttpWire::nullableString($w[12], 'HttpRequest traceparent'),
        ];
    }

    private static function optStr(PackerInterface $p, ?string $s): string
    {
        return $s === null ? $p->packNil() : $p->packStr($s);
    }

    private static function optU32(PackerInterface $p, mixed $v, string $what): string
    {
        $n = HttpWire::nullableUint($v, HttpWire::U32_MAX, $what);
        return $n === null ? $p->packNil() : $p->packUint($n);
    }
}
