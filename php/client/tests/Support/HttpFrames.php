<?php // /php/client/tests/Support/HttpFrames.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Protocol\Codec;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\HttpBody;
use Ferro\Protocol\HttpDone;
use Ferro\Protocol\HttpHead;
use Ferro\Protocol\HttpRequest;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;

/**
 * Engine-side Ferro HTTP frames for the offline tests (M6-F8), built with the PRODUCTION codecs, plus
 * a reader for the frames a session wrote.
 */
final class HttpFrames
{
    public static function frame(int $flags, int $service, int $method, int $rid, string $payload): string
    {
        return (new Codec())->encodeFrame(new Header($flags, $service, $method, $rid, strlen($payload)), $payload);
    }

    public static function helloAck(int $features = C::FEATURE_ENGINE_HTTP): string
    {
        $payload = Message::encode('hello_ack', [
            'engine_version' => 1, 'boot_epoch' => 7, 'features' => $features,
            'pools' => [['name' => 'default', 'kind' => 'postgres', 'server_version' => '17.0', 'literals_are_standard' => true]],
            'type_registry_hash' => C::TYPE_REGISTRY_HASH,
        ], PackerFactory::forEncode());
        return self::frame(0, C::SERVICE_CORE, C::METHOD_CORE_HELLO_ACK, 0, $payload);
    }

    /** @param list<array{0:string,1:string}> $headers */
    public static function headPayload(int $status = 200, array $headers = [], bool $idempotent = false, int $version = 11): string
    {
        return HttpHead::encode([
            'status' => $status, 'version' => $version, 'reason' => 'OK', 'headers' => $headers,
            'decoded' => null, 'idempotent' => $idempotent,
        ], PackerFactory::forEncode());
    }

    /** @param list<array{0:string,1:string}> $headers */
    public static function head(int $rid, int $status = 200, array $headers = [], bool $idempotent = false, int $version = 11): string
    {
        return self::frame(0, C::SERVICE_HTTP, C::METHOD_HTTP_HEAD, $rid, self::headPayload($status, $headers, $idempotent, $version));
    }

    public static function bodyPayload(string $chunk): string
    {
        return HttpBody::encode(['chunk' => $chunk], PackerFactory::forEncode());
    }

    public static function body(int $rid, string $chunk): string
    {
        return self::frame(C::FLAG_STREAM, C::SERVICE_HTTP, C::METHOD_HTTP_BODY, $rid, self::bodyPayload($chunk));
    }

    /** @param list<array{0:string,1:string}> $trailers */
    public static function done(int $rid, array $trailers = []): string
    {
        $p = PackerFactory::forEncode();
        $body = HttpDone::encode(['trailers' => $trailers, 'stats' => [
            'queue_us' => 1, 'connect_us' => 2, 'tls_us' => 0, 'ttfb_us' => 4, 'total_us' => 5,
            'bytes_sent' => 6, 'bytes_received' => 7, 'reused' => true,
        ]], $p);
        return self::frame(C::FLAG_END, C::SERVICE_HTTP, C::METHOD_HTTP_REQUEST, $rid, Outcome::ok($body)->encode($p));
    }

    public static function error(int $rid, int $code, int $branch, ?string $detail, ?int $retryAfterMs = null): string
    {
        $p = PackerFactory::forEncode();
        $ep = new ErrorPayload($code, $branch, null, null, 'engine says no', $detail, $retryAfterMs);
        return self::frame(C::FLAG_END, C::SERVICE_HTTP, C::METHOD_HTTP_REQUEST, $rid, Outcome::error($ep)->encode($p));
    }

    public static function cancelled(int $rid): string
    {
        $p = PackerFactory::forEncode();
        return self::frame(C::FLAG_END, C::SERVICE_HTTP, C::METHOD_HTTP_REQUEST, $rid, Outcome::cancelled()->encode($p));
    }

    /**
     * Every frame in `$bytes` (what a session wrote), in order.
     *
     * @return list<array{0:Header,1:string}>
     */
    public static function written(string $bytes): array
    {
        $out = [];
        $off = 0;
        while ($off < strlen($bytes)) {
            $h = Header::decode(substr($bytes, $off, 16));
            $out[] = [$h, substr($bytes, $off + 16, $h->payloadLen)];
            $off += 16 + $h->payloadLen;
        }
        return $out;
    }

    /**
     * The decoded REQUEST frames in `$bytes`, keyed by request id.
     *
     * @return array<int, array<string, mixed>>
     */
    public static function requests(string $bytes): array
    {
        $out = [];
        foreach (self::written($bytes) as [$h, $payload]) {
            if ($h->service === C::SERVICE_HTTP && $h->method === C::METHOD_HTTP_REQUEST && ($h->flags & C::FLAG_CANCEL) === 0) {
                $off = 0;
                $w = PackerFactory::forDecode()->unpack($payload, $off);
                $out[$h->requestId] = HttpRequest::mapFromWire(is_array($w) ? array_values($w) : []);
            }
        }
        return $out;
    }

    /**
     * The `(frames, bytes)` of every WINDOW_UPDATE written for `$rid`, in order.
     *
     * @return list<array{0:int,1:int}>
     */
    public static function windowUpdates(string $bytes, int $rid): array
    {
        $out = [];
        foreach (self::written($bytes) as [$h, $payload]) {
            if ($h->service === C::SERVICE_CORE && $h->method === C::METHOD_CORE_WINDOW_UPDATE && $h->requestId === $rid) {
                $off = 0;
                $w = PackerFactory::forDecode()->unpack($payload, $off);
                $w = is_array($w) ? array_values($w) : [];
                $out[] = [(int) $w[0], (int) $w[1]];
            }
        }
        return $out;
    }

    /** @return list<int> the request ids a CANCEL was written for, in order */
    public static function cancels(string $bytes): array
    {
        $out = [];
        foreach (self::written($bytes) as [$h]) {
            if (($h->flags & C::FLAG_CANCEL) !== 0) {
                $out[] = $h->requestId;
            }
        }
        return $out;
    }
}
