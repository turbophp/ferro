<?php // /php/psr18/src/Http/Adapter/ResponseParts.php
declare(strict_types=1);
namespace Ferro\Http\Adapter;

use Ferro\Http\ResponseHead;

/**
 * A response head as PSR-7 parts, the way Guzzle's own handlers present it (SPEC §23.11.2):
 *
 *  - **header names are lowercase**, because the engine delivers them so (`hyper` keeps no
 *    response name's case, P6) — a pre-registered drop-in difference (§23.16 C9 item 5);
 *    `getHeader()` is case-insensitive either way;
 *  - when the ENGINE decoded the body (`decode_content`, §23.9.2) the removed `Content-Encoding` and
 *    `Content-Length` come back as `x-encoded-content-encoding` / `x-encoded-content-length`, as
 *    `CurlFactory` and `StreamHandler` rename them — the engine already removed the originals;
 *  - the version is `1.0`, `1.1` or `2` (curl's spelling of HTTP/2);
 *  - the reason phrase is the bytes received, or `''` on HTTP/2 (PSR-7 then supplies the default).
 *
 * @internal shared by `ferro/guzzle` and `ferro/psr18`
 */
final class ResponseParts
{
    private function __construct() {}

    /** @return array<string, list<string>> lowercase name => values in arrival order */
    public static function headers(ResponseHead $head): array
    {
        $headers = $head->headers;
        if ($head->decoded !== null) {
            [$encoding, $length] = $head->decoded;
            $headers['x-encoded-content-encoding'] = [$encoding];
            if ($length !== null) {
                $headers['x-encoded-content-length'] = [(string) $length];
            }
        }
        return $headers;
    }

    public static function version(ResponseHead $head): string
    {
        return match ($head->version) {
            10 => '1.0',
            20 => '2',
            default => '1.1',
        };
    }

    public static function reason(ResponseHead $head): string
    {
        return $head->reason ?? '';
    }
}
