<?php // /php/psr18/src/Http/Fate.php
declare(strict_types=1);
namespace Ferro\Http;

use Ferro\Client\Error\FerroException;
use Ferro\Http\Fate\Carrier;
use Psr\Http\Message\ResponseInterface;

/**
 * **The one reader of a Ferro HTTP fate** (SPEC §23.11.4). Every retry decider Ferro ships
 * (`Ferro\Guzzle\Retry`, `Ferro\Laravel\Http\Retry`) reads fate through this and nothing else.
 *
 * It reads, in this order:
 *
 *  1. an object that carries its fate ({@see Carrier}): a Ferro adapter response
 *     (`Ferro\Http\FerroResponse`, {@see \Ferro\Psr18\FatedResponse}) or a fate-marked Ferro adapter
 *     exception;
 *  2. a `ferro/client` HTTP failure ({@see Error\HttpException}) — the native API's own exceptions;
 *  3. for an exception, its `getPrevious()` chain, applying 1 and 2 to each link — which is how a
 *     Laravel `ConnectionException` wrapping a Ferro `ConnectException` is read;
 *  4. for an exception that carries a response — Guzzle's `RequestException::getResponse()`
 *     (`http_errors`' `ClientException`/`ServerException`), an Illuminate
 *     `Http\Client\RequestException`'s public `$response` — that response;
 *  5. for an Illuminate `Http\Client\Response`, its `toPsrResponse()`.
 *
 * It returns **null when it cannot vouch for the object** — a response a middleware REBUILT (a new
 * `GuzzleHttp\Psr7\Response` loses the fate), a curl exception, anything not from Ferro — and
 * callers **treat null as non-idempotent**: never retried on a fate this reader could not see.
 */
final class Fate
{
    private function __construct() {}

    public static function of(object $x): ?HttpFate
    {
        return self::read($x, 0);
    }

    private static function read(object $x, int $depth): ?HttpFate
    {
        if ($depth > 8) {
            return null; // a response that wraps a response that wraps …: not something Ferro built
        }
        if ($x instanceof \Throwable) {
            for ($e = $x, $links = 0; $e !== null && $links < 32; $e = $e->getPrevious(), ++$links) {
                if ($e instanceof Carrier) {
                    return $e->ferroFate();
                }
                if ($e instanceof FerroException) {
                    return HttpFate::ofFailure($e);
                }
            }
            $response = self::responseOf($x);
            return $response === null ? null : self::read($response, $depth + 1);
        }
        if ($x instanceof Carrier) {
            return $x->ferroFate();
        }
        if ($x instanceof ResponseInterface) {
            return null; // a PSR-7 response no Ferro adapter built (or one a middleware rebuilt)
        }
        if (method_exists($x, 'toPsrResponse')) {
            $psr = $x->toPsrResponse();
            return is_object($psr) ? self::read($psr, $depth + 1) : null;
        }
        return null;
    }

    /** The response an exception carries, by the two conventions §23.11.4 names. */
    private static function responseOf(\Throwable $e): ?object
    {
        if (method_exists($e, 'getResponse')) {
            $response = $e->getResponse();
            if (is_object($response)) {
                return $response;
            }
        }
        // Illuminate\Http\Client\RequestException: `public Response $response`.
        if (property_exists($e, 'response')) {
            $response = (new \ReflectionProperty($e, 'response'))->isPublic() ? $e->response : null;
            if (is_object($response)) {
                return $response;
            }
        }
        return null;
    }
}
