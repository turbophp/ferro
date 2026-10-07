<?php // /php/guzzle/src/Guzzle/UnmappedOriginException.php
declare(strict_types=1);
namespace Ferro\Guzzle;

use Ferro\Http\FateClass;
use Ferro\Http\HttpFate;
use Psr\Http\Message\RequestInterface;

/**
 * The request's origin has no upstream in the handler's map (SPEC §23.11.2): a configuration error,
 * loud by design, and NOTHING was sent. There is no curl fallback unless the handler was built with
 * an explicit `fallback:` handler, which gives up the SSRF guarantee for unmapped origins.
 *
 * A `RequestException` with no response, so in Laravel it escapes `Http::get()` unwrapped (Laravel
 * wraps only `ConnectException`, §23.16 C9 item 9).
 */
final class UnmappedOriginException extends NonRetryableRequestException
{
    public function __construct(string $message, RequestInterface $request)
    {
        parent::__construct($message, $request, new HttpFate(FateClass::NonRetryable));
    }
}
