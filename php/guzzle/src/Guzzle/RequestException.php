<?php // /php/guzzle/src/Guzzle/RequestException.php
declare(strict_types=1);
namespace Ferro\Guzzle;

use Ferro\Http\Fate\Carrier;
use Ferro\Http\FateClass;
use Ferro\Http\HttpFate;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * A Ferro HTTP failure in the cells where curl would have produced a `RequestException` (SPEC
 * §23.11.3): a reset or write error after sending (curl 55/56), a partial or malformed head (8),
 * certificate verification (60), a body that failed after its head (18/56) — which then CARRIES the
 * response the upstream sent — and every refusal of the request itself (`forbidden_*`, an unmapped
 * origin, a body over the frame cap, a refused option), which carries none.
 *
 * Laravel does not wrap a `RequestException`: it escapes `Http::send()` raw, as curl's 56 does. The
 * concrete subclass's MARKER is the fate ({@see forFate}); `getPrevious()` is the `ferro/client`
 * exception — a `ResponseIncompleteException` after the head, whose `wasApplied()` says whether a
 * 2xx head means the request was applied.
 */
abstract class RequestException extends \GuzzleHttp\Exception\RequestException implements Carrier
{
    /**
     * @param array<array-key, mixed> $handlerContext
     */
    public function __construct(
        string $message,
        RequestInterface $request,
        private readonly HttpFate $ferroFate,
        ?ResponseInterface $response = null,
        ?\Throwable $previous = null,
        array $handlerContext = [],
    ) {
        parent::__construct($message, $request, $response, $previous, $handlerContext);
    }

    /**
     * The subclass whose marker is `$fate`.
     *
     * @param array<array-key, mixed> $handlerContext
     */
    public static function forFate(
        string $message,
        RequestInterface $request,
        HttpFate $fate,
        ?ResponseInterface $response = null,
        ?\Throwable $previous = null,
        array $handlerContext = [],
    ): self {
        return match ($fate->fate) {
            FateClass::Retryable => new RetryableRequestException($message, $request, $fate, $response, $previous, $handlerContext),
            FateClass::Indeterminate => new IndeterminateRequestException($message, $request, $fate, $response, $previous, $handlerContext),
            default => new NonRetryableRequestException($message, $request, $fate, $response, $previous, $handlerContext),
        };
    }

    public function ferroFate(): HttpFate
    {
        return $this->ferroFate;
    }
}
