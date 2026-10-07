<?php // /php/guzzle/src/Guzzle/ConnectException.php
declare(strict_types=1);
namespace Ferro\Guzzle;

use Ferro\Http\Fate\Carrier;
use Ferro\Http\FateClass;
use Ferro\Http\HttpFate;
use Psr\Http\Message\RequestInterface;

/**
 * A Ferro HTTP failure in the cells where curl would have produced a `ConnectException` (SPEC
 * §23.11.3): a dial or TLS-handshake failure, an admission refusal that never reached the server,
 * a `timeout`/`read_idle` (curl 28), `eof_empty` (curl 52: got nothing).
 *
 * The CLASS is curl's, so a naive `instanceof ConnectException` decider behaves as it does under curl
 * — and Laravel wraps it in `Illuminate\Http\Client\ConnectionException` as it wraps curl's. The
 * concrete subclass's MARKER is the fate ({@see forFate}): a sent POST whose connection died with no
 * response is an {@see IndeterminateConnectException} even though it is a `ConnectException`, which
 * {@see Retry} and `Ferro\Laravel\Http\Retry` never re-send. `getPrevious()` is the `ferro/client`
 * exception.
 */
abstract class ConnectException extends \GuzzleHttp\Exception\ConnectException implements Carrier
{
    /**
     * @param array<array-key, mixed> $handlerContext
     */
    final public function __construct(
        string $message,
        RequestInterface $request,
        private readonly HttpFate $ferroFate,
        ?\Throwable $previous = null,
        array $handlerContext = [],
    ) {
        parent::__construct($message, $request, $previous, $handlerContext);
    }

    /**
     * The subclass whose marker is `$fate`.
     *
     * @param array<array-key, mixed> $handlerContext
     */
    public static function forFate(string $message, RequestInterface $request, HttpFate $fate, ?\Throwable $previous = null, array $handlerContext = []): self
    {
        return match ($fate->fate) {
            FateClass::Retryable => new RetryableConnectException($message, $request, $fate, $previous, $handlerContext),
            FateClass::Indeterminate => new IndeterminateConnectException($message, $request, $fate, $previous, $handlerContext),
            default => new NonRetryableConnectException($message, $request, $fate, $previous, $handlerContext),
        };
    }

    public function ferroFate(): HttpFate
    {
        return $this->ferroFate;
    }
}
