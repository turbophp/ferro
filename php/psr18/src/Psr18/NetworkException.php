<?php // /php/psr18/src/Psr18/NetworkException.php
declare(strict_types=1);
namespace Ferro\Psr18;

use Ferro\Http\Fate\Carrier;
use Ferro\Http\FateClass;
use Ferro\Http\HttpFate;
use Psr\Http\Client\NetworkExceptionInterface;
use Psr\Http\Message\RequestInterface;

/**
 * PSR-18's `NetworkExceptionInterface` for a Ferro HTTP exchange that did not complete (SPEC
 * §23.11.3's PSR-18 column): every dial, admission, transport and after-head failure — everything
 * but a refusal of the request itself ({@see RequestException}). `getPrevious()` is the `ferro/client`
 * exception; the concrete class's marker is the fate ({@see create}).
 */
abstract class NetworkException extends \RuntimeException implements NetworkExceptionInterface, Carrier
{
    final public function __construct(
        string $message,
        private readonly RequestInterface $request,
        private readonly HttpFate $ferroFate,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
    }

    public static function create(string $message, RequestInterface $request, HttpFate $fate, ?\Throwable $previous = null): self
    {
        return match ($fate->fate) {
            FateClass::Retryable => new RetryableNetworkException($message, $request, $fate, $previous),
            FateClass::Indeterminate => new IndeterminateNetworkException($message, $request, $fate, $previous),
            default => new NonRetryableNetworkException($message, $request, $fate, $previous),
        };
    }

    public function getRequest(): RequestInterface
    {
        return $this->request;
    }

    public function ferroFate(): HttpFate
    {
        return $this->ferroFate;
    }
}
