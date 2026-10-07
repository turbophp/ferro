<?php // /php/psr18/src/Psr18/RequestException.php
declare(strict_types=1);
namespace Ferro\Psr18;

use Ferro\Http\Fate\NonRetryable;
use Ferro\Http\FateClass;
use Ferro\Http\HttpFate;
use Psr\Http\Client\RequestExceptionInterface;
use Psr\Http\Message\RequestInterface;

/**
 * PSR-18's `RequestExceptionInterface`: the request itself was refused and NOTHING was sent (SPEC
 * §23.11.3's PSR-18 column) — a `forbidden_*` policy refusal by the engine, an origin with no
 * upstream ({@see UnmappedOriginException}), a body over the frame cap (§23.9.3), an engine that does
 * not serve Ferro HTTP. Always NonRetryable: sending it again is refused again.
 */
class RequestException extends \RuntimeException implements RequestExceptionInterface, NonRetryable
{
    private readonly HttpFate $ferroFate;

    public function __construct(
        string $message,
        private readonly RequestInterface $request,
        ?HttpFate $fate = null,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
        $this->ferroFate = $fate !== null && $fate->fate === FateClass::NonRetryable ? $fate : new HttpFate(FateClass::NonRetryable, cause: $fate?->cause);
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
