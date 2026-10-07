<?php // /php/psr18/src/Psr18/FatedResponse.php
declare(strict_types=1);
namespace Ferro\Psr18;

use Ferro\Http\Fate\Carrier;
use Ferro\Http\HttpFate;
use Psr\Http\Message\MessageInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\StreamInterface;

/**
 * A response from {@see Client}: the response the application's own PSR-17 factory built, wrapped so
 * that it carries its Ferro fate ({@see Carrier}, read by {@see \Ferro\Http\Fate::of()}) whatever
 * that factory is (SPEC §23.11.5 as built: a wrapper for EVERY factory, not only Guzzle's).
 *
 * Every `with*()` returns another wrapper around the inner response's own `with*()` result, so the
 * fate survives header-modifying middleware, as `FerroResponse`'s does for Guzzle. Code that needs
 * the factory's concrete class reads {@see inner()}.
 */
final class FatedResponse implements ResponseInterface, Carrier
{
    public function __construct(
        private readonly ResponseInterface $inner,
        private readonly HttpFate $ferroFate,
    ) {}

    public function ferroFate(): HttpFate
    {
        return $this->ferroFate;
    }

    /** The response the factory built. */
    public function inner(): ResponseInterface
    {
        return $this->inner;
    }

    public function getProtocolVersion(): string
    {
        return $this->inner->getProtocolVersion();
    }

    public function withProtocolVersion(string $version): MessageInterface
    {
        return new self($this->inner->withProtocolVersion($version), $this->ferroFate);
    }

    /** @return array<string, array<string>> */
    public function getHeaders(): array
    {
        return $this->inner->getHeaders();
    }

    public function hasHeader(string $name): bool
    {
        return $this->inner->hasHeader($name);
    }

    /** @return array<string> */
    public function getHeader(string $name): array
    {
        return $this->inner->getHeader($name);
    }

    public function getHeaderLine(string $name): string
    {
        return $this->inner->getHeaderLine($name);
    }

    public function withHeader(string $name, $value): MessageInterface
    {
        return new self($this->inner->withHeader($name, $value), $this->ferroFate);
    }

    public function withAddedHeader(string $name, $value): MessageInterface
    {
        return new self($this->inner->withAddedHeader($name, $value), $this->ferroFate);
    }

    public function withoutHeader(string $name): MessageInterface
    {
        return new self($this->inner->withoutHeader($name), $this->ferroFate);
    }

    public function getBody(): StreamInterface
    {
        return $this->inner->getBody();
    }

    public function withBody(StreamInterface $body): MessageInterface
    {
        return new self($this->inner->withBody($body), $this->ferroFate);
    }

    public function getStatusCode(): int
    {
        return $this->inner->getStatusCode();
    }

    public function withStatus(int $code, string $reasonPhrase = ''): ResponseInterface
    {
        return new self($this->inner->withStatus($code, $reasonPhrase), $this->ferroFate);
    }

    public function getReasonPhrase(): string
    {
        return $this->inner->getReasonPhrase();
    }
}
