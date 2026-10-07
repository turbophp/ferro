<?php // /php/guzzle/src/Http/FerroResponse.php
declare(strict_types=1);
namespace Ferro\Http;

use Ferro\Http\Fate\Carrier;
use GuzzleHttp\Psr7\Response;
use Psr\Http\Message\StreamInterface;

/**
 * A response delivered by `Ferro\Guzzle\FerroHandler` (SPEC §23.11.2): an ordinary
 * `GuzzleHttp\Psr7\Response` that also carries {@see ferroFate()} — the engine's effective
 * idempotency for the request, the status, and {@see StatusFate}'s advisory verdict on it — which
 * {@see Fate::of()} and `Ferro\Guzzle\Retry` read.
 *
 * PSR-7's `with*()` methods clone, so the fate survives header-modifying middleware (Guzzle's
 * cookie and redirect middleware hand this object on). A middleware that REBUILDS the response
 * (`new Response(...)`) loses it, and the deciders then treat the response as non-idempotent
 * (§23.11.4) — the cautious direction.
 *
 * Header names are lowercase (§23.16 C9 item 5): `getHeaders()` keys differ from curl's; every
 * case-insensitive accessor behaves as stock.
 */
final class FerroResponse extends Response implements Carrier
{
    /**
     * @param array<string, list<string>> $headers
     * @param StreamInterface|resource|string|null $body
     */
    public function __construct(
        private readonly HttpFate $ferroFate,
        int $status = 200,
        array $headers = [],
        $body = null,
        string $version = '1.1',
        ?string $reason = null,
    ) {
        parent::__construct($status, $headers, $body, $version, $reason);
    }

    public function ferroFate(): HttpFate
    {
        return $this->ferroFate;
    }
}
