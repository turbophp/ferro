<?php // /php/psr18/src/Http/Adapter/OutboundRequest.php
declare(strict_types=1);
namespace Ferro\Http\Adapter;

use Ferro\Http\Error\RequestTooLargeException;
use Ferro\Protocol\Generated\Constants as C;
use Psr\Http\Message\RequestInterface;

/**
 * A PSR-7 request as the native API's arguments (SPEC §23.11.2, "What the handler sends"): the
 * method as given; an origin-form `target` built from the path (an empty path becomes `/`) plus
 * `?query`, never the fragment; every header line, order and duplicates kept; and the body.
 *
 * Nothing is rewritten. The engine validates every byte and REFUSES what it will not send (§23.4),
 * so a header or target the upstream would never accept fails loudly there rather than being
 * "fixed" here. guzzlehttp/psr7's `Uri` percent-encodes every ASCII byte §23.4.2 step 3 refuses and
 * cannot represent a raw byte ≥ 0x80 at all (P10, measured and pinned by a test).
 *
 * **The body is read whole, but never past the frame cap** (§23.9.3): a body whose stream reports a
 * size above `MAX_FRAME_PAYLOAD` is refused without reading a byte, and one of unknown size is read
 * up to the cap and refused when there is more. The native API then refuses, exactly, a frame the
 * remaining fields push over.
 *
 * @internal shared by `ferro/guzzle` and `ferro/psr18`
 */
final class OutboundRequest
{
    /** Methods for which an empty PSR-7 body means "no body" rather than a zero-length one. */
    private const BODYLESS = ['GET' => true, 'HEAD' => true, 'OPTIONS' => true, 'DELETE' => true, 'TRACE' => true, 'CONNECT' => true];

    /**
     * @param list<array{0:string,1:string}> $headers
     */
    private function __construct(
        public readonly string $method,
        public readonly string $target,
        public readonly array $headers,
        public readonly ?string $body,
    ) {}

    /** @throws RequestTooLargeException when the body would not fit a `REQUEST` frame */
    public static function from(RequestInterface $request, string $upstream): self
    {
        $uri = $request->getUri();
        $path = $uri->getPath();
        if ($path === '') {
            $path = '/';
        } elseif ($path[0] !== '/') {
            $path = '/' . $path; // a rootless path on an absolute URI: RFC 3986 §5.2.3's merge result
        }
        $query = $uri->getQuery();
        $target = $path . ($query !== '' ? '?' . $query : '');

        $headers = [];
        foreach ($request->getHeaders() as $name => $values) {
            foreach ($values as $value) {
                $headers[] = [(string) $name, $value];
            }
        }

        $body = self::readBody($request, $upstream);
        if ($body === '' && isset(self::BODYLESS[strtoupper($request->getMethod())]) && !$request->hasHeader('Content-Length')) {
            $body = null;
        }
        return new self($request->getMethod(), $target, $headers, $body);
    }

    private static function readBody(RequestInterface $request, string $upstream): string
    {
        $stream = $request->getBody();
        $size = $stream->getSize();
        if ($size !== null && $size > C::MAX_FRAME_PAYLOAD) {
            throw self::tooLarge($upstream, $size);
        }
        if ($stream->isSeekable()) {
            $stream->rewind();
        }
        $body = '';
        while (!$stream->eof() && strlen($body) <= C::MAX_FRAME_PAYLOAD) {
            $chunk = $stream->read(min(1 << 20, C::MAX_FRAME_PAYLOAD + 1 - strlen($body)));
            if ($chunk === '') {
                break;
            }
            $body .= $chunk;
        }
        if (strlen($body) > C::MAX_FRAME_PAYLOAD) {
            throw self::tooLarge($upstream, null);
        }
        return $body;
    }

    private static function tooLarge(string $upstream, ?int $size): RequestTooLargeException
    {
        return new RequestTooLargeException(sprintf(
            'the request body for upstream "%s" is %s, above MAX_FRAME_PAYLOAD (%d bytes); it was not '
                . 'sent. Ferro HTTP v1 has no large-request-body path (SPEC §23.9.3)',
            $upstream,
            $size !== null ? "{$size} bytes" : 'larger than that',
            C::MAX_FRAME_PAYLOAD,
        ));
    }
}
