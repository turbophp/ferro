<?php // /php/psr18/src/Http/Adapter/OriginMap.php
declare(strict_types=1);
namespace Ferro\Http\Adapter;

use Psr\Http\Message\UriInterface;

/**
 * The PHP-side map from an origin to a Ferro upstream NAME (SPEC §23.11.2, §23.16 C8): the engine
 * never sees a URL, and `HELLO_ACK` carries no upstream list, so the application states which
 * upstream each origin is.
 *
 *     new OriginMap(['https://api.openai.com' => 'openai', 'http://127.0.0.1:9200' => 'es']);
 *
 * Origins are compared NORMALISED: scheme and host lowercase, the default port (80 for `http`, 443
 * for `https`) removed, an IPv6 host in brackets — the same normalisation the engine applies to an
 * upstream's `ORIGIN` before it compares the request's `origin` field to it (§23.4.2), so a map that
 * has drifted from the daemon's configuration is a loud `forbidden_origin`, never a silent reroute.
 *
 * **A non-ASCII host is never mapped** (use punycode on both sides); nor is a scheme other than
 * `http`/`https`, a URI with userinfo, or one with no host. Such a key is refused at construction;
 * such a request is unmapped.
 *
 * @internal shared by `ferro/guzzle` and `ferro/psr18`
 */
final class OriginMap
{
    /** @var array<string, string> normalised origin => upstream name */
    private readonly array $map;

    /**
     * @param array<array-key, mixed> $upstreams `origin => upstream name`
     * @throws \InvalidArgumentException for a key that is not an origin, or a value that is not a name
     */
    public function __construct(array $upstreams)
    {
        $map = [];
        foreach ($upstreams as $origin => $name) {
            if (!is_string($origin) || !is_string($name) || $name === '') {
                throw new \InvalidArgumentException('the upstream map is origin (string) => upstream name (non-empty string)');
            }
            $normal = self::normaliseString($origin);
            if ($normal === null) {
                throw new \InvalidArgumentException(sprintf(
                    'upstream map key %s is not an origin: expected http(s)://host[:port] with an ASCII host and nothing else',
                    json_encode($origin, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
                ));
            }
            if (isset($map[$normal]) && $map[$normal] !== $name) {
                throw new \InvalidArgumentException("origin {$normal} is mapped to two upstreams ({$map[$normal]}, {$name})");
            }
            $map[$normal] = $name;
        }
        $this->map = $map;
    }

    /**
     * The upstream for a request's URI and the normalised origin to send with it, or null when the
     * origin is not mapped.
     *
     * @return ?array{0:string,1:string} `[upstream name, normalised origin]`
     */
    public function resolve(UriInterface $uri): ?array
    {
        $origin = self::normaliseUri($uri);
        if ($origin === null || !isset($this->map[$origin])) {
            return null;
        }
        return [$this->map[$origin], $origin];
    }

    /** The normalised origin of a URI, or null when it has none Ferro can map. */
    public static function normaliseUri(UriInterface $uri): ?string
    {
        if ($uri->getUserInfo() !== '') {
            return null;
        }
        return self::normalise($uri->getScheme(), $uri->getHost(), $uri->getPort());
    }

    /** @return array<string, string> */
    public function all(): array
    {
        return $this->map;
    }

    private static function normaliseString(string $origin): ?string
    {
        if (preg_match('#^([A-Za-z][A-Za-z0-9+.-]*)://(\[[0-9A-Fa-f:.]+\]|[^/?\#@:\[\]]+)(?::([0-9]{1,5}))?$#D', $origin, $m) !== 1) {
            return null;
        }
        $port = isset($m[3]) ? (int) $m[3] : null;
        return self::normalise($m[1], $m[2], $port);
    }

    private static function normalise(string $scheme, string $host, ?int $port): ?string
    {
        $scheme = strtolower($scheme);
        if ($scheme !== 'http' && $scheme !== 'https') {
            return null;
        }
        if ($host === '' || preg_match('/[^\x21-\x7E]/', $host) === 1) {
            return null; // no host, or a non-ASCII (or control/space) byte: never mapped
        }
        $host = strtolower($host);
        if (str_contains($host, ':') && $host[0] !== '[') {
            $host = '[' . $host . ']'; // an IPv6 literal from a parsed URI arrives without brackets
        }
        if ($port !== null && ($port < 1 || $port > 65535)) {
            return null;
        }
        $default = $scheme === 'http' ? 80 : 443;
        return $scheme . '://' . $host . ($port === null || $port === $default ? '' : ':' . $port);
    }
}
