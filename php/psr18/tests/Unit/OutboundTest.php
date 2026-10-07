<?php // /php/psr18/tests/Unit/OutboundTest.php
declare(strict_types=1);
namespace Ferro\Psr18\Tests\Unit;

use Ferro\Http\Adapter\OriginMap;
use Ferro\Http\Adapter\OutboundRequest;
use Ferro\Http\Error\RequestTooLargeException;
use Ferro\Protocol\Generated\Constants as C;
use GuzzleHttp\Psr7\FnStream;
use GuzzleHttp\Psr7\PumpStream;
use GuzzleHttp\Psr7\Utils;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Uri;
use PHPUnit\Framework\TestCase;

/**
 * M6-F9: the origin map and the PSR-7 → native request translation (SPEC §23.11.2), plus premise
 * P10 (§23.19), measured against the installed guzzlehttp/psr7.
 */
final class OutboundTest extends TestCase
{
    public function testOriginsAreComparedNormalised(): void
    {
        $map = new OriginMap([
            'HTTPS://API.Example.com:443' => 'api',
            'http://127.0.0.1:9200' => 'es',
            'http://[::1]:8080' => 'six',
            'http://plain.example' => 'plain',
        ]);
        $this->assertSame(['api', 'https://api.example.com'], $map->resolve(new Uri('https://api.example.com/x?y')));
        $this->assertSame(['es', 'http://127.0.0.1:9200'], $map->resolve(new Uri('http://127.0.0.1:9200/_search')));
        $this->assertSame(['six', 'http://[::1]:8080'], $map->resolve(new Uri('http://[::1]:8080/')));
        $this->assertSame(['plain', 'http://plain.example'], $map->resolve(new Uri('http://plain.example:80/')));
        $this->assertNull($map->resolve(new Uri('http://api.example.com/')), 'the scheme is part of the origin');
        $this->assertNull($map->resolve(new Uri('https://api.example.com:8443/')), 'so is the port');
        $this->assertNull($map->resolve(new Uri('https://user:pw@api.example.com/')), 'userinfo is never mapped');
        $this->assertNull($map->resolve(new Uri('/relative')));
    }

    /** @return array<string, array{0: string}> */
    public static function badKeys(): array
    {
        return [
            'path' => ['https://api.example.com/v1'],
            'trailing slash' => ['https://api.example.com/'],
            'query' => ['https://api.example.com?x'],
            'userinfo' => ['https://u@api.example.com'],
            'scheme' => ['ftp://api.example.com'],
            'non-ascii host' => ['https://bücher.example'],
            'no scheme' => ['api.example.com'],
            'port 0' => ['https://api.example.com:0'],
        ];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('badKeys')]
    public function testAKeyThatIsNotAnOriginIsRefusedAtConstruction(string $key): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new OriginMap([$key => 'x']);
    }

    public function testOneOriginCannotNameTwoUpstreams(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new OriginMap(['https://a.example' => 'one', 'https://A.example:443' => 'two']);
    }

    public function testTheTargetIsPathAndQueryNeverTheFragmentAndHeadersKeepOrderAndDuplicates(): void
    {
        $req = (new Request('PATCH', 'https://api.example.com/a/b?c=1&d#frag', ['X-A' => ['1', '2'], 'X-B' => '3'], 'body'));
        $out = OutboundRequest::from($req, 'up');
        $this->assertSame('PATCH', $out->method);
        $this->assertSame('/a/b?c=1&d', $out->target);
        $this->assertSame([['Host', 'api.example.com'], ['X-A', '1'], ['X-A', '2'], ['X-B', '3']], $out->headers);
        $this->assertSame('body', $out->body);
        $this->assertSame('/', OutboundRequest::from(new Request('GET', 'https://api.example.com'), 'up')->target);
    }

    public function testAnEmptyBodyIsNoneOnlyForBodylessMethods(): void
    {
        $this->assertNull(OutboundRequest::from(new Request('GET', 'https://a.example/'), 'up')->body);
        $this->assertNull(OutboundRequest::from(new Request('delete', 'https://a.example/'), 'up')->body);
        $this->assertSame('', OutboundRequest::from(new Request('POST', 'https://a.example/'), 'up')->body);
        $this->assertSame('', OutboundRequest::from(new Request('GET', 'https://a.example/', ['Content-Length' => '0']), 'up')->body);
    }

    public function testAKnownOversizeBodyIsRefusedWithoutReadingIt(): void
    {
        $reads = 0;
        $stream = FnStream::decorate(Utils::streamFor('x'), [
            'getSize' => static fn (): int => C::MAX_FRAME_PAYLOAD + 1,
            'read' => static function () use (&$reads): string { ++$reads; return 'x'; },
        ]);
        try {
            OutboundRequest::from(new Request('POST', 'https://a.example/', [], $stream), 'up');
            $this->fail('expected the refusal');
        } catch (RequestTooLargeException $e) {
            $this->assertStringContainsString('MAX_FRAME_PAYLOAD', $e->getMessage());
        }
        $this->assertSame(0, $reads, 'not a byte read');
    }

    public function testAnUnknownSizeBodyIsReadNoFurtherThanTheCap(): void
    {
        $pumped = 0;
        $stream = new PumpStream(static function () use (&$pumped): string {
            $pumped += 1 << 20;
            return str_repeat('y', 1 << 20);
        });
        try {
            OutboundRequest::from(new Request('POST', 'https://a.example/', [], $stream), 'up');
            $this->fail('expected the refusal');
        } catch (RequestTooLargeException) {
        }
        $this->assertLessThanOrEqual(C::MAX_FRAME_PAYLOAD + (2 << 20), $pumped, 'an endless body is not read without bound');
    }

    /**
     * P10 (§23.19): guzzlehttp/psr7's `Uri` percent-encodes every ASCII byte §23.4.2 step 3 refuses
     * (raw `[`/`]` in a query included), and cannot represent a raw byte ≥ 0x80 at all — so a target
     * built from a PSR-7 URI is always inside the engine's byte set, and a refusal is never this
     * adapter's doing.
     */
    public function testPremiseP10EveryTargetPsr7BuildsIsInsideTheEnginesByteSet(): void
    {
        $refusedByUri = 0;
        for ($b = 0; $b < 256; ++$b) {
            foreach (["https://h/a%sb", "https://h/a?x=%sy", "https://h/a?%s=1"] as $shape) {
                try {
                    $uri = new Uri(sprintf($shape, chr($b)));
                } catch (\InvalidArgumentException) {
                    ++$refusedByUri;
                    $this->assertGreaterThanOrEqual(0x80, $b, sprintf('psr7 refused byte 0x%02x', $b));
                    continue;
                }
                $target = OutboundRequest::from(new Request('GET', $uri), 'up')->target;
                $this->assertMatchesRegularExpression("#^[A-Za-z0-9\\-._~!$&'()*+,;=:@/?%]*$#D", $target, sprintf('byte 0x%02x in %s', $b, $shape));
            }
        }
        $this->assertSame(128 * 3, $refusedByUri, 'every byte >= 0x80, in every position');
        $this->assertSame('/a?b%5B%5D=1', OutboundRequest::from(new Request('GET', (new Uri('https://h/a'))->withQuery('b[]=1')), 'up')->target);
    }
}
