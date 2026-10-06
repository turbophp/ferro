<?php // /php/client/tests/Protocol/HttpCodecTest.php
declare(strict_types=1);
namespace Ferro\Tests\Protocol;

use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\HttpBody;
use Ferro\Protocol\HttpDone;
use Ferro\Protocol\HttpHead;
use Ferro\Protocol\HttpRequest;
use Ferro\Protocol\Msgpack\PurePacker;
use PHPUnit\Framework\TestCase;

/**
 * The HTTP codecs' STRICTNESS (M6-F2, SPEC §23.5) — what the golden vectors cannot show, because a
 * vector is a value the codec accepts. Encode refuses what the engine's decoder would refuse, before a
 * byte is written; decode refuses a frame of the wrong type or width instead of inventing a value.
 */
final class HttpCodecTest extends TestCase
{
    /** @return array<string,mixed> */
    private static function request(): array
    {
        return [
            'upstream' => 'billing', 'method' => 'POST', 'target' => '/v1/charges', 'origin' => null,
            'headers' => [['content-type', 'application/json']], 'body' => null, 'timeout_ms' => null,
            'connect_timeout_ms' => null, 'read_timeout_ms' => null, 'idempotent' => null,
            'decode' => true, 'route' => null, 'traceparent' => null,
        ];
    }

    public function testAnEmptyBodyIsDistinctFromNoBody(): void
    {
        $p = new PurePacker();
        $none = HttpRequest::encode(self::request(), $p);
        $empty = HttpRequest::encode(['body' => ''] + self::request(), $p);
        $this->assertNotSame(bin2hex($none), bin2hex($empty));
        $this->assertStringContainsString('c400', bin2hex($empty), 'a present empty body is bin8(0)');
        $off = 0;
        $wire = $p->unpack($empty, $off);
        $this->assertIsArray($wire);
        $this->assertSame('', HttpRequest::mapFromWire($wire)['body']);
    }

    /** @return iterable<string, array{0:array<string,mixed>}> */
    public static function refusedRequests(): iterable
    {
        yield 'non-UTF-8 target' => [['target' => "/\xff"]];
        yield 'non-UTF-8 header name' => [['headers' => [["x-\xff", 'v']]]];
        // Every strict `str` the engine decodes, one row each — not only the two above.
        yield 'non-UTF-8 upstream' => [['upstream' => "billing\xff"]];
        yield 'non-UTF-8 method' => [['method' => "POS\xc3"]];
        yield 'non-UTF-8 origin' => [['origin' => "https://api.\xfe.com"]];
        yield 'non-UTF-8 route' => [['route' => "/v1/\x80"]];
        yield 'timeout past u32' => [['timeout_ms' => 0x1_0000_0000]];
        yield 'negative timeout' => [['read_timeout_ms' => -1]];
        yield 'header not a pair' => [['headers' => [['only-a-name']]]];
        yield 'header value not a string' => [['headers' => [['x', 1]]]];
        yield 'headers a map' => [['headers' => ['x' => 'v']]];
        yield 'decode not a bool' => [['decode' => 1]];
        yield 'idempotent not a bool' => [['idempotent' => 'yes']];
        yield 'missing upstream' => [['upstream' => null]];
    }

    /** @param array<string,mixed> $override */
    #[\PHPUnit\Framework\Attributes\DataProvider('refusedRequests')]
    public function testEncodeRefusesWhatTheEngineWould(array $override): void
    {
        $this->expectException(CodecException::class);
        HttpRequest::encode($override + self::request(), new PurePacker());
    }

    public function testHeadDecodeRefusesTheWrongWidthsAndTypes(): void
    {
        $p = new PurePacker();
        $head = static fn (string $status, string $tail): string =>
            $p->packArrayLen(6) . $status . $p->packUint(11) . $p->packNil() . $p->packArrayLen(0) . $tail;

        // The control: a well-formed head decodes.
        $ok = HttpHead::decode($head($p->packUint(200), $p->packNil() . $p->packBool(false)), $p);
        $this->assertSame(200, $ok['status']);

        foreach ([
            'status past u16' => $head($p->packUint(70_000), $p->packNil() . $p->packBool(false)),
            'status a string' => $head($p->packStr('200'), $p->packNil() . $p->packBool(false)),
            'version past u8' => $p->packArrayLen(6) . $p->packUint(200) . $p->packUint(256) . $p->packNil()
                . $p->packArrayLen(0) . $p->packNil() . $p->packBool(false),
            'content_length past PHP_INT_MAX' => $head($p->packUint(200),
                $p->packArrayLen(2) . $p->packStr('gzip') . $p->packUint('9223372036854775808') . $p->packBool(false)),
            'idempotent nil' => $head($p->packUint(200), $p->packNil() . $p->packNil()),
            'decoded arity 1' => $head($p->packUint(200), $p->packArrayLen(1) . $p->packStr('gzip') . $p->packBool(false)),
            'trailing byte' => $head($p->packUint(200), $p->packNil() . $p->packBool(false)) . $p->packNil(),
        ] as $why => $bytes) {
            try {
                HttpHead::decode($bytes, $p);
                $this->fail("accepted: {$why}");
            } catch (CodecException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testDoneRefusesAStatPastPhpIntMaxRatherThanInventingOne(): void
    {
        $p = new PurePacker();
        $body = $p->packArrayLen(2) . $p->packArrayLen(0) . $p->packArrayLen(8)
            . str_repeat($p->packUint(1), 6) . $p->packUint('18446744073709551615') . $p->packBool(true);
        $this->expectException(CodecException::class);
        HttpDone::decode($body, $p);
    }

    public function testBodyRoundTripsBinaryAndRefusesTheWrongArity(): void
    {
        $p = new PurePacker();
        $chunk = "\xc0\x00\x80";
        $this->assertSame($chunk, HttpBody::decode(HttpBody::encode(['chunk' => $chunk], $p), $p));
        foreach ([
            'arity 2' => $p->packArrayLen(2) . $p->packBin('a') . $p->packBin('b'),
            'trailing byte' => HttpBody::encode(['chunk' => $chunk], $p) . $p->packNil(),
            'chunk nil' => $p->packArrayLen(1) . $p->packNil(),
        ] as $why => $bytes) {
            try {
                HttpBody::decode($bytes, $p);
                $this->fail("accepted: {$why}");
            } catch (CodecException) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testTheCauseVocabularyIsGeneratedAndClosed(): void
    {
        $this->assertCount(45, C::HTTP_CAUSES);
        $this->assertSame(C::HTTP_CAUSES, array_values(array_unique(C::HTTP_CAUSES)));
        $this->assertSame('unsent_write', C::HTTP_CAUSE_UNSENT_WRITE);
        $this->assertSame('unsent_closed', C::HTTP_CAUSE_UNSENT_CLOSED);
        // Every HTTP_CAUSE_* constant is in the list, and the list holds nothing else.
        $consts = [];
        foreach ((new \ReflectionClass(C::class))->getConstants() as $name => $value) {
            if (str_starts_with($name, 'HTTP_CAUSE_')) {
                $this->assertSame(strtolower(substr($name, strlen('HTTP_CAUSE_'))), $value);
                $consts[] = $value;
            }
        }
        sort($consts);
        $list = C::HTTP_CAUSES;
        sort($list);
        $this->assertSame($list, $consts);
    }
}
