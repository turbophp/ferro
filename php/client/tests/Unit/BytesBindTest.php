<?php // /php/client/tests/Unit/BytesBindTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Bytes;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\ExecCodec;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\ExtPacker;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\Value;
use PHPUnit\Framework\TestCase;

/**
 * **`Ferro\Bytes` — the explicit BINARY bind marker (SPEC §22.2 (k)(4)).**
 *
 * `TAG_BYTES` was unreachable from PHP: every bare string binds `TAG_TEXT`, which rides the msgpack
 * `str` family, and the engine's reader ends in `String::from_utf8` — so a binary payload failed as
 * a generic `malformed ExecRequest`, before the bind pre-flight ever saw it.
 */
final class BytesBindTest extends TestCase
{
    /** `ExecCodec` takes FOUR required args — one factory for the class (mirrors {@see BindTest}). */
    private static function codec(): ExecCodec
    {
        return new ExecCodec(
            new M1ValuePolicy(new TypePolicyOptions()),
            new PlanCache(),
            new PurePacker(),
            new PurePacker(),
        );
    }

    public function testBytesBindsToTagBytesWhileABareStringStaysText(): void
    {
        $codec = self::codec();
        $this->assertSame(C::TAG_BYTES, $codec->bindOne(new Bytes("\x00\x01\xff"))['tag']);
        $this->assertSame("\x00\x01\xff", $codec->bindOne(new Bytes("\x00\x01\xff"))['data']);
        // A bare string's CONTENTS are never sniffed — it stays TEXT even when it is not valid UTF-8
        // shaped, because retagging by content is the silent miscast §9.1 forbids.
        $this->assertSame(C::TAG_TEXT, $codec->bindOne('plain')['tag']);
        $this->assertSame(C::TAG_TEXT, $codec->bindOne("\x00\x01\xff")['tag']);
    }

    public function testBytesEncodesAsAMsgpackBinFrameNotAStr(): void
    {
        $p = new PurePacker();
        $wire = Value::bytes("\x00\x01\xff")->encode($p);
        // fixarray(2), pfix tag, then the BIN marker 0xc4 — never a str marker (0xa0-0xbf/0xd9…).
        $this->assertSame('92', bin2hex(substr($wire, 0, 1)));
        $this->assertSame(dechex(C::TAG_BYTES), ltrim(bin2hex(substr($wire, 1, 1)), '0'));
        $this->assertSame('c4', bin2hex(substr($wire, 2, 1)), 'BYTES must ride the msgpack bin family');
    }

    /**
     * `ExtPacker::packBin` was `\msgpack_pack($s)`, which emits msgpack **str** — and the engine's
     * BYTES decoder is marker-strict for bin. Latent while nothing bound BYTES; this test is the
     * first thing that would have caught it.
     *
     * NOTE on what this proves and where: `PackerFactory::forEncode()` returns `PurePacker`
     * unconditionally, so `ExtPacker::packBin` is NOT on the production encode path — no live round
     * trip exercises it. This unit test is the ONLY coverage, and it is skipped on a host without
     * ext-msgpack (charter rule 7 keeps the extension optional).
     */
    public function testExtPackerPackBinIsByteIdenticalToThePurePacker(): void
    {
        if (!\extension_loaded('msgpack')) {
            $this->markTestSkipped('ext-msgpack absent');
        }
        foreach (['', "\x00", "\x00\x01\xff", str_repeat("\xfe", 300), str_repeat("\x01", 70000)] as $s) {
            $this->assertSame(
                bin2hex((new PurePacker())->packBin($s)),
                bin2hex((new ExtPacker())->packBin($s)),
                'ExtPacker::packBin must emit a real msgpack bin, byte-for-byte',
            );
        }
    }

    /**
     * **The bug the live round trip caught, pinned at unit level.** `bindOne` returns the cell that
     * `ExecRequest::encode` feeds to `SqlValueCodec::encode`, and that arm used to be
     * `bytesFromInts($data)` alone — whose `listOf()` returns `[]` for a string. So a `Ferro\Bytes`
     * param encoded as `c400`, an **EMPTY bin**: an INSERT that succeeded and stored nothing. This
     * asserts the whole bind → encode chain emits the real payload, not just that `bindOne`'s tag is
     * right, which is what makes it able to fail.
     */
    public function testABytesParamSurvivesTheWholeBindToWireChain(): void
    {
        $blob = "\x00\x01\xff";
        $p = new PurePacker();
        $wire = \Ferro\Protocol\ExecRequest::encode([
            'pool' => 'default',
            'sql' => 'INSERT INTO t (b) VALUES (?)',
            'query_id' => null,
            'params' => [self::codec()->bindOne(new Bytes($blob))],
            'timeout_ms' => null,
            'readonly' => false,
            'fetch' => 0,
            'tx_id' => null,
        ], $p);

        // `c4 03 0001ff` — a bin8 of length 3 carrying the exact bytes. NOT `c400`.
        $this->assertStringContainsString('c403' . bin2hex($blob), bin2hex($wire));

        // …and the decoded request round-trips to the same bytes through the JSON `list<int>` shape.
        $off = 0;
        $decoded = \Ferro\Protocol\ExecRequest::mapFromWire((array) $p->unpack($wire, $off));
        $params = $decoded['params'];
        $this->assertIsArray($params);
        $this->assertSame([0, 1, 255], $params[0]['data']);
        $this->assertSame(C::TAG_BYTES, $params[0]['tag']);
    }

    /** Neither shape → a loud codec fault, never the silently-empty `c400` bin. */
    public function testANonStringNonListBytesPayloadIsRefused(): void
    {
        $this->expectException(\Ferro\Protocol\CodecException::class);
        $this->expectExceptionMessageMatches('/expected a byte string or a list<int> of bytes, got int/');
        \Ferro\Protocol\SqlValueCodec::encode(new PurePacker(), ['tag' => C::TAG_BYTES, 'data' => 7]);
    }

    public function testFromStreamMaterialisesTheWholeStream(): void
    {
        $h = fopen('php://memory', 'r+');
        $this->assertIsResource($h);
        fwrite($h, "\x00\xff");
        rewind($h);
        $this->assertSame("\x00\xff", Bytes::fromStream($h)->value);
    }

    public function testFromStreamRefusesANonResource(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/expects an open stream resource, got string/');
        Bytes::fromStream('not a stream');
    }

    /**
     * **The rule that had no guard at all in v1 (probe 2, weak guard 4).** `bindOne` must have NO
     * implicit `is_resource` arm: reading a stream into memory is a decision with a memory cost, and
     * it is the CALLER's to make — explicitly, via {@see Bytes::fromStream}. v1's "mutation 3"
     * observed that adding such an arm broke nothing and concluded the arm should stay absent, which
     * left the rule enforced by a comment. This asserts it.
     *
     * A raw resource is therefore rejected by the DEFAULT arm, and the refusal must name `Bytes` so
     * the message tells the caller what to do instead. The catch is the LEAF class `bindOne` really
     * throws, not the `FerroException` root (hazard 68), and the message is asserted too.
     */
    public function testARawResourceIsRefusedAndTheMessagePointsAtBytesFromStream(): void
    {
        $codec = self::codec();
        $h = fopen('php://memory', 'r+');
        $this->assertIsResource($h);
        fwrite($h, "\x00\xff");
        rewind($h);

        try {
            $codec->bindOne($h);
            $this->fail('a raw stream resource must NOT bind implicitly');
        } catch (ProtocolException $e) {
            $this->assertStringContainsString(
                'Bytes',
                $e->getMessage(),
                'the refusal must name Ferro\\Bytes as the explicit route: ' . $e->getMessage(),
            );
        }
    }
}
