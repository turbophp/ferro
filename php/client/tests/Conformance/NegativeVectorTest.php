<?php // /php/client/tests/Conformance/NegativeVectorTest.php
declare(strict_types=1);
namespace Ferro\Tests\Conformance;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use PHPUnit\Framework\TestCase;

final class NegativeVectorTest extends TestCase
{
    private const DIR = __DIR__ . '/../../../../proto/vectors/negative';

    private static function load(string $name): string
    {
        $path = self::DIR . '/' . $name;
        $buf = file_get_contents($path);
        if ($buf === false) { throw new \RuntimeException("missing negative vector {$path}"); }
        return $buf;
    }

    // Each fixture must be rejected FOR ITS OWN REASON, so the message is asserted, not just the
    // class. `Header::decode` checks magic, then version, then length and stops at the first
    // failure: a `bad_magic.bin` or `oversize_len.bin` whose version byte drifted (e.g. left at 1
    // across the v1->v2 bump) would be rejected by the VERSION check without ever reaching the
    // property the fixture exists to pin — and a class-only `expectException` would stay green.

    public function testBadMagicThrowsForTheMagicReason(): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessageMatches('/bad magic/');
        Header::decode(self::load('bad_magic.bin'));
    }

    public function testBadVersionThrowsForTheVersionReason(): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessageMatches('/bad version/');
        Header::decode(self::load('bad_version.bin'));
    }

    public function testOversizeLenThrowsForTheLengthReason(): void
    {
        $this->expectException(CodecException::class);
        $this->expectExceptionMessageMatches('/frame too large/');
        Header::decode(self::load('oversize_len.bin'));
    }

    // TODO(S3/client): PHP flags::validate parity — PHP has no flag-validation primitive yet, so
    // Header::decode currently accepts a reserved/OOB_FD flag bit rather than rejecting it the way
    // the Rust decoder does. Until that lands, pin the current (accepting) behavior so a future
    // flags::validate change is forced to touch this test deliberately.
    public function testReservedFlagCurrentlyDecodesWithOobFdBitSet(): void
    {
        $h = Header::decode(self::load('reserved_flag.bin'));
        $this->assertSame(C::FLAG_OOB_FD, $h->flags & C::FLAG_OOB_FD, 'OOB_FD flag bit must be set');
    }
}
