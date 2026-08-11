<?php // /php/client/src/Protocol/Msgpack/PackerFactory.php
declare(strict_types=1);
namespace Ferro\Protocol\Msgpack;

final class PackerFactory
{
    public static function forEncode(): PackerInterface { return new PurePacker(); }

    /**
     * Always returns PurePacker: it is the spec-authoritative decoder, and the reason is STRUCTURAL
     * rather than a matter of scalar fidelity — it honors the caller's `$offset`, while
     * {@see ExtPacker::unpack} is `\msgpack_unpack($buf)` and therefore always decodes from the start
     * of `$buf` and reports the WHOLE buffer consumed. Every framed decode in this client
     * ({@see \Ferro\Protocol\Codec}, `Value::decode`, `ExecOk`) resumes at an offset inside a larger
     * body, so the extension cannot serve them at all. Pinned by
     * `PackerConformanceTest::testExtPackerCannotHonourAnOffsetWhichIsWhyItIsNotTheDecoder`.
     *
     * This docblock used to give a second reason — "ExtPacker decodes a uint64 > PHP_INT_MAX to a
     * lossy float" — and that is MEASURABLY FALSE on pecl msgpack 3.0.0, which returns the exact
     * decimal string just as {@see PurePacker} does (`msgpack_unpack("\xcf\xff…\xff")` →
     * `string(20) "18446744073709551615"`). The whole scalar surface is now asserted identical by
     * `PackerConformanceTest`, so the claim is checked rather than assumed. ext-msgpack remains
     * available for conformance-gated fast paths only.
     */
    public static function forDecode(): PackerInterface
    {
        return new PurePacker();
    }
}
