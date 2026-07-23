<?php // /php/client/src/Protocol/Msgpack/PackerFactory.php
declare(strict_types=1);
namespace Ferro\Protocol\Msgpack;

final class PackerFactory
{
    public static function forEncode(): PackerInterface { return new PurePacker(); }

    /**
     * Always returns PurePacker: it is the spec-authoritative decoder — it yields the exact decimal
     * string for a uint64 > PHP_INT_MAX (ExtPacker decodes it to a lossy float instead) and it honors
     * the caller's `$offset` (ExtPacker ignores `$offset` and always decodes from the start of `$buf`,
     * which breaks any multi-value/streaming decode). ext-msgpack therefore must not be the default
     * decode path; it remains available (see ExtPacker) for conformance-gated fast paths only.
     */
    public static function forDecode(): PackerInterface
    {
        return new PurePacker();
    }
}
