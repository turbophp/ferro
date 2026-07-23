<?php // /php/client/src/Protocol/Msgpack/PackerFactory.php
declare(strict_types=1);
namespace Ferro\Protocol\Msgpack;

final class PackerFactory
{
    public static function forEncode(): PackerInterface { return new PurePacker(); }
    public static function forDecode(): PackerInterface
    {
        return \extension_loaded('msgpack') ? new ExtPacker() : new PurePacker();
    }
}
