<?php // /php/client/src/Protocol/Hello.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Builder value object for the `CORE/HELLO` payload (client -> server). Mirrors the Rust
 * `messages::Hello` positional shape:
 *   [client_version:u32, type_registry_hash:String, manifest_hash:Option<String>, pid:u32, features:u32].
 *
 * Encoding delegates to {@see Message::encode} so the bytes stay identical to the golden-vector
 * -locked `hello` encoder path (no second, drifting encoder).
 */
final class Hello
{
    public function __construct(
        public readonly int $clientVersion,
        public readonly string $typeRegistryHash,
        public readonly ?string $manifestHash,
        public readonly int $pid,
        public readonly int $features,
    ) {}

    public function encode(PackerInterface $p): string
    {
        return Message::encode('hello', [
            'client_version' => $this->clientVersion,
            'type_registry_hash' => $this->typeRegistryHash,
            'manifest_hash' => $this->manifestHash,
            'pid' => $this->pid,
            'features' => $this->features,
        ], $p);
    }
}
