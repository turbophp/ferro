<?php // /php/client/src/Protocol/HelloAck.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Decoder value object for the `CORE/HELLO_ACK` reply (server -> client only). Mirrors the Rust
 * `messages::HelloAck` BYTES: a positional fixarray of 5 fields in declaration order (see
 * `engine/crates/ferro-proto/src/messages.rs`) —
 *   [engine_version:u32, boot_epoch:u64, features:u32, pools:Vec<PoolInfo>, type_registry_hash:String].
 *
 * M1-S8a reshaped field 4: each element is now a nested `[name, kind, server_version]` triple
 * ({@see PoolInfo}) rather than a bare name. `HelloAck`'s OWN arity is unchanged (5) — the skew
 * tripwire for that reshape is `protocol_version` 1 -> 2, checked in {@see Header::decode} at byte 1
 * of every frame, NOT this arity check (which would only ever fire on a different kind of bug).
 *
 * `boot_epoch` is a full-range random `u64` (SPEC §19.1) and is stored EXACTLY as {@see PurePacker}
 * yields it: an `int` when it narrowed to a marker <= uint32, or a DECIMAL STRING when it rode a
 * uint64 marker (`be64ToDec`, i.e. any value the codec emitted as `0xcf`, which includes every
 * value > PHP_INT_MAX). It is NEVER `(int)`-coerced here — coercion would collapse distinct large
 * epochs to PHP_INT_MAX and defeat the S7 reconnect loop's epoch-change detection.
 */
final class HelloAck
{
    /**
     * @param int|string $bootEpoch OPAQUE: int, or a decimal string for a uint64-encoded epoch.
     * @param list<PoolInfo> $pools the pools this engine serves — name (what `ExecRequest.pool`
     *        references), backend family, and server version when the engine has learned it.
     */
    public function __construct(
        public readonly int $engineVersion,
        public readonly int|string $bootEpoch,
        public readonly int $features,
        public readonly array $pools,
        public readonly string $typeRegistryHash,
    ) {}

    /** Decode a HELLO_ACK payload (the caller has already stripped the 16-byte header). */
    public static function decode(string $payload, PackerInterface $p): self
    {
        $off = 0;
        $w = $p->unpack($payload, $off);
        if ($off !== strlen($payload)) { throw new CodecException('HelloAck: trailing bytes'); }
        if (!is_array($w)) { throw new CodecException('HelloAck: body is not an array'); }
        $w = array_values($w);
        if (count($w) !== 5) { throw new CodecException('HelloAck arity != 5'); }

        // boot_epoch stays OPAQUE — accept int|string exactly as the packer yielded it, never cast.
        $epoch = $w[1];
        if (!is_int($epoch) && !is_string($epoch)) {
            throw new CodecException('HelloAck: boot_epoch is not int|string');
        }

        $pools = [];
        foreach (SqlValueCodec::listOf($w[3]) as $entry) { $pools[] = PoolInfo::fromWire($entry); }

        return new self(
            SqlValueCodec::toInt($w[0]),
            $epoch,
            SqlValueCodec::toInt($w[2]),
            $pools,
            SqlValueCodec::toStr($w[4]),
        );
    }
}
