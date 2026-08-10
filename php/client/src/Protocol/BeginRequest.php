<?php // /php/client/src/Protocol/BeginRequest.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Positional codec for the TX BEGIN request (service TX, method BEGIN = 1). Mirrors the Rust
 * `messages::tx::BeginRequest` BYTES: a fixarray of 3 fields in declaration order — `pool` (str),
 * `isolation` (`u8 | nil`; §9.1 / {@see Isolation}, bare `nil` ⇒ engine/pool default), `readonly`
 * (bool). TX messages are `Value`-free, so this is the plain rmp-serde positional layout (not the
 * `Value`-splicing SQL codec). Pinned by /proto/PROTOCOL.md §9.2 and the `tx_begin_request` vector.
 */
final class BeginRequest
{
    /**
     * `isolation` accepts an {@see Isolation} case as well as the raw `u8` the golden vectors carry.
     *
     * The enum form is the one callers should use, and accepting it here is what puts the enum on a
     * BYTE-LOCKED path: `IsolationCrossLanguageTest` encodes `Isolation::Serializable` through this
     * method and compares against the Rust-generated `tx_begin_request` frame. Before that the enum
     * was unreachable dead code (both BEGIN sites hardcode `null`), so its 0/1/2 mapping — written
     * out by hand in BOTH languages — was pinned by nothing spanning them.
     *
     * @param array<string,mixed> $m
     * @return string the encoded fixarray(3) payload
     */
    public static function encode(array $m, PackerInterface $p): string
    {
        $iso = $m['isolation'] ?? null;
        if ($iso instanceof Isolation) { $iso = $iso->value; }
        return $p->packArrayLen(3)
            . $p->packStr(SqlValueCodec::toStr($m['pool'] ?? ''))
            . ($iso === null ? $p->packNil() : $p->packUint(SqlValueCodec::toInt($iso)))
            . $p->packBool((bool) ($m['readonly'] ?? false));
    }

    /**
     * Map an already-unpacked 3-element wire array back to the "message" JSON shape.
     * @param array<int,mixed> $w
     * @return array{pool:string,isolation:?int,readonly:bool}
     */
    public static function mapFromWire(array $w): array
    {
        $w = array_values($w);
        if (count($w) !== 3) { throw new CodecException('BeginRequest arity != 3'); }
        return [
            'pool' => SqlValueCodec::toStr($w[0]),
            'isolation' => SqlValueCodec::nullableInt($w[1]),
            'readonly' => (bool) $w[2],
        ];
    }
}
