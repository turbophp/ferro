<?php // /php/client/src/Protocol/PoolInfo.php
declare(strict_types=1);
namespace Ferro\Protocol;

/**
 * One pool's metadata from `CORE/HELLO_ACK`. Mirrors the Rust `messages::PoolInfo` BYTES: a
 * positional fixarray of 3 — [name:str, kind:str, server_version:str|nil].
 *
 * `kind` is the backend family (`"postgres"` / `"mysql"`). `serverVersion` is the backend's own
 * `version()` output VERBATIM — normalising it (stripping PG's leading word, extracting a
 * major.minor.patch) is the consuming tier's job, not the protocol's. It is `null` when the engine
 * has not learned it, e.g. a pool whose backend was unreachable: the handshake never depends on
 * backend availability.
 */
final class PoolInfo
{
    public function __construct(
        public readonly string $name,
        public readonly string $kind,
        public readonly ?string $serverVersion,
    ) {}

    /** Decode one already-unpacked `[name, kind, server_version]` triple. */
    public static function fromWire(mixed $w): self
    {
        if (!is_array($w) || count($w) !== 3) {
            throw new CodecException('PoolInfo: expected a 3-element array');
        }
        $v = array_values($w);
        // Strict on the two required strings too: `SqlValueCodec::toStr` coerces (an int becomes
        // "5", anything else becomes ""), which would turn a malformed triple into a silently
        // empty pool NAME — and a pool name is what routes every subsequent `ExecRequest`.
        if (!is_string($v[0]) || !is_string($v[1])) {
            throw new CodecException('PoolInfo: name and kind must both be str');
        }
        $version = $v[2];
        if ($version !== null && !is_string($version)) {
            throw new CodecException('PoolInfo: server_version is not str|nil');
        }
        return new self($v[0], $v[1], $version);
    }

    /** @return array{0:string,1:string,2:string|null} the positional wire shape. */
    public function toWire(): array
    {
        return [$this->name, $this->kind, $this->serverVersion];
    }
}
