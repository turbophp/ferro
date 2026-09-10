<?php // /php/client/src/Protocol/PoolInfo.php
declare(strict_types=1);
namespace Ferro\Protocol;

/**
 * One pool's metadata from `CORE/HELLO_ACK`. Mirrors the Rust `messages::PoolInfo` BYTES: a
 * positional fixarray of 4 — [name:str, kind:str, server_version:str|nil,
 * literals_are_standard:bool|nil].
 *
 * `kind` is the backend family (`"postgres"` / `"mysql"`). `serverVersion` is the backend's own
 * `version()` output VERBATIM — normalising it (stripping PG's leading word, extracting a
 * major.minor.patch) is the consuming tier's job, not the protocol's. It is `null` when the engine
 * has not learned it, e.g. a pool whose backend was unreachable: the handshake never depends on
 * backend availability.
 *
 * `literalsAreStandard` (M2-C2g) answers ONE question — is a backslash inside a single-quoted string
 * literal an ORDINARY CHARACTER on this backend? — which is exactly the question "is doubling `'`
 * the whole quoting rule?". It is deliberately not named for either family's own setting
 * (PostgreSQL's `standard_conforming_strings`, MySQL's `NO_BACKSLASH_ESCAPES`), so a client need not
 * know which spelling to look for. **`null` means UNKNOWN and a caller must REFUSE to build a
 * literal on it** — never treat it as false, and never as true: an escaping rule is not a place for
 * a default. It costs the engine no round trip on PostgreSQL, where the value arrives as a
 * `GUC_REPORT` `ParameterStatus`; SPEC §21 D5 is why that matters.
 */
final class PoolInfo
{
    public function __construct(
        public readonly string $name,
        public readonly string $kind,
        public readonly ?string $serverVersion,
        public readonly ?bool $literalsAreStandard = null,
    ) {}

    /** Decode one already-unpacked `[name, kind, server_version, literals_are_standard]` entry. */
    public static function fromWire(mixed $w): self
    {
        if (!is_array($w) || count($w) !== 4) {
            throw new CodecException('PoolInfo: expected a 4-element array');
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
        // STRICT on the bool too, for the same reason the two required strings are strict: this
        // value gates whether a caller will build a SQL literal at all, so a coerced `1`/`"on"`
        // must not become `true` behind its back. A malformed cell is a wire fault, not an unknown.
        $literals = $v[3];
        if ($literals !== null && !is_bool($literals)) {
            throw new CodecException('PoolInfo: literals_are_standard is not bool|nil');
        }
        return new self($v[0], $v[1], $version, $literals);
    }

    /** @return array{0:string,1:string,2:string|null,3:bool|null} the positional wire shape. */
    public function toWire(): array
    {
        return [$this->name, $this->kind, $this->serverVersion, $this->literalsAreStandard];
    }
}
