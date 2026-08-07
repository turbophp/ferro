<?php // /php/client/src/Protocol/Msgpack/ExtPacker.php
declare(strict_types=1);
namespace Ferro\Protocol\Msgpack;
use Ferro\Protocol\CodecException;

/** ext-msgpack fast path. Only used where it byte-matches the canonical profile (conformance-gated). */
final class ExtPacker implements PackerInterface
{
    /**
     * The canonical limb encoder, used for {@see packUint} — see that method for why the extension
     * cannot serve it. Constructed once; `PurePacker` is stateless and dependency-free.
     */
    private readonly PurePacker $pure;

    public function __construct()
    {
        if (!\extension_loaded('msgpack')) { throw new CodecException('ext-msgpack absent'); }
        $this->pure = new PurePacker();
    }
    public function packNil(): string { return \msgpack_pack(null); }
    public function packBool(bool $b): string { return \msgpack_pack($b); }
    public function packInt(int $n): string { return \msgpack_pack($n); }

    /**
     * Delegated to the pure limb encoder — deliberately NOT `\msgpack_pack()`.
     *
     * This method used to read `\msgpack_pack(is_string($n) ? (int) $n : $n)`, which SILENTLY
     * SATURATED every `uint64` above `PHP_INT_MAX` at `PHP_INT_MAX`: `'18446744073709551615'` went
     * out as `cf7fffffffffffffff` instead of `cfffffffffffffffff`, and a non-numeric string went out
     * as `0`. That was latent only while nothing passed a string; the M1-S7 bind path does
     * (`TAG_U64` carries its canonical decimal text, hazard 29/48), so on this path the cast is a
     * silent corrupt WRITE.
     *
     * The extension cannot fix it: `\msgpack_pack()` takes a PHP value, and PHP's `int` is signed
     * 64-bit, so the top half of the `uint64` range has no argument it could be handed. Throwing
     * instead would be worse than delegating — a host that happens to load ext-msgpack would then
     * refuse a `BIGINT UNSIGNED` bind that works everywhere else. `PurePacker::packUint` encodes the
     * whole range losslessly with the same canonical narrowing ladder, so the bytes are identical to
     * the default encode path (`PackerFactory::forEncode`), and its validation (non-numeric string,
     * negative int, past `u64::MAX`) becomes a loud {@see CodecException} instead of a `0`.
     *
     * @throws CodecException
     */
    public function packUint(int|string $n): string { return $this->pure->packUint($n); }
    public function packFloat64(float $f): string { return \msgpack_pack($f); }
    public function packStr(string $s): string { return \msgpack_pack($s); }
    /**
     * Delegated to the pure limb encoder — deliberately NOT `\msgpack_pack()`.
     *
     * `\msgpack_pack()` on a PHP string emits msgpack **`str`** (measured on pecl msgpack 3.0.0:
     * `msgpack_pack("ab")` is `a26162`, a fixstr), while the wire contract for `TAG_BYTES` is the
     * **`bin`** family and the engine's decoder is marker-strict (`read_bin` accepts only
     * `0xc4`/`0xc5`/`0xc6`). The extension has no way to express the distinction: PHP has one string
     * type, and the `bin` emitter in the extension's own `pack_template.h` has no callers. This was
     * latent while nothing bound `TAG_BYTES` and `PackerFactory::forEncode()` returned `PurePacker`
     * regardless; {@see \Ferro\Bytes} creates the first call path. Same shape and same reason as
     * {@see packUint}.
     */
    public function packBin(string $s): string { return $this->pure->packBin($s); }
    public function packArrayLen(int $n): string { throw new CodecException('ExtPacker packs whole values, not array headers'); }
    public function unpack(string $buf, int &$offset): mixed { $v = \msgpack_unpack($buf); $offset = strlen($buf); return $v; }
}
