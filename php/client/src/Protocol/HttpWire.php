<?php // /php/client/src/Protocol/HttpWire.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Shared field rules for the HTTP-service codecs (service `HTTP`, M6-F2; SPEC §23.5,
 * /proto/PROTOCOL.md §12): the header-list shape `array<[name: str, value: bin]>`, and the strict
 * scalar checks every HTTP decoder applies.
 *
 * **STRICT, unlike the coercing `SqlValueCodec` helpers the SQL/TX codecs share.** A field of the
 * wrong type is a malformed frame — a {@see CodecException} — never a value to invent (the C3-7b-2
 * `BackupResponse` lesson: a coercing decoder read `[nil, nil, nil]` as a 0-byte success). The same
 * holds on ENCODE: a value the engine's decoder would refuse (a timeout past `u32`, a non-string
 * header name) is refused here, before a byte is written, rather than sent to fail as `Protocol`.
 *
 * **What PHP cannot check, stated:** a msgpack `str` and `bin` both unpack to a PHP string, so this
 * side cannot tell a header VALUE that arrived as `str` from one that arrived as `bin`. The Rust
 * decoder can, and refuses the wrong family; the PHP ENCODER always writes the right one, which the
 * golden vectors lock byte for byte.
 */
final class HttpWire
{
    public const U32_MAX = 0xFFFFFFFF;

    /** @param list<array{0:string,1:string}> $headers */
    public static function packHeaders(PackerInterface $p, array $headers): string
    {
        $out = $p->packArrayLen(count($headers));
        foreach ($headers as $h) {
            $out .= $p->packArrayLen(2) . $p->packStr($h[0]) . $p->packBin($h[1]);
        }
        return $out;
    }

    /**
     * Validate a caller-supplied header list for ENCODE: a list of `[name, value]` string pairs.
     * @return list<array{0:string,1:string}>
     */
    public static function headersIn(mixed $v, string $what): array
    {
        if (!is_array($v) || !array_is_list($v)) {
            throw new CodecException("{$what}: headers must be a list of [name, value] pairs");
        }
        $out = [];
        foreach ($v as $i => $pair) {
            if (!is_array($pair) || !array_is_list($pair) || count($pair) !== 2
                || !is_string($pair[0]) || !is_string($pair[1])) {
                throw new CodecException("{$what}: header {$i} is not a [string, string] pair");
            }
            if (preg_match('//u', $pair[0]) !== 1) {
                // The wire's name is a msgpack `str`, which the engine refuses unless it is UTF-8.
                throw new CodecException("{$what}: header {$i}'s name is not UTF-8");
            }
            $out[] = [$pair[0], $pair[1]];
        }
        return $out;
    }

    /**
     * Decode an unpacked header list (both `str` and `bin` arrive as PHP strings — see the class doc).
     * @return list<array{0:string,1:string}>
     */
    public static function headersOut(mixed $v, string $what): array
    {
        if (!is_array($v) || !array_is_list($v)) {
            throw new CodecException("{$what}: headers is not an array");
        }
        $out = [];
        foreach ($v as $i => $pair) {
            if (!is_array($pair) || count($pair) !== 2) {
                throw new CodecException("{$what}: header {$i} is not a 2-element array");
            }
            $pair = array_values($pair);
            if (!is_string($pair[0]) || !is_string($pair[1])) {
                throw new CodecException("{$what}: header {$i} is not a [str, bin] pair");
            }
            $out[] = [$pair[0], $pair[1]];
        }
        return $out;
    }

    public static function string(mixed $v, string $what): string
    {
        if (!is_string($v)) { throw new CodecException("{$what} is not a string"); }
        return $v;
    }

    public static function nullableString(mixed $v, string $what): ?string
    {
        return $v === null ? null : self::string($v, $what);
    }

    /** A `str` the ENGINE decodes strictly: refused here unless it is UTF-8. */
    public static function utf8(mixed $v, string $what): string
    {
        $s = self::string($v, $what);
        if (preg_match('//u', $s) !== 1) { throw new CodecException("{$what} is not UTF-8"); }
        return $s;
    }

    public static function nullableUtf8(mixed $v, string $what): ?string
    {
        return $v === null ? null : self::utf8($v, $what);
    }

    public static function bool(mixed $v, string $what): bool
    {
        if (!is_bool($v)) { throw new CodecException("{$what} is not a bool"); }
        return $v;
    }

    public static function nullableBool(mixed $v, string $what): ?bool
    {
        return $v === null ? null : self::bool($v, $what);
    }

    /**
     * A non-negative native int no larger than `$max`. A u64 past `PHP_INT_MAX` unpacks as a decimal
     * STRING, and is refused here: every u64 on this service is contractually bounded below 2^63.
     */
    public static function uint(mixed $v, int $max, string $what): int
    {
        if (!is_int($v) || $v < 0 || $v > $max) {
            throw new CodecException("{$what} is not an integer in 0..{$max}");
        }
        return $v;
    }

    public static function nullableUint(mixed $v, int $max, string $what): ?int
    {
        return $v === null ? null : self::uint($v, $max, $what);
    }

    /**
     * Unpack exactly one msgpack value spanning the whole payload into a positional array of
     * `$arity` elements.
     * @return list<mixed>
     */
    public static function unpackArray(string $payload, PackerInterface $p, int $arity, string $what): array
    {
        $off = 0;
        $w = $p->unpack($payload, $off);
        if ($off !== strlen($payload)) { throw new CodecException("{$what}: trailing bytes"); }
        return self::arity($w, $arity, $what);
    }

    /** @return list<mixed> */
    public static function arity(mixed $w, int $arity, string $what): array
    {
        if (!is_array($w) || count($w) !== $arity) {
            throw new CodecException("{$what} arity != {$arity}");
        }
        return array_values($w);
    }
}
