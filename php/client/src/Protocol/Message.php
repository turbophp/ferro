<?php // /php/client/src/Protocol/Message.php
declare(strict_types=1);
namespace Ferro\Protocol;
use Ferro\Protocol\Msgpack\PackerInterface;

/**
 * Encodes core-service messages as positional MessagePack arrays whose field order matches
 * PROTOCOL.md and the Rust structs (compact rmp-serde layout). Byte-identity vs the Rust codec is
 * locked by golden vectors (Task 9). Server->client-only payloads (error Outcomes) are decode-only.
 * All integer fields here are unsigned in the Rust structs, so packUint is correct throughout.
 */
final class Message
{
    /** @param array<string,mixed> $f logical fields (e.g. a golden vector's "message" object) */
    public static function encode(string $name, array $f, PackerInterface $p): string
    {
        return match ($name) {
            'hello' => self::arr($p, [
                $p->packUint(self::i($f, 'client_version')),
                $p->packStr(self::s($f, 'type_registry_hash')),
                ($f['manifest_hash'] ?? null) === null ? $p->packNil() : $p->packStr(self::s($f, 'manifest_hash')),
                $p->packUint(self::i($f, 'pid')),
                $p->packUint(self::i($f, 'features')),
            ]),
            'hello_ack' => self::arr($p, [
                $p->packUint(self::i($f, 'engine_version')),
                $p->packUint(self::u($f, 'boot_epoch')),   // string-safe for > PHP_INT_MAX
                $p->packUint(self::i($f, 'features')),
                self::poolInfoArray($p, $f['pools'] ?? []),
                $p->packStr(self::s($f, 'type_registry_hash')),
            ]),
            'ping', 'pong' => self::arr($p, [$p->packUint(self::u($f, 'token'))]),
            'goodbye' => self::arr($p, []),
            'window_update' => self::arr($p, [$p->packUint(self::i($f, 'frames')), $p->packUint(self::i($f, 'bytes'))]),
            default => throw new CodecException("no client encoder for message '$name'"),
        };
    }

    /** @param list<string> $items already-encoded field byte-strings */
    private static function arr(PackerInterface $p, array $items): string
    {
        return $p->packArrayLen(count($items)) . implode('', $items);
    }
    /**
     * `HelloAck.pools` (M1-S8a): an array of NESTED `[name, kind, server_version]` triples, not
     * bare names. The input is either a golden vector's decoded-JSON `message.pools` (a list of
     * assoc arrays) or a list of {@see PoolInfo}; both narrow to the same positional triple, so the
     * byte lock exercises exactly the shape {@see PoolInfo::toWire} produces.
     */
    private static function poolInfoArray(PackerInterface $p, mixed $pools): string
    {
        $list = is_array($pools) ? $pools : [];
        $out = $p->packArrayLen(count($list));
        foreach ($list as $entry) {
            if ($entry instanceof PoolInfo) { $entry = $entry->toWire(); }
            if (!is_array($entry)) {
                throw new CodecException('hello_ack: each pool must be a [name, kind, server_version] triple');
            }
            $name = $entry['name'] ?? $entry[0] ?? null;
            $kind = $entry['kind'] ?? $entry[1] ?? null;
            $version = $entry['server_version'] ?? $entry[2] ?? null;
            $out .= $p->packArrayLen(3)
                . $p->packStr(self::scalarToStr($name))
                . $p->packStr(self::scalarToStr($kind))
                . ($version === null ? $p->packNil() : $p->packStr(self::scalarToStr($version)));
        }
        return $out;
    }
    /** @param array<string,mixed> $f */
    private static function i(array $f, string $k): int { return self::scalarToInt($f[$k] ?? 0); }
    /** @param array<string,mixed> $f */
    private static function s(array $f, string $k): string { return self::scalarToStr($f[$k] ?? ''); }
    /** @param array<string,mixed> $f @return int|string */
    private static function u(array $f, string $k): int|string
    {
        $v = $f[$k] ?? 0;
        return is_string($v) ? $v : self::scalarToInt($v);
    }

    /** Narrows a decoded-JSON scalar (int|float|string|bool|null) to int; unknown shapes fall back to 0. */
    private static function scalarToInt(mixed $v): int
    {
        return match (true) {
            is_int($v) => $v,
            is_float($v), is_string($v), is_bool($v) => (int) $v,
            default => 0,
        };
    }
    /** Narrows a decoded-JSON scalar (int|float|string|bool|null) to string; unknown shapes fall back to ''. */
    private static function scalarToStr(mixed $v): string
    {
        return match (true) {
            is_string($v) => $v,
            is_int($v), is_float($v) => (string) $v,
            is_bool($v) => $v ? '1' : '',
            default => '',
        };
    }
}
