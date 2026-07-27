<?php // /php/client/src/Protocol/Msgpack/PurePacker.php
declare(strict_types=1);
namespace Ferro\Protocol\Msgpack;

use Ferro\Protocol\CodecException;

/**
 * Dependency-free MessagePack encoder/decoder pinned to Ferro's canonical profile
 * (signed-smallest ints, big-endian float64, str/bin families, fixarray). Mirrors `rmp`.
 */
final class PurePacker implements PackerInterface
{
    public function packNil(): string { return "\xc0"; }
    public function packBool(bool $b): string { return $b ? "\xc3" : "\xc2"; }

    public function packInt(int $n): string
    {
        // Canonical = rmp write_sint: NON-NEGATIVE narrows to unsigned markers (cc/cd/ce/cf),
        // NEGATIVE to signed markers (d0/d1/d2/d3). This is the load-bearing cross-language rule.
        if ($n >= 0) {
            if ($n <= 0x7f) { return chr($n); }                      // positive fixint
            if ($n <= 0xff) { return "\xcc" . chr($n); }             // uint8
            if ($n <= 0xffff) { return "\xcd" . pack('n', $n); }     // uint16 BE
            if ($n <= 0xffffffff) { return "\xce" . pack('N', $n); } // uint32 BE
            return "\xcf" . pack('J', $n);                           // uint64 BE
        }
        if ($n >= -32) { return chr(0xe0 | ($n & 0x1f)); }           // negative fixint
        if ($n >= -128) { return "\xd0" . pack('c', $n); }           // int8
        if ($n >= -32768) { return "\xd1" . pack('n', $n & 0xffff); } // int16 BE
        if ($n >= -2147483648) { return "\xd2" . pack('N', $n & 0xffffffff); } // int32 BE
        return "\xd3" . pack('J', $n);                               // int64 BE
    }

    public function packUint(int|string $n): string
    {
        // Only used for fields known unsigned (e.g. boot_epoch). Accept string for > PHP_INT_MAX.
        if (is_string($n)) {
            // Canonical narrowing: only genuinely-large values use uint64; small strings narrow like ints.
            if (!preg_match('/^\d+$/', $n)) { throw new CodecException('packUint: non-numeric string'); }
            $trimmed = ltrim($n, '0'); if ($trimmed === '') { $trimmed = '0'; }
            $max = '9223372036854775807';
            if (strlen($trimmed) < strlen($max) || (strlen($trimmed) === strlen($max) && strcmp($trimmed, $max) <= 0)) {
                return $this->packUint((int) $n);
            }
            return "\xcf" . self::decToBe64($n);
        }
        if ($n < 0) { throw new CodecException('packUint got negative'); }
        if ($n <= 0x7f) { return chr($n); }
        if ($n <= 0xff) { return "\xcc" . chr($n); }
        if ($n <= 0xffff) { return "\xcd" . pack('n', $n); }
        if ($n <= 0xffffffff) { return "\xce" . pack('N', $n); }
        return "\xcf" . pack('J', $n);
    }

    public function packFloat64(float $f): string { return "\xcb" . pack('E', $f); } // 'E' = double BE

    public function packStr(string $s): string
    {
        $len = strlen($s);
        if ($len <= 31) { return chr(0xa0 | $len) . $s; }
        if ($len <= 0xff) { return "\xd9" . chr($len) . $s; }
        if ($len <= 0xffff) { return "\xda" . pack('n', $len) . $s; }
        return "\xdb" . pack('N', $len) . $s;
    }

    public function packBin(string $s): string
    {
        $len = strlen($s);
        if ($len <= 0xff) { return "\xc4" . chr($len) . $s; }
        if ($len <= 0xffff) { return "\xc5" . pack('n', $len) . $s; }
        return "\xc6" . pack('N', $len) . $s;
    }

    public function packArrayLen(int $n): string
    {
        if ($n <= 15) { return chr(0x90 | $n); }
        if ($n <= 0xffff) { return "\xdc" . pack('n', $n); }
        return "\xdd" . pack('N', $n);
    }

    public function unpack(string $buf, int &$offset): mixed
    {
        self::need($buf, $offset, 1);
        $c = self::readByte($buf, $offset);
        if ($c <= 0x7f) { return $c; }                       // positive fixint
        if ($c >= 0xe0) { return $c - 0x100; }               // negative fixint
        if ($c >= 0x90 && $c <= 0x9f) { return $this->unpackArray($buf, $offset, $c & 0x0f); }
        if ($c >= 0xa0 && $c <= 0xbf) { return $this->take($buf, $offset, $c & 0x1f); } // fixstr
        return match ($c) {
            0xc0 => null,
            0xc2 => false, 0xc3 => true,
            0xcc => self::readByte($buf, $offset),
            0xcd => $this->be($buf, $offset, 2, false),
            0xce => $this->be($buf, $offset, 4, false),
            0xcf => $this->be($buf, $offset, 8, false),
            0xd0 => $this->signed8($buf, $offset),
            0xd1 => $this->be($buf, $offset, 2, true),
            0xd2 => $this->be($buf, $offset, 4, true),
            0xd3 => $this->be($buf, $offset, 8, true),
            0xca => $this->unpackF32($buf, $offset),
            0xcb => $this->unpackF64($buf, $offset),
            0xd9 => $this->take($buf, $offset, self::readByte($buf, $offset)),
            0xda => $this->take($buf, $offset, (int) $this->be($buf, $offset, 2, false)),
            0xdb => $this->take($buf, $offset, (int) $this->be($buf, $offset, 4, false)),
            0xc4 => $this->take($buf, $offset, self::readByte($buf, $offset)),
            0xc5 => $this->take($buf, $offset, (int) $this->be($buf, $offset, 2, false)),
            0xc6 => $this->take($buf, $offset, (int) $this->be($buf, $offset, 4, false)),
            0xdc => $this->unpackArray($buf, $offset, (int) $this->be($buf, $offset, 2, false)),
            0xdd => $this->unpackArray($buf, $offset, (int) $this->be($buf, $offset, 4, false)),
            default => throw new CodecException(sprintf('unknown msgpack marker 0x%02x', $c)),
        };
    }

    /**
     * Bounds check every raw read: a truncated or lying-length frame must throw CodecException
     * rather than silently fabricate 0/"" (empty-string offset access) or loop unbounded.
     */
    private static function need(string $buf, int $offset, int $len): void
    {
        if ($len < 0 || $offset + $len > strlen($buf)) {
            throw new CodecException(sprintf('truncated: need %d bytes at offset %d, have %d', $len, $offset, strlen($buf) - $offset));
        }
    }
    /** Bounds-checked single-byte read; advances $offset. */
    private static function readByte(string $buf, int &$offset): int
    {
        self::need($buf, $offset, 1);
        return ord($buf[$offset++]);
    }

    /** @return list<mixed> */
    private function unpackArray(string $buf, int &$offset, int $n): array
    {
        // Each element is >= 1 byte on the wire, so this is a valid, allocation-free bound that
        // rejects a lying/oversized length (e.g. array32 claiming ~4e9 elements) before looping.
        if ($n > strlen($buf) - $offset) {
            throw new CodecException("array length {$n} exceeds remaining bytes");
        }
        $a = [];
        for ($i = 0; $i < $n; $i++) { $a[] = $this->unpack($buf, $offset); }
        return $a;
    }
    private function take(string $buf, int &$offset, int $len): string
    {
        self::need($buf, $offset, $len);
        $s = substr($buf, $offset, $len); $offset += $len; return $s;
    }
    private function signed8(string $buf, int &$offset): int
    {
        self::need($buf, $offset, 1);
        $v = ord($buf[$offset++]); return $v < 128 ? $v : $v - 256;
    }
    /** Big-endian integer of $bytes width; returns int, or decimal string for uint64 > PHP_INT_MAX. */
    private function be(string $buf, int &$offset, int $bytes, bool $signed): int|string
    {
        self::need($buf, $offset, $bytes);
        $slice = substr($buf, $offset, $bytes); $offset += $bytes;
        if ($bytes < 8) {
            $v = 0; foreach (str_split($slice) as $b) { $v = ($v << 8) | ord($b); }
            if ($signed) { $bits = $bytes * 8; if ($v >= (1 << ($bits - 1))) { $v -= (1 << $bits); } }
            return $v;
        }
        // 8 bytes
        if ($signed) { return self::unpackInt('J', $slice); } // PHP int is 64-bit signed
        return self::be64ToDec($slice); // unsigned 64: return decimal string to preserve > PHP_INT_MAX
    }
    private function unpackF32(string $buf, int &$offset): float
    { self::need($buf, $offset, 4); $s = substr($buf, $offset, 4); $offset += 4; return self::unpackFloat('G', $s); }
    private function unpackF64(string $buf, int &$offset): float
    { self::need($buf, $offset, 8); $s = substr($buf, $offset, 8); $offset += 8; return self::unpackFloat('E', $s); }

    /**
     * unpack() with a single-field format string always yields an array{1: mixed} on well-formed
     * input of the right length; these narrow the array|false PHPStan sees for the 'J'/'G'/'E'
     * formats used here (int, 32-bit float, 64-bit float respectively).
     */
    private static function unpackInt(string $format, string $bytes): int
    {
        $u = unpack($format, $bytes);
        if ($u === false || !isset($u[1]) || !is_int($u[1])) { throw new CodecException('unpack failed'); }
        return $u[1];
    }
    private static function unpackFloat(string $format, string $bytes): float
    {
        $u = unpack($format, $bytes);
        if ($u === false || !isset($u[1]) || !is_float($u[1])) { throw new CodecException('unpack failed'); }
        return $u[1];
    }

    private static function be64ToDec(string $be): string
    {
        // Convert 8 big-endian bytes to an unsigned decimal string without bcmath/gmp.
        $dec = '0';
        foreach (str_split($be) as $byte) {
            $carry = ord($byte);
            // dec = dec*256 + carry, done via simple string math
            $dec = self::mulAdd($dec, 256, $carry);
        }
        return $dec;
    }
    private static function mulAdd(string $dec, int $mul, int $add): string
    {
        $carry = $add; $out = '';
        for ($i = strlen($dec) - 1; $i >= 0; $i--) {
            $prod = ((int) $dec[$i]) * $mul + $carry;
            $out = ((string) ($prod % 10)) . $out;
            $carry = intdiv($prod, 10);
        }
        while ($carry > 0) { $out = ((string) ($carry % 10)) . $out; $carry = intdiv($carry, 10); }
        return ltrim($out, '0') ?: '0';
    }
    private static function decToBe64(string $dec): string
    {
        // Convert an unsigned decimal string to 8 big-endian bytes.
        $bytes = array_fill(0, 8, 0); $n = $dec;
        for ($pos = 7; $pos >= 0 && $n !== '0'; $pos--) {
            [$n, $rem] = self::divmod($n, 256); $bytes[$pos] = $rem;
        }
        if ($n !== '0') { throw new CodecException('packUint value exceeds u64'); }
        return implode('', array_map('chr', $bytes));
    }
    /** @return array{0:string,1:int} */
    private static function divmod(string $dec, int $div): array
    {
        $q = ''; $rem = 0;
        for ($i = 0; $i < strlen($dec); $i++) {
            $cur = $rem * 10 + (int) $dec[$i];
            $q .= (string) intdiv($cur, $div); $rem = $cur % $div;
        }
        return [ltrim($q, '0') ?: '0', $rem];
    }
}
