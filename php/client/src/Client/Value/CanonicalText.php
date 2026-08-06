<?php // /php/client/src/Client/Value/CanonicalText.php
declare(strict_types=1);
namespace Ferro\Client\Value;

use Ferro\Client\Error\ProtocolException;
use Ferro\Protocol\Generated\Constants as C;

/**
 * The ONE place a decoded wire payload is checked against the canonical contract of
 * `/proto/PROTOCOL.md` §3.2 — shared by the §9 value objects ({@see \Ferro\Decimal},
 * {@see \Ferro\Date}, …) and by both M1 policies ({@see M1ValuePolicy},
 * {@see RawStringValuePolicy}) so a payload is judged identically wherever it enters.
 *
 * **Every guard THROWS; none of them coerces (hazard 30).** The M0 idiom
 * ({@see M0ValuePolicy::toInt}/`toStr`, {@see \Ferro\Protocol\SqlValueCodec::toStr}) returns
 * `0`/`0.0`/`''` for an unexpected payload — copied here it would turn a malformed `DECIMAL` into
 * `Decimal('')` and a truncated `TIMESTAMP` into epoch-zero, which is exactly the silent miscast
 * SPEC §9.1 exists to prevent. A malformed payload is a WIRE fault, so it is a
 * {@see ProtocolException}; an operator's policy refusal is a
 * {@see \Ferro\Client\Error\TypePolicyException} and lives in the policy, not here.
 *
 * **Sentinels are validated but never parsed.** `"infinity"` / `"-infinity"` / `"0000-00-00"` /
 * `"0000-00-00 00:00:00"` — and MySQL's zero-IN-date forms (`"2026-00-05"`, legal without
 * `NO_ZERO_IN_DATE`) — are legal canonical payloads that are deliberately NOT constructible as a
 * calendar value. `dateIsSentinel`/`timestampIsInstant`/`timestamptzIsInstant` are how a caller asks
 * that question BEFORE handing the text to a date parser (which would either throw or invent a
 * date, both silent-corruption classes).
 */
final class CanonicalText
{
    /** The largest `uint64`; a decimal payload above it cannot have come off the wire. */
    public const U64_MAX = '18446744073709551615';

    /** PG `NUMERIC` non-finite renderings — legal `DECIMAL` payloads (PROTOCOL.md §3.2). */
    public const DECIMAL_SPECIALS = ['NaN', 'Infinity', '-Infinity'];

    /** The two PG range sentinels, shared by `DATE`/`TIMESTAMP`/`TIMESTAMPTZ`. */
    public const INFINITIES = ['infinity', '-infinity'];

    /** The MySQL zero `TIMESTAMP` sentinel: no `T`, no `Z` — it is not an instant. */
    public const ZERO_DATETIME = '0000-00-00 00:00:00';

    private const RE_DATE = '/^(\d{4})-(\d{2})-(\d{2})$/';
    private const RE_TIME = '/^(-?)(\d{2,})\:(\d{2}):(\d{2})(?:\.(\d{6}))?$/';
    private const RE_TIMESTAMP = '/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.\d{6})?$/';
    private const RE_TIMESTAMPTZ = '/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{6})?Z$/';
    private const RE_UUID = '/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/';
    private const RE_DECIMAL = '/^[+-]?\d+(?:\.\d+)?$/';
    private const RE_DIGITS = '/^\d+$/';

    // ---- payload-family guards (no coercion, ever) -----------------------------------------------

    /** @throws ProtocolException */
    public static function requireNull(mixed $data, int $tag = C::TAG_NULL): null
    {
        if ($data !== null) {
            throw self::wrongFamily($tag, 'nil', $data);
        }
        return null;
    }

    /** @throws ProtocolException */
    public static function requireBool(mixed $data, int $tag = C::TAG_BOOL): bool
    {
        if (!is_bool($data)) {
            throw self::wrongFamily($tag, 'bool', $data);
        }
        return $data;
    }

    /** @throws ProtocolException */
    public static function requireInt(mixed $data, int $tag = C::TAG_I64): int
    {
        if (!is_int($data)) {
            throw self::wrongFamily($tag, 'int', $data);
        }
        return $data;
    }

    /**
     * A `float64` payload — and ONLY that. `/proto/PROTOCOL.md` §3.1 pins `F64`'s family to
     * `float64` (not "the float family"), and the Rust encoder emits the fixed `0xcb` marker
     * unconditionally (`value.rs`, `enc::write_f64`), so an `int` here means the peer broke the
     * contract. Accepting one would re-open the coercion door for a value whose `(float)` cast is
     * lossy above 2^53.
     *
     * @throws ProtocolException
     */
    public static function requireFloat(mixed $data, int $tag = C::TAG_F64): float
    {
        if (!is_float($data)) {
            throw self::wrongFamily($tag, 'float64', $data);
        }
        return $data;
    }

    /** @throws ProtocolException */
    public static function requireString(mixed $data, int $tag): string
    {
        if (!is_string($data)) {
            throw self::wrongFamily($tag, 'str (canonical text)', $data);
        }
        return $data;
    }

    /**
     * A `BYTES` payload: a `list<int>` of bytes ({@see \Ferro\Protocol\SqlValueCodec::fromWire}
     * turns the wire `bin` into one so a non-UTF8 blob survives the golden-vector JSON), or a raw
     * binary string. Every element must be a real byte — `chr()` on a non-int would silently write
     * a NUL.
     *
     * @throws ProtocolException
     */
    public static function requireBytes(mixed $data, int $tag = C::TAG_BYTES): string
    {
        if (is_string($data)) {
            return $data;
        }
        if (!is_array($data)) {
            throw self::wrongFamily($tag, 'bin (a list<int> of bytes)', $data);
        }
        $out = '';
        foreach ($data as $b) {
            if (!is_int($b) || $b < 0 || $b > 255) {
                throw self::malformed($tag, 'BYTES', 'a list of 0..255 byte ints');
            }
            $out .= chr($b);
        }
        return $out;
    }

    // ---- canonical-text validators ---------------------------------------------------------------

    /**
     * `DECIMAL`: `[+-]digits[.digits]` at full precision with the DISPLAY SCALE PRESERVED, or one of
     * `NaN` / `Infinity` / `-Infinity`. Never normalized: `1.10` and `1.1` are distinct payloads and
     * DBAL compares them as strings.
     *
     * @throws ProtocolException
     */
    public static function decimal(string $text): string
    {
        if (in_array($text, self::DECIMAL_SPECIALS, true)) {
            return $text;
        }
        if (preg_match(self::RE_DECIMAL, $text) !== 1) {
            throw self::malformed(C::TAG_DECIMAL, 'DECIMAL', '[+-]digits[.digits], NaN, Infinity or -Infinity');
        }
        return $text;
    }

    /**
     * `DATE`: `YYYY-MM-DD`, or `infinity` / `-infinity`. A zero year/month/day component is legal
     * (a MySQL zero date or zero-in-date) and is carried verbatim; an impossible calendar day
     * (`2026-13-01`, `2026-02-30`) is malformed.
     *
     * @throws ProtocolException
     */
    public static function date(string $text): string
    {
        if (in_array($text, self::INFINITIES, true)) {
            return $text;
        }
        if (preg_match(self::RE_DATE, $text, $m) !== 1) {
            throw self::malformed(C::TAG_DATE, 'DATE', 'YYYY-MM-DD, infinity or -infinity');
        }
        self::checkDateParts((int) $m[1], (int) $m[2], (int) $m[3], C::TAG_DATE, 'DATE');
        return $text;
    }

    /** Whether an ALREADY-VALIDATED `DATE` payload is a sentinel rather than a calendar day. */
    public static function dateIsSentinel(string $text): bool
    {
        if (in_array($text, self::INFINITIES, true)) {
            return true;
        }
        return preg_match(self::RE_DATE, $text, $m) === 1
            && ((int) $m[1] === 0 || (int) $m[2] === 0 || (int) $m[3] === 0);
    }

    /**
     * `TIME`: `[-]HH:MM:SS[.ffffff]`. Hours are NOT bounded at 23 — PG's `time '24:00:00'` is legal
     * and a MySQL `TIME` spans ±838 h — and the leading `-` is MySQL's negative interval.
     *
     * @throws ProtocolException
     */
    public static function time(string $text): string
    {
        if (preg_match(self::RE_TIME, $text, $m) !== 1) {
            throw self::malformed(C::TAG_TIME, 'TIME', '[-]HH:MM:SS[.ffffff] with exactly six fractional digits');
        }
        if ((int) $m[3] > 59 || (int) $m[4] > 59) {
            throw self::malformed(C::TAG_TIME, 'TIME', 'minutes and seconds in 0..59');
        }
        return $text;
    }

    /** Whether an ALREADY-VALIDATED `TIME` payload is negative (MySQL only). */
    public static function timeIsNegative(string $text): bool
    {
        return str_starts_with($text, '-');
    }

    /**
     * `TIMESTAMP`: `YYYY-MM-DD HH:MM:SS[.ffffff]`, NAIVE — no zone suffix, ever. Sentinels:
     * `infinity` / `-infinity` (PG) and `0000-00-00 00:00:00` (a MySQL zero datetime, which the
     * all-zero date components below classify automatically).
     *
     * @throws ProtocolException
     */
    public static function timestamp(string $text): string
    {
        if (in_array($text, self::INFINITIES, true)) {
            return $text;
        }
        if (preg_match(self::RE_TIMESTAMP, $text, $m) !== 1) {
            throw self::malformed(
                C::TAG_TIMESTAMP,
                'TIMESTAMP',
                'YYYY-MM-DD HH:MM:SS[.ffffff] (naive, six fractional digits or none), infinity or -infinity',
            );
        }
        self::checkDateParts((int) $m[1], (int) $m[2], (int) $m[3], C::TAG_TIMESTAMP, 'TIMESTAMP');
        self::checkTimeOfDay((int) $m[4], (int) $m[5], (int) $m[6], C::TAG_TIMESTAMP, 'TIMESTAMP');
        return $text;
    }

    /** Whether an ALREADY-VALIDATED `TIMESTAMP` payload is a constructible wall-clock value. */
    public static function timestampIsInstant(string $text): bool
    {
        return preg_match(self::RE_TIMESTAMP, $text, $m) === 1
            && (int) $m[1] !== 0 && (int) $m[2] !== 0 && (int) $m[3] !== 0;
    }

    /**
     * `TIMESTAMPTZ`: RFC3339 `YYYY-MM-DDTHH:MM:SS[.ffffff]Z`, ALWAYS normalized to UTC and always
     * the literal `Z`. Sentinels: `infinity` / `-infinity` (PG) and the MySQL zero `TIMESTAMP`,
     * which carries the SAME `0000-00-00 00:00:00` text as a zero datetime — no `T`, no `Z` — so it
     * can never be mistaken for an instant.
     *
     * @throws ProtocolException
     */
    public static function timestamptz(string $text): string
    {
        if (in_array($text, self::INFINITIES, true) || $text === self::ZERO_DATETIME) {
            return $text;
        }
        if (preg_match(self::RE_TIMESTAMPTZ, $text, $m) !== 1) {
            throw self::malformed(
                C::TAG_TIMESTAMPTZ,
                'TIMESTAMPTZ',
                'YYYY-MM-DDTHH:MM:SS[.ffffff]Z (RFC3339, UTC), infinity, -infinity or ' . self::ZERO_DATETIME,
            );
        }
        self::checkDateParts((int) $m[1], (int) $m[2], (int) $m[3], C::TAG_TIMESTAMPTZ, 'TIMESTAMPTZ');
        self::checkTimeOfDay((int) $m[4], (int) $m[5], (int) $m[6], C::TAG_TIMESTAMPTZ, 'TIMESTAMPTZ');
        return $text;
    }

    /** Whether an ALREADY-VALIDATED `TIMESTAMPTZ` payload is a constructible UTC instant. */
    public static function timestamptzIsInstant(string $text): bool
    {
        return preg_match(self::RE_TIMESTAMPTZ, $text, $m) === 1
            && (int) $m[1] !== 0 && (int) $m[2] !== 0 && (int) $m[3] !== 0;
    }

    /** `UUID`: the 36-char canonical LOWERCASE hyphenated form — never raw bytes, never braced. */
    public static function uuid(string $text): string
    {
        if (preg_match(self::RE_UUID, $text) !== 1) {
            throw self::malformed(C::TAG_UUID, 'UUID', 'the 36-char lowercase hyphenated canonical form');
        }
        return $text;
    }

    /**
     * `U64` → its canonical DECIMAL STRING, from EITHER wire form (hazard 28).
     *
     * The value's PHP type follows its MAGNITUDE, not its tag: `PurePacker::be()` hands back a
     * decimal STRING for every `0xcf`-marked `uint64`, while the canonical narrowing ladder emits
     * `0xcc`/`0xcd`/`0xce` (→ a PHP `int`) for anything at or below `0xffffffff`. So `5` arrives as
     * `int 5` and `2^33` arrives as `'8589934592'`. A branch on `is_int($data)` mishandles the whole
     * 2^32..2^64 range — both forms normalize here, and the `PHP_INT_MAX` comparison is done on the
     * decimal string (see {@see fitsPhpInt}) so nothing ever rides through a lossy `(int)` cast.
     *
     * @throws ProtocolException
     */
    public static function u64(mixed $data, int $tag = C::TAG_U64): string
    {
        if (is_int($data)) {
            if ($data < 0) {
                throw self::malformed($tag, 'U64', 'a non-negative integer');
            }
            return (string) $data;
        }
        if (!is_string($data) || preg_match(self::RE_DIGITS, $data) !== 1) {
            throw self::wrongFamily($tag, 'uint (an int or a decimal string)', $data);
        }
        $n = ltrim($data, '0');
        if ($n === '') {
            $n = '0';
        }
        if (self::compareDecimals($n, self::U64_MAX) > 0) {
            throw self::malformed($tag, 'U64', 'a value at or below u64::MAX (' . self::U64_MAX . ')');
        }
        return $n;
    }

    /**
     * Whether a canonical (leading-zero-free) decimal string fits a PHP `int`. Done by digit
     * comparison — dependency-free (charter rule 7: no bcmath/gmp requirement) and lossless, unlike
     * `(int) $s` which SATURATES at `PHP_INT_MAX` and would report every overflow as a fit.
     */
    public static function fitsPhpInt(string $decimal): bool
    {
        return self::compareDecimals($decimal, (string) PHP_INT_MAX) <= 0;
    }

    /** A loud, named refusal for a tag this client does not implement. */
    public static function unsupportedTag(int $tag): ProtocolException
    {
        $deferred = [
            C::TAG_ARRAY => 'ARRAY',
            C::TAG_INTERVAL => 'INTERVAL',
            C::TAG_INET => 'INET',
            C::TAG_VECTOR => 'VECTOR',
        ];
        $name = $deferred[$tag] ?? null;
        if ($name !== null) {
            return new ProtocolException(sprintf(
                'value tag %d (%s) is deferred beyond M1-S7 and has no PHP representation yet '
                . '(PROTOCOL.md §3.3, SPEC §22.2) — a loud refusal, never a silent miscast',
                $tag,
                $name,
            ));
        }
        return new ProtocolException(sprintf(
            'value tag %d is not a known canonical TypedValue tag (/proto/types.toml)',
            $tag,
        ));
    }

    // ---- internals -------------------------------------------------------------------------------

    /**
     * A zero year/month/day is a legal MySQL zero date or zero-in-date and is NOT an error — it is
     * simply not a calendar day, so it is reported (`false`) rather than parsed. Anything else must
     * be a real day: `2026-13-01` and `2026-02-30` are malformed wire text.
     *
     * @throws ProtocolException
     */
    private static function checkDateParts(int $y, int $mo, int $d, int $tag, string $what): bool
    {
        if ($mo > 12 || $d > 31) {
            throw self::malformed($tag, $what, 'a month in 0..12 and a day in 0..31');
        }
        if ($y === 0 || $mo === 0 || $d === 0) {
            return false; // a zero date / zero-in-date: carried verbatim, never invented
        }
        if (!checkdate($mo, $d, $y)) {
            throw self::malformed($tag, $what, 'an existing calendar date');
        }
        return true;
    }

    /** @throws ProtocolException */
    private static function checkTimeOfDay(int $h, int $mi, int $s, int $tag, string $what): void
    {
        if ($h > 23 || $mi > 59 || $s > 59) {
            throw self::malformed($tag, $what, 'a time of day in 00:00:00..23:59:59');
        }
    }

    /** Compare two canonical (leading-zero-free) decimal strings. */
    private static function compareDecimals(string $a, string $b): int
    {
        $la = strlen($a);
        $lb = strlen($b);
        return $la === $lb ? strcmp($a, $b) <=> 0 : $la <=> $lb;
    }

    /**
     * The payload arrived in the wrong msgpack family. The message names the tag and the OBSERVED
     * PHP type but NEVER the value — a cell's contents are user data (SPEC §12).
     */
    private static function wrongFamily(int $tag, string $expected, mixed $got): ProtocolException
    {
        return new ProtocolException(sprintf(
            'value tag %d: expected a %s payload, got %s',
            $tag,
            $expected,
            get_debug_type($got),
        ));
    }

    /** The payload is in the right family but is not canonical text for its tag. */
    private static function malformed(int $tag, string $what, string $expected): ProtocolException
    {
        return new ProtocolException(sprintf(
            'value tag %d: malformed %s payload — expected %s (/proto/PROTOCOL.md §3.2)',
            $tag,
            $what,
            $expected,
        ));
    }
}
