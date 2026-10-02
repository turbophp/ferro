<?php // /php/doctrine-dbal/src/CanonicalBinder.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Ferro\Bytes;
use Ferro\DBAL\Exception\DriverException;

/**
 * SPEC §14's "`bindValue()` with DBAL `ParameterType` → canonical mapping" — the half that is the
 * same on both DBAL majors. Each major's binder ({@see ParameterBinder} for DBAL 4,
 * {@see \Ferro\DBAL\Dbal3\ParameterBinder} for DBAL 3) only turns ITS `ParameterType` into a
 * {@see BindKind}; DBAL 4's is an enum and DBAL 3's is a set of int constants, which is the whole
 * difference (M2-C5).
 *
 * **It keys on the PAIR, not on the `ParameterType` alone and not on the PHP type alone**, because
 * DBAL's own type layer produces mismatched pairs on purpose. Measured against 4.4.4:
 * `BooleanType::convertToDatabaseValue(true)` returns `int(1)` tagged `BOOLEAN`; `FloatType`,
 * `DecimalType` and `BigIntType` all tag `STRING` while carrying a float or a numeric string;
 * `BlobType` tags `LARGE_OBJECT` and carries a raw string (or, from a `bindValue` a user wrote
 * themselves, a stream resource). A binder keyed on the PHP type would send that `int(1)` as
 * `TAG_I64`, and PostgreSQL's narrow per-tag bind pre-flight refuses an integer against a `boolean`
 * column — a hard, pre-send `NonRetryable` on every boolean insert.
 *
 * `BINARY` / `LARGE_OBJECT` become {@see Bytes}, which is the ONLY way to reach `TAG_BYTES` from
 * PHP: every bare PHP string binds `TAG_TEXT`, whose msgpack `str` payload the engine's reader
 * validates as UTF-8 — so a binary blob sent as a string fails as a generic "malformed
 * ExecRequest", not as a diagnosable bind error.
 */
final class CanonicalBinder
{
    /** A `null` is a null under every type; every other value is shaped by its {@see BindKind}. */
    public static function bind(mixed $value, BindKind $kind): mixed
    {
        if ($value === null) {
            return null;
        }
        return match ($kind) {
            BindKind::Null => null,
            BindKind::Boolean => self::asBool($value),
            BindKind::Integer => self::asInt($value),
            BindKind::Binary => new Bytes(self::asBinary($value)),
            BindKind::Natural => self::natural($value),
        };
    }

    private static function asBool(mixed $v): bool
    {
        if (is_bool($v)) {
            return $v;
        }
        if (is_int($v)) {
            return $v !== 0;
        }
        if (is_string($v) && ($v === '0' || $v === '1')) {
            return $v === '1';
        }
        throw DriverException::local(sprintf(
            'Ferro: cannot bind %s as ParameterType::BOOLEAN.',
            get_debug_type($v),
        ));
    }

    private static function asInt(mixed $v): int
    {
        if (is_int($v)) {
            return $v;
        }
        if (is_string($v) && preg_match('/^-?\d+$/', $v) === 1) {
            // A `bigint` above PHP_INT_MAX would silently wrap here, which is exactly the class of
            // corruption this project refuses. Let it through only when it round-trips.
            $n = (int) $v;
            if ((string) $n === $v) {
                return $n;
            }
            throw DriverException::local(sprintf(
                'Ferro: integer parameter %s does not fit a PHP int; bind it as a string so it '
                . 'travels as canonical text.',
                $v,
            ));
        }
        throw DriverException::local(sprintf(
            'Ferro: cannot bind %s as ParameterType::INTEGER.',
            get_debug_type($v),
        ));
    }

    private static function asBinary(mixed $v): string
    {
        if (is_string($v)) {
            return $v;
        }
        if (is_resource($v)) {
            $s = stream_get_contents($v);
            if ($s === false) {
                throw DriverException::local('Ferro: could not read the stream bound as a binary parameter.');
            }
            return $s;
        }
        throw DriverException::local(sprintf(
            'Ferro: cannot bind %s as binary; expected a string or a stream resource.',
            get_debug_type($v),
        ));
    }

    /**
     * Under `STRING`/`ASCII` the PHP type decides, because DBAL routes floats, ints and even bools
     * through `STRING`. A stream is materialised rather than stringified into "Resource id #7".
     */
    private static function natural(mixed $v): mixed
    {
        if (is_bool($v) || is_int($v) || is_float($v) || is_string($v)) {
            return $v;
        }
        if (is_resource($v)) {
            return new Bytes(self::asBinary($v));
        }
        if ($v instanceof \Stringable) {
            return (string) $v;
        }
        throw DriverException::local(sprintf(
            'Ferro: cannot bind a value of type %s. Doctrine\'s type layer converts values before '
            . 'they reach the driver, so this usually means a custom Type returned an object from '
            . 'convertToDatabaseValue().',
            get_debug_type($v),
        ));
    }
}
