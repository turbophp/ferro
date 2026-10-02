<?php // /php/doctrine-dbal/src/Dbal3/ParameterBinder.php
declare(strict_types=1);
namespace Ferro\DBAL\Dbal3;

use Doctrine\DBAL\ParameterType;
use Ferro\DBAL\BindKind;
use Ferro\DBAL\CanonicalBinder;
use Ferro\DBAL\Exception\DriverException;

/**
 * DBAL **3**'s half of the bind mapping: its `ParameterType` INT constants → a {@see BindKind}. What
 * each kind does to a value is {@see CanonicalBinder}'s, shared with DBAL 4.
 *
 * DBAL 3's `ParameterType` is a set of int constants rather than an enum, so the match below cannot
 * be exhaustive by construction the way DBAL 4's is; the `default` arm REFUSES instead, which keeps
 * the same property — an unknown type is never funnelled silently into the string path. A `null`
 * value short-circuits ahead of it, as on DBAL 4.
 */
final class ParameterBinder
{
    public static function toCanonical(mixed $value, mixed $type): mixed
    {
        if ($value === null) {
            return null;
        }
        return CanonicalBinder::bind($value, self::kindOf($type));
    }

    public static function kindOf(mixed $type): BindKind
    {
        return match ($type) {
            ParameterType::NULL => BindKind::Null,
            ParameterType::BOOLEAN => BindKind::Boolean,
            ParameterType::INTEGER => BindKind::Integer,
            ParameterType::BINARY, ParameterType::LARGE_OBJECT => BindKind::Binary,
            ParameterType::STRING, ParameterType::ASCII => BindKind::Natural,
            default => throw DriverException::local(sprintf(
                'Ferro: unknown DBAL 3 ParameterType %s.',
                is_int($type) ? (string) $type : get_debug_type($type),
            )),
        };
    }
}
