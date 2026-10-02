<?php // /php/doctrine-dbal/src/ParameterBinder.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\ParameterType;

/**
 * DBAL **4**'s half of the bind mapping: its `ParameterType` ENUM → a {@see BindKind}. What each
 * kind DOES to a value — and why the mapping keys on the PAIR `(ParameterType, PHP type)` rather
 * than either alone — is {@see CanonicalBinder}'s, shared with DBAL 3.
 *
 * The `match` has NO `default` arm. That is the closest thing PHP offers to a compile-forced guard:
 * an eighth `ParameterType` case in a future DBAL release throws `\UnhandledMatchError` here instead
 * of being silently funnelled into the string path. Precisely: a `null` short-circuits AHEAD of the
 * match and would still return `null` under such a case — deliberately, because a null is a null
 * under every type (pinned by `testNullSurvivesEveryParameterType`); every non-null value hits the
 * match.
 */
final class ParameterBinder
{
    public static function toCanonical(mixed $value, ParameterType $type): mixed
    {
        if ($value === null) {
            return null;
        }
        return CanonicalBinder::bind($value, self::kindOf($type));
    }

    public static function kindOf(ParameterType $type): BindKind
    {
        return match ($type) {
            ParameterType::NULL => BindKind::Null,
            ParameterType::BOOLEAN => BindKind::Boolean,
            ParameterType::INTEGER => BindKind::Integer,
            ParameterType::BINARY, ParameterType::LARGE_OBJECT => BindKind::Binary,
            ParameterType::STRING, ParameterType::ASCII => BindKind::Natural,
        };
    }
}
