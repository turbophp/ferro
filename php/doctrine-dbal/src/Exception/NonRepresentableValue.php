<?php // /php/doctrine-dbal/src/Exception/NonRepresentableValue.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;

/**
 * A value that is perfectly legal in the database and on Ferro's wire, but has no representation
 * Doctrine's type layer can parse without CORRUPTING it.
 *
 * Refusing is the whole point. Measured against doctrine/dbal 4.4.4, its stock converters turn
 * `2026-00-05` into `2025-12-05`, `0000-00-00 00:00:00` into `-0001-11-30` and `24:00:00` into
 * `00:00:00` — with no exception. A loud refusal makes a readable-in-the-native-API column
 * unreadable through DBAL; a silent conversion makes it WRONG, which is worse and is the class of
 * defect this project exists to refuse.
 *
 * It is a `Doctrine\DBAL\Driver\Exception`, so `Doctrine\DBAL\Result::fetchAssociative()` converts
 * it like any other driver error rather than letting it escape unconverted.
 */
final class NonRepresentableValue extends AbstractException
{
    public static function forTag(string $what, string $value, string $why): self
    {
        return new self(sprintf(
            'Ferro: the %s value %s cannot be handed to Doctrine\'s type layer — %s. It is a valid '
            . 'value and Ferro can read it: query the column through a Ferro\\Client\\Connection of '
            . 'its own (this driver\'s getNativeConnection() hands back the connection the driver '
            . 'built, which carries THIS policy and refuses identically), or cast the column in SQL '
            . '(e.g. `col::text`) if you only need to display it. It is refused rather than '
            . 'converted because Doctrine\'s stock converters would accept it SILENTLY and produce '
            . 'a different value.',
            $what,
            var_export($value, true),
            $why,
        ));
    }
}
