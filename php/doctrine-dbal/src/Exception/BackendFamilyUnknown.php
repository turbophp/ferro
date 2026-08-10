<?php // /php/doctrine-dbal/src/Exception/BackendFamilyUnknown.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;

/**
 * The driver could not decide WHICH SQL dialect to emit. SPEC §14 is explicit that this must fail
 * loudly rather than fall back to a default platform: a wrong platform is a wrong SQL grammar for
 * every subsequent statement, which is a class of bug a clean error is not.
 */
final class BackendFamilyUnknown extends AbstractException
{
    public static function forKind(string $kind): self
    {
        return new self(sprintf(
            'Ferro: the pool advertises backend family "%s", for which this driver has no Doctrine '
            . 'platform. M1 supports "postgres" and "mysql" (MariaDB reports "mysql" and is '
            . 'distinguished by its version string). No default platform is guessed, because a '
            . 'wrong platform means a wrong SQL dialect for every statement.',
            $kind,
        ));
    }

    public static function beforeConnect(string $version): self
    {
        return new self(sprintf(
            'Ferro: the Doctrine platform was requested before any connection was opened, and the '
            . 'configured serverVersion "%s" does not name a backend family. Either remove the '
            . '`serverVersion` connection parameter so the driver learns the family from the engine '
            . 'handshake, or write a family-bearing version string (e.g. "PostgreSQL 17.10" or '
            . '"11.8.8-MariaDB"). No family is guessed: PostgreSQL and MySQL are different SQL '
            . 'dialects.',
            $version,
        ));
    }
}
