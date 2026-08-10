<?php // /php/doctrine-dbal/src/Exception/NoIdentityValue.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;
use Ferro\DBAL\PlatformVersion;

/**
 * `Doctrine\DBAL\Driver\Connection::lastInsertId(): int|string` is non-nullable and DBAL 4 requires
 * it to THROW when there is no identity value (UPGRADE.md: "Connection::lastInsertId() throws an
 * exception when there's no identity value").
 *
 * On PostgreSQL that is ALWAYS: the protocol carries no such field, and Ferro refuses to emulate it
 * with a follow-up `lastval()` because on a transaction-mode pool that lands on a DIFFERENT
 * connection and returns a silently wrong key. The message names the two working answers.
 */
final class NoIdentityValue extends AbstractException
{
    public static function forKind(string $kind): self
    {
        return new self(
            $kind === PlatformVersion::KIND_POSTGRES
                ? 'Ferro: PostgreSQL reports no generated key on the wire, and Ferro will not '
                    . 'emulate lastInsertId() with a follow-up query — on a transaction-mode pool '
                    . 'that runs on a different connection and returns a wrong key. Use '
                    . '`INSERT … RETURNING id`, or configure Doctrine ORM to use the SEQUENCE '
                    . 'identity strategy on PostgreSQL.'
                : 'Ferro: the last statement reported no generated key. lastInsertId() reflects the '
                    . 'MOST RECENT statement and is cleared by a statement that fails, so read it '
                    . 'immediately after a successful INSERT into an AUTO_INCREMENT column.',
        );
    }
}
