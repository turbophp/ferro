<?php // /php/doctrine-dbal/src/Exception/UnsupportedStatement.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;
use Ferro\DBAL\Wrapper\FerroConnection;

/**
 * A statement Ferro refuses to run, because running it would SUCCEED and do nothing.
 *
 * It extends `Doctrine\DBAL\Driver\AbstractException` (and so implements
 * `Doctrine\DBAL\Driver\Exception`) for the same reason every other exception this driver raises
 * does: `Doctrine\DBAL\Connection::executeStatement()` catches exactly `Driver\Exception`, and
 * anything else escapes DBAL's conversion entirely and reaches the application past every
 * `catch (Doctrine\DBAL\Exception)` a framework has. It carries NO SQLSTATE and NO errno — nothing
 * ever reached a backend — so {@see \Ferro\DBAL\ExceptionConverter} sees a null §9.2 branch and it
 * comes out a plain `DriverException`: never `Indeterminate`, never `Retryable`. That is correct
 * and load-bearing: the statement provably did not execute, and it will fail identically on a
 * retry.
 */
final class UnsupportedStatement extends AbstractException
{
    public static function isolation(string $sql): self
    {
        return new self(sprintf(
            'Ferro refuses this statement: %s. On a transaction-mode pool a session-level isolation '
            . 'setting is meaningless — it lands on whichever pooled connection the checkout hands '
            . 'out, taints it, and is wiped by connection hygiene before the next BEGIN, so the '
            . 'statement would report success and have no effect on any later transaction. Ferro '
            . 'carries isolation per-TRANSACTION instead: add '
            . '\'wrapperClass\' => %s::class to this connection\'s configuration and '
            . 'Doctrine\'s setTransactionIsolation() will be honoured on the next '
            . 'beginTransaction(). Refused rather than ignored because a silently wrong isolation '
            . 'level is the failure this engine exists to prevent.',
            $sql,
            FerroConnection::class,
        ));
    }
}
