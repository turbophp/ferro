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

    /**
     * A statement issued while `Doctrine\DBAL\Connection`'s nesting counter is desynced from this
     * connection — i.e. after a REJECTED `beginTransaction()`, which DBAL counts as open anyway
     * (`++$this->transactionNestingLevel` happens BEFORE the driver call and is never undone).
     *
     * Refused because both ways of running it are wrong, and both were MEASURED: an ordinary
     * statement would execute in AUTOCOMMIT while the caller believes a transaction is open (durable
     * writes no rollback can undo, on a connection whose `isTransactionActive()` says true), and
     * Doctrine's next `beginTransaction()` — which at nesting 2 is `SAVEPOINT DOCTRINE_2` — would be
     * refused by the ENGINE with a message naming a savepoint the application never wrote.
     *
     * The message names the cause and both exits: `rollBack()`, which resynchronises here and costs
     * nothing (nothing ran), or the `wrapperClass` that makes the whole situation transient.
     */
    public static function afterARejectedBegin(string $sql): self
    {
        return new self(sprintf(
            'Ferro refuses this statement: %s. A beginTransaction() on this connection was REJECTED '
            . '(a pool checkout timeout, an unavailable replica, a BEGIN that never reached the '
            . 'backend) — but Doctrine increments its transaction nesting counter BEFORE calling the '
            . 'driver and does not undo it, so Doctrine believes a transaction is open while this '
            . 'connection has none. Running this statement would either commit in AUTOCOMMIT behind '
            . 'a transaction the caller thinks it is inside, or emit a SAVEPOINT with no transaction '
            . 'to hold it. Call rollBack() to resynchronise (nothing has run, so nothing is undone), '
            . 'or configure \'wrapperClass\' => %s::class, which resynchronises automatically and '
            . 'makes the rejected BEGIN retryable as the engine intended.',
            $sql,
            FerroConnection::class,
        ));
    }

    /**
     * `executeStatement()` — DBAL's WRITE entry point — on a connection configured
     * `driverOptions.readonly`, outside a transaction.
     *
     * Refused rather than run, because the alternative is what the whole-branch review measured on
     * PG 17: the INSERT returned `affected = 1` and the row was there, on a connection every §19.3
     * fate was being computed against as "this only reads". Inside a transaction the server itself
     * refuses (`BEGIN READ ONLY` → `25006`), and this makes the two halves agree as far as the SPI
     * allows — see `Connection::refuseAutocommitWriteEntryPoint()` for what it deliberately cannot
     * cover and what protects that instead.
     */
    public static function readonlyAutocommitWrite(string $sql): self
    {
        return new self(sprintf(
            'Ferro refuses this statement: %s. This connection is configured '
            . '`driverOptions.readonly => true`, and executeStatement() is Doctrine\'s WRITE entry '
            . 'point. Outside a transaction nothing on the server can enforce the declaration (a '
            . 'transaction gets BEGIN READ ONLY; an autocommit statement gets nothing), so running '
            . 'it would let a write commit on a connection whose every §19.3 fate is classified as a '
            . 'read — which is how a lost write becomes "retryable" and gets replayed. Use a '
            . 'connection without `readonly`, or run the write inside a transaction on one (where '
            . 'the server will refuse it out loud).',
            $sql,
        ));
    }
}
