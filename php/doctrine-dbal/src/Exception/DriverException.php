<?php // /php/doctrine-dbal/src/Exception/DriverException.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\HandshakeException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;
use Ferro\Protocol\Generated\Constants as C;

/**
 * EVERY Ferro client exception crossing the driver boundary becomes one of these. That is not
 * tidiness: `Doctrine\DBAL\Connection::executeQuery()` catches exactly `Doctrine\DBAL\Driver\Exception`,
 * so anything else escapes DBAL's conversion entirely and reaches the application raw, past every
 * `catch (Doctrine\DBAL\Exception)` an app or framework has.
 *
 * It carries the pair the STOCK converters read: the 5-character SQLSTATE in `getSQLState()` (which
 * `API\PostgreSQL\ExceptionConverter` keys on) and the integer vendor errno in `getCode()` (which
 * `API\MySQL\ExceptionConverter` keys on). PostgreSQL never supplies an errno — its identity IS the
 * SQLSTATE — so `getCode()` is 0 there, which is exactly what the PG table expects.
 *
 * `branch()` preserves the §9.2 fate the wire declared, because DBAL's tree has no third branch and
 * {@see \Ferro\DBAL\ExceptionConverter} (Task 11) needs it to mint one.
 */
final class DriverException extends AbstractException
{
    private function __construct(
        string $message,
        ?string $sqlState,
        int $code,
        private readonly ?int $branch,
        ?\Throwable $previous,
    ) {
        parent::__construct($message, $sqlState, $code, $previous);
    }

    public static function fromFerro(FerroException $e): self
    {
        $sqlstate = null;
        $errno = null;
        $branch = null;
        if ($e instanceof RetryableException
            || $e instanceof IndeterminateException
            || $e instanceof NonRetryableException
        ) {
            $sqlstate = $e->sqlstate();
            $errno = $e->errno();
            $branch = $e->branch();
        }
        return new self($e->getMessage(), $sqlstate, $errno ?? 0, $branch, $e);
    }

    /** A driver-side failure with no wire payload (a bad option, an unreadable value). */
    public static function local(string $message, ?\Throwable $previous = null): self
    {
        return new self($message, null, 0, null, $previous);
    }

    /**
     * A failure of {@see \Ferro\DBAL\Driver::connect} itself — the transport, or the handshake.
     *
     * **It exists because the connect path had NO conversion at all**, which broke this class's own
     * rule: `Ferro::connect()`'s `TransportException` (ferrod down, socket missing, TCP refused) and
     * its `HandshakeException` (registry or version mismatch) sailed straight past
     * `Doctrine\DBAL\Connection::connect()`'s `catch (Driver\Exception)`, so a `ferrod` restart —
     * an EXPECTED operational event (SPEC §18 socket activation, §19.1 boot_epoch storms) — reached
     * every application, framework bundle, health check and migrations run as a class none of them
     * has ever heard of. A drop-in replacement whose failure mode is uncatchable is not drop-in
     * during exactly the window that matters.
     *
     * **The §9.2 fate is carried, and it is not the same for both causes.**
     *
     *  * A **transport** failure at connect is a KNOWN did-not-apply: no user statement can have been
     *    sent on a session that never opened, so it is `Retryable` — and the converter's rule 3 then
     *    mints {@see \Ferro\DBAL\RetryableDriverException}, which is what lets a framework's own
     *    backoff loop ride out a restart instead of failing the request outright. This is not an
     *    engine retry (charter rule 3): nothing here re-issues anything.
     *  * A **handshake rejection** is FATAL and deliberately NOT retryable — `HandshakeException`'s
     *    own docblock says so: the dominant cause is a `type_registry_hash`/version mismatch, and
     *    reconnecting simply re-enters the same rejection. It arrives branch-less, i.e. a plain
     *    `Doctrine\DBAL\Exception\DriverException`, so no retry helper picks it up.
     *
     * A client exception that already carries a wire payload (the three taxonomy classes) is passed
     * to {@see fromFerro} untouched — the engine's own classification always wins over this one.
     */
    public static function connectFailed(FerroException $e, string $context): self
    {
        if ($e instanceof RetryableException
            || $e instanceof IndeterminateException
            || $e instanceof NonRetryableException
        ) {
            return self::fromFerro($e);
        }
        $branch = $e instanceof HandshakeException ? null : C::BRANCH_RETRYABLE;
        return new self($context . ': ' . $e->getMessage(), null, 0, $branch, $e);
    }

    /**
     * A LOST autocommit statement on a connection that declared `driverOptions.readonly`, re-minted
     * from `Retryable` to §19.3 `Indeterminate` — see
     * {@see \Ferro\DBAL\Connection::statementException} for the cell and for why this is the only
     * place the driver overrides an engine verdict.
     *
     * In one line: the engine's `Retryable` here was computed FROM the readonly declaration, the
     * driver cannot verify that declaration for an autocommit statement, and "safe to retry" is
     * exactly the claim that turns a possibly-applied write into a replayed one. The original
     * message and the client exception are both preserved; only the branch changes, upward.
     */
    public static function unbackedReadonlyLoss(RetryableException $e): self
    {
        return new self(
            $e->getMessage()
            . ' — reported as INDETERMINATE by the Ferro DBAL driver: the engine classified this '
            . 'retryable only because driverOptions.readonly declares every statement on this '
            . 'connection a read, and nothing enforces that declaration for an autocommit statement '
            . '(SPEC §19.3). If it wrote, it may or may not have applied; do not replay it.',
            $e->sqlstate(),
            $e->errno() ?? 0,
            C::BRANCH_INDETERMINATE,
            $e,
        );
    }

    /** The §9.2 branch byte (1 Retryable, 2 Indeterminate, 3 NonRetryable), or null. */
    public function branch(): ?int
    {
        return $this->branch;
    }
}
