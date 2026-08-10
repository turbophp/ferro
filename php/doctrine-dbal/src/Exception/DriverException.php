<?php // /php/doctrine-dbal/src/Exception/DriverException.php
declare(strict_types=1);
namespace Ferro\DBAL\Exception;

use Doctrine\DBAL\Driver\AbstractException;
use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;

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

    /** The §9.2 branch byte (1 Retryable, 2 Indeterminate, 3 NonRetryable), or null. */
    public function branch(): ?int
    {
        return $this->branch;
    }
}
