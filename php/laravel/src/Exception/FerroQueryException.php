<?php // /php/laravel/src/Exception/FerroQueryException.php
declare(strict_types=1);
namespace Ferro\Laravel\Exception;

use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;

/**
 * Every Ferro client exception crossing into Illuminate becomes one of these.
 *
 * **It extends `PDOException` and carries the SQLSTATE in `getCode()`, and BOTH halves are
 * load-bearing for `DB::transaction($fn, attempts: N)`.** Verified against the installed
 * `illuminate/database` v11.51.0:
 *
 * - `Illuminate\Database\QueryException extends PDOException`, and its constructor does
 *   `$this->code = $previous->getCode()` — so whatever code THIS exception carries becomes the
 *   `QueryException`'s code.
 * - `DetectsConcurrencyErrors::causedByConcurrencyError()` returns true for
 *   `$e instanceof PDOException && ($e->getCode() === 40001 || $e->getCode() === '40001')`, or for
 *   a message containing one of a fixed substring list.
 *
 * **This is the OPPOSITE convention from the sibling Doctrine tier, deliberately.**
 * `Ferro\DBAL\Exception\DriverException` puts the vendor ERRNO in `getCode()` and the SQLSTATE in
 * `getSQLState()`, because that is what DBAL's converters read. PDO's convention — and therefore
 * Laravel's — is that `getCode()` IS the SQLSTATE. Copying the sibling here would compile, pass
 * every offline test, and **silently disable retry for PostgreSQL serialization failures**: their
 * SQLSTATE is `40001` but their message, `could not serialize access due to concurrent update`,
 * matches none of the substrings, so criterion 2 cannot save it. Deadlocks would keep retrying via
 * message matching and hide the hole. `TransactionRetryLiveTest` is the guard that would go red.
 *
 * The `code` is assigned as a PROPERTY rather than through the constructor because `Exception`'s
 * constructor types `$code` as `int` while SQLSTATEs are five-character strings that are not always
 * numeric (`42P01`). PDO itself does exactly this.
 */
final class FerroQueryException extends \PDOException
{
    private function __construct(string $message, string|int $code, ?\Throwable $previous)
    {
        parent::__construct($message, 0, $previous);
        $this->code = $code;
    }

    /**
     * Wrap a client exception, preserving the SQLSTATE as the code.
     *
     * A payload-less failure (a transport death, a handshake refusal) has no SQLSTATE; it keeps
     * code `0` and is matched — if at all — by Illuminate's own message-based lost-connection
     * detection, which is the same thing a PDO driver would give it.
     */
    public static function fromFerro(FerroException $e): self
    {
        $sqlstate = null;
        if ($e instanceof RetryableException
            || $e instanceof IndeterminateException
            || $e instanceof NonRetryableException
        ) {
            $sqlstate = $e->sqlstate();
        }
        return new self($e->getMessage(), $sqlstate ?? 0, $e);
    }
}
