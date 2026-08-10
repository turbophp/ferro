<?php // /php/doctrine-dbal/src/RetryableDriverException.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Exception\RetryableException;

/**
 * A §9.2 `Retryable` — the statement provably did NOT apply — that Doctrine's stock table does not
 * recognise.
 *
 * DBAL carries its `RetryableException` marker on exactly TWO classes, `DeadlockException` and
 * `LockWaitTimeoutException`, so the stock tables cover only the vendor codes for those. Ferro's
 * Retryable branch is broader by design: a pool checkout that timed out, a connect failure, a lost
 * READ. Those are the cases where retrying is not merely safe but correct, and letting them fall
 * out as a bare `DriverException` would discard the one piece of information §9.2 exists to
 * provide.
 *
 * Used ONLY when the stock converter produced a bare `DriverException`. When it produced a
 * specific class, that class wins — it is more informative, and Deadlock/LockWaitTimeout already
 * carry the marker.
 */
final class RetryableDriverException extends DriverException implements RetryableException
{
}
