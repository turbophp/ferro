<?php // /php/doctrine-dbal/src/ExceptionConverter.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Driver\API\MySQL\ExceptionConverter as MySQLExceptionConverter;
use Doctrine\DBAL\Driver\API\PostgreSQL\ExceptionConverter as PostgreSQLExceptionConverter;
use Doctrine\DBAL\Driver\Exception as DriverExceptionInterface;
use Doctrine\DBAL\Exception\DriverException as DbalDriverException;
use Doctrine\DBAL\Query;
use Ferro\DBAL\Exception\DriverException as FerroDriverException;
use Ferro\Protocol\Generated\Constants as C;

/**
 * SPEC §14's "maps the §9.2 tree to DBAL exceptions uniformly across backends, plus
 * `Ferro\DBAL\IndeterminateWriteException` for the third branch".
 *
 * **Three rules, in this order.**
 *
 *  1. **`Indeterminate` wins over everything.** It is checked FIRST, before the family table,
 *     because the SQLSTATE of an indeterminate write is often one the stock table maps to something
 *     specific — a `40001` whose fate is unknown would otherwise surface as a `DeadlockException`,
 *     which carries DBAL's `RetryableException` marker, which invites a framework to replay a write
 *     that may already have landed.
 *  2. **Everything else delegates to the STOCK per-family converter.** PostgreSQL's keys on
 *     SQLSTATE, MySQL's on the vendor errno in `getCode()`, and M1-S8a put both on the wire
 *     precisely so those tables are reachable. Restating them here would be a second source of
 *     truth that rots as DBAL adds codes — and charter rule 6's spirit is that the drop-in tiers
 *     reuse Doctrine's own knowledge rather than re-deriving it.
 *  3. **A `Retryable` the stock table did not recognise is upgraded**, and only then. DBAL marks
 *     just Deadlock and LockWaitTimeout as retryable, while Ferro's Retryable branch also covers a
 *     pool checkout timeout, a connect failure and a lost read — cases where retrying is correct
 *     and where a bare `DriverException` would silently discard that.
 *
 * **A failure with no wire payload has a NULL branch and matches neither test** — which is the
 * whole reason both are written `===` against a branch that may be null. That covers a bad
 * `driverOptions` value and, notably, the client's `TypePolicyException`: a §9.1 policy refusal
 * raised client-side, deliberately outside the Retryable/Indeterminate/NonRetryable branches. It
 * DOES reach this converter (`Ferro\DBAL\Connection` wraps every `FerroException`, and it must —
 * anything that is not a `Doctrine\DBAL\Driver\Exception` escapes DBAL's conversion entirely), and
 * what matters is that it arrives branch-less and so can never be reported as an indeterminate
 * write nor upgraded to retryable. It comes out a plain `DriverException`, which is what
 * `TypePolicyException`'s own docblock asks for: not a driver protocol failure, not a fate signal.
 */
final class ExceptionConverter implements ExceptionConverterInterface
{
    public function __construct(private readonly string $kind) {}

    public function convert(DriverExceptionInterface $exception, ?Query $query): DbalDriverException
    {
        $branch = $exception instanceof FerroDriverException ? $exception->branch() : null;

        if ($branch === C::BRANCH_INDETERMINATE) {
            return new IndeterminateWriteException($exception, $query);
        }

        $stock = $this->kind === PlatformVersion::KIND_MYSQL
            ? new MySQLExceptionConverter()
            : new PostgreSQLExceptionConverter();
        $converted = $stock->convert($exception, $query);

        // `$converted::class === DbalDriverException::class` is deliberate and is NOT the same as an
        // `instanceof` check: every specialised class IS a DriverException, and we only want to
        // upgrade the ones the stock table left GENERIC.
        if ($branch === C::BRANCH_RETRYABLE && $converted::class === DbalDriverException::class) {
            return new RetryableDriverException($exception, $query);
        }

        return $converted;
    }
}
