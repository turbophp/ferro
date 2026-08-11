<?php // /php/doctrine-dbal/src/IndeterminateWriteException.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\Exception\DriverException;

/**
 * SPEC §9.2's THIRD BRANCH, which Doctrine's exception tree does not have: the write was
 * TRANSMITTED and its fate is UNKNOWN. It may have been applied; it may not.
 *
 * **It deliberately implements NOTHING beyond `DriverException`.** In particular it must never
 * implement `Doctrine\DBAL\Exception\RetryableException`: that is a bare marker interface which
 * Symfony Messenger, ORM retry helpers and every hand-rolled `catch (RetryableException)` loop key
 * on, and replaying an indeterminate write is precisely the at-most-once violation charter rule 3
 * exists to prevent. The engine never transparently retries; neither does this driver; and nothing
 * this driver produces may invite a third party to.
 *
 * Extending `DriverException` (rather than inventing a parallel root) keeps it catchable as
 * `Doctrine\DBAL\Exception`, so an application that catches broadly still sees it — it just cannot
 * mistake it for something safe to repeat. Nothing else in Doctrine's tree extends
 * `DriverException` and carries the retryable marker, so this parent adds no such relation:
 * `ExceptionConverterTest::testNoAncestorOfTheIndeterminateExceptionIsMarkedRetryable` asserts that
 * against the retryable set DERIVED from the installed DBAL, so a future release that changes it
 * goes red here rather than silently.
 *
 * **THE PARENT THAT WAS CONSIDERED AND REJECTED, and why this class did NOT move.**
 * `Doctrine\DBAL\Connection::transactional()` exempts five classes from its post-commit rollback,
 * and an exception on none of them is REPLACED by `NoActiveTransaction` before it reaches the
 * caller (see {@see \Ferro\DBAL\Wrapper\IndeterminateSafeTransactional} for the measured
 * mechanism). Re-parenting onto that list would have fixed the masking with no wrapper at all.
 * Measured against the installed doctrine/dbal, none of the five is available:
 *
 * | exempt class                            | why not                                            |
 * |-----------------------------------------|----------------------------------------------------|
 * | `ConnectionLost`                        | declared **`final`** — cannot be extended. It is    |
 * |                                         | also the only semantically honest one, and the one  |
 * |                                         | `pdo_pgsql` produces for this same event.           |
 * | `DeadlockException`                     | carries `RetryableException`. Forbidden: it would   |
 * |                                         | turn the at-most-once guarantee into at-least-once. |
 * | `TransactionRolledBack`                 | asserts the write did NOT apply — the exact claim   |
 * |                                         | that makes a retry look safe.                       |
 * | `UniqueConstraintViolationException`    | asserts a duplicate exists; applications swallow it.|
 * | `ForeignKeyConstraintViolationException`| same shape of false claim.                          |
 *
 * An indeterminate write is the ABSENCE of a claim about the fate, so the three extendable classes
 * are all lies and the one truthful class is sealed. The ancestry therefore stays exactly as it is,
 * and the masking is repaired in the wrapper instead.
 * `ExceptionAncestryTest` re-derives this whole table from the installed DBAL — including the exact
 * transitive ancestry of this class and which `catch` blocks fire on it — so none of it can rot.
 *
 * The honest application responses are: report it, reconcile it (look for the row), or fail. There
 * is no fourth option, and that is the point of the branch existing at all.
 */
final class IndeterminateWriteException extends DriverException
{
}
