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
 * The honest application responses are: report it, reconcile it (look for the row), or fail. There
 * is no fourth option, and that is the point of the branch existing at all.
 */
final class IndeterminateWriteException extends DriverException
{
}
