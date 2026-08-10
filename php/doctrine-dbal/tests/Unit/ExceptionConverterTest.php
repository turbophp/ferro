<?php // /php/doctrine-dbal/tests/Unit/ExceptionConverterTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\DriverException as DbalDriverException;
use Doctrine\DBAL\Exception\RetryableException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException as FerroRetryable;
use Ferro\DBAL\Exception\DriverException as FerroDriverException;
use Ferro\DBAL\ExceptionConverter;
use Ferro\DBAL\IndeterminateWriteException;
use Ferro\DBAL\PlatformVersion;
use Ferro\DBAL\RetryableDriverException;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 11 — the converter is a SAFETY surface.
 *
 * §9.2's third branch (Indeterminate — "the write was transmitted and its fate is UNKNOWN") has no
 * DBAL equivalent. Flattening it into a generic `DriverException` would be survivable; flattening
 * it into anything a retry loop treats as retryable would replay a write that may already have
 * landed. `Doctrine\DBAL\Exception\RetryableException` is a bare marker interface that Symfony
 * Messenger, `doctrine/orm`'s retry helpers and hand-rolled loops all key on — so the FIRST
 * assertion below is `assertNotInstanceOf`, and it is the most important line in this file.
 *
 * The rest delegates to the STOCK per-family converters. Reproducing their tables here would be a
 * second source of truth that silently rots as DBAL adds vendor codes.
 */
final class ExceptionConverterTest extends TestCase
{
    /**
     * The client's taxonomy exceptions take the decoded `ErrorPayload` and NOTHING else — the
     * message is built from it by `CarriesErrorPayload::__construct`. `IndeterminateException`
     * additionally takes a client-side `cause` label with a default.
     *
     * The `match` has NO default arm and its arms are the generated `/proto` branch constants, so
     * this helper cannot silently mint the wrong taxonomy class for a branch byte (PHP raises
     * `\UnhandledMatchError`) — the nearest thing to a compile-forced mapping PHP offers.
     */
    private function ferro(string $sqlstate, ?int $errno, int $branch): FerroDriverException
    {
        $payload = new ErrorPayload(1, $branch, $sqlstate, $errno, 'boom', null, null);
        $e = match ($branch) {
            C::BRANCH_INDETERMINATE => new IndeterminateException($payload),
            C::BRANCH_RETRYABLE => new FerroRetryable($payload),
            C::BRANCH_NON_RETRYABLE => new NonRetryableException($payload),
        };
        return FerroDriverException::fromFerro($e);
    }

    /** THE safety assertion. */
    public function testAnIndeterminateWriteIsNeverRetryable(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_POSTGRES);
        $out = $c->convert($this->ferro('08006', null, C::BRANCH_INDETERMINATE), null);

        self::assertInstanceOf(IndeterminateWriteException::class, $out);
        self::assertNotInstanceOf(
            RetryableException::class,
            $out,
            'an Indeterminate write must NEVER be marked retryable — a framework retry loop would '
            . 'replay a write that may already have landed (charter rule 3)',
        );
        self::assertInstanceOf(\Doctrine\DBAL\Exception::class, $out, 'still catchable as a DBAL error');
    }

    /**
     * THE SAME PROPERTY, ASSERTED FROM THE ANCESTRY rather than from one converted instance.
     *
     * `instanceof` already walks the whole ancestry, so the test above is complete for the input it
     * was handed — but it is one input. This one asks the question the reviewer actually has: is
     * there ANY class or interface in Doctrine's tree through which an `IndeterminateWriteException`
     * could be caught as retryable? The retryable set is DERIVED from the installed DBAL (every
     * class under `Exception/` that implements the marker) rather than hand-listed, so a DBAL
     * upgrade that marks a new class retryable is covered the day it lands, and the derivation is
     * asserted non-empty first so an empty scan cannot pass for a proof.
     *
     * The MIRROR — `RetryableDriverException` IS in that relationship — is what stops this from
     * being a test that passes because `is_a()` was never reached.
     */
    public function testNoAncestorOfTheIndeterminateExceptionIsMarkedRetryable(): void
    {
        $retryable = self::dbalRetryableClasses();
        self::assertNotEmpty($retryable, 'the derivation itself is broken — nothing was scanned');
        self::assertContains(DeadlockException::class, $retryable, 'the scan must see the known case');

        foreach ($retryable as $class) {
            self::assertFalse(
                is_a(IndeterminateWriteException::class, $class, true),
                "IndeterminateWriteException must not stand in any relation to $class",
            );
        }
        self::assertNotContains(
            RetryableException::class,
            class_implements(IndeterminateWriteException::class),
            'the marker interface must be absent from the whole interface closure',
        );
        // The mirror: the class that IS meant to carry the marker does.
        self::assertContains(RetryableException::class, class_implements(RetryableDriverException::class));
    }

    /**
     * The Indeterminate interception happens BEFORE the family table, so a SQLSTATE that the stock
     * PG converter maps to something specific still comes out as an indeterminate write. Without
     * this ordering, a `40001` whose branch was Indeterminate would surface as a `DeadlockException`
     * — which IS retryable.
     */
    public function testTheIndeterminateBranchWinsOverTheFamilyTable(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_POSTGRES);
        $out = $c->convert($this->ferro('40001', null, C::BRANCH_INDETERMINATE), null);
        self::assertInstanceOf(IndeterminateWriteException::class, $out);
        self::assertNotInstanceOf(DeadlockException::class, $out);
    }

    /** PostgreSQL keys on SQLSTATE; the stock table does the work. */
    public function testPostgresDelegatesToTheStockSqlstateTable(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_POSTGRES);
        self::assertInstanceOf(
            UniqueConstraintViolationException::class,
            $c->convert($this->ferro('23505', null, C::BRANCH_NON_RETRYABLE), null),
        );
        self::assertInstanceOf(
            DeadlockException::class,
            $c->convert($this->ferro('40P01', null, C::BRANCH_RETRYABLE), null),
        );
    }

    /** MySQL keys on the vendor errno in `getCode()` — the S8a errno-on-wire carry, consumed. */
    public function testMysqlDelegatesToTheStockErrnoTable(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_MYSQL);
        self::assertInstanceOf(
            UniqueConstraintViolationException::class,
            $c->convert($this->ferro('23000', 1062, C::BRANCH_NON_RETRYABLE), null),
        );
        self::assertInstanceOf(
            DeadlockException::class,
            $c->convert($this->ferro('40001', 1213, C::BRANCH_RETRYABLE), null),
        );
    }

    /**
     * THE FAMILY CHOICE IS LOAD-BEARING, IN BOTH DIRECTIONS.
     *
     * Half of the two delegation tests above cannot see the family at all: `40001`/`1213` is a
     * `DeadlockException` under EITHER table (PG keys the SQLSTATE, MySQL the errno), so a
     * converter hard-wired to one family passes it. This pins the discrimination with one payload
     * that only the PG table recognises — `42P01` with no errno — asserted BOTH ways round, so
     * neither "always PostgreSQL" nor "always MySQL" survives.
     */
    public function testTheFamilyChoiceIsLoadBearingInBothDirections(): void
    {
        $pgOnly = fn (): FerroDriverException => $this->ferro('42P01', null, C::BRANCH_NON_RETRYABLE);

        self::assertInstanceOf(
            TableNotFoundException::class,
            (new ExceptionConverter(PlatformVersion::KIND_POSTGRES))->convert($pgOnly(), null),
            'the PostgreSQL table reads SQLSTATE',
        );
        self::assertNotInstanceOf(
            TableNotFoundException::class,
            (new ExceptionConverter(PlatformVersion::KIND_MYSQL))->convert($pgOnly(), null),
            'the MySQL table reads the errno and must never consult a PostgreSQL SQLSTATE',
        );
    }

    /**
     * A Ferro `Retryable` the stock table does not recognise (a pool checkout timeout, a lost read)
     * must still SAY it is retryable, or the §9.2 branch is lost at the boundary. Only Deadlock and
     * LockWaitTimeout carry DBAL's marker, so this is the case that needs our own class.
     */
    public function testAnUnrecognisedRetryableStillCarriesTheRetryableMarker(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_POSTGRES);
        $out = $c->convert($this->ferro('57P03', null, C::BRANCH_RETRYABLE), null);
        self::assertInstanceOf(RetryableException::class, $out);
        self::assertNotInstanceOf(IndeterminateWriteException::class, $out);
    }

    /** A NonRetryable the stock table does not recognise stays a plain DriverException. */
    public function testAnUnrecognisedNonRetryableIsNotUpgraded(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_POSTGRES);
        $out = $c->convert($this->ferro('XX000', null, C::BRANCH_NON_RETRYABLE), null);
        self::assertNotInstanceOf(RetryableException::class, $out);
        self::assertNotInstanceOf(IndeterminateWriteException::class, $out);
    }

    /**
     * A driver-side failure carries NO wire payload, so `branch()` is null — and a null branch must
     * match NEITHER of the two branch tests. This is the case a `!==`-shaped condition gets wrong:
     * "anything that is not NonRetryable is Indeterminate" would turn a bad `driverOptions` value,
     * or the client's own `TypePolicyException` (a §9.1 policy refusal, deliberately outside the
     * three fate branches — and it DOES reach here, because `Ferro\DBAL\Connection` wraps every
     * `FerroException`), into "your write may or may not have landed".
     */
    public function testAFailureWithNoWirePayloadIsNeitherIndeterminateNorUpgraded(): void
    {
        $c = new ExceptionConverter(PlatformVersion::KIND_POSTGRES);
        $out = $c->convert(FerroDriverException::local('driverOptions.pool must be a string'), null);

        self::assertSame(DbalDriverException::class, $out::class, 'a payload-less failure stays generic');
        self::assertNotInstanceOf(IndeterminateWriteException::class, $out);
        self::assertNotInstanceOf(RetryableException::class, $out);
    }

    /**
     * Every `Doctrine\DBAL\Exception\*` class the INSTALLED dbal marks retryable.
     *
     * Derived by reflection, not by a hand-written list: the point of the ancestry guard is to
     * survive a DBAL upgrade that adds a retryable class, and a list in this file would not.
     *
     * @return list<class-string>
     */
    private static function dbalRetryableClasses(): array
    {
        $dir = \dirname((string) (new \ReflectionClass(DbalDriverException::class))->getFileName());
        $out = [RetryableException::class];
        foreach ((array) scandir($dir) as $file) {
            if (!is_string($file) || !str_ends_with($file, '.php')) {
                continue;
            }
            /** @var class-string $class */
            $class = 'Doctrine\\DBAL\\Exception\\' . substr($file, 0, -4);
            if (class_exists($class) && is_a($class, RetryableException::class, true)) {
                $out[] = $class;
            }
        }
        return $out;
    }
}
