<?php // /php/doctrine-dbal/tests/Unit/ExceptionAncestryTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Driver\Exception as DoctrineDriverExceptionInterface;
use Doctrine\DBAL\Exception as DbalExceptionInterface;
use Doctrine\DBAL\Exception\ConnectionLost;
use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\DriverException as DbalDriverException;
use Doctrine\DBAL\Exception\ForeignKeyConstraintViolationException;
use Doctrine\DBAL\Exception\LockWaitTimeoutException;
use Doctrine\DBAL\Exception\NoActiveTransaction;
use Doctrine\DBAL\Exception\RetryableException;
use Doctrine\DBAL\Exception\TransactionRolledBack;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Query;
use Doctrine\DBAL\ServerVersionProvider;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\DBAL\Connection as FerroDriverConnection;
use Ferro\DBAL\IndeterminateWriteException;
use Ferro\DBAL\PlatformVersion;
use Ferro\DBAL\RetryableDriverException;
use Ferro\DBAL\Tests\Support\FixedDriver;
use Ferro\DBAL\Wrapper\FerroConnection;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * The trade-off at the heart of the whole-branch review's BLOCKER 2, made falsifiable.
 *
 * `Ferro\DBAL\IndeterminateWriteException`'s PARENT decides two things at once, and they pull in
 * opposite directions:
 *
 *  - which `catch` blocks in an application fire on it — where a single wrong ancestor converts the
 *    project's headline at-most-once guarantee into an at-least-once write; and
 *  - whether `Doctrine\DBAL\Connection::transactional()` lets it reach the caller at all, since that
 *    method exempts five specific classes from a post-commit rollback that would otherwise replace
 *    it with `NoActiveTransaction`.
 *
 * This file derives BOTH from the installed doctrine/dbal rather than restating them, so the
 * decision cannot rot: a DBAL release that adds an exempt class, marks a new class retryable, or
 * unseals `ConnectionLost` goes red HERE.
 */
final class ExceptionAncestryTest extends TestCase
{
    /**
     * The exempt classes that ARE extendable and DO carry the retryable marker — none — plus the
     * ones that are extendable and unmarked but assert something about the write's fate that we do
     * not know. Every entry is a class Ferro must not inherit for a reason a machine cannot check,
     * so it is named here and {@see testNoDbalExemptClassIsAnHonestParentForAnIndeterminateWrite}
     * proves the list is neither stale nor short.
     *
     * @var list<class-string<\Throwable>>
     */
    private const CLAIM_A_FATE_WE_DO_NOT_KNOW = [
        TransactionRolledBack::class,                    // "it did not apply"
        UniqueConstraintViolationException::class,       // "a duplicate already exists"
        ForeignKeyConstraintViolationException::class,   // "a referenced row is missing"
    ];

    // ---------------------------------------------------------------- the ancestry itself --------

    /**
     * The FULL transitive ancestry, spelled out. Any change to it — a new parent, a new interface —
     * is a change to which `catch` blocks in every Doctrine application fire on an indeterminate
     * write, and must be a deliberate, reviewed act rather than a side effect.
     */
    public function testTheFullTransitiveAncestryOfAnIndeterminateWriteIsExactlyThis(): void
    {
        self::assertSame(
            [DbalDriverException::class, \Exception::class],
            array_values(class_parents(IndeterminateWriteException::class) ?: []),
        );

        $interfaces = array_values(class_implements(IndeterminateWriteException::class) ?: []);
        sort($interfaces);
        self::assertSame(
            [
                DoctrineDriverExceptionInterface::class,
                DbalExceptionInterface::class,
                \Stringable::class,
                \Throwable::class,
            ],
            $interfaces,
        );
    }

    /**
     * **THE trade-off assertion.** Exactly which `catch` blocks swallow an indeterminate write, each
     * measured with a REAL catch block on the REAL class, driven through a REAL
     * `transactional()` on this package's wrapper — i.e. the exception an application actually
     * receives, not one built by the test.
     *
     * `false` for `RetryableException` is the load-bearing cell: it is the marker Symfony Messenger
     * and every hand-rolled retry loop key on, and charter rule 3 forbids anything this driver
     * produces from inviting a replay of a write that may already have landed. `true` for the two
     * broad catches is the other half of the deal — an application that catches broadly must still
     * see it.
     */
    public function testExactlyTheseCatchBlocksFireOnAnIndeterminateWrite(): void
    {
        $run = static function (): void {
            self::lostCommitConnection(FerroConnection::class)->transactional(static fn (): int => 1);
        };

        $fired = [];

        try { $run(); } catch (RetryableException) { $fired['RetryableException'] = true; } catch (\Throwable) { $fired['RetryableException'] = false; }
        try { $run(); } catch (DeadlockException) { $fired['DeadlockException'] = true; } catch (\Throwable) { $fired['DeadlockException'] = false; }
        try { $run(); } catch (LockWaitTimeoutException) { $fired['LockWaitTimeoutException'] = true; } catch (\Throwable) { $fired['LockWaitTimeoutException'] = false; }
        try { $run(); } catch (ConnectionLost) { $fired['ConnectionLost'] = true; } catch (\Throwable) { $fired['ConnectionLost'] = false; }
        try { $run(); } catch (TransactionRolledBack) { $fired['TransactionRolledBack'] = true; } catch (\Throwable) { $fired['TransactionRolledBack'] = false; }
        try { $run(); } catch (UniqueConstraintViolationException) { $fired['UniqueConstraintViolationException'] = true; } catch (\Throwable) { $fired['UniqueConstraintViolationException'] = false; }
        try { $run(); } catch (NoActiveTransaction) { $fired['NoActiveTransaction'] = true; } catch (\Throwable) { $fired['NoActiveTransaction'] = false; }
        try { $run(); } catch (IndeterminateWriteException) { $fired['IndeterminateWriteException'] = true; } catch (\Throwable) { $fired['IndeterminateWriteException'] = false; }
        try { $run(); } catch (DbalDriverException) { $fired['Doctrine\DBAL\Exception\DriverException'] = true; } catch (\Throwable) { $fired['Doctrine\DBAL\Exception\DriverException'] = false; }
        try { $run(); } catch (DbalExceptionInterface) { $fired['Doctrine\DBAL\Exception'] = true; } catch (\Throwable) { $fired['Doctrine\DBAL\Exception'] = false; }
        try { $run(); } catch (\Throwable) { $fired['Throwable'] = true; }

        self::assertSame(
            [
                'RetryableException' => false,              // charter rule 3 — never invite a replay
                'DeadlockException' => false,
                'LockWaitTimeoutException' => false,
                'ConnectionLost' => false,
                'TransactionRolledBack' => false,
                'UniqueConstraintViolationException' => false,
                'NoActiveTransaction' => false,             // BLOCKER 2: this was `true` before the fix
                'IndeterminateWriteException' => true,
                'Doctrine\DBAL\Exception\DriverException' => true,
                'Doctrine\DBAL\Exception' => true,
                'Throwable' => true,
            ],
            $fired,
        );
    }

    // ------------------------------------------------- the exempt list, derived behaviourally ----

    /**
     * **The reason the parent was NOT changed, re-derived every run.**
     *
     * Re-parenting `IndeterminateWriteException` onto one of `transactional()`'s exempt classes
     * would have fixed the masking with no wrapper at all — charter rule 6 governs SQL generation,
     * not exception ancestry, so nothing forbade it. It is unavailable, and this test proves that
     * BEHAVIOURALLY rather than by reading DBAL's source: every candidate class is minted by a
     * driver whose COMMIT fails, run through the STOCK `Doctrine\DBAL\Connection::transactional()`,
     * and recorded as surviving or masked.
     *
     * Then each survivor must be disqualified for a stated reason. If a doctrine/dbal release adds
     * an exempt class that is extendable, unmarked and makes no false claim, this goes red — and
     * that is exactly the event that should reopen the decision.
     */
    public function testNoDbalExemptClassIsAnHonestParentForAnIndeterminateWrite(): void
    {
        $survivors = [];
        foreach (self::candidateExceptionClasses() as $cls) {
            if (self::survivesStockTransactional($cls)) {
                $survivors[] = $cls;
            }
        }

        self::assertNotEmpty($survivors, 'the derivation itself is broken if nothing survives');
        self::assertContains(
            ConnectionLost::class,
            $survivors,
            'anchor: ConnectionLost is on DBAL\'s exempt list, and it is what pdo_pgsql reports for '
            . 'this very event',
        );
        self::assertNotContains(
            IndeterminateWriteException::class,
            $survivors,
            'BLOCKER 2 restated as a derivation: our own class is NOT exempt, which is why the '
            . 'wrapper override exists. If this ever becomes false the override may be deleted.',
        );
        self::assertNotContains(RetryableDriverException::class, $survivors, 'nor is the Retryable one');

        $rejected = [];
        foreach ($survivors as $cls) {
            $rejected[$cls] = self::whyItCannotBeOurParent($cls);
        }
        self::assertSame(
            [],
            array_keys(array_filter($rejected, static fn (?string $why): bool => $why === null)),
            'a class on transactional()\'s exempt list that Ferro COULD honestly inherit from would '
            . 'make the wrapper override unnecessary — reopen the ancestry decision',
        );

        // The hand-written half of the reasoning must be fully consumed: a stale entry here would
        // silently excuse a class that is no longer exempt.
        foreach (self::CLAIM_A_FATE_WE_DO_NOT_KNOW as $cls) {
            self::assertContains($cls, $survivors, "$cls is named as an exempt class but is not one");
        }

        // And the one honest class must still be sealed — the whole fix turns on it.
        self::assertTrue(
            (new \ReflectionClass(ConnectionLost::class))->isFinal(),
            'ConnectionLost is the only exempt class that tells the truth about an indeterminate '
            . 'write; if it stops being final, extending it becomes the better fix',
        );
    }

    /**
     * @param class-string<\Throwable> $cls
     *
     * @return ?string null when the class would be an acceptable parent — i.e. when we have a problem
     */
    private static function whyItCannotBeOurParent(string $cls): ?string
    {
        if ((new \ReflectionClass($cls))->isFinal()) {
            return 'final';
        }
        if (is_a($cls, RetryableException::class, true)) {
            return 'carries DBAL\'s RetryableException marker';
        }
        if (in_array($cls, self::CLAIM_A_FATE_WE_DO_NOT_KNOW, true)) {
            return 'asserts a fate the engine did not report';
        }
        return null;
    }

    // ------------------------------------------------------------------------- scaffolding -------

    /**
     * Every concrete `Doctrine\DBAL\Exception\*` that is a `DriverException`, plus this package's
     * two, as the universe the derivation runs over.
     *
     * The class list comes from the installed package's directory, so a DBAL release that ADDS an
     * exception class is included automatically. A class whose constructor differs from
     * `DriverException`'s is a LOUD failure rather than a skip — silently dropping candidates is how
     * a derivation goes blind.
     *
     * @return list<class-string<DbalDriverException>>
     */
    private static function candidateExceptionClasses(): array
    {
        $dir = __DIR__ . '/../../vendor/doctrine/dbal/src/Exception';
        self::assertDirectoryExists($dir);

        $out = [];
        foreach ((array) scandir($dir) as $entry) {
            if (!is_string($entry) || !str_ends_with($entry, '.php')) {
                continue;
            }
            /** @var class-string $cls */
            $cls = 'Doctrine\\DBAL\\Exception\\' . substr($entry, 0, -4);
            if (!class_exists($cls) || !is_a($cls, DbalDriverException::class, true)) {
                continue;
            }
            $out[] = $cls;
        }
        self::assertGreaterThan(10, count($out), 'the vendor scan found suspiciously few classes');

        $out[] = IndeterminateWriteException::class;
        $out[] = RetryableDriverException::class;

        foreach ($out as $cls) {
            $ctor = (new \ReflectionClass($cls))->getConstructor();
            self::assertNotNull($ctor);
            self::assertSame(
                DbalDriverException::class,
                $ctor->getDeclaringClass()->getName(),
                "$cls overrides the DriverException constructor — the derivation below would have "
                . 'to construct it differently, and quietly skipping it would make this test blind',
            );
        }

        /** @var list<class-string<DbalDriverException>> $out */
        return $out;
    }

    /**
     * Does STOCK `Doctrine\DBAL\Connection::transactional()` let `$cls` reach the caller when the
     * COMMIT fails with it?
     *
     * The driver connection and the lost COMMIT are the real ones; only the exception CONVERTER is
     * swapped, so that a single scripted failure can be minted as any candidate class.
     */
    /** @param class-string<DbalDriverException> $cls */
    private static function survivesStockTransactional(string $cls): bool
    {
        $driverConn = new FerroDriverConnection(
            new FerroClientConnection(session: FakeSession::withTxBegin(txId: 1)->thenThrowOnCommit()),
            'default',
            PlatformVersion::KIND_POSTGRES,
            false,
        );
        $c = new DbalConnection(['serverVersion' => '17.10'], self::driverMinting($driverConn, $cls));

        try {
            $c->transactional(static fn (): int => 1);
        } catch (\Throwable $e) {
            return $e instanceof $cls;
        }
        self::fail("a lost COMMIT converted to $cls must not let transactional() return");
    }

    /** @param class-string<DbalDriverException> $cls */
    private static function driverMinting(FerroDriverConnection $conn, string $cls): DriverInterface
    {
        $inner = new FixedDriver($conn, PlatformVersion::KIND_POSTGRES);

        return new class ($inner, $cls) implements DriverInterface {
            /** @param class-string<DbalDriverException> $cls */
            public function __construct(private FixedDriver $inner, private string $cls) {}

            /** @param array<string,mixed> $params */
            public function connect(#[\SensitiveParameter] array $params): FerroDriverConnection
            {
                return $this->inner->connect($params);
            }

            public function getDatabasePlatform(ServerVersionProvider $v): AbstractPlatform
            {
                return $this->inner->getDatabasePlatform($v);
            }

            public function getExceptionConverter(): ExceptionConverterInterface
            {
                return new class ($this->cls) implements ExceptionConverterInterface {
                    /** @param class-string<DbalDriverException> $cls */
                    public function __construct(private string $cls) {}

                    public function convert(
                        DoctrineDriverExceptionInterface $exception,
                        ?Query $query,
                    ): DbalDriverException {
                        return new $this->cls($exception, $query);
                    }
                };
            }
        };
    }

    /** @param class-string<DbalConnection> $wrapperClass */
    private static function lostCommitConnection(string $wrapperClass): DbalConnection
    {
        $driverConn = new FerroDriverConnection(
            new FerroClientConnection(session: FakeSession::withTxBegin(txId: 7)->thenThrowOnCommit()),
            'default',
            PlatformVersion::KIND_POSTGRES,
            false,
        );
        return new $wrapperClass(
            ['serverVersion' => '17.10'],
            new FixedDriver($driverConn, PlatformVersion::KIND_POSTGRES),
        );
    }
}
