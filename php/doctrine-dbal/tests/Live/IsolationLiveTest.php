<?php // /php/doctrine-dbal/tests/Live/IsolationLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception\DriverException as DbalDriverException;
use Doctrine\DBAL\Exception\RetryableException;
use Doctrine\DBAL\TransactionIsolationLevel;
use Ferro\DBAL\Exception\UnsupportedStatement;
use Ferro\DBAL\IndeterminateWriteException;
use Ferro\DBAL\Wrapper\FerroConnection;

/**
 * M1-S8b Task 13, live.
 *
 * The isolation assertion is made from a vantage point where it is OBSERVABLE:
 * `current_setting('transaction_isolation')` INSIDE the open transaction. SPEC §22.2 (s) records
 * why the tempting alternatives cannot fail — a session-variable read-back reports the session
 * default whatever happened, and a "did the next tenant inherit it" check is masked by hygiene in
 * both directions.
 */
final class IsolationLiveTest extends DbalLiveTestCase
{
    private function wrapped(string $pool = 'default'): DbalConnection
    {
        $c = DriverManager::getConnection([
            'driverClass' => \Ferro\DBAL\Driver::class,
            'wrapperClass' => FerroConnection::class,
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => $pool],
        ]);
        self::assertInstanceOf(\Ferro\Client\Connection::class, $c->getNativeConnection());
        return $c;
    }

    public function testTheWrapperMakesSetTransactionIsolationTakeEffect(): void
    {
        $c = $this->wrapped();

        // THE MIRROR, and it is what stops the assertions below from passing for the wrong reason:
        // an engine that ignored the byte and an engine that honoured it would look identical if
        // the pool default were already `serializable`. It is not.
        $c->beginTransaction();
        self::assertSame(
            'read committed',
            $c->fetchOne("SELECT current_setting('transaction_isolation')"),
            'the pool default — so `serializable` below can only come from the level we asked for',
        );
        $c->commit();

        $c->setTransactionIsolation(TransactionIsolationLevel::SERIALIZABLE);

        // Doctrine's own accessor must agree with what the wrapper captured. The field the stock
        // implementation would have set is PRIVATE to `Doctrine\DBAL\Connection`, so a wrapper that
        // captured the level but forgot to override the getter reports the PLATFORM DEFAULT while
        // actually running at the level asked for. It is asserted HERE, at SERIALIZABLE, and not at
        // the READ_COMMITTED leg below: PostgreSQL's platform default IS `READ_COMMITTED`, so that
        // assertion passes identically with and without the override — measured, not assumed
        // (mutation B1 in the journal, which came back GREEN against the first draft of this test).
        self::assertSame(TransactionIsolationLevel::SERIALIZABLE, $c->getTransactionIsolation());

        $c->beginTransaction();
        self::assertSame(
            'serializable',
            $c->fetchOne("SELECT current_setting('transaction_isolation')"),
            'the level must reach the BEGIN itself, not a session variable',
        );
        $c->commit();

        // Sticky, matching Doctrine's semantics: it applies to EVERY subsequent transaction.
        $c->beginTransaction();
        self::assertSame('serializable', $c->fetchOne("SELECT current_setting('transaction_isolation')"));
        $c->commit();

        $c->setTransactionIsolation(TransactionIsolationLevel::READ_COMMITTED);
        $c->beginTransaction();
        self::assertSame('read committed', $c->fetchOne("SELECT current_setting('transaction_isolation')"));
        $c->commit();

        // The mirror of the accessor assertion above: it must track the level DOWN as well as up.
        // (This one alone would be a tautology — see the comment at the SERIALIZABLE assertion.)
        self::assertSame(TransactionIsolationLevel::READ_COMMITTED, $c->getTransactionIsolation());
    }

    /**
     * WITHOUT the wrapper the raw statement is REFUSED. The alternative — letting it through — is
     * the silent no-op this whole task exists to eliminate, and it is invisible: the statement
     * succeeds, `getTransactionIsolation()` reports the requested level, and every later
     * transaction runs at the pool default.
     */
    public function testWithoutTheWrapperTheRawIsolationStatementIsRefusedLoudly(): void
    {
        $c = $this->dbal();
        try {
            $c->setTransactionIsolation(TransactionIsolationLevel::SERIALIZABLE);
            self::fail('the raw SET SESSION … statement must be refused, not silently ignored');
        } catch (DbalDriverException $e) {
            self::assertInstanceOf(UnsupportedStatement::class, $e->getPrevious());
            self::assertStringContainsString('wrapperClass', $e->getMessage());

            // A refusal is NOT a fate signal. Nothing reached a backend, so it carries no SQLSTATE
            // and no errno, the §9.2 branch is null, and the converter must leave it GENERIC:
            // never `Indeterminate` (which would claim a write may have landed) and never
            // `Retryable` (which invites a framework to replay a statement that will fail
            // identically forever).
            self::assertSame(DbalDriverException::class, $e::class, 'must stay the generic DriverException');
            self::assertNotInstanceOf(IndeterminateWriteException::class, $e);
            self::assertNotInstanceOf(RetryableException::class, $e);
        }

        // …and the connection is still perfectly usable afterwards.
        self::assertSame(1, $c->fetchOne('SELECT 1'));
    }

    /**
     * The refusal covers the ZERO-PARAMETER `executeQuery()` path too — on BOTH families.
     *
     * This is not a redundant restatement of the test above. `Doctrine\DBAL\Connection::
     * executeStatement()` with no parameters reaches the driver's `exec()`, but `executeQuery()`
     * with no parameters reaches `query()`, and on PostgreSQL `query()` is Task 12's STREAMING path:
     * it goes to the wire through `streamRaw()` and never touches `runPrepared()`. Guarding only
     * `exec()` and `runPrepared()` — as the plan's Step 4 says — therefore leaves an application
     * that writes `executeQuery('SET SESSION CHARACTERISTICS AS …')` itself unrefused on exactly
     * one of the two engines, which is the silent wrong-isolation bug back again on the family
     * where it is hardest to notice. Measured: this test is RED against the two-site form.
     */
    public function testTheRefusalAlsoCoversTheZeroParameterQueryPath(): void
    {
        $mysqlPool = $this->requireMysqlPool();
        foreach ([
            'postgres' => ['default', 'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE'],
            'mysql' => [$mysqlPool, 'SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE'],
        ] as $family => [$pool, $sql]) {
            $c = $this->dbal($pool);
            try {
                $c->executeQuery($sql);
                self::fail("[$family] executeQuery() must refuse the isolation statement too");
            } catch (DbalDriverException $e) {
                self::assertInstanceOf(UnsupportedStatement::class, $e->getPrevious(), "[$family]");
            }
            self::assertSame(1, (int) $c->fetchOne('SELECT 1'), "[$family] still usable");
        }
    }

    /** The same, on MySQL, where the statement text differs and the level genuinely differs too. */
    public function testTheWrapperAlsoWorksOnMysql(): void
    {
        $pool = $this->requireMysqlPool();
        $a = $this->wrapped($pool);
        $b = $this->dbal($pool);

        $a->executeStatement('DROP TABLE IF EXISTS s8b_iso_dbal');
        $a->executeStatement('CREATE TABLE s8b_iso_dbal (id INT PRIMARY KEY, v INT) ENGINE=InnoDB');
        $a->executeStatement('INSERT INTO s8b_iso_dbal (id, v) VALUES (1, 1)');

        $a->setTransactionIsolation(TransactionIsolationLevel::SERIALIZABLE);
        $a->beginTransaction();
        self::assertSame(1, (int) $a->fetchOne('SELECT v FROM s8b_iso_dbal WHERE id = 1'));

        // Under SERIALIZABLE that plain SELECT took a shared lock, so B must block and time out.
        // The timeout is set INSIDE B's own transaction on purpose: an autocommit `SET SESSION`
        // taints its checkout but does not PIN it (§7.4), so the UPDATE could land on a different
        // pooled connection still carrying the 50 s default — which is how the plan's form measured
        // at ~50 s in Task 3 rather than failing.
        $b->beginTransaction();
        $b->executeStatement('SET SESSION innodb_lock_wait_timeout = 1');
        $blocked = false;
        try {
            $b->executeStatement('UPDATE s8b_iso_dbal SET v = 2 WHERE id = 1');
        } catch (RetryableException) {
            $blocked = true;
        } finally {
            // A 1205 does NOT end B's transaction (`innodb_rollback_on_timeout` is OFF by default:
            // only the STATEMENT is rolled back), so B would keep the metadata lock its UPDATE took
            // and the closing DROP TABLE — a fresh autocommit checkout — would hang to the client's
            // io timeout and surface as an IndeterminateException. Measured in Task 3.
            if ($b->isTransactionActive()) {
                $b->rollBack();
            }
        }
        self::assertTrue($blocked, 'SERIALIZABLE must make the read block a concurrent write');

        $a->commit();

        // THE MIRROR: at the pool default the very same read does NOT block, so the assertion above
        // is testing SERIALIZABLE rather than testing that MySQL locks something.
        $a->setTransactionIsolation(TransactionIsolationLevel::READ_COMMITTED);
        $a->beginTransaction();
        self::assertSame(1, (int) $a->fetchOne('SELECT v FROM s8b_iso_dbal WHERE id = 1'));
        $b->beginTransaction();
        $b->executeStatement('SET SESSION innodb_lock_wait_timeout = 1');
        $b->executeStatement('UPDATE s8b_iso_dbal SET v = 3 WHERE id = 1');
        $b->rollBack();
        $a->commit();

        $a->executeStatement('DROP TABLE s8b_iso_dbal');
    }
}
