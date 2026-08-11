<?php // /php/doctrine-dbal/tests/Live/ReadonlyEnforcementLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Exception\DriverException as DbalDriverException;

/**
 * `driverOptions.readonly`, measured against a real engine on BOTH cells — the exact pair the
 * whole-branch review found disagreeing:
 *
 *     [autocommit] INSERT on readonly connection -> affected=1 (NOT REFUSED); rows now: 1
 *     [in-tx]      INSERT on readonly connection -> "cannot execute INSERT in a read-only transaction"
 *
 * The in-tx half is the SERVER refusing (`BEGIN READ ONLY` → `25006`); the autocommit half is this
 * driver refusing DBAL's write entry point pre-send. Both are asserted here, in one test, because
 * the defect was precisely that an operator who checked one concluded the other.
 *
 * PostgreSQL, because `25006` is where the server-side half is visible: the MySQL family reports
 * `START TRANSACTION READ ONLY` violations as `1792` with a different message, the engine's
 * `error_map` gives it the same NonRetryable branch, and duplicating the shape would add a second
 * path rather than a second proof. The DRIVER-side half is family-independent by construction (it
 * never reaches a backend) and is pinned offline for both by `Unit\ReadonlyEnforcementTest`.
 */
final class ReadonlyEnforcementLiveTest extends DbalLiveTestCase
{
    public function testAWriteIsRefusedOnAReadonlyConnectionInBothAutocommitAndATransaction(): void
    {
        $w = $this->dbal();                                   // the write connection: sets the fixture up
        $w->executeStatement('DROP TABLE IF EXISTS s8b_ro');
        $w->executeStatement('CREATE TABLE s8b_ro (id int primary key)');

        $ro = $this->dbal('default', ['readonly' => true]);

        // --- CELL 1: AUTOCOMMIT. Refused by the DRIVER, before anything is sent.
        try {
            $ro->executeStatement('INSERT INTO s8b_ro (id) VALUES (1)');
            self::fail('an autocommit write on a readonly connection must be refused');
        } catch (DbalDriverException $e) {
            self::assertStringContainsString('driverOptions.readonly', $e->getMessage());
            self::assertNull(
                $e->getSQLState(),
                'refused PRE-SEND: nothing reached a backend, so there is no SQLSTATE — a server-side '
                . 'refusal here would mean the statement had already travelled',
            );
        }

        // --- CELL 2: IN A TRANSACTION. Refused by the SERVER, because BEGIN carried READ ONLY.
        $ro->beginTransaction();
        try {
            $ro->executeStatement('INSERT INTO s8b_ro (id) VALUES (2)');
            self::fail('an in-transaction write on a readonly connection must be refused');
        } catch (DbalDriverException $e) {
            self::assertSame(
                '25006',
                $e->getSQLState(),
                'the SERVER refused it (read_only_sql_transaction) — this is the half that already '
                . 'worked, asserted so the two halves can never drift apart again',
            );
        }
        $ro->rollBack();

        // --- THE CONSEQUENCE, read back through the WRITE connection: the table is still empty.
        // Before the fix this read 1 — the autocommit INSERT had committed.
        self::assertSame(
            0,
            (int) $w->fetchOne('SELECT count(*) FROM s8b_ro'),
            'neither refused write landed',
        );

        // --- AND THE MIRROR: reads still work on the readonly connection, both shapes.
        $w->executeStatement('INSERT INTO s8b_ro (id) VALUES (3)');
        self::assertSame(3, (int) $ro->fetchOne('SELECT id FROM s8b_ro'), 'a parameterless read');
        self::assertSame(
            3,
            (int) $ro->fetchOne('SELECT id FROM s8b_ro WHERE id = ?', [3]),
            'and a parameterized one — the enforcement must not touch the read path',
        );

        $w->executeStatement('DROP TABLE s8b_ro');
    }
}
