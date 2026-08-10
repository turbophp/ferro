<?php // /php/doctrine-dbal/tests/Live/TypeBoundaryLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Types\Types;
use Ferro\DBAL\Exception\NonRepresentableValue;

/**
 * M1-S8b Task 9, live — a real `timestamptz` written and read back through the STOCK Doctrine type
 * layer, on a real PostgreSQL, plus the refusal of a real `24:00:00` that PostgreSQL genuinely
 * stores.
 *
 * The refusal half is the important one: it is the only test in the slice that observes a value
 * which is legal in the database, legal on the wire, readable through the native Ferro API, and
 * SILENTLY WRONG through stock Doctrine.
 */
final class TypeBoundaryLiveTest extends DbalLiveTestCase
{
    public function testATimestamptzRoundTripsThroughTheStockTypeLayer(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_tz');
        $c->executeStatement('CREATE TABLE s8b_tz (id int primary key, at timestamptz)');

        $when = new \DateTimeImmutable('2026-08-05 13:45:07', new \DateTimeZone('UTC'));
        $c->executeStatement(
            'INSERT INTO s8b_tz (id, at) VALUES (?, ?)',
            [1, $when],
            [Types::INTEGER, Types::DATETIMETZ_IMMUTABLE],
        );

        $back = $c->fetchOne('SELECT at FROM s8b_tz WHERE id = ?', [1]);
        self::assertIsString($back);
        $obj = \Doctrine\DBAL\Types\Type::getType(Types::DATETIMETZ_IMMUTABLE)
            ->convertToPHPValue($back, $c->getDatabasePlatform());
        self::assertInstanceOf(\DateTimeInterface::class, $obj);
        self::assertSame(
            $when->getTimestamp(),
            $obj->getTimestamp(),
            'the instant must survive the wire and both conversions',
        );

        $c->executeStatement('DROP TABLE s8b_tz');
    }

    public function testAPostgresTwentyFourHourTimeIsRefusedRatherThanSilentlyWrapped(): void
    {
        $c = $this->dbal();
        $c->executeStatement('DROP TABLE IF EXISTS s8b_t24');
        $c->executeStatement('CREATE TABLE s8b_t24 (id int primary key, t time)');
        // PostgreSQL accepts and STORES this; it is not a malformed value.
        $c->executeStatement("INSERT INTO s8b_t24 (id, t) VALUES (1, TIME '24:00:00')");

        try {
            $c->fetchOne('SELECT t FROM s8b_t24 WHERE id = ?', [1]);
            self::fail('24:00:00 must be refused — Doctrine would read it back as 00:00:00');
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            self::assertInstanceOf(NonRepresentableValue::class, $e->getPrevious());
        }

        // …and it IS readable through the native API, which is what the refusal message says.
        //
        // THE CAVEAT THE MESSAGE HAS TO STATE, and which this test pins rather than leaves in prose:
        // `getNativeConnection()` hands back the very connection the driver built, so it carries
        // THIS policy and refuses identically. "The native API" means a client connection of its
        // OWN — which is what an application that needs such a column has to open.
        $native = \Ferro\Ferro::connect($this->socketPath, 'default');
        self::assertSame('24:00:00', (string) $native->scalar('SELECT t FROM s8b_t24 WHERE id = 1'));

        try {
            $c->getNativeConnection()->scalar('SELECT t FROM s8b_t24 WHERE id = 1');
            self::fail('getNativeConnection() shares the driver policy and must refuse too');
        } catch (NonRepresentableValue $e) {
            self::assertStringContainsString('getNativeConnection', $e->getMessage());
        }

        $c->executeStatement('DROP TABLE s8b_t24');
    }

    public function testAMysqlZeroDateIsRefusedRatherThanSilentlyShifted(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->dbal($pool);
        $c->executeStatement('DROP TABLE IF EXISTS s8b_zero');
        $c->executeStatement('CREATE TABLE s8b_zero (id INT PRIMARY KEY, d DATE)');

        // The `SET SESSION sql_mode = ''` is how a zero-in-date gets INTO the table at all, and it
        // MUST share a checkout with the INSERT: MySQL hygiene resets every reused connection
        // (`clean_reset_profile() = Some(Full)` → `COM_RESET_CONNECTION`), so as two autocommit
        // statements the mode is wiped before the INSERT runs and the INSERT fails 1292 under the
        // 8.4 default `STRICT_TRANS_TABLES,NO_ZERO_IN_DATE`. MEASURED, and the reason this fixture
        // is wrapped in an explicit transaction: a transaction PINS one backend connection, so the
        // mode is still in force one statement later. The taint is wiped at the next checkout,
        // which is why the SELECT below reads under the ordinary strict mode.
        // If Task 13's refusal of isolation SQL is ever generalised to all `SET SESSION`, this
        // fixture must switch to an engine-side `sql_mode`; do not silently drop it.
        $c->beginTransaction();
        $c->executeStatement("SET SESSION sql_mode = ''");
        $c->executeStatement("INSERT INTO s8b_zero (id, d) VALUES (1, '2026-00-05')");
        $c->commit();

        try {
            $c->fetchOne('SELECT d FROM s8b_zero WHERE id = ?', [1]);
            self::fail('a zero-in-date must be refused — Doctrine would read it back as 2025-12-05');
        } catch (\Doctrine\DBAL\Exception\DriverException $e) {
            self::assertInstanceOf(NonRepresentableValue::class, $e->getPrevious());
        }

        // The value really is in the table, and really is the zero-in-date: without this the test
        // could not tell a refusal of a STORED `2026-00-05` from a fixture that never inserted one.
        $native = \Ferro\Ferro::connect($this->socketPath, $pool);
        self::assertSame('2026-00-05', (string) $native->scalar('SELECT d FROM s8b_zero WHERE id = 1'));

        $c->executeStatement('DROP TABLE s8b_zero');
    }
}
