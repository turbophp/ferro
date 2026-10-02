<?php // /php/doctrine-dbal/tests/Dbal3/Dbal3IsolationTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Dbal3;

use Doctrine\DBAL\Driver\Exception as DriverExceptionInterface;
use Doctrine\DBAL\Platforms\MySQL80Platform;
use Doctrine\DBAL\Platforms\PostgreSQL100Platform;
use Doctrine\DBAL\TransactionIsolationLevel;
use Ferro\DBAL\Dbal3\FerroConnection;
use Ferro\DBAL\IsolationStatement;
use Ferro\Protocol\Isolation;
use PHPUnit\Framework\TestCase;

/**
 * M2-C5 — DBAL 3's isolation surface: the level mapping (DBAL 3 levels are int constants) and the
 * refusal's matcher against the strings DBAL 3's OWN platforms emit.
 */
final class Dbal3IsolationTest extends TestCase
{
    /** Derived by reflection, so a level a later 3.x adds fails here rather than in production. */
    public function testEveryDbal3LevelMapsLikeDbal4s(): void
    {
        $expected = [
            'READ_UNCOMMITTED' => Isolation::ReadCommitted,
            'READ_COMMITTED' => Isolation::ReadCommitted,
            'REPEATABLE_READ' => Isolation::RepeatableRead,
            'SERIALIZABLE' => Isolation::Serializable,
        ];
        $levels = (new \ReflectionClass(TransactionIsolationLevel::class))->getConstants();
        self::assertSame(array_keys($expected), array_keys($levels), 'DBAL 3 changed its isolation levels');
        foreach ($levels as $name => $value) {
            self::assertSame($expected[$name], FerroConnection::toFerroIsolation($value), $name);
        }
    }

    public function testAnUnknownLevelIsRefused(): void
    {
        $this->expectException(DriverExceptionInterface::class);
        FerroConnection::toFerroIsolation(9);
    }

    /** The matcher is shared; the strings it must recognise are DBAL 3's own. */
    public function testEveryStockDbal3IsolationStatementIsRecognised(): void
    {
        foreach ((new \ReflectionClass(TransactionIsolationLevel::class))->getConstants() as $level) {
            self::assertTrue(IsolationStatement::matches((new PostgreSQL100Platform())->getSetTransactionIsolationSQL($level)));
            self::assertTrue(IsolationStatement::matches((new MySQL80Platform())->getSetTransactionIsolationSQL($level)));
        }
    }
}
