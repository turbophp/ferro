<?php // /php/doctrine-dbal/tests/Live/DriverSmokeLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;

/**
 * M1-S8b Task 5 — the walking skeleton, driven through the REAL
 * `Doctrine\DBAL\DriverManager::getConnection(['driverClass' => …])` against a real ferrod on real
 * PostgreSQL and real MySQL. If this passes, the SPI wiring is right and every later task is a
 * refinement of something that already works.
 */
final class DriverSmokeLiveTest extends DbalLiveTestCase
{
    public function testAQueryAStatementAndATransactionAllWorkOnPostgres(): void
    {
        $c = $this->dbal();
        self::assertInstanceOf(PostgreSQLPlatform::class, $c->getDatabasePlatform());

        $c->executeStatement('DROP TABLE IF EXISTS s8b_smoke');
        $c->executeStatement('CREATE TABLE s8b_smoke (id int primary key, note text)');
        self::assertSame(
            2,
            $c->executeStatement('INSERT INTO s8b_smoke (id, note) VALUES (1, \'a\'), (2, \'b\')'),
            'executeStatement returns the terminal affected count, not count($rows)',
        );

        self::assertSame(
            [['id' => 1, 'note' => 'a'], ['id' => 2, 'note' => 'b']],
            $c->fetchAllAssociative('SELECT id, note FROM s8b_smoke ORDER BY id'),
        );
        self::assertSame('b', $c->fetchOne('SELECT note FROM s8b_smoke WHERE id = ?', [2]));
        self::assertSame([[1, 'a'], [2, 'b']], $c->fetchAllNumeric('SELECT id, note FROM s8b_smoke ORDER BY id'));

        $c->beginTransaction();
        $c->executeStatement('UPDATE s8b_smoke SET note = \'z\' WHERE id = 1');
        self::assertSame('z', $c->fetchOne('SELECT note FROM s8b_smoke WHERE id = 1'));
        $c->rollBack();
        self::assertSame('a', $c->fetchOne('SELECT note FROM s8b_smoke WHERE id = 1'), 'the rollback reached the pinned tx');

        $c->executeStatement('DROP TABLE s8b_smoke');
    }

    public function testTheSameDriverServesAMysqlPoolAndSelectsAMysqlPlatform(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->dbal($pool);
        self::assertInstanceOf(
            AbstractMySQLPlatform::class,
            $c->getDatabasePlatform(),
            'one driverClass, two families — the platform comes from HELLO_ACK, not from the class',
        );

        $c->executeStatement('DROP TABLE IF EXISTS s8b_smoke');
        $c->executeStatement('CREATE TABLE s8b_smoke (id INT PRIMARY KEY, note VARCHAR(32))');
        $c->executeStatement('INSERT INTO s8b_smoke (id, note) VALUES (1, \'a\')');
        self::assertSame('a', $c->fetchOne('SELECT note FROM s8b_smoke WHERE id = ?', [1]));
        $c->executeStatement('DROP TABLE s8b_smoke');
    }

    /**
     * §14 claims "DBAL middlewares (logging, schema managers, migrations) operate above the driver
     * SPI and work unchanged". Middlewares wrap the DRIVER (`Driver\Middleware::wrap(Driver): Driver`,
     * applied by `DriverManager` before the wrapper Connection is built), so this is testable in
     * four lines — and worth testing, because a middleware also wraps our `Result`, and
     * `AbstractResultMiddleware::getColumnName()` forwards through a `method_exists` guard that
     * would throw if we had skipped that method.
     */
    public function testTheDriverComposesWithADbalMiddleware(): void
    {
        $seen = [];
        $middleware = new class ($seen) implements \Doctrine\DBAL\Driver\Middleware {
            /** @param list<string> $seen */
            public function __construct(private array &$seen) {}

            public function wrap(\Doctrine\DBAL\Driver $driver): \Doctrine\DBAL\Driver
            {
                $seen = &$this->seen;
                return new class ($driver, $seen) extends \Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware {
                    /** @param list<string> $seen */
                    public function __construct(\Doctrine\DBAL\Driver $driver, private array &$seen)
                    {
                        parent::__construct($driver);
                    }

                    /** @param array<string,mixed> $params */
                    public function connect(#[\SensitiveParameter] array $params): \Doctrine\DBAL\Driver\Connection
                    {
                        $this->seen[] = 'connect';
                        return parent::connect($params);
                    }
                };
            }
        };

        $config = new \Doctrine\DBAL\Configuration();
        $config->setMiddlewares([$middleware]);
        $c = \Doctrine\DBAL\DriverManager::getConnection([
            'driverClass' => \Ferro\DBAL\Driver::class,
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => 'default'],
        ], $config);

        self::assertSame([[1]], $c->fetchAllNumeric('SELECT 1'));
        self::assertSame(['connect'], $seen, 'the middleware really wrapped the driver');
        // The Result travelled through the middleware stack, so getColumnName() was forwarded
        // through its method_exists guard rather than throwing a LogicException.
        self::assertSame('one', $c->executeQuery('SELECT 1 AS one')->getColumnName(0));
    }
}
