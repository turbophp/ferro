<?php // /php/client/tests/Live/QueryLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Session;
use Ferro\Tests\Support\PersonDto;

/**
 * End-to-end query API against a real `ferrod` + Docker Postgres: a scalar read, assoc-row
 * hydration, `final readonly` DTO hydration (snake_case→camelCase), and a syntax error surfacing as
 * a NonRetryableException carrying a class-42 SQLSTATE — the three-branch taxonomy grounded on the
 * live wire.
 */
final class QueryLiveTest extends LiveTestCase
{
    private function connection(): Connection
    {
        $session = $this->connect(); // handshakes (LiveTestCase::connect)
        return new Connection($session, 'default');
    }

    public function testScalarSelectOne(): void
    {
        $conn = $this->connection();
        try {
            $this->assertSame(1, $conn->scalar('SELECT 1'));
        } finally {
            $conn->session()->close();
        }
    }

    public function testQueryReturnsAssocRows(): void
    {
        $conn = $this->connection();
        try {
            $this->assertSame([['n' => 1, 'm' => 2]], $conn->query('SELECT 1 AS n, 2 AS m'));
        } finally {
            $conn->session()->close();
        }
    }

    public function testQueryOneHydratesFinalReadonlyDto(): void
    {
        $conn = $this->connection();
        try {
            $dto = $conn->queryOne(
                "SELECT 1 AS id, 'Ada'::text AS first_name, true AS is_active",
                [],
                PersonDto::class,
            );
            $this->assertInstanceOf(PersonDto::class, $dto);
            $this->assertSame(1, $dto->id);
            $this->assertSame('Ada', $dto->firstName);
            $this->assertTrue($dto->isActive);
        } finally {
            $conn->session()->close();
        }
    }

    public function testSyntaxErrorIsNonRetryableWithClass42Sqlstate(): void
    {
        $conn = $this->connection();
        try {
            $conn->query('SELCT 1');
            $this->fail('expected a NonRetryableException for a syntax error');
        } catch (NonRetryableException $e) {
            $sqlstate = $e->sqlstate();
            $this->assertNotNull($sqlstate, 'a syntax error carries a SQLSTATE');
            $this->assertStringStartsWith('42', $sqlstate, 'PG syntax errors are class 42');
        } finally {
            $conn->session()->close();
        }
    }
}
