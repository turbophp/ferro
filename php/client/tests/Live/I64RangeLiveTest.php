<?php // /php/client/tests/Live/I64RangeLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\Value\RawStringValuePolicy;

/**
 * **M1-S8c — the `bigint` range, end to end against a real backend.**
 *
 * The offline guard ({@see \Ferro\Tests\Unit\I64BoundaryDecodeTest}) proves the codec; this proves
 * the CLAIM, which is about the product: a `bigint` column is readable. It is the shape the defect
 * was measured in — `Client\Connection` against real Postgres through a real `ferrod`, no DBAL
 * anywhere — where `SELECT 4294967295::int8` returned an `int` and `SELECT 4294967296::int8` threw
 * `ProtocolException: value tag 2: expected a int payload, got string`. Every `bigint` PK past
 * 4.29e9 and every epoch-millis column was unreadable on every backend and every value policy.
 *
 * Reading a LITERAL is not enough on its own, so each boundary also makes a round trip through a
 * real `int8`/`BIGINT` COLUMN and back through a bound parameter — a stored value has to survive
 * the engine's bind path, the column, and the read path, which a `SELECT <literal>` skips entirely.
 */
final class I64RangeLiveTest extends LiveTestCase
{
    private const TABLE = 's8c_i64_range';

    /**
     * The boundaries, as SQL-literal text (never as PHP ints in the SQL, so the *server* parses the
     * exact value). `-9223372036854775808` is quoted: PostgreSQL parses the magnitude of a bare
     * negative literal FIRST and rejects it as `22003 bigint out of range`, which is a PG literal
     * quirk and has nothing to do with this client.
     *
     * @return array<string, array{0: string, 1: int}>
     */
    public static function boundaries(): array
    {
        return [
            'PHP_INT_MIN' => ["'-9223372036854775808'", PHP_INT_MIN],
            '-(2^53)-1'   => ['-9007199254740993',      -9007199254740993],
            '-(2^32)'     => ['-4294967296',            -4294967296],
            '-(2^31)'     => ['-2147483648',            -2147483648],
            'int -1'      => ['-1',                     -1],
            'int 0'       => ['0',                      0],
            '2^31-1'      => ['2147483647',             2147483647],
            '2^31'        => ['2147483648',             2147483648],
            // THE DEFECT BOUNDARY: the last uint32 and the first uint64 on the wire.
            '2^32-1'      => ['4294967295',             4294967295],
            '2^32'        => ['4294967296',             4294967296],
            '2^32+1'      => ['4294967297',             4294967297],
            // the float cliff — a `(float)` round trip loses these
            '2^53-1'      => ['9007199254740991',       9007199254740991],
            '2^53'        => ['9007199254740992',       9007199254740992],
            '2^53+1'      => ['9007199254740993',       9007199254740993],
            'PHP_INT_MAX' => ['9223372036854775807',    PHP_INT_MAX],
        ];
    }

    public function testEveryBigintBoundaryIsReadableOnPostgres(): void
    {
        $this->assertBoundariesReadable($this->connectConnection(), 'int8', 'int8', 'PostgreSQL');
    }

    public function testEveryBigintBoundaryIsReadableOnTheMysqlFamily(): void
    {
        $pool = $this->requireMysqlPool();
        // MySQL's CAST target for a 64-bit integer is `SIGNED`, not the column type name.
        $this->assertBoundariesReadable(
            $this->connectConnection(null, $pool),
            'BIGINT',
            'SIGNED',
            'MySQL family',
        );
    }

    /**
     * The DBAL hand-off policy decodes `I64` through the same guard, so a regression that only
     * reached the driver tier would still be caught here rather than one package away.
     */
    public function testTheDriverNativePolicyReadsTheSameRange(): void
    {
        $conn = new Connection($this->connect(), 'default', values: new RawStringValuePolicy());
        $this->assertBoundariesReadable($conn, 'int8', 'int8', 'PostgreSQL / RawStringValuePolicy');
    }

    private function assertBoundariesReadable(
        Connection $conn,
        string $columnType,
        string $castType,
        string $where,
    ): void {
        $conn->exec('DROP TABLE IF EXISTS ' . self::TABLE);
        $conn->exec('CREATE TABLE ' . self::TABLE . ' (id INT PRIMARY KEY, v ' . $columnType . ')');

        $i = 0;
        foreach (self::boundaries() as $label => [$literal, $expected]) {
            ++$i;

            // 1. read the value the SERVER produced from its own literal.
            $read = $conn->scalar('SELECT CAST(' . $literal . ' AS ' . $castType . ')');
            $this->assertSame($expected, $read, "{$where}: SELECT literal {$label}");

            // 2. store it via a BOUND parameter and read the column back — the round trip the
            //    literal read does not exercise.
            $conn->exec(
                'INSERT INTO ' . self::TABLE . ' (id, v) VALUES (?, ?)',
                [$i, $expected],
            );
            $back = $conn->scalar('SELECT v FROM ' . self::TABLE . ' WHERE id = ?', [$i]);
            $this->assertSame($expected, $back, "{$where}: bound round trip {$label}");
        }

        // 3. and the whole set in ONE result set, so a per-row type flip (an `int` in one row and a
        //    string in the next for the same column) cannot hide behind single-value reads.
        $rows = $conn->rows('SELECT id, v FROM ' . self::TABLE . ' ORDER BY id');
        $this->assertCount(count(self::boundaries()), $rows);
        $expectedAll = array_map(
            static fn (array $c): int => $c[1],
            array_values(self::boundaries()),
        );
        $got = [];
        foreach ($rows as $row) {
            $v = $row['v'] ?? null;
            $this->assertIsInt($v, "{$where}: every row of a bigint column must be an int");
            $got[] = $v;
        }
        $this->assertSame($expectedAll, $got, "{$where}: the whole column, in order");

        $conn->exec('DROP TABLE IF EXISTS ' . self::TABLE);
    }
}
