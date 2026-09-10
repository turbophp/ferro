<?php // /php/laravel/tests/Live/WriteLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

/** C1c: writes through `statement()` / `affectingStatement()` / `unprepared()`. */
final class WriteLiveTest extends LaravelLiveTestCase
{
    private function fresh(string $table): \Ferro\Laravel\FerroPostgresConnection
    {
        $conn = $this->connection();
        $conn->unprepared("drop table if exists {$table}");
        $conn->unprepared("create table {$table} (id int primary key, n int not null default 0)");
        return $conn;
    }

    public function testAffectingStatementReportsTheENGINESCountNotTheRowCount(): void
    {
        $t = 'c1c_aff';
        $conn = $this->fresh($t);
        $conn->statement("insert into {$t} (id, n) values (1, 0), (2, 0), (3, 0)");

        // Matches 3 rows and changes NONE of them to a different value. A tier that counted
        // returned rows, or that inferred "changed" from the data, would answer 0 here.
        $affected = $conn->affectingStatement("update {$t} set n = 0 where id <= 3");

        self::assertSame(3, $affected, 'affected must be the engine\'s count, not a row count');
    }

    public function testStatementReturnsTrueAndActuallyWrites(): void
    {
        $t = 'c1c_stmt';
        $conn = $this->fresh($t);

        self::assertTrue($conn->statement("insert into {$t} (id, n) values (?, ?)", [7, 42]));

        $rows = $conn->select("select n from {$t} where id = ?", [7]);
        self::assertSame(42, $rows[0]->n, 'the write must be visible — a true return is not proof');
    }

    public function testDeleteReportsItsAffectedCount(): void
    {
        $t = 'c1c_del';
        $conn = $this->fresh($t);
        $conn->statement("insert into {$t} (id) values (1), (2), (3), (4)");

        self::assertSame(2, $conn->affectingStatement("delete from {$t} where id <= 2"));
        self::assertCount(2, $conn->select("select id from {$t}"));
    }

    public function testPretendingWritesNothing(): void
    {
        $t = 'c1c_pretend';
        $conn = $this->fresh($t);

        $conn->pretend(static function ($c) use ($t): void {
            $c->statement("insert into {$t} (id) values (99)");
            $c->affectingStatement("delete from {$t}");
        });

        self::assertSame([], $conn->select("select id from {$t}"),
            'pretend() must not reach the engine — `migrate --pretend` depends on it');
    }
}
