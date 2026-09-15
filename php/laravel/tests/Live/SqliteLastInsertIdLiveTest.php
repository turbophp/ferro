<?php // /php/laravel/tests/Live/SqliteLastInsertIdLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

/**
 * C3-6b: `getPdo()->lastInsertId()`, which SQLite reaches on EVERY Eloquent insert.
 *
 * `PostgresProcessor::processInsertGetId` uses `insert … returning id`, so the PostgreSQL tier never
 * touches this. SQLite has no such override: its inserts go through the base
 * `Processor::processInsertGetId`, which calls `$connection->getPdo()->lastInsertId()` — so every
 * `Model::create()` on this family depends on the two properties below.
 */
final class SqliteLastInsertIdLiveTest extends SqliteLiveTestCase
{
    public function testInsertReportsItsKey(): void
    {
        $conn = $this->sqliteConnection();
        $conn->statement('create table t (id integer primary key autoincrement, v text)');

        $conn->statement('insert into t (v) values (?)', ['a']);
        self::assertSame('1', $conn->getPdo()->lastInsertId());

        $conn->statement('insert into t (v) values (?)', ['b']);
        self::assertSame('2', $conn->getPdo()->lastInsertId());
    }

    /**
     * **The regression this class exists for.** The client's `lastInsertId()` is per-STATEMENT and
     * is cleared on the way in to every request; PDO's belongs to the HANDLE and keeps answering
     * until another insert replaces it. `Processor::processInsertGetId()` looks like it reads the
     * value immediately — `insert(); getPdo()->lastInsertId();` — but `Connection::insert()` fires
     * `QueryExecuted` first, and any listener that runs a query clears the client's value in
     * between. MEASURED: 182 of 225 errors in the framework suite's SQLite column, all on that line.
     *
     * Deleting `rememberInsertId()`'s call sites in `FerroConnectionBody` makes this test fail while
     * the one above still passes — which is the point: the naive version is green.
     */
    public function testTheKeySurvivesAnInterveningStatement(): void
    {
        $conn = $this->sqliteConnection();
        $conn->statement('create table t (id integer primary key autoincrement, v text)');
        $conn->statement('insert into t (v) values (?)', ['a']);

        // Exactly what a `QueryExecuted` listener does between the insert and Illuminate's read.
        $conn->select('select count(*) as n from t');

        self::assertSame(
            '1',
            $conn->getPdo()->lastInsertId(),
            'the key was cleared by an intervening statement — pdo_sqlite would still report it',
        );
    }

    /** An UPDATE moves rows but generates no key, so PDO keeps answering the previous insert's. */
    public function testAnUpdateDoesNotReplaceTheKey(): void
    {
        $conn = $this->sqliteConnection();
        $conn->statement('create table t (id integer primary key autoincrement, v text)');
        $conn->statement('insert into t (v) values (?)', ['a']);
        $conn->affectingStatement('update t set v = ? where v = ?', ['b', 'a']);

        self::assertSame('1', $conn->getPdo()->lastInsertId());
    }

    /**
     * Before any insert there is genuinely nothing to report, and the shim THROWS rather than
     * returning PDO's falsy `false` — which `Processor::processInsertGetId` would hand back as a
     * model's primary key. §22.2 (m) records a wrong key as strictly worse than none.
     */
    public function testItThrowsWhenNothingHasBeenInserted(): void
    {
        $conn = $this->sqliteConnection();
        $conn->statement('create table t (id integer primary key autoincrement, v text)');

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/has generated a key/');
        $conn->getPdo()->lastInsertId();
    }
}
