<?php // /php/client/tests/Live/RawFetchLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\RetryPolicy;

/**
 * M1-S8b Task 1, live: `fetchRaw()` against a real ferrod on a real PostgreSQL, proving the three
 * properties a DBAL `Result` stands on — positional rows, `affected` from the terminal (not
 * `count($rows)`), and the caller's `readonly` flag actually travelling — plus `poolInfo()`
 * answering for the pool this connection is bound to.
 */
final class RawFetchLiveTest extends LiveTestCase
{
    public function testPositionalRowsAndAffectedComeBackSeparately(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8b_raw');
        $c->exec('CREATE TABLE s8b_raw (id int primary key, note text)');
        $c->exec('INSERT INTO s8b_raw (id, note) VALUES (1, \'a\'), (2, \'b\')');

        $read = $c->fetchRaw('SELECT id, note FROM s8b_raw ORDER BY id', [], true);
        self::assertSame(['id', 'note'], $read['cols']);
        self::assertSame([[1, 'a'], [2, 'b']], $read['rows'], 'rows must be POSITIONAL');

        // An UPDATE touching 2 rows: `affected` is 2 while `rows` is empty. A Result that returned
        // count($rows) here would report 0 — the research spike's bug.
        $upd = $c->fetchRaw('UPDATE s8b_raw SET note = \'z\'', [], false, false);
        self::assertSame(2, $upd['affected']);
        self::assertSame([], $upd['rows']);

        $c->exec('DROP TABLE s8b_raw');
    }

    /**
     * Two columns that a DBAL `fetchNumeric()` must keep apart, live: PostgreSQL happily returns two
     * result columns both named `x`, and {@see \Ferro\Client\Connection::rows}' `array_combine`
     * collapses them to one key. The positional shape is the whole reason this method exists.
     */
    public function testDuplicateColumnNamesSurviveLive(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());

        $raw = $c->fetchRaw('SELECT 1 AS x, 2 AS x', [], true);
        self::assertSame(['x', 'x'], $raw['cols']);
        self::assertSame([[1, 2]], $raw['rows']);

        // The mirror: the assoc accessor really does collapse them, so the guard above is measuring
        // a genuine difference rather than restating the same value twice.
        self::assertSame([['x' => 2]], $c->rows('SELECT 1 AS x, 2 AS x'));
    }

    /**
     * `INSERT … RETURNING` through `fetchRaw` with `readonly = false` — the exact shape that made
     * this method necessary. It must return the row AND be declared a write.
     */
    public function testInsertReturningIsAWriteThatStillYieldsRows(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $c->exec('DROP TABLE IF EXISTS s8b_ret');
        $c->exec('CREATE TABLE s8b_ret (id serial primary key, note text)');

        $res = $c->fetchRaw('INSERT INTO s8b_ret (note) VALUES ($1) RETURNING id', ['hello'], false);
        self::assertCount(1, $res['rows']);
        self::assertIsInt($res['rows'][0][0]);

        $c->exec('DROP TABLE s8b_ret');
    }

    public function testPoolInfoAnswersForThisConnectionsPool(): void
    {
        $c = $this->connectConnection(RetryPolicy::none());
        $info = $c->poolInfo();
        self::assertNotNull($info, 'the default pool must be advertised');
        self::assertSame('default', $info->name);
        self::assertSame('postgres', $info->kind, 'the kind is inferred from the DSN scheme and is never nil');
    }

    /**
     * The selection half, live and with TWO real pools advertised: a connection bound to the MySQL
     * pool must get the MySQL entry, not simply the first one the handshake listed. `default` is
     * listed first ({@see LiveTestCase::poolNames}), so a `poolInfo()[0]` implementation passes the
     * test above and fails this one.
     */
    public function testPoolInfoPicksTheRightEntryWhenTwoPoolsAreAdvertised(): void
    {
        $mysqlPool = $this->requireMysqlPool();
        $c = $this->connectConnection(RetryPolicy::none(), $mysqlPool);

        $info = $c->poolInfo();
        self::assertNotNull($info);
        self::assertSame($mysqlPool, $info->name);
        self::assertSame('mysql', $info->kind);
    }
}
