<?php // /php/laravel/tests/Live/MySqlConnectionLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Laravel\FerroMySqlConnection;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Database\QueryException;
use Illuminate\Database\UniqueConstraintViolationException;
use Illuminate\Contracts\Events\Dispatcher;

/**
 * M2-C1f: the three things the MySQL family needed beyond the shared execution layer, each one
 * found by the framework suite and each pinned here against a real MySQL/MariaDB so that the
 * suite is not the only thing that would notice it breaking.
 */
final class MySqlConnectionLiveTest extends MySqlLiveTestCase
{
    private ?FerroMySqlConnection $conn = null;

    protected function tearDown(): void
    {
        if ($this->conn !== null) {
            foreach (['c1f_ai', 'c1f_plain', 'c1f_uniq', 'c1f_ts'] as $t) {
                $this->conn->statement("drop table if exists {$t}");
            }
        }
        parent::tearDown();
    }

    private function conn(): FerroMySqlConnection
    {
        return $this->conn ??= $this->mysqlConnection();
    }

    /**
     * Review F1: on MySQL `getPdo()->lastInsertId()` follows `pdo_mysql` — the statement just run,
     * `"0"` after a SELECT, an UPDATE or a key-less insert — not the handle-sticky key SQLite's PDO
     * keeps (which would answer a STALE key here).
     */
    public function testThePdoLastInsertIdIsPerStatement(): void
    {
        $c = $this->conn();
        $c->statement('drop table if exists c1f_ai');
        $c->statement('drop table if exists c1f_plain');
        $c->statement('create table c1f_ai (id int auto_increment primary key, v varchar(10))');
        $c->statement('create table c1f_plain (v varchar(10))');

        $c->table('c1f_ai')->insert(['v' => 'a']);
        self::assertSame('1', $c->getPdo()->lastInsertId());
        $c->select('select 1');
        self::assertSame('0', $c->getPdo()->lastInsertId(), 'after a SELECT pdo_mysql answers "0"');
        $c->table('c1f_ai')->insert(['v' => 'b']);
        $c->table('c1f_plain')->insert(['v' => 'x']);
        self::assertSame('0', $c->getPdo()->lastInsertId(), 'after a key-less insert pdo_mysql answers "0"');
    }

    /**
     * `DB::pretend()` must log an insert, never execute it — the guard inside insert()'s callback.
     *
     * The binding is an INTEGER because this test predates C1g: pretend mode logs the query with
     * its bindings substituted (`substituteBindingsIntoRawSql` → `escape()`), and a STRING binding
     * reaches `quote()`, which was refused on a MySQL pool until C1g. The string-binding case is
     * `MySqlEscapeLiveTest::testPretendWithAStringBindingLogsTheRenderedStatement`.
     */
    public function testAnInsertUnderPretendIsNotExecuted(): void
    {
        $c = $this->conn();
        $c->statement('drop table if exists c1f_ai');
        $c->statement('create table c1f_ai (id int auto_increment primary key, n int)');
        $log = $c->pretend(function () use ($c): void {
            $c->table('c1f_ai')->insert(['n' => 5]);
        });
        self::assertCount(1, $log, 'the insert was logged');
        self::assertSame(0, $c->table('c1f_ai')->count(), 'and not executed');
    }

    /** `isMaria()` reads the version through the shim, which must pass `-MariaDB` through. */
    public function testIsMariaAgreesWithTheServer(): void
    {
        $c = $this->conn();
        $comment = (string) $c->select('select @@version_comment as c')[0]->c;
        $version = (string) $c->select('select version() as v')[0]->v;
        self::assertSame(
            stripos($comment . ' ' . $version, 'mariadb') !== false,
            $c->isMaria(),
            "server says '{$version}' / '{$comment}'",
        );
    }

    /**
     * Stock `MySqlConnection::insert()` executes through `getPdo()->prepare()`, which the shim
     * refuses — 443 of the first measured column's 456 errors. And `pdo_mysql`'s `lastInsertId()` is
     * the STATEMENT's key, `"0"` when it generated none (measured), which `insertGetId()` returns.
     */
    public function testInsertGetIdReturnsTheKeyAndAKeylessInsertReportsZero(): void
    {
        $c = $this->conn();
        $c->statement('drop table if exists c1f_ai');
        $c->statement('drop table if exists c1f_plain');
        $c->statement('create table c1f_ai (id int auto_increment primary key, v varchar(10))');
        $c->statement('create table c1f_plain (v varchar(10))');

        self::assertSame(1, $c->table('c1f_ai')->insertGetId(['v' => 'a']));
        self::assertSame(2, $c->table('c1f_ai')->insertGetId(['v' => 'b']));

        self::assertTrue($c->table('c1f_plain')->insert(['v' => 'x']));
        self::assertSame('0', $c->getLastInsertId(), 'pdo_mysql answers "0" for a key-less insert');
    }

    /**
     * **The key is read INSIDE `run()`, before `QueryExecuted` fires.** A listener that runs a query
     * clears the client's per-statement key; reading it after `run()` returns would hand
     * `insertGetId()` a `0`. Moving the read out of the callback fails this test.
     */
    public function testTheKeyIsReadBeforeAQueryExecutedListenerRuns(): void
    {
        $c = $this->conn();
        $c->statement('drop table if exists c1f_ai');
        $c->statement('create table c1f_ai (id int auto_increment primary key, v varchar(10))');
        $c->table('c1f_ai')->insert(['v' => 'a']);

        // A minimal dispatcher: this package does not depend on illuminate/events, and the only
        // behaviour needed is Connection::event() reaching the listener registered below.
        $c->setEventDispatcher(new class implements Dispatcher {
            /** @var list<callable> */
            private array $listeners = [];
            public function listen($events, $listener = null): void { $this->listeners[] = $listener; }
            public function hasListeners($eventName): bool { return $this->listeners !== []; }
            public function subscribe($subscriber): void {}
            public function until($event, $payload = []): mixed { return null; }
            public function dispatch($event, $payload = [], $halt = false): mixed
            {
                foreach ($this->listeners as $l) {
                    $l($event);
                }
                return null;
            }
            public function push($event, $payload = []): void {}
            public function flush($event): void {}
            public function forget($event): void {}
            public function forgetPushed(): void {}
        });
        $ran = 0;
        $c->listen(function (QueryExecuted $e) use ($c, &$ran): void {
            if ($ran++ === 0) {
                $c->select('select count(*) as n from c1f_ai');
            }
        });

        self::assertSame(2, $c->table('c1f_ai')->insertGetId(['v' => 'b']));
        self::assertGreaterThan(0, $ran, 'the listener never ran, so this proved nothing');
    }

    /**
     * Stock detection matches `pdo_mysql`'s wording (`Integrity constraint violation: 1062`), which a
     * Ferro error does not carry, so `createOrFirst()` re-threw the duplicate it exists to catch — 8
     * suite failures. The CONTROL: a different integrity error (a NOT NULL violation) stays a plain
     * QueryException.
     */
    public function testADuplicateKeyIsAUniqueConstraintViolationAndNothingElseIs(): void
    {
        $c = $this->conn();
        $c->statement('drop table if exists c1f_uniq');
        $c->statement('create table c1f_uniq (id int primary key, name varchar(20) not null unique)');
        $c->table('c1f_uniq')->insert(['id' => 1, 'name' => 'a']);

        try {
            $c->table('c1f_uniq')->insert(['id' => 2, 'name' => 'a']);
            self::fail('the duplicate must be refused');
        } catch (UniqueConstraintViolationException) {
        }

        try {
            $c->insert('insert into c1f_uniq (id, name) values (?, ?)', [3, null]);
            self::fail('the NULL must be refused');
        } catch (UniqueConstraintViolationException $e) {
            self::fail('a NOT NULL violation is not a unique violation: ' . $e->getMessage());
        } catch (QueryException) {
        }

        $existing = $c->table('c1f_uniq')->where('name', 'a')->first();
        self::assertNotNull($existing);
        self::assertSame(1, (int) $existing->id, 'the duplicate was never written');
    }

    /**
     * A `TIMESTAMP` column — what `$table->timestamps()` creates — reads back as the naive UTC wall
     * clock `pdo_mysql` returns from a `+00:00` session, byte-for-byte what Eloquent wrote. With the
     * raw RFC3339 form (`…T…Z`) Illuminate would read a UTC INSTANT where it wrote a wall clock.
     */
    public function testATimestampRoundTripsAsTheStringEloquentWrote(): void
    {
        $c = $this->conn();
        $c->statement('drop table if exists c1f_ts');
        $c->statement('create table c1f_ts (id int primary key, at timestamp null, at6 timestamp(6) null, dt datetime null)');
        $c->table('c1f_ts')->insert([
            'id' => 1,
            'at' => '2017-11-12 13:14:15',
            'at6' => '2017-11-12 13:14:15.250000',
            'dt' => '2017-11-12 13:14:15',
        ]);

        $row = $c->table('c1f_ts')->where('id', 1)->first();
        self::assertNotNull($row);
        self::assertSame('2017-11-12 13:14:15', $row->at);
        self::assertSame('2017-11-12 13:14:15.250000', $row->at6);
        self::assertSame('2017-11-12 13:14:15', $row->dt, 'DATETIME (naive TIMESTAMP tag) is unchanged — the control');
    }

    /**
     * PINNED DIVERGENCE, not a pass: a FRACTIONAL column's rendering is the canonical wire text's
     * (no fraction when it is zero, otherwise exactly six digits — PROTOCOL.md §3.2), not
     * `pdo_mysql`'s, which pads to the column's own precision. The test above asserts only the two
     * points where the forms coincide (`TIMESTAMP(0)`, and a six-digit fraction in `TIMESTAMP(6)`),
     * which is how the first version of this slice came to call the rendering "byte-for-byte"
     * (M2-C1f review). The instant is the same in every row; only the raw string differs. If this
     * starts failing because the strings now match `pdo_mysql`, update SPEC §22.2 (cb) and the
     * incompatibilities page — it means the precision reached the wire.
     */
    public function testAFractionalTimestampRendersTheCanonicalFractionNotTheColumnsPrecision(): void
    {
        $c = $this->conn();
        $c->statement('drop table if exists c1f_fsp');
        $c->statement('create table c1f_fsp (id int primary key, at3 timestamp(3) null, at6 timestamp(6) null)');
        $c->table('c1f_fsp')->insert([
            ['id' => 1, 'at3' => '2017-11-12 13:14:15.250', 'at6' => '2017-11-12 13:14:15.000000'],
        ]);
        $row = $c->table('c1f_fsp')->where('id', 1)->first();
        self::assertNotNull($row);
        // pdo_mysql: '2017-11-12 13:14:15.250' and '2017-11-12 13:14:15.000000'.
        self::assertSame('2017-11-12 13:14:15.250000', $row->at3, 'TIMESTAMP(3): six digits, not the column\'s three');
        self::assertSame('2017-11-12 13:14:15', $row->at6, 'TIMESTAMP(6) at a whole second: no fraction at all');
        $c->statement('drop table c1f_fsp');
    }
}
