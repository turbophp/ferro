<?php // /php/laravel/tests/Live/ReconnectLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Laravel\FerroConnections;
use Ferro\Laravel\FerroPostgresConnection;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\DatabaseManager;
use PHPUnit\Framework\Attributes\DataProvider;

/**
 * C1e-2: `DB::reconnect()`, `DB::disconnect()` and Illuminate's lost-connection reconnect, driven
 * through the framework's OWN `DatabaseManager` rather than by calling `setPdo()` by hand.
 *
 * **Why the manager and not a bare connection.** A connection built straight from the resolver has
 * no reconnector, so `reconnect()` throws `LostConnectionException` and none of this is reachable.
 * The manager installs one, and it is the reconnector that does the interesting thing: it builds a
 * WHOLE NEW connection and transplants that connection's `getRawPdo()` into the old one
 * (`DatabaseManager::refreshPdoConnections()`, verified in v11.51.0). The old connection object
 * lives on — it is the one the application holds — and only its PDO is replaced.
 *
 * So the property under test is that a Ferro connection's statements and its transactions keep
 * running on the SAME client after that transplant, which is what PDO gives for free: there, the
 * PDO object IS the session, so replacing it replaces both.
 */
final class ReconnectLiveTest extends LaravelLiveTestCase
{
    private DatabaseManager $db;

    protected function tearDown(): void
    {
        // Released before the parent stops ferrod, so an idle session does not hold the drain.
        unset($this->db);
        parent::tearDown();
    }

    /** A connection named `ferro` from a real `DatabaseManager`, with the contact assertion. */
    private function managed(): FerroPostgresConnection
    {
        FerroConnections::register();
        $capsule = new Capsule();
        $capsule->addConnection([
            'driver' => 'ferro-pgsql',
            'ferro_socket' => $this->socketPath,
            'pool' => 'default',
            'database' => 'ferro',
        ], 'ferro');
        $this->db = $capsule->getDatabaseManager();

        $conn = $this->db->connection('ferro');
        self::assertInstanceOf(FerroPostgresConnection::class, $conn,
            'the manager did not build a Ferro connection — the test would measure the wrong engine');

        $nonce = bin2hex(random_bytes(8));
        self::assertSame($nonce, $conn->select("select '{$nonce}' as nonce")[0]->nonce,
            'the contact probe did not round-trip through the managed connection');

        return $conn;
    }

    /** @return list<int> the ids an INDEPENDENT session sees committed */
    private function committed(string $table): array
    {
        $rows = $this->connection()->select("select id from {$table} order by id");
        return array_map(static fn (\stdClass $r): int => (int) $r->id, $rows);
    }

    private function table(FerroPostgresConnection $conn, string $table): void
    {
        $conn->unprepared("drop table if exists {$table}");
        $conn->unprepared("create table {$table} (id int primary key)");
    }

    /**
     * `DB::reconnect()`, then a transaction that throws. The insert must be rolled back.
     *
     * The failure this guards is SILENT and lands on the wrong side: if the transaction opens on
     * the transplanted client while the statement runs on the original one, the statement is an
     * autocommit write and the rollback rolls back an empty transaction — the row the application
     * was told was discarded is committed.
     */
    public function testAReconnectedConnectionStillRollsBackItsOwnWrites(): void
    {
        $t = 'c1e2_reconnect_rollback';
        $conn = $this->managed();
        $this->table($conn, $t);

        $this->db->reconnect('ferro');

        try {
            $conn->transaction(static function ($c) use ($t): void {
                $c->insert("insert into {$t} (id) values (1)");
                throw new RollbackSignal('roll me back');
            });
            self::fail('the closure throws, so transaction() must rethrow');
        } catch (RollbackSignal $e) {
            self::assertSame('roll me back', $e->getMessage());
        }

        self::assertSame([], $this->committed($t),
            'a rolled-back insert is visible to another session: the statement ran outside the transaction');
    }

    /**
     * The commit half of the same property, so a fix that merely stopped writing cannot pass the
     * rollback test above.
     */
    public function testAReconnectedConnectionStillCommitsItsOwnWrites(): void
    {
        $t = 'c1e2_reconnect_commit';
        $conn = $this->managed();
        $this->table($conn, $t);

        $this->db->reconnect('ferro');
        $conn->transaction(static function ($c) use ($t): void {
            $c->insert("insert into {$t} (id) values (1)");
        });

        self::assertSame([1], $this->committed($t));
    }

    /**
     * `DB::disconnect()` while a transaction is open, then a write that needs the abandoned
     * transaction's ROW LOCK — with and without a reconnect beforehand.
     *
     * Illuminate's `disconnect()` is `setPdo(null)`, which also zeroes its transaction counter. With
     * PDO the session goes with the PDO object, the server rolls the transaction back, and the lock
     * is free at once. The Ferro tier must match: the abandoned transaction's row is not committed,
     * the session that held it is RELEASED (not merely unreachable), and the next write autocommits
     * on a fresh session.
     *
     * Re-inserting the SAME key is what makes "released" observable: if the abandoned transaction is
     * still open somewhere, that insert waits on its lock until the engine's idle-in-transaction
     * deadline (10 s by default) tears it down. The review measured exactly that after a PRIOR
     * reconnect — the superseded client survived inside a throwaway connection's reference cycle —
     * while the no-reconnect case passed, which is why both rows run.
     */
    #[DataProvider('priorReconnect')]
    public function testADisconnectMidTransactionReleasesTheSession(bool $reconnectFirst): void
    {
        $t = 'c1e2_disconnect_mid_tx';
        $conn = $this->managed();
        $this->table($conn, $t);
        if ($reconnectFirst) {
            $this->db->reconnect('ferro');
        }

        $conn->beginTransaction();
        $conn->insert("insert into {$t} (id) values (1)");
        $held = \WeakReference::create($conn->getFerroConnection());
        $this->db->disconnect('ferro');
        self::assertSame(0, $conn->transactionLevel(), 'Illuminate zeroes its counter on setPdo()');
        self::assertNull($held->get(),
            'the client holding the abandoned transaction must be released by disconnect() itself, '
            . 'without waiting for the cycle collector');

        $started = microtime(true);
        $conn->insert("insert into {$t} (id) values (1)");
        $waited = microtime(true) - $started;

        self::assertLessThan(3.0, $waited, sprintf(
            'the same-key insert waited %.1f s — the abandoned transaction was still holding its lock',
            $waited,
        ));
        self::assertSame([1], $this->committed($t),
            'after a disconnect the next statement must autocommit on a fresh session');
    }

    /** @return iterable<string,array{bool}> */
    public static function priorReconnect(): iterable
    {
        yield 'no prior reconnect' => [false];
        yield 'after a prior DB::reconnect()' => [true];
    }

    /**
     * PostgreSQL's `insertGetId()` — and so every `Model::create()` — runs `insert … returning id`
     * through `select()`, not `insert()`. A reconnect must leave THAT path on the transaction's client
     * too. The review found a mutation pinning `select()` to the first client survived every other
     * test while leaving this row committed.
     */
    public function testInsertGetIdAfterAReconnectRollsBackWithItsTransaction(): void
    {
        $t = 'c1e2_reconnect_insert_get_id';
        $conn = $this->managed();
        $conn->unprepared("drop table if exists {$t}");
        $conn->unprepared("create table {$t} (id serial primary key, v text not null)");

        $this->db->reconnect('ferro');
        try {
            $conn->transaction(static function ($c) use ($t): void {
                self::assertSame(1, (int) $c->table($t)->insertGetId(['v' => 'discard me']));
                throw new RollbackSignal('roll me back');
            });
            self::fail('the closure throws, so transaction() must rethrow');
        } catch (RollbackSignal) {
        }

        self::assertSame([], $this->committed($t),
            'insertGetId() inside a rolled-back transaction committed: select() ran outside it');
    }

    /** `cursor()` after a reconnect reads INSIDE the transaction — it sees the transaction's own row. */
    public function testCursorAfterAReconnectReadsInsideTheTransaction(): void
    {
        $t = 'c1e2_reconnect_cursor';
        $conn = $this->managed();
        $this->table($conn, $t);

        $this->db->reconnect('ferro');
        try {
            $conn->transaction(static function ($c) use ($t): void {
                $c->insert("insert into {$t} (id) values (7)");
                $seen = array_map(static fn (\stdClass $r): int => (int) $r->id, iterator_to_array($c->cursor("select id from {$t}"), false));
                self::assertSame([7], $seen, 'cursor() did not see its own transaction\'s row');
                throw new RollbackSignal('roll me back');
            });
        } catch (RollbackSignal) {
        }
        self::assertSame([], $this->committed($t));
    }

    /** `update()` (`affectingStatement()`) after a reconnect rolls back with its transaction. */
    public function testUpdateAfterAReconnectRollsBackWithItsTransaction(): void
    {
        $t = 'c1e2_reconnect_update';
        $conn = $this->managed();
        $conn->unprepared("drop table if exists {$t}");
        $conn->unprepared("create table {$t} (id int primary key, n int not null)");
        $conn->insert("insert into {$t} (id, n) values (1, 0)");

        $this->db->reconnect('ferro');
        try {
            $conn->transaction(static function ($c) use ($t): void {
                self::assertSame(1, $c->update("update {$t} set n = 5 where id = 1"));
                throw new RollbackSignal('roll me back');
            });
        } catch (RollbackSignal) {
        }

        $n = (int) $this->connection()->select("select n from {$t} where id = 1")[0]->n;
        self::assertSame(0, $n, 'a rolled-back update was committed: affectingStatement() ran outside the transaction');
    }

    /**
     * After `DB::purge()`, the purged connection object and a NEW one from the manager are two
     * connections, and must be two SESSIONS. Illuminate reconnects the purged object by building a
     * fresh connection and transplanting ITS closure, so the same closure is resolved by both: a
     * closure that captured an already-dialled client made them share it, and the purged object's
     * `rollBack()` discarded the new object's autocommitted write (review finding F5). PDO dials per
     * resolution; so must this tier.
     */
    public function testPurgedAndNewConnectionsDoNotShareASession(): void
    {
        $t = 'c1e2_purge';
        $old = $this->managed();
        $this->table($old, $t);

        $this->db->purge('ferro');
        $old->select('select 1'); // reconnects the purged object
        $new = $this->db->connection('ferro');
        self::assertNotSame($old, $new);
        self::assertNotSame($old->getFerroConnection(), $new->getFerroConnection(),
            'two connection objects must not share one Ferro session');

        $old->beginTransaction();
        $old->insert("insert into {$t} (id) values (1)");
        $new->insert("insert into {$t} (id) values (2)");
        $old->rollBack();

        self::assertSame([2], $this->committed($t),
            'one connection\'s rollback must neither discard nor depend on the other\'s autocommitted write');
    }

    /**
     * C1e-3: a long-lived connection RECOVERS by itself after `ferrod` restarts — which is what an
     * Octane or queue worker depends on.
     *
     * Before C1e-3 every statement after the restart failed `Indeterminate` ("write failed after 0
     * of 47 bytes"), so Illuminate's lost-connection retry was — correctly — refused for each, and
     * the connection stayed broken until something called `DB::reconnect()`. Now the dead session's
     * write fails NOT SENT, the client reports `Retryable` connection-lost, the tier's type guard
     * lets Illuminate reconnect (dialling a fresh session) and re-run, and the statement succeeds —
     * once, because it never reached the engine the first time.
     */
    public function testALongLivedConnectionRecoversAfterAnEngineRestart(): void
    {
        $t = 'c1e3_engine_restart';
        $conn = $this->managed();
        $this->table($conn, $t);

        $this->restartFerrod();

        self::assertTrue($conn->insert("insert into {$t} (id) values (1)"),
            'the first write after an engine restart must be reconnected and re-run, not fail');
        self::assertSame(1, (int) $conn->select('select 1 as one')[0]->one);
        self::assertSame([1], $this->committed($t), 'the re-run write must be applied exactly once');
    }

    /**
     * C1e-3 review F5: the engine restarts WHILE A CURSOR IS STREAMING. The cursor must fail, and
     * the very next write must still recover — reconnected and re-run once.
     *
     * Reproduced live by the review before the fix: the transport failure left the session's
     * "stream open" guard set, so every later request was refused `ProtocolException` ("a stream is
     * open on this session") instead of "not sent", which nothing above can recognise as a dead
     * session — the connection never recovered. Poisoning now clears the guard.
     */
    public function testAConnectionRecoversAfterAnEngineRestartMidCursor(): void
    {
        $t = 'c1e3_restart_mid_cursor';
        $conn = $this->managed();
        $this->table($conn, $t);

        $seen = 0;
        $failed = null;
        try {
            foreach ($conn->cursor('select g from generate_series(1, 2000000) g') as $_) {
                if (++$seen === 10) {
                    $this->restartFerrod();
                }
            }
        } catch (\Throwable $e) {
            $failed = $e;
        }
        self::assertNotNull($failed, 'a cursor whose engine restarted under it must fail, not complete');

        self::assertTrue($conn->insert("insert into {$t} (id) values (1)"),
            'the first write after a restart mid-cursor must be reconnected and re-run');
        self::assertSame([1], $this->committed($t), 'applied exactly once');
    }

    /** The plain case: reconnect, then autocommit statements keep working and are visible. */
    public function testStatementsAfterAReconnectAreCommitted(): void
    {
        $t = 'c1e2_reconnect_autocommit';
        $conn = $this->managed();
        $this->table($conn, $t);

        $this->db->reconnect('ferro');
        $conn->insert("insert into {$t} (id) values (1)");
        $this->db->disconnect('ferro');
        $conn->insert("insert into {$t} (id) values (2)");

        self::assertSame([1, 2], $this->committed($t));
    }
}
