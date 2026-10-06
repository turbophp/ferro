<?php // /php/laravel/tests/Live/SqliteReconnectLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Laravel\FerroConnections;
use Ferro\Laravel\FerroSQLiteConnection;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\DatabaseManager;

/**
 * C1e-2 on SQLite: `insertGetId()` across a `DB::reconnect()`.
 *
 * SQLite is where Illuminate reads a generated key through `getPdo()->lastInsertId()` (the BASE
 * `Processor::processInsertGetId`; PostgreSQL reads it out of `returning id` instead), so it is the
 * family on which the handle's remembered key is load-bearing. Before C1e-2 that key was read
 * through a closure bound to the connection that BUILT the shim — after a `DatabaseManager`
 * reconnect, the throwaway connection the manager discards — so every `insertGetId()` after a
 * reconnect read a key nothing would ever write.
 */
final class SqliteReconnectLiveTest extends SqliteLiveTestCase
{
    private DatabaseManager $db;

    protected function tearDown(): void
    {
        // Released before the parent stops ferrod, so an idle session does not hold the drain.
        unset($this->db);
        parent::tearDown();
    }

    private function managed(): FerroSQLiteConnection
    {
        FerroConnections::register();
        $capsule = new Capsule();
        $capsule->addConnection([
            'driver' => 'ferro-sqlite',
            'ferro_socket' => $this->socketPath,
            'pool' => self::SQLITE_POOL,
            'database' => 'ferro_sqlite_label',
        ], 'lite');
        $this->db = $capsule->getDatabaseManager();

        $conn = $this->db->connection('lite');
        self::assertInstanceOf(FerroSQLiteConnection::class, $conn,
            'the manager did not build a Ferro SQLite connection');
        $nonce = bin2hex(random_bytes(8));
        self::assertSame($nonce, $conn->select("select '{$nonce}' as nonce")[0]->nonce,
            'the contact probe did not round-trip through the managed connection');
        return $conn;
    }

    public function testInsertGetIdAfterAReconnectReturnsTheNewKey(): void
    {
        $conn = $this->managed();
        $conn->unprepared('drop table if exists c1e2_ids');
        $conn->unprepared('create table c1e2_ids (id integer primary key autoincrement, v text not null)');

        self::assertSame(1, (int) $conn->table('c1e2_ids')->insertGetId(['v' => 'before']));

        $this->db->reconnect('lite');

        self::assertSame(2, (int) $conn->table('c1e2_ids')->insertGetId(['v' => 'after']),
            'insertGetId() after a reconnect must return the key the new handle just generated');
    }

    /**
     * The key is scoped to the HANDLE, as PDO's is: a FRESH handle has no key from before it
     * existed, and it remembers its own. Read straight off `getPdo()` the way ecosystem code does.
     *
     * One divergence, deliberate and pre-existing: where `pdo_sqlite` answers `"0"` for a handle
     * that has generated nothing, this tier THROWS (see `FerroPdoShim::lastInsertId`), because
     * `"0"` is indistinguishable from a key.
     */
    public function testTheKeyBelongsToTheHandleNotTheConnection(): void
    {
        $conn = $this->managed();
        $conn->unprepared('drop table if exists c1e2_handle');
        $conn->unprepared('create table c1e2_handle (id integer primary key autoincrement, v text not null)');
        $conn->insert("insert into c1e2_handle (v) values ('a')");
        self::assertSame('1', $conn->getPdo()->lastInsertId());

        $this->db->reconnect('lite');
        try {
            $conn->getPdo()->lastInsertId();
            self::fail('a fresh handle must not answer a key generated through the old one');
        } catch (\LogicException $e) {
            self::assertStringContainsString('no statement on this connection has generated a key', $e->getMessage());
        }

        $conn->insert("insert into c1e2_handle (v) values ('b')");
        self::assertSame('2', $conn->getPdo()->lastInsertId());
    }
}
