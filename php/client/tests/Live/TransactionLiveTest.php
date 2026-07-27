<?php // /php/client/tests/Live/TransactionLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\TxHandle;

/**
 * End-to-end `transaction(closure)` against a real `ferrod` + Docker Postgres: a committed closure
 * persists (a fresh autocommit SELECT sees the row), and a closure that throws rolls back (the row is
 * absent). Uses a REAL table (not a temp table): the pool is transaction-mode, so a committed row is
 * the only thing guaranteed visible to a subsequent autocommit checkout.
 */
final class TransactionLiveTest extends LiveTestCase
{
    private static function uniqueTable(): string
    {
        return 'ferro_s7_tx_' . getmypid() . '_' . bin2hex(random_bytes(4));
    }

    public function testCommitPersists(): void
    {
        $conn = $this->connectConnection();
        $table = self::uniqueTable();
        try {
            // bigint id: M0 binds canonical I64 → PG int8 directly (a narrower int4 needs a cast).
            $conn->exec("CREATE TABLE {$table} (id bigint PRIMARY KEY, note text)");

            $affected = $conn->transaction(function (TxHandle $tx) use ($table): int {
                return $tx->exec("INSERT INTO {$table} (id, note) VALUES (?, ?)", [1, 'hello']);
            });
            $this->assertSame(1, $affected, 'the tx-scoped INSERT affected one row');

            // A fresh autocommit read (possibly a different pooled backend conn) sees the COMMITted row.
            $this->assertSame(1, $conn->scalar("SELECT count(*) FROM {$table}"));
            $this->assertSame('hello', $conn->scalar("SELECT note FROM {$table} WHERE id = 1"));
        } finally {
            self::cleanup($conn, $table);
        }
    }

    public function testThrownClosureRollsBack(): void
    {
        $conn = $this->connectConnection();
        $table = self::uniqueTable();
        try {
            $conn->exec("CREATE TABLE {$table} (id int PRIMARY KEY)");

            try {
                $conn->transaction(function (TxHandle $tx) use ($table): void {
                    $tx->exec("INSERT INTO {$table} (id) VALUES (1)");
                    throw new \RuntimeException('abort the transaction');
                });
                $this->fail('expected the closure exception to propagate');
            } catch (\RuntimeException $e) {
                $this->assertSame('abort the transaction', $e->getMessage());
            }

            // The INSERT was rolled back — the fresh autocommit read sees nothing.
            $this->assertSame(0, $conn->scalar("SELECT count(*) FROM {$table}"));
        } finally {
            self::cleanup($conn, $table);
        }
    }

    private static function cleanup(Connection $conn, string $table): void
    {
        try {
            $conn->exec("DROP TABLE IF EXISTS {$table}");
        } catch (\Throwable) {
            // best-effort — the ferrod teardown drops the whole process anyway.
        }
        $conn->session()->close();
    }
}
