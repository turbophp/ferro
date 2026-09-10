<?php // /php/laravel/tests/Live/TransactionLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

/**
 * C1c: transactions, driven entirely by Illuminate's OWN `ManagesTransactions` over the
 * {@see \Ferro\Laravel\FerroPdoShim} seam. The point of these tests is that the framework's
 * bookkeeping — its counter, its savepoint naming, its retry loop — is inherited unchanged, so what
 * is being proven is that the seam is faithful, not that a reimplementation is correct.
 */
final class TransactionLiveTest extends LaravelLiveTestCase
{
    private function fresh(string $table): \Ferro\Laravel\FerroPostgresConnection
    {
        $conn = $this->connection();
        $conn->unprepared("drop table if exists {$table}");
        $conn->unprepared("create table {$table} (id int primary key, n int not null default 0)");
        return $conn;
    }

    public function testCommitPersists(): void
    {
        $t = 'c1c_tx_commit';
        $conn = $this->fresh($t);

        $conn->transaction(static function ($c) use ($t): void {
            $c->statement("insert into {$t} (id, n) values (1, 10)");
        });

        self::assertSame(10, $conn->select("select n from {$t} where id = 1")[0]->n);
    }

    public function testRollbackDiscards(): void
    {
        $t = 'c1c_tx_rollback';
        $conn = $this->fresh($t);

        try {
            $conn->transaction(static function ($c) use ($t): void {
                $c->statement("insert into {$t} (id, n) values (1, 10)");
                throw new \RuntimeException('caller aborted');
            });
            self::fail('the closure threw; transaction() must propagate');
        } catch (\RuntimeException $e) {
            self::assertSame('caller aborted', $e->getMessage());
        }

        self::assertSame([], $conn->select("select id from {$t}"), 'the insert must not have persisted');
        self::assertSame(0, $conn->transactionLevel(), 'the transaction counter must unwind');
    }

    /**
     * NESTED transactions are savepoints, compiled by the STOCK grammar and executed through the
     * shim's `exec()` — the SPEC §22.2 (r) passthrough. The inner rollback must discard only the
     * inner write.
     */
    public function testANestedRollbackDiscardsOnlyTheInnerWrite(): void
    {
        $t = 'c1c_tx_nested';
        $conn = $this->fresh($t);

        $conn->transaction(static function ($c) use ($t): void {
            $c->statement("insert into {$t} (id, n) values (1, 1)");
            try {
                $c->transaction(static function ($c2) use ($t): void {
                    $c2->statement("insert into {$t} (id, n) values (2, 2)");
                    throw new \RuntimeException('inner aborted');
                });
            } catch (\RuntimeException) {
                // swallowed on purpose: the OUTER transaction continues and must still commit.
            }
        });

        $ids = array_map(static fn (\stdClass $r): int => $r->id, $conn->select("select id from {$t} order by id"));
        self::assertSame([1], $ids, 'the savepoint rollback must discard only the inner insert');
    }

    public function testManualBeginCommitRoundTrips(): void
    {
        $t = 'c1c_tx_manual';
        $conn = $this->fresh($t);

        $conn->beginTransaction();
        self::assertSame(1, $conn->transactionLevel());
        $conn->statement("insert into {$t} (id, n) values (5, 50)");
        $conn->commit();

        self::assertSame(0, $conn->transactionLevel());
        self::assertSame(50, $conn->select("select n from {$t} where id = 5")[0]->n);
    }
}
