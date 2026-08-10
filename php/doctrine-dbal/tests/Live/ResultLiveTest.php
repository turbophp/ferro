<?php // /php/doctrine-dbal/tests/Live/ResultLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

/**
 * M1-S8b Task 8, live — the `Result` behaviours that only a real backend can show: the `affected`
 * count on a real write, the documented `rowCount()`-on-a-SELECT divergence, and the column names
 * as they actually come off the wire.
 *
 * The divergence is asserted per FAMILY, DERIVED from the pool set this harness itself configured
 * `ferrod` with (the DSN scheme is the engine's only source for `PoolSpec.kind`), so a run pointed
 * at different backends still carries the right expectation while a backend that changes its answer
 * fails here instead of being absorbed.
 *
 * **Which DBAL path reaches `Result` is load-bearing here, and it is not obvious.**
 * `Doctrine\DBAL\Connection::executeStatement()` with ZERO parameters calls the driver's `exec()`
 * (dbal 4.4.4 `src/Connection.php:891-911`), which returns the terminal's `affected` field directly
 * and never constructs a `Result` at all; only the PARAMETERISED form goes
 * `prepare()` → `execute()` → `Result::rowCount()`. MEASURED: with `rowCount()` mutated to
 * `count($this->rows)` a suite that asserts affected counts only through the zero-parameter form
 * stays entirely GREEN. Every write assertion below therefore states which of the two paths it is
 * on, and the parameterised one carries the load.
 */
final class ResultLiveTest extends DbalLiveTestCase
{
    public function testAffectedCountsComeFromTheTerminalOnBothFamilies(): void
    {
        foreach ($this->families() as $kind => $pool) {
            $c = $this->dbal($pool);
            self::assertSame(
                $kind,
                $c->getNativeConnection()->poolInfo()?->kind,
                "[$kind] the engine agrees with the harness about this pool's family",
            );

            $c->executeStatement('DROP TABLE IF EXISTS s8b_res');
            $c->executeStatement(
                $kind === 'postgres'
                    ? 'CREATE TABLE s8b_res (id int primary key, n int)'
                    : 'CREATE TABLE s8b_res (id INT PRIMARY KEY, n INT)',
            );

            // ── the ZERO-PARAMETER path: Connection::exec(), the terminal field, no Result ──
            self::assertSame(
                3,
                $c->executeStatement('INSERT INTO s8b_res (id, n) VALUES (1,1),(2,1),(3,2)'),
                "[$kind] exec(): the terminal's affected count",
            );
            self::assertSame(
                1,
                $c->executeStatement('UPDATE s8b_res SET n = 4 WHERE n = 2'),
                "[$kind] exec(): an UPDATE that changes one row",
            );

            // ── the PARAMETERISED path: prepare()/execute() → Result::rowCount() ──
            // Each of these returns ZERO rows while affecting a NON-ZERO number of them, so
            // count($rows) cannot pass for affected.
            self::assertSame(
                2,
                $c->executeStatement('UPDATE s8b_res SET n = 9 WHERE n = ?', [1]),
                "[$kind] Result::rowCount(): an UPDATE affecting 2 rows and returning none",
            );
            self::assertSame(
                1,
                $c->executeStatement('DELETE FROM s8b_res WHERE id = ?', [3]),
                "[$kind] Result::rowCount(): a DELETE affecting 1 row and returning none",
            );
            self::assertSame(
                0,
                $c->executeStatement('DELETE FROM s8b_res WHERE id = ?', [99]),
                "[$kind] Result::rowCount(): a no-op DELETE affects nothing",
            );
            self::assertSame(
                [[1, 9], [2, 9]],
                $c->fetchAllNumeric('SELECT id, n FROM s8b_res ORDER BY id'),
                "[$kind] and the writes really landed",
            );

            $c->executeStatement('DROP TABLE s8b_res');
        }
    }

    /**
     * `rowCount()` after a SELECT: DBAL documents it as driver-specific, and ours diverges on TWO
     * axes, both pinned here so a silent change is caught.
     *
     * By FAMILY: PostgreSQL's command tag carries the row count, MySQL's carries 0.
     *
     * By ROUTE, **since Task 12**: the zero-parameter `executeQuery()` reaches
     * `Connection::query()`, which STREAMS on PostgreSQL — and a `HEAD`/`DATA`/`END` producer has no
     * `affected` field at all, so a streamed result reports **0**. The parameterised form reaches
     * `Statement::execute()` → `runPrepared()`, which buffers and still reports PostgreSQL's count.
     * That asymmetry is the price of §14's never-buffer requirement (adding `affected` to the stream
     * terminal is a `/proto` change and is deferred), it is why the PREPARED path deliberately does
     * NOT stream — `executeStatement()` RETURNS this number — and it is a real drop-in difference
     * that belongs in `docs/known-incompatibilities.md`.
     *
     * Asserted per route rather than collapsed, so the pair also proves the streaming fork itself:
     * a driver that stopped streaming would make the two PostgreSQL numbers equal again.
     */
    public function testRowCountAfterASelectIsTheDocumentedPerFamilyValue(): void
    {
        foreach ($this->families() as $kind => $pool) {
            $c = $this->dbal($pool);
            $c->executeStatement('DROP TABLE IF EXISTS s8b_res2');
            $c->executeStatement(
                $kind === 'postgres'
                    ? 'CREATE TABLE s8b_res2 (id int primary key)'
                    : 'CREATE TABLE s8b_res2 (id INT PRIMARY KEY)',
            );
            $c->executeStatement('INSERT INTO s8b_res2 (id) VALUES (1),(2),(3)');

            $result = $c->executeQuery('SELECT id FROM s8b_res2');
            self::assertCount(3, $result->fetchAllNumeric(), "[$kind] the rows are all there");
            self::assertSame(
                0,
                $result->rowCount(),
                "[$kind] a zero-parameter SELECT streams on PostgreSQL and buffers on MySQL, and "
                . 'BOTH report 0 — the streamed terminal carries no affected field, and MySQL never '
                . 'reports one for a SELECT',
            );

            $paramd = $c->executeQuery('SELECT id FROM s8b_res2 WHERE id > ?', [1]);
            self::assertSame([[2], [3]], $paramd->fetchAllNumeric(), "[$kind] the parameterised rows");
            self::assertSame(
                $kind === 'postgres' ? 2 : 0,
                $paramd->rowCount(),
                "[$kind] the PREPARED route buffers on both families, so PostgreSQL's count comes "
                . 'back here — the route divergence, not just the family one',
            );

            $c->executeStatement('DROP TABLE s8b_res2');
        }
    }

    /**
     * The column header, end to end. `getColumnName()` is asserted through
     * `Doctrine\DBAL\Result`'s own `method_exists` guard — the vantage point that makes it
     * effectively mandatory — and the DUPLICATE-name case proves the rows really do arrive
     * POSITIONALLY: `fetchNumeric()` keeps both cells while `fetchAssociative()` collapses to one,
     * which is impossible for a driver that rebuilt the numeric shape out of an associative row.
     */
    public function testColumnNamesComeOffTheWireIncludingDuplicates(): void
    {
        foreach ($this->families() as $kind => $pool) {
            $c = $this->dbal($pool);

            $r = $c->executeQuery('SELECT 11 AS a, 22 AS b');
            self::assertSame(2, $r->columnCount(), "[$kind] columnCount");
            self::assertSame('a', $r->getColumnName(0), "[$kind] first column name");
            self::assertSame('b', $r->getColumnName(1), "[$kind] second column name");

            $dup = $c->executeQuery('SELECT 11 AS a, 22 AS a');
            self::assertSame([[11, 22]], $dup->fetchAllNumeric(), "[$kind] duplicates survive numerically");
            self::assertSame(
                [['a' => 22]],
                $c->executeQuery('SELECT 11 AS a, 22 AS a')->fetchAllAssociative(),
                "[$kind] and collapse associatively, the last winning",
            );
        }
    }

    /**
     * @return array<string,string> kind => pool name, derived from the DSNs this harness handed
     *                              `ferrod` rather than hard-coded per test
     */
    private function families(): array
    {
        $this->requireMysqlPool(); // a run with no MySQL pool SKIPS — fatal under --fail-on-skipped
        $map = array_combine($this->launchedPoolKinds(), $this->launchedPools());
        self::assertSame(
            ['postgres', 'mysql'],
            array_keys($map),
            'this run must configure exactly one pool of each family',
        );
        return $map;
    }
}
