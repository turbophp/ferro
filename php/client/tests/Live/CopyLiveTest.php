<?php // /php/client/tests/Live/CopyLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Pg\Copy;
use Ferro\Protocol\Generated\Constants as C;

/**
 * `Ferro\Pg\Copy` against a real `ferrod` + PostgreSQL (M3-D4). The PHP-side companion of the Rust
 * `copy_it` gate: what only the CLIENT can show — that neither direction buffers in PHP memory, that
 * abandoning a COPY leaves the session usable, and the fates as the application sees them.
 *
 * The memory bounds are measured against data that a buffering client could not hold under them: a
 * 1 000 000-row COPY is ~25 MiB of COPY text, generated lazily, against an 8 MiB ceiling — a client
 * that collected the iterable, or an export, before sending/yielding would exceed it however fast it
 * ran. The abandonment probe exports an effectively ENDLESS result (10^12 rows), so an abandonment that did not
 * CANCEL could never finish draining it: the test cannot pass by being slow.
 */
final class CopyLiveTest extends LiveTestCase
{
    private const MEMORY_CEILING_BYTES = 8 * 1024 * 1024;

    private function conn(): Connection
    {
        return $this->connectConnection();
    }

    /** 1 000 000 rows of COPY text, lazily, with the format's specials inside the values. */
    private static function rows(int $n): \Generator
    {
        for ($i = 1; $i <= $n; $i++) {
            yield Copy::textRow([$i, "name\t{$i}\\x\n", $i % 7 === 0 ? null : ($i % 2 === 0)]);
        }
    }

    public function testAMillionRowsRoundTripByteIdenticalWithBoundedMemory(): void
    {
        $conn = $this->conn();
        $conn->exec('DROP TABLE IF EXISTS d4_php_rt');
        $conn->exec('CREATE TABLE d4_php_rt (id int PRIMARY KEY, name text, flag bool)');
        $copy = new Copy($conn);
        $this->assertSame(1, $conn->scalar('SELECT 1')); // warm up the session machinery

        // The expected bytes, hashed while they are generated — never held.
        $expected = hash_init('sha256');
        $source = (static function () use ($expected): \Generator {
            foreach (self::rows(1_000_000) as $line) {
                hash_update($expected, $line);
                yield $line;
            }
        })();

        gc_collect_cycles();
        $base = memory_get_usage(true);
        $peakBefore = memory_get_peak_usage(true);
        $n = $copy->in('COPY d4_php_rt (id, name, flag) FROM STDIN', $source);
        $inPeak = memory_get_peak_usage(true) - max($peakBefore, $base);
        $this->assertSame(1_000_000, $n);
        $this->assertLessThan(self::MEMORY_CEILING_BYTES, $inPeak,
            "copyIn buffered: peak grew by {$inPeak} bytes for ~25 MiB of COPY data");

        $got = hash_init('sha256');
        $bytes = 0;
        $chunks = 0;
        $out = $copy->out('COPY (SELECT id, name, flag FROM d4_php_rt ORDER BY id) TO STDOUT', readonly: true);
        foreach ($out as $chunk) {
            hash_update($got, $chunk);
            $bytes += strlen($chunk);
            $chunks++;
        }
        $outPeak = memory_get_peak_usage(true) - max($peakBefore, $base);
        $this->assertSame(1_000_000, $out->getReturn(), 'getReturn() is the exported row count');
        $this->assertGreaterThan(20 * 1024 * 1024, $bytes, 'the export is larger than the ceiling');
        $this->assertGreaterThan(1, $chunks);
        $this->assertSame(hash_final($expected), hash_final($got), 'byte-identical round trip');
        $this->assertLessThan(self::MEMORY_CEILING_BYTES, $outPeak, "copyOut buffered: {$outPeak} bytes");
        $this->assertSame(1, $conn->scalar('SELECT 1'));
    }

    public function testAbandoningAnEndlessExportCancelsItAndTheSessionGoesOn(): void
    {
        $conn = $this->conn();
        $out = (new Copy($conn))->out(
            // A set-returning function in the SELECT LIST is evaluated per row (ProjectSet), never
            // materialised — `FROM generate_series(…)` would spool all of it to a temp file first.
            'COPY (SELECT generate_series(1, 1000000000000)) TO STDOUT',
            readonly: true,
        );
        $seen = 0;
        foreach ($out as $chunk) {
            $seen += strlen($chunk);
            if ($seen > 1024 * 1024) {
                break; // abandon: the generator's finally CANCELs and drains to the terminal
            }
        }
        unset($out);
        $this->assertSame(42, $conn->scalar('SELECT 42'), 'the next query on the session works');
    }

    public function testAnIterableThatThrowsAbandonsTheCopyAndAppliesNothing(): void
    {
        $conn = $this->conn();
        $conn->exec('DROP TABLE IF EXISTS d4_php_throw');
        $conn->exec('CREATE TABLE d4_php_throw (id int)');
        $source = (static function (): \Generator {
            for ($i = 0; $i < 100_000; $i++) {
                yield "{$i}\n";
            }
            throw new \RuntimeException('the producer failed');
        })();
        try {
            $conn->copyIn('COPY d4_php_throw FROM STDIN', $source);
            $this->fail('the producer\'s exception must propagate');
        } catch (\RuntimeException $e) {
            $this->assertSame('the producer failed', $e->getMessage(), 'the caller\'s own error, not a wire one');
        }
        $this->assertSame(0, $conn->scalar('SELECT count(*)::int FROM d4_php_throw'), 'nothing applied');
        $this->assertSame(1, $conn->copyIn('COPY d4_php_throw FROM STDIN', ["7\n"]), 'the session goes on');
    }

    public function testAMalformedRowIsTheServersKnownErrorAndTheConnectionIsRecycledNotRedialled(): void
    {
        $conn = $this->conn();
        $conn->exec('DROP TABLE IF EXISTS d4_php_bad');
        $conn->exec('CREATE TABLE d4_php_bad (id int)');
        $pid = $conn->scalar('SELECT pg_backend_pid()');
        try {
            $conn->copyIn('COPY d4_php_bad FROM STDIN', ["1\n", "2\n", "not-an-int\n", "3\n"]);
            $this->fail('a malformed row must fail the COPY');
        } catch (NonRetryableException $e) {
            $this->assertSame('22P02', $e->errorPayload()->sqlstate);
        }
        // A SUCCEEDING statement between the failure and the probe, so the probe really is served
        // the recycled connection (a failed last statement can turn a recycle test into a
        // fresh-dial one — the C3-4 lesson).
        $this->assertSame(2, $conn->copyIn('COPY d4_php_bad FROM STDIN', ["8\n9\n"]));
        $this->assertSame($pid, $conn->scalar('SELECT pg_backend_pid()'), 'the same backend connection: recycled');
        $this->assertSame(2, $conn->scalar('SELECT count(*)::int FROM d4_php_bad'), 'the failed COPY applied nothing');
    }

    public function testCopyInsideATransactionRollsBackAndCommits(): void
    {
        $conn = $this->conn();
        $conn->exec('DROP TABLE IF EXISTS d4_php_tx');
        $conn->exec('CREATE TABLE d4_php_tx (id int)');
        $conn->begin();
        $this->assertSame(1000, $conn->copyIn('COPY d4_php_tx FROM STDIN', self::ints(1000)));
        $seen = 0;
        foreach ($conn->copyOut('COPY d4_php_tx TO STDOUT', readonly: true) as $chunk) {
            $seen += substr_count($chunk, "\n");
        }
        $this->assertSame(1000, $seen, 'the transaction sees its own uncommitted COPY');
        $conn->rollBack();
        $this->assertSame(0, $conn->scalar('SELECT count(*)::int FROM d4_php_tx'), 'rolled back');

        $n = $conn->transaction(static fn ($tx): int => (new Copy($tx))->in('COPY d4_php_tx FROM STDIN', self::ints(10)));
        $this->assertSame(10, $n);
        $this->assertSame(10, $conn->scalar('SELECT count(*)::int FROM d4_php_tx'), 'committed');
    }

    /** @return \Generator<int, string> */
    private static function ints(int $n): \Generator
    {
        for ($i = 0; $i < $n; $i++) {
            yield "{$i}\n";
        }
    }

    public function testACommitTimeFailureIsReportedNotSwallowed(): void
    {
        $conn = $this->conn();
        $conn->exec('DROP TABLE IF EXISTS d4_php_child');
        $conn->exec('DROP TABLE IF EXISTS d4_php_parent');
        $conn->exec('CREATE TABLE d4_php_parent (id int PRIMARY KEY)');
        $conn->exec('CREATE TABLE d4_php_child (id int, p int REFERENCES d4_php_parent(id) DEFERRABLE INITIALLY DEFERRED)');
        try {
            $conn->copyIn('COPY d4_php_child FROM STDIN', ["1\t999\n"]);
            $this->fail('a COPY whose implicit COMMIT failed must not report success');
        } catch (NonRetryableException $e) {
            $this->assertSame('23503', $e->errorPayload()->sqlstate);
        }
        $this->assertSame(0, $conn->scalar('SELECT count(*)::int FROM d4_php_child'));
    }

    public function testTheShapeGuardRefusesANonCopyBeforeItRuns(): void
    {
        $conn = $this->conn();
        $conn->exec('DROP TABLE IF EXISTS d4_php_guard');
        $conn->exec('CREATE TABLE d4_php_guard (id int)');
        $conn->exec('INSERT INTO d4_php_guard VALUES (1)');
        try {
            $conn->copyIn('DELETE FROM d4_php_guard', ["x\n"]);
            $this->fail('a DELETE is not a COPY');
        } catch (NonRetryableException $e) {
            $this->assertSame(C::ERR_UNSUPPORTED, $e->errorPayload()->code);
        }
        $this->assertSame(1, $conn->scalar('SELECT count(*)::int FROM d4_php_guard'), 'the DELETE never ran');
        try {
            $conn->exec('COPY d4_php_guard FROM STDIN');
            $this->fail('EXEC cannot carry COPY');
        } catch (NonRetryableException $e) {
            $this->assertSame(C::ERR_UNSUPPORTED, $e->errorPayload()->code);
            $this->assertStringContainsString('COPY_IN', $e->getMessage());
        }
    }

    public function testCopyOnAMysqlPoolIsUnsupported(): void
    {
        $pool = $this->requireMysqlPool();
        $conn = $this->connectConnection(null, $pool);
        try {
            $conn->copyIn('COPY t FROM STDIN', ["1\n"]);
            $this->fail('COPY is PostgreSQL-only');
        } catch (NonRetryableException $e) {
            $this->assertSame(C::ERR_UNSUPPORTED, $e->errorPayload()->code);
        }
        $this->assertSame(1, $conn->scalar('SELECT 1'), 'the session goes on');
    }

    /**
     * D1a multiplexing: a query submitted before a COPY is still served while the COPY runs, and its
     * result is waiting for it afterwards.
     */
    public function testARequestInFlightBeforeTheCopyIsServedAlongsideIt(): void
    {
        $conn = $this->conn();
        $conn->exec('DROP TABLE IF EXISTS d4_php_mx');
        $conn->exec('CREATE TABLE d4_php_mx (id int)');
        $slow = $conn->scalarAsync('SELECT 7 FROM pg_sleep(0.3)');
        $t0 = microtime(true);
        $this->assertSame(50_000, $conn->copyIn('COPY d4_php_mx FROM STDIN', self::ints(50_000)));
        $this->assertSame(7, $slow->await());
        $this->assertLessThan(5.0, microtime(true) - $t0);
    }

    public function testCopyRefusesNonStringData(): void
    {
        $conn = $this->conn();
        $conn->exec('DROP TABLE IF EXISTS d4_php_types');
        $conn->exec('CREATE TABLE d4_php_types (id int)');
        try {
            $conn->copyIn('COPY d4_php_types FROM STDIN', ["1\n", 2]);
            $this->fail('an int is not COPY bytes');
        } catch (\InvalidArgumentException) {
            $this->addToAssertionCount(1);
        }
        $this->assertSame(0, $conn->scalar('SELECT count(*)::int FROM d4_php_types'));
        $this->assertSame(1, $conn->scalar('SELECT 1'), 'the refused COPY was abandoned cleanly');
    }
}
