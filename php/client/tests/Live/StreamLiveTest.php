<?php // /php/client/tests/Live/StreamLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;

/**
 * End-to-end `Connection::stream(...)` against a real `ferrod` + Docker Postgres (M1-S5 Task 7, the
 * PHP-side companion to the Rust `stream_it` live gate). Two properties the Rust harness cannot show
 * — they are CLIENT properties of the lazy generator:
 *
 *  - **constant memory:** a genuinely large result (100000 rows) consumed lazily keeps PHP peak
 *    memory bounded (a few arena chunks), NOT proportional to the row count — the generator yields
 *    each DATA batch and sends a replenishing `WINDOW_UPDATE`, never buffering the whole result. A
 *    regression that materialized the result would blow past the ceiling by tens of MiB.
 *  - **abandonment recovery:** a `foreach … break` destroys the generator, whose `finally` sends the
 *    outbound CANCEL + drains to the terminal, so the SAME session round-trips a fresh request
 *    cleanly with no stale DATA frame desyncing the wire.
 *
 * Skips clean (via {@see LiveTestCase}) when `FERRO_TEST_PG_URL` is unset or no `ferrod` binary is
 * found, so `composer test` stays green offline. The live ferrod runs with its DEFAULT credit window
 * (`Config::from_env` exposes no window knob); backpressure under a SMALL window is proven by the
 * Rust `stream_it` gate — here the point is that the PHP client stays bounded whatever the window.
 */
final class StreamLiveTest extends LiveTestCase
{
    /** A large multi-frame result: 100000 single-int rows (~98 DATA frames at 1024 rows/frame). */
    private const BIG_SQL = 'SELECT generate_series(1, 100000) AS n';
    private const BIG_ROWS = 100000;

    /** Ceiling on the streaming loop's peak-memory growth: a fully-buffered result would be tens of
     * MiB (100000 assoc rows), a lazy stream a small constant. 8 MiB cleanly separates the two. */
    private const MEMORY_CEILING_BYTES = 8 * 1024 * 1024;

    private function connection(): Connection
    {
        $session = $this->connect(); // handshakes (LiveTestCase::connect)
        return new Connection($session, 'default');
    }

    public function testLargeStreamStaysBoundedInMemory(): void
    {
        $conn = $this->connection();
        try {
            // Warm up so the transport read buffers + session machinery are already allocated; the
            // measured delta then isolates the streaming loop's own allocation.
            $this->assertSame(1, $conn->scalar('SELECT 1'));

            gc_collect_cycles();
            $peakBefore = memory_get_peak_usage(true);

            $count = 0;
            $expected = 1;
            /** @var array{0:int,1:mixed}|null $firstMismatch */
            $firstMismatch = null;
            // Consume LAZILY: keep only a counter + an out-of-order tripwire, never store the rows,
            // and never call an assertion INSIDE the loop (that would itself allocate and pollute the
            // measurement). A client that buffered the whole result would exceed the ceiling below.
            foreach ($conn->stream(self::BIG_SQL) as $row) {
                if ($firstMismatch === null && $row['n'] !== $expected) {
                    $firstMismatch = [$expected, $row['n']];
                }
                $expected++;
                $count++;
            }

            $growth = memory_get_peak_usage(true) - $peakBefore;

            $this->assertSame(self::BIG_ROWS, $count, 'every streamed row was delivered');
            $this->assertNull(
                $firstMismatch,
                $firstMismatch === null
                    ? ''
                    : sprintf('rows must stream in order; expected %d got %s', $firstMismatch[0], var_export($firstMismatch[1], true)),
            );
            $this->assertLessThan(
                self::MEMORY_CEILING_BYTES,
                $growth,
                sprintf(
                    'streaming %d rows must stay bounded in memory (peak grew %d bytes) — a buffered '
                    . 'result would be tens of MiB',
                    self::BIG_ROWS,
                    $growth,
                ),
            );

            // The session is healthy after a full stream (its per-session cap returned to baseline).
            $this->assertSame(2, $conn->scalar('SELECT 2'));
        } finally {
            $conn->session()->close();
        }
    }

    public function testForeachBreakRecoversCleanly(): void
    {
        $conn = $this->connection();
        try {
            $seen = 0;
            // Abandon mid-stream: `break` drops the only reference to the generator, so it is
            // destroyed here and its `finally` sends the outbound CANCEL + drains to the terminal.
            foreach ($conn->stream(self::BIG_SQL) as $row) {
                $seen++;
                if ($seen >= 5) {
                    break;
                }
            }
            $this->assertSame(5, $seen, 'the break stops the foreach after five rows');

            // RECOVERY: the wire was re-framed by the abandonment drain, so the SAME session
            // round-trips fresh requests cleanly — no stale streamed frame leaks into the next reply.
            $this->assertSame(42, $conn->scalar('SELECT 42'));
            $this->assertSame([['n' => 7]], $conn->query('SELECT 7 AS n'));
        } finally {
            $conn->session()->close();
        }
    }
}
