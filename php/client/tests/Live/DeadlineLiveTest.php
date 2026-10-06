<?php // /php/client/tests/Live/DeadlineLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Error\FerroException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\RetryPolicy;
use Ferro\Ferro;
use Ferro\Protocol\Generated\Constants as C;

/**
 * **M3-D1c, live: a slow statement is not a dead engine.**
 *
 * Before this slice the transport's read timeout (5 s by default) was every request's deadline and
 * closed the whole session: a statement slower than it failed — a write as `Indeterminate` — and
 * took every in-flight request with it. Now silence is probed with a PING, a request deadline is
 * opt-in (`statementTimeout`, enforced by the engine itself with a client backstop) and cancels only
 * that request, and only an engine that answers nothing — not even a PING — closes the session.
 */
final class DeadlineLiveTest extends LiveTestCase
{
    public function testAStatementSlowerThanTheReadTimeoutSucceedsOnTheSameSession(): void
    {
        $c = Ferro::connect($this->socketPath, ioTimeout: 0.5);
        $this->assertSame(7, $c->scalar('SELECT 7 FROM pg_sleep(1.6)'));
        $this->assertSame(0, $c->reconnectCount(), 'the session was never closed');
        $this->assertSame(1, $c->scalar('SELECT 1'));
    }

    /**
     * The engine enforces the statement timeout (`timeout_ms`) and answers with the statement's
     * fate; the session — and the connection — carry on.
     */
    public function testAStatementTimeoutCancelsOnlyThatStatement(): void
    {
        $c = Ferro::connect($this->socketPath, statementTimeout: 0.3);
        $start = microtime(true);
        try {
            $c->scalar('SELECT 1 FROM pg_sleep(5)'); // not `SELECT pg_sleep(5)`: a `void` column is refused before it runs
            $this->fail('the statement must not outlive its timeout');
        } catch (NonRetryableException $e) {
            // The ENGINE's answer — its `Cancelled` code, at the timeout. The client's backstop
            // (0.3 s + 1 s) would also end the wait, but later: bounding the elapsed time below the
            // backstop is what proves `timeout_ms` reached the engine (review F7: with the bound at
            // 2 s, either half alone passed).
            $this->assertSame(C::ERR_CANCELLED, $e->errorCode());
        }
        $this->assertLessThan(1.0, microtime(true) - $start, 'answered by the engine at the timeout, not by the backstop');
        $this->assertSame(1, $c->scalar('SELECT 1'));
        $this->assertSame(0, $c->reconnectCount(), 'only the statement was cancelled, not the session');
    }

    /**
     * Review F1: a COMMIT carries no `timeout_ms` and the engine does not act on a CANCEL for it, so
     * it gets no client backstop either. A COMMIT slowed by a deferred trigger past the statement
     * timeout, the backstop AND the read timeout used to be reported `Indeterminate` — with the
     * session closed — while it went on to commit.
     */
    public function testASlowCommitUnderAStatementTimeoutCommits(): void
    {
        $admin = Ferro::connect($this->socketPath);
        $admin->exec('DROP TABLE IF EXISTS d1c_slow_commit');
        $admin->exec('CREATE TABLE d1c_slow_commit (id int)');
        $admin->exec('CREATE OR REPLACE FUNCTION d1c_slow() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2.2); RETURN NULL; END $$');
        $admin->exec('CREATE CONSTRAINT TRIGGER d1c_slow_t AFTER INSERT ON d1c_slow_commit DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION d1c_slow()');

        $c = Ferro::connect($this->socketPath, ioTimeout: 0.5, statementTimeout: 0.3, policy: RetryPolicy::none());
        $c->begin();
        $c->exec('INSERT INTO d1c_slow_commit VALUES (1)');
        $start = microtime(true);
        $c->commit();
        $this->assertGreaterThan(2.0, microtime(true) - $start, 'the COMMIT really ran the slow trigger');
        $this->assertSame(1, $admin->scalar('SELECT count(*) FROM d1c_slow_commit'));
        $this->assertFalse($c->session()->isPoisoned(), 'the session survived the slow COMMIT');
        $this->assertSame(1, $c->scalar('SELECT 1'));
        $admin->exec('DROP TABLE d1c_slow_commit');
        $admin->exec('DROP FUNCTION d1c_slow()');
    }

    /**
     * Review F1: `timeout_ms` bounds the wait for a pooled connection too. With every connection
     * pinned by an open transaction, a write is answered by ITS statement timeout as a pool timeout
     * — Retryable, nothing was sent — where the client used to CANCEL it at its backstop, get no
     * answer, close the session, and call a never-dispatched write `Indeterminate`.
     */
    public function testAWriteWaitingForAConnectionIsAKnownNotAppliedAtItsTimeout(): void
    {
        $holders = [];
        try {
            for ($i = 0; $i < 16; ++$i) { // ferrod's pool size
                $h = Ferro::connect($this->socketPath);
                $h->begin();
                $h->scalar('SELECT 1');
                $holders[] = $h;
            }
            $c = Ferro::connect($this->socketPath, ioTimeout: 0.5, statementTimeout: 0.3, policy: RetryPolicy::none());
            $start = microtime(true);
            try {
                $c->exec('CREATE TEMP TABLE IF NOT EXISTS d1c_never (i int)');
                $this->fail('there is no connection to run it on');
            } catch (RetryableException $e) {
                $this->assertSame(C::ERR_POOL_TIMEOUT, $e->errorCode());
            }
            $this->assertLessThan(1.0, microtime(true) - $start, 'answered at the statement timeout, not the pool\'s');
            $this->assertFalse($c->session()->isPoisoned());
        } finally {
            foreach ($holders as $h) {
                $h->rollBack();
            }
        }
    }

    /**
     * Review F5: a stream carries no `timeout_ms` — the engine would bound its WHOLE life with it,
     * including the waits for a caller that consumes rows slowly — so a stream consumed for longer
     * than the statement timeout completes.
     */
    public function testAStreamConsumedForLongerThanTheStatementTimeoutCompletes(): void
    {
        $c = Ferro::connect($this->socketPath, statementTimeout: 0.3, policy: RetryPolicy::none());
        $n = 0;
        foreach ($c->stream('SELECT g FROM generate_series(1, 200000) g') as $_) {
            if (++$n === 10) {
                usleep(800_000);
            }
        }
        $this->assertSame(200000, $n);
    }

    /**
     * An engine that answers nothing at all — modelled by SIGSTOPping `ferrod` — fails the request
     * after one silent read timeout plus one unanswered PING, not never.
     */
    public function testAStalledEngineFailsTheRequestInsteadOfHanging(): void
    {
        $c = Ferro::connect($this->socketPath, ioTimeout: 0.4, policy: RetryPolicy::none());
        $this->assertSame(1, $c->scalar('SELECT 1'));
        $pid = $this->stopFerrodAndWait(); // every thread stopped: a STOP still in flight answered SELECT 2
        try {
            $start = microtime(true);
            try {
                $c->scalar('SELECT 2');
                $this->fail('a stalled engine must not answer');
            } catch (FerroException) {
            }
            $elapsed = microtime(true) - $start;
            $this->assertGreaterThan(0.7, $elapsed, 'silence was probed, not treated as failure at once');
            $this->assertLessThan(2.5, $elapsed, 'two read timeouts, not a hang');
        } finally {
            $this->continueFerrod($pid);
        }
    }

    /**
     * M6-F8 review round 2 (R2-1), against a real `ferrod` on the `fread` path: an engine stopped
     * in the MIDDLE of writing a frame — its header and a socket buffer's worth of a 24 MB payload
     * sent — still meets the backstop (CANCEL at the deadline, the session closed one read timeout
     * later). Round 1 counted the partly read frame as "readable", which disabled the backstop AND
     * liveness: measured, the await hung 25 s until the probe's SIGCONT, then returned the value.
     */
    public function testAnEngineStoppedMidFrameStillMeetsTheBackstop(): void
    {
        $conn = Ferro::connect($this->socketPath, ioTimeout: 5.0, policy: RetryPolicy::none(), statementTimeout: 0.5, receiveFds: false);
        $this->assertSame(1, $conn->scalar('SELECT 1'));
        $f = $conn->scalarAsync("SELECT length(repeat('x', 12000000)) || repeat('y', 12000000)");
        usleep(1_000_000); // ferrod writes the frame's header and what the socket takes, then blocks
        $pid = $this->stopFerrodAndWait();
        // A regression must fail, not hang the suite: resume the engine after 20 s regardless.
        exec(sprintf('(sleep 20; kill -CONT %d) > /dev/null 2>&1 &', $pid));
        try {
            $t0 = microtime(true);
            try {
                $f->await();
                $this->fail('a stopped engine must not answer');
            } catch (FerroException) {
            }
            // The deadline (0.5 s + the 2 s margin, from the submit) + one read timeout (5 s).
            $this->assertLessThan(12.0, microtime(true) - $t0, 'the backstop, not the 20 s SIGCONT');
        } finally {
            $this->continueFerrod($pid);
        }
    }
}
