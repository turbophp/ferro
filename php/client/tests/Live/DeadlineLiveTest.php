<?php // /php/client/tests/Live/DeadlineLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Error\FerroException;
use Ferro\Client\RetryPolicy;
use Ferro\Ferro;

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
            $c->scalar('SELECT pg_sleep(5)');
            $this->fail('the statement must not outlive its timeout');
        } catch (FerroException) {
        }
        $this->assertLessThan(2.0, microtime(true) - $start, 'answered at the timeout, not after the sleep');
        $this->assertSame(1, $c->scalar('SELECT 1'));
        $this->assertSame(0, $c->reconnectCount(), 'only the statement was cancelled, not the session');
    }

    /**
     * An engine that answers nothing at all — modelled by SIGSTOPping `ferrod` — fails the request
     * after one silent read timeout plus one unanswered PING, not never.
     */
    public function testAStalledEngineFailsTheRequestInsteadOfHanging(): void
    {
        $c = Ferro::connect($this->socketPath, ioTimeout: 0.4, policy: RetryPolicy::none());
        $this->assertSame(1, $c->scalar('SELECT 1'));
        $pid = $this->ferrodPid();
        exec('kill -STOP ' . $pid);
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
            exec('kill -CONT ' . $pid);
        }
    }
}
