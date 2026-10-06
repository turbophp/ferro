<?php // /php/client/src/Client/RevoltWatch.php
declare(strict_types=1);
namespace Ferro\Client;

use Revolt\EventLoop;
use Revolt\EventLoop\Suspension;

/**
 * One {@see Session} as the Revolt event loop sees it while Fibers are suspended on it (M3-D1d,
 * {@see \Ferro\Revolt}): a readable watcher on the socket, one timer for request deadlines and
 * liveness, and the suspended waiters to resume.
 *
 * Exists only while it has waiters: when the last one leaves, both watchers are cancelled and the
 * session is no longer observed, so an idle connection never keeps `EventLoop::run()` alive.
 *
 * The rules it enforces are {@see \Ferro\Loop}'s, driven by the event loop instead of a select:
 *  - a readable socket is read one frame at a time and each frame is filed by the session's router;
 *    PHP's own stream buffer is drained too, because a descriptor does not report bytes PHP has
 *    already read;
 *  - frames filed by anyone — this watcher, or another Fiber's blocking call on the same session —
 *    wake the waiter whose request they answer ({@see Session::observe});
 *  - the timer fires at the nearest request deadline or at the end of the read timeout since the
 *    last frame: due requests are CANCELled, and a session silent that long is first checked for
 *    unread bytes (M3-D1c review F6), then PINGed, then closed if a second timeout passes in silence;
 *  - a failed session resumes every waiter (each fails at its own await) and stops watching the
 *    closed socket before the loop can select on it again.
 *
 * **Nothing it runs throws out of the event loop.** The session's scheduler hooks never throw; if
 * this class itself does, the throwable is thrown into every waiting Fiber instead.
 *
 * @internal
 */
final class RevoltWatch
{
    /** @var array<int, self> by `spl_object_id` of the session */
    private static array $active = [];

    /** @var array<int, array{0: Waiter, 1: Suspension<mixed>, 2: object}> */
    private array $waiting = [];

    private int $nextKey = 0;
    private ?string $readId = null;
    private ?string $timerId = null;
    private float $timerAt = \INF;
    private float $lastProgress;
    /** Frames read off this session since the watch began; tells a read that made progress. */
    private int $frames = 0;
    private bool $stopped = false;

    private function __construct(private readonly Session $session, private readonly float $readTimeout)
    {
        $this->lastProgress = microtime(true);
    }

    /** How many sessions are being watched (for {@see \Ferro\Revolt::uninstall}). */
    public static function active(): int
    {
        return count(self::$active);
    }

    /** The watch for `$session`, started if need be; null when its socket cannot be watched. */
    public static function for(Session $session): ?self
    {
        $id = spl_object_id($session);
        if (isset(self::$active[$id])) {
            return self::$active[$id];
        }
        $stream = $session->selectableStream();
        $timeout = $session->readTimeout();
        if ($stream === null || $timeout === null) {
            return null;
        }
        $watch = new self($session, $timeout);
        self::$active[$id] = $watch;
        $session->observe($watch->observed(...));
        $watch->readId = EventLoop::onReadable($stream, $watch->readable(...));
        $watch->arm();
        return $watch;
    }

    /**
     * Suspend the current Fiber (or run the loop from `{main}`) once, until this watch resumes it.
     * The caller re-checks its condition and parks again if need be.
     *
     * @throws \LogicException if something other than this watch resumed the Fiber.
     */
    public function park(Waiter $waiter): void
    {
        $suspension = EventLoop::getSuspension();
        $token = new \stdClass();
        $key = ++$this->nextKey;
        $this->waiting[$key] = [$waiter, $suspension, $token];
        if ($this->readId !== null) {
            EventLoop::enable($this->readId); // may have been parked by a read that found nothing
        }
        if ($this->session->bufferedBytes() > 0) {
            // Bytes PHP has buffered do not make the descriptor readable: read them on the next tick.
            EventLoop::defer(fn () => $this->readable());
        }
        try {
            $value = $suspension->suspend();
        } finally {
            unset($this->waiting[$key]);
            if ($this->waiting === []) {
                $this->stop();
            }
        }
        if ($value !== $token) {
            throw new \LogicException(
                'a Fiber suspended on a Ferro request through Ferro\\Revolt was resumed by something '
                    . 'other than the Revolt event loop; under Ferro\\Revolt, await only in {main} or in '
                    . 'Fibers the Revolt loop drives (another scheduler\'s Fibers must not await Ferro Futures)',
            );
        }
    }

    /** The session's observer ({@see Session::observe}). */
    private function observed(?int $requestId): void
    {
        try {
            if ($requestId === null) {
                $this->recheck();
                return;
            }
            $this->lastProgress = microtime(true);
            ++$this->frames;
            foreach ($this->waiting as $key => [$waiter]) {
                if ($waiter->requestId === $requestId && $waiter->ready()) {
                    $this->resume($key);
                }
            }
            if ($this->waiting === []) {
                $this->stop();
            }
        } catch (\Throwable $e) {
            $this->fail($e);
        }
    }

    /** The socket is readable: read frames until neither the descriptor nor PHP's buffer has any. */
    private function readable(): void
    {
        try {
            $progressed = false;
            do {
                if ($this->stopped || $this->session->isPoisoned()) {
                    break;
                }
                $seen = $this->frames;
                $this->session->pollOnce();
                $more = $this->frames !== $seen;
                $progressed = $progressed || $more;
            } while ($more && $this->session->bufferedBytes() > 0);
            if (!$progressed && !$this->stopped && $this->readId !== null) {
                // Readable, yet nothing was read: no request is in flight to read for (an EOF, or a
                // frame nobody awaits yet). A level-triggered watcher would spin on it; park it until
                // a waiter arrives or the session changes.
                EventLoop::disable($this->readId);
            }
            $this->recheck();
        } catch (\Throwable $e) {
            $this->fail($e);
        }
    }

    /** The timer: act on due deadlines, then on silence. */
    private function timer(): void
    {
        $this->timerId = null;
        $this->timerAt = \INF;
        try {
            if ($this->stopped) {
                return;
            }
            $this->session->enforceDeadlines(); // never throws: a failed CANCEL poisons the session
            if (!$this->session->isPoisoned() && microtime(true) - $this->lastProgress >= $this->readTimeout) {
                if ($this->unreadWaiting()) {
                    // An answer is already there (a PONG behind a slow terminal is legal): read it
                    // before judging the session silent (M3-D1c review F6).
                    $this->lastProgress = microtime(true); // a frame read moves it too; nothing to judge if none is
                    $this->readable();
                    return;
                }
                $this->session->probeLiveness(); // PING, or close after an unanswered one
                $this->lastProgress = microtime(true);
            }
            $this->recheck();
        } catch (\Throwable $e) {
            $this->fail($e);
        }
    }

    /** Resume every waiter that is ready; stop when none is left or the session has failed. */
    private function recheck(): void
    {
        if ($this->stopped) {
            return;
        }
        $failed = $this->session->isPoisoned(); // every waiter is ready then: each fails at its await
        foreach ($this->waiting as $key => [$waiter]) {
            if ($failed || $waiter->ready()) {
                $this->resume($key);
            }
        }
        if ($this->waiting === []) { // always so after a failure: the closed socket is never selected
            $this->stop();
            return;
        }
        if ($this->readId !== null) {
            EventLoop::enable($this->readId);
        }
        $this->arm();
    }

    /** Make sure the timer fires no later than the nearest deadline or the end of the read timeout. */
    private function arm(): void
    {
        if ($this->stopped) {
            return;
        }
        $at = $this->lastProgress + $this->readTimeout;
        $deadline = $this->session->nearestDeadline();
        if ($deadline !== null && $deadline < $at) {
            $at = $deadline;
        }
        if ($this->timerId !== null && $this->timerAt <= $at) {
            return; // it fires no later than needed, and re-arms then
        }
        if ($this->timerId !== null) {
            EventLoop::cancel($this->timerId);
        }
        $this->timerAt = $at;
        $this->timerId = EventLoop::delay(max($at - microtime(true), 0.001), fn () => $this->timer());
    }

    private function resume(int $key): void
    {
        [, $suspension, $token] = $this->waiting[$key];
        unset($this->waiting[$key]); // resumed exactly once: Revolt refuses a second resume
        $suspension->resume($token);
    }

    /** Something here threw: hand it to every waiter rather than to the event loop. */
    private function fail(\Throwable $e): void
    {
        $waiting = $this->waiting;
        $this->waiting = [];
        $this->stop();
        foreach ($waiting as [, $suspension]) {
            try {
                $suspension->throw($e);
            } catch (\Throwable) {
                // not pending any more: nothing to deliver it to
            }
        }
    }

    /** Cancel both watchers and stop observing. Idempotent; leaves a newer watch alone. */
    private function stop(): void
    {
        if ($this->stopped) {
            return;
        }
        $this->stopped = true;
        if ($this->readId !== null) {
            EventLoop::cancel($this->readId);
            $this->readId = null;
        }
        if ($this->timerId !== null) {
            EventLoop::cancel($this->timerId);
            $this->timerId = null;
        }
        $id = spl_object_id($this->session);
        if ((self::$active[$id] ?? null) === $this) {
            unset(self::$active[$id]);
            $this->session->observe(null);
        }
    }

    /** Whether the socket has bytes to read right now, buffered by PHP or waiting in the kernel. */
    private function unreadWaiting(): bool
    {
        if ($this->session->bufferedBytes() > 0) {
            return true;
        }
        $stream = $this->session->selectableStream();
        if (!is_resource($stream)) {
            return false;
        }
        $read = [$stream];
        $write = null;
        $except = null;
        return @stream_select($read, $write, $except, 0, 0) > 0;
    }
}
