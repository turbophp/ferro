<?php // /php/client/src/Loop.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Session;
use Ferro\Client\Waiter;

/**
 * The built-in Fiber scheduler (SPEC §10.1, M3-D1b): run several tasks as Fibers, and when one of
 * them awaits a {@see Future} whose terminal has not arrived, suspend it and run the others.
 *
 *     [$profile, $orders] = Ferro\Loop::run([
 *         fn () => $db->queryOneAsync('select … where id = ?', [$id])->await(),
 *         fn () => $db->queryAsync('select … where user_id = ?', [$id])->await(),
 *     ]);
 *
 * **An imperative transaction belongs to the Fiber that began it** ({@see \Ferro\Client\Connection::begin}):
 * another Fiber's statement on the same Connection is refused while it is open, never routed into
 * it. Use a Connection per concurrent transaction, or the closure form `transaction()`.
 *
 * Every Fiber shares the one socket per {@see Session}: the loop selects on the sessions its
 * waiting Fibers need and reads one frame at a time, filing each under its `request_id`, then
 * resumes every Fiber whose terminal is there. Each session keeps its own liveness clock, its
 * transport's read timeout: a session silent that long is PINGed without blocking the others, and
 * one that answers neither its requests nor the PING within another timeout fails, as it would
 * synchronously (M3-D1c), whatever the others do. Request deadlines ({@see Session::setDeadline})
 * wake the loop too, so a due request is CANCELled on time. A task that throws does not stop the others; the
 * first failure, in task order, is thrown once every task has finished (the {@see await} rule).
 *
 * **Waiting on another Fiber's open stream suspends too**: a Fiber that wants the session while
 * a different Fiber has a stream open on it waits for that stream to close.
 *
 * **Only `await` on a Future returned by an `…Async` method suspends.** A synchronous call inside a
 * task, and an `await` on a Future that settled at once (inside a transaction, for example), block
 * the whole loop until they complete, which is still correct: frames for other Fibers that arrive
 * meanwhile are kept for them. A Fiber this loop did not start never suspends into it.
 *
 * Swoole coroutines and Fibers do not compose: under Octane/Swoole, do not run this loop (§10.1).
 */
final class Loop
{
    /** @var \WeakMap<\Fiber<mixed, mixed, mixed, mixed>, true>|null the Fibers the running loop owns */
    private static ?\WeakMap $owned = null;

    /**
     * When each waited-on session last delivered a frame (or was first waited on), keyed by
     * `spl_object_id`. A session silent for its own transport read timeout is probed with a PING
     * ({@see Session::probeLiveness}), never read with a blocking read that would stall the other
     * sessions (M3-D1c); one global counter once let a busy session keep a silent one waiting
     * forever (M3-D1b review F2), which is why the clock is per session.
     *
     * @var array<int, float>
     */
    private static array $lastProgress = [];

    /**
     * Run every task to completion and return their results, keyed as given.
     *
     * @template T
     * @param array<array-key, \Closure(): T> $tasks
     * @return array<array-key, T>
     */
    public static function run(array $tasks): array
    {
        if (self::$owned !== null) {
            throw new \LogicException('Ferro\\Loop::run() is already running; await inside it instead of nesting it');
        }
        self::$owned = new \WeakMap();
        self::$lastProgress = [];
        try {
            return self::drive($tasks);
        } finally {
            self::$owned = null;
            self::$lastProgress = [];
        }
    }

    /**
     * Called by {@see Future::await}: suspend the current Fiber until `$waiter` is ready, if — and
     * only if — the running loop owns this Fiber. Otherwise return at once and let the caller block.
     */
    public static function waitFor(Waiter $waiter): void
    {
        $fiber = \Fiber::getCurrent();
        if ($fiber === null || self::$owned === null || !isset(self::$owned[$fiber])) {
            return;
        }
        while (!$waiter->ready()) {
            \Fiber::suspend($waiter);
        }
    }

    /**
     * @template T
     * @param array<array-key, \Closure(): T> $tasks
     * @return array<array-key, T>
     */
    private static function drive(array $tasks): array
    {
        /** @var array<array-key, \Fiber<mixed, mixed, mixed, mixed>> $fibers */
        $fibers = [];
        /** @var array<array-key, Waiter> $waiting */
        $waiting = [];
        $results = [];
        /** @var array<array-key, \Throwable> $errors */
        $errors = [];

        $step = static function (int|string $key, \Closure $advance) use (&$fibers, &$waiting, &$results, &$errors): void {
            try {
                $suspendedWith = $advance();
            } catch (\Throwable $e) {
                $errors[$key] = $e;
                return;
            }
            $fiber = $fibers[$key];
            if ($fiber->isTerminated()) {
                $results[$key] = $fiber->getReturn();
                return;
            }
            if (!$suspendedWith instanceof Waiter) {
                $errors[$key] = new \LogicException(
                    'a task suspended its Fiber with something other than a Ferro await; run that '
                        . 'scheduler outside Ferro\\Loop',
                );
                return;
            }
            $waiting[$key] = $suspendedWith;
        };

        foreach ($tasks as $key => $task) {
            $fiber = new \Fiber($task);
            assert(self::$owned !== null);
            self::$owned[$fiber] = true;
            $fibers[$key] = $fiber;
            $step($key, static fn (): mixed => $fiber->start());
        }

        while ($waiting !== []) {
            $resumed = false;
            foreach ($waiting as $key => $waiter) {
                if ($waiter->ready()) {
                    unset($waiting[$key]);
                    $fiber = $fibers[$key];
                    $step($key, static fn (): mixed => $fiber->resume());
                    $resumed = true;
                }
            }
            if (!$resumed && $waiting !== []) {
                self::readAny($waiting);
            }
        }

        foreach (array_keys($tasks) as $key) {
            if (isset($errors[$key])) {
                throw $errors[$key];
            }
        }
        $ordered = [];
        foreach (array_keys($tasks) as $key) {
            $ordered[$key] = $results[$key] ?? null;
        }
        /** @var array<array-key, T> $ordered */
        return $ordered;
    }

    /**
     * Read at least one frame for the waiting Fibers' sessions.
     *
     * One session: a blocking read, exactly the synchronous behaviour, read timeout included.
     * Several: `stream_select` across the ones that can be selected, waiting no longer than the
     * nearest liveness clock or request deadline. A session that has delivered nothing for its own
     * read timeout is selected on first, and only if nothing is waiting on its socket is it PINGed —
     * or closed, if a PING is already out and nothing at all has arrived since; due requests are
     * CANCELled after every select. A session that cannot be selected (a test double) is read
     * directly.
     *
     * @param array<array-key, Waiter> $waiting
     */
    private static function readAny(array $waiting): void
    {
        /** @var array<int, Session> $sessions */
        $sessions = [];
        foreach ($waiting as $waiter) {
            $sessions[spl_object_id($waiter->session)] = $waiter->session;
        }
        if (count($sessions) === 1) {
            array_values($sessions)[0]->pollOnce();
            return;
        }

        $now = microtime(true);
        $streams = [];
        /** @var array<int, true> $silent sessions silent for their whole read timeout */
        $silent = [];
        $nearest = 1.0;
        foreach ($sessions as $id => $session) {
            $stream = $session->selectableStream();
            $timeout = $session->readTimeout();
            if ($stream === null || $timeout === null) {
                $session->pollOnce();
                self::$lastProgress[$id] = microtime(true);
                return;
            }
            $since = self::$lastProgress[$id] ??= $now;
            $remaining = $timeout - ($now - $since);
            if ($remaining <= 0.0) {
                // Silent for its whole read timeout — as far as this loop has READ. It is judged only
                // after the select below has looked at its socket (M3-D1c review F6): an answer may
                // already be waiting there unread (a liveness PONG that followed the last request's
                // terminal is the legal case), and judging first closed a healthy session.
                $silent[$id] = true;
                $remaining = 0.0;
            }
            $nearest = min($nearest, $remaining);
            $deadline = $session->nearestDeadline();
            if ($deadline !== null) {
                $nearest = min($nearest, max($deadline - $now, 0.001));
            }
            $streams[$id] = $stream;
        }

        $read = array_values($streams);
        $write = null;
        $except = null;
        $sec = (int) $nearest;
        $usec = (int) (($nearest - $sec) * 1_000_000);
        $n = @stream_select($read, $write, $except, $sec, max($usec, 1000));
        foreach ($sessions as $session) {
            $session->enforceDeadlines(); // CANCEL whatever is due; close a session past its grace
        }
        $readable = ($n === false || $n === 0) ? [] : $read;
        foreach ($streams as $id => $stream) {
            if (in_array($stream, $readable, true)) {
                $sessions[$id]->pollOnce();
                self::$lastProgress[$id] = microtime(true);
            } elseif (isset($silent[$id])) {
                // Nothing to read after a whole read timeout. Silence is not failure (M3-D1c): probe
                // it with a PING — without blocking the other sessions — and give it another
                // timeout; a second one with nothing read since the PING went out closes it.
                $sessions[$id]->probeLiveness();
                self::$lastProgress[$id] = microtime(true);
            }
        }
    }
}
