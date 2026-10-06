<?php // /php/client/src/Loop.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Session;
use Ferro\Client\Waiter;

/**
 * The built-in Fiber scheduler (SPEC §10.1, M3-D1b): run several tasks as Fibers, and when one of
 * them awaits a {@see Future} whose terminal has not arrived, suspend it and run the others.
 *
 *     [$profile, $orders] = Ferro\Loop::run(
 *         fn () => $db->queryOneAsync('select … where id = ?', [$id])->await(),
 *         fn () => $db->queryAsync('select … where user_id = ?', [$id])->await(),
 *     );
 *
 * Every Fiber shares the one socket per {@see Session}: the loop selects on the sessions its
 * waiting Fibers need and reads one frame at a time, filing each under its `request_id`, then
 * resumes every Fiber whose terminal is there. A task that throws does not stop the others; the
 * first failure, in task order, is thrown once every task has finished (the {@see await} rule).
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
     * How many consecutive one-second selects may find nothing before the loop falls back to a
     * blocking read, which the session's own transport read timeout bounds. Without it, a peer
     * that stops answering without closing would keep the loop waiting forever, where a
     * synchronous call would have failed at its read timeout.
     */
    private const IDLE_SELECTS_BEFORE_BLOCKING_READ = 30;

    private static int $idleSelects = 0;

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
        self::$idleSelects = 0;
        try {
            return self::drive($tasks);
        } finally {
            self::$owned = null;
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
     * Read at least one frame for the waiting Fibers' sessions: select on the ones that can be
     * selected and read from whichever is ready; a session that cannot be selected is read directly.
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
        $first = array_values($sessions)[0];
        if (count($sessions) === 1) {
            // One session: a blocking read is exactly the synchronous behaviour, read timeout included.
            $first->pollOnce();
            return;
        }
        $streams = [];
        foreach ($sessions as $id => $session) {
            $stream = $session->selectableStream();
            if ($stream === null) {
                // Not selectable (a test double, or a session that just failed): read directly.
                $session->pollOnce();
                return;
            }
            $streams[$id] = $stream;
        }
        $read = array_values($streams);
        $write = null;
        $except = null;
        // A bounded wait: each session's own read timeout still bounds a dead peer, this only keeps
        // the loop from spinning.
        $n = @stream_select($read, $write, $except, 1, 0);
        if ($n === false || $n === 0) {
            if (++self::$idleSelects >= self::IDLE_SELECTS_BEFORE_BLOCKING_READ) {
                self::$idleSelects = 0;
                $first->pollOnce();
            }
            return;
        }
        self::$idleSelects = 0;
        foreach ($streams as $id => $stream) {
            if (in_array($stream, $read, true)) {
                $sessions[$id]->pollOnce();
            }
        }
    }
}
