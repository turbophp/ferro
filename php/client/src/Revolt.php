<?php // /php/client/src/Revolt.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\RevoltWatch;
use Ferro\Client\Waiter;
use Revolt\EventLoop;
use Revolt\EventLoop\Internal\AbstractDriver;

/**
 * The Revolt adapter (SPEC §10.1, M3-D1d): with it installed, an `await` on a {@see Future} from an
 * `…Async` method suspends the calling Fiber into the Revolt event loop — which is what AMPHP v3
 * runs on — instead of blocking the process, so the loop keeps running timers, other I/O and other
 * Fibers while the query is in flight:
 *
 *     Ferro\Revolt::install();               // once, at boot
 *     $a = Amp\async(fn () => $db->queryAsync('select …')->await());
 *     $b = Amp\async(fn () => $db->queryAsync('select …')->await());
 *     [$x, $y] = Amp\Future\await([$a, $b]); // both statements in flight at once
 *
 * **Opt-in, never detected** (SPEC §22.2 (cv)). PHP cannot tell which scheduler, if any, will resume
 * a suspended Fiber, so suspending one into Revolt is a promise Ferro cannot check. A process that
 * merely has `revolt/event-loop` installed is not using it, and a Fiber another scheduler drives
 * would be stranded. Installing the adapter is the application saying the promise holds. Without it
 * an `await` under Revolt blocks: correct, but serial. `revolt/event-loop` is NOT a dependency of
 * this package (charter rule 7); {@see install} refuses when it is absent.
 *
 * **Which awaits suspend, once installed:**
 *  - a Fiber that {@see Loop::run} owns is still that loop's — the adapter never sees it;
 *  - `{main}` suspends: Revolt runs its event loop until the terminal arrives (AMPHP's own rule
 *    for an `await` outside any Fiber);
 *  - any other Fiber suspends only while it runs UNDER the Revolt loop (the loop's Fiber is mid-
 *    dispatch: an `Amp\async()` task, a Revolt callback, a Fiber either of them resumed). A Fiber
 *    that `{main}` starts and drives by hand blocks instead, because the loop would never resume it;
 *  - a Fiber that is resumed by something other than the Revolt loop while it waits (it was being
 *    driven by another scheduler after all) gets a `LogicException` from the `await`, and the
 *    adapter never resumes it again; it does not crash the event loop.
 *
 * **What the loop watches.** Each session with a suspended waiter gets one readable watcher on its
 * socket and one timer: request deadlines (`statementTimeout`'s backstop CANCEL, M3-D1c) and the
 * liveness rule (a session silent for its read timeout is PINGed, and closed after a second one)
 * are enforced on the timer exactly as {@see Loop} enforces them. Neither watcher outlives the last
 * waiter, so an idle connection never keeps `EventLoop::run()` alive. A failure of the session —
 * a lost link, an unanswered PING, a CANCEL that cannot be written — reaches every waiting Fiber at
 * its own `await`, with its own fate; nothing the adapter does throws out of `EventLoop::run()`.
 *
 * **What still blocks the loop:** a synchronous call (`query()`, `transaction()`, reading a stream),
 * and an `await` on a Future that settled at once (inside a transaction). They stay correct: frames
 * for other Fibers that arrive meanwhile are filed for them and wake them.
 */
final class Revolt
{
    private static bool $installed = false;

    /**
     * Install the adapter. Idempotent.
     *
     * @throws \LogicException when `revolt/event-loop` is not installed.
     */
    public static function install(): void
    {
        if (!class_exists(EventLoop::class)) {
            throw new \LogicException(
                'Ferro\\Revolt::install() needs revolt/event-loop, which is not installed '
                    . '(composer require revolt/event-loop); ferro/client does not depend on it',
            );
        }
        self::$installed = true;
        Loop::delegate(self::waitFor(...));
    }

    /**
     * Remove the adapter: awaits block again.
     *
     * @throws \LogicException while a Fiber is suspended on a Ferro request through it.
     */
    public static function uninstall(): void
    {
        if (RevoltWatch::active() > 0) {
            throw new \LogicException('Ferro\\Revolt::uninstall(): Fibers are still suspended on Ferro requests');
        }
        self::$installed = false;
        Loop::delegate(null);
    }

    public static function isInstalled(): bool
    {
        return self::$installed;
    }

    private static ?\ReflectionProperty $loopFiber = null;

    /**
     * Whether the code running now runs UNDER the Revolt loop: the loop's own Fiber is mid-`resume`
     * of a callback — or of a Fiber that callback resumed — so the current Fiber was started or
     * resumed, directly or not, by the loop (M3-D1d).
     *
     * `Driver::isRunning()` is not that: it stays true while the loop Fiber is merely SUSPENDED,
     * which is its state for the rest of the program once `{main}` has awaited anything (measured:
     * a Fiber `{main}` started afterwards was suspended into a loop that was not running). PHP
     * exposes no Fiber's parent, so this reads Revolt's loop Fiber off its `AbstractDriver`, which
     * every bundled driver extends. Any other driver (a custom one, `TracingDriver`) falls back to
     * `isRunning()`, the weaker test.
     */
    private static function loopIsDispatching(): bool
    {
        $driver = EventLoop::getDriver();
        if ($driver instanceof AbstractDriver) {
            try {
                self::$loopFiber ??= new \ReflectionProperty(AbstractDriver::class, 'fiber');
                $fiber = self::$loopFiber->getValue($driver);
                if ($fiber instanceof \Fiber) {
                    return $fiber->isRunning();
                }
            } catch (\ReflectionException) {
                // a Revolt that renamed it: fall back
            }
        }
        return $driver->isRunning();
    }

    /**
     * Suspend the current Fiber (or run the loop from `{main}`) until `$waiter` is ready — or return
     * at once, and let the caller block, when that cannot be done safely.
     *
     * @internal called by {@see Loop::waitFor}
     */
    public static function waitFor(Waiter $waiter): void
    {
        if ($waiter->ready()) {
            return;
        }
        if (\Fiber::getCurrent() !== null && !self::loopIsDispatching()) {
            // A Fiber, but not one running under the Revolt loop: whoever resumes it, it is not
            // Revolt — suspending it would wait for an event loop nobody runs. Block instead.
            return;
        }
        while (!$waiter->ready()) {
            $watch = RevoltWatch::for($waiter->session);
            if ($watch === null) {
                return; // a session that cannot be watched (no selectable socket): block, as before
            }
            $watch->park($waiter);
        }
    }
}
