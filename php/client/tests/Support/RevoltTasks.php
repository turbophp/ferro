<?php // /php/client/tests/Support/RevoltTasks.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Revolt\EventLoop;

/**
 * Run closures the way `Amp\async()` does — each queued on the Revolt event loop, so it runs in a
 * Fiber the loop drives — then run the loop to completion (M3-D1d). Each result is the closure's
 * value, or the Throwable it threw (caught, as an `Amp\Future` would hold it).
 */
final class RevoltTasks
{
    /**
     * @param array<array-key, \Closure(): mixed> $tasks
     * @return array<array-key, mixed>
     */
    public static function run(array $tasks): array
    {
        $out = [];
        foreach ($tasks as $key => $task) {
            EventLoop::queue(static function () use ($key, $task, &$out): void {
                try {
                    $out[$key] = $task();
                } catch (\Throwable $e) {
                    $out[$key] = $e;
                }
            });
        }
        EventLoop::run();
        $ordered = [];
        foreach (array_keys($tasks) as $key) {
            $ordered[$key] = array_key_exists($key, $out) ? $out[$key] : new \RuntimeException("task {$key} never finished");
        }
        return $ordered;
    }

    /** Suspend the current Revolt Fiber for `$seconds` (what `Amp\delay()` does). */
    public static function delay(float $seconds): void
    {
        $s = EventLoop::getSuspension();
        EventLoop::delay($seconds, static fn () => $s->resume());
        $s->suspend();
    }
}
