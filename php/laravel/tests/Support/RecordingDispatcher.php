<?php // /php/laravel/tests/Support/RecordingDispatcher.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Support;

use Illuminate\Contracts\Events\Dispatcher;

/** An events dispatcher that only records what was dispatched (illuminate/events is not a dependency). */
final class RecordingDispatcher implements Dispatcher
{
    /** @var list<object|string> */
    public array $dispatched = [];

    public function listen($events, $listener = null) {}

    public function hasListeners($eventName)
    {
        return false;
    }

    public function subscribe($subscriber) {}

    public function until($event, $payload = [])
    {
        return null;
    }

    public function dispatch($event, $payload = [], $halt = false)
    {
        $this->dispatched[] = $event;
        return null;
    }

    public function push($event, $payload = []) {}

    public function flush($event) {}

    public function forget($event) {}

    public function forgetPushed() {}

    /** @return list<class-string> */
    public function classes(): array
    {
        return array_values(array_map(static fn ($e): string => is_object($e) ? $e::class : (string) $e, $this->dispatched));
    }
}
