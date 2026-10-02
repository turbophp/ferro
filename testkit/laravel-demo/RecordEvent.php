<?php

namespace Illuminate\Tests\Integration\Database\FerroDemo;

use Illuminate\Bus\Batchable;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;

/** A queued job that writes one row — the demo's "did the worker really run it" witness. */
class RecordEvent implements ShouldQueue
{
    use Batchable, Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public function __construct(public string $name)
    {
    }

    public function handle(): void
    {
        if ($this->batch()?->cancelled()) {
            return;
        }

        DB::table('demo_events')->insert(['name' => $this->name]);
    }
}
