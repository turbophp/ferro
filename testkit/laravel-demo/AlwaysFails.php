<?php

namespace Illuminate\Tests\Integration\Database\FerroDemo;

use Illuminate\Bus\Batchable;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;

/** A queued job that always throws, with one try — so the worker records it in `failed_jobs`. */
class AlwaysFails implements ShouldQueue
{
    use Batchable, Dispatchable, InteractsWithQueue, Queueable;

    public int $tries = 1;

    public function handle(): void
    {
        throw new \RuntimeException('ferro demo: this job fails on purpose');
    }
}
