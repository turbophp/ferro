<?php
declare(strict_types=1);

/**
 * bench_fanout.php — SPEC §16's fan-out target: "10-query fan-out latency ≤ max(single query) +
 * 2 ms under Fibers" (M3-D1e).
 *
 * Every iteration times, back to back on ONE connection:
 *   - single: one synchronous `SELECT pg_sleep(s)` — the slowest query, since all k are equal;
 *   - fibers: k tasks under `Ferro\Loop::run`, each awaiting its own `scalarAsync` of the same
 *             statement — the §16 shape ("under Fibers");
 *   - await:  the same k statements submitted together and awaited with `Ferro\await()` — plain
 *             FPM, no scheduler (M3-D1a);
 * and the orchestrator reports each distribution and `fanout − single` per iteration. The sleep
 * keeps the measurement about OVERLAP: k sequential sleeps would cost k × s.
 *
 * GC stays on and the samples are emitted once after the loop, as in bench_client.php.
 *
 * Usage: php bench_fanout.php <autoload.php> <socket> <warmup> <measured> <k> <sleep_ms>
 * Output: {"header": {...}, "single": [ns…], "fibers": [ns…], "await": [ns…]}
 */

if ($argc < 7) {
    fwrite(STDERR, "usage: bench_fanout.php <autoload.php> <socket> <warmup> <measured> <k> <sleep_ms>\n");
    exit(64);
}
[$autoload, $socket] = [$argv[1], $argv[2]];
[$warmup, $measured, $k] = [(int) $argv[3], (int) $argv[4], (int) $argv[5]];
$sleep = ((float) $argv[6]) / 1000.0;
if (!is_file($autoload)) {
    fwrite(STDERR, "bench_fanout: autoloader not found at {$autoload}\n");
    exit(65);
}
require $autoload;

use Ferro\Ferro;
use Ferro\Loop;
use Ferro\Protocol\Msgpack\PackerFactory;

$conn = null;
$deadline = microtime(true) + 15.0;
$lastError = 'no attempt';
while (microtime(true) < $deadline) {
    try {
        $c = Ferro::connect($socket, 'default', 2.0, 5.0);
        if ($c->scalar('SELECT 1') !== null) {
            $conn = $c;
            break;
        }
    } catch (\Throwable $e) {
        $lastError = get_class($e) . ': ' . $e->getMessage();
    }
    usleep(100_000);
}
if ($conn === null) {
    fwrite(STDERR, "bench_fanout: ferrod not ready: {$lastError}\n");
    exit(66);
}

$sql = sprintf('SELECT 1 FROM pg_sleep(%.6F)', $sleep);

$single = static function () use ($conn, $sql): int {
    $t = hrtime(true);
    $conn->scalar($sql);
    return hrtime(true) - $t;
};
$fibers = static function () use ($conn, $sql, $k): int {
    $tasks = [];
    for ($i = 0; $i < $k; ++$i) {
        $tasks[] = static fn (): mixed => $conn->scalarAsync($sql)->await();
    }
    $t = hrtime(true);
    $out = Loop::run($tasks);
    $ns = hrtime(true) - $t;
    if (count($out) !== $k) {
        throw new \RuntimeException('fan-out lost a result');
    }
    return $ns;
};
$await = static function () use ($conn, $sql, $k): int {
    $t = hrtime(true);
    $futures = [];
    for ($i = 0; $i < $k; ++$i) {
        $futures[] = $conn->scalarAsync($sql);
    }
    $out = \Ferro\await($futures);
    $ns = hrtime(true) - $t;
    if (count($out) !== $k) {
        throw new \RuntimeException('fan-out lost a result');
    }
    return $ns;
};

for ($i = 0; $i < $warmup; ++$i) {
    $single();
    $fibers();
    $await();
}
$s = array_fill(0, $measured, 0);
$f = array_fill(0, $measured, 0);
$a = array_fill(0, $measured, 0);
for ($i = 0; $i < $measured; ++$i) {
    $s[$i] = $single();
    $f[$i] = $fibers();
    $a[$i] = $await();
}

$jitRaw = null;
if (function_exists('opcache_get_status')) {
    $status = @opcache_get_status(false);
    if (is_array($status) && isset($status['jit']) && is_array($status['jit'])) {
        $jitRaw = $status['jit'];
    }
}
$jitOn = is_array($jitRaw) && !empty($jitRaw['enabled']) && !empty($jitRaw['on']);
echo json_encode([
    'header' => [
        'php_version' => PHP_VERSION,
        'ext_msgpack' => extension_loaded('msgpack'),
        'gc_enabled' => gc_enabled(),
        'jit_effective' => $jitOn ? 'on' : 'off',
        'jit_status' => $jitRaw,
        'packer_class' => get_class(PackerFactory::forEncode()),
        'k' => $k,
        'sleep_ms' => $sleep * 1000.0,
        'warmup_n' => $warmup,
        'samples_n' => $measured,
    ],
    'single' => $s,
    'fibers' => $f,
    'await' => $a,
], JSON_THROW_ON_ERROR), "\n";
