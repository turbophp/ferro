<?php
declare(strict_types=1);

/**
 * bench_client.php — the FERRO half of the D12 boundary-latency benchmark (SPEC §16.1).
 *
 * It drives the SHIPPED PHP client (`Ferro\Ferro::connect` -> `Connection::scalar`) against a
 * running release `ferrod` over its UDS socket, timing the trivial round trip
 * `PHP client -> ferrod -> live SELECT 1 -> response`. The orchestrator (`ferro-bench`) launches
 * this under two JIT modes and folds the emitted samples + header into the committed result.
 *
 * Honesty invariants (each folds a verification fix from the S8 plan):
 *   [V5] the client autoloader path is passed in (a bare run has no autoloader -> fatal).
 *   [V7] readiness = a bounded connect-retry around the FIRST `scalar('SELECT 1')` (a bare socket
 *        connect would pass even with the pool's upstream down — pools connect lazily).
 *   [V4] the timing window is TIGHT: `hrtime(true)` immediately before/after `scalar('SELECT 1')`
 *        and nothing else between; samples land in a pre-sized array; emitted ONCE after the loop.
 *   [V3] GC stays ON — the recorded p99 must honestly include cyclic-GC pauses. NO gc_disable().
 *   [V6] the EFFECTIVE JIT (`opcache_get_status(false)['jit']`) is emitted so the orchestrator can
 *        assert it matches the intended mode (a WSL2 JIT buffer can silently fail to engage).
 *
 * Usage: php [-d ...] bench_client.php <autoload.php> <socket> <warmup> <measured>
 * Output (stdout): one JSON document {"header": {...}, "samples": [ns, ...]}.
 */

if ($argc < 5) {
    fwrite(STDERR, "usage: bench_client.php <autoload.php> <socket> <warmup> <measured>\n");
    exit(64);
}
$autoload = $argv[1];
$socket   = $argv[2];
$warmup   = (int) $argv[3];
$measured = (int) $argv[4];

if (!is_file($autoload)) {
    fwrite(STDERR, "bench_client: autoloader not found at {$autoload} — run (cd php/client && composer install)\n");
    exit(65);
}
require $autoload; // [V5]

use Ferro\Ferro;
use Ferro\Protocol\Msgpack\PackerFactory;

/** Normalize opcache_get_status(false)['jit'] to "on"/"off" + return the raw array. */
function jit_state(): array
{
    $raw = null;
    if (function_exists('opcache_get_status')) {
        $status = @opcache_get_status(false);
        if (is_array($status) && isset($status['jit']) && is_array($status['jit'])) {
            $raw = $status['jit'];
        }
    }
    $on = is_array($raw) && !empty($raw['enabled']) && !empty($raw['on']);
    return ['effective' => $on ? 'on' : 'off', 'raw' => $raw];
}

$jit = jit_state();

// ---- [V7] readiness: bounded connect-retry around the first SELECT 1 --------------------------
$readyTimeout = 15.0;
$deadline = microtime(true) + $readyTimeout;
$conn = null;
$lastError = 'no attempt made';
while (microtime(true) < $deadline) {
    try {
        $c = Ferro::connect($socket, 'default', 2.0, 5.0);
        $v = $c->scalar('SELECT 1'); // the real readiness probe (forces a live upstream round trip)
        if ($v !== null) {
            $conn = $c;
            break;
        }
        $lastError = 'SELECT 1 returned null';
    } catch (\Throwable $e) {
        $lastError = get_class($e) . ': ' . $e->getMessage();
    }
    usleep(100_000);
}
if ($conn === null) {
    fwrite(STDERR, "bench_client: ferrod not ready within {$readyTimeout}s; last error: {$lastError}\n");
    exit(3);
}

// ---- warmup (not measured): ferrod pool steady-state + JIT trace compilation [V2] -------------
for ($i = 0; $i < $warmup; $i++) {
    $conn->scalar('SELECT 1');
}

// ---- measured loop [V4]: tight window, pre-sized buffer, nothing else between the timers -------
$samples = array_fill(0, $measured, 0);
for ($i = 0; $i < $measured; $i++) {
    $t = hrtime(true);
    $conn->scalar('SELECT 1');
    $samples[$i] = hrtime(true) - $t;
}

// ---- emit ONCE after the loop -----------------------------------------------------------------
$header = [
    'target'       => 'ferro',
    'php_version'  => PHP_VERSION,
    'ext_msgpack'  => extension_loaded('msgpack'),
    'pdo_pgsql'    => extension_loaded('pdo_pgsql'),
    'gc_enabled'   => gc_enabled(), // [V3]
    'jit_effective' => $jit['effective'], // [V6]
    'jit_status'   => $jit['raw'],
    'packer_class' => get_class(PackerFactory::forEncode()), // the ACTUAL codec measured
    'warmup_n'     => $warmup,
    'samples_n'    => $measured,
    'skipped'      => false,
    'skip_reason'  => null,
];

echo json_encode(['header' => $header, 'samples' => $samples], JSON_THROW_ON_ERROR);

try {
    $conn->session()->close(); // best-effort GOODBYE so ferrod releases the session
} catch (\Throwable) {
    // teardown best-effort; the numbers are already emitted.
}
