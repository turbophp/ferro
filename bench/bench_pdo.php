<?php
declare(strict_types=1);

/**
 * bench_pdo.php — the PDO BASELINE for the D12 benchmark: raw `PDO/pdo_pgsql -> Postgres` in the
 * same environment, so the committed result carries an honest `overhead_vs_pdo` delta (SPEC §16.1).
 *
 * [fairness] The baseline shape mirrors ferro's trivial call as closely as PDO allows:
 *   - construct `PDO` ONCE, outside the loop;
 *   - per iteration `$pdo->query('SELECT 1')->fetchColumn()` — an UNPREPARED query + fetch = one
 *     round trip + a column fetch, matching `Connection::scalar('SELECT 1')`.
 * It deliberately does NOT use `ATTR_PERSISTENT` nor a prepared-once statement re-executed — either
 * makes the baseline artificially fast and overstates ferro's overhead.
 *
 * The transport-library caveat (recorded in bench/README.md): this path is PHP/libpq -> Postgres,
 * whereas ferro is PHP -> ferrod (Rust/tokio-postgres) -> Postgres. So the delta is Ferro's added
 * boundary overhead IN THIS ENV, not a pure hop-isolation.
 *
 * Usage: php [-d ...] bench_pdo.php <postgres-url> <warmup> <measured>
 * Output (stdout): one JSON document {"header": {...}, "samples": [ns, ...]}, same shape as
 * bench_client.php. If pdo_pgsql is absent the run is emitted as skipped (the ferro number still
 * records) and exits 0.
 */

if ($argc < 4) {
    fwrite(STDERR, "usage: bench_pdo.php <postgres-url> <warmup> <measured>\n");
    exit(64);
}
$url      = $argv[1];
$warmup   = (int) $argv[2];
$measured = (int) $argv[3];

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

/** Base header shared by the skip and success paths. */
function base_header(array $jit, int $warmup, int $measured): array
{
    return [
        'target'        => 'pdo',
        'php_version'   => PHP_VERSION,
        'ext_msgpack'   => extension_loaded('msgpack'),
        'pdo_pgsql'     => extension_loaded('pdo_pgsql'),
        'gc_enabled'    => gc_enabled(),
        'jit_effective' => $jit['effective'],
        'jit_status'    => $jit['raw'],
        'packer_class'  => 'n/a',
        'warmup_n'      => $warmup,
        'samples_n'     => $measured,
        'skipped'       => false,
        'skip_reason'   => null,
    ];
}

if (!extension_loaded('pdo_pgsql')) {
    $header = base_header($jit, $warmup, 0);
    $header['skipped'] = true;
    $header['samples_n'] = 0;
    $header['skip_reason'] = 'pdo_pgsql extension not loaded';
    echo json_encode(['header' => $header, 'samples' => []], JSON_THROW_ON_ERROR);
    exit(0);
}

// ---- parse postgres://user:pass@host:port/db into a PDO DSN -----------------------------------
$parts = parse_url($url);
if ($parts === false || !isset($parts['host'])) {
    fwrite(STDERR, "bench_pdo: could not parse postgres url: {$url}\n");
    exit(65);
}
$host = $parts['host'];
$port = $parts['port'] ?? 5432;
$db   = isset($parts['path']) ? ltrim($parts['path'], '/') : 'postgres';
$user = $parts['user'] ?? 'postgres';
$pass = $parts['pass'] ?? '';
$dsn  = sprintf('pgsql:host=%s;port=%d;dbname=%s', $host, $port, $db);

try {
    $pdo = new PDO($dsn, $user, $pass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        // No ATTR_PERSISTENT: a persistent handle would make the baseline artificially fast.
    ]);
} catch (\Throwable $e) {
    fwrite(STDERR, 'bench_pdo: PDO connect failed: ' . $e->getMessage() . "\n");
    exit(3);
}

// ---- warmup (not measured) --------------------------------------------------------------------
for ($i = 0; $i < $warmup; $i++) {
    $pdo->query('SELECT 1')->fetchColumn();
}

// ---- measured loop: tight window, pre-sized buffer, unprepared query + fetch -------------------
$samples = array_fill(0, $measured, 0);
for ($i = 0; $i < $measured; $i++) {
    $t = hrtime(true);
    $pdo->query('SELECT 1')->fetchColumn();
    $samples[$i] = hrtime(true) - $t;
}

echo json_encode(['header' => base_header($jit, $warmup, $measured), 'samples' => $samples], JSON_THROW_ON_ERROR);
