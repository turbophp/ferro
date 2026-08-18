<?php // /php/client/tests/Support/chaos_killer.php
declare(strict_types=1);

// M1-S9 chaos sidecar: watch for $marker as a PROVABLY IN-FLIGHT statement, then SIGKILL ferrod.
// Never sleep-and-hope — a kill that lands before dispatch proves nothing and passes for the
// wrong reason (SPEC §20.3 chaos discipline, learned live in S6/S9a). Exit codes:
//   0 = observed in flight, kill delivered · 2 = budget expired, NOTHING killed · 3 = usage/connect
//
// The poll rides a SECOND session through the SAME daemon (ferro/client itself — no ext-pdo, no
// ext-posix; charter rule 7). The daemon multiplexes, and the pool hands the poll its own
// connection while the sleeping statement pins another (DEFAULT_POOL_MAX_SIZE = 16,
// engine/crates/ferrod/src/pools.rs:45). Discipline notes, each measured:
//   - the marker is a string-literal predicate in the WATCHED statement (never a comment —
//     MariaDB strips comments from processlist INFO);
//   - the MySQL poll filters COMMAND IN ('Execute', 'Query') — without it the PREPARE phase
//     matches (the S6 C14 flake, and a §20.3-documented time bomb);
//   - the LIKE pattern is PARAM-BOUND so this poll can never match its own statement text (both
//     backends show placeholder text for a prepared statement), with a NOT LIKE belt besides.

require __DIR__ . '/../../vendor/autoload.php';

use Ferro\Ferro;

if ($argc !== 8) {
    fwrite(
        STDERR,
        "usage: chaos_killer.php <socket> <pool> <pg|mysql> <marker> <ferrod-pid> <budget-sec> <stamp-file>\n",
    );
    exit(3);
}
[, $socket, $pool, $family, $marker, $pid, $budget, $stampFile] = $argv;

try {
    $conn = Ferro::connect($socket, $pool, 2.0, 5.0);
} catch (\Throwable $e) {
    fwrite(STDERR, 'killer connect failed: ' . $e->getMessage() . "\n");
    exit(3);
}

$sql = $family === 'pg'
    ? "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE \$1"
        . " AND query NOT LIKE '%pg_stat_activity%'"
    : "SELECT count(*) FROM information_schema.processlist WHERE command IN ('Execute', 'Query')"
        . " AND info LIKE ? AND info NOT LIKE '%processlist%'";
$pattern = '%' . $marker . '%';

$deadline = microtime(true) + (float) $budget;
while (microtime(true) < $deadline) {
    try {
        if ((int) $conn->scalar($sql, [$pattern]) >= 1) {
            // Stamp BEFORE the signal, so the recorded instant is a strict LOWER BOUND on the
            // kill: the test asserts stamp <= the moment it caught the loss, which is the only
            // race-free proof that the daemon death PRECEDED the classification. A "has the
            // killer exited by now?" check is NOT equivalent and was measured insufficient —
            // under a shrunken client io timeout the killer simply catches up inside the grace.
            file_put_contents($stampFile, (string) microtime(true));
            shell_exec('kill -9 ' . (int) $pid . ' 2>/dev/null');
            fwrite(STDOUT, "killed {$pid} after observing {$marker} in flight\n");
            exit(0);
        }
    } catch (\Throwable $e) {
        fwrite(STDERR, 'killer poll error: ' . $e->getMessage() . "\n");
        exit(3);
    }
    usleep(50_000);
}
fwrite(STDERR, "budget expired without observing {$marker} — NOTHING was killed\n");
exit(2);
