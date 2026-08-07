<?php // /php/client/tests/Live/LiveTestCase.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Client\RetryPolicy;
use Ferro\Client\Session;
use Ferro\Client\Transport;
use Ferro\Ferro;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PackerFactory;
use PHPUnit\Framework\TestCase;

/**
 * Base for tests that need a REAL `ferrod` process (PHP can't host the daemon in-process). Each
 * test spins up its own `ferrod`, configured ENTIRELY by env (verified against `config.rs`
 * `Config::from_env` — no ferrod change needed), pointed at the S2 Docker Postgres, and tears it
 * down afterwards.
 *
 * SKIP-CLEAN when the live prerequisites are absent, so `composer test` stays green offline:
 *   - `FERRO_TEST_PG_URL` unset/empty, OR
 *   - no `ferrod` binary (build with `cargo build -p ferrod`, or set `FERRO_FERROD_BIN`).
 *
 * Run live:
 *   docker compose -f testkit/docker-compose.yml up -d
 *   export FERRO_TEST_PG_URL=postgres://ferro:ferro@localhost:55432/ferro
 *   cargo build -p ferrod
 *   (cd php/client && composer test)
 */
abstract class LiveTestCase extends TestCase
{
    /** SIGTERM/SIGKILL as integers so this harness needs no ext-pcntl (charter rule 7). */
    private const SIGTERM = 15;
    private const SIGKILL = 9;

    /** Readiness poll budget and the SIGTERM->SIGKILL fallback window. */
    private const READY_TIMEOUT_SEC = 5.0;
    private const STOP_TIMEOUT_SEC = 6.0;
    private const POLL_INTERVAL_US = 100_000;

    /**
     * The SECOND pool's name (M1-S8a). `ferrod` infers a pool's KIND from its DSN scheme — there is
     * no `kind=` knob — so naming it `mysql` is documentation, not configuration
     * (`engine/crates/ferrod/src/config.rs:88-104`).
     */
    protected const MYSQL_POOL = 'mysql';

    protected string $socketPath = '';
    private string $stderrPath = '';
    /** The located ferrod binary + upstream DSN, kept so {@see restartFerrod} can relaunch. */
    private string $ferrodBin = '';
    private string $pgUrl = '';
    /** The optional second (MySQL-family) pool's DSN; `''` when the env var is unset. */
    private string $mysqlUrl = '';
    /** @var resource|null the ferrod process handle */
    private $proc = null;

    protected function setUp(): void
    {
        $pgUrl = getenv('FERRO_TEST_PG_URL');
        if (!is_string($pgUrl) || $pgUrl === '') {
            $this->markTestSkipped('FERRO_TEST_PG_URL is unset — skipping live ferrod tests');
        }

        // MySQL is OPTIONAL for the harness (so a PG-only dev loop still runs every PG test) but
        // MANDATORY in CI: the `php` job provisions it and the live tier runs with
        // `--fail-on-skipped`, so a missing second pool turns into a red lane rather than a silent
        // no-op. A test that needs it calls {@see requireMysqlPool}.
        $mysqlUrl = getenv('FERRO_TEST_MYSQL_URL');
        $this->mysqlUrl = is_string($mysqlUrl) ? $mysqlUrl : '';

        $bin = self::locateFerrod();
        if ($bin === null) {
            $this->markTestSkipped('ferrod binary not found (run `cargo build -p ferrod` or set FERRO_FERROD_BIN)');
        }

        // sockaddr_un.sun_path is 108 bytes — a long path (e.g. a deep session scratch dir)
        // overflows it and UnixListener::bind fails at ferrod startup. Keep it under sys temp.
        // A per-CLASS token (short hash of the concrete test class) is folded in alongside the pid
        // so two live test classes in one run never collide on the same socket/log (Task-2 review).
        $token = getmypid() . '-' . substr(hash('crc32b', static::class), 0, 8);
        $this->socketPath = sys_get_temp_dir() . '/ferro-test-' . $token . '.sock';
        $this->assertLessThan(104, strlen($this->socketPath), 'socket path must fit sun_path (108B)');
        $this->stderrPath = sys_get_temp_dir() . '/ferro-test-' . $token . '.log';

        if (file_exists($this->socketPath)) { @unlink($this->socketPath); }

        $this->ferrodBin = $bin;
        $this->pgUrl = $pgUrl;
        $this->launchFerrod($bin, $pgUrl);
        $this->waitUntilReady();
    }

    protected function tearDown(): void
    {
        $this->stopFerrod();
        if ($this->socketPath !== '' && file_exists($this->socketPath)) { @unlink($this->socketPath); }
        if ($this->stderrPath !== '' && file_exists($this->stderrPath)) { @unlink($this->stderrPath); }
    }

    /** Connect a fresh {@see Session} to this test's running ferrod over its UDS socket. */
    protected function connect(): Session
    {
        return new Session(Transport::connectUnix($this->socketPath, 2.0, 5.0));
    }

    /**
     * A resilient {@see Connection} (the real {@see Ferro::connect} path) bound to this test's ferrod
     * over its UDS socket — the epoch-aware reconnect loop + fate classifier are wired in, so the
     * daemon-restart test exercises the true §19.1 recovery path.
     *
     * `$pool` selects which of the launched pools the connection binds to; it defaults to the PG
     * `default` pool, so every pre-S8a caller is unchanged.
     */
    protected function connectConnection(?RetryPolicy $policy = null, string $pool = 'default'): Connection
    {
        return Ferro::connect($this->socketPath, $pool, 2.0, 5.0, $policy);
    }

    /**
     * The MySQL-family pool's name, or a SKIP when this run has no `FERRO_TEST_MYSQL_URL`. The skip
     * is fatal in the CI live lane (`phpunit tests/Live --fail-on-skipped`), which is what stops a
     * MySQL-only assertion from quietly vanishing.
     */
    protected function requireMysqlPool(): string
    {
        if ($this->mysqlUrl === '') {
            $this->markTestSkipped('FERRO_TEST_MYSQL_URL is unset — skipping the MySQL-pool live test');
        }
        return self::MYSQL_POOL;
    }

    /**
     * The pool names THIS harness configured `ferrod` with, in order — the one source of truth for
     * both {@see launchFerrod}'s `FERRO_POOLS` and any assertion about what `HELLO_ACK` advertises.
     * It is derived from the run's env (one pool without `FERRO_TEST_MYSQL_URL`, two with it), so an
     * assertion against it still FAILS if the engine drops, renames or invents a pool — what it
     * removes is the hard-coded `['default']` that broke the moment the harness grew a second pool.
     *
     * @return list<string>
     */
    protected function launchedPools(): array
    {
        return $this->mysqlUrl === '' ? ['default'] : ['default', self::MYSQL_POOL];
    }

    /**
     * SIGTERM the running ferrod and relaunch a fresh one on the SAME socket — the §19.1 restart
     * proof. The new process draws a NEW random `boot_epoch`, so a cached epoch no longer matches
     * and the reconnect loop must void engine-side state. Blocks until the new instance is ready.
     */
    protected function restartFerrod(): void
    {
        $this->stopFerrod();
        if ($this->socketPath !== '' && file_exists($this->socketPath)) {
            @unlink($this->socketPath); // ferrod stale-unlinks at bind, but be explicit
        }
        $this->launchFerrod($this->ferrodBin, $this->pgUrl);
        $this->waitUntilReady();
    }

    /** The repo-relative candidate binary paths, plus `FERRO_FERROD_BIN`. */
    private static function locateFerrod(): ?string
    {
        $override = getenv('FERRO_FERROD_BIN');
        if (is_string($override) && $override !== '' && is_executable($override)) {
            return $override;
        }
        $repoRoot = dirname(__DIR__, 4); // php/client/tests/Live -> repo root
        foreach (['/target/debug/ferrod', '/engine/target/debug/ferrod', '/target/release/ferrod'] as $rel) {
            $candidate = $repoRoot . $rel;
            if (is_executable($candidate)) { return $candidate; }
        }
        return null;
    }

    private function launchFerrod(string $bin, string $pgUrl): void
    {
        // Inherit the current environment, then add the ferrod config (verified recipe, D-S7-1).
        $env = getenv();
        $env['FERRO_SOCK'] = $this->socketPath;
        // `ferrod` resolves per-pool DSNs from FERRO_POOL_<env_name(NAME)>_DSN and infers each
        // pool's KIND from the DSN scheme (there is no kind= knob) —
        // engine/crates/ferrod/src/config.rs:88-104,:332. Pools are LAZY (Pool::new dials nothing),
        // so declaring a second one costs no connection until a request names it.
        $env['FERRO_POOLS'] = implode(',', $this->launchedPools());
        $env['FERRO_POOL_DEFAULT_DSN'] = $pgUrl;
        if ($this->mysqlUrl !== '') {
            $env['FERRO_POOL_MYSQL_DSN'] = $this->mysqlUrl;
        }

        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['file', $this->stderrPath, 'w'],
            2 => ['file', $this->stderrPath, 'w'],
        ];
        $pipes = [];
        $proc = proc_open([$bin], $descriptors, $pipes, null, $env);
        if (!is_resource($proc)) {
            $this->fail("proc_open failed to launch ferrod at {$bin}");
        }
        $this->proc = $proc;
        if (isset($pipes[0]) && is_resource($pipes[0])) { fclose($pipes[0]); }
    }

    /**
     * Poll (<= READY_TIMEOUT) until a FULL HELLO + `SELECT 1` round-trips — a bare socket connect
     * would pass even with an empty registry, so the probe exercises the real path. Fail FAST with
     * the captured stderr if ferrod exited.
     */
    private function waitUntilReady(): void
    {
        $deadline = microtime(true) + self::READY_TIMEOUT_SEC;
        $lastError = 'no attempt made';

        while (microtime(true) < $deadline) {
            $status = $this->procStatus();
            if ($status !== null && $status['running'] === false) {
                $this->fail(sprintf(
                    "ferrod exited during startup (exit code %s):\n%s",
                    (string) ($status['exitcode'] ?? '?'),
                    $this->readStderr(),
                ));
            }

            $session = null;
            try {
                $session = $this->connect();
                $session->hello();
                $outcome = $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, self::selectOnePayload());
                if ($outcome->isOk()) {
                    $session->close();
                    return;
                }
                $lastError = 'SELECT 1 returned a non-Ok outcome';
            } catch (\Throwable $e) {
                $lastError = $e->getMessage();
            } finally {
                if ($session !== null) {
                    try { $session->close(); } catch (\Throwable) { /* ignore */ }
                }
            }
            usleep(self::POLL_INTERVAL_US);
        }

        $this->fail(sprintf(
            "ferrod did not become ready within %.0fs. last error: %s\nstderr:\n%s",
            self::READY_TIMEOUT_SEC,
            $lastError,
            $this->readStderr(),
        ));
    }

    private function stopFerrod(): void
    {
        if ($this->proc === null || !is_resource($this->proc)) {
            return;
        }

        $status = $this->procStatus();
        if ($status !== null && $status['running'] === true) {
            @proc_terminate($this->proc, self::SIGTERM);
            $deadline = microtime(true) + self::STOP_TIMEOUT_SEC;
            while (microtime(true) < $deadline) {
                $s = $this->procStatus();
                if ($s === null || $s['running'] === false) { break; }
                usleep(self::POLL_INTERVAL_US);
            }
            $s = $this->procStatus();
            if ($s !== null && $s['running'] === true) {
                @proc_terminate($this->proc, self::SIGKILL);
            }
        }

        proc_close($this->proc);
        $this->proc = null;
    }

    /** @return array{running:bool,exitcode:int|null}|null */
    private function procStatus(): ?array
    {
        if ($this->proc === null || !is_resource($this->proc)) { return null; }
        $s = proc_get_status($this->proc);
        return ['running' => (bool) $s['running'], 'exitcode' => is_int($s['exitcode']) ? $s['exitcode'] : null];
    }

    private function readStderr(): string
    {
        if ($this->stderrPath === '' || !is_file($this->stderrPath)) { return '(no stderr captured)'; }
        $contents = @file_get_contents($this->stderrPath);
        return $contents === false || $contents === '' ? '(stderr empty)' : $contents;
    }

    /** A read-only `SELECT 1` against the `default` pool, fetch=rows. */
    private static function selectOnePayload(): string
    {
        return ExecRequest::encode([
            'pool' => 'default',
            'sql' => 'SELECT 1',
            'query_id' => null,
            'params' => [],
            'timeout_ms' => null,
            'readonly' => true,
            'fetch' => 0,
            'tx_id' => null,
        ], PackerFactory::forEncode());
    }
}
