<?php // /php/client/tests/Live/HttpLiveTestCase.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

/**
 * Base for the Ferro HTTP live tests (M6-F8): a real `ferrod` (via {@see LiveTestCase}) serving two
 * upstreams that both point at one recording loopback upstream ({@see Fixtures/http-upstream.php})
 * started before the daemon:
 *
 *  - `up`  — `IDEMPOTENT_METHODS` empty (the default): only a request's own declaration makes it
 *    idempotent;
 *  - `ops` — the same origin with `IDEMPOTENT_METHODS=GET`: the operator's declaration, which the
 *    client can see only after `HEAD` (§23.7.3).
 *
 * Every request the upstream fully read is logged, so at-most-once is a read-back
 * ({@see received}).
 */
abstract class HttpLiveTestCase extends LiveTestCase
{
    /** @var resource|null */
    private $upstreamProc = null;
    private string $upstreamLog = '';
    private string $upstreamPortFile = '';
    protected int $upstreamPort = 0;

    protected function setUp(): void
    {
        $token = getmypid() . '-' . substr(hash('crc32b', static::class), 0, 8);
        $this->upstreamLog = sys_get_temp_dir() . "/ferro-http-up-{$token}.log";
        $this->upstreamPortFile = sys_get_temp_dir() . "/ferro-http-up-{$token}.port";
        @unlink($this->upstreamLog);
        @unlink($this->upstreamPortFile);
        touch($this->upstreamLog);
        $proc = proc_open(
            [PHP_BINARY, __DIR__ . '/Fixtures/http-upstream.php', $this->upstreamPortFile, $this->upstreamLog],
            [0 => ['pipe', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', $this->upstreamLog . '.err', 'w']],
            $pipes,
        );
        if (!is_resource($proc)) {
            $this->fail('could not start the loopback HTTP upstream');
        }
        $this->upstreamProc = $proc;
        $deadline = microtime(true) + 5.0;
        while (!is_file($this->upstreamPortFile) && microtime(true) < $deadline) {
            usleep(10_000);
        }
        $port = is_file($this->upstreamPortFile) ? (int) file_get_contents($this->upstreamPortFile) : 0;
        if ($port <= 0) {
            $this->fail('the loopback HTTP upstream did not report its port: ' . @file_get_contents($this->upstreamLog . '.err'));
        }
        $this->upstreamPort = $port;
        try {
            parent::setUp();
        } catch (\Throwable $e) {
            $this->stopUpstream(); // a skip or a failed launch must not leak the upstream process
            throw $e;
        }
    }

    private function stopUpstream(): void
    {
        if ($this->upstreamProc !== null) {
            @proc_terminate($this->upstreamProc, 9);
            proc_close($this->upstreamProc);
            $this->upstreamProc = null;
        }
    }

    protected function tearDown(): void
    {
        try {
            parent::tearDown();
        } finally {
            $this->stopUpstream();
            foreach ([$this->upstreamLog, $this->upstreamLog . '.err', $this->upstreamPortFile] as $f) {
                @unlink($f);
            }
        }
    }

    /**
     * Besides `up` and `ops`: `dead`, a loopback port nothing listens on (a dial failure), and
     * `tls`, an `https` origin on the plain-HTTP upstream's port with a 300 ms connect bound: the
     * upstream never answers the ClientHello, so the dial (DNS + TCP + TLS) times out.
     *
     * @return array<string, string>
     */
    protected function extraEnv(): array
    {
        $origin = 'http://127.0.0.1:' . $this->upstreamPort;
        return [
            'FERRO_UPSTREAMS' => 'up,ops,dead,tls',
            'FERRO_UPSTREAM_UP_ORIGIN' => $origin,
            'FERRO_UPSTREAM_UP_ADDRESS_CLASSES' => 'loopback',
            // The F8 slot tests park 256 streams on `up` to reach the CLIENT's own 256-request cap.
            // M6-F6's engine limits (MAX_CONNECTIONS 32, MAX_REQUESTS 128 by default) would refuse
            // first, so `up` is sized for that concurrency, as §23.8.2 tells operators to do.
            'FERRO_UPSTREAM_UP_MAX_CONNECTIONS' => '512',
            'FERRO_UPSTREAM_UP_MAX_REQUESTS' => '512',
            'FERRO_UPSTREAM_OPS_ORIGIN' => $origin,
            'FERRO_UPSTREAM_OPS_ADDRESS_CLASSES' => 'loopback',
            'FERRO_UPSTREAM_OPS_IDEMPOTENT_METHODS' => 'GET',
            'FERRO_UPSTREAM_DEAD_ORIGIN' => 'http://127.0.0.1:' . self::closedPort(),
            'FERRO_UPSTREAM_DEAD_ADDRESS_CLASSES' => 'loopback',
            'FERRO_UPSTREAM_TLS_ORIGIN' => 'https://127.0.0.1:' . $this->upstreamPort,
            'FERRO_UPSTREAM_TLS_ADDRESS_CLASSES' => 'loopback',
            'FERRO_UPSTREAM_TLS_CONNECT_TIMEOUT_MS' => '300',
        ];
    }

    /** A loopback port with nothing listening on it (bound, read, released). */
    private static function closedPort(): int
    {
        $s = stream_socket_server('tcp://127.0.0.1:0');
        if ($s === false) {
            return 1;
        }
        $name = (string) stream_socket_get_name($s, false);
        fclose($s);
        return (int) substr($name, strrpos($name, ':') + 1);
    }

    /** @return list<array<string, mixed>> every line the upstream logged, in order */
    protected function upstreamLog(): array
    {
        $out = [];
        foreach (file($this->upstreamLog, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [] as $line) {
            $decoded = json_decode($line, true);
            if (is_array($decoded)) {
                $out[] = $decoded;
            }
        }
        return $out;
    }

    /** How many requests whose target starts with `$prefix` the upstream fully read. */
    protected function received(string $prefix): int
    {
        return count(array_filter(
            $this->upstreamLog(),
            static fn (array $l): bool => isset($l['method']) && str_starts_with((string) $l['target'], $prefix),
        ));
    }

    /** How many connections that were serving `$prefix` the upstream saw closed by the engine. */
    protected function closedConnections(string $prefix): int
    {
        return count(array_filter(
            $this->upstreamLog(),
            static fn (array $l): bool => ($l['event'] ?? null) === 'closed' && str_starts_with((string) $l['target'], $prefix),
        ));
    }

    /** Poll until `$cond` holds (or fail after `$seconds`). */
    protected function eventually(\Closure $cond, float $seconds, string $what): void
    {
        $deadline = microtime(true) + $seconds;
        while (!$cond()) {
            if (microtime(true) >= $deadline) {
                $this->fail("timed out waiting for: {$what}");
            }
            usleep(20_000);
        }
        $this->addToAssertionCount(1);
    }
}
