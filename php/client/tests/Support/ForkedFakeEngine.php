<?php // /php/client/tests/Support/ForkedFakeEngine.php
declare(strict_types=1);
namespace Ferro\Tests\Support;

use Ferro\Protocol\Codec;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;

/**
 * A scripted "engine" in a FORKED child process, listening on a Unix socket (M3-D1c review F7).
 *
 * The deadline and liveness rules are about TIME, and the in-memory fakes cannot answer while the
 * client is blocked in a read or a `stream_select`; a real `ferrod` cannot be told to ignore a
 * `timeout_ms`, to answer a PONG after a terminal, or to stop reading. A child process can. It
 * accepts ONE connection, answers the HELLO, and then runs `$serve` with the connected stream; the
 * helpers below build and read frames. The child ends with SIGKILL on itself, so none of PHPUnit's
 * shutdown machinery runs twice.
 *
 * Needs `pcntl` and `posix` (test-only; the client itself needs neither — charter rule 7).
 */
final class ForkedFakeEngine
{
    private function __construct(public readonly string $path, private readonly int $pid) {}

    /** @param \Closure(resource): void $serve */
    public static function start(\Closure $serve, int $features = 0): self
    {
        static $n = 0;
        $path = sys_get_temp_dir() . '/ferro-fake-' . getmypid() . '-' . (++$n) . '.sock';
        @unlink($path);
        $server = stream_socket_server('unix://' . $path, $errno, $errstr);
        if ($server === false) {
            throw new \RuntimeException("fake engine: cannot listen on {$path}: {$errstr}");
        }
        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new \RuntimeException('fake engine: fork failed');
        }
        if ($pid === 0) {
            try {
                $c = @stream_socket_accept($server, 10.0);
                if ($c !== false) {
                    stream_set_timeout($c, 30);
                    self::readFrame($c); // HELLO
                    fwrite($c, self::helloAck($features));
                    $serve($c);
                }
            } catch (\Throwable $e) {
                fwrite(STDERR, 'fake engine: ' . $e->getMessage() . "\n");
            }
            posix_kill(getmypid(), SIGKILL);
            exit(0); // unreachable
        }
        fclose($server);
        return new self($path, $pid);
    }

    public function stop(): void
    {
        @posix_kill($this->pid, SIGKILL);
        pcntl_waitpid($this->pid, $status);
        @unlink($this->path);
    }

    /**
     * @param resource $c
     * @return array{0:Header,1:string}|null null at EOF
     */
    public static function readFrame($c): ?array
    {
        $h = self::readN($c, 16);
        if ($h === null) {
            return null;
        }
        $header = Header::decode($h);
        $payload = $header->payloadLen > 0 ? self::readN($c, $header->payloadLen) : '';
        return $payload === null ? null : [$header, $payload];
    }

    /** @param resource $c */
    private static function readN($c, int $n): ?string
    {
        $buf = '';
        while (strlen($buf) < $n) {
            $b = @fread($c, $n - strlen($buf));
            if ($b === false || $b === '') {
                return null;
            }
            $buf .= $b;
        }
        return $buf;
    }

    public static function frame(int $flags, int $service, int $method, int $rid, string $payload): string
    {
        return (new Codec())->encodeFrame(new Header($flags, $service, $method, $rid, strlen($payload)), $payload);
    }

    public static function helloAck(int $features = 0): string
    {
        $payload = Message::encode('hello_ack', [
            'engine_version' => 1, 'boot_epoch' => 1, 'features' => $features,
            'pools' => [['name' => 'default', 'kind' => 'postgres', 'server_version' => '17.0', 'literals_are_standard' => true]],
            'type_registry_hash' => C::TYPE_REGISTRY_HASH,
        ], PackerFactory::forEncode());
        return self::frame(0, C::SERVICE_CORE, C::METHOD_CORE_HELLO_ACK, 0, $payload);
    }

    /** An Ok EXEC terminal with one row holding the integer `$value`. */
    public static function ok(int $rid, int $value = 1): string
    {
        $p = PackerFactory::forEncode();
        $body = ExecOk::encode([
            'cols' => [['name' => 'v', 'tag' => C::TAG_I64]],
            'rows' => [[['tag' => C::TAG_I64, 'data' => $value]]],
            'affected' => 1, 'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ], $p);
        return self::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, Outcome::ok($body)->encode($p));
    }

    /** The terminal a CANCELled EXEC gets. */
    public static function cancelled(int $rid): string
    {
        return self::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, Outcome::cancelled()->encode(PackerFactory::forEncode()));
    }

    public static function pong(int $rid): string
    {
        return self::frame(0, C::SERVICE_CORE, C::METHOD_CORE_PONG, $rid, Message::encode('pong', ['token' => $rid], PackerFactory::forEncode()));
    }

    public static function isPing(Header $h): bool
    {
        return $h->service === C::SERVICE_CORE && $h->method === C::METHOD_CORE_PING;
    }

    public static function isCancel(Header $h): bool
    {
        return ($h->flags & C::FLAG_CANCEL) !== 0;
    }

    public static function isExec(Header $h): bool
    {
        return $h->service === C::SERVICE_SQL && $h->method === C::METHOD_SQL_EXEC && !self::isCancel($h);
    }
}
