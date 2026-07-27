<?php // /php/client/src/Client/Transport.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Client\Error\TransportException;

/**
 * Dependency-free stream transport (charter rule 7): `stream_socket_client` over `unix://`
 * (primary, `/run/ferro/{schema_hash}.sock`) or `tcp://` (the `FERRO_ADDR` fallback) — NOT
 * `ext-sockets`. Blocking, single-in-flight: the S7 M0 client writes one framed request and
 * block-reads exactly its one terminal.
 *
 * The stream is held as `/** @var resource *​/ private $sock` (a stream resource has no native
 * property type, so this class is deliberately NOT `readonly`); both
 * `stream_socket_client`'s `resource|false` and `fread`'s `string|false` are handled explicitly so
 * the type stays a bare `resource` for PHPStan level 9.
 */
final class Transport implements TransportInterface
{
    private const DEFAULT_CONNECT_TIMEOUT = 5.0;
    private const DEFAULT_READ_TIMEOUT = 30.0;

    /** @var resource the connected stream */
    private $sock;

    /**
     * @param resource $sock an already-connected, blocking stream
     */
    private function __construct($sock)
    {
        $this->sock = $sock;
    }

    /** Connect over a Unix domain socket at `$socketPath` (the primary transport). */
    public static function connectUnix(
        string $socketPath,
        float $connectTimeout = self::DEFAULT_CONNECT_TIMEOUT,
        float $readTimeout = self::DEFAULT_READ_TIMEOUT,
    ): self {
        return self::open('unix://' . $socketPath, $connectTimeout, $readTimeout);
    }

    /** Connect over TCP (the `FERRO_ADDR` fallback), e.g. host `127.0.0.1`, port `7777`. */
    public static function connectTcp(
        string $host,
        int $port,
        float $connectTimeout = self::DEFAULT_CONNECT_TIMEOUT,
        float $readTimeout = self::DEFAULT_READ_TIMEOUT,
    ): self {
        return self::open('tcp://' . $host . ':' . $port, $connectTimeout, $readTimeout);
    }

    private static function open(string $remote, float $connectTimeout, float $readTimeout): self
    {
        $errno = 0;
        $errstr = '';
        // STREAM_CLIENT_CONNECT is the default; being explicit keeps the intent clear.
        $sock = @stream_socket_client(
            $remote,
            $errno,
            $errstr,
            $connectTimeout,
            STREAM_CLIENT_CONNECT,
        );
        if ($sock === false) {
            throw new TransportException(sprintf(
                'connect failed to %s: %s (errno %d)',
                $remote,
                $errstr !== '' ? $errstr : 'unknown error',
                $errno,
            ));
        }
        stream_set_blocking($sock, true);
        // Split the read timeout into whole seconds + microseconds for stream_set_timeout.
        $sec = (int) $readTimeout;
        $usec = (int) round(($readTimeout - $sec) * 1_000_000);
        stream_set_timeout($sock, $sec, $usec);
        return new self($sock);
    }

    public function readExact(int $n): string
    {
        if ($n < 0) { throw new TransportException("readExact: negative length {$n}"); }
        if ($n === 0) { return ''; }

        $buf = '';
        $remaining = $n;
        while ($remaining > 0) {
            $chunk = fread($this->sock, $remaining);
            if ($chunk === false || $chunk === '') {
                $meta = stream_get_meta_data($this->sock);
                if ($meta['timed_out'] === true) {
                    throw new TransportException(sprintf('read timed out after %d of %d bytes', $n - $remaining, $n));
                }
                if (feof($this->sock)) {
                    throw new TransportException(sprintf('unexpected EOF after %d of %d bytes', $n - $remaining, $n));
                }
                throw new TransportException(sprintf('read failed after %d of %d bytes', $n - $remaining, $n));
            }
            $buf .= $chunk;
            $remaining -= strlen($chunk);
        }
        return $buf;
    }

    public function writeAll(string $bytes): void
    {
        $len = strlen($bytes);
        $written = 0;
        while ($written < $len) {
            $n = fwrite($this->sock, substr($bytes, $written));
            if ($n === false || $n === 0) {
                $meta = stream_get_meta_data($this->sock);
                if ($meta['timed_out'] === true) {
                    throw new TransportException(sprintf('write timed out after %d of %d bytes', $written, $len));
                }
                throw new TransportException(sprintf('write failed after %d of %d bytes', $written, $len));
            }
            $written += $n;
        }
    }

    public function close(): void
    {
        if (is_resource($this->sock)) {
            fclose($this->sock);
        }
    }
}
