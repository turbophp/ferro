<?php // /php/laravel/src/ConnectionOptions.php
declare(strict_types=1);
namespace Ferro\Laravel;

/**
 * The Ferro-specific slice of a Laravel `config/database.php` connection array.
 *
 * §15's goal is that adoption is a `driver` change and nothing else, so this reads the keys a
 * Ferro connection needs and deliberately IGNORES the ones that no longer mean anything: `host`,
 * `port`, `username`, `password` and `database` describe an upstream the application no longer
 * dials — credentials live in `ferrod` (§12, D8). They are not rejected, because a real config
 * array arrives carrying them and refusing would make adoption a rewrite rather than a one-line
 * change; they are simply not read.
 */
final class ConnectionOptions
{
    private function __construct(
        public readonly string $pool,
        public readonly ?string $socketPath,
        public readonly ?string $host,
        public readonly int $port,
        public readonly float $connectTimeout,
        public readonly float $ioTimeout,
    ) {}

    /**
     * @param array<string,mixed> $config the connection's `config/database.php` array
     * @throws \InvalidArgumentException if neither a socket nor a host is configured
     */
    public static function fromConfig(array $config): self
    {
        $pool = self::str($config, 'pool') ?? 'default';
        $socket = self::str($config, 'ferro_socket') ?? self::str($config, 'unix_socket');
        $host = self::str($config, 'ferro_host');
        $port = self::int($config, 'ferro_port') ?? 7777;

        if ($socket === null && $host === null) {
            throw new \InvalidArgumentException(
                'Ferro: a connection needs either "ferro_socket" (the ferrod UDS path, the normal '
                . 'deployment) or "ferro_host"/"ferro_port" (the TCP fallback). Note "host" is NOT '
                . 'read: it describes the upstream database, which the application no longer dials.',
            );
        }

        return new self(
            $pool,
            $socket,
            $host,
            $port,
            self::float($config, 'ferro_connect_timeout') ?? 2.0,
            self::float($config, 'ferro_io_timeout') ?? 30.0,
        );
    }

    /** @param array<string,mixed> $c */
    private static function str(array $c, string $k): ?string
    {
        $v = $c[$k] ?? null;
        return is_string($v) && $v !== '' ? $v : null;
    }

    /** @param array<string,mixed> $c */
    private static function int(array $c, string $k): ?int
    {
        $v = $c[$k] ?? null;
        return is_int($v) ? $v : (is_string($v) && ctype_digit($v) ? (int) $v : null);
    }

    /** @param array<string,mixed> $c */
    private static function float(array $c, string $k): ?float
    {
        $v = $c[$k] ?? null;
        return is_int($v) || is_float($v) ? (float) $v : null;
    }
}
