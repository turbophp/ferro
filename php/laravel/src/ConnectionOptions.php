<?php // /php/laravel/src/ConnectionOptions.php
declare(strict_types=1);
namespace Ferro\Laravel;

/**
 * The Ferro-specific slice of a Laravel `config/database.php` connection array.
 *
 * §15's goal is that adoption is a `driver` change and nothing else, so this reads the keys a
 * Ferro connection needs and deliberately IGNORES the ones that no longer mean anything: `host`,
 * `port`, `username`, `password`, `unix_socket` and `database` describe an upstream the
 * application no longer dials — credentials live in `ferrod` (§12, D8). They are not rejected, because a real config
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
        public readonly ?bool $receiveFds = null,
    ) {}

    /**
     * @param array<string,mixed> $config the connection's `config/database.php` array
     * @throws \InvalidArgumentException if neither a socket nor a host is configured
     */
    public static function fromConfig(array $config): self
    {
        $pool = self::str($config, 'pool') ?? 'default';
        // `ferro_socket` ONLY. A stock MySQL config carries `unix_socket` (Laravel's `DB_SOCKET`)
        // pointing at mysqld's own socket; reading it as the ferrod socket turned a leftover key into
        // `bad magic 0x6b` with no hint at the cause (M2-C1f review). Ignored, like `host`.
        $socket = self::str($config, 'ferro_socket');
        $host = self::str($config, 'ferro_host');
        $port = self::int($config, 'ferro_port') ?? 7777;

        if ($socket === null && $host === null) {
            throw new \InvalidArgumentException(
                'Ferro: a connection needs either "ferro_socket" (the ferrod UDS path, the normal '
                . 'deployment) or "ferro_host"/"ferro_port" (the TCP fallback). Note "host" is NOT '
                . 'read: it describes the upstream database, which the application no longer dials.',
            );
        }

        $receiveFds = self::nullableBool($config, 'ferro_receive_fds');
        if ($socket === null && $receiveFds === true) {
            // `true` insists (`Ferro::connect(receiveFds: true)` throws when it cannot), and the TCP
            // fallback can never receive an fd: refused rather than quietly ignored.
            throw new \InvalidArgumentException(
                'Ferro: "ferro_receive_fds" => true needs "ferro_socket"; the TCP fallback '
                . '("ferro_host"/"ferro_port") cannot receive fds. Leave it unset for auto.',
            );
        }

        return new self(
            $pool,
            $socket,
            $host,
            $port,
            self::float($config, 'ferro_connect_timeout') ?? 2.0,
            self::float($config, 'ferro_io_timeout') ?? 30.0,
            $receiveFds,
        );
    }

    /**
     * `ferro_receive_fds` (SPEC §5.1, `Ferro::connect(receiveFds:)`): unset or null is auto; `false`
     * opts the connection out of receiving large results as a memfd. Also read from the strings an
     * uncast `env()` hands over. Anything else is refused rather than guessed, since the point of the
     * key is to switch a path OFF.
     *
     * @param array<string,mixed> $c
     */
    private static function nullableBool(array $c, string $k): ?bool
    {
        $v = $c[$k] ?? null;
        if ($v === null || is_bool($v)) {
            return $v;
        }
        if (is_string($v)) {
            $t = strtolower(trim($v));
            if (in_array($t, ['', 'null', 'auto'], true)) {
                return null;
            }
            if (in_array($t, ['true', '1', 'on', 'yes'], true)) {
                return true;
            }
            if (in_array($t, ['false', '0', 'off', 'no'], true)) {
                return false;
            }
        }
        if ($v === 0 || $v === 1) {
            return $v === 1;
        }
        throw new \InvalidArgumentException("Ferro: \"$k\" must be true, false or null (auto).");
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
