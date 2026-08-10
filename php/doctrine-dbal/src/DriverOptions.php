<?php // /php/doctrine-dbal/src/DriverOptions.php
declare(strict_types=1);
namespace Ferro\DBAL;

/**
 * The typed read of DBAL's `$params`. One responsibility: turn `array<string,mixed>` into narrowed
 * scalars, loudly, so nothing downstream has to guess.
 *
 * **Configuration lives in `driverOptions`, not in a top-level `ferro` key.** SPEC §14's example
 * shows `'ferro' => ['pool' => …]`, but `Doctrine\DBAL\Driver::connect()` is `@phpstan-param Params`
 * and `Params` is a SEALED array shape with no such key — reading it MEASURED as two
 * `nullCoalesce.offset` errors at PHPStan level 9, which is a charter Definition-of-Done gate (and
 * Symfony's `doctrine.dbal` config rejects unknown top-level keys too). `driverOptions?: array<mixed>`
 * is the sanctioned slot. §14 is amended in the same slice.
 *
 * Recognised keys:
 *   `unix_socket` (top level) or `driverOptions.socket` — the ferrod UDS path.
 *   `host` + `port` (top level) — the `FERRO_ADDR` TCP fallback.
 *   `driverOptions.pool` — the engine pool name; defaults to `default`.
 *   `driverOptions.readonly` — declare EVERY statement on this connection a read for §19.3 fate
 *      purposes. Off by default and deliberately explicit: the DBAL SPI carries no read/write
 *      signal and charter rule 6 forbids inferring one, so the safe default is "write". This is the
 *      charter-compliant shape of §14's `read_pool` idea — a second, explicitly-configured
 *      connection, never inference.
 *   `driverOptions.connect_timeout` / `driverOptions.io_timeout` — seconds, floats.
 */
final class DriverOptions
{
    private function __construct(
        public readonly ?string $socketPath,
        public readonly ?string $host,
        public readonly int $port,
        public readonly string $pool,
        public readonly bool $readonly,
        public readonly float $connectTimeout,
        public readonly float $ioTimeout,
    ) {}

    /** @param array<string,mixed> $params */
    public static function fromParams(array $params): self
    {
        $raw = $params['driverOptions'] ?? [];
        if (!is_array($raw)) {
            throw new \InvalidArgumentException('Ferro: `driverOptions` must be an array.');
        }
        /** @var array<string,mixed> $opts */
        $opts = $raw;

        $socket = self::optString($opts, 'socket');
        if ($socket === null && isset($params['unix_socket']) && is_string($params['unix_socket'])) {
            $socket = $params['unix_socket'];
        }
        $host = null;
        if (isset($params['host']) && is_string($params['host'])) {
            $host = $params['host'];
        }
        $port = 0;
        if (isset($params['port']) && is_int($params['port'])) {
            $port = $params['port'];
        }
        if ($socket === null && $host === null) {
            throw new \InvalidArgumentException(
                'Ferro: no engine transport configured. Set `unix_socket` (or '
                . '`driverOptions.socket`) to the ferrod socket path, or `host`+`port` for the TCP '
                . 'fallback. Ferro holds no database credentials in PHP — the DSN lives in the '
                . 'engine (SPEC §12 / D8).',
            );
        }

        return new self(
            $socket,
            $host,
            $port === 0 ? 7777 : $port,
            self::optString($opts, 'pool') ?? 'default',
            self::optBool($opts, 'readonly'),
            self::optFloat($opts, 'connect_timeout') ?? 2.0,
            self::optFloat($opts, 'io_timeout') ?? 5.0,
        );
    }

    /** @param array<string,mixed> $opts */
    private static function optString(array $opts, string $key): ?string
    {
        if (!array_key_exists($key, $opts)) {
            return null;
        }
        $v = $opts[$key];
        if (!is_string($v)) {
            throw new \InvalidArgumentException("Ferro: driverOptions.$key must be a string.");
        }
        return $v;
    }

    /** @param array<string,mixed> $opts */
    private static function optBool(array $opts, string $key): bool
    {
        if (!array_key_exists($key, $opts)) {
            return false;
        }
        $v = $opts[$key];
        if (!is_bool($v)) {
            throw new \InvalidArgumentException("Ferro: driverOptions.$key must be a bool.");
        }
        return $v;
    }

    /** @param array<string,mixed> $opts */
    private static function optFloat(array $opts, string $key): ?float
    {
        if (!array_key_exists($key, $opts)) {
            return null;
        }
        $v = $opts[$key];
        if (!is_float($v) && !is_int($v)) {
            throw new \InvalidArgumentException("Ferro: driverOptions.$key must be a number.");
        }
        return (float) $v;
    }
}
