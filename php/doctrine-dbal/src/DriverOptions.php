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
 *
 *      **What it actually does, BOTH halves — believing only one of them was a shipped defect.**
 *       1. **In a transaction the SERVER enforces it.** `beginTransaction()` carries the flag, the
 *          engine composes `BEGIN READ ONLY` / `START TRANSACTION READ ONLY`, and a write is refused
 *          (PostgreSQL `25006`, "cannot execute INSERT in a read-only transaction").
 *       2. **In autocommit nothing on the server CAN enforce it** — an autocommit statement has no
 *          transaction to carry the attribute. So the driver refuses what the SPI lets it see:
 *          `executeStatement()` without parameters (`Connection::exec()`, Doctrine's WRITE entry
 *          point) is rejected pre-send. A PARAMETERIZED `executeStatement('UPDATE … WHERE id = ?')`
 *          is indistinguishable from an `executeQuery()` at the SPI and still executes; what keeps
 *          THAT safe is that the declaration can no longer weaken a lost statement's fate
 *          ({@see \Ferro\DBAL\Connection::statementException}, which re-mints that one cell to §19.3
 *          `Indeterminate`).
 *
 *      Set it because the connection genuinely only reads. It buys the honest "statement cancelled"
 *      answer for a read killed by a server-side `statement_timeout` (§22.2 (ac)); it can no longer
 *      buy "safe to retry" for a statement that was lost in flight.
 *   `driverOptions.connect_timeout` / `driverOptions.io_timeout` — seconds, floats.
 *
 * **Anything else is REFUSED, loudly** ({@see refuseUnrecognised}). An ignored key is how a
 * misconfiguration becomes a wrong DATABASE: `['ferro' => ['pool' => 'main']]` — the shape this
 * repository's own README shipped, and the shape SPEC §14 documented before this slice — parses to
 * `pool = "default"`, i.e. a different pool, therefore a different DSN, therefore possibly a
 * different database, with no exception, no warning and no PHPStan error (it is the operator's
 * array, not the driver's). The whole-branch review measured exactly that.
 */
final class DriverOptions
{
    /**
     * The COMPLETE set of `driverOptions` keys this driver reads. `driverOptions` is Ferro's own
     * namespace — DBAL hands it to the driver verbatim and reads nothing out of it itself — so
     * anything unrecognised in there is a Ferro misconfiguration by construction, never another
     * layer's option. A leftover `PDO::ATTR_*` entry from the config this one replaced is refused
     * for the same reason: under Ferro it does nothing, and doing nothing quietly is the defect.
     */
    private const RECOGNISED = ['socket', 'pool', 'readonly', 'connect_timeout', 'io_timeout'];

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
        self::refuseUnrecognised($params, $opts);

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

    /**
     * The loud refusal of a configuration this driver would otherwise IGNORE.
     *
     * It runs before the transport check so the diagnosis names the actual mistake: a copied
     * `['ferro' => ['pool' => 'main']]` block with no socket would otherwise be reported as "no
     * engine transport configured", which is true and useless.
     *
     * Scope is deliberate. Top level, only the two keys this project itself published and then
     * removed are refused (`ferro`, `read_pool`) — DBAL's `$params` is a shared array that
     * middleware, DoctrineBundle and the application all write into, so a blanket unknown-key
     * refusal there would reject configurations that are none of our business. Inside
     * `driverOptions` the opposite holds: that array is ours alone, so the check is exhaustive.
     *
     * @param array<string,mixed> $params
     * @param array<string,mixed> $opts
     */
    private static function refuseUnrecognised(array $params, array $opts): void
    {
        if (array_key_exists('ferro', $params)) {
            throw new \InvalidArgumentException(
                'Ferro: the top-level `ferro` DBAL parameter is not read by this driver, and was '
                . 'silently ignored by earlier builds — which meant the connection quietly used '
                . 'pool "default", a different DSN and possibly a different database. Move it into '
                . "`driverOptions`:\n"
                . "    'driverOptions' => ['pool' => 'main', 'readonly' => false],\n"
                . 'There is no `read_pool` key; see the message for it below.',
            );
        }
        if (array_key_exists('read_pool', $params) || array_key_exists('read_pool', $opts)) {
            throw new \InvalidArgumentException(self::READ_POOL_MESSAGE);
        }

        $unknown = array_values(array_diff(array_keys($opts), self::RECOGNISED));
        if ($unknown !== []) {
            throw new \InvalidArgumentException(
                'Ferro: unrecognised driverOptions key(s): `' . implode('`, `', $unknown)
                . '`. This driver reads exactly `' . implode('`, `', self::RECOGNISED)
                . '` (plus the top-level `unix_socket`, `host` and `port`). An unrecognised key is '
                . 'refused rather than ignored: ignoring one is how a connection ends up on the '
                . 'wrong pool without a single diagnostic.',
            );
        }
    }

    /**
     * Its own constant because the `read_pool` mistake arrives from two directions — top level (the
     * README's removed example) and inside `driverOptions` (the natural place to retry it) — and the
     * two must not drift apart, since the answer is the same and is a DESIGN answer, not a spelling.
     */
    private const READ_POOL_MESSAGE =
        'Ferro: there is no `read_pool` option, at any level. Charter rule 6 forbids inferring '
        . 'read-vs-write from a statement, so a read/write split is a SECOND, explicitly configured '
        . "DBAL connection:\n"
        . "    'driverOptions' => ['pool' => 'main_ro', 'readonly' => true],\n"
        . 'Read `readonly`\'s two consequences in docs/known-incompatibilities.md first: it is a '
        . 'fate DECLARATION, not an enforcement, for autocommit statements, and it does make the '
        . 'engine open explicit transactions READ ONLY.';

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
