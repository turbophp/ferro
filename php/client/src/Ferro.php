<?php // /php/client/src/Ferro.php
declare(strict_types=1);
namespace Ferro;

use Ferro\Client\Backoff;
use Ferro\Client\Connection;
use Ferro\Client\FateClassifier;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\RetryPolicy;
use Ferro\Client\Session;
use Ferro\Client\SessionInterface;
use Ferro\Client\Transport;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Client\Value\ValuePolicy;

/**
 * The M0 entry-point facade: open a transport, run the HELLO handshake, and hand back a
 * {@see Connection} bound to a pool. The config-only drop-in tiers (Doctrine/Eloquent, M1) build on
 * this.
 *
 * The returned {@see Connection} is RESILIENT by default (SPEC §19): a session factory that re-opens
 * the transport + re-handshakes is captured, and an epoch-aware {@see ReconnectLoop} + a
 * {@see RetryPolicy} are wired in, so a `Retryable` READ that loses the connection transparently
 * reconnects and re-issues (a lost WRITE / `Indeterminate` / lost COMMIT still propagates — §19.3).
 * Pass a {@see RetryPolicy} to tune the budget (`RetryPolicy::none()` disables retries).
 */
final class Ferro
{
    /**
     * Connect to a `ferrod` over its Unix domain socket, complete the handshake, and return a
     * ready, resilient {@see Connection}.
     *
     * @param string $socketPath the UDS path (e.g. `/run/ferro/{schema_hash}.sock`).
     * @param string $pool the pool name to bind requests to (must be advertised in HELLO_ACK).
     * @param ?TypePolicyOptions $types the SPEC §9.1 type policy (`decimal`, `naive_datetime_zone`,
     *   `u64_overflow`, `uuid`). Client-side in M1 — see {@see TypePolicyOptions} for why the engine
     *   has no matching knob. Defaults to the safe object forms.
     * @param ?ValuePolicy $values a ready-made §9.1 decode policy, MUTUALLY EXCLUSIVE with `$types`
     *   (a ValuePolicy already embeds whichever options it was built with, so passing both would
     *   silently discard one — {@see Connection::__construct} rejects the combination). This exists
     *   for the M1-S8b Doctrine tier, whose whole type boundary is a custom ValuePolicy: without it
     *   the facade's resilient wiring (ReconnectLoop + FateClassifier + epoch tracking) would have
     *   to be rebuilt inside the driver package.
     */
    public static function connect(
        string $socketPath,
        string $pool = 'default',
        float $connectTimeout = 2.0,
        float $ioTimeout = 5.0,
        ?RetryPolicy $policy = null,
        ?TypePolicyOptions $types = null,
        ?ValuePolicy $values = null,
    ): Connection {
        $factory = static function () use ($socketPath, $connectTimeout, $ioTimeout): SessionInterface {
            $session = new Session(Transport::connectUnix($socketPath, $connectTimeout, $ioTimeout));
            $session->hello();
            return $session;
        };
        return self::assemble($factory, $pool, $policy, $types, $values);
    }

    /**
     * Connect over TCP (the `FERRO_ADDR` fallback) instead of a Unix socket.
     *
     * @param ?TypePolicyOptions $types see {@see connect}.
     * @param ?ValuePolicy $values see {@see connect}.
     */
    public static function connectTcp(
        string $host,
        int $port,
        string $pool = 'default',
        float $connectTimeout = 2.0,
        float $ioTimeout = 5.0,
        ?RetryPolicy $policy = null,
        ?TypePolicyOptions $types = null,
        ?ValuePolicy $values = null,
    ): Connection {
        $factory = static function () use ($host, $port, $connectTimeout, $ioTimeout): SessionInterface {
            $session = new Session(Transport::connectTcp($host, $port, $connectTimeout, $ioTimeout));
            $session->hello();
            return $session;
        };
        return self::assemble($factory, $pool, $policy, $types, $values);
    }

    /**
     * Open the first session via `$factory`, then wire the epoch-aware reconnect loop + fate
     * classifier around it.
     *
     * `$types` is deliberately REQUIRED (no `= null` default), and that is load-bearing rather than
     * stylistic: with a default, dropping `$types` from either `self::assemble(...)` call above left
     * PHPUnit green AND PHPStan level 9 clean while `Ferro::connect(types: …)` became an inert
     * public knob — every DECIMAL/TIMESTAMP/UUID/U64 read silently reverting to the default policy.
     * Required, that same drop is a static error (`invoked with 3 parameters, 4 required`), so the
     * forward cannot rot unnoticed. The behavioural half of the guard is
     * `tests/Live/TypesLiveTest::testFerroConnectForwardsTheTypePolicyLive`.
     *
     * `$values` (M1-S8b) is REQUIRED here for exactly the same reason, and it needs the behavioural
     * half just as much: dropping `values: $values` from the `new Connection(...)` below leaves the
     * offline suite AND PHPStan level 9 green while `Ferro::connect(values: …)` decodes with the
     * default §9.1 policy. `tests/Live/ValuePolicyFacadeLiveTest` is the guard that goes red.
     *
     * @param \Closure(): SessionInterface $factory
     */
    private static function assemble(
        \Closure $factory,
        string $pool,
        ?RetryPolicy $policy,
        ?TypePolicyOptions $types,
        ?ValuePolicy $values,
    ): Connection {
        $policy ??= RetryPolicy::default();
        $session = $factory();
        $loop = new ReconnectLoop(
            $session,
            $factory,
            new Backoff($policy->baseDelaySeconds, $policy->maxDelaySeconds),
            $policy->maxAttempts,
        );
        return new Connection(
            session: $session,
            pool: $pool,
            reconnect: $loop,
            policy: $policy,
            fate: new FateClassifier($policy->retryReads),
            values: $values,
            types: $types,
        );
    }
}
