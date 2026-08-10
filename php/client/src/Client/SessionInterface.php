<?php // /php/client/src/Client/SessionInterface.php
declare(strict_types=1);
namespace Ferro\Client;

use Ferro\Protocol\Outcome;
use Ferro\Protocol\PoolInfo;

/**
 * The post-handshake surface the S7 runtime drives: send one request → read its one terminal, read
 * the cached opaque `boot_epoch`, learn the last in-flight `(service, method)` (the §19.3 lost-COMMIT
 * carve-out keys on it), and close. Abstracted so the {@see Connection} / {@see TxHandle} /
 * {@see ReconnectLoop} can be unit-tested over a scripted fake session — no socket, no ferrod — while
 * the real {@see Session} implements it over a live {@see TransportInterface}.
 *
 * HELLO is deliberately NOT on this interface: the handshake runs once at connect time (via the
 * concrete {@see Session}/{@see \Ferro\Ferro} facade or the {@see ReconnectLoop} factory), and the
 * runtime only ever touches a session that is already handshaken.
 */
interface SessionInterface
{
    /**
     * Send one request-bearing frame and block-read its single terminal `Outcome`. A session-fatal
     * `request_id=0` terminal or a dead transport surfaces as a
     * {@see \Ferro\Client\Error\ConnectionLostException} / {@see \Ferro\Client\Error\TransportException}.
     */
    public function sendRequest(int $service, int $method, string $payload): Outcome;

    /**
     * The opaque `boot_epoch` cached at handshake — `int`, or a decimal STRING for a `u64 > PHP_INT_MAX`.
     * NEVER `(int)`-coerce it: the reconnect loop compares epochs with strict `===` so a real restart
     * is always detected (coercion would collapse distinct large epochs and void nothing, §19.1/§19.3).
     */
    public function bootEpoch(): int|string;

    /**
     * The `(service, method)` of the frame most recently put on the wire by {@see sendRequest}, or
     * null if none has been sent. In the synchronous single-in-flight model this is exactly the op
     * that was in flight when a {@see \Ferro\Client\Error\ConnectionLostException} fired — the signal
     * the §19.3 lost-COMMIT carve-out reads to force `Indeterminate`.
     *
     * @return array{0:int,1:int}|null
     */
    public function lastInFlight(): ?array;

    /**
     * The pool metadata this session's `HELLO_ACK` advertised — name, backend family, and the
     * backend's own `version()` string VERBATIM (or null when the engine has not learned it).
     *
     * On the interface (M1-S8b) rather than on the concrete {@see Session} alone because the
     * Doctrine tier chooses its SQL DIALECT from it: `Ferro\DBAL\Driver::getDatabasePlatform()`
     * needs the backend family, and MariaDB-vs-MySQL is decided by the version string alone (both
     * report `kind = "mysql"`). Reaching it through an `instanceof Session` narrowing would make
     * every fake session in a driver unit test unusable.
     *
     * It is a SNAPSHOT taken once during the handshake — re-reading it on the same session can
     * never yield a new value. A caller that needs a fresher answer must re-handshake (which the
     * {@see ReconnectLoop} may already have done, replacing this object) or ask the backend.
     *
     * @return list<PoolInfo>
     */
    public function poolInfo(): array;

    /** Best-effort GOODBYE, then close the transport. Idempotent. */
    public function close(): void;
}
