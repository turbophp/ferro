<?php // /php/client/src/Client/Error/ConnectionLostException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

use Ferro\Protocol\ErrorPayload;

/**
 * The session died out from under an in-flight request: either the transport itself failed, or the
 * engine emitted a session-fatal terminal on `request_id=0` with `flags::END` (a `Fatal`
 * `SessionError`, `error.rs`). This is the signal the S7 reconnect loop + fate classifier (Task 4)
 * act on — compare the reconnected `boot_epoch`, decide retry-eligibility from (branch, readonly,
 * op-kind). When the engine sent a decoded reason it is carried in {@see errorPayload}; a pure
 * transport failure carries `null`.
 *
 * Deliberately a distinct class from a generic {@see ProtocolException}: a `request_id=0` terminal
 * must NOT be mistaken for an id-mismatch, which would mask a real session-fatal and its fate.
 */
final class ConnectionLostException extends FerroException
{
    public function __construct(string $message, private readonly ?ErrorPayload $errorPayload = null)
    {
        parent::__construct($message);
    }

    public function errorPayload(): ?ErrorPayload { return $this->errorPayload; }
}
