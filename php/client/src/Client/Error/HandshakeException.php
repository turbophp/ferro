<?php // /php/client/src/Client/Error/HandshakeException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;

/**
 * The engine rejected the HELLO handshake with a session-fatal `Outcome::Error` terminal on
 * `request_id=0` (SPEC §5) — the daemon's `validate_hello` path (`error.rs terminal_frame(0, ep)`).
 * The dominant cause is a `type_registry_hash` / version mismatch, reported as
 * {@see Constants::ERR_UNSUPPORTED}; {@see isUnsupported} keys on that code. This is FATAL and never
 * retried: the client must regen/redeploy, not reconnect into the same rejection.
 *
 * The decoded {@see ErrorPayload} is carried verbatim so the caller can inspect code/branch/message.
 */
final class HandshakeException extends FerroException
{
    public function __construct(private readonly ErrorPayload $errorPayload, ?string $message = null)
    {
        parent::__construct($message ?? sprintf(
            'handshake rejected by engine: %s (code=%d, branch=%d)',
            $errorPayload->message,
            $errorPayload->code,
            $errorPayload->branch,
        ));
    }

    public function errorPayload(): ErrorPayload { return $this->errorPayload; }

    /** True when the rejection is the type-registry/version mismatch case (`ERR_UNSUPPORTED`). */
    public function isUnsupported(): bool { return $this->errorPayload->code === C::ERR_UNSUPPORTED; }
}
