<?php // /php/client/src/Client/Error/CancelledException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * The engine terminated the request with `Outcome::Cancelled` (status 2) rather than an
 * `Ok`/`Error` — a deadline/cancel signal, not a backend error, so it carries no `ErrorPayload`.
 * Treated as NON-retryable: the client never silently re-runs a cancelled request.
 */
final class CancelledException extends FerroException
{
    public function __construct(string $message = 'request was cancelled by the engine (Outcome::Cancelled)')
    {
        parent::__construct($message);
    }
}
