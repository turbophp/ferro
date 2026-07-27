<?php // /php/client/src/Client/Error/CarriesErrorPayload.php
declare(strict_types=1);
namespace Ferro\Client\Error;

use Ferro\Protocol\ErrorPayload;

/**
 * Shared behaviour for the three-branch taxonomy exceptions ({@see RetryableException},
 * {@see IndeterminateException}, {@see NonRetryableException}): each is classified from the wire
 * `branch` byte and carries the decoded {@see ErrorPayload} verbatim so the caller (and the Task-4
 * fate classifier) can read `code` / `branch` / `sqlstate` off the raised exception.
 *
 * A trait (not an intermediate base class) so every taxonomy exception `extends FerroException`
 * DIRECTLY — the tree stays flat and `instanceof FerroException` catches them all.
 */
trait CarriesErrorPayload
{
    public function __construct(private readonly ErrorPayload $errorPayload)
    {
        parent::__construct(sprintf(
            '%s (code=%d, branch=%d%s)',
            $errorPayload->message,
            $errorPayload->code,
            $errorPayload->branch,
            $errorPayload->sqlstate !== null ? ', sqlstate=' . $errorPayload->sqlstate : '',
        ));
    }

    /** The decoded engine/backend error carried verbatim from the `Outcome::Error` terminal. */
    public function errorPayload(): ErrorPayload { return $this->errorPayload; }

    /** The wire error code (`/proto errors.toml`); classification is on {@see branch}, not this. */
    public function errorCode(): int { return $this->errorPayload->code; }

    /** The wire branch byte the exception class was chosen from (1/2/3). */
    public function branch(): int { return $this->errorPayload->branch; }

    /** The SQLSTATE if the backend supplied one (e.g. `42601` for a syntax error), else null. */
    public function sqlstate(): ?string { return $this->errorPayload->sqlstate; }
}
