<?php // /php/client/src/Client/Error/FerroException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * Root of the Ferro client exception tree. The three-branch taxonomy
 * (Retryable / Indeterminate / NonRetryable) mapped from the wire `branch` byte lands in Task 3
 * as subclasses; the S7 Task-2 surface already needs the base plus the transport/protocol/
 * handshake/connection-lost fatals below.
 */
class FerroException extends \RuntimeException {}
