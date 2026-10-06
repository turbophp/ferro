<?php // /php/client/src/Http/Error/RequestTooLargeException.php
declare(strict_types=1);
namespace Ferro\Http\Error;

use Ferro\Client\Error\FerroException;

/**
 * The `REQUEST` frame — body included — would exceed `MAX_FRAME_PAYLOAD` (16 MiB), so it was refused
 * before a byte was written (SPEC §23.9.3, §22.2 (ak)). Nothing reached the engine: it is "not sent",
 * but retrying cannot help, and v1 has no large-request-body path (§23.18 Q3).
 */
final class RequestTooLargeException extends FerroException
{
}
