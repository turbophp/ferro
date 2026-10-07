<?php // /php/psr18/src/Psr18/UnmappedOriginException.php
declare(strict_types=1);
namespace Ferro\Psr18;

/**
 * The request's origin is not in the client's upstream map (SPEC §23.11.2): a configuration error,
 * loud by design. Nothing was sent, and nothing falls back to another transport — a request Ferro
 * cannot route never leaves PHP by another path, which is the SSRF rule working.
 */
final class UnmappedOriginException extends RequestException
{
}
