<?php // /php/client/src/Client/Error/HydrationException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * A DTO hydration could not be built or applied: the result set has no column for a
 * constructor-promoted parameter (after snake_case→camelCase mapping), or the DTO has no
 * constructor. A client-usage error surfaced inside the {@see FerroException} tree so callers catch
 * it uniformly with the rest of the client surface.
 */
final class HydrationException extends FerroException {}
