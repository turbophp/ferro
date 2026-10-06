<?php // /php/client/src/Client/Error/ManifestException.php
declare(strict_types=1);
namespace Ferro\Client\Error;

/**
 * A checked-SQL manifest could not be used (SPEC §11, M3-D2e): it does not load, its recorded hash
 * is not the hash of its queries, or a `…ById` call names an id it does not declare. Raised before
 * anything is sent, so it carries no fate.
 */
final class ManifestException extends FerroException {}
