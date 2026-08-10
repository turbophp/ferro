<?php // /php/doctrine-dbal/src/FixedVersion.php
declare(strict_types=1);
namespace Ferro\DBAL;

use Doctrine\DBAL\ServerVersionProvider;

/**
 * A `ServerVersionProvider` over a string we already hold. DBAL ships its own
 * (`Doctrine\DBAL\Connection\StaticServerVersionProvider`), but it is an internal detail of the
 * wrapper `Connection` rather than a documented extension point, and this is eight lines.
 */
final class FixedVersion implements ServerVersionProvider
{
    public function __construct(private readonly string $version) {}

    public function getServerVersion(): string
    {
        return $this->version;
    }
}
