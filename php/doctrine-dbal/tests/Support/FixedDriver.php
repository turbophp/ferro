<?php // /php/doctrine-dbal/tests/Support/FixedDriver.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Support;

use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\API\ExceptionConverter as ExceptionConverterInterface;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\ServerVersionProvider;
use Ferro\DBAL\Connection as FerroDriverConnection;
use Ferro\DBAL\ExceptionConverter;
use Ferro\DBAL\PlatformVersion;

/**
 * A `Doctrine\DBAL\Driver` that hands back a driver connection someone else built.
 *
 * It exists so a test can drive the REAL `Doctrine\DBAL\Connection` — its client-side transaction
 * nesting, its savepoint generation, its statement routing — against a `Ferro\DBAL\Connection`
 * sitting on a scripted `FakeSession`, with no socket and no ferrod. Everything above the driver
 * SPI is the genuine Doctrine code path; only the transport underneath is fake.
 *
 * It is NOT a mock of `Ferro\DBAL\Driver`: platform selection and exception conversion delegate to
 * the same `Ferro\DBAL\{PlatformVersion,ExceptionConverter}` production code, so a test that reaches
 * either of those reaches the shipped implementation.
 */
final class FixedDriver implements DriverInterface
{
    public function __construct(
        private readonly FerroDriverConnection $conn,
        private readonly string $kind = PlatformVersion::KIND_POSTGRES,
    ) {}

    /** @param array<string,mixed> $params */
    public function connect(#[\SensitiveParameter] array $params): FerroDriverConnection
    {
        return $this->conn;
    }

    public function getDatabasePlatform(ServerVersionProvider $versionProvider): AbstractPlatform
    {
        return PlatformVersion::platformFor($this->kind, $versionProvider->getServerVersion());
    }

    public function getExceptionConverter(): ExceptionConverterInterface
    {
        return new ExceptionConverter($this->kind);
    }
}
