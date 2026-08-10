<?php // /php/doctrine-dbal/tests/Live/DbalLiveTestCase.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\DriverManager;
use Ferro\Client\Connection as FerroClientConnection;
use Ferro\Tests\Live\LiveTestCase;

/**
 * The base class for every S8b live test. It inherits `Ferro\Tests\Live\LiveTestCase` wholesale —
 * reached through this package's own `autoload-dev` mapping of `Ferro\Tests\` to `../client/tests/`,
 * which works because the path repository installs `vendor/ferro/client` as a SYMLINK.
 *
 * Inheriting rather than re-implementing is deliberate: `LiveTestCase::waitUntilReady()` does a full
 * HELLO plus a real `SELECT 1` against the real upstream before any test body runs, and that
 * readiness probe is the STRUCTURAL proof of database contact for the PHP tier. A hand-rolled base
 * class that merely started a process and connected a socket would let "N tests passed" mean zero
 * database contact — which is precisely how the upstream DBAL suite reports green against
 * in-memory SQLite (see Task 14).
 *
 * Cost note: `LiveTestCase` spawns and reaps a ferrod PER TEST (~0.5 s). That is acceptable for this
 * package's own conformance tier; the curated UPSTREAM subset in Task 14 launches one ferrod per
 * RUN instead.
 */
abstract class DbalLiveTestCase extends LiveTestCase
{
    /** @param array<string,mixed> $extraOptions */
    protected function dbal(string $pool = 'default', array $extraOptions = []): DbalConnection
    {
        $conn = DriverManager::getConnection([
            'driverClass' => \Ferro\DBAL\Driver::class,
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => $pool] + $extraOptions,
        ]);
        // THE CONTACT ASSERTION. Without it, a driver that quietly fell back to something else
        // would still make every assertion below pass.
        self::assertInstanceOf(
            FerroClientConnection::class,
            $conn->getNativeConnection(),
            'this DBAL connection is not a Ferro one — the test would be measuring the wrong engine',
        );
        return $conn;
    }
}
