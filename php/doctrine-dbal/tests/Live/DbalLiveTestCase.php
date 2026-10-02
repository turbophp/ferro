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
    /**
     * Collect reference cycles BEFORE the base class stops `ferrod`.
     *
     * DBAL 3's wrapper `Connection` is in a reference cycle with itself (its constructor builds an
     * `ExpressionBuilder` holding the connection), so a test's connection is NOT freed when the test
     * method returns — measured: the Ferro client is still alive after `unset()` under 3.10.6, and
     * gone after `gc_collect_cycles()`; under DBAL 4 it is gone at `unset()`. Left alone, its session
     * stays open into the base class's shutdown, which waits out `ferrod`'s graceful drain: about
     * 5 s per test, which is what made the DBAL 3 lane ~10x slower than the DBAL 4 one. Upstream
     * behaviour (PDO connections under DBAL 3 linger identically), so it is handled here rather
     * than in the driver.
     */
    protected function tearDown(): void
    {
        gc_collect_cycles();
        parent::tearDown();
    }

    /**
     * Whether the INSTALLED doctrine/dbal is a 3.x (M2-C5). The same live suite runs in both lanes
     * — `vendor/` (DBAL 4) and `vendor-dbal3/` (DBAL 3) — so every test exercises the driver class
     * of whichever major is installed, and a test asserting a major-specific behaviour asks this.
     * `VersionAwarePlatformDriver` is the interface whose absence forces the second driver class,
     * so it is also the honest thing to probe for.
     */
    public static function isDbal3(): bool
    {
        return interface_exists(\Doctrine\DBAL\VersionAwarePlatformDriver::class);
    }

    /** @return class-string<\Doctrine\DBAL\Driver> */
    public static function driverClass(): string
    {
        return self::isDbal3() ? \Ferro\DBAL\Dbal3\Driver::class : \Ferro\DBAL\Driver::class;
    }

    /** @return class-string<DbalConnection> the isolation `wrapperClass` for the installed major */
    public static function wrapperClass(): string
    {
        return self::isDbal3() ? \Ferro\DBAL\Dbal3\FerroConnection::class : \Ferro\DBAL\Wrapper\FerroConnection::class;
    }

    /**
     * The DRIVER connection under a DBAL wrapper — `getNativeConnection()` hands back the Ferro
     * CLIENT, one level too far. Reached the way DBAL's own tests do, and per major because the
     * two disagree: DBAL 4's protected `connect()` RETURNS the driver connection, DBAL 3's public
     * `connect()` returns a bool and leaves it in the protected `$_conn`.
     */
    public static function driverConnectionOf(DbalConnection $c): \Ferro\DBAL\AbstractConnection
    {
        $driver = self::outermostDriverConnection($c);
        self::assertInstanceOf(\Ferro\DBAL\AbstractConnection::class, $driver);
        return $driver;
    }

    /** The wrapper's own driver-connection handle — a middleware's, when one is configured. */
    public static function outermostDriverConnection(DbalConnection $c): object
    {
        if (self::isDbal3()) {
            $c->getNativeConnection(); // connects from inside doctrine/dbal — no deprecation notice
            $driver = (new \ReflectionProperty($c, '_conn'))->getValue($c);
        } else {
            $driver = (new \ReflectionMethod($c, 'connect'))->invoke($c);
        }
        self::assertIsObject($driver);
        return $driver;
    }

    /**
     * The server version through the wrapper, per major: DBAL 4's wrapper exposes
     * `getServerVersion()` publicly, DBAL 3's keeps it private — there it is the driver
     * connection's (`ServerInfoAwareConnection`), which is exactly what DBAL 3 itself asks.
     */
    public static function serverVersionOf(DbalConnection $c): string
    {
        return self::isDbal3() ? self::driverConnectionOf($c)->getServerVersion() : $c->getServerVersion();
    }

    /** @param array<string,mixed> $extraOptions */
    protected function dbal(string $pool = 'default', array $extraOptions = []): DbalConnection
    {
        $conn = DriverManager::getConnection([
            'driverClass' => self::driverClass(),
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
