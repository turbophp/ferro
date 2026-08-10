<?php // /php/doctrine-dbal/tests/Live/ServerVersionLiveTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Live;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\PostgreSQL120Platform;
use Ferro\DBAL\Exception\ServerVersionUnavailable;

/**
 * M1-S8b Task 6, live — the SPEC §14 nil-version decision, observed rather than argued.
 *
 * The `dead` pool points at a port nothing listens on, so `ferrod` boots fine (pools are LAZY —
 * `Pool::new` dials nothing) and its `HELLO_ACK` advertises the pool with `server_version: nil`,
 * because the version probe cannot reach a backend that is not there. That is the ONLY way to see
 * this branch: on a healthy pool the version is learned within a second or two, and a test that
 * waited for the 600 s TTL to expire would not be a test.
 */
final class ServerVersionLiveTest extends DbalLiveTestCase
{
    /** @return array<string,string> */
    protected function extraPoolDsns(): array
    {
        // Port 1 refuses immediately (ECONNREFUSED), so this fails FAST rather than sitting in the
        // OS connect timeout — see docs/followups/2026-08-10-unbounded-backend-dial.md for why a
        // black-holed address would be a very different, much slower, test.
        return ['dead' => 'postgres://ferro:ferro@127.0.0.1:1/ferro'];
    }

    public function testAHealthyPoolResolvesItsVersionAndSelectsTheRightPlatform(): void
    {
        $c = $this->dbal();

        self::assertInstanceOf(PostgreSQL120Platform::class, $c->getDatabasePlatform());
        self::assertStringContainsString(
            'PostgreSQL',
            $c->getServerVersion(),
            'the VERBATIM engine string reaches the driver; normalisation happens inside PlatformVersion',
        );
    }

    /**
     * The decision itself. Three things are asserted, and each fails for a different wrong
     * implementation:
     *   1. connecting SUCCEEDS — the handshake never depends on backend availability, which is what
     *      makes "defer" a real strategy rather than a fig leaf;
     *   2. the platform is NEVER guessed (a `return '0';` here selects `PostgreSQLPlatform`, the
     *      pre-12 fallback, for what may well be a PG 17 server — a silently downgraded dialect);
     *   3. the failure carries a CAUSE, which is the only black-box evidence that the driver really
     *      tried the `SELECT version()` fallback rather than giving up on the nil.
     *
     * The healthy pool is resolved FIRST, inside this same test, so a cache shared between
     * connections (e.g. a `static`) is caught here regardless of test order.
     */
    public function testAPoolWhoseBackendIsDownFailsLOUDLYAndNamesItself(): void
    {
        $healthy = $this->dbal();
        self::assertStringContainsString('PostgreSQL', $healthy->getServerVersion());

        $c = $this->dbal('dead');
        self::assertInstanceOf(\Ferro\Client\Connection::class, $c->getNativeConnection());

        try {
            $c->getDatabasePlatform();
            self::fail('a nil server_version must not silently produce a default platform');
        } catch (ServerVersionUnavailable $e) {
            self::assertStringContainsString('"dead"', $e->getMessage(), 'name the pool that failed');
            self::assertStringContainsString('serverVersion', $e->getMessage(), 'name the escape hatch');
            self::assertStringContainsString('transient', $e->getMessage(), 'nil is a normal state');
            self::assertNotNull(
                $e->getPrevious(),
                'the deferral must actually ATTEMPT a SELECT version(); a bare nil-check has no cause',
            );
        }
    }

    /**
     * The operator escape hatch, on the SAME dead pool: Doctrine builds a `StaticServerVersionProvider`
     * from `serverVersion` and never asks our connection at all (hazard 15), so the platform resolves
     * even though the backend is unreachable. The previous test is what makes this one meaningful —
     * it proves that asking THIS pool's connection throws, so a platform coming back here can only
     * have come from the parameter.
     */
    public function testTheServerVersionParamShortCircuitsTheWholeProblem(): void
    {
        $c = DriverManager::getConnection([
            'driverClass' => \Ferro\DBAL\Driver::class,
            'unix_socket' => $this->socketPath,
            'driverOptions' => ['pool' => 'dead'],
            'serverVersion' => 'PostgreSQL 17.10',
        ]);

        self::assertInstanceOf(PostgreSQL120Platform::class, $c->getDatabasePlatform());
    }
}
