<?php // /php/laravel/tests/Unit/DriverRegistrationTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Unit;

use Ferro\Laravel\FerroConnections;
use Illuminate\Database\Connection as IlluminateConnection;
use PHPUnit\Framework\TestCase;

/**
 * The resolver MAP only — no engine contact, so these run offline.
 *
 * Illuminate resolves a connection BY DRIVER NAME, and the name is observable to application and
 * third-party code (`$connection->getDriverName()`), so which names this package claims is part of
 * its contract rather than an implementation detail. M2-C2 MEASURED the consequence: six of
 * laravel/framework v11.51.0's own `QueryBuilderTest` cases gate their assertions on
 * `in_array($this->driver, ['pgsql', 'sqlsrv'])`.
 */
final class DriverRegistrationTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->forget();
    }

    protected function tearDown(): void
    {
        $this->forget();
        parent::tearDown();
    }

    /**
     * Illuminate's resolver map is static PROCESS state and it has no public un-register (
     * `resolverFor()` types its callback as `Closure`, so it cannot take a null), which would
     * order-couple this suite to anything else that registers. Reflection on the one static
     * property is the only way to put the process back how it was found.
     */
    private function forget(): void
    {
        $p = new \ReflectionProperty(IlluminateConnection::class, 'resolvers');
        /** @var array<string,\Closure> $map */
        $map = $p->getValue();
        foreach ([...FerroConnections::drivers(), 'pgsql'] as $name) {
            unset($map[$name]);
        }
        $p->setValue(null, $map);
    }

    public function testTheFerroDriverIsRegisteredByDefault(): void
    {
        FerroConnections::register();
        self::assertNotNull(IlluminateConnection::getResolver('ferro-pgsql'));
    }

    public function testTheStockNameIsNotClaimedWithoutAnAlias(): void
    {
        FerroConnections::register();
        self::assertNull(
            IlluminateConnection::getResolver('pgsql'),
            'registering Ferro must NOT hijack the stock pgsql name unless the application asks',
        );
    }

    public function testAnAliasClaimsTheStockName(): void
    {
        FerroConnections::register(['pgsql' => 'ferro-pgsql']);
        self::assertNotNull(IlluminateConnection::getResolver('pgsql'));
        // The Ferro name keeps working alongside it: an app mid-migration has both.
        self::assertNotNull(IlluminateConnection::getResolver('ferro-pgsql'));
    }

    public function testAnAliasPointingAtAnUnknownDriverIsRefused(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/unknown driver "ferro-oracle"/');
        FerroConnections::register(['pgsql' => 'ferro-oracle']);
    }

    public function testRegisteringTwiceIsIdempotent(): void
    {
        FerroConnections::register(['pgsql' => 'ferro-pgsql']);
        FerroConnections::register(['pgsql' => 'ferro-pgsql']);
        self::assertNotNull(IlluminateConnection::getResolver('pgsql'));
    }
}
