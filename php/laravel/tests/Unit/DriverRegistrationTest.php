<?php // /php/laravel/tests/Unit/DriverRegistrationTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Unit;

use Ferro\Laravel\FerroConnections;
use Ferro\Laravel\FerroServiceProvider;
use Illuminate\Container\Container;
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
        foreach ([...FerroConnections::drivers(), 'pgsql', 'sqlite', 'mysql', 'mariadb'] as $name) {
            unset($map[$name]);
        }
        $p->setValue(null, $map);
    }

    /**
     * §15's "change `driver` and nothing else" depends on this provider (§22.2 (ch)): without it a
     * config entry naming `ferro-pgsql` fails `Unsupported driver [ferro-pgsql]` unless the
     * application writes PHP. It registers every Ferro driver and NO stock-name alias — the alias
     * replaces every connection using that name, so it must stay an explicit call.
     */
    public function testTheServiceProviderRegistersEveryFerroDriverAndNoAlias(): void
    {
        foreach (FerroConnections::drivers() as $driver) {
            self::assertNull(IlluminateConnection::getResolver($driver), "precondition: $driver unregistered");
        }

        (new FerroServiceProvider(new Container()))->register();

        foreach (FerroConnections::drivers() as $driver) {
            self::assertNotNull(IlluminateConnection::getResolver($driver), $driver);
        }
        foreach (['pgsql', 'sqlite', 'mysql', 'mariadb'] as $stock) {
            self::assertNull(IlluminateConnection::getResolver($stock), "the provider must not alias $stock");
        }
    }

    /**
     * The provider is DISCOVERED, not installed by hand: Laravel's `PackageManifest` reads
     * `extra.laravel.providers` from the package's composer.json. The demo app proves the
     * framework's own discovery reads it (`DemoAppTest::testThePackageIsDiscoveredFromItsComposerJson`);
     * this keeps the declaration and the class in step offline.
     */
    public function testComposerJsonDeclaresTheProviderForPackageDiscovery(): void
    {
        $json = json_decode((string) file_get_contents(__DIR__ . '/../../composer.json'), true, 512, JSON_THROW_ON_ERROR);
        self::assertIsArray($json);
        self::assertSame([FerroServiceProvider::class], $json['extra']['laravel']['providers'] ?? null);
    }

    public function testTheFerroDriverIsRegisteredByDefault(): void
    {
        FerroConnections::register();
        self::assertNotNull(IlluminateConnection::getResolver('ferro-pgsql'));
    }

    /**
     * One Ferro driver per Laravel family, and MariaDB IS one: Laravel 11 resolves it through its
     * own `mariadb` driver (`MariaDbConnection`, `MariaDbGrammar`, `MariaDbBuilder`), so a
     * MariaDB application's `driver` is `mariadb` and its one-word change is `ferro-mariadb`.
     */
    public function testEveryFamilyHasAFerroDriverIncludingMariaDb(): void
    {
        self::assertSame(
            ['ferro-mariadb', 'ferro-mysql', 'ferro-pgsql', 'ferro-sqlite'],
            (function (): array { $d = FerroConnections::drivers(); sort($d); return $d; })(),
        );
        FerroConnections::register();
        foreach (FerroConnections::drivers() as $name) {
            self::assertNotNull(IlluminateConnection::getResolver($name), $name);
        }
    }

    public function testNoStockNameIsClaimedWithoutAnAlias(): void
    {
        FerroConnections::register();
        foreach (['pgsql', 'sqlite', 'mysql', 'mariadb'] as $stock) {
            self::assertNull(IlluminateConnection::getResolver($stock), $stock);
        }
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
