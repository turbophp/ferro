<?php // /php/laravel/tests/Unit/PdoShimAttributeTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Unit;

use Ferro\Client\Connection as FerroClient;
use Ferro\Laravel\FerroPdoShim;
use Ferro\Protocol\PoolInfo;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\TestCase;

/**
 * The refusal half of {@see FerroPdoShim::getAttribute}. The SUCCESS half needs a live engine and
 * is covered by `SchemaBuilderLiveTest`, which only passes because the attribute resolves.
 */
final class PdoShimAttributeTest extends TestCase
{
    /**
     * An unimplemented attribute must name ITSELF and the one that IS implemented. A bare "not
     * supported" would send the reader to the source to find out which constant they asked for —
     * `getAttribute()` takes an int, so the number alone is unreadable at the call site.
     */
    public function testAnUnsupportedAttributeRefusesByNumberAndNamesTheSupportedOne(): void
    {
        // `Ferro\Client\Connection` is FINAL, so it is built for real over a FakeSession rather
        // than doubled — the same pattern the sibling package's unit tests use.
        $shim = new FerroPdoShim(new FerroClient(new FakeSession(), 'main'));

        try {
            $shim->getAttribute(\PDO::ATTR_DRIVER_NAME);
            $this->fail('an unimplemented attribute must refuse');
        } catch (\LogicException $e) {
            self::assertStringContainsString((string) \PDO::ATTR_DRIVER_NAME, $e->getMessage());
            self::assertStringContainsString((string) \PDO::ATTR_SERVER_VERSION, $e->getMessage());
        }
    }

    /**
     * A NULL server version is LOUD. `version_compare(null, '12.0', '<')` is true, so a silent
     * default would make stock `PostgresGrammar::compileColumns()` emit pre-12 introspection SQL
     * against a modern PostgreSQL — wrong, and quietly so. Same call the Doctrine tier makes
     * (§22.2, D-S8b-1): a wrong version is a silently wrong dialect.
     */
    public function testANullServerVersionThrowsRatherThanDefaulting(): void
    {
        $session = new FakeSession();
        $session->poolInfo = [new PoolInfo('main', 'postgres', null)];
        $shim = new FerroPdoShim(new FerroClient($session, 'main'));

        $this->expectException(\LogicException::class);
        $this->expectExceptionMessageMatches('/no server_version for pool "main"/');
        $shim->getAttribute(\PDO::ATTR_SERVER_VERSION);
    }
}
