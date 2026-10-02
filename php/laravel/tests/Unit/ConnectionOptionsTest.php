<?php // /php/laravel/tests/Unit/ConnectionOptionsTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Unit;

use Ferro\Laravel\ConnectionOptions;
use PHPUnit\Framework\TestCase;

final class ConnectionOptionsTest extends TestCase
{
    public function testASocketConfigIsRead(): void
    {
        $o = ConnectionOptions::fromConfig([
            'driver' => 'ferro-pgsql',
            'ferro_socket' => '/run/ferro/app.sock',
            'pool' => 'main',
        ]);
        self::assertSame('/run/ferro/app.sock', $o->socketPath);
        self::assertSame('main', $o->pool);
    }

    public function testThePoolDefaultsToDefault(): void
    {
        $o = ConnectionOptions::fromConfig(['ferro_socket' => '/s.sock']);
        self::assertSame('default', $o->pool);
    }

    /**
     * §15's whole claim is that adoption is a `driver` change and nothing else, so a REAL config
     * array — one still carrying the upstream credentials it had before — must parse. Those keys
     * describe a database the application no longer dials (§12, D8); they are ignored, not
     * rejected, because rejecting them would make adoption a rewrite.
     */
    public function testAStockLaravelConfigStillCarryingCredentialsIsAccepted(): void
    {
        $o = ConnectionOptions::fromConfig([
            'driver' => 'ferro-pgsql',
            'host' => '127.0.0.1',
            'port' => 5432,
            'database' => 'app',
            'username' => 'app',
            'password' => 'secret',
            'charset' => 'utf8',
            'ferro_socket' => '/run/ferro/app.sock',
        ]);
        self::assertSame('/run/ferro/app.sock', $o->socketPath);
        self::assertNull($o->host, 'the upstream `host` must NOT be read as the ferrod host');
    }

    /**
     * The failure a misconfigured connection must get: `host` alone is NOT enough, because it means
     * the upstream, not ferrod. Without this the connection would silently try to dial the database
     * directly on the TCP fallback path.
     */
    public function testNeitherSocketNorFerroHostIsARefusalNamingBoth(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/ferro_socket.*ferro_host/s');
        ConnectionOptions::fromConfig(['driver' => 'ferro-pgsql', 'host' => '127.0.0.1']);
    }

    /**
     * `unix_socket` (Laravel's `DB_SOCKET`) names mysqld's OWN socket in a stock MySQL config.
     * Reading it as the ferrod socket made a leftover key dial the database server and fail with a
     * wire-magic error that named neither key (M2-C1f review). It is ignored like `host`, so a
     * config that sets only it is the same refusal as one that sets nothing.
     */
    public function testUnixSocketIsNotReadAsTheFerrodSocket(): void
    {
        $o = ConnectionOptions::fromConfig([
            'driver' => 'ferro-mysql',
            'unix_socket' => '/var/run/mysqld/mysqld.sock',
            'ferro_socket' => '/run/ferro/app.sock',
        ]);
        self::assertSame('/run/ferro/app.sock', $o->socketPath);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/ferro_socket/');
        ConnectionOptions::fromConfig(['driver' => 'ferro-mysql', 'unix_socket' => '/var/run/mysqld/mysqld.sock']);
    }

    public function testTheTcpFallbackIsReadWhenNoSocketIsGiven(): void
    {
        $o = ConnectionOptions::fromConfig(['ferro_host' => '10.0.0.5', 'ferro_port' => 9999]);
        self::assertSame('10.0.0.5', $o->host);
        self::assertSame(9999, $o->port);
        self::assertNull($o->socketPath);
    }
}
