<?php // /php/laravel/src/FerroConnections.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Ferro\Client\Connection as FerroClient;
use Ferro\Client\RetryPolicy;
use Ferro\Ferro;
use Illuminate\Database\Connection;

/**
 * Registers Ferro's Illuminate connection resolvers (§15).
 *
 * `Illuminate\Database\Connection::resolverFor($driver, Closure)` is a static map the framework
 * consults when a config entry names a `driver` it does not know — which is the whole mechanism
 * behind "change `driver` and nothing else". A Laravel application calls {@see register} from a
 * service provider's `register()`; a plain Illuminate application (or a test) calls it directly.
 * It is deliberately a static registrar rather than a `ServiceProvider` subclass so that this
 * package depends only on `illuminate/database`, not on the full framework.
 */
final class FerroConnections
{
    /** The `driver` values this package answers to, mapped to their connection classes. */
    private const DRIVERS = [
        'ferro-pgsql' => FerroPostgresConnection::class,
    ];

    /**
     * Idempotent: registering twice simply overwrites the same resolver, which matters because a
     * service provider's `register()` can run more than once in a test process.
     */
    public static function register(): void
    {
        foreach (self::DRIVERS as $driver => $_class) {
            Connection::resolverFor($driver, static function (
                $_pdo,
                string $database,
                string $prefix,
                array $config,
            ) use ($driver): Connection {
                return self::make($driver, $database, $prefix, $config);
            });
        }
    }

    /**
     * The driver names this package registers.
     *
     * @return list<string>
     */
    public static function drivers(): array
    {
        return array_keys(self::DRIVERS);
    }

    /**
     * Build one connection from its config array. Split out of the resolver closure so a test can
     * construct a connection without going through the framework's resolver map.
     *
     * @param array<string,mixed> $config
     */
    public static function make(string $driver, string $database, string $prefix, array $config): Connection
    {
        $class = self::DRIVERS[$driver] ?? throw new \InvalidArgumentException(
            sprintf('Ferro: unknown driver "%s"; this package registers %s.', $driver, implode(', ', self::drivers())),
        );
        $o = ConnectionOptions::fromConfig($config);
        return new $class(self::client($o), $database, $prefix, $config);
    }

    /**
     * `RetryPolicy::none()` matches the DBAL tier's choice and for the same reason: the layer above
     * owns the retry decision (`DB::transaction($fn, attempts: N)`), and the client's own
     * autocommit read-retry must not double up with it.
     */
    private static function client(ConnectionOptions $o): FerroClient
    {
        return $o->socketPath !== null
            ? Ferro::connect($o->socketPath, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none())
            : Ferro::connectTcp((string) $o->host, $o->port, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none());
    }
}
