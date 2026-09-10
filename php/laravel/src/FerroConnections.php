<?php // /php/laravel/src/FerroConnections.php
declare(strict_types=1);
namespace Ferro\Laravel;

use Ferro\Client\Connection as FerroClient;
use Ferro\Client\RetryPolicy;
use Ferro\Client\Value\RawStringValuePolicy;
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
     *
     * **`$aliases` is the full drop-in escape hatch, and it is opt-in for a reason.** Illuminate
     * resolves a connection BY DRIVER NAME (`ConnectionFactory::createConnection()` consults
     * `Connection::getResolver($driver)` before its own `match`, verified in v11.51.0), so the
     * default `ferro-pgsql` name means adoption is a one-word config change — but it also means the
     * application now reports a driver the wider ecosystem does not recognise. That is not
     * cosmetic: MEASURED against laravel/framework v11.51.0's own `QueryBuilderTest` (M2-C2), six
     * tests gate their assertions on `in_array($this->driver, ['pgsql', 'sqlsrv'])` and simply do
     * not run their check under any other name. Third-party packages branch on `$connection
     * ->getDriverName()` the same way.
     *
     * Passing `['pgsql' => 'ferro-pgsql']` registers Ferro UNDER the stock name, so such code takes
     * its PostgreSQL branch. **The sharp edge, stated plainly: that hijacks EVERY connection in the
     * application whose `driver` is `pgsql`, including any that was meant to dial PostgreSQL
     * directly.** An application with a mixed setup should rename those connections' driver instead,
     * or not use the alias. Nothing is dialled either way — the factory hands the resolver a LAZY
     * PDO closure which these connections discard unresolved.
     *
     * @param array<string,string> $aliases extra `driver` names => the Ferro driver each resolves
     *   to (a key of {@see DRIVERS}).
     */
    public static function register(array $aliases = []): void
    {
        foreach (self::DRIVERS as $driver => $_class) {
            self::resolve($driver, $driver);
        }
        foreach ($aliases as $name => $driver) {
            if (!isset(self::DRIVERS[$driver])) {
                throw new \InvalidArgumentException(sprintf(
                    'Ferro: alias "%s" points at unknown driver "%s"; this package registers %s.',
                    $name,
                    $driver,
                    implode(', ', self::drivers()),
                ));
            }
            self::resolve($name, $driver);
        }
    }

    /** Bind one Illuminate `driver` name to one Ferro driver. */
    private static function resolve(string $name, string $driver): void
    {
        Connection::resolverFor($name, static function (
            $_pdo,
            string $database,
            string $prefix,
            array $config,
        ) use ($driver): Connection {
            return self::make($driver, $database, $prefix, $config);
        });
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
     *
     * **{@see RawStringValuePolicy} is load-bearing, not a preference.** Illuminate is written
     * against PDO, and PDO hands up scalars — a driver-native string for every non-trivial column
     * type. The client's DEFAULT {@see \Ferro\Client\Value\M1ValuePolicy} hands up the SPEC §9
     * value objects instead ({@see \Ferro\NaiveTimestamp}, {@see \Ferro\Decimal},
     * {@see \Ferro\Date}, …), and stock Illuminate code puts those straight into positions where
     * only a scalar works. Two shapes MEASURED against laravel/framework v11.51.0's own
     * `QueryBuilderTest` (M2-C2), both from ordinary application-level calls:
     *
     *  - `Builder::pluck($column, $key)` uses the key column's value as an ARRAY KEY —
     *    `TypeError: Cannot access offset of type Ferro\NaiveTimestamp on array`.
     *  - the same test then calls `substr($v, 0, 19)` on those keys, i.e. it requires the value to
     *    BE the driver-native string, not merely to stringify.
     *
     * So the tier hands Illuminate what PDO would. Charter rule 6 is intact: this is the driver's
     * own decode step (exactly as `Ferro\DBAL\Value\DbalValuePolicy` is for the Doctrine tier),
     * not a change to Grammar, Processor or schema.
     *
     * **One deliberate divergence from PDO_pgsql, kept because it is SAFER.** `TIMESTAMPTZ` arrives
     * as the canonical RFC3339 `Y-m-d\TH:i:s[.u]Z` rather than PDO's `Y-m-d H:i:s[.u]+00`. Do not
     * "fix" this by re-rendering to PDO's shape: `Model::asDateTime()` runs
     * `Date::createFromFormat($grammar->getDateFormat(), $value)` with `Y-m-d H:i:s`, and PHP's
     * format parser IGNORES trailing data — so PDO's offset form parses and the `+00` is silently
     * discarded, reinterpreting the instant in the application timezone. The canonical form fails
     * that `createFromFormat` on its `T` separator and falls through to Illuminate's own
     * `Date::parse($value)`, which reads the `Z` and preserves the instant. SPEC §22.2.
     */
    private static function client(ConnectionOptions $o): FerroClient
    {
        $values = new RawStringValuePolicy();
        return $o->socketPath !== null
            ? Ferro::connect($o->socketPath, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, $values)
            : Ferro::connectTcp((string) $o->host, $o->port, $o->pool, $o->connectTimeout, $o->ioTimeout, RetryPolicy::none(), null, $values);
    }
}
