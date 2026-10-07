<?php // /php/psr18/src/Http/Adapter/ConnectionProvider.php
declare(strict_types=1);
namespace Ferro\Http\Adapter;

use Ferro\Client\Connection;

/**
 * The ONE Ferro connection an adapter's requests share (SPEC §23.11.2: every upstream "over the
 * connection's one multiplexed session").
 *
 * An adapter may be given a closure that dials on first use (`fn () => Ferro::connect(…)`, as the
 * Laravel wiring does). It is called once — on the first request, whichever upstream it addresses —
 * and its Connection is kept; a closure that THROWS is called again on the next request, since
 * nothing was dialled. Without this, a closure was called once per distinct upstream/origin, so a
 * worker calling three upstreams held three ferrod sessions (M6-F9 review F-B).
 *
 * The returned closure is shared by every clone of the adapter that holds it (PSR-18's
 * `withIdempotent()` copies), so those share the connection too.
 *
 * @internal shared by `ferro/guzzle`, `ferro/psr18` and `ferro/laravel`
 */
final class ConnectionProvider
{
    private function __construct() {}

    /**
     * @param Connection|\Closure(): Connection $connection
     * @return \Closure(): Connection
     */
    public static function memoise(Connection|\Closure $connection): \Closure
    {
        if ($connection instanceof Connection) {
            return static fn (): Connection => $connection;
        }
        $held = null;
        return static function () use (&$held, $connection): Connection {
            return $held ??= $connection();
        };
    }
}
