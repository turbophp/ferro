<?php // /php/laravel/src/Exception/ConnectFailed.php
declare(strict_types=1);
namespace Ferro\Laravel\Exception;

use Ferro\Client\Error\TransportException;

/**
 * The Ferro client could not be DIALLED — the one failure on this tier that provably sent nothing.
 *
 * The client is dialled lazily, when Illuminate first resolves the connection's PDO (M2-C1e-2), so
 * a dial failure surfaces inside `run()` — exactly where `pdo_pgsql`'s lazy PDO surfaces its own
 * connect failure — and Illuminate's lost-connection retry may reconnect and re-run the statement,
 * as it does with PDO. That is safe only because no statement reached the engine, and this TYPE is
 * how {@see \Ferro\Laravel\FerroConnectionBody::causedByLostConnection} knows it, rather than by
 * matching the transport's message text.
 *
 * Deliberately NOT a `FerroException`: the execution paths wrap every `FerroException` into a
 * {@see FerroQueryException}, and the lost-connection guard refuses any Ferro failure other than a
 * known-safe one. Extends `PDOException` with code 0, the convention a payload-less failure takes
 * on this tier ({@see FerroQueryException::fromFerro}).
 */
final class ConnectFailed extends \PDOException
{
    public static function from(TransportException $e): self
    {
        return new self('Ferro: could not connect to ferrod: ' . $e->getMessage(), 0, $e);
    }
}
