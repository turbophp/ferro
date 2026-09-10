<?php // /php/laravel/tests/Live/LaravelLiveTestCase.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Client\Connection as FerroClient;
use Ferro\Laravel\FerroConnections;
use Ferro\Laravel\FerroPostgresConnection;
use Ferro\Tests\Live\LiveTestCase;
use Illuminate\Database\Connection as IlluminateConnection;

/**
 * Base for the Eloquent tier's live tests. Inherits `Ferro\Tests\Live\LiveTestCase` wholesale —
 * reached through this package's `autoload-dev` mapping of `Ferro\Tests\` to `../client/tests/`,
 * which works because the path repository installs `vendor/ferro/client` as a SYMLINK — exactly as
 * the DBAL tier's `DbalLiveTestCase` does.
 *
 * **The contact assertion is the point of this class, and it is a hard gate, not a nicety.** The
 * DBAL acceptance suite taught this the expensive way: upstream's `TestUtil` honoured only
 * `driver`, silently fell back to in-memory SQLite, and reported `OK (105 tests, 211 assertions)`
 * with ZERO engine contact and nothing skipped. A connection object alone proves nothing. So
 * {@see connection} refuses to hand back a connection until it has established BOTH that the thing
 * is a Ferro connection AND that a real `SELECT 1` round-tripped through it.
 */
abstract class LaravelLiveTestCase extends LiveTestCase
{
    /** @param array<string,mixed> $extra */
    protected function connection(string $pool = 'default', array $extra = []): FerroPostgresConnection
    {
        FerroConnections::register();

        // Built through the framework's own resolver map rather than by `new` — otherwise the test
        // would prove the connection class works while saying nothing about whether `driver` =>
        // 'ferro-pgsql' actually reaches it, which IS the config-only-adoption claim (§15).
        $resolver = IlluminateConnection::getResolver('ferro-pgsql');
        self::assertNotNull($resolver, 'the ferro-pgsql resolver is not registered');

        $conn = $resolver(null, 'ferro', '', [
            'driver' => 'ferro-pgsql',
            'ferro_socket' => $this->socketPath,
            'pool' => $pool,
        ] + $extra);

        self::assertInstanceOf(
            FerroPostgresConnection::class,
            $conn,
            'the resolver did not build a Ferro connection — the test would measure the wrong engine',
        );
        self::assertInstanceOf(
            FerroClient::class,
            $conn->getFerroConnection(),
            'this connection is not backed by a Ferro client',
        );

        // THE ROUND TRIP. Everything above is structural; this is the only part that proves bytes
        // reached a real engine with a real PostgreSQL behind it.
        //
        // A per-call NONCE, not `select 1`. Mutation-proven necessary: with `select()` stubbed to
        // return a fixed row shaped like the probe's answer, a `select 1 as ferro_contact` gate
        // passed — a stub only has to guess a constant. A random nonce cannot be guessed, so the
        // value coming back is evidence that something RECEIVED it. `version()` rides along to
        // prove which engine answered, since a nonce alone would be satisfied by any SQL database.
        $nonce = bin2hex(random_bytes(8));
        $probe = $conn->select("select '{$nonce}' as nonce, version() as v");
        self::assertCount(1, $probe, 'the contact probe returned no row — nothing was executed');
        self::assertSame($nonce, $probe[0]->nonce,
            'the contact probe did not round-trip: whatever answered did not receive the nonce');
        self::assertStringContainsString('PostgreSQL', (string) $probe[0]->v,
            'something answered, but it was not PostgreSQL — the DBAL suite\'s SQLite-fallback trap');

        return $conn;
    }
}
