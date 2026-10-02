<?php // /php/laravel/tests/Live/MySqlLiveTestCase.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Client\Connection as FerroClient;
use Ferro\Laravel\FerroConnections;
use Ferro\Laravel\FerroMariaDbConnection;
use Ferro\Laravel\FerroMySqlConnection;
use Illuminate\Database\Connection as IlluminateConnection;

/**
 * Base for the Eloquent tier's MySQL-family live tests (M2-C1f).
 *
 * The base harness already launches a `mysql` pool on `FERRO_TEST_MYSQL_URL` — the variable CI's
 * `php` lane provisions — and {@see requireMysqlPool} SKIPS without it, which that lane's
 * `--fail-on-skipped` turns into a failure. This class adds ONE more pool, `mysql_schema`, on the
 * same server's `laravel_tests` database (testkit/mysql-init.sql), because the schema-builder tests
 * call `dropAllTables()`, which drops EVERY table in its database and must never be pointed at the
 * shared `ferro` one.
 */
abstract class MySqlLiveTestCase extends LaravelLiveTestCase
{
    protected const SCHEMA_POOL = 'mysql_schema';
    protected const SCHEMA_DATABASE = 'laravel_tests';

    /** @return array<string,string> */
    protected function extraPoolDsns(): array
    {
        $url = getenv('FERRO_TEST_MYSQL_URL');
        if (!is_string($url) || $url === '') {
            return [];
        }
        $schemaUrl = preg_replace('#/[^/?]*(\?|$)#', '/' . self::SCHEMA_DATABASE . '$1', $url, 1);
        return [self::SCHEMA_POOL => (string) $schemaUrl];
    }

    /** The database the base `mysql` pool's DSN names — what a `database` label must equal. */
    protected static function mysqlDatabase(): string
    {
        $path = parse_url((string) getenv('FERRO_TEST_MYSQL_URL'), PHP_URL_PATH);
        return is_string($path) && $path !== '/' ? ltrim($path, '/') : 'ferro';
    }

    /**
     * A MySQL-family connection built through Illuminate's OWN resolver map, with the sibling bases'
     * contact discipline: an unguessable nonce must round-trip, and the family's name must appear in
     * `version()` or `@@version_comment` (a row for `@@version_comment` is itself a MySQL-family
     * signal: PostgreSQL and SQLite refuse the name).
     *
     * @param 'ferro-mysql'|'ferro-mariadb' $driver
     */
    protected function mysqlConnection(
        ?string $pool = null,
        ?string $database = null,
        string $driver = 'ferro-mysql',
    ): FerroMySqlConnection|FerroMariaDbConnection {
        $pool ??= $this->requireMysqlPool();
        FerroConnections::register();

        $resolver = IlluminateConnection::getResolver($driver);
        self::assertNotNull($resolver, "the {$driver} resolver is not registered");

        $conn = $resolver(null, $database ?? self::mysqlDatabase(), '', [
            'driver' => $driver,
            'ferro_socket' => $this->socketPath,
            'pool' => $pool,
        ]);

        $want = $driver === 'ferro-mariadb' ? FerroMariaDbConnection::class : FerroMySqlConnection::class;
        self::assertInstanceOf($want, $conn, "the resolver did not build a {$want}");
        self::assertInstanceOf(FerroClient::class, $conn->getFerroConnection());

        $nonce = bin2hex(random_bytes(8));
        $probe = $conn->select("select '{$nonce}' as nonce, version() as v, @@version_comment as c");
        self::assertCount(1, $probe, 'the contact probe returned no row — nothing was executed');
        self::assertSame($nonce, $probe[0]->nonce, 'the contact probe did not round-trip');
        self::assertMatchesRegularExpression('/mysql|mariadb/i', $probe[0]->v . ' ' . $probe[0]->c);

        return $conn;
    }

    /** A connection on the dedicated `laravel_tests` pool, labelled with that database. */
    protected function schemaConnection(string $driver = 'ferro-mysql'): FerroMySqlConnection|FerroMariaDbConnection
    {
        $this->requireMysqlPool();
        return $this->mysqlConnection(self::SCHEMA_POOL, self::SCHEMA_DATABASE, $driver);
    }
}
