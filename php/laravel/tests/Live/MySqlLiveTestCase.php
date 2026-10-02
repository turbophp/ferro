<?php // /php/laravel/tests/Live/MySqlLiveTestCase.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Client\Connection as FerroClient;
use Ferro\Laravel\FerroConnections;
use Ferro\Laravel\FerroMySqlConnection;
use Illuminate\Database\Connection as IlluminateConnection;

/**
 * Base for the Eloquent tier's MySQL / MariaDB live tests (M2-C1f).
 *
 * A SECOND pool through {@see extraPoolDsns}, as the SQLite base does, so the shared `default`
 * PostgreSQL pool every other live test uses is untouched. It dials `FERRO_TEST_MYSQL_URL` — the
 * variable CI's `php` lane already provisions for the Doctrine tier — and SKIPS without it, which
 * that lane's `--fail-on-skipped` turns into a failure, so a lane that silently stopped reaching
 * MySQL would go red rather than green.
 */
abstract class MySqlLiveTestCase extends LaravelLiveTestCase
{
    protected const MYSQL_POOL = 'mysql';

    /** @return array<string,string> */
    protected function extraPoolDsns(): array
    {
        $url = getenv('FERRO_TEST_MYSQL_URL');
        if ($url === false || $url === '') {
            self::markTestSkipped('FERRO_TEST_MYSQL_URL is unset');
        }
        return [self::MYSQL_POOL => $url];
    }

    /**
     * A MySQL-family connection built through Illuminate's OWN resolver map, with the sibling bases'
     * contact discipline: an unguessable nonce must round-trip, and `@@version_comment` must name
     * the family (a bare `version()` here is `8.4.11` — it names nothing).
     */
    protected function mysqlConnection(): FerroMySqlConnection
    {
        FerroConnections::register();

        $resolver = IlluminateConnection::getResolver('ferro-mysql');
        self::assertNotNull($resolver, 'the ferro-mysql resolver is not registered');

        $conn = $resolver(null, 'ferro_mysql_label', '', [
            'driver' => 'ferro-mysql',
            'ferro_socket' => $this->socketPath,
            'pool' => self::MYSQL_POOL,
        ]);

        self::assertInstanceOf(FerroMySqlConnection::class, $conn, 'the resolver did not build a Ferro MySQL connection');
        self::assertInstanceOf(FerroClient::class, $conn->getFerroConnection());

        $nonce = bin2hex(random_bytes(8));
        // `@@version_comment` is a syntax error on PostgreSQL and SQLite, so a row at all is the
        // family signal; the name is in the comment on MySQL (`MySQL Community Server - GPL`) and
        // only in `version()` on a distro MariaDB (comment `Ubuntu 24.04`, measured) — so both.
        $probe = $conn->select("select '{$nonce}' as nonce, version() as v, @@version_comment as c");
        self::assertCount(1, $probe, 'the contact probe returned no row — nothing was executed');
        self::assertSame($nonce, $probe[0]->nonce, 'the contact probe did not round-trip');
        self::assertMatchesRegularExpression('/mysql|mariadb/i', $probe[0]->v . ' ' . $probe[0]->c);

        return $conn;
    }
}
