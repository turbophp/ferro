<?php // /php/client/tests/Live/ErrnoLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Error\NonRetryableException;

/**
 * THE end-to-end proof that the vendor errno reaches the PHP client (M1-S8a).
 *
 * Doctrine DBAL's stock MySQL `ExceptionConverter` keys **exclusively** on the vendor errno via
 * `getCode()`; SQLSTATE cannot substitute, because a duplicate key and a NOT NULL violation BOTH
 * arrive as `23000`. PostgreSQL's converter keys on SQLSTATE only — PG has no integer errno, so it
 * stays `null` there by construction.
 */
final class ErrnoLiveTest extends LiveTestCase
{
    public function testMysqlDuplicateKeyCarriesErrno1062AlongsideSqlstate23000(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->connectConnection(null, $pool);
        $c->exec('DROP TABLE IF EXISTS s8a_errno');
        $c->exec('CREATE TABLE s8a_errno (id INT PRIMARY KEY)');
        $c->exec('INSERT INTO s8a_errno (id) VALUES (1)');

        try {
            $c->exec('INSERT INTO s8a_errno (id) VALUES (1)');
            $this->fail('the duplicate insert must be rejected');
        } catch (NonRetryableException $e) {
            $this->assertSame('23000', $e->sqlstate());
            $this->assertSame(1062, $e->errno(), 'the vendor errno must reach the client');
            $this->assertStringContainsString('errno=1062', $e->getMessage(),
                'the exception message surfaces the errno too');
        }
    }

    public function testMysqlNotNullAndDuplicateShareASqlstateButNotAnErrno(): void
    {
        $pool = $this->requireMysqlPool();
        $c = $this->connectConnection(null, $pool);
        $c->exec('DROP TABLE IF EXISTS s8a_errno_nn');
        $c->exec('CREATE TABLE s8a_errno_nn (id INT PRIMARY KEY, v INT NOT NULL)');

        $errno = null;
        $sqlstate = null;
        try {
            $c->exec('INSERT INTO s8a_errno_nn (id, v) VALUES (1, NULL)');
            $this->fail('the NOT NULL violation must be rejected');
        } catch (NonRetryableException $e) {
            $errno = $e->errno();
            $sqlstate = $e->sqlstate();
        }
        // THE reason the errno has to be on the wire at all.
        $this->assertSame('23000', $sqlstate, 'MySQL reuses 23000 for a NOT NULL violation');
        $this->assertSame(1048, $errno, 'only the errno distinguishes it from a duplicate key');
    }

    public function testPostgresCarriesNoErrno(): void
    {
        $c = $this->connectConnection();
        try {
            $c->exec('SELEKT 1');
            $this->fail('syntax error expected');
        } catch (NonRetryableException $e) {
            $this->assertSame('42601', $e->sqlstate());
            $this->assertNull($e->errno(), 'PG has no integer errno — null by construction');
            $this->assertStringNotContainsString('errno=', $e->getMessage(),
                'a null errno must not be rendered into the message');
        }
    }
}
