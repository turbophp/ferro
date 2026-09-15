<?php // /php/laravel/tests/Live/SqliteLiveTestCase.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Client\Connection as FerroClient;
use Ferro\Laravel\FerroConnections;
use Ferro\Laravel\FerroSQLiteConnection;
use Illuminate\Database\Connection as IlluminateConnection;

/**
 * Base for the Eloquent tier's SQLite live tests (C3-6b).
 *
 * It adds a SECOND pool through {@see extraPoolDsns} — the seam the harness already has — so the
 * shared `default` PostgreSQL pool every other live test uses is untouched. The database is a file
 * under the harness's own temp directory, deleted before the pool opens it, because §7.6/§22.2 (bd)
 * make an in-memory DSN a hard refusal: SQLite gives every `:memory:` connection its own private
 * database, so a pool would hand different tenants different databases.
 *
 * **These tests still skip when `FERRO_TEST_PG_URL` is unset**, inherited from
 * `Ferro\Tests\Live\LiveTestCase`, even though SQLite itself needs no server. That is a harness
 * limitation rather than a property of the subject: CI provisions the variable and runs this tier
 * with `--fail-on-skipped`, so they are a real gate there. Recorded rather than worked around,
 * because loosening the base class's skip affects every other live tier too.
 */
abstract class SqliteLiveTestCase extends LaravelLiveTestCase
{
    protected const SQLITE_POOL = 'sqlite';

    private string $sqlitePath = '';

    /** @return array<string,string> */
    protected function extraPoolDsns(): array
    {
        if ($this->sqlitePath === '') {
            $this->sqlitePath = sprintf(
                '%s/ferro-lv-%s-%d.sqlite',
                sys_get_temp_dir(),
                substr(hash('xxh128', static::class), 0, 8),
                getmypid(),
            );
        }
        // Deleted BEFORE the daemon opens it, sidecars included — the same ordering the acceptance
        // runner needs, and for the same reason: a stale `-wal` beside a deleted database restores
        // the rows the reset was meant to remove.
        foreach (['', '-wal', '-shm'] as $suffix) {
            @unlink($this->sqlitePath . $suffix);
        }

        return [self::SQLITE_POOL => 'sqlite://' . $this->sqlitePath];
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        if ($this->sqlitePath !== '') {
            foreach (['', '-wal', '-shm'] as $suffix) {
                @unlink($this->sqlitePath . $suffix);
            }
        }
    }

    /**
     * A SQLite connection built through Illuminate's OWN resolver map, with the same contact
     * discipline {@see LaravelLiveTestCase::connection} applies to PostgreSQL.
     *
     * The round-trip probe is a per-call NONCE rather than a constant, for the reason the sibling
     * records: a stub only has to guess a constant. `sqlite_version()` rides along because SQLite
     * has no `version()` at all — the premise that measured FALSE for the engine's own version
     * probe at C3-3e.
     */
    protected function sqliteConnection(): FerroSQLiteConnection
    {
        FerroConnections::register();

        $resolver = IlluminateConnection::getResolver('ferro-sqlite');
        self::assertNotNull($resolver, 'the ferro-sqlite resolver is not registered');

        $conn = $resolver(null, 'ferro_sqlite_label', '', [
            'driver' => 'ferro-sqlite',
            'ferro_socket' => $this->socketPath,
            'pool' => self::SQLITE_POOL,
        ]);

        self::assertInstanceOf(
            FerroSQLiteConnection::class,
            $conn,
            'the resolver did not build a Ferro SQLite connection',
        );
        self::assertInstanceOf(FerroClient::class, $conn->getFerroConnection());

        $nonce = bin2hex(random_bytes(8));
        $probe = $conn->select("select '{$nonce}' as nonce, sqlite_version() as v");
        self::assertCount(1, $probe, 'the contact probe returned no row — nothing was executed');
        self::assertSame($nonce, $probe[0]->nonce, 'the contact probe did not round-trip');
        self::assertMatchesRegularExpression('/^3\./', (string) $probe[0]->v);

        return $conn;
    }
}
