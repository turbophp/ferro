<?php // /php/client/tests/Live/BackupLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Error\NonRetryableException;
use Ferro\Protocol\Generated\Constants as C;

/**
 * M2-C3-7b-2 — `Connection::backup()` end to end through a real `ferrod` whose `FERRO_ADMIN_UIDS`
 * names THIS process (SPEC §7.6, D15). The refusal side is {@see BackupForbiddenLiveTest}.
 */
final class BackupLiveTest extends BackupLiveTestCase
{
    protected function extraEnv(): array
    {
        return ['FERRO_ADMIN_UIDS' => (string) self::ownUid()];
    }

    public function testASnapshotIsTakenAndReplacedAtomically(): void
    {
        $conn = $this->connectConnection(null, self::LITE_POOL);
        $conn->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
        for ($i = 1; $i <= 25; $i++) {
            $conn->exec('INSERT INTO t (v) VALUES (?)', ["row {$i}"]);
        }

        $result = $conn->backup('nightly.db', false, 30_000);
        $snap = $this->liteDir() . '/nightly.db';
        clearstatcache();
        self::assertFileExists($snap);
        self::assertSame(filesize($snap), $result->bytes, 'bytes is the published snapshot\'s size');
        self::assertSame("SQLite format 3\0", (string) file_get_contents($snap, false, null, 0, 16));
        self::assertSame(0600, fileperms($snap) & 0777, 'a snapshot is a full copy of the data');
        self::assertGreaterThan(0, $result->execUs);

        // Without `replace` the existing name is refused, and the snapshot is untouched.
        $before = (string) file_get_contents($snap);
        try {
            $conn->backup('nightly.db');
            self::fail('an existing name without replace must be refused');
        } catch (NonRetryableException $e) {
            self::assertSame(C::ERR_FORBIDDEN, $e->errorPayload()->code);
        }
        self::assertSame($before, (string) file_get_contents($snap));

        // With it, a NEW snapshot (more rows → a different file) is swapped in.
        for ($i = 26; $i <= 2000; $i++) {
            $conn->exec('INSERT INTO t (v) VALUES (?)', [str_repeat('x', 50)]);
        }
        $replaced = $conn->backup('nightly.db', true);
        clearstatcache();
        self::assertSame(filesize($snap), $replaced->bytes);
        self::assertGreaterThan(strlen($before), $replaced->bytes, 'the replacement holds the new rows');

        // No temporary is left in the directory.
        self::assertSame([], glob($this->liteDir() . '/.*.ferro-backup-*') ?: []);
    }

    /** The engine's destination policy reaches the client as `Forbidden`, and the live database's
     * own name is never a target. */
    public function testThePolicyAndTheLiveDatabaseAreRefused(): void
    {
        $conn = $this->connectConnection(null, self::LITE_POOL);
        $conn->exec('CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)');
        foreach (['../escape.db', 'sub/dir.db', '.hidden.db', 'main.db', 'main.db-wal'] as $bad) {
            try {
                $conn->backup($bad, true);
                self::fail("{$bad} must be refused");
            } catch (NonRetryableException $e) {
                self::assertSame(C::ERR_FORBIDDEN, $e->errorPayload()->code, $bad);
            }
        }
        self::assertSame(1, $conn->scalar('SELECT 1'), 'the connection is still usable');
    }

    /** A server database is not snapshotted by the engine. */
    public function testAServerPoolIsUnsupported(): void
    {
        $conn = $this->connectConnection(null, 'default');
        try {
            $conn->backup('snap.db');
            self::fail('a PostgreSQL pool must be refused');
        } catch (NonRetryableException $e) {
            self::assertSame(C::ERR_UNSUPPORTED, $e->errorPayload()->code);
        }
    }
}
