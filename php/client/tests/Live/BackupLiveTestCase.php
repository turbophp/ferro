<?php // /php/client/tests/Live/BackupLiveTestCase.php
declare(strict_types=1);
namespace Ferro\Tests\Live;


/**
 * M2-C3-7b-2 — the shared fixture for `Connection::backup()` live tests: a `ferrod` with a SQLite pool
 * in a fresh private directory. {@see BackupLiveTest} names THIS process in `FERRO_ADMIN_UIDS`;
 * {@see BackupForbiddenLiveTest} leaves it at the default (empty → OPERATE disabled). The engine's
 * own matrix (live database names, symlinks, atomic replace, cancellation) is `ferrod`'s
 * `admin_it.rs`.
 *
 * The SQLite pool is created by the test and seeded THROUGH `ferrod`, so the snapshot is checked
 * against rows the engine itself wrote. The snapshot is verified by its file header and size rather
 * than by opening it, because the client is dependency-free (charter rule 7) and the test should not
 * assume `ext-sqlite3`.
 */
abstract class BackupLiveTestCase extends LiveTestCase
{
    protected const LITE_POOL = 'lite';

    private ?string $dir = null;

    /** The directory the SQLite pool's database lives in — and so, by default, its snapshots. */
    protected function liteDir(): string
    {
        if ($this->dir === null) {
            $dir = sys_get_temp_dir() . '/ferro-backup-' . getmypid() . '-' . bin2hex(random_bytes(4));
            mkdir($dir, 0700);
            $this->dir = $dir;
        }
        return $this->dir;
    }

    protected function extraPoolDsns(): array
    {
        return [self::LITE_POOL => 'sqlite://' . $this->liteDir() . '/main.db'];
    }

    /**
     * This process's effective uid without `ext-posix` (charter rule 7): the owner of a file it just
     * created. That is the uid `SO_PEERCRED` attests for its socket.
     */
    protected static function ownUid(): int
    {
        $probe = tempnam(sys_get_temp_dir(), 'ferro-uid-');
        if ($probe === false) {
            self::fail('cannot create a probe file to learn the process uid');
        }
        $uid = fileowner($probe);
        unlink($probe);
        if ($uid === false) {
            self::fail('cannot read the probe file owner');
        }
        return $uid;
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        if ($this->dir !== null && is_dir($this->dir)) {
            foreach (glob($this->dir . '/{,.}*', GLOB_BRACE) ?: [] as $f) {
                if (is_file($f)) { unlink($f); }
            }
            rmdir($this->dir);
        }
    }
}
