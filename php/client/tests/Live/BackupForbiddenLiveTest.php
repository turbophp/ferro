<?php // /php/client/tests/Live/BackupForbiddenLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Error\NonRetryableException;
use Ferro\Protocol\Generated\Constants as C;

/**
 * The DEFAULT deployment (SPEC D15): `FERRO_ADMIN_UIDS` unset, so OPERATE is disabled for every
 * peer — including the daemon's own uid, which this test process usually is. Same pool, same
 * request as {@see BackupLiveTest}'s success path; only the engine's configuration differs (this
 * class adds nothing to the base's empty {@see LiveTestCase::extraEnv}).
 */
final class BackupForbiddenLiveTest extends BackupLiveTestCase
{
    public function testTheDefaultConfigurationRefusesBackup(): void
    {
        $conn = $this->connectConnection(null, self::LITE_POOL);
        $conn->exec('CREATE TABLE t (id INTEGER PRIMARY KEY)');
        try {
            $conn->backup('nightly.db');
            self::fail('with FERRO_ADMIN_UIDS empty, BACKUP must be refused');
        } catch (NonRetryableException $e) {
            self::assertSame(C::ERR_FORBIDDEN, $e->errorPayload()->code);
        }
        self::assertFileDoesNotExist($this->liteDir() . '/nightly.db');
    }

    public function testTheRefusalComesBeforeTheNamePolicy(): void
    {
        // The refusal comes BEFORE the policy: even a valid name is Forbidden, and an unknown pool
        // is too (the D15 gate runs before any pool lookup).
        $conn = $this->connectConnection(null, self::LITE_POOL);
        try {
            $conn->backup('valid.db');
            self::fail('refused');
        } catch (NonRetryableException $e) {
            self::assertSame(C::ERR_FORBIDDEN, $e->errorPayload()->code);
        }
    }

    public function testAServerPoolIsForbiddenNotUnsupported(): void
    {
        // Without OPERATE, a server pool is ALSO Forbidden — the gate answers first, so a non-admin
        // learns nothing about what kind of pool a name refers to.
        $conn = $this->connectConnection(null, 'default');
        try {
            $conn->backup('snap.db');
            self::fail('refused');
        } catch (NonRetryableException $e) {
            self::assertSame(C::ERR_FORBIDDEN, $e->errorPayload()->code);
        }
    }
}
