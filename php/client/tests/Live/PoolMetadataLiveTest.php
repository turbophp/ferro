<?php // /php/client/tests/Live/PoolMetadataLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Session;
use Ferro\Protocol\PoolInfo;

/**
 * M1-S8a Task 12 — the connect-time metadata a Doctrine DBAL driver reads, end to end through a
 * REAL `ferrod` process against a REAL Postgres and a REAL MySQL.
 *
 * A driver picks its PLATFORM from these two fields: `kind` gives it the backend family before any
 * statement has run, and `serverVersion` is the raw `version()` string it version-gates on. Both
 * must arrive on the handshake — not after a probe query the driver would have to write in a
 * dialect it does not yet know it needs.
 */
final class PoolMetadataLiveTest extends LiveTestCase
{
    public function testHandshakeAdvertisesEachPoolsKindAndVersion(): void
    {
        $this->requireMysqlPool();
        // `connect()` handshakes (see LiveTestCase) — without that, `poolInfo()` is [] and every
        // assertion below fails for a reason unrelated to what is being tested.
        $session = $this->connect();
        $this->assertInstanceOf(Session::class, $session);
        $this->assertNotSame([], $session->poolInfo(), 'the session must have handshaken');

        $byName = [];
        foreach ($session->poolInfo() as $p) {
            $byName[$p->name] = $p;
        }
        $this->assertArrayHasKey('default', $byName);
        $this->assertArrayHasKey(self::MYSQL_POOL, $byName);

        $this->assertSame('postgres', $byName['default']->kind);
        $this->assertStringStartsWith('PostgreSQL ', (string) $byName['default']->serverVersion);

        $this->assertSame('mysql', $byName[self::MYSQL_POOL]->kind);
        $myVersion = $byName[self::MYSQL_POOL]->serverVersion;
        $this->assertNotNull($myVersion);
        $this->assertMatchesRegularExpression(
            '/^\d/',
            $myVersion,
            "the MySQL family reports version() starting with a digit, got '{$myVersion}'",
        );

        // The name-only accessor is unchanged for existing callers.
        $this->assertEqualsCanonicalizing(['default', self::MYSQL_POOL], $session->pools());

        $session->close();
    }

    /**
     * A SECOND connection to the SAME running `ferrod` sees the identical metadata — the version is
     * learned per DAEMON (one shared `PoolRegistry` behind `serve`), not per connection.
     *
     * This asserts STABILITY only, and says so: it would pass identically against an engine that
     * re-probed on every handshake. The claim that the value is genuinely CACHED is asserted on the
     * engine side, against a probe COUNTER that a lost cache makes go up
     * (`engine/crates/ferrod/tests/hello_meta_it.rs`). Overstating this one would be exactly the
     * "guard that cannot fail" this project keeps finding.
     */
    public function testASecondConnectionSeesTheSameMetadata(): void
    {
        $this->requireMysqlPool();
        $a = $this->connect();
        $b = $this->connect();

        $flatten = static fn (Session $s): array => array_map(
            static fn (PoolInfo $p): array => [$p->name, $p->kind, $p->serverVersion],
            $s->poolInfo(),
        );

        $this->assertSame($flatten($a), $flatten($b));

        $a->close();
        $b->close();
    }
}
