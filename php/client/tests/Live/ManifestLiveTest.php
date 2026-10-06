<?php // /php/client/tests/Live/ManifestLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Error\HandshakeException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\RetryPolicy;
use Ferro\Ferro;
use Ferro\Manifest;

/**
 * **M3-D2e, live: queries by id, and SPEC §9.2's one licence to re-send a write whose fate is
 * unknown, against a real `ferrod` with `FERRO_MANIFEST` and a real PostgreSQL.**
 *
 * The chaos shape: the statement timeout is 0.5 s and the statement's FIRST execution sleeps 1 s,
 * so the engine cancels it after it was sent and answers `WriteUnconfirmed` — the `Indeterminate`
 * case, declared by the engine. Only the first execution sleeps: the sleep is gated on a SEQUENCE,
 * which is non-transactional, so a re-send sees the counter already moved and runs at once, and the
 * counter afterwards says exactly how many times the engine was asked to run the statement.
 *
 * Until M3-D1c the loss was a client-side link cut instead: a 0.5 s READ timeout closed the socket
 * mid-statement. D1c made a read timeout a liveness probe rather than a failure — a 1 s statement
 * under a 0.5 s read timeout now simply succeeds — so the shape moved to the engine-declared half of
 * the same licence, which the client treats identically (SPEC §22.2 (co), (cp)).
 */
final class ManifestLiveTest extends LiveTestCase
{
    private string $manifestPath = '';

    /**
     * This run's names for the fixture's tables and sequences. The live tiers of several checkouts
     * share one PostgreSQL database, and fixed names let one run's DROP/CREATE land in the middle of
     * another's statement (`relation "d2e_kv" does not exist`, reproduced in every one of 16 rounds
     * of two concurrent runs). The manifest is generated per run from these, so its SQL — and its
     * hash — are this run's too.
     */
    private static function t(string $sql): string
    {
        $suffix = '_' . getmypid();
        return strtr($sql, [
            'd2e_kv' => 'd2e_kv' . $suffix,
            'd2e_log' => 'd2e_log' . $suffix,
            'd2e_put_attempts' => 'd2e_put_attempts' . $suffix,
            'd2e_add_attempts' => 'd2e_add_attempts' . $suffix,
        ]);
    }

    /** @return array<string, array{sql: string, pool: string, readonly: bool, idempotent: bool}> */
    private static function queries(): array
    {
        $q = self::QUERIES;
        foreach ($q as $id => $entry) {
            $q[$id]['sql'] = self::t($entry['sql']);
        }
        return $q;
    }

    private const QUERIES = [
        'kv.put' => [
            // `hits` counts APPLICATIONS (review F5: `SET v = EXCLUDED.v` alone passes whether the
            // statement applied once or twice, so "applied once" was asserted by nothing).
            'sql' => "INSERT INTO d2e_kv(k, v, hits) SELECT \$1, \$2, 1 FROM pg_sleep(CASE WHEN nextval('d2e_put_attempts') = 1 THEN 1 ELSE 0 END) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, hits = d2e_kv.hits + 1",
            'pool' => 'default', 'readonly' => false, 'idempotent' => true,
        ],
        'log.add' => [
            'sql' => "INSERT INTO d2e_log(v) SELECT \$1 FROM pg_sleep(CASE WHEN nextval('d2e_add_attempts') = 1 THEN 1 ELSE 0 END)",
            'pool' => 'default', 'readonly' => false, 'idempotent' => false,
        ],
        'kv.get' => [
            'sql' => 'SELECT v FROM d2e_kv WHERE k = $1',
            'pool' => 'default', 'readonly' => true, 'idempotent' => false,
        ],
    ];

    protected function extraEnv(): array
    {
        $this->manifestPath = sys_get_temp_dir() . '/ferro-d2e-manifest-' . getmypid() . '.json';
        file_put_contents($this->manifestPath, json_encode(['version' => 1, 'queries' => self::queries()], JSON_THROW_ON_ERROR));
        return ['FERRO_MANIFEST' => $this->manifestPath];
    }

    protected function tearDown(): void
    {
        try {
            if ($this->socketPath !== '' && file_exists($this->socketPath)) {
                $c = $this->connectConnection();
                foreach (['DROP TABLE IF EXISTS d2e_kv', 'DROP TABLE IF EXISTS d2e_log', 'DROP SEQUENCE IF EXISTS d2e_put_attempts', 'DROP SEQUENCE IF EXISTS d2e_add_attempts'] as $sql) {
                    $c->exec(self::t($sql));
                }
                $c->session()->close();
            }
        } catch (\Throwable) {
            // best effort: a leftover fixture is this run's alone
        } finally {
            parent::tearDown();
        }
        if ($this->manifestPath !== '' && file_exists($this->manifestPath)) {
            @unlink($this->manifestPath);
        }
    }

    private function setUpTables(): void
    {
        $c = $this->connectConnection();
        foreach ([
            'DROP TABLE IF EXISTS d2e_kv', 'DROP TABLE IF EXISTS d2e_log',
            'DROP SEQUENCE IF EXISTS d2e_put_attempts', 'DROP SEQUENCE IF EXISTS d2e_add_attempts',
            'CREATE TABLE d2e_kv (k int PRIMARY KEY, v text NOT NULL, hits int NOT NULL)',
            'CREATE TABLE d2e_log (id serial PRIMARY KEY, v text NOT NULL)',
            'CREATE SEQUENCE d2e_put_attempts', 'CREATE SEQUENCE d2e_add_attempts',
        ] as $sql) {
            $c->exec(self::t($sql));
        }
    }

    private function withManifest(?float $statementTimeout = null, ?RetryPolicy $policy = null): \Ferro\Client\Connection
    {
        return Ferro::connect($this->socketPath, 'default', 2.0, 5.0, $policy, statementTimeout: $statementTimeout, manifest: Manifest::fromFile($this->manifestPath));
    }

    /** @return int how many times the engine was asked to run the statement */
    private function attempts(string $sequence): int
    {
        return (int) $this->connectConnection()->scalar(self::t("SELECT last_value FROM {$sequence}"), []);
    }

    public function testQueriesRunByIdOnTheEnginesSql(): void
    {
        $this->setUpTables();
        $c = $this->withManifest();
        $this->assertSame(1, $c->execById('kv.put', [1, 'one']));
        $this->assertSame('one', $c->scalarById('kv.get', [1]));
        $this->assertSame([['v' => 'one']], $c->queryById('kv.get', [1]));
    }

    /**
     * **The licence, end to end.** The first send's reply is lost; the declared-idempotent upsert is
     * re-sent on a fresh session, the call SUCCEEDS, and the row is there exactly once.
     */
    public function testALostIdempotentWriteIsResentAndAppliedOnce(): void
    {
        $this->setUpTables();
        $c = $this->withManifest(statementTimeout: 0.5);
        $this->assertSame(1, $c->execById('kv.put', [7, 'seven']));

        $this->assertSame(2, $this->attempts('d2e_put_attempts'), 'sent twice: the loss, then the licensed re-send');
        $check = $this->connectConnection();
        $this->assertSame(1, (int) $check->scalar(self::t('SELECT count(*) FROM d2e_kv WHERE k = 7'), []));
        $this->assertSame('seven', $check->scalar(self::t('SELECT v FROM d2e_kv WHERE k = 7'), []));
        // Two sends, ONE application: the first send's statement was cancelled by `ferrod` at its
        // statement timeout mid-sleep, so the licensed re-send is the only one that applied. (Had
        // it applied too, the declared-idempotent upsert would still leave one row — which is why
        // the licence is safe — but `hits` would read 2.)
        $this->assertSame(1, (int) $check->scalar(self::t('SELECT hits FROM d2e_kv WHERE k = 7'), []));
    }

    /** Review F3: inside a closure transaction a query by id runs IN it, and its rollback undoes it. */
    public function testAQueryByIdInAClosureTransactionIsRolledBackWithIt(): void
    {
        $this->setUpTables();
        $c = $this->withManifest();
        try {
            $c->transaction(static function (\Ferro\Client\TxHandle $tx): void {
                $tx->execById('kv.put', [4, 'four']);
                throw new \LogicException('roll it back');
            }, RetryPolicy::none());
        } catch (\LogicException) {
        }
        $this->assertSame(0, (int) $this->connectConnection()->scalar(self::t('SELECT count(*) FROM d2e_kv WHERE k = 4'), []));
    }

    /** **The control.** The identical loss on a write NOT declared idempotent surfaces; one send. */
    public function testALostUndeclaredWriteIsIndeterminateAndSentOnce(): void
    {
        $this->setUpTables();
        $c = $this->withManifest(statementTimeout: 0.5);
        try {
            $c->execById('log.add', ['x']);
            $this->fail('a lost undeclared write must surface');
        } catch (IndeterminateException) {
        }
        $this->assertSame(1, $this->attempts('d2e_add_attempts'), 'never re-sent');
    }

    /** The policy switch turns the licence off: the same loss as the licence test now surfaces. */
    public function testTheLicenceCanBeSwitchedOff(): void
    {
        $this->setUpTables();
        $c = $this->withManifest(0.5, new RetryPolicy(retryIdempotentWrites: false));
        try {
            $c->execById('kv.put', [8, 'eight']);
            $this->fail('with the licence off, the loss surfaces');
        } catch (IndeterminateException) {
        }
        $this->assertSame(1, $this->attempts('d2e_put_attempts'));
    }

    /** A client built against another manifest is refused at connect (M3-D2d). */
    public function testAClientWithAnotherManifestIsRefusedAtConnect(): void
    {
        $other = self::queries();
        $other['log.add']['idempotent'] = true; // the edit that would license a double write
        $this->expectException(HandshakeException::class);
        $this->expectExceptionMessage('manifest_hash mismatch');
        Ferro::connect($this->socketPath, manifest: Manifest::fromJson(json_encode(['version' => 1, 'queries' => $other], JSON_THROW_ON_ERROR)));
    }

    /** A client with no manifest cannot run by id, even by sending the request itself. */
    public function testASessionWithoutTheHashCannotRunById(): void
    {
        $this->setUpTables();
        $raw = $this->connect();
        $payload = (new \Ferro\Client\ExecCodec(
            new \Ferro\Client\Value\M1ValuePolicy(new \Ferro\Client\Value\TypePolicyOptions()),
            new \Ferro\Client\Hydration\PlanCache(),
            \Ferro\Protocol\Msgpack\PackerFactory::forEncode(),
            \Ferro\Protocol\Msgpack\PackerFactory::forDecode(),
        ))->encode('default', '', [1], true, \Ferro\Client\ExecCodec::FETCH_ROWS, null, 'kv.get');
        $outcome = $raw->sendRequest(\Ferro\Protocol\Generated\Constants::SERVICE_SQL, \Ferro\Protocol\Generated\Constants::METHOD_SQL_EXEC, $payload);
        $this->assertFalse($outcome->isOk());
        $this->expectException(NonRetryableException::class);
        $this->expectExceptionMessage('manifest_hash');
        throw \Ferro\Client\Error\ErrorMapper::fromOutcome($outcome);
    }
}
