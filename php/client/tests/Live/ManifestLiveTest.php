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
 * The chaos shape: the client's read timeout is 0.5 s and the statement's FIRST execution sleeps
 * 1 s, so the reply is lost AFTER the request was sent — the `Indeterminate` case. Only the first
 * execution sleeps: the sleep is gated on a SEQUENCE, which is non-transactional, so a re-send sees
 * the counter already moved and runs at once, and the counter afterwards says exactly how many times
 * the engine was asked to run the statement. The loss is a link cut on the client's side (the read
 * deadline closes the socket), the same fate class as a daemon restart: the request was written and
 * its answer never came.
 */
final class ManifestLiveTest extends LiveTestCase
{
    private string $manifestPath = '';

    private const QUERIES = [
        'kv.put' => [
            'sql' => "INSERT INTO d2e_kv(k, v) SELECT \$1, \$2 FROM pg_sleep(CASE WHEN nextval('d2e_put_attempts') = 1 THEN 1 ELSE 0 END) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
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
        file_put_contents($this->manifestPath, json_encode(['version' => 1, 'queries' => self::QUERIES], JSON_THROW_ON_ERROR));
        return ['FERRO_MANIFEST' => $this->manifestPath];
    }

    protected function tearDown(): void
    {
        parent::tearDown();
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
            'CREATE TABLE d2e_kv (k int PRIMARY KEY, v text NOT NULL)',
            'CREATE TABLE d2e_log (id serial PRIMARY KEY, v text NOT NULL)',
            'CREATE SEQUENCE d2e_put_attempts', 'CREATE SEQUENCE d2e_add_attempts',
        ] as $sql) {
            $c->exec($sql);
        }
    }

    private function withManifest(float $ioTimeout = 5.0, ?RetryPolicy $policy = null): \Ferro\Client\Connection
    {
        return Ferro::connect($this->socketPath, 'default', 2.0, $ioTimeout, $policy, manifest: Manifest::fromFile($this->manifestPath));
    }

    /** @return int how many times the engine was asked to run the statement */
    private function attempts(string $sequence): int
    {
        return (int) $this->connectConnection()->scalar("SELECT last_value FROM {$sequence}", []);
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
        $c = $this->withManifest(ioTimeout: 0.5);
        $this->assertSame(1, $c->execById('kv.put', [7, 'seven']));

        $this->assertSame(2, $this->attempts('d2e_put_attempts'), 'sent twice: the loss, then the licensed re-send');
        $check = $this->connectConnection();
        $this->assertSame(1, (int) $check->scalar('SELECT count(*) FROM d2e_kv WHERE k = 7', []));
        $this->assertSame('seven', $check->scalar('SELECT v FROM d2e_kv WHERE k = 7', []));
    }

    /** **The control.** The identical loss on a write NOT declared idempotent surfaces; one send. */
    public function testALostUndeclaredWriteIsIndeterminateAndSentOnce(): void
    {
        $this->setUpTables();
        $c = $this->withManifest(ioTimeout: 0.5);
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
        $other = self::QUERIES;
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
