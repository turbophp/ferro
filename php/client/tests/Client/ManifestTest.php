<?php // /php/client/tests/Client/ManifestTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Backoff;
use Ferro\Client\Connection;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\ManifestException;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\RetryPolicy;
use Ferro\Client\Session;
use Ferro\Manifest;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * M3-D2e: the client side of checked SQL — the manifest, queries by id, and SPEC §9.2's one licence
 * to re-send a write whose fate is unknown.
 */
final class ManifestTest extends TestCase
{
    /** @param array<string, array<string, mixed>> $queries */
    private static function json(array $queries, ?string $hash = null): string
    {
        $doc = ['version' => 1, 'queries' => $queries];
        if ($hash !== null) {
            $doc['hash'] = $hash;
        }
        return json_encode($doc, JSON_THROW_ON_ERROR);
    }

    /** @return array<string, array<string, mixed>> */
    private static function queries(): array
    {
        return [
            'users.upsert' => ['sql' => 'INSERT INTO u VALUES (?) ON CONFLICT DO NOTHING', 'pool' => 'reports', 'readonly' => false, 'idempotent' => true],
            'users.bump' => ['sql' => 'UPDATE u SET n = n + 1', 'pool' => 'default', 'readonly' => false, 'idempotent' => false],
            'users.find' => ['sql' => 'SELECT 1', 'pool' => 'default', 'readonly' => true, 'idempotent' => false],
        ];
    }

    /**
     * The hash is the ENGINE's: these are `ferro-manifest`'s known answers, computed with
     * `sha256sum` outside both codebases (`the_hash_is_a_known_answer_not_only_its_bytes`), so a PHP
     * escaping that differs from serde_json's by one byte fails here, not at HELLO in production.
     */
    public function testTheHashIsTheEnginesKnownAnswer(): void
    {
        $a = Manifest::fromJson(self::json(['a' => ['sql' => 'SELECT 1', 'pool' => 'default', 'readonly' => true, 'idempotent' => false]]));
        $this->assertSame('7c185434dc7b0d35d0fc7c66490341463e7118bdec905fdcd76a3dfb4416c84b', $a->hash());
        $b = Manifest::fromJson(self::json(['a.x' => [
            'sql' => "SELECT 'éé' -- ☃\n\t\x01 \"q\" \\ /", 'pool' => 'p', 'readonly' => false, 'idempotent' => true,
        ]]));
        $this->assertSame(
            '{"v":1,"q":{"a.x":{"sql":"SELECT \'éé\' -- ☃\n\t\u0001 \"q\" \\\\ /","pool":"p","readonly":false,"idempotent":true}}}',
            Manifest::canonical(['a.x' => $b->query('a.x')]),
        );
        $this->assertSame('87def7dc2e2b8a97d15156587806242864fc30b9b61c4119e709c1293f4fe274', $b->hash());
        // U+2028 is left raw by serde_json; PHP escapes it unless told not to.
        $c = Manifest::fromJson(self::json(['a' => ['sql' => "SELECT '\u{2028}'", 'pool' => 'p', 'readonly' => true, 'idempotent' => false]]));
        $this->assertStringContainsString("\u{2028}", Manifest::canonical(['a' => $c->query('a')]));
    }

    public function testIdOrderAndDtoSourceDoNotChangeTheHash(): void
    {
        $q = self::queries();
        $reordered = array_reverse($q, true);
        $reordered['users.find']['dto'] = 'App\\Dto\\User';
        $reordered['users.find']['source'] = 'x.sql';
        $this->assertSame(Manifest::fromJson(self::json($q))->hash(), Manifest::fromJson(self::json($reordered))->hash());
    }

    /**
     * A file whose recorded hash is not the hash of its queries was edited after `ferro manifest`
     * wrote it — the classic edit is flipping `idempotent` — and is refused rather than believed.
     */
    public function testAnEditedFileIsRefused(): void
    {
        $q = self::queries();
        $hash = Manifest::fromJson(self::json($q))->hash();
        Manifest::fromJson(self::json($q, $hash)); // the honest file loads
        $q['users.bump']['idempotent'] = true;
        $this->expectException(ManifestException::class);
        $this->expectExceptionMessage('edited after');
        Manifest::fromJson(self::json($q, $hash));
    }

    /** @return array<string, array{0: string, 1: string}> */
    public static function malformed(): array
    {
        $ok = ['sql' => 'SELECT 1', 'pool' => 'default', 'readonly' => true, 'idempotent' => false];
        return [
            'not JSON' => ['{', 'not JSON'],
            'version 2' => ['{"version":2,"queries":{"a":' . json_encode($ok) . '}}', 'version'],
            'no queries' => ['{"version":1,"queries":{}}', 'no queries'],
            'unknown top field' => ['{"version":1,"hsh":"x","queries":{"a":' . json_encode($ok) . '}}', 'unknown manifest field'],
            'unknown query field' => [self::json(['a' => $ok + ['idempotnet' => true]]), 'unknown field `idempotnet`'],
            'string flag' => [self::json(['a' => ['idempotent' => 'true'] + $ok]), 'boolean'],
            'digit id' => [self::json(['1abc' => $ok]), 'invalid query id'],
            'empty sql' => [self::json(['a' => ['sql' => ''] + $ok]), 'non-empty'],
        ];
    }

    #[DataProvider('malformed')]
    public function testAMalformedManifestIsRefused(string $json, string $message): void
    {
        $this->expectException(ManifestException::class);
        $this->expectExceptionMessage($message);
        Manifest::fromJson($json);
    }

    // ---- the wire ----------------------------------------------------------------------------

    private static function frame(int $flags, int $service, int $method, int $rid, string $payload): string
    {
        return (new Codec())->encodeFrame(new Header($flags, $service, $method, $rid, strlen($payload)), $payload);
    }

    private static function helloAck(): string
    {
        $payload = Message::encode('hello_ack', [
            'engine_version' => 1, 'boot_epoch' => 1, 'features' => C::FEATURE_ENGINE_MANIFEST,
            'pools' => [['name' => 'default', 'kind' => 'postgres', 'server_version' => null, 'literals_are_standard' => true]],
            'type_registry_hash' => C::TYPE_REGISTRY_HASH,
        ], PackerFactory::forEncode());
        return self::frame(0, C::SERVICE_CORE, C::METHOD_CORE_HELLO_ACK, 0, $payload);
    }

    private static function ok(int $rid, int $affected = 1): string
    {
        $p = PackerFactory::forEncode();
        $body = ExecOk::encode([
            'cols' => [], 'rows' => [], 'affected' => $affected, 'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
        ], $p);
        return self::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, Outcome::ok($body)->encode($p));
    }

    private static function indeterminate(int $rid): string
    {
        $err = new ErrorPayload(C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, null, null, 'write unconfirmed', null, null);
        return self::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, Outcome::error($err)->encode(PackerFactory::forEncode()));
    }

    /**
     * Every frame the client wrote, decoded positionally.
     *
     * @return list<array{0: Header, 1: array<int, mixed>}>
     */
    private static function sent(FakeTransport $t): array
    {
        $out = [];
        $bytes = $t->written;
        while ($bytes !== '') {
            [$h, $payload] = (new Codec())->decodeFrame($bytes);
            $bytes = (string) substr($bytes, 16 + $h->payloadLen);
            $off = 0;
            $w = PackerFactory::forDecode()->unpack($payload, $off);
            $out[] = [$h, is_array($w) ? array_values($w) : []];
        }
        return $out;
    }

    /**
     * A connection with `$manifest` over `$first`, reconnecting onto `$fresh`.
     *
     * @param list<FakeTransport> $fresh
     * @return array{0: Connection, 1: \ArrayObject<int, int>}
     */
    private static function connection(FakeTransport $first, array $fresh, ?Manifest $manifest, ?RetryPolicy $policy = null): array
    {
        $dials = new \ArrayObject();
        $hash = $manifest?->hash();
        $first->feed(self::helloAck());
        $s = new Session($first, new RequestIdAllocator(0));
        $s->hello($hash);
        $loop = new ReconnectLoop(
            $s,
            static function () use (&$fresh, $dials, $hash): Session {
                $dials->append(1);
                $t = array_shift($fresh);
                self::assertNotNull($t, 'no more sessions to dial');
                $session = new Session($t, new RequestIdAllocator(0));
                $session->hello($hash);
                return $session;
            },
            new Backoff(0, 0, rng: static fn (): float => 0.0, sleep: static function (float $_): void {}),
            1,
        );
        $conn = new Connection(
            session: $loop->session(),
            pool: 'default',
            reconnect: $loop,
            policy: $policy ?? new RetryPolicy(maxAttempts: 3, baseDelaySeconds: 0.0, maxDelaySeconds: 0.0),
            manifest: $manifest,
        );
        return [$conn, $dials];
    }

    public function testHelloCarriesTheManifestHashAndAQueryByIdCarriesTheDeclarations(): void
    {
        $m = Manifest::fromJson(self::json(self::queries()));
        $t = new FakeTransport();
        [$conn] = self::connection($t, [], $m);
        $t->feed(self::ok(1));
        $this->assertSame(1, $conn->execById('users.upsert', [5]));

        [[$helloHeader, $hello], [, $exec]] = self::sent($t);
        $this->assertSame(C::METHOD_CORE_HELLO, $helloHeader->method);
        $this->assertSame($m->hash(), $hello[2], 'HELLO field 3 is the manifest hash');
        $req = ExecRequest::mapFromWire($exec);
        $this->assertNull($req['sql'], 'sql is nil when a query_id is sent');
        $this->assertSame('users.upsert', $req['query_id']);
        $this->assertSame('reports', $req['pool'], 'the declared pool, not the connection default');
        $this->assertFalse($req['readonly']);
    }

    public function testAClientWithoutAManifestSendsNoHashAndCannotRunById(): void
    {
        $t = new FakeTransport();
        [$conn] = self::connection($t, [], null);
        [[, $hello]] = self::sent($t);
        $this->assertNull($hello[2]);
        $this->expectException(ManifestException::class);
        $conn->execById('users.upsert');
    }

    public function testAnUnknownIdIsRefusedBeforeAnythingIsSent(): void
    {
        $t = new FakeTransport();
        [$conn] = self::connection($t, [], Manifest::fromJson(self::json(self::queries())));
        $before = $t->writeCalls;
        try {
            $conn->execById('users.nope');
            $this->fail('an unknown id must be refused');
        } catch (ManifestException $e) {
            $this->assertStringContainsString('users.nope', $e->getMessage());
        }
        $this->assertSame($before, $t->writeCalls);
    }

    /**
     * **The licence.** A declared-idempotent write whose reply is lost is re-sent on a fresh
     * session and answers from there.
     */
    public function testALostIdempotentWriteIsResentOnAFreshSession(): void
    {
        $first = new FakeTransport();
        $second = new FakeTransport();
        [$conn, $dials] = self::connection($first, [$second], Manifest::fromJson(self::json(self::queries())));
        $second->feed(self::helloAck());
        $second->feed(self::ok(1));
        // Nothing is fed on `$first` for the request: it was written, and the read hits EOF.
        $this->assertSame(1, $conn->execById('users.upsert', [5]));
        $this->assertCount(1, $dials);
        $this->assertCount(2, self::sent($first), 'HELLO + the first send');
        $this->assertCount(2, self::sent($second), 'HELLO + the re-send');
    }

    /** **The control.** The same loss on a write NOT declared idempotent surfaces, never re-sent. */
    public function testALostUndeclaredWriteIsIndeterminateAndNeverResent(): void
    {
        $first = new FakeTransport();
        $second = new FakeTransport();
        [$conn, $dials] = self::connection($first, [$second], Manifest::fromJson(self::json(self::queries())));
        try {
            $conn->execById('users.bump');
            $this->fail('a lost undeclared write must not succeed');
        } catch (IndeterminateException) {
        }
        $this->assertCount(0, $dials, 'no reconnect was made to re-send it');
    }

    /** An inline write is never licensed, whatever the manifest declares for its text. */
    public function testAnInlineWriteIsNeverLicensedEvenIfAManifestQueryHasTheSameSql(): void
    {
        $first = new FakeTransport();
        [$conn, $dials] = self::connection($first, [new FakeTransport()], Manifest::fromJson(self::json(self::queries())));
        try {
            $conn->exec('INSERT INTO u VALUES (?) ON CONFLICT DO NOTHING', [5]);
            $this->fail('a lost inline write must not succeed');
        } catch (IndeterminateException) {
        }
        $this->assertCount(0, $dials);
    }

    /** The engine itself may report Indeterminate (a timed-out write); the licence covers that too. */
    public function testAServerDeclaredIndeterminateIdempotentWriteIsResentOnTheLiveSession(): void
    {
        $t = new FakeTransport();
        [$conn, $dials] = self::connection($t, [], Manifest::fromJson(self::json(self::queries())));
        $t->feed(self::indeterminate(1));
        $t->feed(self::ok(2));
        $this->assertSame(1, $conn->execById('users.upsert', [5]));
        $this->assertCount(0, $dials, 'the session was alive: no reconnect');
        $this->assertCount(3, self::sent($t), 'HELLO + two sends');
    }

    public function testThePolicyCanSwitchTheLicenceOff(): void
    {
        $first = new FakeTransport();
        [$conn, $dials] = self::connection(
            $first,
            [new FakeTransport()],
            Manifest::fromJson(self::json(self::queries())),
            new RetryPolicy(maxAttempts: 3, baseDelaySeconds: 0.0, maxDelaySeconds: 0.0, retryIdempotentWrites: false),
        );
        try {
            $conn->execById('users.upsert', [5]);
            $this->fail('with the licence off, a lost write surfaces');
        } catch (IndeterminateException) {
        }
        $this->assertCount(0, $dials);
    }

    /** The licence is bounded by `maxAttempts` like any retry. */
    public function testTheLicenceIsBoundedByMaxAttempts(): void
    {
        $first = new FakeTransport();
        $second = new FakeTransport();
        [$conn, $dials] = self::connection(
            $first,
            [$second],
            Manifest::fromJson(self::json(self::queries())),
            new RetryPolicy(maxAttempts: 2, baseDelaySeconds: 0.0, maxDelaySeconds: 0.0),
        );
        $second->feed(self::helloAck()); // and then EOF again
        try {
            $conn->execById('users.upsert', [5]);
            $this->fail('two lost sends with maxAttempts=2 surface the second');
        } catch (IndeterminateException) {
        }
        $this->assertCount(1, $dials);
    }

    public function testRetryPolicyNoneSwitchesTheLicenceOff(): void
    {
        $this->assertFalse(RetryPolicy::none()->retryIdempotentWrites);
        $this->assertTrue(RetryPolicy::default()->retryIdempotentWrites);
    }

    // ---- M3-D2e review round ----------------------------------------------------------------

    private static function begin(int $rid, int $txId): string
    {
        $p = PackerFactory::forEncode();
        return self::frame(C::FLAG_END, C::SERVICE_TX, C::METHOD_TX_BEGIN, $rid, Outcome::ok(\Ferro\Protocol\BeginResponse::encode(['tx_id' => $txId], $p))->encode($p));
    }

    private static function txDone(int $rid, int $method): string
    {
        return self::frame(C::FLAG_END, C::SERVICE_TX, $method, $rid, Outcome::ok('')->encode(PackerFactory::forEncode()));
    }

    /** @return list<array<string, mixed>> the EXEC requests the client wrote */
    private static function execs(FakeTransport $t): array
    {
        $out = [];
        foreach (self::sent($t) as [$h, $w]) {
            if ($h->service === C::SERVICE_SQL && $h->method === C::METHOD_SQL_EXEC) {
                $out[] = ExecRequest::mapFromWire($w);
            }
        }
        return $out;
    }

    /** F4/M3: the licence is for an UNKNOWN fate only — a definite error is never re-sent. */
    public function testADefiniteErrorOnAnIdempotentWriteIsNotResent(): void
    {
        $t = new FakeTransport();
        [$conn, $dials] = self::connection($t, [], Manifest::fromJson(self::json(self::queries())));
        $err = new ErrorPayload(C::ERR_UNIQUE, C::BRANCH_NON_RETRYABLE, '23505', null, 'duplicate key', null, null);
        $t->feed(self::frame(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, Outcome::error($err)->encode(PackerFactory::forEncode())));
        try {
            $conn->execById('users.upsert', [5]);
            $this->fail('a definite error must surface');
        } catch (\Ferro\Client\Error\NonRetryableException) {
        }
        $this->assertCount(1, self::execs($t), 'sent once');
        $this->assertCount(0, $dials);
    }

    /**
     * F2: when the licensed re-send cannot reconnect, the caller learns the WRITE's fate
     * (Indeterminate), not the dial's — a dial failure reads as "nothing was sent".
     */
    public function testAFailedReconnectDuringTheLicenceStillReportsIndeterminate(): void
    {
        $first = new FakeTransport();
        $dead = new FakeTransport(); // no HELLO_ACK: the handshake on the "restarted" engine fails
        [$conn] = self::connection($first, [$dead], Manifest::fromJson(self::json(self::queries())));
        $this->expectException(IndeterminateException::class);
        $conn->execById('users.upsert', [5]);
    }

    /** F3: a query by id in an IMPERATIVE transaction rides the transaction, and is never re-sent. */
    public function testAQueryByIdInAnImperativeTransactionCarriesTheTxId(): void
    {
        $t = new FakeTransport();
        [$conn] = self::connection($t, [], Manifest::fromJson(self::json(self::queries())));
        $t->feed(self::begin(1, 42));
        $t->feed(self::ok(2));
        $conn->begin();
        $conn->execById('users.bump');
        $execs = self::execs($t);
        $this->assertSame(42, $execs[0]['tx_id']);
        $this->assertSame('users.bump', $execs[0]['query_id']);
        // Declared for another pool than the transaction's: refused before sending.
        $this->expectException(ManifestException::class);
        $this->expectExceptionMessage('this transaction runs on `default`');
        $conn->execById('users.upsert');
    }

    /**
     * F3: inside a closure transaction, the TxHandle runs a query by id IN the transaction, and the
     * Connection refuses one — it used to run it outside the transaction, surviving its rollback.
     */
    public function testInsideAClosureTransactionQueriesByIdRunOnTheTxHandle(): void
    {
        $t = new FakeTransport();
        [$conn] = self::connection($t, [], Manifest::fromJson(self::json(self::queries())));
        $t->feed(self::begin(1, 43));
        $t->feed(self::ok(2));
        $t->feed(self::txDone(3, C::METHOD_TX_COMMIT));
        $refused = null;
        $conn->transaction(static function (\Ferro\Client\TxHandle $tx) use ($conn, &$refused): void {
            $tx->execById('users.bump');
            try {
                $conn->execById('users.bump');
            } catch (ManifestException $e) {
                $refused = $e->getMessage();
            }
        });
        $this->assertNotNull($refused);
        $this->assertStringContainsString('TxHandle', (string) $refused);
        $execs = self::execs($t);
        $this->assertCount(1, $execs, 'only the TxHandle statement was sent');
        $this->assertSame(43, $execs[0]['tx_id']);
    }

    public function testTheTxHandleRefusesAQueryForAnotherPool(): void
    {
        $t = new FakeTransport();
        [$conn] = self::connection($t, [], Manifest::fromJson(self::json(self::queries())));
        $t->feed(self::begin(1, 44));
        $t->feed(self::txDone(2, C::METHOD_TX_ROLLBACK));
        try {
            $conn->transaction(static function (\Ferro\Client\TxHandle $tx): void {
                $tx->execById('users.upsert'); // declared for `reports`
            }, \Ferro\Client\RetryPolicy::none());
            $this->fail('refused');
        } catch (ManifestException $e) {
            $this->assertStringContainsString('declared for pool `reports`', $e->getMessage());
        }
        $this->assertCount(0, self::execs($t));
    }

    /** F4/M22: the hash covers the pool exactly as written, case included. */
    public function testThePoolIsHashedVerbatim(): void
    {
        $m = Manifest::fromJson(self::json(['a' => ['sql' => 'SELECT 1', 'pool' => 'Reports', 'readonly' => true, 'idempotent' => false]]));
        $this->assertStringContainsString('"pool":"Reports"', Manifest::canonical(['a' => $m->query('a')]));
    }

    /** @return array<string, array{0: string, 1: string}> */
    public static function engineRefusals(): array
    {
        $ok = ['sql' => 'SELECT 1', 'pool' => 'default', 'readonly' => true, 'idempotent' => false];
        return [
            'id with a trailing newline' => [self::json(["abc\n" => $ok]), 'invalid query id'],
            'untrimmed sql' => [self::json(['a' => ['sql' => " SELECT 1"] + $ok]), 'trimmed'],
            'non-breaking-space sql' => [self::json(['a' => ['sql' => "SELECT 1\u{a0}"] + $ok]), 'trimmed'],
            'pool with a space' => [self::json(['a' => ['pool' => 'de fault'] + $ok]), 'invalid pool'],
            'non-string source' => [self::json(['a' => $ok + ['source' => 3]]), 'needs'],
        ];
    }

    /** F1: the client refuses what the engine's loader refuses. */
    #[DataProvider('engineRefusals')]
    public function testTheClientRefusesWhatTheEngineRefuses(string $json, string $message): void
    {
        $this->expectException(ManifestException::class);
        $this->expectExceptionMessage($message);
        Manifest::fromJson($json);
    }

    public function testANullRecordedHashIsAcceptedAsTheEngineAcceptsIt(): void
    {
        $doc = json_decode(self::json(self::queries()), true);
        $doc['hash'] = null;
        $this->assertSame(Manifest::fromJson(self::json(self::queries()))->hash(), Manifest::fromJson(json_encode($doc, JSON_THROW_ON_ERROR))->hash());
    }

    /** M3-D2b: a manifest `ferro check --write` produced loads, with the same hash; a malformed shape does not. */
    public function testACheckedManifestLoadsAndItsShapesAreNotHashed(): void
    {
        $q = self::queries();
        $checked = $q;
        $checked['users.find']['params'] = [null, 'int8'];
        $checked['users.find']['columns'] = [['name' => 'id', 'tag' => 2, 'type' => 'int8']];
        $this->assertSame(Manifest::fromJson(self::json($q))->hash(), Manifest::fromJson(self::json($checked))->hash());

        $bad = $q;
        $bad['users.find']['columns'] = [['name' => 'id', 'tag' => 2, 'typ' => 'int8']];
        $this->expectException(ManifestException::class);
        Manifest::fromJson(self::json($bad));
    }
}

