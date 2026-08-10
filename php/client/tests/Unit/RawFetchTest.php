<?php // /php/client/tests/Unit/RawFetchTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Backoff;
use Ferro\Client\Connection;
use Ferro\Client\ReconnectLoop;
use Ferro\Client\SessionInterface;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Protocol\PoolInfo;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 1 — `fetchRaw()` is the ONLY client entry point whose `readonly` fate flag is chosen
 * by the CALLER. Every other result-producing method hard-codes `readonly=true`
 * ({@see Connection::query}, {@see Connection::queryOne}, {@see Connection::scalar},
 * {@see Connection::rows}, {@see Connection::stream}), and the engine gates the §19.3 Indeterminate
 * split on that flag ALONE. A DBAL driver has no read/write signal to give
 * (`executeQuery('INSERT … RETURNING id')` reaches the same code path as a SELECT), so it must be
 * able to say "write" for everything — otherwise a lost `INSERT … RETURNING` is reported
 * `Retryable`, i.e. "provably did not apply", for a write whose fate is unknown.
 *
 * The `readonly` table below carries BOTH flag values so the assertion is a mirror property, not a
 * one-sided negative that cannot fail.
 */
final class RawFetchTest extends TestCase
{
    /**
     * Decode a recorded `ExecRequest` payload back to its field map. `ExecRequest` has no
     * `decode()`; the payload is unpacked first and then mapped. `PurePacker` is what
     * `PackerFactory::forEncode()` returns, and unlike `ExtPacker::unpack` it honours the offset.
     *
     * @return array<string,mixed>
     */
    private static function decodeExec(string $payload): array
    {
        $off = 0;
        return ExecRequest::mapFromWire((array) (new PurePacker())->unpack($payload, $off));
    }

    /** @return array<string, array{0: bool}> */
    public static function fates(): array
    {
        return ['declared write' => [false], 'declared read' => [true]];
    }

    #[DataProvider('fates')]
    public function testTheCallerChosenReadonlyFlagReachesTheWire(bool $readonly): void
    {
        $session = (new FakeSession())->push(
            FakeSession::execOk([
                'cols' => [['name' => 'id', 'tag' => C::TAG_I64]],
                'rows' => [[['tag' => C::TAG_I64, 'data' => 7]]],
                'affected' => 3,
                'last_insert_id' => null,
                'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
            ]),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $conn = new Connection($session, 'default');

        $conn->fetchRaw('INSERT INTO t (v) VALUES (1) RETURNING id', [], $readonly);

        $req = self::decodeExec($session->lastRequest()['payload']);
        self::assertSame($readonly, $req['readonly'], 'fetchRaw must send the caller-chosen fate flag verbatim');
    }

    /**
     * POSITIONAL rows, and `affected` SEPARATE from `count($rows)` — the two things a DBAL `Result`
     * needs and no public method provides. `query()`/`rows()` `array_combine` (which collapses
     * duplicate column names, breaking `fetchNumeric()`), and the research spike shipped
     * `rowCount() === 0` for an UPDATE that affected 1 row precisely because it used `count($rows)`.
     */
    public function testItReturnsPositionalRowsAndTheAffectedCountSeparately(): void
    {
        $session = (new FakeSession())->push(
            FakeSession::execOk([
                'cols' => [['name' => 'x', 'tag' => C::TAG_I64], ['name' => 'x', 'tag' => C::TAG_TEXT]],
                'rows' => [[['tag' => C::TAG_I64, 'data' => 1], ['tag' => C::TAG_TEXT, 'data' => 'a']]],
                'affected' => 9,
                'last_insert_id' => ['tag' => C::TAG_I64, 'data' => 42],
                'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
            ]),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $conn = new Connection($session, 'default');

        $raw = $conn->fetchRaw('SELECT 1 AS x, \'a\' AS x', [], true);

        self::assertSame(['x', 'x'], $raw['cols'], 'duplicate column names must survive, not collapse');
        self::assertSame([[1, 'a']], $raw['rows'], 'rows are POSITIONAL');
        self::assertSame(9, $raw['affected'], 'affected is the terminal field, not count($rows)');
        self::assertSame(42, $raw['last_insert_id']);
    }

    /** `wantRows: false` is `fetch=none` — a write that must not drag a result set back. */
    public function testWantRowsFalseSendsFetchNone(): void
    {
        $session = (new FakeSession())->thenExecOk(null);
        $conn = new Connection($session, 'default');

        $conn->fetchRaw('DELETE FROM t', [], false, false);

        $req = self::decodeExec($session->lastRequest()['payload']);
        self::assertSame(1, $req['fetch'], 'fetch:none is 1 (ExecCodec::FETCH_NONE)');
    }

    /** The mirror of the above: the default `wantRows: true` is `fetch=rows` (0), not "some mode". */
    public function testWantRowsTrueSendsFetchRows(): void
    {
        $session = (new FakeSession())->thenEmptyRows();
        $conn = new Connection($session, 'default');

        $conn->fetchRaw('SELECT v FROM t', [], true);

        $req = self::decodeExec($session->lastRequest()['payload']);
        self::assertSame(0, $req['fetch'], 'fetch:rows is 0 (ExecCodec::FETCH_ROWS)');
    }

    /**
     * `poolInfo()` resolves LIVE off `session()` every call. Caching it would be wrong: the
     * ReconnectLoop replaces the Session object, and a restarted engine can advertise a different
     * `server_version` — which is exactly the value the platform (i.e. the SQL dialect) is chosen
     * from.
     */
    public function testPoolInfoResolvesThisConnectionsPoolAndNothingElse(): void
    {
        $session = new FakeSession();
        $session->poolInfo = [
            new PoolInfo('default', 'postgres', 'PostgreSQL 17.10 (Debian)'),
            new PoolInfo('mysql', 'mysql', '8.4.11'),
        ];
        $conn = new Connection($session, 'mysql');

        $info = $conn->poolInfo();
        self::assertNotNull($info);
        self::assertSame('mysql', $info->name);
        self::assertSame('mysql', $info->kind);
        self::assertSame('8.4.11', $info->serverVersion);

        self::assertNull((new Connection($session, 'nope'))->poolInfo(), 'an unadvertised pool is null, never a guess');
    }

    /**
     * The NOT-CACHED half, made observable: a reconnect swaps the whole {@see SessionInterface}
     * object underneath the Connection, and the fresh handshake can advertise a DIFFERENT
     * `server_version` for the same pool name — a restarted engine, or a rolling backend upgrade.
     * The Doctrine tier turns that string into a PLATFORM, i.e. into which SQL dialect it emits, so
     * a value memoised on first call is a silently wrong dialect for the rest of the process.
     *
     * The plan predicted this hazard was unobservable offline; driving the real {@see ReconnectLoop}
     * (which is what {@see Connection::session} reads through) makes it observable without a socket.
     */
    public function testPoolInfoIsReReadAfterAReconnectSwapsTheSession(): void
    {
        $before = new FakeSession(epoch: 1);
        $before->poolInfo = [new PoolInfo('default', 'postgres', 'PostgreSQL 16.4 (Debian)')];
        $after = new FakeSession(epoch: 2);
        $after->poolInfo = [new PoolInfo('default', 'postgres', 'PostgreSQL 17.10 (Debian)')];

        $loop = new ReconnectLoop(
            $before,
            static fn (): SessionInterface => $after,
            new Backoff(sleep: static function (float $seconds): void {}),
        );
        $conn = new Connection($before, 'default', reconnect: $loop);

        $first = $conn->poolInfo();
        self::assertNotNull($first);
        self::assertSame('PostgreSQL 16.4 (Debian)', $first->serverVersion);

        self::assertTrue($loop->reconnect(), 'the fixture must model a RESTARTED engine (epoch 1 -> 2)');

        $second = $conn->poolInfo();
        self::assertNotNull($second);
        self::assertSame(
            'PostgreSQL 17.10 (Debian)',
            $second->serverVersion,
            'poolInfo() must re-read the LIVE session; a memoised copy is a silently wrong SQL dialect',
        );
    }
}
