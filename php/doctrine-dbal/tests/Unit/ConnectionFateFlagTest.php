<?php // /php/doctrine-dbal/tests/Unit/ConnectionFateFlagTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Ferro\Client\Connection as FerroClientConnection;
use Ferro\Client\ExecCodec;
use Ferro\DBAL\Connection;
use Ferro\DBAL\PlatformVersion;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 5, ADDED beyond the plan — the §19.3 fate declaration this driver sends, asserted at
 * the only vantage point where it is observable offline: the encoded `ExecRequest`.
 *
 * This is the driver's single most load-bearing decision. The DBAL 4 SPI carries no read/write
 * signal, so every statement is declared a WRITE unless the operator configured
 * `driverOptions.readonly`; the engine gates the §19.3 Indeterminate split on that flag ALONE and
 * infers nothing from the SQL. A driver that sent `readonly = true` would tell an application a
 * lost `INSERT … RETURNING id` **provably did not apply** — the safety inversion this project
 * exists to refuse — and NOTHING else in Task 5 would notice: the live smoke passes either way,
 * because the flag only changes how a LOST statement is classified.
 *
 * The table carries BOTH flag values, so this is a mirror property rather than a one-sided negative
 * (a hard-coded `false` fails the readonly row; a hard-coded `true` fails the write row).
 *
 * Task 11 pins the same decision behaviourally and live, including the 57014 cell where declaring
 * "write" turns a cancelled SELECT into an `IndeterminateWriteException`. This is the cheap offline
 * half, and it exists from the walking skeleton on so the flag cannot be lost in a later refactor.
 */
final class ConnectionFateFlagTest extends TestCase
{
    /**
     * Decode a recorded `ExecRequest` payload back to its field map — the established idiom
     * (`ConnectionImperativeTxTest`, `RawFetchTest`). `PurePacker`, never `ExtPacker`: the latter
     * consumes the whole buffer regardless of the offset it is handed.
     *
     * @return array<string,mixed>
     */
    private static function decodeExec(string $payload): array
    {
        $off = 0;
        return ExecRequest::mapFromWire(array_values((array) (new PurePacker())->unpack($payload, $off)));
    }

    private static function driverConn(
        FakeSession $session,
        bool $readonly,
        string $kind = PlatformVersion::KIND_POSTGRES,
    ): Connection {
        return new Connection(
            new FerroClientConnection($session, 'default'),
            'default',
            $kind,
            $readonly,
        );
    }

    /** @return array<string, array{0: bool}> */
    public static function fates(): array
    {
        return ['a write connection (the default)' => [false], 'driverOptions.readonly' => [true]];
    }

    /**
     * Amended twice, each time widening rather than weakening: M1-S9 B1b made the PREPARED path
     * stream on PostgreSQL (§22.2 (ah)), and **B2c made it stream on the MySQL family too**, now
     * that the engine supports it (§22.2 (n) closed). So `fetch:stream` is expected on EVERY
     * family — the provider still enumerates both, so a family that silently stops streaming is
     * still caught, and the fate declaration stays pinned beside it.
     */
    #[DataProvider('preparedFetchModes')]
    public function testTheConnectionLevelFateDeclarationReachesTheWireOnTheParameterisedPath(
        bool $readonly,
        string $kind,
        int $expectedFetch,
    ): void {
        // B2c: no family fork in the fixture either — BOTH families stream, so both are scripted
        // with a stream head. A regression that re-introduces buffering on MySQL fails loudly here
        // (the fake refuses an unscripted buffered exec) as well as on the `fetch` assertion.
        $session = (new FakeSession())->thenStreamHead([['name' => 'id', 'tag' => C::TAG_I64]]);
        $c = self::driverConn($session, $readonly, $kind);

        $c->prepare('INSERT INTO t (v) VALUES (?) RETURNING id')->execute();

        $req = self::decodeExec($session->lastRequest()['payload']);
        self::assertSame($readonly, $req['readonly'], 'the driver declares the fate; the engine never infers it');
        self::assertSame($expectedFetch, $req['fetch']);
        self::assertNotSame(ExecCodec::FETCH_NONE, $req['fetch'], 'the prepared path must be able to return rows');
    }

    /** @return array<string, array{0: bool, 1: string, 2: int}> */
    public static function preparedFetchModes(): array
    {
        return [
            'write conn, PG streams' => [false, PlatformVersion::KIND_POSTGRES, ExecCodec::FETCH_STREAM],
            'readonly conn, PG streams' => [true, PlatformVersion::KIND_POSTGRES, ExecCodec::FETCH_STREAM],
            'write conn, MySQL streams too (B2c)' => [false, PlatformVersion::KIND_MYSQL, ExecCodec::FETCH_STREAM],
            'readonly conn, MySQL streams too (B2c)' => [true, PlatformVersion::KIND_MYSQL, ExecCodec::FETCH_STREAM],
        ];
    }

    #[DataProvider('fates')]
    public function testTheSameDeclarationReachesTheWireOnTheParameterlessExecPath(bool $readonly): void
    {
        $session = (new FakeSession())->thenExecOk(null);
        $c = self::driverConn($session, $readonly);

        $c->exec('DELETE FROM t');

        $req = self::decodeExec($session->lastRequest()['payload']);
        self::assertSame($readonly, $req['readonly']);
        self::assertSame(1, $req['fetch'], 'exec() must not drag a result set back — fetch:none is 1');
    }

    /**
     * `query()` is the parameterless RESULT path and must never share `exec()`'s `fetch:none` —
     * the mirror of the `exec()` row above. Without it, `query()` and `exec()` could share one
     * `fetch` value and the pair would still look consistent.
     *
     * **Amended by Task 12 and again by B2c, not weakened.** `query()` streams, and since B2c it
     * streams on EVERY family — the MySQL fork (which existed only because
     * `supports_row_streaming()` was false there, §22.2 (n)) is gone. Both families are still
     * enumerated per fate, so a regression that reverts either one is caught, and the original
     * property survives in the `assertNotSame(FETCH_NONE)` row that closes the method.
     *
     * @param bool $readonly the connection-level fate declaration
     * @param string $kind the pool family, which decides the fetch mode
     * @param int $expectedFetch `ExecCodec::FETCH_*`
     */
    #[DataProvider('queryFetchModes')]
    public function testQueryIsAResultPathOnBothFamiliesAndCarriesTheSameDeclaration(
        bool $readonly,
        string $kind,
        int $expectedFetch,
    ): void {
        // B2c: see the sibling above — one scripted shape for every family.
        $session = (new FakeSession())->thenStreamHead([['name' => 'c', 'tag' => C::TAG_I64]]);
        $c = self::driverConn($session, $readonly, $kind);

        $c->query('SELECT 1');

        $req = self::decodeExec($session->lastRequest()['payload']);
        self::assertSame($readonly, $req['readonly'], 'the driver declares the fate on the streamed path too');
        self::assertSame($expectedFetch, $req['fetch']);
        self::assertNotSame(ExecCodec::FETCH_NONE, $req['fetch'], 'query() must be able to return rows');
    }

    /** @return array<string, array{0: bool, 1: string, 2: int}> */
    public static function queryFetchModes(): array
    {
        $rows = [];
        foreach (self::fates() as $fate => [$readonly]) {
            $rows["postgres streams, $fate"] = [$readonly, PlatformVersion::KIND_POSTGRES, ExecCodec::FETCH_STREAM];
            $rows["mysql streams too, $fate"] = [$readonly, PlatformVersion::KIND_MYSQL, ExecCodec::FETCH_STREAM];
        }
        return $rows;
    }

    /**
     * The affected count comes from the TERMINAL, never from `count($rows)` — they are different
     * numbers, and conflating them reports 0 for an `UPDATE` that changed rows (the exact bug the
     * research spike shipped). `exec()` is where DBAL's `executeStatement()` reads it.
     */
    public function testExecReturnsTheTerminalAffectedCountNotTheRowCount(): void
    {
        $session = (new FakeSession())->push(
            FakeSession::execOk([
                'cols' => [],
                'rows' => [],
                'affected' => 7,
                'last_insert_id' => null,
                'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
            ]),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $c = self::driverConn($session, false);

        self::assertSame(7, $c->exec('UPDATE t SET v = 1'));
    }
}
