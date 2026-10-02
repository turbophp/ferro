<?php // /php/laravel/tests/Unit/LostConnectionRetryTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Unit;

use Ferro\Client\Connection as FerroClient;
use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\Error\TransportException;
use Ferro\Laravel\Exception\ConnectFailed;
use Ferro\Laravel\Exception\FerroQueryException;
use Ferro\Laravel\FerroPdoShim;
use Ferro\Laravel\FerroPostgresConnection;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeSession;
use Illuminate\Database\QueryException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * C1e-2: Illuminate's lost-connection RETRY and its concurrency RE-RUN, driven through the real
 * `Connection::run()` / `transaction()` with nothing faked above the client's session.
 *
 * `Connection::tryAgainIfCausedByLostConnection()` reconnects and RE-RUNS the statement when the
 * error looks like a lost connection; `DB::transaction(attempts:)` re-runs the WHOLE transaction
 * when it looks like a concurrency error. Both decide by message substring in stock Illuminate.
 * The tier's guards decide by TYPE whenever a Ferro failure is involved, and these tests pin:
 *
 * - the re-run lands on the RECONNECTED client, not the one that just failed;
 * - a dial failure (nothing sent) IS reconnected and retried, as after a PDO connect failure;
 * - a write the engine reported `Indeterminate` is never re-run, whatever its message says;
 * - the full type matrix of the lost-connection guard.
 */
final class LostConnectionRetryTest extends TestCase
{
    /** The engine's own text for a known-fate connection loss (`ferrod`'s `fate.rs`). */
    private const ENGINE_KNOWN_FATE_LOSS = 'connection lost with a known-fate outcome (statement not '
        . 'transmitted, a readonly read, or an in-tx statement whose transaction is now dead) — '
        . 'retryable; the engine never retries';

    private static function payload(int $code, int $branch, string $message): ErrorPayload
    {
        return new ErrorPayload($code, $branch, null, null, $message, null, null);
    }

    private static function error(int $code, int $branch, string $message): Outcome
    {
        return Outcome::error(self::payload($code, $branch, $message));
    }

    /**
     * A connection whose first client answers with `$first`, and whose reconnector — standing in for
     * `DatabaseManager`'s, which replaces the PDO and nothing else — moves it onto `$second`.
     */
    private static function connection(FakeSession $first, FakeSession $second): FerroPostgresConnection
    {
        $conn = new FerroPostgresConnection(static fn (): FerroClient => new FerroClient($first, 'main'), 'db', '', []);
        $conn->setReconnector(static function ($c) use ($second): void {
            $c->setPdo(new FerroPdoShim(new FerroClient($second, 'main')));
        });
        return $conn;
    }

    /**
     * The control: the engine's own known-fate loss IS retried — by TYPE, since its text matches
     * none of Illuminate's substrings — and the retry reaches the FRESH client. Before this slice
     * the statement paths held their own client, so the retry re-ran on the session that had just
     * failed.
     */
    public function testAKnownFateConnectionLossIsRetriedOnTheReconnectedClient(): void
    {
        $first = (new FakeSession())->push(
            self::error(C::ERR_CONNECTION_LOST, C::BRANCH_RETRYABLE, self::ENGINE_KNOWN_FATE_LOSS),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $second = (new FakeSession())->thenExecOk();
        $conn = self::connection($first, $second);

        self::assertTrue($conn->insert('insert into t (id) values (1)'));
        self::assertSame(1, $first->sendCount());
        self::assertSame(1, $second->sendCount(), 'the retry must reach the reconnected client');
    }

    /**
     * The guard: an `Indeterminate` write is NOT a lost connection to Illuminate, even carrying a
     * message that matches the stock substrings. Not re-sent anywhere; the caller gets the fate.
     */
    public function testAnIndeterminateWriteIsNeverResentWhateverItsMessageSays(): void
    {
        $first = (new FakeSession())->push(
            self::error(C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'Lost connection mid-write; it may have applied'),
            [C::SERVICE_SQL, C::METHOD_SQL_EXEC],
        );
        $second = new FakeSession(); // anything sent here would be the forbidden re-send
        $conn = self::connection($first, $second);

        try {
            $conn->insert('insert into t (id) values (1)');
            self::fail('an indeterminate write must surface, not succeed');
        } catch (QueryException $e) {
            self::assertInstanceOf(IndeterminateException::class, $e->getPrevious()?->getPrevious(),
                'the caller must receive the indeterminate fate itself');
        }
        self::assertSame(1, $first->sendCount());
        self::assertSame(0, $second->sendCount(),
            'an Indeterminate write was re-sent by Illuminate\'s lost-connection retry (§19.3)');
    }

    /**
     * A DIAL failure sent nothing, so it is reconnected and retried exactly as a PDO connect failure
     * is. Reachable at all only because the client is dialled lazily, inside `run()`: a client dialled
     * at construction failed outside `run()` as a raw `TransportException` that nothing retried.
     */
    public function testAConnectFailureIsReconnectedAndRetried(): void
    {
        $second = (new FakeSession())->thenExecOk();
        $conn = new FerroPostgresConnection(
            // `Connection refused` alone is NOT one of Illuminate's substrings (only the PDO-prefixed
            // `SQLSTATE[HY000] [2002] Connection refused` is), so this row is decided by TYPE.
            static fn (): FerroClient => throw new TransportException(
                'connect failed to unix:///run/ferro/app.sock: Connection refused (errno 111)',
            ),
            'db',
            '',
            [],
        );
        $conn->setReconnector(static function ($c) use ($second): void {
            $c->setPdo(new FerroPdoShim(new FerroClient($second, 'main')));
        });

        self::assertTrue($conn->insert('insert into t (id) values (1)'));
        self::assertSame(1, $second->sendCount());
    }

    /**
     * The client is dialled when the PDO is RESOLVED, once per resolution — never at construction.
     */
    public function testTheClientIsDialledOnFirstUseAndOncePerResolution(): void
    {
        $dials = 0;
        $session = (new FakeSession())->thenExecOk()->thenExecOk();
        $conn = new FerroPostgresConnection(static function () use (&$dials, $session): FerroClient {
            ++$dials;
            return new FerroClient($session, 'main');
        }, 'db', '', []);

        self::assertSame(0, $dials, 'constructing the connection must not dial');
        $conn->insert('insert into t (id) values (1)');
        $conn->insert('insert into t (id) values (2)');
        self::assertSame(1, $dials, 'one resolution, one client — memoised by getPdo()');
    }

    /**
     * The guard's whole type matrix. Each Ferro row's text is chosen AGAINST its expected answer —
     * a refused failure whose text matches Illuminate's substrings, an accepted one whose text
     * matches none — so a row can only pass if the TYPE decided it.
     *
     * @return iterable<string,array{\Throwable,bool}>
     */
    public static function chains(): iterable
    {
        $indeterminate = self::payload(C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'Lost connection');
        yield 'Indeterminate' => [FerroQueryException::fromFerro(new IndeterminateException($indeterminate)), false];
        yield 'raw ConnectionLost carrying an Indeterminate payload (cursor stream-open path)' =>
            [FerroQueryException::fromFerro(new ConnectionLostException('Lost connection', $indeterminate)), false];
        yield 'raw TransportException after the request was written (cursor stream-open path)' =>
            [FerroQueryException::fromFerro(new TransportException('read failed: Broken pipe')), false];
        yield 'NonRetryable forwarding a backend message verbatim' => [FerroQueryException::fromFerro(new NonRetryableException(
            self::payload(C::ERR_SYNTAX, 3, 'server closed the connection unexpectedly'),
        )), false];
        yield 'Retryable deadlock whose text matches' => [FerroQueryException::fromFerro(new RetryableException(
            self::payload(C::ERR_DEADLOCK, C::BRANCH_RETRYABLE, 'Lost connection'),
        )), true];
        yield 'Retryable deadlock whose text does not match' => [FerroQueryException::fromFerro(new RetryableException(
            self::payload(C::ERR_DEADLOCK, C::BRANCH_RETRYABLE, 'deadlock detected'),
        )), false];
        yield 'Retryable known-fate connection loss, engine text' => [FerroQueryException::fromFerro(new RetryableException(
            self::payload(C::ERR_CONNECTION_LOST, C::BRANCH_RETRYABLE, self::ENGINE_KNOWN_FATE_LOSS),
        )), true];
        yield 'dial failure, text matching nothing' => [ConnectFailed::from(new TransportException('connect failed: Connection refused (errno 111)')), true];
        yield 'non-Ferro PDOException — stock detector, unchanged' => [new \PDOException('server has gone away'), true];
        yield 'non-Ferro PDOException, no match — stock detector, unchanged' => [new \PDOException('syntax error'), false];
    }

    #[DataProvider('chains')]
    public function testTheLostConnectionGuardDecidesByType(\Throwable $e, bool $expected): void
    {
        $conn = self::connection(new FakeSession(), new FakeSession());
        $detect = fn (\Throwable $t): bool => (fn (): bool => $this->causedByLostConnection($t))->call($conn);
        self::assertSame($expected, $detect($e));
    }

    /**
     * `DB::transaction(attempts:)` re-runs the WHOLE transaction when the stock concurrency detector
     * matches — including after a COMMIT. A COMMIT whose fate is unknown must never be re-run,
     * whatever its message; a genuine Retryable deadlock still is (the control).
     */
    public function testTheConcurrencyGuardRefusesAnIndeterminateCommit(): void
    {
        $conn = self::connection(new FakeSession(), new FakeSession());
        $detect = fn (\Throwable $t): bool => (fn (): bool => $this->causedByConcurrencyError($t))->call($conn);

        $lostCommit = FerroQueryException::fromFerro(new IndeterminateException(self::payload(
            C::ERR_WRITE_UNCONFIRMED,
            C::BRANCH_INDETERMINATE,
            'COMMIT sent with no confirmed response: database is locked',
        )));
        self::assertFalse($detect($lostCommit), 'an Indeterminate COMMIT must never re-run the transaction');

        $deadlock = FerroQueryException::fromFerro(new RetryableException(new ErrorPayload(
            C::ERR_DEADLOCK, C::BRANCH_RETRYABLE, '40P01', null, 'deadlock detected', null, null,
        )));
        self::assertTrue($detect($deadlock), 'the control: a genuine deadlock is still retried');
    }
}
