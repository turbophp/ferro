<?php // /php/doctrine-dbal/tests/Unit/ConnectionStreamingTest.php
declare(strict_types=1);
namespace Ferro\DBAL\Tests\Unit;

use Ferro\Client\Connection as FerroClientConnection;
use Ferro\DBAL\Connection;
use Ferro\DBAL\PlatformVersion;
use Ferro\DBAL\Result;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 12, ADDED beyond the plan — the CONNECTION half of streaming, offline.
 *
 * The plan leaves `query()`'s streaming fork and the whole settle/abandon design to the live tier.
 * That is one vantage point for two properties that fail differently, and the live tier costs a
 * ferrod per test; these are the cheap guards that make the plan's live mutations redden here too.
 *
 * Two things are asserted that a `FakeSession` genuinely CAN witness:
 *
 * 1. **`query()` returns a streamed result whose columns come from the `HEAD` frame** (the fetch
 *    MODE per family is pinned next door, in `ConnectionFateFlagTest`).
 * 2. **Whether the next statement DRAINS the open result** — because this fixture models no DATA
 *    frames at all and says so loudly (`FakeSession::readStreamFrame()` throws
 *    `LogicException: FakeSession models no DATA frames`), an attempt to drain is directly visible.
 *    A result whose caller is gone must NOT be drained; a result the caller still holds MUST be.
 *    The two are the same code path taking opposite branches on a `\WeakReference`, and they are
 *    driven over every wire-touching method so that deleting `settleOpenStream()` from any ONE of
 *    them is caught. The real magnitudes — `settledRowCount()` 0 vs the full remainder — are
 *    measured live in `tests/Live/StreamingLiveTest.php`; this is the structural half.
 */
final class ConnectionStreamingTest extends TestCase
{
    /**
     * A driver connection on a POSTGRES pool whose session will answer `openStream()` with a HEAD
     * carrying two columns and nothing else.
     */
    private static function pgConn(FakeSession $session, bool $readonly = false): Connection
    {
        return new Connection(
            new FerroClientConnection($session, 'default'),
            'default',
            PlatformVersion::KIND_POSTGRES,
            $readonly,
        );
    }

    private static function head(): FakeSession
    {
        return (new FakeSession())->thenStreamHead([
            ['name' => 'id', 'tag' => C::TAG_I64],
            ['name' => 'note', 'tag' => C::TAG_TEXT],
        ]);
    }

    public function testQueryReturnsALazyStreamedResultCarryingTheHeadColumns(): void
    {
        $session = self::head();
        $result = self::pgConn($session)->query('SELECT id, note FROM t');

        self::assertInstanceOf(Result::class, $result);
        self::assertTrue($result->isStreaming(), 'query() on a PostgreSQL pool must stream');
        self::assertSame(2, $result->columnCount(), 'the HEAD frame is read during the open');
        self::assertSame('note', $result->getColumnName(1));
        self::assertSame(0, $result->rowCount(), 'a streamed terminal carries no affected count');
        self::assertSame(0, $session->abandonCount, 'and nothing is cancelled while it is live');
    }

    /**
     * A result the caller DROPPED cancels itself, and the next statement therefore has nothing to
     * drain — the `\WeakReference` design, at the only offline vantage point where the difference
     * shows.
     *
     * With a STRONG reference (plan v1's design, and this task's mutation #5) the connection would
     * still consider the result live and `settleOpenStream()` would try to pull DATA frames, which
     * this fixture refuses loudly. So the mutation turns this green test into an error with a
     * message naming the drain — while `testAResultTheCallerStillHoldsIsDrainedFirst` below is
     * the mirror that stops "never settle anything" from passing both.
     */
    #[DataProvider('wireOps')]
    public function testAnAbandonedStreamedResultIsNotDrainedByTheNextStatement(string $op): void
    {
        $session = self::scriptFor($op);
        $c = self::pgConn($session);
        self::openTransactionIfNeeded($c, $op);

        $c->query('SELECT id, note FROM t');   // discarded in statement position: destroyed here
        self::assertSame(1, $session->abandonCount, 'dropping the result must CANCEL the stream');

        self::runOp($c, $op);

        self::assertSame(0, $c->settledRowCount(), 'an abandoned stream costs no drained rows');
    }

    /**
     * The MIRROR: a result the caller can still fetch from is drained BEFORE the next statement
     * goes out, on every wire-touching method. The session is strictly single-in-flight, so the
     * alternative is a `ProtocolException` in the middle of the canonical batch idiom.
     *
     * The drain is observed through this fixture's loud refusal to produce DATA frames, which is
     * exactly what makes it observable at all offline: without `settleOpenStream()` the statement
     * would simply succeed against the scripted reply. `tests/Live/StreamingLiveTest.php` measures
     * the same property where the rows are real.
     */
    #[DataProvider('wireOps')]
    public function testAResultTheCallerStillHoldsIsDrainedFirst(string $op): void
    {
        $session = self::scriptFor($op);
        $c = self::pgConn($session);
        self::openTransactionIfNeeded($c, $op);

        $open = $c->query('SELECT id, note FROM t');
        self::assertInstanceOf(Result::class, $open);
        self::assertTrue($open->isStreaming());

        try {
            self::runOp($c, $op);
            self::fail("$op must settle the open stream before it reaches the wire");
        } catch (\LogicException $e) {
            self::assertStringContainsString(
                'FakeSession models no DATA frames',
                $e->getMessage(),
                'the drain must be what failed here — any other LogicException means the settle never ran',
            );
        }
    }

    /** @return array<string, array{0: string}> */
    public static function wireOps(): array
    {
        return [
            'exec' => ['exec'],
            'runPrepared' => ['runPrepared'],
            'beginTransaction' => ['beginTransaction'],
            'commit' => ['commit'],
            'rollBack' => ['rollBack'],
        ];
    }

    /**
     * A session scripted so the op SUCCEEDS if it ever reaches the wire — otherwise
     * {@see testAResultTheCallerStillHoldsIsDrainedFirst} would go red for the wrong reason (an
     * unscripted fixture, not a drain) and the mutation would look proven when it was not.
     *
     * `commit`/`rollBack` need an open transaction, so their scripts BEGIN first; the HEAD is queued
     * either way and `FakeSession::openStream()` prefers it over any queued terminal.
     */
    private static function scriptFor(string $op): FakeSession
    {
        $session = self::head();
        return match ($op) {
            'exec', 'runPrepared' => $session->thenExecOk(null),
            'beginTransaction' => $session->push(FakeSession::beginOk(1), [C::SERVICE_TX, C::METHOD_TX_BEGIN]),
            'commit' => $session
                ->push(FakeSession::beginOk(1), [C::SERVICE_TX, C::METHOD_TX_BEGIN])
                ->push(FakeSession::controlOk(), [C::SERVICE_TX, C::METHOD_TX_COMMIT]),
            'rollBack' => $session
                ->push(FakeSession::beginOk(1), [C::SERVICE_TX, C::METHOD_TX_BEGIN])
                ->push(FakeSession::controlOk(), [C::SERVICE_TX, C::METHOD_TX_ROLLBACK]),
        };
    }

    /**
     * `commit()`/`rollBack()` need an OPEN transaction to mean anything, and it has to be opened
     * BEFORE the stream — otherwise the `beginTransaction()` in the middle would be the call that
     * settles, and the row would prove nothing about the method it names.
     */
    private static function openTransactionIfNeeded(Connection $c, string $op): void
    {
        if ($op === 'commit' || $op === 'rollBack') {
            $c->beginTransaction();
        }
    }

    private static function runOp(Connection $c, string $op): void
    {
        match ($op) {
            'exec' => $c->exec('DELETE FROM t'),
            'runPrepared' => $c->runPrepared('UPDATE t SET v = ? WHERE id = ?', [1, 2]),
            'beginTransaction' => $c->beginTransaction(),
            'commit' => $c->commit(),
            'rollBack' => $c->rollBack(),
        };
    }
}
