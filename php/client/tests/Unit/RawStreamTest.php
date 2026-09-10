<?php // /php/client/tests/Unit/RawStreamTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\Connection;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\RawStream;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Msgpack\PurePacker;
use Ferro\Tests\Support\FakeSession;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * M1-S8b Task 2 — `streamRaw()` opens EAGERLY (the DBAL `Statement::execute()` contract runs the
 * statement) and hands back a handle whose columns are readable BEFORE the first row, because
 * `Doctrine\DBAL\Result::columnCount()`/`getColumnName()` are callable before any fetch.
 *
 * The eager open creates a hazard {@see Connection::stream} does not have: there, `openStream()`
 * runs INSIDE the generator, so a generator that is never started never opened anything. Here the
 * stream is already open, so a handle that is dropped without being iterated would leave it open
 * and desync the session. {@see RawStream::close} is what closes that hole, and the abandonment
 * guards below are the guards for it.
 *
 * What this class does NOT prove: that the CANCEL+drain actually leaves the wire usable.
 * {@see FakeSession} only counts the call. `tests/Live/RawStreamLiveTest.php` is where that half
 * lives, and both halves have their own mutation.
 */
final class RawStreamTest extends TestCase
{
    /**
     * Decode a recorded `ExecRequest` payload back to its field map. `ExecRequest` has no
     * `decode()`; the payload is unpacked first and then mapped. `PurePacker` is used explicitly
     * rather than `PackerFactory::forEncode()` because, unlike `ExtPacker::unpack`, it honours the
     * `$off` cursor — the same reason `RawFetchTest` does it this way.
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

    /**
     * The caller's fate flag reaches the wire on the STREAMED path too. Both values are driven, so
     * this is a mirror property rather than a one-sided negative: an implementation that hard-codes
     * either constant fails on one of the two rows.
     */
    #[DataProvider('fates')]
    public function testTheCallerChosenReadonlyFlagReachesTheStreamedRequest(bool $readonly): void
    {
        $session = (new FakeSession())->thenStreamEnd();
        $conn = new Connection($session, 'default');

        $stream = $conn->streamRaw('INSERT INTO t (v) VALUES (1) RETURNING id', [], $readonly);
        self::assertSame([], $stream->columns(), 'an immediate terminal advertises no columns');

        $req = self::decodeExec($session->lastRequest()['payload']);
        self::assertSame($readonly, $req['readonly'], 'streamRaw must send the caller-chosen fate flag verbatim');
        self::assertSame(2, $req['fetch'], 'fetch:stream is 2 (ExecCodec::FETCH_STREAM)');
    }

    /**
     * A stream handle is closable, closing it twice is harmless — and, the falsifiable half, the
     * second call does NOT hit the wire again. `assertTrue(true)` after two calls would pass for an
     * implementation with no idempotence at all; the abandon COUNT is what makes the `$closed` flag
     * observable.
     */
    public function testCloseIsIdempotentAndAbandonsExactlyOnce(): void
    {
        $session = (new FakeSession())->thenStreamHead([['name' => 'id', 'tag' => C::TAG_I64]]);
        $conn = new Connection($session, 'default');

        $stream = $conn->streamRaw('SELECT id FROM t', [], true);
        $stream->close();
        $stream->close();

        self::assertSame(1, $session->abandonCount, 'close() must abandon once, however often it is called');
        self::assertTrue($stream->isClosed());
    }

    /**
     * THE guard for the eager-open hazard: a handle that is closed WITHOUT ever being iterated must
     * still abandon the engine-side stream. `FakeSession` records every `abandonStream` call, so
     * this is behavioural, not a signature assertion.
     */
    public function testClosingAnUniteratedStreamStillAbandonsItOnTheWire(): void
    {
        $session = (new FakeSession())->thenStreamHead([['name' => 'id', 'tag' => C::TAG_I64]]);
        $conn = new Connection($session, 'default');

        $stream = $conn->streamRaw('SELECT id FROM t', [], true);
        self::assertSame(['id'], $stream->columns());
        self::assertSame(0, $session->abandonCount, 'nothing abandoned yet');

        $stream->close();
        self::assertSame(1, $session->abandonCount, 'a never-iterated stream must still be abandoned');
    }

    /**
     * The mirror of the guard above, and the reason {@see RawStream::close} can be called
     * unconditionally by a `Result::free()`: an immediate-terminal open never opened a stream, so
     * there is nothing to abandon and `close()` must not invent a wire operation. Without this the
     * abandonment guards would be satisfied by a `close()` that always calls `abandonStream()`,
     * including on a request id that was never a stream.
     */
    public function testClosingAnImmediateTerminalStreamTouchesNoWire(): void
    {
        $session = (new FakeSession())->thenStreamEnd();
        $conn = new Connection($session, 'default');

        $stream = $conn->streamRaw('SELECT 1', [], true);
        $stream->close();

        self::assertSame(0, $session->abandonCount, 'a stream that never opened has nothing to abandon');
    }

    /**
     * M1-S9 B1a: the terminal's `affected` reaches the handle — but only AFTER the drain. The wire
     * has ALWAYS carried it (the engine reads the command tag post-drain); what this pins is the
     * client-side capture that §22.2 (ac) wrongly recorded as a missing wire field. Mid-iteration
     * the answer is honestly "not yet" (`settled() === false`, `affected() === null`), never a
     * hardcoded 0 — the exact defect class the engine side's `StreamEnd` docs refuse. The
     * WINDOW_UPDATE count rides along: one per consumed DATA frame, so the settled capture cannot
     * have been bought by skipping the credit discipline.
     */
    public function testAffectedSettlesOnlyAfterTheDrain(): void
    {
        $session = (new FakeSession())
            ->thenStreamHead([['name' => 'id', 'tag' => C::TAG_I64]])
            ->thenStreamFrames([
                [
                    'type' => 'data',
                    'rows' => [
                        [['tag' => C::TAG_I64, 'data' => 7]],
                        [['tag' => C::TAG_I64, 'data' => 8]],
                    ],
                    'bytes' => 16,
                ],
                [
                    'type' => 'end',
                    'outcome' => FakeSession::execOk([
                        'cols' => [],
                        'rows' => [],
                        'affected' => 2,
                        'last_insert_id' => null,
                        'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 2, 'bytes' => 16],
                    ]),
                ],
            ]);
        $conn = new Connection($session, 'default');

        $stream = $conn->streamRaw('SELECT id FROM t', [], true);
        $rows = [];
        foreach ($stream->rows() as $row) {
            $rows[] = $row;
            self::assertFalse($stream->settled(), 'mid-iteration the terminal has not arrived');
            self::assertNull($stream->affected(), 'never a premature (or hardcoded-0) affected');
        }

        self::assertSame([[7], [8]], $rows);
        self::assertTrue($stream->settled(), 'the drain reached the Ok terminal');
        self::assertSame(2, $stream->affected(), 'the terminal ExecOk affected must surface');
        self::assertNull($stream->lastInsertId(), 'PG streams report no generated key');
        self::assertSame(1, $session->windowUpdateCount, 'one WINDOW_UPDATE per consumed DATA frame');
        self::assertSame(0, $session->abandonCount, 'a drained stream has nothing to abandon');
    }

    /**
     * The two never-settle shapes, pinned so `settled()` cannot rot into "true-ish once anything
     * happened": an ABANDONED stream (the command tag was never read — inventing an affected
     * would be the hardcoded-0 defect) and the defensive BODY-LESS Ok end-at-open (no ExecOk
     * payload means no information, not a decode fault — the fixture's `thenStreamEnd()` is
     * exactly that shape).
     */
    public function testAbandonedAndBodylessTerminalsNeverSettle(): void
    {
        $abandoned = (new FakeSession())->thenStreamHead([['name' => 'id', 'tag' => C::TAG_I64]]);
        $stream = (new Connection($abandoned, 'default'))->streamRaw('SELECT id FROM t', [], true);
        $stream->close();
        self::assertFalse($stream->settled());
        self::assertNull($stream->affected());

        $bodyless = (new FakeSession())->thenStreamEnd();
        $immediate = (new Connection($bodyless, 'default'))->streamRaw('SELECT 1', [], true);
        self::assertFalse($immediate->settled(), 'a body-less Ok carries no ExecOk to settle from');
        self::assertNull($immediate->affected());
    }

    /**
     * `rows()` after `close()` is a caller bug, not a silently empty result. The generator behind a
     * closed handle is pointed at a stream the engine has already been told to cancel, so yielding
     * from it would read frames belonging to somebody else's request.
     */
    public function testRowsAfterCloseIsRefusedLoudly(): void
    {
        $session = (new FakeSession())->thenStreamHead([['name' => 'id', 'tag' => C::TAG_I64]]);
        $conn = new Connection($session, 'default');

        $stream = $conn->streamRaw('SELECT id FROM t', [], true);
        self::assertFalse($stream->isClosed());
        $stream->close();

        $this->expectException(ProtocolException::class);
        $stream->rows();
    }

    /**
     * **A streamed statement's generated key reaches the CONNECTION** — the offline guard for the
     * B2c regression, which only a live MySQL caught the first time.
     *
     * `lastInsertId()` is read from the Connection, not from the stream, so it is not enough for the
     * key to land in the `StreamTerminal` cell. Until B2c the DBAL driver buffered on the MySQL
     * family and the key arrived through `dispatch()`; the moment the driver started streaming
     * there, every streamed INSERT reported no key and `lastInsertId()` threw `NoIdentityValue` —
     * which is what Doctrine ORM's IdentityGenerator calls on every MySQL insert. The live suite
     * caught it; nothing offline did. This is that missing test.
     *
     * Both halves of the contract are asserted: `null` while the statement is in flight (never a
     * stale carry-over), and the real key once the terminal settles.
     */
    public function testAStreamedStatementsGeneratedKeyReachesTheConnection(): void
    {
        $session = (new FakeSession())
            ->thenExecOk(4242)                       // a PRIOR buffered INSERT sets a key…
            ->thenStreamHead([['name' => 'id', 'tag' => C::TAG_I64]])
            ->thenStreamFrames([
                [
                    'type' => 'end',
                    'outcome' => FakeSession::execOk([
                        'cols' => [],
                        'rows' => [],
                        'affected' => 1,
                        'last_insert_id' => ['tag' => C::TAG_I64, 'data' => 99],
                        'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 0, 'bytes' => 0],
                    ]),
                ],
            ]);
        $conn = new Connection($session, 'default');

        $conn->fetchRaw('INSERT INTO t (v) VALUES (1)', [], false, false);
        self::assertSame(4242, $conn->lastInsertId(), 'the buffered path still reports its key');

        $stream = $conn->streamRaw('INSERT INTO t (v) VALUES (2) RETURNING id', [], false);
        self::assertNull(
            $conn->lastInsertId(),
            'in flight: the previous key is CLEARED, and the new one has not arrived — never stale',
        );

        foreach ($stream->rows() as $_) {
            // drain to the terminal
        }

        self::assertTrue($stream->settled(), 'the drain reached the Ok terminal');
        self::assertSame(99, $stream->lastInsertId(), 'the terminal cell carries the key');
        self::assertSame(
            99,
            $conn->lastInsertId(),
            'and it reaches the CONNECTION — this is what DBAL lastInsertId() reads, and what '
            . 'ORM IdentityGenerator needs on every MySQL insert',
        );
    }
}
