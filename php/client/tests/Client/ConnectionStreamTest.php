<?php // /php/client/tests/Client/ConnectionStreamTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Connection;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\TransportException;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\ExecOk;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\StreamData;
use Ferro\Protocol\StreamHead;
use Ferro\Tests\Support\FakeTransport;
use Ferro\Tests\Support\PersonDto;
use PHPUnit\Framework\TestCase;

/**
 * `Connection::stream()` (M1-S5 Task 6) over an in-memory transport (no ferrod): rows are yielded
 * lazily as `STREAM/HEAD` + `STREAM/DATA*` + a terminal `END` arrive, a replenishing
 * `WINDOW_UPDATE` follows each consumed `DATA` frame, a mid-stream error terminal throws AFTER the
 * pre-error rows, and — the wire-desync regression this task exists to close — abandoning the
 * stream (`foreach ... break`) drains the socket via an outbound `CANCEL` so the NEXT request on the
 * same session reads its own reply cleanly. The buffered `query()`/`rows()` path is untouched
 * (asserted at the bottom).
 */
final class ConnectionStreamTest extends TestCase
{
    private PackerInterface $packer;
    private Codec $codec;

    protected function setUp(): void
    {
        $this->packer = PackerFactory::forEncode();
        $this->codec = new Codec();
    }

    /** A Connection whose session reads exactly the fed frames (first allocated request_id = 1). */
    private function connectionWith(FakeTransport $t): Connection
    {
        return new Connection(new Session($t, new RequestIdAllocator(0)), 'default');
    }

    private function feedHead(FakeTransport $t, int $rid, array $cols): void
    {
        $payload = StreamHead::encode(['cols' => $cols], $this->packer);
        $header = new Header(0, C::SERVICE_STREAM, C::METHOD_STREAM_HEAD, $rid, strlen($payload));
        $t->feed($this->codec->encodeFrame($header, $payload));
    }

    private function feedData(FakeTransport $t, int $rid, array $rows): void
    {
        $payload = StreamData::encode(['rows' => $rows], $this->packer);
        $header = new Header(C::FLAG_STREAM, C::SERVICE_STREAM, C::METHOD_STREAM_DATA, $rid, strlen($payload));
        $t->feed($this->codec->encodeFrame($header, $payload));
    }

    private function feedTerminalOk(FakeTransport $t, int $rid): void
    {
        $payload = Outcome::ok($this->packer->packNil())->encode($this->packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, strlen($payload));
        $t->feed($this->codec->encodeFrame($header, $payload));
    }

    private function feedTerminalError(FakeTransport $t, int $rid, ErrorPayload $ep): void
    {
        $payload = Outcome::error($ep)->encode($this->packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, strlen($payload));
        $t->feed($this->codec->encodeFrame($header, $payload));
    }

    /** Frame an `Ok` ExecOk terminal for the given request_id (the buffered path). */
    private function feedOk(FakeTransport $t, int $rid, array $execOk): void
    {
        $body = ExecOk::encode($execOk, $this->packer);
        $payload = Outcome::ok($body)->encode($this->packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, $rid, strlen($payload));
        $t->feed($this->codec->encodeFrame($header, $payload));
    }

    /** @return int how many CORE/WINDOW_UPDATE frames appear in the bytes the session wrote. */
    private function countWindowUpdates(string $written): int
    {
        $count = 0;
        $off = 0;
        while ($off < strlen($written)) {
            $h = Header::decode(substr($written, $off, 16));
            if ($h->service === C::SERVICE_CORE && $h->method === C::METHOD_CORE_WINDOW_UPDATE) {
                ++$count;
            }
            $off += 16 + $h->payloadLen;
        }
        return $count;
    }

    /** @return list<Header> every frame header the session wrote, in order. */
    private function writtenHeaders(string $written): array
    {
        $out = [];
        $off = 0;
        while ($off < strlen($written)) {
            $h = Header::decode(substr($written, $off, 16));
            $out[] = $h;
            $off += 16 + $h->payloadLen;
        }
        return $out;
    }

    // ---- happy path: lazy yield + WINDOW_UPDATE + a single terminal read ------------------------

    public function testStreamYieldsAllRowsAndSendsAWindowUpdatePerDataFrame(): void
    {
        $t = new FakeTransport();
        $this->feedHead($t, 1, [['name' => 'n', 'tag' => C::TAG_I64]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 1]]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 2]]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 3]]]);
        $this->feedTerminalOk($t, 1);

        $conn = $this->connectionWith($t);
        $rows = [];
        foreach ($conn->stream('SELECT n FROM t') as $row) {
            $rows[] = $row;
        }

        $this->assertSame([['n' => 1], ['n' => 2], ['n' => 3]], $rows);
        $this->assertSame(3, $this->countWindowUpdates($t->written), 'one WINDOW_UPDATE per consumed DATA frame');

        // Exactly one request frame (the EXEC) was sent with a non-END, non-CANCEL flag; the
        // terminal was consumed exactly once (no leftover bytes on the transport to misread).
        $written = $this->writtenHeaders($t->written);
        $this->assertSame(C::SERVICE_SQL, $written[0]->service);
        $this->assertSame(0, $written[0]->flags);
    }

    public function testStreamHydratesDtoLazily(): void
    {
        $t = new FakeTransport();
        $this->feedHead($t, 1, [
            ['name' => 'id', 'tag' => C::TAG_I64],
            ['name' => 'first_name', 'tag' => C::TAG_TEXT],
            ['name' => 'is_active', 'tag' => C::TAG_BOOL],
        ]);
        $this->feedData($t, 1, [[
            ['tag' => C::TAG_I64, 'data' => 7],
            ['tag' => C::TAG_TEXT, 'data' => 'Ada'],
            ['tag' => C::TAG_BOOL, 'data' => true],
        ]]);
        $this->feedTerminalOk($t, 1);

        $conn = $this->connectionWith($t);
        $people = [];
        foreach ($conn->stream('SELECT id, first_name, is_active FROM person', [], PersonDto::class) as $p) {
            $people[] = $p;
        }

        $this->assertCount(1, $people);
        $this->assertInstanceOf(PersonDto::class, $people[0]);
        $this->assertSame(7, $people[0]->id);
        $this->assertSame('Ada', $people[0]->firstName);
        $this->assertTrue($people[0]->isActive);
    }

    /**
     * The laziness proof: only HEAD + the FIRST DATA frame are fed. Pulling the first row succeeds
     * (proving the Generator does not pre-read past what it needs); asking for the NEXT row before
     * anything more is fed hits the exhausted FakeTransport — proving DATA2/DATA3/END were never
     * read ahead of time.
     */
    public function testStreamPullsFramesOnDemandNotAllUpfront(): void
    {
        $t = new FakeTransport();
        $this->feedHead($t, 1, [['name' => 'n', 'tag' => C::TAG_I64]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 1]]]);
        // Deliberately NOT feeding DATA2/DATA3/terminal yet.

        $gen = $this->connectionWith($t)->stream('SELECT n FROM t');
        $this->assertSame(['n' => 1], $gen->current(), 'the first row is available from HEAD + DATA1 alone');

        $this->expectException(TransportException::class);
        $gen->next(); // must try to read past DATA1 — the fake transport has nothing more queued
    }

    // ---- mid-stream error terminal -----------------------------------------------------------------

    public function testMidStreamErrorTerminalThrowsAfterThePreErrorRows(): void
    {
        $t = new FakeTransport();
        $this->feedHead($t, 1, [['name' => 'n', 'tag' => C::TAG_I64]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 1]]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 2]]]);
        $this->feedTerminalError($t, 1, new ErrorPayload(
            C::ERR_SYNTAX,
            C::ERR_SYNTAX_BRANCH,
            null,
            null,
            'syntax error mid-stream',
            null,
            null,
        ));

        $conn = $this->connectionWith($t);
        $rows = [];
        try {
            foreach ($conn->stream('SELECT n FROM t') as $row) {
                $rows[] = $row;
            }
            $this->fail('expected the mapped exception to be thrown');
        } catch (NonRetryableException $e) {
            $this->assertSame(C::ERR_SYNTAX, $e->errorPayload()->code);
        }

        $this->assertSame([['n' => 1], ['n' => 2]], $rows, 'the pre-error rows were already yielded');
    }

    // ---- abandonment: the wire-desync regression ----------------------------------------------------

    public function testAbandoningTheStreamCancelsAndDrainsSoTheNextRequestRoundTripsCleanly(): void
    {
        $t = new FakeTransport();
        $this->feedHead($t, 1, [['name' => 'n', 'tag' => C::TAG_I64]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 1]]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 2]]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 3]]]);
        $this->feedTerminalOk($t, 1);

        $conn = $this->connectionWith($t);

        $rows = [];
        foreach ($conn->stream('SELECT n FROM t') as $row) {
            $rows[] = $row;
            break; // abandon after 1 of 3 DATA frames
        }
        $this->assertSame([['n' => 1]], $rows, 'only the row read before the break was yielded');

        // The Generator's destruction (no other reference survives the loop) must have run its
        // `finally`: an outbound CANCEL for request_id 1, then a drain to the terminal.
        $headers = $this->writtenHeaders($t->written);
        $cancelFrames = array_values(array_filter($headers, static fn (Header $h): bool => ($h->flags & C::FLAG_CANCEL) !== 0));
        $this->assertCount(1, $cancelFrames, 'exactly one CANCEL frame was written');
        $this->assertSame(1, $cancelFrames[0]->requestId);

        // THE regression this task exists to close: a subsequent request on the SAME session must
        // read its OWN reply — not a stale DATA/END frame left over from the abandoned stream.
        $this->feedOk($t, 2, [
            'cols' => [['name' => 'm', 'tag' => C::TAG_I64]],
            'rows' => [[['tag' => C::TAG_I64, 'data' => 99]]],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ]);
        $this->assertSame([['m' => 99]], $conn->query('SELECT 99 AS m'));
    }

    public function testSendRequestWhileAStreamIsOpenThrowsProtocolException(): void
    {
        $t = new FakeTransport();
        $this->feedHead($t, 1, [['name' => 'n', 'tag' => C::TAG_I64]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 1]]]);
        $this->feedTerminalOk($t, 1);

        $conn = $this->connectionWith($t);
        $gen = $conn->stream('SELECT n FROM t');
        $gen->current(); // opens the stream + reads the first row, WITHOUT reaching the terminal

        $this->expectException(ProtocolException::class);
        $conn->query('SELECT 1'); // a buffered request while the stream is still open
    }

    // ---- the buffered path is unchanged -------------------------------------------------------------

    public function testBufferedQueryStillWorksUnchanged(): void
    {
        $t = new FakeTransport();
        $this->feedOk($t, 1, [
            'cols' => [['name' => 'n', 'tag' => C::TAG_I64]],
            'rows' => [[['tag' => C::TAG_I64, 'data' => 1]]],
            'affected' => 0,
            'last_insert_id' => null,
            'stats' => ['queue_us' => 0, 'exec_us' => 0, 'rows' => 1, 'bytes' => 0],
        ]);
        $this->assertSame([['n' => 1]], $this->connectionWith($t)->query('SELECT 1 AS n'));
    }
}
