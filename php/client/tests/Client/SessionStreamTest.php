<?php // /php/client/tests/Client/SessionStreamTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\ProtocolException;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Msgpack\PackerInterface;
use Ferro\Protocol\Outcome;
use Ferro\Protocol\StreamData;
use Ferro\Protocol\StreamHead;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * The low-level {@see Session} streamed-read primitives (M1-S5 Task 6): `openStream` +
 * `readStreamFrame` walk HEAD -> DATA* -> terminal END; `sendCancel`/`sendWindowUpdate` are the new
 * outbound frames; the "stream open" guard refuses a `sendRequest`/second `openStream` while a
 * stream is in flight. {@see ConnectionStreamTest} covers the hydrated `Connection::stream()` surface
 * and the abandonment wire-desync regression end to end.
 */
final class SessionStreamTest extends TestCase
{
    private PackerInterface $packer;
    private Codec $codec;

    protected function setUp(): void
    {
        $this->packer = PackerFactory::forEncode();
        $this->codec = new Codec();
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

    /** @return list<Header> every frame header written, in order (payloads skipped). */
    private function writtenHeaders(FakeTransport $t): array
    {
        $bytes = $t->written;
        $out = [];
        $off = 0;
        while ($off < strlen($bytes)) {
            $h = Header::decode(substr($bytes, $off, 16));
            $out[] = $h;
            $off += 16 + $h->payloadLen;
        }
        return $out;
    }

    public function testOpenStreamReadsHeadAndReadStreamFrameWalksDataThenTerminal(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0)); // first id -> 1

        $this->feedHead($t, 1, [['name' => 'n', 'tag' => C::TAG_I64]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 1]]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 2]]]);
        $this->feedTerminalOk($t, 1);

        $opened = $session->openStream(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'body');
        $this->assertSame('head', $opened['type']);
        $this->assertSame(1, $opened['requestId']);
        $this->assertSame([['name' => 'n', 'tag' => C::TAG_I64]], $opened['cols']);

        $frame1 = $session->readStreamFrame(1);
        $this->assertSame('data', $frame1['type']);
        $this->assertSame([[['tag' => C::TAG_I64, 'data' => 1]]], $frame1['rows']);

        $frame2 = $session->readStreamFrame(1);
        $this->assertSame('data', $frame2['type']);
        $this->assertSame([[['tag' => C::TAG_I64, 'data' => 2]]], $frame2['rows']);

        $frame3 = $session->readStreamFrame(1);
        $this->assertSame('end', $frame3['type']);
        $this->assertTrue($frame3['outcome']->isOk());

        // The request frame the session wrote: flags=0 (not END), echoed id 1, SQL/EXEC.
        $written = Header::decode($t->written);
        $this->assertSame(1, $written->requestId);
        $this->assertSame(0, $written->flags);
        $this->assertSame(C::SERVICE_SQL, $written->service);
    }

    public function testSendCancelWritesEmptyCancelFrameTargetingRequestId(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));

        $session->sendCancel(42);

        $h = Header::decode($t->written);
        $this->assertSame(C::FLAG_CANCEL, $h->flags);
        $this->assertSame(C::SERVICE_CORE, $h->service);
        $this->assertSame(0, $h->method);
        $this->assertSame(42, $h->requestId);
        $this->assertSame(0, $h->payloadLen, 'CANCEL carries an empty payload');
    }

    /** The wire byte-lock: sendWindowUpdate's frame must match proto/vectors/window_update.json. */
    public function testSendWindowUpdateByteMatchesGoldenVector(): void
    {
        $vectorPath = __DIR__ . '/../../../../proto/vectors/window_update.json';
        $vector = json_decode((string) file_get_contents($vectorPath), true, 512, JSON_THROW_ON_ERROR);
        $this->assertIsArray($vector);

        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(4)); // next() -> 5, matches the vector's request_id

        $session->sendWindowUpdate(5, 64, 4194304);

        $this->assertSame((string) $vector['frame_hex'], bin2hex($t->written));
    }

    public function testSendRequestWhileStreamOpenThrowsProtocolException(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $this->feedHead($t, 1, [['name' => 'n', 'tag' => C::TAG_I64]]);

        $session->openStream(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'body');

        $this->expectException(ProtocolException::class);
        $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'other body');
    }

    public function testSecondOpenStreamWhileOneIsOpenThrowsProtocolException(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));
        $this->feedHead($t, 1, [['name' => 'n', 'tag' => C::TAG_I64]]);

        $session->openStream(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'body');

        $this->expectException(ProtocolException::class);
        $session->openStream(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'body2');
    }

    /** A checkout failure before any HEAD ever went out: openStream returns the 'end' shape directly. */
    public function testOpenStreamImmediateTerminalErrorNeverOpensGuard(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));

        $ep = new ErrorPayload(C::ERR_POOL_TIMEOUT, C::ERR_POOL_TIMEOUT_BRANCH, null, null, 'pool exhausted', null, null);
        $payload = Outcome::error($ep)->encode($this->packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, strlen($payload));
        $t->feed($this->codec->encodeFrame($header, $payload));

        $opened = $session->openStream(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'body');
        $this->assertSame('end', $opened['type']);
        $this->assertTrue($opened['outcome']->isError());

        // No guard was ever set — a subsequent sendRequest works immediately.
        $this->feedTerminalOk($t, 2);
        $outcome = $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'next body');
        $this->assertTrue($outcome->isOk());
    }

    public function testAbandonStreamIsANoOpWhenNoStreamIsOpen(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));

        $session->abandonStream(999); // never opened

        $this->assertSame('', $t->written);
    }

    public function testAbandonStreamSendsCancelThenDrainsToTerminal(): void
    {
        $t = new FakeTransport();
        $session = new Session($t, new RequestIdAllocator(0));

        $this->feedHead($t, 1, [['name' => 'n', 'tag' => C::TAG_I64]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 1]]]);
        $this->feedData($t, 1, [[['tag' => C::TAG_I64, 'data' => 2]]]);
        $this->feedTerminalOk($t, 1);

        $opened = $session->openStream(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'body');
        $this->assertSame('head', $opened['type']);

        $session->abandonStream(1);

        $headers = $this->writtenHeaders($t);
        // [0] the EXEC request, [1] the outbound CANCEL.
        $this->assertCount(2, $headers);
        $this->assertSame(C::FLAG_CANCEL, $headers[1]->flags);
        $this->assertSame(1, $headers[1]->requestId);

        // The drain consumed both DATA frames + the terminal — sendRequest works cleanly next.
        $this->feedTerminalOk($t, 2);
        $outcome = $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'next body');
        $this->assertTrue($outcome->isOk());
    }
}
