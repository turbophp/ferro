<?php // /php/client/tests/Client/SessionRequestTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\ConnectionLostException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class SessionRequestTest extends TestCase
{
    /** (d) A well-formed terminal (END, echoed id) round-trips to an Ok Outcome. */
    public function testRequestTerminalRoundTripAssertsEndAndEchoedId(): void
    {
        $packer = PackerFactory::forEncode();
        $codec = new Codec();

        $transport = new FakeTransport();
        $session = new Session($transport, new RequestIdAllocator(0)); // first next() -> 1

        // Ok terminal for request_id 1 (the id the session will allocate), END set.
        $okBody = $packer->packNil(); // one complete opaque msgpack value
        $payload = Outcome::ok($okBody)->encode($packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, strlen($payload));
        $transport->feed($codec->encodeFrame($header, $payload));

        $outcome = $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'the-request-body');
        $this->assertTrue($outcome->isOk());

        // The request frame the session wrote: flags=0 (not END), echoed id 1.
        $written = Header::decode($transport->written);
        $this->assertSame(1, $written->requestId);
        $this->assertSame(0, $written->flags & C::FLAG_END, 'a request frame does not carry END');
        $this->assertSame(C::SERVICE_SQL, $written->service);
    }

    /** A terminal echoing the WRONG id is a protocol fault (not silently accepted). */
    public function testTerminalWithMismatchedIdThrowsProtocol(): void
    {
        $packer = PackerFactory::forEncode();
        $codec = new Codec();
        $transport = new FakeTransport();
        $session = new Session($transport, new RequestIdAllocator(0)); // will send id 1

        $payload = Outcome::ok($packer->packNil())->encode($packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 2, strlen($payload)); // id 2 != 1
        $transport->feed($codec->encodeFrame($header, $payload));

        $this->expectException(ProtocolException::class);
        $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'body');
    }

    /** A terminal WITHOUT the END flag is a protocol fault. */
    public function testTerminalWithoutEndThrowsProtocol(): void
    {
        $packer = PackerFactory::forEncode();
        $codec = new Codec();
        $transport = new FakeTransport();
        $session = new Session($transport, new RequestIdAllocator(0));

        $payload = Outcome::ok($packer->packNil())->encode($packer);
        $header = new Header(0, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, strlen($payload)); // flags=0, no END
        $transport->feed($codec->encodeFrame($header, $payload));

        $this->expectException(ProtocolException::class);
        $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'body');
    }

    /**
     * (e) A terminal on request_id=0 with END is SESSION-FATAL: surfaced as
     * ConnectionLostException carrying the decoded Outcome::Error — NOT a generic id-mismatch.
     */
    public function testRequestIdZeroEndTerminalIsSessionFatalNotIdMismatch(): void
    {
        $packer = PackerFactory::forEncode();
        $codec = new Codec();
        $transport = new FakeTransport();
        $session = new Session($transport, new RequestIdAllocator(0)); // sends id 1

        $ep = new ErrorPayload(
            C::ERR_CONNECTION_LOST,
            C::ERR_CONNECTION_LOST_BRANCH,
            null,
            null,
            'connection reset by peer',
            null,
            null,
        );
        $payload = Outcome::error($ep)->encode($packer);
        $header = new Header(C::FLAG_END, C::SERVICE_CORE, 0, 0, strlen($payload)); // rid=0
        $transport->feed($codec->encodeFrame($header, $payload));

        try {
            $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'body');
            $this->fail('expected ConnectionLostException');
        } catch (ConnectionLostException $e) {
            $this->assertNotNull($e->errorPayload(), 'the decoded Outcome::Error is carried');
            $this->assertSame(C::ERR_CONNECTION_LOST, $e->errorPayload()?->code);
            $this->assertSame(C::BRANCH_RETRYABLE, $e->errorPayload()?->branch);
        }
    }
}
