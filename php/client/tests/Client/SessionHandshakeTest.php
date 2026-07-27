<?php // /php/client/tests/Client/SessionHandshakeTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\HandshakeException;
use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Protocol\Codec;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Message;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

final class SessionHandshakeTest extends TestCase
{
    /** (a) A rid=0 + END Outcome::Error(UNSUPPORTED) reply is the fatal registry-mismatch case. */
    public function testHandshakeRejectionThrowsHandshakeException(): void
    {
        $packer = PackerFactory::forEncode();
        $codec = new Codec();
        $ep = new ErrorPayload(
            C::ERR_UNSUPPORTED,
            C::ERR_UNSUPPORTED_BRANCH,
            null,
            null,
            'type_registry_hash mismatch: client sent "deadbeef", engine is "67b32e3e616e27f5"',
            null,
            null,
        );
        // The exact shape `error.rs terminal_frame(0, ep)` emits: service=CORE, method=0, rid=0, END.
        $payload = Outcome::error($ep)->encode($packer);
        $header = new Header(C::FLAG_END, C::SERVICE_CORE, 0, 0, strlen($payload));

        $transport = new FakeTransport();
        $transport->feed($codec->encodeFrame($header, $payload));
        $session = new Session($transport);

        try {
            $session->hello();
            $this->fail('expected HandshakeException on a registry-mismatch rejection');
        } catch (HandshakeException $e) {
            $this->assertTrue($e->isUnsupported(), 'keyed on ERR_UNSUPPORTED');
            $this->assertSame(C::ERR_UNSUPPORTED, $e->errorPayload()->code);
            $this->assertFalse($session->handshakeComplete());
        }
    }

    /**
     * (b) A HELLO_ACK whose boot_epoch is a u64 > PHP_INT_MAX decodes to a DECIMAL STRING and is
     * cached WITHOUT coercion — it stays a string and is `===` the original.
     */
    public function testBootEpochAboveIntMaxStaysAnUncoercedDecimalString(): void
    {
        $packer = PackerFactory::forEncode();
        $codec = new Codec();
        $bigEpoch = '18446744073709551615'; // u64::MAX, well above PHP_INT_MAX (9223372036854775807)

        $payload = Message::encode('hello_ack', [
            'engine_version' => 1,
            'boot_epoch' => $bigEpoch,
            'features' => 0,
            'pools' => ['default'],
            'type_registry_hash' => C::TYPE_REGISTRY_HASH,
        ], $packer);
        // request_id deliberately NOT the HELLO id (=1): the session must not assert they match.
        $header = new Header(0, C::SERVICE_CORE, C::METHOD_CORE_HELLO_ACK, 999, strlen($payload));

        $transport = new FakeTransport();
        $transport->feed($codec->encodeFrame($header, $payload));
        $session = new Session($transport, new RequestIdAllocator(0));

        $ack = $session->hello();

        $this->assertIsString($ack->bootEpoch, 'a > PHP_INT_MAX epoch must stay a string');
        $this->assertSame($bigEpoch, $ack->bootEpoch, 'value preserved exactly, not collapsed');
        $this->assertSame($bigEpoch, $session->bootEpoch(), 'cached opaquely on the session');
        $this->assertSame(['default'], $ack->pools);
        $this->assertTrue($session->handshakeComplete());
    }

    /** A small boot_epoch narrows to an int and is cached as an int (opaque means "as decoded"). */
    public function testSmallBootEpochStaysAnInt(): void
    {
        $packer = PackerFactory::forEncode();
        $codec = new Codec();
        $payload = Message::encode('hello_ack', [
            'engine_version' => 1,
            'boot_epoch' => 42,
            'features' => 0,
            'pools' => ['default', 'replica'],
            'type_registry_hash' => C::TYPE_REGISTRY_HASH,
        ], $packer);
        $header = new Header(0, C::SERVICE_CORE, C::METHOD_CORE_HELLO_ACK, 1, strlen($payload));

        $transport = new FakeTransport();
        $transport->feed($codec->encodeFrame($header, $payload));
        $session = new Session($transport, new RequestIdAllocator(0));

        $ack = $session->hello();
        $this->assertSame(42, $ack->bootEpoch);
        $this->assertSame(['default', 'replica'], $ack->pools);
    }
}
