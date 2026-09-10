<?php // /php/client/tests/Protocol/FrameSizeGuardTest.php
declare(strict_types=1);
namespace Ferro\Tests\Protocol;

use Ferro\Client\RequestIdAllocator;
use Ferro\Client\Session;
use Ferro\Protocol\Codec;
use Ferro\Protocol\CodecException;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Header;
use Ferro\Protocol\Msgpack\PackerFactory;
use Ferro\Protocol\Outcome;
use Ferro\Tests\Support\FakeTransport;
use PHPUnit\Framework\TestCase;

/**
 * B6a — the OUTBOUND frame-size guard.
 *
 * The client used to enforce `MAX_FRAME_PAYLOAD` on DECODE only, so it would put on the wire a
 * frame its own {@see Header::decode} would reject. `ferrod` treats an oversize `payload_len` as a
 * FATAL protocol fault and closes the session (it must — the framing is desynchronised, and a
 * payload it refused to read cannot be skipped; proven engine-side by
 * `session_rules::oversize_payload_len_is_fatal`), so what should have been a local refusal was a
 * dropped session. The Rust codec has always guarded its encoder; this is the missing mirror.
 */
final class FrameSizeGuardTest extends TestCase
{
    public function testEncodeFrameRefusesAPayloadOverTheFrameCap(): void
    {
        $over = str_repeat("\0", C::MAX_FRAME_PAYLOAD + 1);
        $header = new Header(0, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, strlen($over));

        try {
            (new Codec())->encodeFrame($header, $over);
            $this->fail('an oversize payload must be refused before it reaches the wire');
        } catch (CodecException $e) {
            // The message must name BOTH numbers: "too large" alone leaves the caller guessing how
            // much to shed, and this is the one error a large-object binder will actually hit.
            $this->assertStringContainsString((string) (C::MAX_FRAME_PAYLOAD + 1), $e->getMessage());
            $this->assertStringContainsString((string) C::MAX_FRAME_PAYLOAD, $e->getMessage());
        }
    }

    /**
     * The boundary is INCLUSIVE: a payload of exactly `MAX_FRAME_PAYLOAD` is legal and must still
     * encode. Without this the guard could be an off-by-one that silently costs a whole frame's
     * worth of capacity — and the S5 credit window is deliberately sized so that any single valid
     * frame fits the initial window (SPEC §5.2), so a cap that rejected its own maximum would break
     * the large-row rule rather than merely being conservative.
     */
    public function testAPayloadOfExactlyTheCapStillEncodes(): void
    {
        $exact = str_repeat("\0", C::MAX_FRAME_PAYLOAD);
        $header = new Header(0, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 1, strlen($exact));

        $frame = (new Codec())->encodeFrame($header, $exact);
        $this->assertSame(16 + C::MAX_FRAME_PAYLOAD, strlen($frame));
    }

    /**
     * THE POINT, and the reason this is a fix rather than a nicer error message: the refusal is
     * raised BEFORE any byte reaches the transport, so the session is untouched and still usable.
     * Today's behaviour is the opposite — the frame goes out and the engine closes the connection.
     *
     * Asserts both halves: nothing was written, and the very next ordinary request on the SAME
     * session completes normally.
     */
    public function testARefusedOversizeFrameLeavesTheSessionUsable(): void
    {
        $packer = PackerFactory::forEncode();
        $transport = new FakeTransport();
        $session = new Session($transport, new RequestIdAllocator(0));

        $over = str_repeat("\0", C::MAX_FRAME_PAYLOAD + 1);
        try {
            $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, $over);
            $this->fail('an oversize request body must be refused');
        } catch (CodecException) {
            // expected
        }

        $this->assertSame('', $transport->written,
            'the guard must fire before any byte reaches the transport — a partially written frame '
            . 'would desynchronise the wire exactly like the oversize frame it is preventing');
        $this->assertFalse($transport->closed, 'a refused frame must not close the session');

        // The session still works. NOTE the terminal is fed for id 2, not 1: the refused request
        // had already taken id 1 from the allocator (`sendRequest` allocates before it writes).
        // That gap is harmless and is asserted here rather than hidden — ids are monotonic and
        // never reused, and since NOTHING was written the engine never saw id 1, so there is no
        // in-flight state on either side to reconcile. `Session::$lastInFlight` is likewise set
        // before the write and so is left pointing at the refused call, which is also harmless: it
        // exists only to classify a LOSS (§19.3), no loss occurred, and the next `sendRequest`
        // overwrites it before writing anything.
        $payload = Outcome::ok($packer->packNil())->encode($packer);
        $header = new Header(C::FLAG_END, C::SERVICE_SQL, C::METHOD_SQL_EXEC, 2, strlen($payload));
        $transport->feed((new Codec())->encodeFrame($header, $payload));

        $outcome = $session->sendRequest(C::SERVICE_SQL, C::METHOD_SQL_EXEC, 'a normal body');
        $this->assertTrue($outcome->isOk(), 'the session must survive a locally-refused frame');

        $wrote = Header::decode($transport->written);
        $this->assertSame(2, $wrote->requestId,
            'the refused request consumed id 1; the next one is 2 — a gap, never a reuse');
    }
}
