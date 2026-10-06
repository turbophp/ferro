<?php // /php/client/tests/Live/HttpNotServedLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Session;
use Ferro\Ferro;
use Ferro\Http\Error\HttpException;
use Ferro\Protocol\Generated\Constants as C;

/**
 * M6-F8: an engine that does not serve Ferro HTTP (`FERRO_UPSTREAMS` unset) clears the `HTTP`
 * feature bit, and the client refuses BEFORE sending — the bit is the one signal, because a build
 * without the `http` feature has the same registry hash (§23.5).
 */
final class HttpNotServedLiveTest extends LiveTestCase
{
    public function testTheClientRefusesBeforeSendingWhenTheEngineDoesNotServeHttp(): void
    {
        $conn = Ferro::connect($this->socketPath);
        try {
            $session = $conn->session();
            $this->assertInstanceOf(Session::class, $session);
            $this->assertSame(0, $session->engineFeatures() & C::FEATURE_ENGINE_HTTP);
            $before = $session->lastInFlight();
            try {
                $conn->upstream('up')->request('GET', '/');
                $this->fail('expected the refusal');
            } catch (NonRetryableException $e) {
                $this->assertNotInstanceOf(HttpException::class, $e);
                $this->assertSame(C::ERR_UNSUPPORTED, $e->errorCode());
            }
            $this->assertSame($before, $session->lastInFlight(), 'no request frame was written');
            $this->assertSame(1, $conn->scalar('SELECT 1'));
        } finally {
            $conn->session()->close();
        }
    }
}
