<?php // /php/client/tests/Live/HttpChaosLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Ferro;
use Ferro\Http\Error\HttpException;
use Ferro\Http\Error\HttpIndeterminateException;
use Ferro\Http\Error\HttpRetryableException;
use Ferro\Http\Error\ResponseIncompleteException;
use Ferro\Http\FateClass;

/**
 * SPEC §23.14 chaos case 8: `SIGKILL ferrod` with requests in flight — §23.7.3, cell by cell, on ONE
 * multiplexed session, against a real daemon and a recording upstream, so at-most-once is a
 * read-back.
 *
 * | In flight when the link died      | client-idempotent | otherwise |
 * |-----------------------------------|-------------------|-----------|
 * | `REQUEST` not completely written  | Retryable         | Retryable (H) |
 * | written, no `HEAD`                | Retryable (B)     | Indeterminate (A; C — the OPERATOR's declaration is invisible before HEAD) |
 * | `HEAD` received                   | Retryable (F — operator, read off the HEAD; I — caller) | ResponseIncomplete (D 2xx, E 500, G streamed) |
 *
 * The connection is `Ferro::connect`'s, with its reconnect loop and the default retry policy — the
 * configuration under which the client WOULD re-issue a lost SQL read. Every request is asserted
 * received at most once, after a relaunched daemon has served the connection again.
 */
final class HttpChaosLiveTest extends HttpLiveTestCase
{
    private ?Connection $conn = null;

    protected function tearDown(): void
    {
        try {
            $this->conn?->session()->close();
        } catch (\Throwable) {
        }
        $this->conn = null;
        parent::tearDown();
    }

    public function testSigkillWithRequestsInFlightFollowsTheClientFateTableCellByCell(): void
    {
        $conn = $this->conn = Ferro::connect($this->socketPath);
        $up = $conn->upstream('up');
        $ops = $conn->upstream('ops');

        $f = [
            'A' => $up->requestAsync('POST', '/hold?c=A', body: 'a'),
            'B' => $up->requestAsync('GET', '/hold?c=B', idempotent: true),
            'C' => $ops->requestAsync('GET', '/hold?c=C'),
            'D' => $up->requestAsync('POST', '/head-then-hold?status=201&c=D', body: 'd'),
            'E' => $up->requestAsync('POST', '/head-then-hold?status=500&c=E', body: 'e'),
            'F' => $ops->requestAsync('GET', '/head-then-hold?c=F'),
            'I' => $up->requestAsync('GET', '/head-then-hold?c=I', idempotent: true),
        ];
        $g = $up->stream('POST', '/head-then-hold?c=G', body: 'g');
        $this->assertSame(200, $g->status);
        $this->eventually(fn (): bool => $this->received('/') === 8, 5.0, 'the upstream received all eight requests');
        usleep(300_000); // the heads travel upstream → engine → client socket
        // A barrier: its terminal follows every frame already sent, so D/E/F/I's heads and partial
        // bodies are read and filed before the link dies.
        $this->assertSame(1, $conn->scalar('SELECT 1'));

        // H: stop the engine reading, start writing a REQUEST larger than the socket can buffer, and
        // kill the engine during the write — the frame never completely leaves the client.
        $pid = $this->stopFerrodAndWait(); // stopped for real: no thread reads the REQUEST
        exec(sprintf('(sleep 1; kill -KILL %d) > /dev/null 2>&1 &', $pid));
        $f['H'] = $up->requestAsync('POST', '/echo?c=H', body: str_repeat('h', 15 * 1024 * 1024));
        $this->killFerrod(); // reap it (already dead, or killed now)

        $fate = [];
        foreach ($f as $k => $future) {
            try {
                $future->await();
                $fate[$k] = 'ok';
            } catch (HttpException $e) {
                $fate[$k] = $e;
            }
        }
        $chunks = [];
        try {
            foreach ($g as $chunk) {
                $chunks[] = $chunk;
            }
            $fate['G'] = 'ok';
        } catch (HttpException $e) {
            $fate['G'] = $e;
        }

        $expect = [
            'A' => [HttpIndeterminateException::class, FateClass::Indeterminate],
            'B' => [HttpRetryableException::class, FateClass::Retryable],
            'C' => [HttpIndeterminateException::class, FateClass::Indeterminate],
            'D' => [ResponseIncompleteException::class, FateClass::NonRetryable],
            'E' => [ResponseIncompleteException::class, FateClass::Indeterminate],
            'F' => [HttpRetryableException::class, FateClass::Retryable],
            'G' => [ResponseIncompleteException::class, FateClass::NonRetryable],
            'H' => [HttpRetryableException::class, FateClass::Retryable],
            'I' => [HttpRetryableException::class, FateClass::Retryable],
        ];
        ksort($fate);
        $this->assertSame(array_keys($expect), array_keys($fate));
        foreach ($expect as $k => [$class, $fateClass]) {
            $e = $fate[$k];
            $this->assertInstanceOf($class, $e, "cell {$k}");
            $this->assertInstanceOf(HttpException::class, $e);
            $this->assertTrue($e->clientSynthesised(), "cell {$k}: the client classified it");
            $this->assertSame(HttpException::CLIENT_LINK_LOST, $e->cause(), "cell {$k}");
            $this->assertSame($fateClass, $e->fate(), "cell {$k}");
        }
        $this->assertStringContainsString('not sent', $fate['H'] instanceof \Throwable ? $fate['H']->getMessage() : '');
        $this->assertInstanceOf(ResponseIncompleteException::class, $fate['D']);
        $this->assertTrue($fate['D']->wasApplied(), 'a 201 head: applied');
        $this->assertSame(201, $fate['D']->status());
        $this->assertInstanceOf(ResponseIncompleteException::class, $fate['E']);
        $this->assertNull($fate['E']->wasApplied());
        $this->assertSame(['partial'], $chunks, 'G: what arrived before the link died, then the fate');

        // The daemon comes back; the connection serves again (its closed session replaced before the
        // next request — not a retry), and nothing in flight was ever sent twice.
        $this->restartFerrod();
        $this->assertSame('{', $up->request('POST', '/echo?c=after', body: 'z')->body[0]);
        foreach (['A', 'B', 'C', 'D', 'E', 'F', 'G', 'I'] as $k) {
            $this->assertSame(1, $this->received("/hold?c={$k}") + $this->received("/head-then-hold?status=201&c={$k}")
                + $this->received("/head-then-hold?status=500&c={$k}") + $this->received("/head-then-hold?c={$k}"),
                "request {$k} reached the upstream exactly once");
        }
        $this->assertSame(0, $this->received('/echo?c=H'), 'H never reached the upstream');
        $this->assertSame(1, $this->received('/echo?c=after'));
    }
}
