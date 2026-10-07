<?php // /php/guzzle/tests/Live/GuzzleChaosLiveTest.php
declare(strict_types=1);
namespace Ferro\Guzzle\Tests\Live;

use Ferro\Client\Connection;
use Ferro\Ferro;
use Ferro\Guzzle\FerroHandler;
use Ferro\Guzzle\IndeterminateRequestException;
use Ferro\Guzzle\NonRetryableRequestException;
use Ferro\Guzzle\Retry;
use Ferro\Guzzle\RetryableConnectException;
use Ferro\Http\Error\HttpException;
use Ferro\Http\Error\ResponseIncompleteException;
use Ferro\Http\Exception\NonRetryableBodyReadException;
use Ferro\Http\Fate;
use Ferro\Http\FateClass;
use Ferro\Tests\Live\HttpLiveTestCase;
use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use Psr\Http\Message\RequestInterface;

/**
 * SPEC §23.7.3 cell by cell THROUGH GUZZLE (§23.14 chaos case 8's shape): requests in flight on one
 * session of a `Ferro::connect` connection, behind `Middleware::retry` with Ferro's decider, then
 * `SIGKILL ferrod`. Each cell rejects with curl's class for the event and the fate's marker; the
 * decider re-sends only the Retryable ones (to a dead engine, so they never arrive); and after a
 * relaunch the upstream's log shows every request received EXACTLY once, and the unsent one never.
 *
 * | In flight when the link died      | client-idempotent           | otherwise |
 * |-----------------------------------|-----------------------------|-----------|
 * | `REQUEST` not completely written  | Retryable                   | Retryable (H) |
 * | written, no `HEAD`                | Retryable (B)               | Indeterminate (A) |
 * | `HEAD` received                   | Retryable (F, operator)     | ResponseIncomplete (D 201, E 500, G streamed) |
 */
final class GuzzleChaosLiveTest extends HttpLiveTestCase
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

    private function client(Connection $conn, string $upstream, array &$attempts): Client
    {
        $stack = HandlerStack::create(new FerroHandler($conn, ['http://127.0.0.1:' . $this->upstreamPort => $upstream]));
        $stack->push(Retry::middleware(2, 0, 0));
        $stack->push(Middleware::tap(static function (RequestInterface $r) use (&$attempts): void {
            $q = $r->getUri()->getQuery();
            $attempts[$q] = ($attempts[$q] ?? 0) + 1;
        }));
        return new Client(['handler' => $stack, 'base_uri' => 'http://127.0.0.1:' . $this->upstreamPort]);
    }

    public function testSigkillWithRequestsInFlightFollowsTheClientFateTableThroughGuzzle(): void
    {
        $conn = $this->conn = Ferro::connect($this->socketPath);
        $attempts = [];
        $up = $this->client($conn, 'up', $attempts);
        $ops = $this->client($conn, 'ops', $attempts);

        $p = [
            'A' => $up->postAsync('/hold?c=A', ['body' => 'a']),
            'B' => $up->getAsync('/hold?c=B', ['ferro' => ['idempotent' => true]]),
            'D' => $up->postAsync('/head-then-hold?status=201&c=D', ['body' => 'd']),
            'E' => $up->postAsync('/head-then-hold?status=500&c=E', ['body' => 'e', 'http_errors' => false]),
            'F' => $ops->getAsync('/head-then-hold?c=F'),
        ];
        $g = $up->post('/head-then-hold?c=G', ['body' => 'g', 'stream' => true])->getBody();
        $this->assertSame('partial', $g->read(7));
        $this->eventually(fn (): bool => $this->received('/') === 6, 5.0, 'the upstream received all six requests');
        usleep(300_000);
        $this->assertSame(1, $conn->scalar('SELECT 1')); // a barrier: the heads already sent are filed

        // H: a REQUEST that never completely leaves the client.
        $pid = $this->stopFerrodAndWait();
        exec(sprintf('(sleep 1; kill -KILL %d) > /dev/null 2>&1 &', $pid));
        $p['H'] = $up->postAsync('/echo?c=H', ['body' => str_repeat('h', 15 * 1024 * 1024)]);
        $this->killFerrod();

        $fate = [];
        foreach ($p as $k => $promise) {
            try {
                $promise->wait();
                $fate[$k] = 'fulfilled';
            } catch (\Throwable $e) {
                $fate[$k] = $e;
            }
        }
        try {
            $g->getContents();
            $fate['G'] = 'read to the end';
        } catch (\Throwable $e) {
            $fate['G'] = $e;
        }

        $expect = [
            'A' => [IndeterminateRequestException::class, FateClass::Indeterminate],
            'B' => [RetryableConnectException::class, FateClass::Retryable],
            'D' => [NonRetryableRequestException::class, FateClass::NonRetryable],
            'E' => [IndeterminateRequestException::class, FateClass::Indeterminate],
            'F' => [RetryableConnectException::class, FateClass::Retryable],
            'G' => [NonRetryableBodyReadException::class, FateClass::NonRetryable],
            'H' => [RetryableConnectException::class, FateClass::Retryable],
        ];
        ksort($fate);
        $this->assertSame(array_keys($expect), array_keys($fate));
        foreach ($expect as $k => [$class, $fateClass]) {
            $e = $fate[$k];
            $this->assertInstanceOf($class, $e, "cell {$k}");
            $read = Fate::of($e);
            $this->assertNotNull($read, "cell {$k}");
            $this->assertSame($fateClass, $read->fate, "cell {$k}");
            $this->assertSame(HttpException::CLIENT_LINK_LOST, $read->cause, "cell {$k}: the client classified it");
        }
        $this->assertInstanceOf(\GuzzleHttp\Exception\RequestException::class, $fate['D']);
        $this->assertSame(201, $fate['D']->getResponse()?->getStatusCode(), 'D carries the head that arrived');
        $this->assertInstanceOf(ResponseIncompleteException::class, $fate['D']->getPrevious());
        $this->assertTrue($fate['D']->getPrevious()->wasApplied());
        $this->assertInstanceOf(\GuzzleHttp\Exception\RequestException::class, $fate['E']);
        $this->assertSame(500, $fate['E']->getResponse()?->getStatusCode());

        // The decider re-sent only the Retryable cells (to a dead engine); never A, D, E, G.
        $this->assertSame(1, $attempts['c=A'] ?? null, json_encode($attempts) ?: '');
        $this->assertSame(1, $attempts['status=201&c=D']);
        $this->assertSame(1, $attempts['status=500&c=E']);
        $this->assertSame(3, $attempts['c=B'], 'Retryable: one attempt and two retries');
        $this->assertSame(3, $attempts['c=F']);
        $this->assertSame(3, $attempts['c=H']);

        $this->restartFerrod();
        $this->assertSame(200, $up->post('/echo?c=after', ['body' => 'z'])->getStatusCode());
        foreach (['/hold?c=A', '/hold?c=B', '/head-then-hold?status=201&c=D', '/head-then-hold?status=500&c=E', '/head-then-hold?c=F', '/head-then-hold?c=G'] as $target) {
            $this->assertSame(1, $this->received($target), "{$target} reached the upstream exactly once");
        }
        $this->assertSame(0, $this->received('/echo?c=H'), 'H never reached the upstream');
        $this->assertSame(1, $this->received('/echo?c=after'));
    }
}
