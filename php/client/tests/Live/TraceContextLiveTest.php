<?php // /php/client/tests/Live/TraceContextLiveTest.php
declare(strict_types=1);
namespace Ferro\Tests\Live;

use Ferro\Client\TraceContext;
use Ferro\Client\TxHandle;

/**
 * M2-C4c-1 against a real `ferrod`: whatever the trace provider does, the statement runs.
 *
 * The engine's half — that a valid header reaches the slow log and a malformed one is counted —
 * is proven in `ferrod`'s own `slow_log_it`/`metrics_it`. This proves the cross-language half:
 * the version-4, nine-field EXEC both codecs agree on, on all three statement paths, with a
 * valid, a malformed and a throwing provider. A malformed header must be dropped by the engine,
 * never refused, and a throwing provider must never reach the statement at all.
 */
final class TraceContextLiveTest extends LiveTestCase
{
    private const VALID = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';

    protected function tearDown(): void
    {
        TraceContext::useProvider(null);
        parent::tearDown();
    }

    /** @return iterable<string, array{0: \Closure(): mixed}> */
    public static function providers(): iterable
    {
        yield 'valid' => [static fn (): string => self::VALID];
        yield 'malformed (engine drops and counts it)' => [static fn (): string => 'not-a-traceparent'];
        yield 'uppercase (refused by the W3C grammar)' => [static fn (): string => strtoupper(self::VALID)];
        yield 'throwing (the client sends none)' => [static function (): string {
            throw new \RuntimeException('tracer down');
        }];
    }

    /** @param \Closure(): mixed $provider */
    #[\PHPUnit\Framework\Attributes\DataProvider('providers')]
    public function testEveryStatementPathRunsWhateverTheProviderDoes(\Closure $provider): void
    {
        TraceContext::useProvider($provider);
        $c = $this->connectConnection();

        // Autocommit, buffered.
        $this->assertSame([['n' => 1]], $c->query('SELECT 1 AS n'));

        // Autocommit, streamed.
        $streamed = [];
        foreach ($c->stream('SELECT g AS n FROM generate_series(1, 3) AS g') as $row) {
            $streamed[] = $row;
        }
        $this->assertCount(3, $streamed);

        // Tx-scoped.
        $got = $c->transaction(static fn (TxHandle $tx): array => $tx->query('SELECT 2 AS n'));
        $this->assertSame([['n' => 2]], $got);
    }
}
