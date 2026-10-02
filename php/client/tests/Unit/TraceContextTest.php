<?php // /php/client/tests/Unit/TraceContextTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\ExecCodec;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\TraceContext;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Msgpack\PurePacker;
use PHPUnit\Framework\TestCase;

/**
 * M2-C4c-1: the caller's W3C trace context rides every EXEC, read from a process-wide provider when
 * the EXEC is encoded — and tracing never fails a statement.
 *
 * Asserted on the ENCODED request, through `ExecCodec::encode()` — the one encoder the autocommit,
 * tx-scoped and streamed paths share — rather than on `TraceContext::current()` alone, which would
 * pass with the field never wired.
 */
final class TraceContextTest extends TestCase
{
    private const TP = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';

    protected function tearDown(): void
    {
        TraceContext::useProvider(null);
    }

    private function sent(?int $txId = null): ?string
    {
        $codec = new ExecCodec(
            new M1ValuePolicy(new TypePolicyOptions()),
            new PlanCache(),
            new PurePacker(),
            new PurePacker(),
        );
        $payload = $codec->encode('main', 'select 1', [], true, ExecCodec::FETCH_ROWS, $txId);
        $off = 0;
        $wire = (new PurePacker())->unpack($payload, $off);
        self::assertIsArray($wire);
        $tp = ExecRequest::mapFromWire($wire)['traceparent'];
        self::assertTrue($tp === null || is_string($tp));
        return $tp;
    }

    public function testNoProviderSendsNoContext(): void
    {
        self::assertNull($this->sent());
    }

    public function testTheProvidersHeaderIsSentOnAutocommitAndTxScopedExecs(): void
    {
        TraceContext::useProvider(static fn (): string => self::TP);
        self::assertSame(self::TP, $this->sent());
        self::assertSame(self::TP, $this->sent(7), 'a tx-scoped EXEC carries it too');
    }

    /**
     * Read per EXEC, never cached: two statements in two different spans must carry two different
     * contexts. A client that read the provider once (at connect, say) would pin every statement to
     * the first request's trace.
     */
    public function testTheProviderIsCalledForEachExec(): void
    {
        $n = 0;
        TraceContext::useProvider(static function () use (&$n): string {
            $n++;
            return sprintf('00-%032x-%016x-01', $n, $n);
        });
        $a = $this->sent();
        $b = $this->sent();
        self::assertSame(2, $n);
        self::assertNotSame($a, $b);
        self::assertSame(sprintf('00-%032x-%016x-01', 2, 2), $b);
    }

    /** Tracing never fails a statement: a provider that throws sends nothing, and encode succeeds. */
    public function testAThrowingProviderSendsNoContextAndDoesNotThrow(): void
    {
        TraceContext::useProvider(static function (): string {
            throw new \RuntimeException('the tracer is broken');
        });
        self::assertNull($this->sent());

        TraceContext::useProvider(static function (): string {
            throw new \Error('a fatal-class error in the tracer');
        });
        self::assertNull($this->sent());
    }

    /** @return iterable<string, array{0: mixed}> */
    public static function unusableValues(): iterable
    {
        yield 'null' => [null];
        yield 'empty' => [''];
        yield 'int' => [42];
        yield 'array' => [['traceparent' => self::TP]];
        yield 'over the cap' => [str_repeat('a', TraceContext::MAX_LENGTH + 1)];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('unusableValues')]
    public function testAnUnusableValueSendsNoContext(mixed $value): void
    {
        TraceContext::useProvider(static fn (): mixed => $value);
        self::assertNull($this->sent());
    }

    /** At the cap is sent: the client bounds the length and leaves the GRAMMAR to the engine. */
    public function testAValueAtTheCapIsSentUnvalidated(): void
    {
        $atCap = str_repeat('a', TraceContext::MAX_LENGTH);
        TraceContext::useProvider(static fn (): string => $atCap);
        self::assertSame($atCap, $this->sent(), 'the engine, not the client, judges the grammar');
    }

    /**
     * A provider that itself reaches the client (an instrumented tracer exporting through a Ferro
     * connection, say) must not recurse: the inner read sends nothing, the outer one still works.
     */
    public function testAReentrantProviderDoesNotRecurse(): void
    {
        $inner = 'unset';
        TraceContext::useProvider(static function () use (&$inner): string {
            $inner = TraceContext::current();
            return self::TP;
        });
        self::assertSame(self::TP, $this->sent());
        self::assertNull($inner, 'the nested read must send no context');
        self::assertSame(self::TP, $this->sent(), 'and the guard must be released afterwards');
    }

    public function testRemovingTheProviderStopsSending(): void
    {
        TraceContext::useProvider(static fn (): string => self::TP);
        self::assertSame(self::TP, $this->sent());
        TraceContext::useProvider(null);
        self::assertNull($this->sent());
    }
}
