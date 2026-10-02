<?php // /php/client/tests/Unit/TraceContextTest.php
declare(strict_types=1);
namespace Ferro\Tests\Unit;

use Ferro\Client\ExecCodec;
use Ferro\Client\Hydration\PlanCache;
use Ferro\Client\TraceContext;
use Ferro\Client\Value\M1ValuePolicy;
use Ferro\Client\Value\TypePolicyOptions;
use Ferro\Protocol\ExecRequest;
use Ferro\Protocol\Generated\Constants as C;
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
        // The C4c-1 review's MAJOR finding: a non-UTF-8 byte made the ENGINE refuse the whole
        // request as `Protocol`, failing the statement. 55 bytes, under the cap — so only an
        // ASCII check stops it. A provider that forwards an inbound HTTP header verbatim hands an
        // external caller this byte.
        yield 'a non-UTF-8 byte' => ["00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0\xff"];
        yield 'valid UTF-8 but not ASCII' => ["00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0\u{e9}"];
        yield 'a space' => [' 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'];
        yield 'a control character' => ["00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\n"];
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
     *
     * The provider stops at depth 2 by itself, so a MISSING guard fails the assertion below rather
     * than recursing until the process is killed (the C4c-1 review's mutation was "caught" only by
     * a 13.6 GB OOM).
     */
    public function testAReentrantProviderDoesNotRecurse(): void
    {
        $inner = 'unset';
        $depth = 0;
        TraceContext::useProvider(static function () use (&$inner, &$depth): string {
            if (++$depth > 1) {
                return 'RECURSED';
            }
            $inner = TraceContext::current();
            return self::TP;
        });
        self::assertSame(self::TP, $this->sent());
        self::assertNull($inner, 'the nested read must send no context');
        $depth = 0;
        self::assertSame(self::TP, $this->sent(), 'and the guard must be released afterwards');
    }

    /** The guard is released when the provider THROWS, too — asserted directly, not by test order. */
    public function testTheGuardIsReleasedAfterAProviderThrows(): void
    {
        $throw = true;
        TraceContext::useProvider(static function () use (&$throw): string {
            if ($throw) {
                throw new \RuntimeException('tracer down');
            }
            return self::TP;
        });
        self::assertNull($this->sent());
        $throw = false;
        self::assertSame(self::TP, $this->sent(), 'a throw left the re-entrancy guard set');
    }

    /**
     * The re-entrancy guard is PER FIBER (C4c-1 review F4). A provider that suspends its fiber —
     * one doing I/O under Revolt or Swoole hooks, say — must not blank the context of every OTHER
     * fiber while it is parked: each reads its own. The original process-wide flag returned null
     * to the main fiber and to an unrelated fiber B for as long as A stayed suspended.
     */
    public function testAProviderSuspendedInOneFiberDoesNotBlankAnother(): void
    {
        $a = null;
        TraceContext::useProvider(static function () use (&$a): string {
            $current = \Fiber::getCurrent();
            if ($current !== null && $current === $a) {
                \Fiber::suspend();
                return 'ctx-a';
            }
            return $current === null ? 'ctx-main' : 'ctx-b';
        });

        $a = new \Fiber(static fn (): ?string => TraceContext::current());
        $a->start();
        self::assertTrue($a->isSuspended(), 'fiber A must be parked inside its provider');

        self::assertSame('ctx-main', TraceContext::current(), 'the main fiber was blanked');
        $b = new \Fiber(static fn (): ?string => TraceContext::current());
        $b->start();
        self::assertSame('ctx-b', $b->getReturn(), 'an unrelated fiber was blanked');

        $a->resume();
        self::assertSame('ctx-a', $a->getReturn());
    }

    /** ...while re-entrancy inside ONE fiber is still refused. */
    public function testReentrancyInsideAFiberIsStillRefused(): void
    {
        $inner = 'unset';
        $depth = 0;
        TraceContext::useProvider(static function () use (&$inner, &$depth): string {
            if (++$depth > 1) {
                return 'RECURSED';
            }
            $inner = TraceContext::current();
            return self::TP;
        });
        $f = new \Fiber(static fn (): ?string => TraceContext::current());
        $f->start();
        self::assertSame(self::TP, $f->getReturn());
        self::assertNull($inner);
    }

    /**
     * C4c-1 review F5: a statement that fits the 16 MiB frame cap WITHOUT a trace context must not
     * be failed by adding one. The encoder drops the context instead, and the statement is sent.
     */
    public function testATraceContextNeverPushesAStatementOverTheFrameCap(): void
    {
        $codec = new ExecCodec(
            new M1ValuePolicy(new TypePolicyOptions()),
            new PlanCache(),
            new PurePacker(),
            new PurePacker(),
        );
        $probe = $codec->encode('main', 'select ?', ['x'], false, ExecCodec::FETCH_NONE, null);
        // A string param sized so the payload WITHOUT a trace sits 20 bytes under the cap.
        $len = C::MAX_FRAME_PAYLOAD - 20 - (strlen($probe) - 1);
        $param = str_repeat('x', $len);
        $without = $codec->encode('main', 'select ?', [$param], false, ExecCodec::FETCH_NONE, null);
        self::assertGreaterThan(C::MAX_FRAME_PAYLOAD - 64, strlen($without), 'the fixture must sit near the cap');
        self::assertLessThanOrEqual(C::MAX_FRAME_PAYLOAD, strlen($without), 'the fixture must fit without a trace');

        TraceContext::useProvider(static fn (): string => self::TP);
        $with = $codec->encode('main', 'select ?', [$param], false, ExecCodec::FETCH_NONE, null);
        self::assertLessThanOrEqual(C::MAX_FRAME_PAYLOAD, strlen($with), 'the trace pushed the frame over the cap');
        self::assertSame($without, $with, 'the context is dropped, nothing else changes');

        // CONTROL: a statement that fits WITH its trace still carries it.
        $small = $codec->encode('main', 'select 1', [], true, ExecCodec::FETCH_ROWS, null);
        $off = 0;
        $w = (new PurePacker())->unpack($small, $off);
        self::assertIsArray($w);
        self::assertSame(self::TP, ExecRequest::mapFromWire($w)['traceparent']);
    }

    /** The PHP decoder's arity is strict too (the review's P-j2 mutation survived). */
    public function testTheDecoderRefusesThePreviousArity(): void
    {
        $this->expectException(\Ferro\Protocol\CodecException::class);
        ExecRequest::mapFromWire(['main', 'select 1', null, [], null, true, 0, null]);
    }

    public function testRemovingTheProviderStopsSending(): void
    {
        TraceContext::useProvider(static fn (): string => self::TP);
        self::assertSame(self::TP, $this->sent());
        TraceContext::useProvider(null);
        self::assertNull($this->sent());
    }
}
