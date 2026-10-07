<?php // /php/client/tests/Client/HttpFateTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\CancelledException;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\FateClassifier;
use Ferro\Client\OpKind;
use Ferro\Http\Error\HttpCancelledException;
use Ferro\Http\Error\HttpException;
use Ferro\Http\Error\HttpFates;
use Ferro\Http\Error\HttpIndeterminateException;
use Ferro\Http\Error\HttpNonRetryableException;
use Ferro\Http\Error\HttpRetryableException;
use Ferro\Http\Error\ResponseIncompleteException;
use Ferro\Http\FateClass;
use Ferro\Http\ResponseHead;
use Ferro\Http\StatusFate;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Outcome;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * M6-F8: SPEC §23.7.1's engine fates, §23.7.3's client fates and §23.7.4's status table, as the PHP
 * client maps them ({@see HttpFates}, {@see StatusFate}). Every `[http.causes]` token and every
 * registry error code is mapped from the GENERATED constants, so a code or a token added to the
 * registry is covered without editing this file.
 */
final class HttpFateTest extends TestCase
{
    private static function head(int $status = 200, bool $idempotent = false, array $headers = []): ResponseHead
    {
        return ResponseHead::fromWire([
            'status' => $status, 'version' => 11, 'reason' => null, 'headers' => $headers,
            'decoded' => null, 'idempotent' => $idempotent,
        ]);
    }

    private static function error(int $code, int $branch, ?string $detail, ?int $retryAfter = null): Outcome
    {
        return Outcome::error(new ErrorPayload($code, $branch, null, null, 'm', $detail, $retryAfter));
    }

    /** @return array<string, int> every registry error code, by constant name */
    private static function codes(): array
    {
        $out = [];
        foreach ((new \ReflectionClass(C::class))->getConstants() as $name => $value) {
            if (str_starts_with($name, 'ERR_') && !str_ends_with($name, '_BRANCH') && is_int($value)) {
                $out[$name] = $value;
            }
        }
        return $out;
    }

    public function testEveryCodeAndEveryCauseMapsByTheWireBranch(): void
    {
        $codes = self::codes();
        $this->assertArrayHasKey('ERR_RESPONSE_INCOMPLETE', $codes, 'the registry walk found the HTTP codes');
        $this->assertCount(45, C::HTTP_CAUSES, '§23.5.6 has 45 tokens');
        $checked = 0;
        foreach ($codes as $name => $code) {
            $branch = constant(C::class . '::' . $name . '_BRANCH');
            foreach (C::HTTP_CAUSES as $cause) {
                foreach ([null, self::head()] as $head) {
                    $e = HttpFates::fromOutcome(self::error($code, $branch, $cause), $head);
                    $this->assertInstanceOf(HttpException::class, $e, "{$name}/{$cause}");
                    $this->assertSame($cause, $e->cause(), "{$name}/{$cause}: cause is the detail token");
                    $this->assertFalse($e->clientSynthesised());
                    $expected = match ($branch) {
                        C::BRANCH_RETRYABLE => HttpRetryableException::class,
                        C::BRANCH_INDETERMINATE => HttpIndeterminateException::class,
                        default => $code === C::ERR_RESPONSE_INCOMPLETE && $head !== null
                            ? ResponseIncompleteException::class
                            : HttpNonRetryableException::class,
                    };
                    $this->assertInstanceOf($expected, $e, "{$name}/{$cause}");
                    $base = [
                        C::BRANCH_RETRYABLE => RetryableException::class,
                        C::BRANCH_INDETERMINATE => IndeterminateException::class,
                        C::BRANCH_NON_RETRYABLE => NonRetryableException::class,
                    ][$branch];
                    $this->assertInstanceOf($base, $e, "{$name}/{$cause}: extends the taxonomy base of its branch");
                    $this->assertSame($code, $e->errorCode());
                    ++$checked;
                }
            }
        }
        $this->assertSame(count($codes) * 45 * 2, $checked);
    }

    public function testAGarbledBranchIsNeverRetryable(): void
    {
        foreach ([0, 4, 9, 255] as $branch) {
            $e = HttpFates::fromOutcome(self::error(C::ERR_UPSTREAM_UNAVAILABLE, $branch, C::HTTP_CAUSE_DNS), null);
            $this->assertInstanceOf(HttpNonRetryableException::class, $e, "branch {$branch}");
            $this->assertFalse((new FateClassifier())->mayRetryException($e, true, OpKind::Read));
        }
    }

    /**
     * §23.11.1's table: the PHP exception base per code, with `retryAfterMs` on the two Retryable
     * codes.
     *
     * @return iterable<string, array{int, int, string, class-string}>
     */
    public static function specCodeTable(): iterable
    {
        yield 'UpstreamUnavailable' => [C::ERR_UPSTREAM_UNAVAILABLE, C::ERR_UPSTREAM_UNAVAILABLE_BRANCH, C::HTTP_CAUSE_BREAKER_OPEN, RetryableException::class];
        yield 'RateLimited' => [C::ERR_RATE_LIMITED, C::ERR_RATE_LIMITED_BRANCH, C::HTTP_CAUSE_RATE_LIMITED, RetryableException::class];
        yield 'TlsRefused' => [C::ERR_TLS_REFUSED, C::ERR_TLS_REFUSED_BRANCH, C::HTTP_CAUSE_TLS_VERIFY, NonRetryableException::class];
        yield 'ResponseIncomplete' => [C::ERR_RESPONSE_INCOMPLETE, C::ERR_RESPONSE_INCOMPLETE_BRANCH, C::HTTP_CAUSE_BODY_EOF, ResponseIncompleteException::class];
        yield 'WriteUnconfirmed' => [C::ERR_WRITE_UNCONFIRMED, C::ERR_WRITE_UNCONFIRMED_BRANCH, C::HTTP_CAUSE_EOF_EMPTY, IndeterminateException::class];
    }

    #[DataProvider('specCodeTable')]
    public function testTheSpecCodeTable(int $code, int $branch, string $cause, string $base): void
    {
        $e = HttpFates::fromOutcome(self::error($code, $branch, $cause, 1500), self::head());
        $this->assertInstanceOf($base, $e);
        $this->assertSame($cause, $e instanceof HttpException ? $e->cause() : null);
        if ($base === RetryableException::class) {
            $this->assertInstanceOf(HttpRetryableException::class, $e);
            $this->assertSame(1500, $e->retryAfterMs());
        }
    }

    /**
     * §23.7.1's table, row by row and column by column (the engine's verdicts as it sends them):
     * the class, the cause and the fate the client exposes.
     *
     * @return iterable<string, array{int, string, bool, class-string, FateClass}>
     */
    public static function specFateRows(): iterable
    {
        $n = C::BRANCH_NON_RETRYABLE;
        $r = C::BRANCH_RETRYABLE;
        $i = C::BRANCH_INDETERMINATE;
        $rows = [
            // before dispatch
            ['policy', C::ERR_FORBIDDEN, $n, C::HTTP_CAUSE_FORBIDDEN_TARGET, false, HttpNonRetryableException::class, FateClass::NonRetryable],
            ['rate', C::ERR_RATE_LIMITED, $r, C::HTTP_CAUSE_RETRY_AFTER_HOLD, false, HttpRetryableException::class, FateClass::Retryable],
            ['breaker', C::ERR_UPSTREAM_UNAVAILABLE, $r, C::HTTP_CAUSE_BREAKER_PROBE_BUSY, false, HttpRetryableException::class, FateClass::Retryable],
            ['drain', C::ERR_UPSTREAM_UNAVAILABLE, $r, C::HTTP_CAUSE_DRAINING, false, HttpRetryableException::class, FateClass::Retryable],
            ['queue', C::ERR_POOL_TIMEOUT, $r, C::HTTP_CAUSE_QUEUE_FULL, false, HttpRetryableException::class, FateClass::Retryable],
            ['budget', C::ERR_POOL_TIMEOUT, $r, C::HTTP_CAUSE_BODY_BUDGET, false, HttpRetryableException::class, FateClass::Retryable],
            ['deadline', C::ERR_POOL_TIMEOUT, $r, C::HTTP_CAUSE_DEADLINE, false, HttpRetryableException::class, FateClass::Retryable],
            ['dial', C::ERR_UPSTREAM_UNAVAILABLE, $r, C::HTTP_CAUSE_CONNECT_REFUSED, false, HttpRetryableException::class, FateClass::Retryable],
            ['tls_handshake', C::ERR_UPSTREAM_UNAVAILABLE, $r, C::HTTP_CAUSE_TLS_HANDSHAKE, false, HttpRetryableException::class, FateClass::Retryable],
            ['tls_verify', C::ERR_TLS_REFUSED, $n, C::HTTP_CAUSE_TLS_VERIFY, false, HttpNonRetryableException::class, FateClass::NonRetryable],
            // dispatched, not sent
            ['unsent', C::ERR_CONNECTION_LOST, $r, C::HTTP_CAUSE_UNSENT_CLOSED, false, HttpRetryableException::class, FateClass::Retryable],
            // sent, no head
            ['link idem', C::ERR_CONNECTION_LOST, $r, C::HTTP_CAUSE_EOF_EMPTY, false, HttpRetryableException::class, FateClass::Retryable],
            ['link non-idem', C::ERR_WRITE_UNCONFIRMED, $i, C::HTTP_CAUSE_RESET, false, HttpIndeterminateException::class, FateClass::Indeterminate],
            ['timeout idem', C::ERR_QUERY_TIMEOUT, $n, C::HTTP_CAUSE_TIMEOUT, false, HttpNonRetryableException::class, FateClass::NonRetryable],
            ['timeout non-idem', C::ERR_WRITE_UNCONFIRMED, $i, C::HTTP_CAUSE_TIMEOUT, false, HttpIndeterminateException::class, FateClass::Indeterminate],
            ['cancel non-idem', C::ERR_WRITE_UNCONFIRMED, $i, C::HTTP_CAUSE_CANCELLED, false, HttpIndeterminateException::class, FateClass::Indeterminate],
            // head received
            ['body idem', C::ERR_CONNECTION_LOST, $r, C::HTTP_CAUSE_BODY_RESET, true, HttpRetryableException::class, FateClass::Retryable],
            ['body non-idem', C::ERR_RESPONSE_INCOMPLETE, $n, C::HTTP_CAUSE_BODY_FRAMING, true, ResponseIncompleteException::class, FateClass::NonRetryable],
            ['decode', C::ERR_RESPONSE_INCOMPLETE, $n, C::HTTP_CAUSE_DECODE, true, ResponseIncompleteException::class, FateClass::NonRetryable],
            ['read_idle', C::ERR_QUERY_TIMEOUT, $n, C::HTTP_CAUSE_READ_IDLE, true, HttpNonRetryableException::class, FateClass::NonRetryable],
            ['drain after head', C::ERR_RESPONSE_INCOMPLETE, $n, C::HTTP_CAUSE_DRAINING, true, ResponseIncompleteException::class, FateClass::NonRetryable],
        ];
        foreach ($rows as [$label, $code, $branch, $cause, $afterHead, $class, $fate]) {
            yield $label => [$code, $branch, $cause, $afterHead, $class, $fate];
        }
    }

    #[DataProvider('specFateRows')]
    public function testTheSpecFateTable(int $code, int $branch, string $cause, bool $afterHead, string $class, FateClass $fate): void
    {
        $this->assertSame(constant(C::class . '::' . self::nameOf($code) . '_BRANCH'), $branch, 'the row uses the registry branch');
        $e = HttpFates::fromOutcome(self::error($code, $branch, $cause), $afterHead ? self::head() : null);
        $this->assertInstanceOf($class, $e);
        $this->assertInstanceOf(HttpException::class, $e);
        $this->assertSame($cause, $e->cause());
        $this->assertSame($fate, $e->fate());
    }

    private static function nameOf(int $code): string
    {
        return (string) array_search($code, self::codes(), true);
    }

    public function testCancelledIsItsOwnClass(): void
    {
        $e = HttpFates::fromOutcome(Outcome::cancelled(), self::head());
        $this->assertInstanceOf(HttpCancelledException::class, $e);
        $this->assertInstanceOf(CancelledException::class, $e);
        $this->assertSame(C::HTTP_CAUSE_CANCELLED, $e->cause());
        $this->assertSame(FateClass::NonRetryable, $e->fate());
        // It keeps the head it was cancelled after (review LOW): applied, if that was a 2xx.
        $this->assertSame(200, $e->head()?->status);
        $this->assertTrue($e->wasApplied());
        $before = HttpFates::fromOutcome(Outcome::cancelled(), null);
        $this->assertInstanceOf(HttpCancelledException::class, $before);
        $this->assertNull($before->head());
        $this->assertNull($before->wasApplied(), 'never false');
    }

    public function testATerminalWithoutACauseIsNotAnExchangeFate(): void
    {
        // Protocol and Unsupported carry no detail (§23.5.6 as amended at F2): plain taxonomy classes.
        foreach ([C::ERR_UNSUPPORTED, C::ERR_PROTOCOL] as $code) {
            $e = HttpFates::fromOutcome(self::error($code, C::BRANCH_NON_RETRYABLE, null), null);
            $this->assertInstanceOf(NonRetryableException::class, $e);
            $this->assertNotInstanceOf(HttpException::class, $e);
        }
    }

    public function testResponseIncompleteCarriesTheHeadAndTheCombinedFate(): void
    {
        $applied = HttpFates::fromOutcome(
            self::error(C::ERR_RESPONSE_INCOMPLETE, C::BRANCH_NON_RETRYABLE, C::HTTP_CAUSE_BODY_EOF),
            self::head(201, false, [['x-a', '1'], ['x-a', '2']]),
        );
        $this->assertInstanceOf(ResponseIncompleteException::class, $applied);
        $this->assertSame(201, $applied->status());
        $this->assertSame(['x-a' => ['1', '2']], $applied->headers());
        $this->assertTrue($applied->wasApplied(), 'a 2xx head: the request was applied');
        $this->assertSame(FateClass::NonRetryable, $applied->fate());

        $five = HttpFates::fromOutcome(
            self::error(C::ERR_RESPONSE_INCOMPLETE, C::BRANCH_NON_RETRYABLE, C::HTTP_CAUSE_BODY_RESET),
            self::head(500, false),
        );
        $this->assertInstanceOf(ResponseIncompleteException::class, $five);
        $this->assertNull($five->wasApplied(), 'never false: a 500 promises nothing');
        $this->assertSame(FateClass::Indeterminate, $five->fate(), '§23.7.1: non-idempotent 5xx + truncated body is Indeterminate');

        $idem500 = HttpFates::fromOutcome(
            self::error(C::ERR_RESPONSE_INCOMPLETE, C::BRANCH_NON_RETRYABLE, C::HTTP_CAUSE_DECODE),
            self::head(500, true),
        );
        $this->assertSame(FateClass::NonRetryable, $idem500 instanceof HttpException ? $idem500->fate() : null);
    }

    /**
     * §23.7.3, cell by cell — the client classifies a link that died with the request in flight.
     *
     * @return iterable<string, array{bool, bool, ?ResponseHead, class-string, FateClass}>
     */
    public static function clientCells(): iterable
    {
        yield 'not written, declared' => [false, true, null, HttpRetryableException::class, FateClass::Retryable];
        yield 'not written, undeclared' => [false, false, null, HttpRetryableException::class, FateClass::Retryable];
        yield 'written no head, declared' => [true, true, null, HttpRetryableException::class, FateClass::Retryable];
        yield 'written no head, undeclared' => [true, false, null, HttpIndeterminateException::class, FateClass::Indeterminate];
        yield 'head, engine says idempotent' => [true, false, self::head(200, true), HttpRetryableException::class, FateClass::Retryable];
        yield 'head, engine says not (declared true is overridden by the head)' => [true, true, self::head(200, false), ResponseIncompleteException::class, FateClass::NonRetryable];
        yield 'head 2xx, non-idempotent' => [true, false, self::head(200, false), ResponseIncompleteException::class, FateClass::NonRetryable];
        yield 'head 502, non-idempotent' => [true, false, self::head(502, false), ResponseIncompleteException::class, FateClass::Indeterminate];
    }

    #[DataProvider('clientCells')]
    public function testTheClientFateTable(bool $sent, bool $declared, ?ResponseHead $head, string $class, FateClass $fate): void
    {
        $e = HttpFates::linkLost($sent, $declared, $head, 'link died');
        $this->assertInstanceOf($class, $e);
        $this->assertInstanceOf(HttpException::class, $e);
        $this->assertSame(HttpException::CLIENT_LINK_LOST, $e->cause());
        $this->assertTrue($e->clientSynthesised());
        $this->assertSame($fate, $e->fate());
        $this->assertFalse(
            (new FateClassifier())->mayRetryException($e, false, OpKind::Write),
            'the client policy never re-issues an HTTP request',
        );
        if ($e instanceof HttpIndeterminateException) {
            $this->assertSame(IndeterminateException::CAUSE_LINK_LOST, $e->inferredCause(), 'never engine_restart: no reconnect was made');
        }
    }

    public function testTheClientCauseIsNotARegistryToken(): void
    {
        $this->assertNotContains(HttpException::CLIENT_LINK_LOST, C::HTTP_CAUSES);
        $this->assertNotContains(IndeterminateException::CAUSE_ENGINE_RESTART, C::HTTP_CAUSES);
    }

    /** @return iterable<string, array{int, bool, ?string, FateClass, ?int}> */
    public static function statusTable(): iterable
    {
        $now = 1_000_000.0;
        yield '200' => [200, false, null, FateClass::NotAFailure, null];
        yield '301' => [301, false, null, FateClass::NotAFailure, null];
        yield '101' => [101, false, null, FateClass::NotAFailure, null];
        yield '408 non-idem' => [408, false, null, FateClass::Retryable, null];
        yield '425 non-idem' => [425, false, null, FateClass::Retryable, null];
        yield '429 no header' => [429, false, null, FateClass::Retryable, null];
        yield '429 seconds' => [429, false, '3', FateClass::Retryable, 3000];
        yield '503 seconds non-idem' => [503, false, '2', FateClass::Retryable, 2000];
        yield '503 date' => [503, false, gmdate('D, d M Y H:i:s', (int) $now + 5) . ' GMT', FateClass::Retryable, 5000];
        yield '503 past date' => [503, false, gmdate('D, d M Y H:i:s', (int) $now - 5) . ' GMT', FateClass::Retryable, 0];
        yield '503 rfc850' => [503, true, gmdate('l, d-M-y H:i:s', (int) $now + 7) . ' GMT', FateClass::Retryable, 7000];
        yield '503 garbage non-idem' => [503, false, 'soon', FateClass::Indeterminate, null];
        yield '503 none idem' => [503, true, null, FateClass::Retryable, null];
        yield '404' => [404, true, null, FateClass::NonRetryable, null];
        yield '400' => [400, false, '5', FateClass::NonRetryable, null];
        yield '501' => [501, false, null, FateClass::NonRetryable, null];
        yield '505' => [505, true, null, FateClass::NonRetryable, null];
        yield '500 idem' => [500, true, null, FateClass::Retryable, null];
        yield '500 non-idem' => [500, false, null, FateClass::Indeterminate, null];
        yield '502 non-idem' => [502, false, null, FateClass::Indeterminate, null];
        yield '504 non-idem' => [504, false, null, FateClass::Indeterminate, null];
        yield '599 non-idem' => [599, false, null, FateClass::Indeterminate, null];
        // RFC 9110 §15: an invalid status (600..=999, which `ferrod` passes through) is processed as a 5xx.
        yield '600 idem' => [600, true, null, FateClass::Retryable, null];
        yield '600 non-idem' => [600, false, null, FateClass::Indeterminate, null];
        yield '999 non-idem' => [999, false, '5', FateClass::Indeterminate, null];
    }

    #[DataProvider('statusTable')]
    public function testTheStatusTable(int $status, bool $idempotent, ?string $retryAfter, FateClass $fate, ?int $delay): void
    {
        $v = StatusFate::of($status, $idempotent, $retryAfter, 1_000_000.0);
        $this->assertSame($fate, $v->fate);
        $this->assertSame($delay, $v->retryAfterMs);
    }

    public function testAStatusThatIsNotThreeDigitsIsRefused(): void
    {
        foreach ([99, 1000] as $status) {
            try {
                StatusFate::of($status, true, null);
                $this->fail("{$status} accepted");
            } catch (\InvalidArgumentException) {
            }
        }
        $this->addToAssertionCount(1);
    }

    public function testCombineIsTheMostCautious(): void
    {
        $order = [FateClass::NotAFailure, FateClass::Retryable, FateClass::NonRetryable, FateClass::Indeterminate];
        foreach ($order as $i => $a) {
            foreach ($order as $j => $b) {
                $this->assertSame($order[max($i, $j)], FateClass::combine($a, $b), "{$a->value} + {$b->value}");
            }
        }
    }
}
