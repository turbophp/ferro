<?php // /php/psr18/tests/Unit/FateTest.php
declare(strict_types=1);
namespace Ferro\Psr18\Tests\Unit;

use Ferro\Client\Error\InFlightLimitException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Http\Adapter\Failure;
use Ferro\Http\Error\HttpIndeterminateException;
use Ferro\Http\Error\HttpRetryableException;
use Ferro\Http\Error\RequestTooLargeException;
use Ferro\Http\Error\ResponseIncompleteException;
use Ferro\Http\Fate;
use Ferro\Http\FateClass;
use Ferro\Http\HttpFate;
use Ferro\Http\ResponseHead;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Psr18\FatedResponse;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\TestCase;

/**
 * M6-F9: `Ferro\Http\Fate::of()`, the one reader (SPEC §23.11.4), and the shared cause table.
 */
final class FateTest extends TestCase
{
    private static function payload(int $code, int $branch, ?string $detail): ErrorPayload
    {
        return new ErrorPayload($code, $branch, null, null, 'm', $detail, 250);
    }

    private static function head(int $status, bool $idempotent): ResponseHead
    {
        return new ResponseHead($status, 11, 'R', [], [], null, $idempotent);
    }

    public function testItReadsACarrierAndAClientFailure(): void
    {
        $fate = new HttpFate(FateClass::Retryable, 10, true, 503);
        $this->assertSame($fate, Fate::of(new FatedResponse(new Response(503), $fate)));

        $e = new HttpIndeterminateException(self::payload(C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'reset'), 'reset');
        $read = Fate::of($e);
        $this->assertSame(FateClass::Indeterminate, $read?->fate);
        $this->assertSame('reset', $read->cause);
        $this->assertSame(250, $read->retryAfterMs);
    }

    public function testItReadsThroughThePreviousChainAsLaravelWrapsAConnectException(): void
    {
        $inner = new HttpRetryableException(self::payload(C::ERR_UPSTREAM_UNAVAILABLE, C::BRANCH_RETRYABLE, 'dns'), 'dns');
        $laravelLike = new \RuntimeException('Connection refused', 0, new \LogicException('wrapper', 0, $inner));
        $this->assertSame(FateClass::Retryable, Fate::of($laravelLike)?->fate);
    }

    public function testItReadsTheResponseAnExceptionCarries(): void
    {
        $response = new FatedResponse(new Response(429), new HttpFate(FateClass::Retryable, 1000, false, 429));
        $guzzleLike = new class ('429', $response) extends \RuntimeException {
            public function __construct(string $m, private readonly object $r) { parent::__construct($m); }
            public function getResponse(): object { return $this->r; }
        };
        $this->assertSame(1000, Fate::of($guzzleLike)?->retryAfterMs);

        // Illuminate's RequestException: a public $response whose toPsrResponse() is the PSR response.
        $illuminateResponse = new class ($response) {
            public function __construct(private readonly object $psr) {}
            public function toPsrResponse(): object { return $this->psr; }
        };
        $illuminateLike = new class ($illuminateResponse) extends \RuntimeException {
            public function __construct(public object $response) { parent::__construct('HTTP request returned status code 429'); }
        };
        $this->assertSame(FateClass::Retryable, Fate::of($illuminateLike)?->fate);
        $this->assertSame(FateClass::Retryable, Fate::of($illuminateResponse)?->fate);
    }

    public function testItVouchesForNothingElse(): void
    {
        $this->assertNull(Fate::of(new Response(503)), 'a response no Ferro adapter built (or one a middleware rebuilt)');
        $this->assertNull(Fate::of(new \RuntimeException('curl error 28')));
        $this->assertNull(Fate::of(new \stdClass()));
        $private = new class extends \RuntimeException {
            private object $response;
            public function __construct() { parent::__construct('x'); $this->response = new FatedResponse(new Response(500), new HttpFate(FateClass::Retryable)); }
        };
        $this->assertNull(Fate::of($private), 'only a PUBLIC response property is read');
    }

    public function testACarrierInTheChainWinsOverTheResponseItCarries(): void
    {
        // A Ferro RequestException after a head: its marker is the combined fate; the response it carries
        // (a 2xx, NotAFailure on its own) must not override it.
        $response = new FatedResponse(new Response(200), new HttpFate(FateClass::NotAFailure, null, false, 200));
        $e = new class ('body failed', $response, new HttpIndeterminateException(self::payload(C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, 'reset'), 'reset')) extends \RuntimeException {
            public function __construct(string $m, private readonly object $r, \Throwable $prev) { parent::__construct($m, 0, $prev); }
            public function getResponse(): object { return $this->r; }
        };
        $this->assertSame(FateClass::Indeterminate, Fate::of($e)?->fate);
    }

    public function testTheResponseIncompleteFateIsCombinedWithItsHead(): void
    {
        $e = new ResponseIncompleteException(self::payload(C::ERR_RESPONSE_INCOMPLETE, C::BRANCH_NON_RETRYABLE, 'body_eof'), 'body_eof', self::head(504, false));
        $fate = Fate::of($e);
        $this->assertSame(FateClass::Indeterminate, $fate?->fate);
        $this->assertSame(504, $fate->status);
        $this->assertFalse($fate->idempotent);
    }

    public function testFailureClassifiesClientErrorsThatCarryNoCause(): void
    {
        $this->assertSame(Failure::CONNECT, Failure::of(new InFlightLimitException('full'))->kind);
        $this->assertSame(FateClass::Retryable, Failure::of(new InFlightLimitException('full'))->fate->fate);
        $this->assertSame(Failure::REFUSED, Failure::of(new RequestTooLargeException('big'))->kind);
        $this->assertSame(Failure::REFUSED, Failure::of(new \InvalidArgumentException('bad header'))->kind);
        $unsupported = new NonRetryableException(new ErrorPayload(C::ERR_UNSUPPORTED, C::BRANCH_NON_RETRYABLE, null, null, 'no http', null, null));
        $this->assertSame(Failure::REFUSED, Failure::of($unsupported)->kind);
        $this->assertSame(FateClass::NonRetryable, Failure::of(new \LogicException('?'))->fate->fate, 'unknown: never Retryable');
    }

    public function testAClientFailureOfNoRetryableOrIndeterminateTaxonomyIsNonRetryable(): void
    {
        foreach ([
            new \Ferro\Client\Error\ProtocolException('desync'),
            new RequestTooLargeException('big'),
            new \Ferro\Client\Error\CancelledException(),
            new \Ferro\Client\Error\ReentrantWriteException('nested'),
        ] as $e) {
            $this->assertSame(FateClass::NonRetryable, HttpFate::ofFailure($e)?->fate, $e::class . ': never Retryable');
        }
        $this->assertSame(FateClass::Retryable, HttpFate::ofFailure(new InFlightLimitException('full'))?->fate);
        $this->assertNull(HttpFate::ofFailure(new \RuntimeException('not ferro')));
    }

    public function testAClientSynthesisedLinkLossIsAConnectFailureOnlyWhenRetryable(): void
    {
        $notSent = new HttpRetryableException(self::payload(C::ERR_CONNECTION_LOST, C::BRANCH_RETRYABLE, null), 'link_lost', true);
        $this->assertSame(Failure::CONNECT, Failure::of($notSent)->kind);
        $sent = new HttpIndeterminateException(self::payload(C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, null), 'link_lost', true);
        $this->assertSame(Failure::REQUEST, Failure::of($sent)->kind, 'as curl\'s 56: a naive decider does not re-send it');
        $this->assertTrue(Failure::of($sent)->fate->clientSynthesised);
    }

    public function testTheCauseTableIsTotalOverTheRegistry(): void
    {
        $this->assertEqualsCanonicalizing(C::HTTP_CAUSES, array_keys(Failure::CAUSE_KIND));
    }
}
