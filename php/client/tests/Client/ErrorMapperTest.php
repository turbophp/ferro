<?php // /php/client/tests/Client/ErrorMapperTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\CancelledException;
use Ferro\Client\Error\ErrorMapper;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\RetryableException;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use Ferro\Protocol\Outcome;
use PHPUnit\Framework\TestCase;

/**
 * The three-branch taxonomy is chosen from the WIRE `branch` byte alone, and — the load-bearing
 * §19.3 case — an unknown/garbled branch maps to NON-retryable, never a retryable default.
 */
final class ErrorMapperTest extends TestCase
{
    private static function payload(int $branch, ?string $sqlstate = null, int $code = 12289): ErrorPayload
    {
        return new ErrorPayload($code, $branch, $sqlstate, null, 'boom', null, null);
    }

    public function testBranchRetryableMapsToRetryable(): void
    {
        $e = ErrorMapper::fromErrorPayload(self::payload(C::BRANCH_RETRYABLE));
        $this->assertInstanceOf(RetryableException::class, $e);
        $this->assertSame(C::BRANCH_RETRYABLE, $e->branch());
    }

    public function testBranchIndeterminateMapsToIndeterminate(): void
    {
        $e = ErrorMapper::fromErrorPayload(self::payload(C::BRANCH_INDETERMINATE, code: C::ERR_WRITE_UNCONFIRMED));
        $this->assertInstanceOf(IndeterminateException::class, $e);
        $this->assertSame(C::BRANCH_INDETERMINATE, $e->branch());
        $this->assertSame(C::ERR_WRITE_UNCONFIRMED, $e->errorCode());
    }

    public function testBranchNonRetryableMapsToNonRetryableAndCarriesSqlstate(): void
    {
        $e = ErrorMapper::fromErrorPayload(self::payload(C::BRANCH_NON_RETRYABLE, '42601', C::ERR_SYNTAX));
        $this->assertInstanceOf(NonRetryableException::class, $e);
        $this->assertSame('42601', $e->sqlstate());
        $this->assertSame(C::ERR_SYNTAX, $e->errorCode());
    }

    /**
     * THE safety case (§19.3): a branch byte NOT in {1,2,3} must NOT default to Retryable — a
     * garbled branch that silently retried would breach the never-retry property. It maps to
     * NonRetryable, and is explicitly NOT a RetryableException.
     */
    public function testUnknownBranchMapsToNonRetryableNeverRetryable(): void
    {
        $e = ErrorMapper::fromErrorPayload(self::payload(7));
        $this->assertInstanceOf(NonRetryableException::class, $e);
        $this->assertNotInstanceOf(RetryableException::class, $e);
        $this->assertSame(7, $e->branch());
    }

    public function testCancelledOutcomeMapsToCancelled(): void
    {
        $e = ErrorMapper::fromOutcome(Outcome::cancelled());
        $this->assertInstanceOf(CancelledException::class, $e);
        $this->assertNotInstanceOf(RetryableException::class, $e);
    }

    public function testErrorOutcomeIsClassifiedOnBranch(): void
    {
        $outcome = Outcome::error(self::payload(C::BRANCH_INDETERMINATE, code: C::ERR_WRITE_UNCONFIRMED));
        $this->assertInstanceOf(IndeterminateException::class, ErrorMapper::fromOutcome($outcome));
    }

    /** Calling the mapper on an Ok outcome is a caller bug → a protocol fault, never a silent pass. */
    public function testOkOutcomeYieldsProtocolException(): void
    {
        $e = ErrorMapper::fromOutcome(Outcome::ok("\xc0"));
        $this->assertInstanceOf(ProtocolException::class, $e);
    }
}
