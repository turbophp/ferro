<?php // /php/client/tests/Client/ErrorMapperTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\CancelledException;
use Ferro\Client\Error\ErrorMapper;
use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\ProtocolException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\FateClassifier;
use Ferro\Client\OpKind;
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

    /**
     * M1-S4 T5: an engine-replied `Outcome::Error{code: WRITE_UNCONFIRMED, branch: Indeterminate}`
     * (the cancelled/timed-out autocommit-write case, S4 T2) decodes to
     * {@see IndeterminateException::CAUSE_ENGINE_REPORTED} — the wire cannot distinguish it from any
     * OTHER engine-reported WriteUnconfirmed, so this honest generic label is used, NEVER `"timeout"`.
     * It is NEVER auto-retried by {@see FateClassifier}, even with `retry_reads=true` or a (M3-only,
     * not consulted here) idempotent hint.
     */
    public function testEngineReportedWriteUnconfirmedIsIndeterminateCauseAndNeverAutoRetried(): void
    {
        $outcome = Outcome::error(self::payload(C::BRANCH_INDETERMINATE, code: C::ERR_WRITE_UNCONFIRMED));
        $ex = ErrorMapper::fromOutcome($outcome);

        $this->assertInstanceOf(IndeterminateException::class, $ex);
        $this->assertSame(IndeterminateException::CAUSE_ENGINE_REPORTED, $ex->cause());

        $fate = new FateClassifier(retryReads: true);
        $this->assertFalse($fate->mayRetryException($ex, false, OpKind::Write, idempotent: true));
        // Even mis-declared as a retryable-eligible read shape, an Indeterminate is never retried —
        // the never-retry check on branch happens before readonly/idempotent are ever consulted.
        $this->assertFalse($fate->mayRetryException($ex, true, OpKind::Read, idempotent: true));
    }
}
