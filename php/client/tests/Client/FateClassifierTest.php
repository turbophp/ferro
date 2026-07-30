<?php // /php/client/tests/Client/FateClassifierTest.php
declare(strict_types=1);
namespace Ferro\Tests\Client;

use Ferro\Client\Error\IndeterminateException;
use Ferro\Client\Error\NonRetryableException;
use Ferro\Client\Error\RetryableException;
use Ferro\Client\FateClassifier;
use Ferro\Client\OpKind;
use Ferro\Protocol\ErrorPayload;
use Ferro\Protocol\Generated\Constants as C;
use PHPUnit\Framework\TestCase;

/**
 * The §19.3 never-retry chokepoint. The decision table (branch × readonly × opKind × idempotent) and
 * the no-response classification (including the lost-COMMIT carve-out) are the client half of the
 * engine's defining safety property — proven exhaustively here.
 */
final class FateClassifierTest extends TestCase
{
    private FateClassifier $reads;   // retry_reads = true (default)
    private FateClassifier $noReads; // retry_reads = false

    protected function setUp(): void
    {
        $this->reads = new FateClassifier(retryReads: true);
        $this->noReads = new FateClassifier(retryReads: false);
    }

    // ---- the never-retry set (§19.3) ------------------------------------------------------------

    /** NEVER retry an Indeterminate — regardless of readonly, opKind, or an (impossible) idempotent flag. */
    public function testNeverRetriesIndeterminate(): void
    {
        foreach ([OpKind::Read, OpKind::Write, OpKind::TxStatement] as $op) {
            foreach ([true, false] as $ro) {
                $this->assertFalse(
                    $this->reads->mayRetry(C::BRANCH_INDETERMINATE, $ro, $op, idempotent: true),
                    "Indeterminate must never retry (op={$op->value}, readonly=" . ($ro ? '1' : '0') . ')',
                );
            }
        }
    }

    /** NEVER retry a lost/failed COMMIT — even on branch 1 (Retryable), readonly, with idempotent set. */
    public function testNeverRetriesLostCommit(): void
    {
        $this->assertFalse($this->reads->mayRetry(C::BRANCH_RETRYABLE, true, OpKind::TxCommit, idempotent: true));
        $this->assertFalse($this->reads->mayRetry(C::BRANCH_INDETERMINATE, false, OpKind::TxCommit));
    }

    /** NEVER retry a Retryable WRITE in M0 (no idempotent license — manifests are M3). */
    public function testNeverRetriesRetryableWriteWithoutIdempotentLicense(): void
    {
        $this->assertFalse($this->reads->mayRetry(C::BRANCH_RETRYABLE, false, OpKind::Write));
        // The license exists in the classifier (M3 will pass it) — proving the gate, not enabling it in M0.
        $this->assertTrue($this->reads->mayRetry(C::BRANCH_RETRYABLE, false, OpKind::Write, idempotent: true));
    }

    /** NEVER retry a NonRetryable (branch 3) or a garbled branch — the strictly safe fate. */
    public function testNeverRetriesNonRetryableOrGarbledBranch(): void
    {
        $this->assertFalse($this->reads->mayRetry(C::BRANCH_NON_RETRYABLE, true, OpKind::Read));
        $this->assertFalse($this->reads->mayRetry(7, true, OpKind::Read)); // garbled ⇒ not retryable
    }

    // ---- the one yes: a Retryable READ ----------------------------------------------------------

    public function testRetriesRetryableReadWhenRetryReads(): void
    {
        $this->assertTrue($this->reads->mayRetry(C::BRANCH_RETRYABLE, true, OpKind::Read));
    }

    public function testDoesNotRetryReadWhenRetryReadsDisabled(): void
    {
        $this->assertFalse($this->noReads->mayRetry(C::BRANCH_RETRYABLE, true, OpKind::Read));
    }

    // ---- classifyLoss: the §19.1 matrix + the carve-out -----------------------------------------

    /** A lost COMMIT (no response) is ALWAYS Indeterminate — the carve-out, checked first. */
    public function testClassifyLossCommitIsIndeterminate(): void
    {
        $ex = $this->reads->classifyLoss(OpKind::TxCommit, false, 'link dropped');
        $this->assertInstanceOf(IndeterminateException::class, $ex);
        $this->assertSame(C::ERR_WRITE_UNCONFIRMED, $ex->errorCode());
        $this->assertSame(C::BRANCH_INDETERMINATE, $ex->branch());
    }

    /** The carve-out beats even a (misbehaving) server hint that says Retryable. */
    public function testClassifyLossCommitStaysIndeterminateDespiteServerRetryableHint(): void
    {
        $serverRetryable = new ErrorPayload(C::ERR_CONNECTION_LOST, C::BRANCH_RETRYABLE, null, null, 'reset', null, null);
        $ex = $this->reads->classifyLoss(OpKind::TxCommit, false, 'reset', $serverRetryable);
        $this->assertInstanceOf(IndeterminateException::class, $ex);
    }

    /** A lost autocommit WRITE (no response) is Indeterminate. */
    public function testClassifyLossAutocommitWriteIsIndeterminate(): void
    {
        $ex = $this->reads->classifyLoss(OpKind::Write, false, 'link dropped');
        $this->assertInstanceOf(IndeterminateException::class, $ex);
    }

    /** A lost READ (no response) is Retryable{ConnectionLost} — a read has no write-fate. */
    public function testClassifyLossReadIsRetryable(): void
    {
        $ex = $this->reads->classifyLoss(OpKind::Read, true, 'link dropped');
        $this->assertInstanceOf(RetryableException::class, $ex);
        $this->assertSame(C::ERR_CONNECTION_LOST, $ex->errorCode());
    }

    /** A lost mid-tx statement / BEGIN / ROLLBACK is Retryable (the tx is dead → rolled back). */
    public function testClassifyLossMidTxIsRetryable(): void
    {
        foreach ([OpKind::TxBegin, OpKind::TxStatement, OpKind::TxRollback, OpKind::TxSavepoint] as $op) {
            $this->assertInstanceOf(
                RetryableException::class,
                $this->reads->classifyLoss($op, false, 'link dropped'),
                "a lost {$op->value} is Retryable, not Indeterminate",
            );
        }
    }

    /** For a NON-commit loss the engine's own branch hint is trusted verbatim. */
    public function testClassifyLossTrustsServerBranchForNonCommit(): void
    {
        $ind = new ErrorPayload(C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, null, null, 'x', null, null);
        $this->assertInstanceOf(IndeterminateException::class, $this->reads->classifyLoss(OpKind::Read, true, 'x', $ind));

        $nonRetry = new ErrorPayload(C::ERR_SYNTAX, C::BRANCH_NON_RETRYABLE, '42601', null, 'x', null, null);
        $this->assertInstanceOf(NonRetryableException::class, $this->reads->classifyLoss(OpKind::Read, true, 'x', $nonRetry));
    }

    // ---- mayRetryException reads the branch off the exception ------------------------------------

    public function testMayRetryExceptionRoutesByBranch(): void
    {
        $retryRead = new RetryableException(new ErrorPayload(C::ERR_CONNECTION_LOST, C::BRANCH_RETRYABLE, null, null, 'x', null, null));
        $ind = new IndeterminateException(new ErrorPayload(C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, null, null, 'x', null, null));
        $nonRetry = new NonRetryableException(new ErrorPayload(C::ERR_SYNTAX, C::BRANCH_NON_RETRYABLE, null, null, 'x', null, null));

        $this->assertTrue($this->reads->mayRetryException($retryRead, true, OpKind::Read));
        $this->assertFalse($this->reads->mayRetryException($retryRead, false, OpKind::Write)); // a write, never
        $this->assertFalse($this->reads->mayRetryException($ind, true, OpKind::Read));
        $this->assertFalse($this->reads->mayRetryException($nonRetry, true, OpKind::Read));
    }

    // ---- IndeterminateException::cause() — client-side inference, NEVER a wire field (M1-S4 T5) ---
    //
    // The wire carries only `code=WRITE_UNCONFIRMED, branch=Indeterminate`; `cause()` is scoped to
    // exactly what `classifyLoss`'s call site can honestly tell apart. Do NOT read a "timeout"
    // specificity into any of these — the wire cannot carry it.

    /** A no-response lost autocommit WRITE, no known epoch change -> the honest generic link_lost. */
    public function testClassifyLossWriteNoEpochChangeCauseIsLinkLostAndNeverRetried(): void
    {
        $ex = $this->reads->classifyLoss(OpKind::Write, false, 'link dropped mid-write');
        $this->assertInstanceOf(IndeterminateException::class, $ex);
        $this->assertSame(IndeterminateException::CAUSE_LINK_LOST, $ex->cause());
        $this->assertFalse($this->reads->mayRetryException($ex, false, OpKind::Write, idempotent: true));
    }

    /** The SAME no-response write loss, but the reconnect loop already knows the epoch changed. */
    public function testClassifyLossWriteEpochChangedCauseIsEngineRestartAndNeverRetried(): void
    {
        $ex = $this->reads->classifyLoss(OpKind::Write, false, 'link dropped mid-write', epochChanged: true);
        $this->assertInstanceOf(IndeterminateException::class, $ex);
        $this->assertSame(IndeterminateException::CAUSE_ENGINE_RESTART, $ex->cause());
        $this->assertFalse($this->reads->mayRetryException($ex, false, OpKind::Write, idempotent: true));
    }

    /** The lost-COMMIT carve-out gets the identical link_lost / engine_restart split. */
    public function testClassifyLossCommitCauseTracksEpochChanged(): void
    {
        $linkLost = $this->reads->classifyLoss(OpKind::TxCommit, false, 'link dropped during COMMIT');
        $this->assertInstanceOf(IndeterminateException::class, $linkLost);
        $this->assertSame(IndeterminateException::CAUSE_LINK_LOST, $linkLost->cause());

        $restart = $this->reads->classifyLoss(OpKind::TxCommit, false, 'link dropped during COMMIT', epochChanged: true);
        $this->assertInstanceOf(IndeterminateException::class, $restart);
        $this->assertSame(IndeterminateException::CAUSE_ENGINE_RESTART, $restart->cause());
    }

    /** A trusted server-reported Indeterminate hint is always engine_reported — epochChanged never overrides it. */
    public function testClassifyLossTrustedServerIndeterminateCauseIsEngineReported(): void
    {
        $serverInd = new ErrorPayload(C::ERR_WRITE_UNCONFIRMED, C::BRANCH_INDETERMINATE, null, null, 'x', null, null);
        $ex = $this->reads->classifyLoss(OpKind::Read, true, 'x', $serverInd, epochChanged: true);
        $this->assertInstanceOf(IndeterminateException::class, $ex);
        $this->assertSame(IndeterminateException::CAUSE_ENGINE_REPORTED, $ex->cause());
    }
}
