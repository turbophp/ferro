<?php // /php/doctrine-dbal/src/Wrapper/IndeterminateSafeTransactional.php
declare(strict_types=1);
namespace Ferro\DBAL\Wrapper;

use Closure;
use Doctrine\DBAL\Connection as DbalConnection;
use Doctrine\DBAL\Exception\DriverException as DbalDriverException;
use Doctrine\DBAL\Exception\NoActiveTransaction;

/**
 * The one override that keeps SPEC §9.2's `Indeterminate` branch alive through
 * `Doctrine\DBAL\Connection::transactional()` — the canonical Doctrine transaction idiom, and the
 * one place where stock DBAL 4 throws the driver's verdict away.
 *
 * **THE MEASURED MECHANISM** (doctrine/dbal 4.4.4, `src/Connection.php:973-992`). After the closure
 * returns, `transactional()` commits inside a `try/catch/finally`:
 *
 * ```php
 * $shouldRollback = true;
 * try   { $this->commit(); $shouldRollback = false; }
 * catch (TheDriverException $t) {
 *     $shouldRollback = ! ($t instanceof TransactionRolledBack
 *         || $t instanceof UniqueConstraintViolationException
 *         || $t instanceof ForeignKeyConstraintViolationException
 *         || $t instanceof DeadlockException
 *         || $t instanceof ConnectionLost);
 *     throw $t;
 * } finally { if ($shouldRollback) { $this->rollBack(); } }
 * ```
 *
 * `Ferro\DBAL\IndeterminateWriteException` is on none of those five, so `$shouldRollback` stays
 * true — and `commit()`'s OWN `finally` has already decremented the nesting level to 0, so the
 * `rollBack()` in that `finally` throws `Doctrine\DBAL\Exception\NoActiveTransaction`, which
 * REPLACES the exception in flight. The application is handed **"There is no active transaction."**
 * — a message that reads like a programming error and is routinely logged and ignored — for a write
 * that may have landed. `catch (IndeterminateWriteException)` never fires; the real fate survives
 * only as `getPrevious()`. Stock `pdo_pgsql` reports the same event as `ConnectionLost`, which IS
 * exempt, so without this override Ferro is strictly WORSE than the driver it replaces on precisely
 * the case it exists to report.
 *
 * **WHY THIS IS NOT FIXED BY CHOOSING A DIFFERENT PARENT CLASS.** Charter rule 6 constrains SQL
 * generation, not exception ancestry, so re-parenting onto DBAL's exempt list was the first thing
 * tried. It is not available. Enumerated against the installed DBAL
 * (`ExceptionAncestryTest::testNoDbalExemptClassIsAnHonestParentForAnIndeterminateWrite` re-derives
 * this and goes red if any of it changes):
 *
 *  - `ConnectionLost` — the only semantically defensible target, and the one `pdo_pgsql` produces —
 *    is declared **`final`**. It cannot be extended.
 *  - `DeadlockException` carries `Doctrine\DBAL\Exception\RetryableException`, the marker Symfony
 *    Messenger and every hand-rolled retry loop key on. Inheriting it would convert the headline
 *    at-most-once guarantee into an at-least-once write (charter rule 3).
 *  - `TransactionRolledBack`, `UniqueConstraintViolationException` and
 *    `ForeignKeyConstraintViolationException` are extendable, but each makes a POSITIVE FALSE CLAIM
 *    about the fate: "it did not apply", "a duplicate already exists". Those are exactly the claims
 *    that make a retry — or a silent swallow — look safe. An indeterminate write is the absence of
 *    such a claim.
 *
 * So the ancestry stays `Doctrine\DBAL\Exception\DriverException` and the repair happens here.
 *
 * **WHY IT IS AN UNMASK RATHER THAN A REIMPLEMENTATION.** Copying `transactional()` with a widened
 * exempt list would pin us to one upstream body and drift silently when DBAL changes it. This calls
 * the real `parent::transactional()` and only undoes the substitution afterwards, so it is a NO-OP
 * the day upstream stops masking: there is then no `NoActiveTransaction` to catch and the driver's
 * exception arrives on its own.
 *
 * **WHY THE UNMASK IS DELIBERATELY NARROW.** It fires only on `NoActiveTransaction` — DBAL's own
 * transaction bookkeeping, which no application throws — and only when the driver's verdict is its
 * IMMEDIATE `previous`, which is the shape PHP produces when an exception escapes a `finally` while
 * another is in flight. It therefore cannot second-guess an application that CAUGHT the
 * indeterminate write inside the closure and threw its own exception carrying it as a cause: that
 * application has handled it, and its exception is what must come out.
 *
 * **KNOWN RESIDUAL, recorded rather than buried.** A driver exception raised INSIDE the closure is
 * masked by a different route — the first `finally`'s `rollBack()` runs at nesting level 1, reaches
 * the driver, and if IT fails loudly the rollback failure replaces the original. That route is not
 * covered here, because distinguishing it from application wrapping needs a chain walk, and a chain
 * walk is what lets this override overrule an application. In practice the client already swallows
 * the two reachable rollback failures (a lost frame and a tombstoned `tx_id` —
 * `TransactionTerminalTest::testRollbackSwallowsOnlyWhatTheClientSwallows`), and §19.3 classifies an
 * in-transaction statement `Retryable`, never `Indeterminate`, so the commit boundary above is where
 * the branch actually arrives.
 *
 * **USE.** `Ferro\DBAL\Wrapper\FerroConnection` already uses it, so the documented
 * `'wrapperClass' => Ferro\DBAL\Wrapper\FerroConnection::class` is all an application needs. It is a
 * trait rather than a method on that class so an application that must configure a DIFFERENT
 * `wrapperClass` — `Doctrine\DBAL\Connections\PrimaryReadReplicaConnection` is the shipped example —
 * can still recover the guarantee in one line:
 *
 * ```php
 * final class MyConnection extends PrimaryReadReplicaConnection
 * {
 *     use \Ferro\DBAL\Wrapper\IndeterminateSafeTransactional;
 * }
 * ```
 *
 * An application on the STOCK `Doctrine\DBAL\Connection` gets the masked behaviour, and that is
 * documented in `docs/known-incompatibilities.md` rather than silently tolerated.
 */
trait IndeterminateSafeTransactional
{
    /**
     * @template T
     *
     * @param Closure(DbalConnection):T $func
     *
     * @return T
     *
     * @throws \Throwable
     */
    public function transactional(Closure $func): mixed
    {
        try {
            return parent::transactional($func);
        } catch (NoActiveTransaction $masked) {
            $verdict = $masked->getPrevious();
            if ($verdict instanceof DbalDriverException) {
                // The driver already spoke. DBAL's cleanup does not get to overrule it — least of
                // all with a message that reads like an application bug.
                throw $verdict;
            }
            throw $masked;
        }
    }
}
