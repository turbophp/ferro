<?php // /php/doctrine-dbal/src/Wrapper/ResyncsNestingOnARejectedBegin.php
declare(strict_types=1);
namespace Ferro\DBAL\Wrapper;

/**
 * The override that keeps a REJECTED `beginTransaction()` from wedging the connection for good.
 *
 * **THE MEASURED MECHANISM** (doctrine/dbal 4.4.4, `src/Connection.php:1047-1062`):
 *
 * ```php
 * public function beginTransaction(): void
 * {
 *     $connection = $this->connect();
 *     ++$this->transactionNestingLevel;          // <-- BEFORE the driver call
 *     if ($this->transactionNestingLevel === 1) {
 *         try { $connection->beginTransaction(); }
 *         catch (Driver\Exception $e) { throw $this->convertException($e); }   // <-- no decrement
 *     } else { $this->createSavepoint(…); }
 * }
 * ```
 *
 * Under a PDO driver that is nearly harmless: a failed BEGIN there means a dead connection, and the
 * application is going to reconnect anyway. **Ferro rejects a BEGIN retryably on a perfectly healthy
 * client session** — `PoolTimeout` under load, a `ConnectionLost` that was never transmitted, an
 * unavailable replica — so the shape is ROUTINE, and the driver hands it back as
 * `Ferro\DBAL\RetryableDriverException implements Doctrine\DBAL\Exception\RetryableException`, i.e.
 * it tells the framework to retry.
 *
 * Without this override the retry cannot succeed. MEASURED, live, with 16 connections holding open
 * transactions against an `max_size = 16` pool:
 *
 *     [mysql] 1st beginTransaction -> Ferro\DBAL\RetryableDriverException  (retryable-marked: YES)
 *     [mysql]   isTransactionActive after the FAILED begin: true   nesting: 1
 *     [mysql] retry transactional() threw: … "SAVEPOINT DOCTRINE_2" refused
 *     [pg]    same, and nesting climbs 1 -> 2 and never comes back
 *
 * — because at nesting ≥ 2 DBAL's own `rollBack()` takes the savepoint branch and its `--` never
 * runs either. A Messenger / Octane / RoadRunner worker that hit ONE pool timeout was finished.
 *
 * **WHY THE REPAIR IS A `rollBack()` AND NOT A DECREMENT.** `transactionNestingLevel` is private, so
 * a subclass cannot touch it; the ONE public path that lowers it is `rollBack()`, which at level 1
 * sets it to 0 **before** calling the driver (`Connection.php:1133-1140`) — so the counter is
 * repaired even if the driver's rollback fails. On the Ferro driver it does not fail: a rollBack
 * after a rejected BEGIN is a no-op that clears the driver's own desync flag, and it is honest
 * because every statement in that window is REFUSED (`Ferro\DBAL\Connection::rollBack()` and
 * `::refuseWhileNestingIsDesynced()`), so there is provably nothing to undo.
 *
 * **WHY ONLY AT LEVEL 1.** At nesting ≥ 2 the failure came from `createSavepoint()` on a REAL open
 * transaction, and `parent::rollBack()` would then issue `ROLLBACK TO SAVEPOINT DOCTRINE_n` for a
 * savepoint that was never created — turning a reported failure into a second, unrelated one. That
 * case is left exactly as DBAL leaves it.
 *
 * **THE RESYNC NEVER REPLACES THE CAUSE.** It runs inside a `catch` and rethrows `$e`; its own
 * failure is swallowed, deliberately, because the exception the caller must see is the one that
 * explains why the transaction did not start. (`autoCommit = false` is the one shape that can throw
 * from `rollBack()`'s `finally` — DBAL re-begins there — and swallowing is what keeps that from
 * overwriting the original verdict.)
 *
 * It is a trait rather than a method on {@see FerroConnection} for the same reason
 * {@see IndeterminateSafeTransactional} is: an application that must configure a DIFFERENT
 * `wrapperClass` (`Doctrine\DBAL\Connections\PrimaryReadReplicaConnection` is the shipped example)
 * can recover the behaviour in one line.
 *
 * An application on the STOCK `Doctrine\DBAL\Connection` does not get the resync. What it gets
 * instead — and this is the half that needs no configuration — is a driver that REFUSES every
 * statement in the desynced window with a message naming the cause and the one-line fix, and a
 * `rollBack()` that resynchronises. Silent autocommit writes behind a believed-open transaction are
 * closed either way.
 */
trait ResyncsNestingOnARejectedBegin
{
    public function beginTransaction(): void
    {
        try {
            parent::beginTransaction();
        } catch (\Throwable $e) {
            if ($this->getTransactionNestingLevel() === 1) {
                try {
                    parent::rollBack();
                } catch (\Throwable) {
                    // Best effort by design — see the class docblock. The verdict below is what the
                    // caller needs, and nothing here may replace it.
                }
            }
            throw $e;
        }
    }
}
