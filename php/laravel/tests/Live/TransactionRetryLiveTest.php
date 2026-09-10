<?php // /php/laravel/tests/Live/TransactionRetryLiveTest.php
declare(strict_types=1);
namespace Ferro\Laravel\Tests\Live;

use Ferro\Laravel\Exception\FerroQueryException;
use Illuminate\Database\DetectsConcurrencyErrors;
use Illuminate\Database\QueryException;

/**
 * **C1c's exit gate.** `DB::transaction($fn, attempts: N)` must actually RE-RUN the closure on a
 * PostgreSQL serialization failure — not merely classify it `Retryable` somewhere below.
 *
 * This is the guard for the trap recorded in `docs/dev-loop/PHASE-C-SCOPE.md`: Illuminate decides
 * whether to retry with `causedByConcurrencyError()`, which fires either on
 * `$e instanceof PDOException && $e->getCode() === '40001'` or on a message from a fixed substring
 * list. PostgreSQL's serialization message — `could not serialize access due to concurrent update`
 * — is NOT in that list, so the SQLSTATE path is the only one that can work. The sibling Doctrine
 * tier puts the vendor ERRNO in `getCode()`; copying that convention here would leave `attempts:`
 * silently inert for exactly this case while deadlocks kept retrying through message matching and
 * hid it.
 */
final class TransactionRetryLiveTest extends LaravelLiveTestCase
{
    use DetectsConcurrencyErrors {
        causedByConcurrencyError as public detects;
    }

    /**
     * THE END-TO-END PROOF: a real PG `40001`, raised by two genuinely concurrent SERIALIZABLE
     * transactions, makes Illuminate re-enter the closure.
     *
     * The conflict is forced with two SEPARATE connections (two `ferrod` sessions, two pooled
     * backend connections) so the serialization failure is real rather than simulated. The first
     * attempt updates a row that the OTHER transaction has already updated and committed under
     * SERIALIZABLE, which PostgreSQL refuses with `40001`. The second attempt runs after that
     * conflict is gone and succeeds — so `$attempts` reaching 2 is the observable that a retry
     * happened at all.
     */
    public function testASerializationFailureActuallyReRunsTheClosure(): void
    {
        $conn = $this->connection();
        $conn->unprepared('drop table if exists c1c_retry');
        $conn->unprepared('create table c1c_retry (id int primary key, n int not null)');
        $conn->statement('insert into c1c_retry (id, n) values (1, 0)');

        // A second, independent connection — the conflicting writer.
        $other = $this->connection();

        $attempts = 0;
        $conn->transaction(function ($c) use (&$attempts, $other): void {
            $attempts++;
            $c->unprepared('set transaction isolation level serializable');
            // Read under SERIALIZABLE, establishing the predicate PostgreSQL will check.
            $c->select('select n from c1c_retry where id = 1');

            if ($attempts === 1) {
                // The other connection writes and COMMITS while we hold our snapshot. Its write is
                // what makes our own update a serialization failure.
                $other->statement('update c1c_retry set n = n + 1 where id = 1');
            }

            $c->affectingStatement('update c1c_retry set n = n + 10 where id = 1');
        }, 3);

        self::assertSame(2, $attempts,
            'the closure must have RUN TWICE — attempts:3 is inert unless the SQLSTATE reaches '
            . 'Illuminate\'s causedByConcurrencyError()');
    }

    /**
     * The mechanism the test above depends on, asserted directly so a failure says WHICH half broke.
     *
     * Without this, a red end-to-end test could mean the retry wiring OR the SQLSTATE plumbing, and
     * the two have very different fixes.
     */
    public function testAFerroSerializationErrorIsRecognisedAsAConcurrencyError(): void
    {
        $e = FerroQueryException::fromFerro(
            new \Ferro\Client\Error\RetryableException(
                new \Ferro\Protocol\ErrorPayload(
                    \Ferro\Protocol\Generated\Constants::ERR_SERIALIZATION_FAILURE,
                    \Ferro\Protocol\Generated\Constants::BRANCH_RETRYABLE,
                    '40001',
                    null,
                    'could not serialize access due to concurrent update',
                    null,
                    null,
                ),
            ),
        );

        self::assertSame('40001', $e->getCode(), 'the SQLSTATE must be the exception CODE (PDO convention)');

        // Wrapped exactly as Illuminate wraps it, then classified exactly as Illuminate classifies it.
        $wrapped = new QueryException('pgsql', 'update c1c_retry set n = n + 10', [], $e);
        self::assertSame('40001', $wrapped->getCode(), 'QueryException copies the previous exception\'s code');
        self::assertTrue($this->detects($wrapped),
            'Illuminate must see this as a concurrency error, or attempts: never retries it');

        // The NEGATIVE control: PostgreSQL's serialization MESSAGE matches none of Illuminate's
        // substrings, so the code path above is the ONLY one that can work. If this ever starts
        // passing, the test above stopped proving anything.
        $messageOnly = new QueryException('pgsql', 'x', [], new \RuntimeException(
            'could not serialize access due to concurrent update',
        ));
        self::assertFalse($this->detects($messageOnly),
            'if the message alone were enough, the SQLSTATE plumbing would be untested');
    }
}
