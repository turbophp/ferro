# Follow-up: Doctrine's `transactional()` masks a failed COMMIT under "There is no active transaction."

> **STATUS: OPEN** — an UPSTREAM doctrine/dbal behaviour, identical on 3.10.6 and 4.4.4, that no
> driver can reach. Found by the M2-C5 adversarial review (SPEC §22.2 (by)); pinned by a tripwire
> test on each major so a change upstream turns it red.

## What happens

`Doctrine\DBAL\Connection::transactional()` commits and, when the COMMIT throws, decides whether to
roll back:

```php
$shouldRollback = true;
try {
    $this->commit();
    $shouldRollback = false;
} catch (TheDriverException $t) {
    $shouldRollback = ! ($t instanceof TransactionRolledBack || $t instanceof UniqueConstraintViolationException
        || $t instanceof ForeignKeyConstraintViolationException || $t instanceof DeadlockException
        || $t instanceof ConnectionLost);            // ConnectionLost: DBAL 4 only
    throw $t;
} finally {
    if ($shouldRollback) {
        $this->rollBack();
    }
}
```

But `commit()` resets the transaction nesting level in its own `finally`, **whether or not the COMMIT
succeeded** (`updateTransactionStateAfterCommit()`). So for any COMMIT failure outside that list,
the rollback in the `finally` above finds a nesting level of 0 and throws
`ConnectionException: There is no active transaction.` PHP then chains the real failure beneath it
as `getPrevious()`.

The rollback is thrown by the WRAPPER before it ever calls the driver, so no driver can prevent it.

## What it costs Ferro

Ferro's `IndeterminateWriteException` (§19.3: a COMMIT sent with no confirmed reply) is not in the
list, so through `transactional()`, an indeterminate COMMIT reaches the caller as:

```
Doctrine\DBAL\ConnectionException: There is no active transaction.
  └─ Ferro\DBAL\IndeterminateWriteException: … COMMIT sent with no confirmed response …
```

**The safety property holds:** nothing retryable reaches the top, so a framework that retries on
`RetryableException` does not replay a transaction that may have committed. **The documented class
does not:** `catch (IndeterminateWriteException)` around `transactional()` does not match. The
imperative form (`beginTransaction()` … `commit()`) is unaffected — `commit()` throws the
`IndeterminateWriteException` itself.

The same masking applies to stock drivers for any COMMIT-time failure not in the list — for example a
PostgreSQL deferred `EXCLUDE` or `CHECK` constraint trigger — so this is a general upstream defect
that Ferro's taxonomy happens to reach more often, not a Ferro-specific one.

## Pinned by

- DBAL 4: `TransactionalCommitFailureTest::testTransactionalMasksAnIndeterminateCommitUnderNoActiveTransaction`
- DBAL 3: `Dbal3TransactionTest::testTransactionalMasksAnIndeterminateCommitUnderNoActiveTransaction`

Each asserts the masked shape, that the `IndeterminateWriteException` is the chained previous, and
that nothing retryable reaches the top. A fix upstream turns the first assertion red; at that point
the incompatibility entry is marked FIXED and this file RESOLVED.

## Remedies considered

- **Upstream (preferred):** roll back in that `finally` only while a transaction is still active
  (`$shouldRollback && $this->isTransactionActive()`), which is correct for every driver — after a
  COMMIT attempt the transaction is over whichever way it went. Drafted here, not filed.
- **In Ferro's optional `wrapperClass`:** an override of `transactional()` could unmask it, but the
  wrapper is opt-in (it exists for `setTransactionIsolation()`), so most applications would not get
  it, and copying upstream's commit/rollback logic into two wrappers is the kind of second source of
  truth that rots across DBAL releases. Not done.
- **Making `IndeterminateWriteException` extend a listed class:** every listed class asserts a
  KNOWN outcome (rolled back, or a constraint/deadlock), and `ConnectionLost` is a
  `RetryableException` — the exact replay invitation the class exists to refuse. Rejected.
