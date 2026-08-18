# Follow-up: when `ferrod` DIES, the MySQL implicit-commit protection dies with it — the client cannot latch what no wire field carries

**Found:** M1-S9 Task 3, cell **2b** of the new §20.3 kill-`ferrod` chaos harness
(`php/client/tests/Live/DaemonKillFateLiveTest.php::testImplicitCommitPrefixSurvivesDaemonKillOnMysqlPinnedResidual`),
on MySQL 8.4. It is the residual left by M1-S9a's blocker fix, and it was PREDICTED by that slice
rather than discovered here — what is new is that it has now been measured end to end and pinned.
**Belongs to:** `/proto` first, then `php/client`. **Not** an engine defect:
`ferro-pool`'s `tx_writes_persisted` latch is correct while the daemon lives, and it is the daemon's
death that destroys it.
**Severity:** medium-high, and narrow: MySQL/MariaDB only, inside an explicit transaction, after an
implicitly-committing statement, and only when the DAEMON (not the backend link) dies. In that cell
a retry-the-transaction wrapper re-applies writes that are already durable — the exact
at-least-once shape M1-S9a exists to close.
**NOT fixed here, deliberately:** closing it client-side would require a PHP mirror of
`ferro_classify::implicit_commit_hazard`, i.e. inferring semantics from SQL text at the client tier
— **charter rule 6 forbids it**, and the plan says so in as many words (Global Constraints,
charter rule 6). The honest fix is a wire signal, which is a `/proto` change and therefore a
DEFERRED CANDIDATE (charter rule 2), not something to hand-roll at an exit gate.

## What was measured (MySQL 8.4, through `ferro/client`, daemon SIGKILLed mid-statement)

```
BEGIN
INSERT … (k1)                       -- plain DML
CREATE TABLE chaos_ic_…(id INT)     -- IMPLICIT COMMIT: k1 is durable from here on
INSERT … (k2)  ← SIGKILL ferrod while this statement is parked in the server
```

Result, after restarting `ferrod` and reading back on a fresh connection:

| observation | value |
|---|---|
| `k1` (the implicitly-committed prefix) | **present — `count = 1`**, with **no COMMIT ever sent** |
| `k2` (the in-flight statement) | absent — `count = 0` |
| what the client threw | `Ferro\Client\Error\TransportException :: unexpected EOF after 0 of 16 bytes` |
| is it `IndeterminateException`? | **NO** |

So the caller is told "the connection went away during your transaction" — which reads as *nothing
in that transaction survived*. Half of it did.

## Why the engine's fix cannot reach this case

M1-S9a made this safe **while the daemon lives**: the pool tracks `tx_writes_persisted` from the
protocol authority plus a pre-dispatch lexical assist, and once set, the `Retryable` BRANCH becomes
unmintable for that transaction, so a later loss reports `WRITE_UNCONFIRMED{Indeterminate}` and a
retry wrapper correctly stops retrying.

That latch is **engine-side state in the pool**. A SIGKILL destroys it, and **no wire field carries
it** — the client learns nothing about it during the transaction, and after the death there is no
one left to ask. `HELLO_ACK`'s `boot_epoch` tells the client the engine restarted (§19.1); it says
nothing about which statements in the dead transaction had already committed.

This is also why the harness cell is a **pinned MEASUREMENT and not an assertion of desired
behaviour** — the same instrument §22.2 (ac)'s cry-wolf guard uses. If that test ever fails on its
last assertion, the client has grown a signal, and §19.3's residual note, §22.2 (aq) and the pin
must be updated together.

## The deferral candidate (do not build it in isolation)

**A per-statement `tx_writes_persisted` flag on the in-transaction EXEC terminal.** The client
latches it exactly as the engine does, from a signal it is GIVEN rather than one it infers, so
charter rule 6 stays intact; a client that has latched it then reports `Indeterminate` for a
subsequent transport loss instead of a bare connection error, with no SQL inspection anywhere.

Open questions a design must answer:

- **Which terminals carry it** — every in-transaction EXEC terminal, or only the one that sets it?
  Only-once is cheaper but is lost by a client that reconnects mid-transaction; every-terminal is
  idempotent and self-healing.
- **What a client does with it across a `boot_epoch` change.** The flag is per-transaction state on
  a `tx_id` the restart has already voided, so the client must decide the fate of the LAST statement
  it sent, not resume anything.
- **The stream terminal too?** A streamed in-transaction statement has the same exposure.

**Milestone: with the next `/proto`-touching slice**, alongside the two other codes this milestone
has already deferred rather than hand-rolled — `affected` on the stream terminal, and a
`TxNotFound` code so `rollBack()` need not swallow `ERR_PROTOCOL`. Batching them is the point: a
`/proto` change costs the registry, the golden vectors and BOTH codecs in one change set
(charter rule 2), so three deferred codes should land as one slice, not three.

## What NOT to do

- **Do not** mirror `ferro_classify` in PHP, in any form — not a keyword list, not a regex, not "a
  small allow-list of DDL verbs". That is read/write-adjacent inference from SQL text at the client
  tier (charter rule 6), and the engine-side version has ALREADY been wrong twice in this milestone
  (`EXECUTE`, then MySQL's executable `/*! … */` comments, which resurrected the at-least-once
  blocker end to end). A PHP copy would be a second thing to get wrong, with no test suite behind it.
- **Do not** widen the client to report `Indeterminate` for every in-transaction transport loss on
  MySQL. That is cry-wolf on the branch that must never be routinely retried (§22.2 (ac) records
  what that already costs the DBAL tier), and it would fire for PostgreSQL-shaped transactions where
  the prefix genuinely cannot have committed.
- **Do not** treat this as blocking M1 exit. It is recorded in `docs/known-incompatibilities.md`
  under *MySQL/MariaDB: an implicit commit changes what a lost statement reports*, and the operator
  advice there ("reconcile, do not retry") is correct for this case too.

## Where it is pinned

`php/client/tests/Live/DaemonKillFateLiveTest.php::testImplicitCommitPrefixSurvivesDaemonKillOnMysqlPinnedResidual`
— the whole cell, including the read-back that proves `k1` survived. Note the harness's ordering
belt (`assertKillerObservedAndKilled`): without it the cell can pass with `ferrod` still alive, a
false green measured at +0.94 s during Task 3's own mutation round.
