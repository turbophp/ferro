<?php // /php/client/src/Client/OpKind.php
declare(strict_types=1);
namespace Ferro\Client;

/**
 * What the client was doing when a failure occurred — the axis the §19.1/§19.3 fate matrix keys on
 * alongside the wire `branch` and the client-declared `readonly` flag. Distinct from `readonly`
 * because a lost COMMIT and a lost autocommit write both carry the same `readonly=false` yet a lost
 * COMMIT is the ONE transactional `Indeterminate` (the carve-out), while a lost mid-tx statement is
 * `Retryable` (the tx is dead → rolled back → the closure re-runs).
 */
enum OpKind: string
{
    /** An autocommit read (`readonly=true`, `tx_id=null`) — a lost read has no write-fate: Retryable. */
    case Read = 'read';

    /** An autocommit write (`readonly=false`, `tx_id=null`) — a lost write is `WriteUnconfirmed{Indeterminate}`. */
    case Write = 'write';

    /** `TX/BEGIN` — a lost BEGIN never opened the tx (nothing applied): Retryable, safe to re-run. */
    case TxBegin = 'tx_begin';

    /** A statement inside an open tx — a lost one dies with the (rolled-back) tx: Retryable, closure re-runs. */
    case TxStatement = 'tx_statement';

    /** `TX/COMMIT` — a lost COMMIT is the ONE transactional `Indeterminate` (§19.3). NEVER re-run. */
    case TxCommit = 'tx_commit';

    /** `TX/ROLLBACK` — a lost rollback is not a lost write (the tx is gone either way): Retryable. */
    case TxRollback = 'tx_rollback';

    /** `TX/SAVEPOINT`/`RELEASE`/`ROLLBACK_TO` — savepoint control, not a durable write: Retryable. */
    case TxSavepoint = 'tx_savepoint';
}
