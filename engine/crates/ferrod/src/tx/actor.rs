//! The per-`tx_id` transaction actor (S6): ONE task that OWNS the pinned `Checkout` for a
//! transaction's whole life and serializes [`TxCommand`]s onto it, plus the two deadline timers and
//! the out-of-band mid-statement cancel path.
//!
//! **Loop shape (PINNED — the naive shape double-borrows the `Checkout` and will not compile).**
//! The task owns `co: Checkout<B>` and never lets a second task touch it (the borrow checker
//! enforces this). ONE outer `tokio::select!` waits on `{ abort_signal, idle_timer, max_timer,
//! cmd_rx.recv() }`. To run a user statement INTERRUPTIBLY, the `co.query(..)` future is
//! `tokio::pin!`ned and a second `select!` waits on `{ that future, max_timer, abort_signal }` — the
//! idle timer is deliberately absent there (a running statement is not "idle in transaction"; only
//! the absolute `max_tx` bounds it). On a timer/abort arm firing mid-statement the actor:
//!   1. fires the OUT-OF-BAND cancel captured BEFORE the borrow (`co.cancel_handle()` returns an
//!      owned handle; firing it borrows nothing the live query future holds `&mut`), then
//!   2. AWAITs the pinned query future to its now-erroring (`57014`) completion — it does NOT drop
//!      it — so the connection returns to `ReadyForQuery` before any teardown ROLLBACK pipelines
//!      behind it, then
//!   3. rolls back (or taints, if the rollback itself errors) and ends.
//!
//! The statement is NEVER re-dispatched (charter rule 3); a mid-statement deadline yields exactly
//! ONE terminal (`TxDeadline`) for the in-flight request.

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use ferro_pool::backend::{Cancel, Dialect, PoolBackend, QueryResult};
use ferro_pool::error::PoolError;
use ferro_pool::pool::Checkout;
use ferro_proto::messages::tx::Isolation;

use crate::services::fate;
use crate::services::sql::StreamEnded;

use super::{CtlReply, ExecReply, TxCommand, TxRegistry};

/// Compose the engine's transaction-opening statement for `dialect` from the request's `isolation`
/// (a `u8` off the wire) and `readonly` flag. Pure, and unit-tested per (dialect × isolation ×
/// readonly) cell. `isolation == None` leaves the server/pool default in place (no `ISOLATION LEVEL`
/// clause). An unknown isolation byte is a client error (`Err`, mapped by the handler to
/// `Protocol`), never coerced to a default.
///
/// **The result is ONE statement string, always.** [`Checkout::begin_tx_with`] issues exactly one
/// `simple_query` and wraps it in the whole pin/RFQ/tracker/Rule-A sequence, so a second call would
/// re-run that sequence — the MySQL isolation forms are therefore a `CLIENT_MULTI_STATEMENTS` batch
/// (already negotiated by the vendored fork), not two calls.
///
/// **Why MySQL cannot use the PG spelling** (measured on MySQL 8.4.11 AND MariaDB 11.8.8):
/// `BEGIN READ ONLY`, `BEGIN ISOLATION LEVEL …` and `START TRANSACTION ISOLATION LEVEL …` are all
/// `ERROR 1064 (42000)`. The only working forms are `START TRANSACTION [READ ONLY]` and a
/// `SET TRANSACTION …;` prefix, whose modifier applies to the NEXT transaction only — so it must
/// immediately precede the `START TRANSACTION`.
///
/// **Why the BATCH and not two statements** (also measured): a STANDALONE `SET TRANSACTION …`
/// returns an OK packet with `SERVER_SESSION_STATE_CHANGED` and NO trackers, which
/// `ferro_backend_mysql::tracker::is_mutation` reads as a real session mutation — so it would taint
/// EVERY isolation/readonly transaction into a full `COM_RESET_CONNECTION` at the next recycle.
/// Batched, `query_drop` drains both result sets and the FINAL OK packet carries a
/// `TransactionState` tracker, which gates the bare-flag path off: no taint, `tx_status` reads
/// `InTx`, and `SERVER_STATUS_IN_TRANS_READONLY` confirms the read-only mode took.
///
/// The batch consequently MASKS the intermediate statement's own trackers (only the final OK packet
/// is read). That is acceptable here and only here: the engine composes this string itself. A USER
/// batch still goes through `Checkout::exec`, which is unchanged.
///
/// **The `SESSION`/`GLOBAL` scoped spellings are FORBIDDEN here.**
/// `SET SESSION TRANSACTION ISOLATION LEVEL …` would persist the level on the POOLED connection past
/// `COMMIT`, so the next tenant would inherit it — a cross-tenant connection-state leak (charter
/// rule 6). The next-transaction-only spelling is correct *because* it does not persist; its
/// observable consequence is that the level is NOT readable back from `@@transaction_isolation`
/// (which keeps reporting the session default, rendered by MySQL with a hyphen: `REPEATABLE-READ`),
/// so it must be verified by a LOCK CONFLICT, never by reading that variable. See SPEC §22.2 (s).
///
/// Examples: `(Postgres, None, false) → "BEGIN"`; `(Postgres, None, true) → "BEGIN READ ONLY"`;
/// `(MySql, Some(Serializable), true) →
/// "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION READ ONLY"`.
pub fn compose_begin_sql(
    dialect: Dialect,
    isolation: Option<u8>,
    readonly: bool,
) -> Result<String, String> {
    let level = match isolation {
        None => None,
        Some(iso) => Some(match Isolation::try_from(iso).map_err(|e| e.to_string())? {
            Isolation::ReadCommitted => "READ COMMITTED",
            Isolation::RepeatableRead => "REPEATABLE READ",
            Isolation::Serializable => "SERIALIZABLE",
        }),
    };

    // Exhaustive on purpose: a new `Dialect` variant must break the build here rather than silently
    // inherit PG syntax. NEVER add a `_ =>` arm.
    match dialect {
        Dialect::Postgres => {
            let mut sql = String::from("BEGIN");
            if let Some(level) = level {
                sql.push_str(" ISOLATION LEVEL ");
                sql.push_str(level);
            }
            if readonly {
                sql.push_str(" READ ONLY");
            }
            Ok(sql)
        }
        Dialect::MySql => {
            let start = if readonly {
                "START TRANSACTION READ ONLY"
            } else {
                "START TRANSACTION"
            };
            Ok(match level {
                None => start.to_string(),
                Some(level) => format!("SET TRANSACTION ISOLATION LEVEL {level}; {start}"),
            })
        }
        Dialect::Sqlite => {
            if level.is_some() || readonly {
                Err(
                    "isolation/readonly BEGIN is not supported on the sqlite dialect (no SQLite \
                     backend exists yet; this arm exists so one cannot silently inherit PG syntax)"
                        .to_string(),
                )
            } else {
                Ok("BEGIN".to_string())
            }
        }
    }
}

/// The engine-named savepoint stack for one transaction (`sp_1`, `sp_2`, … — a monotonic per-tx
/// counter, NEVER a client string in the SQL). A client may attach an optional alias to a savepoint
/// and reference it later by that alias; the alias is a lookup key ONLY — the engine still runs its
/// own composed `sp_N` name, so there is no savepoint-name injection surface (charter rule 6).
#[derive(Debug, Default)]
struct SavepointStack {
    counter: u64,
    stack: Vec<SavepointEntry>,
}

#[derive(Debug)]
struct SavepointEntry {
    engine_name: String,
    client_name: Option<String>,
}

impl SavepointStack {
    fn new() -> Self {
        Self::default()
    }

    /// Allocate the next engine name (`sp_N`), push it with an optional client alias, and return
    /// the engine name to run `SAVEPOINT <name>` with.
    fn push(&mut self, client_name: Option<String>) -> String {
        self.counter += 1;
        let engine_name = format!("sp_{}", self.counter);
        self.stack.push(SavepointEntry {
            engine_name: engine_name.clone(),
            client_name,
        });
        engine_name
    }

    /// Undo the most recent [`SavepointStack::push`] (its backend `SAVEPOINT` then failed).
    fn pop(&mut self) {
        self.stack.pop();
    }

    /// Resolve a client-provided name (`None` → the top of the stack) to `(stack index, engine
    /// name)`, matching a client alias first, then a bare engine name, searching from the top.
    fn resolve(&self, name: Option<&str>) -> Option<(usize, String)> {
        match name {
            None => self
                .stack
                .last()
                .map(|e| (self.stack.len() - 1, e.engine_name.clone())),
            Some(n) => self
                .stack
                .iter()
                .enumerate()
                .rev()
                .find(|(_, e)| e.client_name.as_deref() == Some(n) || e.engine_name == n)
                .map(|(i, e)| (i, e.engine_name.clone())),
        }
    }

    /// RELEASE semantics: the named savepoint AND every one established after it are destroyed.
    fn truncate_release(&mut self, idx: usize) {
        self.stack.truncate(idx);
    }

    /// ROLLBACK TO semantics: every savepoint after the named one is destroyed; it stays.
    fn truncate_rollback_to(&mut self, idx: usize) {
        self.stack.truncate(idx + 1);
    }

    #[cfg(test)]
    fn depth(&self) -> usize {
        self.stack.len()
    }
}

/// Why the actor's loop ended — decides the teardown (rollback-or-not) and the registry
/// bookkeeping (deregister vs tombstone).
#[derive(Debug, Clone, Copy)]
enum TxEnd {
    /// An explicit COMMIT/ROLLBACK ran: the pinned conn is released and the tx deregistered.
    Ended,
    /// Session death / an abort token: roll back (or taint), release, deregister.
    Abort,
    /// A deadline (idle or max) fired: any in-flight statement was already cancelled + drained;
    /// roll back (or taint), release, and TOMBSTONE the tx_id as `TxDeadline`.
    Deadline,
}

/// The result of one interruptible-statement `select!`.
enum ExecStep {
    Completed(Result<QueryResult, PoolError>, u64),
    Deadline,
    Abort,
}

/// Run one transaction's actor to completion. Owns `co` (moved in at BEGIN) and drops it on the way
/// out (RAII → the pinned conn returns to the pool). Signals `done_tx = true` last, so
/// `TxRegistry::abort_session` observing `done` knows the conn is already back in the pool.
#[allow(clippy::too_many_arguments)]
pub async fn run<B: PoolBackend>(
    tx_id: u64,
    mut co: Checkout<B>,
    mut cmd_rx: mpsc::Receiver<TxCommand>,
    abort: CancellationToken,
    done_tx: watch::Sender<bool>,
    registry: TxRegistry,
    idle_timeout: Duration,
    max_timeout: Duration,
    teardown_timeout: Duration,
) {
    // `idle_timeout` is reset on every processed command (inter-statement idle); `max_timeout` is
    // absolute from BEGIN and never reset.
    let idle_deadline = tokio::time::sleep(idle_timeout);
    let max_deadline = tokio::time::sleep(max_timeout);
    tokio::pin!(idle_deadline, max_deadline);

    let mut sp = SavepointStack::new();

    let end: TxEnd = 'actor: loop {
        let cmd = tokio::select! {
            biased;

            // Teardown signals take priority over a queued command: an aborting/timed-out tx must
            // not first drain another statement.
            () = abort.cancelled() => break 'actor TxEnd::Abort,
            () = &mut idle_deadline => break 'actor TxEnd::Deadline,
            () = &mut max_deadline => break 'actor TxEnd::Deadline,

            cmd = cmd_rx.recv() => match cmd {
                Some(c) => c,
                // All senders (the handle's `cmd_tx` clones) dropped without an abort — treat as an
                // abort so the conn is still rolled back and released rather than leaked.
                None => break 'actor TxEnd::Abort,
            },
        };

        match cmd {
            TxCommand::Commit { reply } => {
                let _ = reply.send(ctl_reply(co.commit_tx().await));
                break 'actor TxEnd::Ended;
            }
            TxCommand::Rollback { reply } => {
                let _ = reply.send(ctl_reply(co.rollback_tx().await));
                break 'actor TxEnd::Ended;
            }
            TxCommand::Savepoint { name, reply } => {
                let engine = sp.push(name);
                match co.tx_control(&format!("SAVEPOINT {engine}")).await {
                    Ok(()) => {
                        let _ = reply.send(CtlReply::Ok);
                    }
                    Err(e) => {
                        sp.pop(); // the SAVEPOINT did not take
                        let _ = reply.send(CtlReply::Err(e));
                    }
                }
            }
            TxCommand::Release { name, reply } => match sp.resolve(name.as_deref()) {
                None => {
                    let _ = reply.send(CtlReply::UnknownSavepoint);
                }
                Some((idx, engine)) => match co.tx_control(&format!("RELEASE {engine}")).await {
                    Ok(()) => {
                        sp.truncate_release(idx);
                        let _ = reply.send(CtlReply::Ok);
                    }
                    Err(e) => {
                        let _ = reply.send(CtlReply::Err(e));
                    }
                },
            },
            TxCommand::RollbackTo { name, reply } => match sp.resolve(name.as_deref()) {
                None => {
                    let _ = reply.send(CtlReply::UnknownSavepoint);
                }
                Some((idx, engine)) => {
                    match co.tx_control(&format!("ROLLBACK TO {engine}")).await {
                        Ok(()) => {
                            sp.truncate_rollback_to(idx);
                            let _ = reply.send(CtlReply::Ok);
                        }
                        Err(e) => {
                            let _ = reply.send(CtlReply::Err(e));
                        }
                    }
                }
            },
            TxCommand::Exec {
                sql,
                params,
                timeout_ms,
                cancel,
                reply,
            } => {
                // Capture the out-of-band cancel BEFORE borrowing `co` for the query — it returns an
                // owned handle, so the shared borrow ends immediately and does not conflict with the
                // `&mut co` the query future then holds. Named `cancel_handle` (NOT `cancel`) to
                // stay DISTINCT from the per-request `cancel: CancellationToken` this command now
                // carries (M1-S4 Task 3) — the two are different things: this is the out-of-band
                // primitive that actually interrupts the SERVER statement; `cancel` below is the
                // per-request SIGNAL that we should do so.
                let cancel_handle = co.cancel_handle();
                let exec_start = std::time::Instant::now();
                // M1-S1: `co.query` (`ferro-pool`'s `Checkout::query`) reads the real RFQ status byte
                // after this statement drains and calls `apply_tx_status`, so a clean success/failure
                // is ALSO confirmed by the real protocol signal here, not just inferred. But the actual
                // safety GUARANTEE on this Err arm — that a statement erroring mid-tx (e.g. a
                // constraint violation, flipping the real tx to `E`) leaves `tx_open`/`tainted` armed
                // for cleanup — comes from `Checkout`'s Rule-A unconditional Err-arm fail-safe (forces
                // both bits on ANY `r.is_err()`, regardless of what the RFQ byte reads), not from the
                // RFQ read itself: the atomic is stale-untrustworthy on the Err arm (SPEC §7.1;
                // `ferro-backend-pg/tests/pg_pool_it.rs`'s `pg_rfq_failed_stmt_holds_pin_until_rollback`
                // states the same caveat). `teardown`'s own `set_tainted(true)` (below) stays
                // belt-and-braces on top of that fail-safe, not the sole mechanism.
                let query_fut = co.query(&sql, &params);
                tokio::pin!(query_fut);

                let step = tokio::select! {
                    biased;

                    // Prefer completion over interruption if both are ready in the same poll, so a
                    // statement that just finished is never spuriously reported as a deadline.
                    r = &mut query_fut => {
                        ExecStep::Completed(r, exec_start.elapsed().as_micros() as u64)
                    }
                    () = &mut max_deadline => ExecStep::Deadline,
                    () = abort.cancelled() => ExecStep::Abort,
                    // M1-S4 Task 3: the per-STATEMENT `ExecRequest.timeout_ms` deadline and the
                    // per-REQUEST CANCEL token. Both resolve to the SAME `ExecStep::Deadline` the
                    // actor's own absolute `max_tx` timer uses below — a client cancel/timeout
                    // in-tx gets the identical cancel -> drain -> ROLLBACK -> tombstone ->
                    // `TxDeadline{Retryable}` treatment (§19.3: the safe uniform in-tx action is
                    // roll back; the client restarts), never the `Abort` (drop-reply, no fate)
                    // path — that stays reserved for the session-level `abort` above.
                    () = sleep_opt(timeout_ms) => ExecStep::Deadline,
                    () = cancel.cancelled() => ExecStep::Deadline,
                };

                match step {
                    ExecStep::Completed(result, exec_us) => {
                        // An app-set `statement_timeout` (or any other bare 57014) can resolve
                        // through the query's OWN completion rather than any select arm above —
                        // PG has already aborted the tx block on this error exactly like a
                        // mid-statement deadline would (the next statement would see 25P02), so it
                        // MUST take the SAME rollback+tombstone+TxDeadline exit, not be forwarded
                        // as a statement error the client might mistake for retry-in-place-able.
                        // No cancel_handle fire / drain needed: the query already resolved on its
                        // own. A NON-cancel statement error (e.g. 23505) is NOT touched here and
                        // falls through to the ordinary `Completed` reply below, unchanged from
                        // pre-M1-S4 behavior (no auto-rollback).
                        if let Err(e) = &result
                            && fate::is_57014(e)
                        {
                            let _ = reply.send(ExecReply::Deadline);
                            break 'actor TxEnd::Deadline;
                        }
                        let _ = reply.send(ExecReply::Completed { result, exec_us });
                    }
                    ExecStep::Deadline => {
                        // (1) fire the out-of-band cancel; (2) DRAIN the pinned future to its
                        // now-erroring completion (do NOT drop it) so the conn is back at
                        // ReadyForQuery; (3) reply the ONE TxDeadline terminal; teardown rolls back.
                        //
                        // This same exit now also serves the per-statement `timeout_ms` and
                        // per-request `cancel` arms above: whichever of the three fired, the query
                        // has definitely been dispatched (biased query-first), so draining before
                        // replying is correct for all of them, including the raced-`Ok` case (the
                        // cancel/timeout LOST the race to completion) — §19.3 says the client asked
                        // to stop, so the safe uniform in-tx action is still roll back regardless of
                        // whether the drained value is `Ok` or `Err`.
                        cancel_handle.cancel().await;
                        let _ = query_fut.await;
                        let _ = reply.send(ExecReply::Deadline);
                        break 'actor TxEnd::Deadline;
                    }
                    ExecStep::Abort => {
                        cancel_handle.cancel().await;
                        let _ = query_fut.await;
                        // Drop the reply sender: the forwarding handler's recv returns `Err` and it
                        // declares its one prompt terminal — the request still ends in exactly one END.
                        drop(reply);
                        break 'actor TxEnd::Abort;
                    }
                }
            }
            TxCommand::ExecStreamed {
                sql,
                params,
                timeout_ms,
                readonly,
                cancel,
                responder,
                done,
            } => {
                // Combine the FOUR stop sources a tx-scoped stream must honor into the ONE cancel +
                // ONE deadline the shared producer takes:
                //  * cancel = a CHILD of the session `abort` (so it fires on session teardown) that a
                //    small linker task ALSO fires when THIS request's own `cancel` fires;
                //  * deadline = min(now + timeout_ms, the actor's ABSOLUTE max_tx deadline) — so a
                //    per-statement timeout AND the tx max-lifetime both bound the stream (open,
                //    mid-pull, and mid-backpressure alike). There is ALWAYS a deadline in a tx (the
                //    max_tx bound), unlike the autocommit path.
                let child = abort.child_token();
                let linker = {
                    let child = child.clone();
                    let req_cancel = cancel.clone();
                    tokio::spawn(async move {
                        req_cancel.cancelled().await;
                        child.cancel();
                    })
                };
                let max_instant = max_deadline.deadline();
                let deadline = Some(match timeout_ms {
                    Some(ms) => std::cmp::min(
                        tokio::time::Instant::now() + Duration::from_millis(u64::from(ms)),
                        max_instant,
                    ),
                    None => max_instant,
                });

                // Stream off the pinned `co` via the SHARED producer (`in_tx: true`). It declares the
                // ONE terminal itself (through the moved `responder`); we only learn whether the tx
                // survives.
                let ended = crate::services::sql::run_tx_streamed(
                    &mut co, &sql, &params, responder, &child, deadline, readonly,
                )
                .await;

                // The request-cancel linker is no longer needed — stop it (no leak if it never fired).
                linker.abort();
                // Let the forwarding handler RETURN; the supervisor then delivers the already-declared
                // terminal, strictly AFTER the last DATA the producer enqueued (B4).
                let _ = done.send(());

                match ended {
                    // The statement reached its own conclusion (clean drain, or a non-cancel error the
                    // client may still `ROLLBACK`): keep the tx OPEN, exactly like the buffered 23505
                    // path. Fall through to reset the idle deadline and process the next command.
                    StreamEnded::Intact => {}
                    // §19.3 uniform in-tx action: the tx is dead. Preserve the S4 abort-vs-deadline
                    // DISTINCTION — a session `abort` deregisters (no fate: `TxEnd::Abort`), a request
                    // cancel / per-statement timeout / max-deadline TOMBSTONEs (`TxEnd::Deadline` →
                    // `TxDeadline{Retryable}`). The terminal is already TxDeadline (the producer's
                    // in-tx classify_fate), so the abort case yields a TxDeadline terminal + a
                    // deregister teardown; both are single-terminal, single-teardown (the session is
                    // dying either way — `abort_session`'s final purge cleans a deregistered OR
                    // tombstoned entry identically).
                    StreamEnded::Broken => {
                        if abort.is_cancelled() {
                            break 'actor TxEnd::Abort;
                        }
                        break 'actor TxEnd::Deadline;
                    }
                }
            }
        }

        // A non-terminal command was processed: reset the idle deadline (it measures the idle gap
        // BETWEEN statements, so a long statement does not consume its budget).
        idle_deadline
            .as_mut()
            .reset(tokio::time::Instant::now() + idle_timeout);
    };

    teardown(tx_id, co, end, &registry, teardown_timeout).await;
    // Drain any commands still BUFFERED in `cmd_rx` at teardown (the outer `select!` can `break` on a
    // teardown signal with commands received-but-not-yet-pulled). A buffered `ExecStreamed` MOVED its
    // `Responder` into the command, so dropping `cmd_rx` would drop that `Responder` UNDECLARED — the
    // supervisor would then synthesize a generic `Protocol{NonRetryable}`, diverging from the buffered
    // `Exec` sibling (whose handler recovers `actor_gone_terminal` = `TxDeadline{Retryable}` for the
    // torn-down tx). So declare the PRECISE tear-down terminal on each such `Responder` here.
    drain_buffered_on_teardown(&mut cmd_rx, end);
    // Signal LAST — after the conn is back in the pool — so an `abort_session` awaiter that sees
    // `done` can rely on the connection already being released.
    let _ = done_tx.send(true);
}

/// After the actor's loop has broken and the tx is torn down, CLOSE the command channel — so any
/// LATE forwarding handler send now fails cleanly and that handler recovers its OWN terminal
/// (`services::sql`'s send-Err arms) — then DRAIN whatever is already buffered.
///
/// A buffered [`TxCommand::ExecStreamed`] carries a MOVED [`Responder`]; if it just dropped here the
/// supervisor would synthesize a generic `Protocol{NonRetryable}` terminal, diverging from the
/// buffered [`TxCommand::Exec`] sibling (whose handler recovers `actor_gone_terminal` =
/// `TxDeadline{Retryable}` for a torn-down tx). So we declare the precise tear-down terminal on the
/// moved `Responder` and signal its `done` ack (unblocking a handler still parked on `done_rx`):
///  * `Deadline`/`Abort` — the tx is dead (timed out / session death); the queued statement never
///    ran, so its fate is the KNOWN "the tx will never commit" → `TxDeadline{Retryable}` (matching
///    the buffered sibling's Retryable outcome, and consistent with an in-flight streamed abort).
///  * `Ended` — a clean COMMIT/ROLLBACK already ended the tx; a stream queued on the now-closed
///    tx_id is a client-protocol error → `Protocol`, exactly the buffered sibling's outcome.
///
/// Buffered `Exec`/control commands are simply dropped: their handlers recover the correct fate
/// themselves via their own reply-channel `Err` (`actor_gone_terminal`), unchanged.
fn drain_buffered_on_teardown(cmd_rx: &mut mpsc::Receiver<TxCommand>, end: TxEnd) {
    cmd_rx.close();
    while let Ok(cmd) = cmd_rx.try_recv() {
        if let TxCommand::ExecStreamed {
            responder, done, ..
        } = cmd
        {
            let ep = match end {
                TxEnd::Deadline | TxEnd::Abort => crate::services::sql::tx_deadline(
                    "transaction torn down before this queued streamed statement ran \
                     (retryable — the engine never re-runs)",
                ),
                TxEnd::Ended => crate::services::sql::protocol(
                    "transaction already ended before this queued streamed statement ran",
                ),
            };
            responder.end_error(ep);
            let _ = done.send(());
        }
    }
}

/// Map a pool tx-control `Result` to the reply the forwarding handler declares from.
fn ctl_reply(r: Result<(), PoolError>) -> CtlReply {
    match r {
        Ok(()) => CtlReply::Ok,
        Err(e) => CtlReply::Err(e),
    }
}

/// `Some(ms)` → a real `tokio::time::sleep` deadline for one statement; `None` → a future that
/// NEVER resolves (NOT a 0ms timer), so the `TxCommand::Exec` select's per-statement timeout arm is
/// effectively absent when the caller passed no `timeout_ms` — mirrors `services::sql`'s identical
/// `sleep_opt` (M1-S4 Task 2) exactly, so a `timeout_ms: None` tx-scoped statement behaves exactly
/// as it did before M1-S4 Task 3.
async fn sleep_opt(ms: Option<u32>) {
    match ms {
        Some(ms) => tokio::time::sleep(Duration::from_millis(u64::from(ms))).await,
        None => std::future::pending().await,
    }
}

/// Tear the transaction down: for an abort/deadline, roll back the pinned conn (or taint it if the
/// rollback errors, so the pool's recycle path resets/evicts it rather than handing out a dirty
/// conn); then drop `co` (RAII → conn to pool) and update the registry — TOMBSTONE on a deadline
/// (`TxDeadline`), otherwise DEREGISTER.
///
/// The teardown ROLLBACK is BOUNDED by `teardown_timeout` (S6 hardening): a wedged upstream must not
/// keep the actor holding the pinned conn + its pool permit until an OS TCP timeout. On timeout OR
/// error the conn is TAINTED and dropped, and the pool's own (bounded) recycle-at-next-checkout then
/// resets or evicts it — symmetric with `Pool::checkout`'s bounded recycle.
async fn teardown<B: PoolBackend>(
    tx_id: u64,
    mut co: Checkout<B>,
    end: TxEnd,
    registry: &TxRegistry,
    teardown_timeout: Duration,
) {
    match end {
        // COMMIT/ROLLBACK already ran; nothing more to do to the conn.
        TxEnd::Ended => {}
        TxEnd::Abort | TxEnd::Deadline => {
            // A clean rollback within the bound leaves the conn reusable; a timeout OR an error
            // taints it (tx still open on the wire) so the next checkout's recycle handles it.
            //
            // M1-S1: `co.rollback_tx()` itself now reads the real RFQ status and applies
            // `apply_tx_status`, so on its `Err` arm it ALREADY forces `tx_open`/`tainted`
            // unconditionally (the Rule-A fail-safe in `ferro-pool`) — this `set_tainted(true)` below
            // is belt-and-braces (defense-in-depth) for that case, not the mechanism catching it.
            // It stays load-bearing for the OTHER case: a `teardown_timeout` firing drops the
            // in-flight `rollback_tx()` future mid-await, before it can observe/apply anything, so
            // this is the ONLY thing that taints the conn when the teardown ROLLBACK itself wedges.
            let rolled_back = matches!(
                tokio::time::timeout(teardown_timeout, co.rollback_tx()).await,
                Ok(Ok(()))
            );
            if !rolled_back {
                co.set_tainted(true);
            }
        }
    }
    drop(co);

    match end {
        TxEnd::Deadline => registry.tombstone(tx_id),
        TxEnd::Ended | TxEnd::Abort => registry.deregister(tx_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::SessionId;
    use crate::session::codec::ControlMsg;
    use crate::session::flow::{Credit, CreditCell, SessionCap};
    use crate::session::responder::{Responder, Terminal};
    use crate::tx::{TxHandle, TxLookupErr, next_tx_id};

    use ferro_pool::config::PoolConfig;
    use ferro_pool::fake::{FakeBackend, StreamScript};
    use ferro_pool::pin::{PinCause, PinState, TxId};
    use ferro_pool::pool::Pool;
    use ferro_proto::consts::{branch, errc, method_stream, service, tag};
    use ferro_proto::messages::StreamData;
    use ferro_proto::messages::sql::{ColMeta, ExecOk};
    use ferro_proto::value::Value;
    use std::sync::Arc;
    use tokio::sync::oneshot;

    /// The wire `request_id` the streamed-tx tests stamp on their HEAD/DATA frames.
    const STREAM_REQ_ID: u32 = 91;

    /// Build a stream-capable [`Responder`] wired to a drainable control channel, mirroring what the
    /// session layer hands a `fetch:stream` handler. Returns the responder, its terminal cell, and
    /// the receiver end of the control channel (all HEAD/DATA frames land there). The local
    /// `control_tx` is dropped so the channel CLOSES once the actor consumes the responder — a test's
    /// `control_rx.recv()` drain then terminates instead of hanging.
    fn streaming_responder(
        credit_frames: u32,
    ) -> (
        Responder,
        Arc<std::sync::Mutex<Option<Terminal>>>,
        mpsc::Receiver<ControlMsg>,
    ) {
        let (control_tx, control_rx) = mpsc::channel::<ControlMsg>(64);
        let credit_cell = Arc::new(CreditCell::new(Credit::new(credit_frames, u32::MAX)));
        let session_cap = Arc::new(SessionCap::new(1_000_000));
        let (responder, cell) =
            Responder::new_streaming(STREAM_REQ_ID, credit_cell, session_cap, control_tx);
        (responder, cell, control_rx)
    }

    // ---- pure helpers -----------------------------------------------------------------------

    /// The full (dialect × isolation × readonly) matrix, verbatim. The 8 PostgreSQL strings are
    /// UNCHANGED from M1-S6 — M1-S8a Task 8 must not move them.
    #[test]
    fn compose_begin_sql_table() {
        use ferro_pool::backend::Dialect;
        let iso_rc = Some(u8::from(Isolation::ReadCommitted));
        let iso_rr = Some(u8::from(Isolation::RepeatableRead));
        let iso_ser = Some(u8::from(Isolation::Serializable));

        // --- PostgreSQL: unchanged.
        let pg: &[(Option<u8>, bool, &str)] = &[
            (None, false, "BEGIN"),
            (None, true, "BEGIN READ ONLY"),
            (iso_rc, false, "BEGIN ISOLATION LEVEL READ COMMITTED"),
            (
                iso_rc,
                true,
                "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY",
            ),
            (iso_rr, false, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
            (
                iso_rr,
                true,
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            ),
            (iso_ser, false, "BEGIN ISOLATION LEVEL SERIALIZABLE"),
            (
                iso_ser,
                true,
                "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY",
            ),
        ];
        for (iso, ro, want) in pg {
            assert_eq!(
                compose_begin_sql(Dialect::Postgres, *iso, *ro).unwrap(),
                *want,
                "pg({iso:?}, {ro})"
            );
        }

        // --- MySQL/MariaDB: `BEGIN READ ONLY` and `BEGIN ISOLATION LEVEL …` are ERROR 1064 on BOTH
        // engines (measured), and so is `START TRANSACTION ISOLATION LEVEL …`. The isolation forms
        // are therefore a `SET TRANSACTION …;` prefix in the SAME statement string — ONE
        // `simple_query`, which is all `begin_tx_with` issues, and the only form that does not taint.
        let my: &[(Option<u8>, bool, &str)] = &[
            (None, false, "START TRANSACTION"),
            (None, true, "START TRANSACTION READ ONLY"),
            (
                iso_rc,
                false,
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION",
            ),
            (
                iso_rc,
                true,
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION READ ONLY",
            ),
            (
                iso_rr,
                false,
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION",
            ),
            (
                iso_rr,
                true,
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION READ ONLY",
            ),
            (
                iso_ser,
                false,
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION",
            ),
            (
                iso_ser,
                true,
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; START TRANSACTION READ ONLY",
            ),
        ];
        for (iso, ro, want) in my {
            assert_eq!(
                compose_begin_sql(Dialect::MySql, *iso, *ro).unwrap(),
                *want,
                "mysql({iso:?}, {ro})"
            );
        }

        // The SESSION form is FORBIDDEN (it would persist the level on the pooled connection past
        // COMMIT — a cross-tenant leak, charter rule 6). Asserted over the WHOLE composed matrix,
        // not a spot check, so no future cell can reintroduce it.
        for (iso, ro, _) in pg.iter().chain(my.iter()) {
            for d in [Dialect::Postgres, Dialect::MySql] {
                let sql = compose_begin_sql(d, *iso, *ro).unwrap();
                assert!(
                    !sql.to_ascii_uppercase().contains("SET SESSION"),
                    "{d:?}({iso:?}, {ro}) must never emit a SESSION-scoped isolation: {sql:?}"
                );
                assert!(
                    !sql.to_ascii_uppercase().contains("GLOBAL"),
                    "{d:?}({iso:?}, {ro}) must never emit a GLOBAL-scoped isolation: {sql:?}"
                );
            }
        }

        // --- SQLite: no backend exists yet. A bare BEGIN is composable; anything else is a LOUD
        // refusal rather than a silently-PG-shaped string a future backend would choke on.
        assert_eq!(
            compose_begin_sql(Dialect::Sqlite, None, false).unwrap(),
            "BEGIN"
        );
        assert!(compose_begin_sql(Dialect::Sqlite, None, true).is_err());
        assert!(compose_begin_sql(Dialect::Sqlite, iso_ser, false).is_err());

        // An unknown isolation byte is a client error on EVERY dialect, never coerced to a default.
        for d in [Dialect::Postgres, Dialect::MySql, Dialect::Sqlite] {
            assert!(compose_begin_sql(d, Some(3), false).is_err(), "{d:?}");
            assert!(compose_begin_sql(d, Some(99), false).is_err(), "{d:?}");
        }
    }

    #[test]
    fn savepoint_stack_push_release_rollback_to_transitions() {
        let mut sp = SavepointStack::new();
        // Engine names are sp_1, sp_2, sp_3, monotonic.
        assert_eq!(sp.push(Some("a".into())), "sp_1");
        assert_eq!(sp.push(None), "sp_2");
        assert_eq!(sp.push(Some("c".into())), "sp_3");
        assert_eq!(sp.depth(), 3);

        // Resolve by client alias, by engine name, and top-of-stack (None).
        assert_eq!(sp.resolve(Some("a")), Some((0, "sp_1".into())));
        assert_eq!(sp.resolve(Some("sp_2")), Some((1, "sp_2".into())));
        assert_eq!(sp.resolve(None), Some((2, "sp_3".into())));
        assert_eq!(sp.resolve(Some("nope")), None);

        // ROLLBACK TO sp_2 keeps sp_1 + sp_2, drops sp_3.
        let (idx, engine) = sp.resolve(Some("sp_2")).unwrap();
        assert_eq!(engine, "sp_2");
        sp.truncate_rollback_to(idx);
        assert_eq!(sp.depth(), 2);
        assert_eq!(sp.resolve(Some("c")), None, "sp_3 destroyed by rollback-to");

        // RELEASE "a" (sp_1) destroys sp_1 AND everything after it (sp_2).
        let (idx, _) = sp.resolve(Some("a")).unwrap();
        sp.truncate_release(idx);
        assert_eq!(sp.depth(), 0);
        assert_eq!(sp.resolve(None), None, "empty stack resolves nothing");
    }

    // ---- FakeBackend actor integration (deterministic, no Postgres) --------------------------

    fn test_pool_config() -> PoolConfig {
        PoolConfig {
            max_size: 1, // one conn: a fresh checkout after teardown reuses (and inspects) it
            checkout_timeout: Duration::from_secs(5),
            max_lifetime: Duration::from_secs(3600),
            reap_interval: None, // deterministic: no background reaper
            ..PoolConfig::default()
        }
    }

    /// Check out a FakeBackend conn, BEGIN a tx on it, spawn the actor MOVING the checkout in, and
    /// register it — mirroring exactly what the BEGIN handler does. Returns the pieces a test drives.
    async fn spawn_actor(
        pool: &Pool<FakeBackend>,
        registry: &TxRegistry,
        owner: SessionId,
        idle: Duration,
        max: Duration,
    ) -> (u64, mpsc::Sender<TxCommand>, watch::Receiver<bool>) {
        let mut co = pool.checkout().await.expect("checkout");
        let tx_id = next_tx_id();
        co.begin_tx_with(TxId(tx_id), "BEGIN").await.expect("begin");
        // Pin-cause DoD assertion: a pinned tx conn reports PinnedTx(tx_id) / PinCause::Tx.
        assert_eq!(co.pin_state(), PinState::PinnedTx(TxId(tx_id)));
        assert_eq!(co.last_pin_cause(), Some(PinCause::Tx));

        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (done_tx, done_rx) = watch::channel(false);
        let abort = CancellationToken::new();
        registry.register(
            tx_id,
            TxHandle {
                owner,
                cmd_tx: cmd_tx.clone(),
                abort: abort.clone(),
                done: done_rx.clone(),
                // Derived from the ONE authority, exactly as `begin_on_pool` does (FakeBackend
                // inherits the `true` default) — never a literal restated here.
                streaming: pool.backend().supports_row_streaming(),
            },
        );
        tokio::spawn(run(
            tx_id,
            co,
            cmd_rx,
            abort,
            done_tx,
            registry.clone(),
            idle,
            max,
            // A generous teardown bound: these tests never block the teardown ROLLBACK, so it always
            // completes well inside this. The bounded-teardown behaviour has its own test below.
            Duration::from_secs(600),
        ));
        (tx_id, cmd_tx, done_rx)
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_mid_statement_cancels_rolls_back_and_tombstones() {
        let backend = FakeBackend::new();
        backend.block_query(); // freeze the tx-scoped statement mid-flight
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();

        // A SHORT absolute max_tx so the timer fires while the statement is blocked.
        let (tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_millis(50),
        )
        .await;

        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "SELECT pg_sleep(9)".into(),
                params: vec![],
                timeout_ms: None,
                cancel: CancellationToken::new(),
                reply: reply_tx,
            })
            .await
            .expect("send exec");

        // The blocked statement + the fired max timer → the out-of-band cancel path → ONE terminal.
        let reply = reply_rx
            .await
            .expect("the actor replies, never drops silently");
        assert!(
            matches!(reply, ExecReply::Deadline),
            "a mid-statement deadline yields exactly one TxDeadline terminal, got {reply:?}"
        );

        // The tx tombstoned as TxDeadline (owner sees it; anyone else sees NotFoundOrForbidden).
        done_rx.wait_for(|t| *t).await.expect("actor tears down");
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::Tombstoned
        );

        // The pinned conn was rolled back + released (permit freed → a fresh checkout succeeds), and
        // the statement ran EXACTLY ONCE — never re-dispatched (charter rule 3).
        let co2 = pool
            .checkout()
            .await
            .expect("permit released, conn returned");
        let recorded = co2.conn().recorded.clone();
        assert_eq!(
            recorded.iter().filter(|s| s.contains("pg_sleep")).count(),
            1,
            "statement transmitted exactly once, never re-run: {recorded:?}"
        );
        assert!(
            recorded.contains(&"ROLLBACK".to_string()),
            "the tx was rolled back on the deadline: {recorded:?}"
        );
    }

    // ---- M1-S4 Task 3: per-statement timeout_ms + per-request CANCEL on the tx-scoped path -----

    /// A per-statement `ExecRequest.timeout_ms` (NOT the actor's own absolute `max_tx`) elapses
    /// mid-statement: the NEW `sleep_opt(timeout_ms)` select arm fires, taking the exact same
    /// cancel→drain→ROLLBACK→tombstone→`TxDeadline` exit `max_deadline` already used. `max_tx`/
    /// `idle` are both deliberately LONG here, so only Task 3's new arm can be what fires.
    #[tokio::test(start_paused = true)]
    async fn per_statement_timeout_ms_cancels_rolls_back_and_tombstones() {
        let backend = FakeBackend::new();
        backend.block_query(); // freeze the tx-scoped statement mid-flight
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();

        let (tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "SELECT pg_sleep(9)".into(),
                params: vec![],
                timeout_ms: Some(50),
                cancel: CancellationToken::new(), // never fired — the per-statement TIMER must fire
                reply: reply_tx,
            })
            .await
            .expect("send exec");

        let reply = reply_rx
            .await
            .expect("the actor replies, never drops silently");
        assert!(
            matches!(reply, ExecReply::Deadline),
            "a per-statement timeout_ms yields exactly one TxDeadline terminal, got {reply:?}"
        );

        done_rx.wait_for(|t| *t).await.expect("actor tears down");
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::Tombstoned
        );

        let co2 = pool
            .checkout()
            .await
            .expect("permit released, conn returned");
        let recorded = co2.conn().recorded.clone();
        assert_eq!(
            recorded.iter().filter(|s| s.contains("pg_sleep")).count(),
            1,
            "statement transmitted exactly once, never re-run: {recorded:?}"
        );
        assert!(
            recorded.contains(&"ROLLBACK".to_string()),
            "the tx was rolled back on the per-statement timeout: {recorded:?}"
        );
    }

    /// A per-REQUEST CANCEL (the `TxCommand::Exec::cancel` token, NOT the actor's `abort` and NOT a
    /// timer) races an in-flight tx-scoped statement: fired only once the statement is PROVABLY in
    /// flight (parked on the query gate), proving the cancel ARM itself — not a lucky pre-dispatch
    /// race — is what unblocks it. Same rollback+tombstone+TxDeadline exit as a deadline.
    #[tokio::test]
    async fn per_request_cancel_races_in_flight_stmt_rolls_back_and_tombstones() {
        let backend = FakeBackend::new();
        backend.block_query();
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();

        let (tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        let cancel = CancellationToken::new();
        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "SELECT pg_sleep(9)".into(),
                params: vec![],
                timeout_ms: None,
                cancel: cancel.clone(),
                reply: reply_tx,
            })
            .await
            .expect("send exec");

        // Wait until the statement is actually in flight (parked on the query gate) before firing
        // the per-request CANCEL — a cancel that fires before dispatch is a different (pre-dispatch)
        // case; this proves the in-flight cancel arm specifically.
        while pool.backend().queries_waiting() == 0 {
            tokio::task::yield_now().await;
        }
        cancel.cancel();

        let reply = reply_rx
            .await
            .expect("the actor replies, never drops silently");
        assert!(
            matches!(reply, ExecReply::Deadline),
            "a per-request CANCEL of an in-flight tx statement yields TxDeadline, got {reply:?}"
        );

        done_rx.wait_for(|t| *t).await.expect("actor tears down");
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::Tombstoned
        );

        let co2 = pool.checkout().await.expect("conn released");
        assert!(
            co2.conn().recorded.contains(&"ROLLBACK".to_string()),
            "the tx was rolled back on the per-request cancel: {:?}",
            co2.conn().recorded
        );
    }

    /// SPEC §22.2 COLLISION REGRESSION (reviewer round 2): the round-1 version of this test did
    /// NOT genuinely contend the two signals — it fully drained the cancel arm's reply AND awaited
    /// `done_rx` (i.e. waited for `teardown()`, which runs `registry.tombstone(tx_id)` strictly
    /// before `done_tx.send(true)`) BEFORE ever calling `abort_session`. By then the entry was
    /// already `Tombstoned`, so `abort_session`'s handle-collection filter (`TxEntry::Active` only)
    /// never matched this tx, `handle.abort.cancel()` was never fired on it, and the actor's inner
    /// `select!` never had to arbitrate `abort.cancelled()` vs `cancel.cancelled()` both ready —
    /// signal #2 was a structural no-op, proving only things already covered elsewhere.
    ///
    /// This version puts BOTH tokens genuinely live at once: `cancel.cancel()` fires, then
    /// `registry.abort_session(owner)` is called with NO intervening `.await` on THIS task — so
    /// `abort_session`'s handle-snapshot (a synchronous, non-yielding `std::sync::Mutex`-guarded
    /// scan) still sees the tx `Active` (the actor's task cannot have run in between: a
    /// `#[tokio::test]` defaults to tokio's single-threaded `current_thread` flavor, which only
    /// switches tasks at an actual `.await` yield point, and there is none between our
    /// `cancel.cancel()` and `abort_session`'s own synchronous `handle.abort.cancel()` loop inside
    /// it) — so `abort_session` fires `abort` on this SAME tx too, before the actor's task has been
    /// polled even once since `cancel` fired. By the time the actor IS next polled, its inner
    /// `select!` has BOTH `abort.cancelled()` and `cancel.cancelled()` ready simultaneously: a
    /// genuine collision, not a sequenced non-collision.
    ///
    /// Empirically confirmed (50 local instrumented runs, tracking whether `reply_rx` resolved
    /// `Ok(Deadline)` [cancel arm] or `Err` [abort arm]): `abort` won all 50/50 — consistent with
    /// `biased` always preferring the FIRST ready arm in source order (`abort.cancelled()` is
    /// listed before `cancel.cancelled()` in `actor.rs`'s select!, and both are already-ready by
    /// the very first poll after this test's two signals fire back-to-back with no yield between
    /// them), not with any actual non-determinism in the collision itself. That specific WINNER is
    /// therefore an implementation detail of arm ORDER, not a safety property this test should
    /// pin — so it deliberately asserts ONLY the invariants that must hold regardless of which arm
    /// wins: exactly one terminal for the request, the actor completes without hanging or
    /// panicking, exactly one `ROLLBACK` is recorded (teardown runs exactly once), the conn is
    /// released, and the registry ends up fully purged (never left `Active`).
    #[tokio::test]
    async fn cancel_and_abort_contend_same_inflight_stmt_tear_down_exactly_once() {
        let backend = FakeBackend::new();
        backend.block_query();
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();

        let (tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        let cancel = CancellationToken::new();
        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "SELECT pg_sleep(9)".into(),
                params: vec![],
                timeout_ms: None,
                cancel: cancel.clone(),
                reply: reply_tx,
            })
            .await
            .expect("send exec");

        while pool.backend().queries_waiting() == 0 {
            tokio::task::yield_now().await;
        }

        // Fire BOTH teardown signals before the actor's task gets a chance to react to EITHER:
        // `cancel.cancel()` (mirrors `session::registry::Registry::cancel_all()`), then
        // IMMEDIATELY `abort_session` (mirrors session death's subsequent, unconditional call) —
        // with no `.await` on this task in between, so `abort_session`'s synchronous
        // handle-snapshot still finds the tx `Active` and fires `abort` on it too. This is what
        // makes the collision GENUINE (see the doc comment above): both tokens are cancelled
        // before the actor's `select!` is ever polled again.
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), registry.abort_session(owner))
            .await
            .expect("abort_session must return promptly even genuinely racing an in-flight actor");

        // Whichever arm the actor's biased select picked, it has ALREADY run to completion by the
        // time `abort_session` above returned (its own internal `done.wait_for` blocks on exactly
        // that) — read the final state, asserting ONLY the invariants that hold either way, never
        // which arm won (see doc comment: that's an arm-order implementation detail, not a
        // property this test should pin).
        let reply_outcome = reply_rx.await;
        done_rx
            .wait_for(|t| *t)
            .await
            .expect("the actor completes teardown without hanging or panicking either way");

        // INVARIANT 1: exactly one terminal outcome for the original request. A oneshot channel
        // can only ever be fulfilled once, so "exactly one" holds structurally; here we also
        // confirm the ONLY two legitimate shapes appear — a declared `Deadline` reply (cancel arm
        // won) or a dropped sender (abort arm won, `Err`, same shape as the pre-existing
        // `session_death_mid_statement_drops_reply_exactly_once` proof) — never anything else.
        // `Err(_)` (the abort arm won: reply sender dropped) needs no further assertion here.
        if let Ok(reply) = reply_outcome {
            assert!(
                matches!(reply, ExecReply::Deadline),
                "if the cancel arm won, the ONLY legitimate reply is Deadline, got {reply:?}"
            );
        }

        // INVARIANT 2: the registry ends up fully purged (never left Active) regardless of which
        // arm won — `abort_session`'s own final cleanup purges a Tombstoned entry (cancel won)
        // just as completely as an already-deregistered one (abort won).
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden,
            "the tx must be fully gone from the registry after both signals resolve, whichever won"
        );

        // INVARIANT 3: the conn was rolled back EXACTLY once (teardown runs exactly once no
        // matter which arm won) and released — a fresh checkout on the size-1 pool succeeds (no
        // leaked permit).
        let co2 = pool
            .checkout()
            .await
            .expect("permit released, conn returned, despite both signals genuinely racing it");
        let recorded = co2.conn().recorded.clone();
        assert_eq!(
            recorded.iter().filter(|s| *s == "ROLLBACK").count(),
            1,
            "the tx is rolled back EXACTLY once despite two GENUINELY racing teardown signals: {recorded:?}"
        );
    }

    /// A BARE `57014` (an app-set `statement_timeout`) resolves through the statement's OWN
    /// completion (`ExecStep::Completed`) — no deadline/cancel select arm ever fires (no gate is
    /// armed; `arm_next_query_err` returns the error immediately). It must STILL take the same
    /// rollback+tombstone+TxDeadline exit as a genuine mid-statement deadline (§19.3: PG has
    /// already aborted the tx block on this error — a next statement would see 25P02 — so it must
    /// not be forwarded as a bare statement error the client might retry in place).
    #[tokio::test]
    async fn bare_57014_via_completed_rolls_back_and_tombstones() {
        let backend = FakeBackend::new();
        backend.arm_next_query_err(PoolError::Sql {
            code: 0,
            branch: 0,
            sqlstate: Some("57014".to_string()),
            errno: None,
            message: "canceling statement due to statement timeout".to_string(),
        });
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();

        let (tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "UPDATE t SET n = n + 1".into(),
                params: vec![],
                timeout_ms: None,
                cancel: CancellationToken::new(),
                reply: reply_tx,
            })
            .await
            .expect("send exec");

        let reply = reply_rx
            .await
            .expect("the actor replies, never drops silently");
        assert!(
            matches!(reply, ExecReply::Deadline),
            "a bare 57014 resolving via ExecStep::Completed must STILL yield TxDeadline \
             (rolled back), never a bare Completed error, got {reply:?}"
        );

        done_rx.wait_for(|t| *t).await.expect("actor tears down");
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::Tombstoned
        );

        let co2 = pool.checkout().await.expect("conn released");
        assert!(
            co2.conn().recorded.contains(&"ROLLBACK".to_string()),
            "a bare 57014 must still roll the tx back: {:?}",
            co2.conn().recorded
        );
    }

    /// S6 REGRESSION GUARD: a NON-cancel statement error (23505, a genuine constraint violation)
    /// resolving via `ExecStep::Completed` must NOT be treated like a 57014 — it is reported to the
    /// client as an ordinary `ExecReply::Completed { result: Err(..), .. }` WITHOUT any auto-
    /// rollback, exactly as before M1-S4 Task 3: the actor keeps running (no `break`), the tx stays
    /// Active, and a subsequent client-driven COMMIT on the SAME tx_id still works.
    #[tokio::test]
    async fn non_cancel_statement_error_reported_without_auto_rollback() {
        let backend = FakeBackend::new();
        backend.arm_next_query_err(PoolError::Sql {
            code: 0,
            branch: 0,
            sqlstate: Some("23505".to_string()),
            errno: None,
            message: "duplicate key value violates unique constraint".to_string(),
        });
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();

        let (_tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "INSERT INTO t (id) VALUES (1)".into(),
                params: vec![],
                timeout_ms: None,
                cancel: CancellationToken::new(),
                reply: reply_tx,
            })
            .await
            .expect("send exec");

        match reply_rx.await.expect("the actor replies") {
            ExecReply::Completed { result, .. } => {
                let e = result.expect_err("a constraint violation must surface as an Err");
                assert!(
                    matches!(&e, PoolError::Sql { sqlstate, .. } if sqlstate.as_deref() == Some("23505")),
                    "expected the 23505 error to pass through verbatim, got {e:?}"
                );
            }
            other => panic!(
                "a NON-cancel statement error must reply Completed, never Deadline: {other:?}"
            ),
        }

        // No auto-rollback: the tx is still Active (not tombstoned) and a client-driven COMMIT on
        // the SAME tx_id still works — proving the actor never broke out of its loop on this error.
        let (commit_tx, commit_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Commit { reply: commit_tx })
            .await
            .expect("send commit");
        assert!(
            matches!(commit_rx.await.expect("commit replies"), CtlReply::Ok),
            "the tx survived the 23505 and a subsequent COMMIT still succeeds"
        );
        done_rx
            .wait_for(|t| *t)
            .await
            .expect("actor ends after commit");

        let co2 = pool.checkout().await.expect("conn released after commit");
        assert!(
            !co2.conn().recorded.contains(&"ROLLBACK".to_string()),
            "a non-cancel statement error must NOT auto-rollback: {:?}",
            co2.conn().recorded
        );
        assert!(co2.conn().recorded.contains(&"COMMIT".to_string()));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_deadline_tombstones_with_no_statement_in_flight() {
        let backend = FakeBackend::new();
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();

        // A SHORT idle deadline, a long max: with no command ever sent, the idle timer fires.
        let (tx_id, _cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_millis(20),
            Duration::from_secs(600),
        )
        .await;

        done_rx
            .wait_for(|t| *t)
            .await
            .expect("idle deadline tears the tx down");
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::Tombstoned
        );
        let co2 = pool.checkout().await.expect("conn released");
        assert!(co2.conn().recorded.contains(&"ROLLBACK".to_string()));
    }

    #[tokio::test]
    async fn session_death_aborts_actor_and_returns_a_clean_conn() {
        let backend = FakeBackend::new();
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();

        let (tx_id, _cmd_tx, _done) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        // Session death: abort_session fires the actor's abort token and awaits its teardown.
        registry.abort_session(owner).await;

        // Deregistered (a session death is not a deadline → NOT tombstoned).
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden
        );
        // The conn was rolled back + released: a fresh checkout succeeds (no leaked permit) + clean.
        let co2 = pool
            .checkout()
            .await
            .expect("conn released, permit not leaked");
        assert!(
            co2.conn().recorded.contains(&"ROLLBACK".to_string()),
            "the actor rolled back on abort: {:?}",
            co2.conn().recorded
        );
    }

    #[tokio::test(start_paused = true)]
    async fn teardown_rollback_timeout_taints_and_releases_the_conn() {
        // A wedged upstream must not keep the actor holding the pinned conn + its pool permit until
        // an OS TCP timeout: the teardown ROLLBACK is bounded, and on timeout the conn is tainted +
        // dropped so the pool's recycle (also bounded) resets/evicts it at the next checkout.
        let backend = FakeBackend::new();
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();

        // Open a tx on the size-1 pool's only conn (BEGIN runs before the gate is armed).
        let mut co = pool.checkout().await.expect("checkout");
        let tx_id = next_tx_id();
        co.begin_tx_with(TxId(tx_id), "BEGIN").await.expect("begin");

        // Freeze every SUBSEQUENT simple_query so the teardown ROLLBACK hangs.
        pool.backend().block_simple_query();

        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (done_tx, done_rx) = watch::channel(false);
        let abort = CancellationToken::new();
        registry.register(
            tx_id,
            TxHandle {
                owner,
                cmd_tx,
                abort: abort.clone(),
                done: done_rx,
                // Derived from the ONE authority, exactly as `begin_on_pool` does.
                streaming: pool.backend().supports_row_streaming(),
            },
        );
        // A SHORT teardown bound: the blocked ROLLBACK is abandoned at 50ms, not held forever.
        tokio::spawn(run(
            tx_id,
            co,
            cmd_rx,
            abort,
            done_tx,
            registry.clone(),
            Duration::from_secs(600),
            Duration::from_secs(600),
            Duration::from_millis(50),
        ));

        // Session death aborts the actor. Despite the wedged ROLLBACK, `abort_session` returns
        // PROMPTLY (bounded by the teardown timeout, well inside the registry's abort deadline) —
        // the actor taints, releases the conn + permit, and signals done rather than pinning them.
        registry.abort_session(owner).await;
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden,
            "an abort deregisters (never tombstones)"
        );

        // Un-freeze so the next checkout's recycle can complete, then prove the permit was NOT
        // leaked behind the wedged teardown: a fresh checkout on the size-1 pool succeeds, and the
        // recycled conn shows a hygiene RESET (it was TAINTED because the teardown ROLLBACK timed out).
        pool.backend().release_simple_query();
        let co2 = pool
            .checkout()
            .await
            .expect("permit released despite the wedged teardown ROLLBACK");
        assert!(
            co2.conn().recorded.contains(&"RESET:Full".to_string()),
            "a timed-out teardown taints the conn → the recycle resets it: {:?}",
            co2.conn().recorded
        );
    }

    #[tokio::test]
    async fn session_death_mid_statement_drops_reply_exactly_once() {
        let backend = FakeBackend::new();
        backend.block_query();
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();

        let (_tx_id, cmd_tx, _done) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        let (reply_tx, reply_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "SELECT 1".into(),
                params: vec![],
                timeout_ms: None,
                cancel: CancellationToken::new(),
                reply: reply_tx,
            })
            .await
            .expect("send exec");

        // Wait until the statement is actually in flight (parked on the query gate).
        while pool.backend().queries_waiting() == 0 {
            tokio::task::yield_now().await;
        }

        // Session death mid-statement: cancel + drain + drop the reply sender + rollback + release.
        registry.abort_session(owner).await;

        // The in-flight request's reply resolves EXACTLY ONCE — a recv Err (sender dropped), which
        // drives the forwarding handler's single prompt terminal (never a hang, never two ENDs).
        assert!(
            reply_rx.await.is_err(),
            "the in-flight Exec reply sender is dropped exactly once on abort"
        );
        let co2 = pool.checkout().await.expect("conn released");
        assert!(co2.conn().recorded.contains(&"ROLLBACK".to_string()));
    }

    #[tokio::test]
    async fn cross_session_reject_leaves_owner_actor_usable() {
        let backend = FakeBackend::new();
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let session_a = registry.next_session_id();
        let session_b = registry.next_session_id();

        let (tx_id, _cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            session_a,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        // Session B using A's tx_id → NotFoundOrForbidden (indistinguishable from unknown).
        assert_eq!(
            registry.lookup(tx_id, session_b).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden
        );

        // A's actor is undisturbed: a COMMIT from A still works and ends the tx cleanly.
        let handle = registry
            .lookup(tx_id, session_a)
            .expect("A still owns its tx");
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .cmd_tx
            .send(TxCommand::Commit { reply: reply_tx })
            .await
            .expect("send commit");
        assert!(matches!(
            reply_rx.await.expect("commit replies"),
            CtlReply::Ok
        ));
        done_rx
            .wait_for(|t| *t)
            .await
            .expect("actor ends after commit");
        assert_eq!(
            registry.lookup(tx_id, session_a).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden
        );

        let co2 = pool.checkout().await.expect("conn released after commit");
        assert!(co2.conn().recorded.contains(&"COMMIT".to_string()));
    }

    #[tokio::test]
    async fn savepoint_release_rollback_to_run_engine_named_only() {
        let backend = FakeBackend::new();
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();

        let (_tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        // SAVEPOINT with client alias "a" → engine composes sp_1.
        let (r1_tx, r1_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Savepoint {
                name: Some("a".into()),
                reply: r1_tx,
            })
            .await
            .unwrap();
        assert!(matches!(r1_rx.await.unwrap(), CtlReply::Ok));

        // ROLLBACK TO "a" → ROLLBACK TO sp_1.
        let (r2_tx, r2_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::RollbackTo {
                name: Some("a".into()),
                reply: r2_tx,
            })
            .await
            .unwrap();
        assert!(matches!(r2_rx.await.unwrap(), CtlReply::Ok));

        // RELEASE an unknown name → UnknownSavepoint (→ Protocol), backend never touched for it.
        let (r3_tx, r3_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Release {
                name: Some("nope".into()),
                reply: r3_tx,
            })
            .await
            .unwrap();
        assert!(matches!(r3_rx.await.unwrap(), CtlReply::UnknownSavepoint));

        // COMMIT ends it.
        let (r4_tx, r4_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Commit { reply: r4_tx })
            .await
            .unwrap();
        assert!(matches!(r4_rx.await.unwrap(), CtlReply::Ok));
        done_rx.wait_for(|t| *t).await.unwrap();

        let co2 = pool.checkout().await.unwrap();
        let rec = co2.conn().recorded.clone();
        assert!(
            rec.contains(&"SAVEPOINT sp_1".to_string()),
            "engine-composed savepoint name reaches the backend: {rec:?}"
        );
        assert!(
            rec.contains(&"ROLLBACK TO sp_1".to_string()),
            "engine-composed rollback-to reaches the backend: {rec:?}"
        );
        assert!(
            !rec.iter().any(|s| s.contains("nope")),
            "a client savepoint name NEVER reaches the backend SQL: {rec:?}"
        );
        assert!(rec.contains(&"COMMIT".to_string()));
    }

    // ---- M1-S5 Task 5: tx-scoped fetch:stream from the pinned conn -----------------------------

    fn stream_script(rows: Vec<Vec<Value>>, affected: u64) -> StreamScript {
        StreamScript {
            cols: vec![ColMeta {
                name: "n".into(),
                tag: tag::I64,
            }],
            rows,
            affected,
            error_at: None,
        }
    }

    /// A tx-scoped `fetch:stream` streams HEAD + DATA off the pinned conn, declares exactly ONE `Ok`
    /// terminal (carrying the streamed `affected`/row count), and — the stream being INTACT — leaves
    /// the transaction OPEN so a following COMMIT still succeeds. Proves the whole Task-5 wiring plus
    /// the `StreamEnded::Intact` "keep the tx open" branch.
    #[tokio::test]
    async fn tx_scoped_stream_streams_head_data_then_ok_and_stays_open_to_commit() {
        let backend = FakeBackend::new();
        backend.set_stream_script(stream_script(
            vec![
                vec![Value::I64(1)],
                vec![Value::I64(2)],
                vec![Value::I64(3)],
            ],
            3,
        ));
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        let (tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        // Generous credit so nothing parks: the whole stream flows to a clean end.
        let (responder, cell, mut control_rx) = streaming_responder(1000);
        let (ack_tx, ack_rx) = oneshot::channel::<()>();
        cmd_tx
            .send(TxCommand::ExecStreamed {
                sql: "SELECT n".into(),
                params: vec![],
                timeout_ms: None,
                readonly: true,
                cancel: CancellationToken::new(),
                responder,
                done: ack_tx,
            })
            .await
            .expect("send exec-streamed");
        ack_rx.await.expect("the actor acks the streamed exec");

        // Drain the emitted frames (the responder is now dropped -> the channel closes -> drain ends).
        let mut heads = 0;
        let mut datas = 0;
        let mut rows_seen: Vec<Vec<Value>> = Vec::new();
        while let Some(msg) = control_rx.recv().await {
            match (msg.frame.header.service, msg.frame.header.method) {
                (service::STREAM, method_stream::HEAD) => heads += 1,
                (service::STREAM, method_stream::DATA) => {
                    datas += 1;
                    let sd = StreamData::decode(&msg.frame.payload).expect("decode DATA");
                    rows_seen.extend(sd.rows);
                }
                other => panic!("unexpected stream frame {other:?}"),
            }
        }
        assert_eq!(heads, 1, "exactly one HEAD");
        assert!(datas >= 1, "at least one DATA frame carried the rows");
        assert_eq!(
            rows_seen,
            vec![
                vec![Value::I64(1)],
                vec![Value::I64(2)],
                vec![Value::I64(3)]
            ],
            "the rows stream in order off the pinned conn"
        );

        // Exactly ONE terminal: an Ok carrying the streamed affected/row count (no rows/cols — those
        // went out as DATA/HEAD).
        match cell.lock().unwrap().take().expect("exactly one terminal") {
            Terminal::Ok(body) => {
                let ok = ExecOk::decode(&body).expect("decode ExecOk");
                assert_eq!(ok.affected, 3);
                assert_eq!(ok.stats.rows, 3);
                assert!(ok.rows.is_empty(), "the stream terminal carries no rows");
                assert!(ok.cols.is_empty(), "the stream terminal carries no cols");
            }
            other => panic!("expected exactly one Ok terminal, got {other:?}"),
        }

        // INTACT: the tx is still open — a COMMIT still works (never auto-torn-down).
        let (commit_tx, commit_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Commit { reply: commit_tx })
            .await
            .expect("send commit");
        assert!(
            matches!(commit_rx.await.expect("commit replies"), CtlReply::Ok),
            "the tx survived the stream and COMMIT succeeds"
        );
        done_rx
            .wait_for(|t| *t)
            .await
            .expect("actor ends on commit");
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden,
            "a committed tx is deregistered, never tombstoned"
        );

        let co2 = pool.checkout().await.expect("conn released after commit");
        let rec = co2.conn().recorded.clone();
        assert!(
            rec.contains(&"SELECT n".to_string()),
            "the streamed SQL ran: {rec:?}"
        );
        assert!(
            rec.contains(&"COMMIT".to_string()),
            "the tx committed: {rec:?}"
        );
        assert!(
            !rec.contains(&"ROLLBACK".to_string()),
            "an intact stream never rolls the tx back: {rec:?}"
        );
    }

    /// A per-request CANCEL mid-stream inside a tx → the shared producer classifies
    /// `TxDeadline{Retryable}` (`in_tx: true`), the actor ROLLS BACK + TOMBSTONEs, and the request
    /// ends in exactly ONE terminal after whatever DATA already went out. A subsequent touch of the
    /// tx_id by its owner sees the tombstone.
    #[tokio::test]
    async fn tx_scoped_stream_cancel_mid_stream_rolls_back_tombstones_one_retryable_terminal() {
        let backend = FakeBackend::new();
        backend.set_stream_script(stream_script(vec![vec![Value::I64(1)]], 1));
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        let (tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        // 1-frame credit window: HEAD consumes it, so the first DATA send PARKS on the empty window —
        // the mid-stream point a CANCEL must unwind (never a live PG needed).
        let (responder, cell, mut control_rx) = streaming_responder(1);
        let cancel = CancellationToken::new();
        let (ack_tx, ack_rx) = oneshot::channel::<()>();
        cmd_tx
            .send(TxCommand::ExecStreamed {
                sql: "SELECT n".into(),
                params: vec![],
                timeout_ms: None,
                readonly: true,
                cancel: cancel.clone(),
                responder,
                done: ack_tx,
            })
            .await
            .expect("send exec-streamed");

        // HEAD goes out and consumes the window; the first DATA then parks. Cancel unwinds it.
        let head = control_rx.recv().await.expect("HEAD frame");
        assert_eq!(head.frame.header.method, method_stream::HEAD);
        cancel.cancel();
        ack_rx
            .await
            .expect("the actor acks after the cancel unwind");

        // Exactly ONE terminal: TxDeadline{Retryable} (the in-tx cancel fate).
        match cell.lock().unwrap().take().expect("exactly one terminal") {
            Terminal::Error(ep) => {
                assert_eq!(ep.code, errc::TX_DEADLINE);
                assert_eq!(ep.branch, branch::RETRYABLE);
            }
            other => panic!("expected exactly one TxDeadline terminal, got {other:?}"),
        }

        // Rolled back + tombstoned: the owner sees Tombstoned, the conn recorded a ROLLBACK, and the
        // out-of-band cancel was fired (an interruption abort).
        done_rx.wait_for(|t| *t).await.expect("actor tears down");
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::Tombstoned
        );
        assert!(
            pool.backend().cancel_calls() >= 1,
            "a mid-stream cancel fires the out-of-band backend cancel"
        );
        let co2 = pool.checkout().await.expect("conn released");
        assert!(
            co2.conn().recorded.contains(&"ROLLBACK".to_string()),
            "the tx was rolled back on the mid-stream cancel: {:?}",
            co2.conn().recorded
        );
    }

    /// A per-statement `timeout_ms` elapsing mid-stream inside a tx → the SAME
    /// rollback+tombstone+`TxDeadline` exit as an explicit cancel (the §19.3 uniform in-tx action).
    /// Proven under the paused clock: the deadline auto-fires while the first DATA is parked on the
    /// exhausted credit window.
    #[tokio::test(start_paused = true)]
    async fn tx_scoped_stream_timeout_mid_stream_rolls_back_tombstones_one_retryable_terminal() {
        let backend = FakeBackend::new();
        backend.set_stream_script(stream_script(vec![vec![Value::I64(1)]], 1));
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        // Generous max_tx: only the per-statement `timeout_ms` (below) can fire.
        let (tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        // 1-frame window: HEAD consumes it; the first DATA parks and the `timeout_ms` fires there.
        let (responder, cell, mut control_rx) = streaming_responder(1);
        let (ack_tx, ack_rx) = oneshot::channel::<()>();
        cmd_tx
            .send(TxCommand::ExecStreamed {
                sql: "SELECT n".into(),
                params: vec![],
                timeout_ms: Some(50),
                readonly: true,
                cancel: CancellationToken::new(), // never fired — the per-statement TIMER must fire
                responder,
                done: ack_tx,
            })
            .await
            .expect("send exec-streamed");
        ack_rx
            .await
            .expect("the deadline aborts the parked stream, then acks");

        // Exactly one HEAD went out; no DATA (it was aborted while parked).
        let mut heads = 0;
        let mut datas = 0;
        while let Some(msg) = control_rx.recv().await {
            match (msg.frame.header.service, msg.frame.header.method) {
                (service::STREAM, method_stream::HEAD) => heads += 1,
                (service::STREAM, method_stream::DATA) => datas += 1,
                other => panic!("unexpected stream frame {other:?}"),
            }
        }
        assert_eq!(heads, 1);
        assert_eq!(datas, 0, "the parked DATA was aborted, never enqueued");

        match cell.lock().unwrap().take().expect("exactly one terminal") {
            Terminal::Error(ep) => {
                assert_eq!(ep.code, errc::TX_DEADLINE);
                assert_eq!(ep.branch, branch::RETRYABLE);
            }
            other => panic!("expected one TxDeadline terminal, got {other:?}"),
        }

        done_rx.wait_for(|t| *t).await.expect("actor tears down");
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::Tombstoned
        );
        let co2 = pool.checkout().await.expect("conn released");
        assert!(
            co2.conn().recorded.contains(&"ROLLBACK".to_string()),
            "the tx was rolled back on the mid-stream timeout: {:?}",
            co2.conn().recorded
        );
    }

    /// A NATURAL (non-cancel) mid-stream error inside a tx → exactly ONE terminal carrying the error's
    /// CLASSIFIED fate (NOT `TxDeadline`), and the transaction STAYS OPEN — the client may `ROLLBACK`
    /// it itself, mirroring the buffered 23505 path (`non_cancel_statement_error_reported_without_auto_rollback`).
    /// This is the `StreamEnded::Intact`-on-natural-error branch; the out-of-band cancel is NOT fired.
    #[tokio::test]
    async fn tx_scoped_stream_natural_mid_stream_error_keeps_tx_open() {
        let backend = FakeBackend::new();
        // error_at: Some(1) → emit 1 row, then a natural (non-cancel) mid-stream Err on the next pull.
        backend.set_stream_script(StreamScript {
            cols: vec![ColMeta {
                name: "n".into(),
                tag: tag::I64,
            }],
            rows: vec![vec![Value::I64(1)], vec![Value::I64(2)]],
            affected: 0,
            error_at: Some(1),
        });
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        let (tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        let (responder, cell, mut control_rx) = streaming_responder(1000);
        let (ack_tx, ack_rx) = oneshot::channel::<()>();
        cmd_tx
            .send(TxCommand::ExecStreamed {
                sql: "SELECT n".into(),
                params: vec![],
                timeout_ms: None,
                readonly: true,
                cancel: CancellationToken::new(),
                responder,
                done: ack_tx,
            })
            .await
            .expect("send exec-streamed");
        ack_rx
            .await
            .expect("the actor acks after the natural error");

        // Drain whatever went out (HEAD, and possibly the batched row) — the point is the terminal.
        while control_rx.recv().await.is_some() {}

        // ONE terminal: the natural error's classified fate (the fake's Backend error → Protocol),
        // NEVER TxDeadline (a natural error is not a cancel/deadline).
        match cell.lock().unwrap().take().expect("exactly one terminal") {
            Terminal::Error(ep) => {
                assert_eq!(
                    ep.code,
                    errc::PROTOCOL,
                    "a natural mid-stream error reports its classified fate, not TxDeadline"
                );
                assert_ne!(ep.code, errc::TX_DEADLINE);
            }
            other => panic!("expected exactly one Error terminal, got {other:?}"),
        }

        // INTACT: the tx is NOT tombstoned (still Active), and the out-of-band cancel was NOT fired
        // (the statement self-terminated). A client-driven ROLLBACK on the SAME tx_id still works.
        assert!(
            registry.lookup(tx_id, owner).is_ok(),
            "the tx stays OPEN after a natural mid-stream error (no engine auto-rollback)"
        );
        assert_eq!(
            pool.backend().cancel_calls(),
            0,
            "a natural mid-stream error must NOT fire the out-of-band cancel"
        );
        let (rb_tx, rb_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Rollback { reply: rb_tx })
            .await
            .expect("send rollback");
        assert!(matches!(
            rb_rx.await.expect("rollback replies"),
            CtlReply::Ok
        ));
        done_rx
            .wait_for(|t| *t)
            .await
            .expect("actor ends on the client rollback");
    }

    /// The OPEN itself aborted inside a tx (a blocked `query_stream` + a fired cancel): exactly ONE
    /// `TxDeadline` terminal, no HEAD/DATA (the open never returned a handle), the conn FORCE-TAINTED
    /// (its recycle at the next checkout resets it), rolled back, and the tx tombstoned.
    #[tokio::test]
    async fn tx_scoped_stream_open_abort_taints_rolls_back_and_tombstones() {
        let backend = FakeBackend::new();
        backend.set_stream_script(stream_script(vec![vec![Value::I64(1)]], 1));
        backend.block_stream_open(); // freeze the OPEN (prepare + query_raw)
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        let (tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        let (responder, cell, mut control_rx) = streaming_responder(1000);
        let cancel = CancellationToken::new();
        let (ack_tx, ack_rx) = oneshot::channel::<()>();
        cmd_tx
            .send(TxCommand::ExecStreamed {
                sql: "SELECT n".into(),
                params: vec![],
                timeout_ms: None,
                readonly: true,
                cancel: cancel.clone(),
                responder,
                done: ack_tx,
            })
            .await
            .expect("send exec-streamed");

        // Wait until the OPEN is provably parked, then fire the request cancel.
        for _ in 0..1000 {
            if pool.backend().stream_opens_waiting() > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            pool.backend().stream_opens_waiting(),
            1,
            "the open must be parked before the cancel"
        );
        cancel.cancel();
        ack_rx
            .await
            .expect("the cancel aborts the blocked open, then acks");

        // No HEAD/DATA (the open never returned a handle); exactly ONE TxDeadline terminal.
        assert!(
            control_rx.recv().await.is_none(),
            "a cancelled open emits no HEAD/DATA"
        );
        match cell.lock().unwrap().take().expect("exactly one terminal") {
            Terminal::Error(ep) => {
                assert_eq!(ep.code, errc::TX_DEADLINE);
                assert_eq!(ep.branch, branch::RETRYABLE);
            }
            other => panic!("expected one TxDeadline terminal, got {other:?}"),
        }

        done_rx.wait_for(|t| *t).await.expect("actor tears down");
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::Tombstoned
        );
        assert!(
            pool.backend().cancel_calls() >= 1,
            "the aborted open fires the out-of-band backend cancel"
        );
        // The aborted open FORCE-TAINTED the conn → the recycle at the next checkout resets it.
        let co2 = pool.checkout().await.expect("conn released");
        assert!(
            co2.conn().recorded.contains(&"RESET:Full".to_string()),
            "an aborted open taints the conn → Full reset at the next checkout: {:?}",
            co2.conn().recorded
        );
    }

    /// SHOULD-FIX regression (review round): an `ExecStreamed` sitting BUFFERED in `cmd_rx` when the
    /// tx tears down must NOT drop its moved `Responder` undeclared (→ a synthesized
    /// `Protocol{NonRetryable}`). The teardown drain declares the PRECISE tear-down terminal —
    /// `TxDeadline{Retryable}`, matching the buffered `Exec` sibling's Retryable outcome — and signals
    /// the done-ack. Here command #1 (a buffered `Exec`) is frozen in-flight so the actor is busy in
    /// its inner select (not polling `cmd_rx`) while command #2 (`ExecStreamed`) sits buffered; a
    /// session abort then tears the tx down with #2 still queued.
    #[tokio::test]
    async fn tx_scoped_stream_buffered_at_teardown_gets_one_retryable_terminal() {
        let backend = FakeBackend::new();
        backend.block_query(); // freeze the FIRST (buffered Exec) statement in-flight
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        let (tx_id, cmd_tx, _done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        // Command #1: a buffered Exec the actor STARTS and parks on (block_query) — so it stays in the
        // inner select and never polls cmd_rx while #2 sits buffered behind it.
        let (r1_tx, _r1_rx) = oneshot::channel();
        cmd_tx
            .send(TxCommand::Exec {
                sql: "SELECT pg_sleep(9)".into(),
                params: vec![],
                timeout_ms: None,
                cancel: CancellationToken::new(),
                reply: r1_tx,
            })
            .await
            .expect("send #1");
        while pool.backend().queries_waiting() == 0 {
            tokio::task::yield_now().await;
        }

        // Command #2: an ExecStreamed queued BEHIND #1 — it sits BUFFERED in cmd_rx, never processed.
        let (responder, cell, _control_rx) = streaming_responder(1000);
        let (ack_tx, ack_rx) = oneshot::channel::<()>();
        cmd_tx
            .send(TxCommand::ExecStreamed {
                sql: "SELECT n".into(),
                params: vec![],
                timeout_ms: None,
                readonly: true,
                cancel: CancellationToken::new(),
                responder,
                done: ack_tx,
            })
            .await
            .expect("send #2 (buffered)");

        // Session death while #2 is buffered: #1 (in-flight) takes the Abort exit, then the drain
        // declares #2's terminal.
        registry.abort_session(owner).await;

        // #2 got exactly ONE TxDeadline{Retryable} terminal (NOT a synthesized Protocol), and its
        // done-ack resolved (a handler parked on `done_rx` would return cleanly).
        assert!(
            ack_rx.await.is_ok(),
            "the buffered streamed request's done-ack is signaled"
        );
        match cell.lock().unwrap().take().expect("exactly one terminal") {
            Terminal::Error(ep) => {
                assert_eq!(
                    ep.code,
                    errc::TX_DEADLINE,
                    "a buffered stream torn down reports TxDeadline, never a synthesized Protocol"
                );
                assert_eq!(ep.branch, branch::RETRYABLE);
            }
            other => panic!("expected one TxDeadline terminal, got {other:?}"),
        }
        // Session death → the Abort teardown: deregistered (not tombstoned) + one ROLLBACK.
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden
        );
        let co2 = pool.checkout().await.expect("conn released");
        assert!(co2.conn().recorded.contains(&"ROLLBACK".to_string()));
    }

    /// Acceptance-bar exit (test-gap fold-in): a session `abort` fired mid-IN-FLIGHT-`ExecStreamed`
    /// (streaming, not buffered) → exactly ONE terminal and the `TxEnd::Abort` teardown (DEREGISTER,
    /// owner lookup → `NotFoundOrForbidden`, NOT tombstoned) + one ROLLBACK. Per the review, the
    /// in-flight streamed abort declares `TxDeadline{Retryable}` on the wire (an ACCEPTED, documented
    /// divergence from the buffered path's `Protocol`); this test PINS that current behavior.
    #[tokio::test]
    async fn tx_scoped_stream_session_abort_mid_stream_one_terminal_abort_teardown() {
        let backend = FakeBackend::new();
        backend.set_stream_script(stream_script(vec![vec![Value::I64(1)]], 1));
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        let (tx_id, cmd_tx, _done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
        .await;

        // 1-frame window: HEAD out, the first DATA parks mid-stream — the point a session abort hits.
        let (responder, cell, mut control_rx) = streaming_responder(1);
        let (ack_tx, ack_rx) = oneshot::channel::<()>();
        cmd_tx
            .send(TxCommand::ExecStreamed {
                sql: "SELECT n".into(),
                params: vec![],
                timeout_ms: None,
                readonly: true,
                cancel: CancellationToken::new(),
                responder,
                done: ack_tx,
            })
            .await
            .expect("send exec-streamed");
        let head = control_rx.recv().await.expect("HEAD");
        assert_eq!(head.frame.header.method, method_stream::HEAD);

        // Session death mid-stream: `abort` (the child token's parent) unwinds the parked DATA send.
        registry.abort_session(owner).await;
        ack_rx.await.expect("the actor acks after the abort unwind");

        // Exactly ONE terminal (the accepted in-flight-abort TxDeadline{Retryable}).
        match cell.lock().unwrap().take().expect("exactly one terminal") {
            Terminal::Error(ep) => {
                assert_eq!(ep.code, errc::TX_DEADLINE);
                assert_eq!(ep.branch, branch::RETRYABLE);
            }
            other => panic!("expected one terminal, got {other:?}"),
        }
        // Session death takes `TxEnd::Abort`: DEREGISTERED (never tombstoned) + one ROLLBACK.
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::NotFoundOrForbidden,
            "a session abort deregisters (never tombstones), even mid-stream"
        );
        let co2 = pool.checkout().await.expect("conn released");
        assert!(
            co2.conn().recorded.contains(&"ROLLBACK".to_string()),
            "the actor rolled back on the mid-stream abort: {:?}",
            co2.conn().recorded
        );
    }

    /// Acceptance-bar exit (test-gap fold-in): a SHORT actor `max_tx` firing mid-stream (NO
    /// per-statement `timeout_ms` — only the tx max-lifetime can fire) → the SAME
    /// rollback+tombstone+`TxDeadline{Retryable}` exit as `timeout_ms`, proving the combined deadline
    /// carries the actor's absolute `max_tx` into the stream (not just the per-statement timer).
    #[tokio::test(start_paused = true)]
    async fn tx_scoped_stream_max_tx_mid_stream_rolls_back_and_tombstones() {
        let backend = FakeBackend::new();
        backend.set_stream_script(stream_script(vec![vec![Value::I64(1)]], 1));
        let pool = Pool::new(backend, test_pool_config());
        let registry = TxRegistry::new(Duration::from_secs(5));
        let owner = registry.next_session_id();
        // SHORT max_tx, generous idle, NO per-statement timeout_ms below → only max_tx can fire.
        let (tx_id, cmd_tx, mut done_rx) = spawn_actor(
            &pool,
            &registry,
            owner,
            Duration::from_secs(600),
            Duration::from_millis(50),
        )
        .await;

        // 1-frame window: HEAD out, the first DATA parks; the max_tx deadline fires while it is parked.
        let (responder, cell, mut control_rx) = streaming_responder(1);
        let (ack_tx, ack_rx) = oneshot::channel::<()>();
        cmd_tx
            .send(TxCommand::ExecStreamed {
                sql: "SELECT n".into(),
                params: vec![],
                timeout_ms: None, // no per-statement timer — the ACTOR max_tx must be what fires
                readonly: true,
                cancel: CancellationToken::new(),
                responder,
                done: ack_tx,
            })
            .await
            .expect("send exec-streamed");
        ack_rx
            .await
            .expect("the max_tx deadline aborts the parked stream, then acks");

        let head = control_rx.recv().await.expect("HEAD");
        assert_eq!(head.frame.header.method, method_stream::HEAD);

        match cell.lock().unwrap().take().expect("exactly one terminal") {
            Terminal::Error(ep) => {
                assert_eq!(ep.code, errc::TX_DEADLINE);
                assert_eq!(ep.branch, branch::RETRYABLE);
            }
            other => panic!("expected one TxDeadline terminal, got {other:?}"),
        }
        done_rx.wait_for(|t| *t).await.expect("actor tears down");
        assert_eq!(
            registry.lookup(tx_id, owner).unwrap_err(),
            TxLookupErr::Tombstoned,
            "the actor max_tx path tombstones the tx_id, exactly like a per-statement timeout"
        );
        let co2 = pool.checkout().await.expect("conn released");
        assert!(
            co2.conn().recorded.contains(&"ROLLBACK".to_string()),
            "the tx was rolled back on the max_tx deadline: {:?}",
            co2.conn().recorded
        );
    }
}
