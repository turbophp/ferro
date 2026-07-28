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

use ferro_pool::backend::{Cancel, PoolBackend, QueryResult};
use ferro_pool::error::PoolError;
use ferro_pool::pool::Checkout;
use ferro_proto::messages::tx::Isolation;

use super::{CtlReply, ExecReply, TxCommand, TxRegistry};

/// Compose the engine's `BEGIN` statement from the request's `isolation` (a `u8` off the wire) and
/// `readonly` flag — a pure function, unit-tested per combination. `readonly` uses `READ ONLY`
/// (there is no separate `SET`), and `isolation == None` leaves the server/pool default in place
/// (no `ISOLATION LEVEL` clause). An unknown isolation `u8` is a client error (returned as `Err`,
/// mapped by the handler to `Protocol`), never coerced to a default.
///
/// Examples: `(None,false) → "BEGIN"`; `(None,true) → "BEGIN READ ONLY"`;
/// `(Some(RepeatableRead),true) → "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"`.
pub fn compose_begin_sql(isolation: Option<u8>, readonly: bool) -> Result<String, String> {
    let mut sql = String::from("BEGIN");
    if let Some(iso) = isolation {
        let level = match Isolation::try_from(iso).map_err(|e| e.to_string())? {
            Isolation::ReadCommitted => "READ COMMITTED",
            Isolation::RepeatableRead => "REPEATABLE READ",
            Isolation::Serializable => "SERIALIZABLE",
        };
        sql.push_str(" ISOLATION LEVEL ");
        sql.push_str(level);
    }
    if readonly {
        sql.push_str(" READ ONLY");
    }
    Ok(sql)
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
            TxCommand::Exec { sql, params, reply } => {
                // Capture the out-of-band cancel BEFORE borrowing `co` for the query — it returns an
                // owned handle, so the shared borrow ends immediately and does not conflict with the
                // `&mut co` the query future then holds.
                let cancel = co.cancel_handle();
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
                };

                match step {
                    ExecStep::Completed(result, exec_us) => {
                        let _ = reply.send(ExecReply::Completed { result, exec_us });
                    }
                    ExecStep::Deadline => {
                        // (1) fire the out-of-band cancel; (2) DRAIN the pinned future to its
                        // now-erroring completion (do NOT drop it) so the conn is back at
                        // ReadyForQuery; (3) reply the ONE TxDeadline terminal; teardown rolls back.
                        cancel.cancel().await;
                        let _ = query_fut.await;
                        let _ = reply.send(ExecReply::Deadline);
                        break 'actor TxEnd::Deadline;
                    }
                    ExecStep::Abort => {
                        cancel.cancel().await;
                        let _ = query_fut.await;
                        // Drop the reply sender: the forwarding handler's recv returns `Err` and it
                        // declares its one prompt terminal — the request still ends in exactly one END.
                        drop(reply);
                        break 'actor TxEnd::Abort;
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
    // Signal LAST — after the conn is back in the pool — so an `abort_session` awaiter that sees
    // `done` can rely on the connection already being released.
    let _ = done_tx.send(true);
}

/// Map a pool tx-control `Result` to the reply the forwarding handler declares from.
fn ctl_reply(r: Result<(), PoolError>) -> CtlReply {
    match r {
        Ok(()) => CtlReply::Ok,
        Err(e) => CtlReply::Err(e),
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
    use crate::tx::{TxHandle, TxLookupErr, next_tx_id};

    use ferro_pool::config::PoolConfig;
    use ferro_pool::fake::FakeBackend;
    use ferro_pool::pin::{PinCause, PinState, TxId};
    use ferro_pool::pool::Pool;
    use tokio::sync::oneshot;

    // ---- pure helpers -----------------------------------------------------------------------

    #[test]
    fn compose_begin_sql_table() {
        // isolation None: server/pool default, optionally READ ONLY.
        assert_eq!(compose_begin_sql(None, false).unwrap(), "BEGIN");
        assert_eq!(compose_begin_sql(None, true).unwrap(), "BEGIN READ ONLY");
        // Each isolation level × readonly.
        assert_eq!(
            compose_begin_sql(Some(Isolation::ReadCommitted.into()), false).unwrap(),
            "BEGIN ISOLATION LEVEL READ COMMITTED"
        );
        assert_eq!(
            compose_begin_sql(Some(Isolation::ReadCommitted.into()), true).unwrap(),
            "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY"
        );
        assert_eq!(
            compose_begin_sql(Some(Isolation::RepeatableRead.into()), false).unwrap(),
            "BEGIN ISOLATION LEVEL REPEATABLE READ"
        );
        assert_eq!(
            compose_begin_sql(Some(Isolation::RepeatableRead.into()), true).unwrap(),
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
        );
        assert_eq!(
            compose_begin_sql(Some(Isolation::Serializable.into()), false).unwrap(),
            "BEGIN ISOLATION LEVEL SERIALIZABLE"
        );
        assert_eq!(
            compose_begin_sql(Some(Isolation::Serializable.into()), true).unwrap(),
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY"
        );
        // An unknown isolation u8 is rejected (not coerced to a default).
        assert!(compose_begin_sql(Some(3), false).is_err());
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
            co2.conn().recorded.contains(&"RESET".to_string()),
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
}
