//! `TxRegistry` — the session-layer home of the per-`tx_id` transaction actors (SPEC §4/§7; S6).
//!
//! A transaction pins a pooled connection to a `tx_id`, NOT to the client socket — the property
//! that makes Fiber-suspended / multiplexed requests correct. Each open transaction is fronted by
//! a [`TxHandle`]: who owns it, how to command its actor, and how to await its teardown.
//!
//! **This module lands the SEAM only (S6 Task 2):** the registry data structure, the monotonic
//! `SessionId` counter, the (owner-checked, unknown-vs-forbidden-indistinguishable) lookup, and the
//! session-death [`TxRegistry::abort_session`] hook. The per-`tx_id` actor a `TxHandle` fronts —
//! the task that owns the pinned `Checkout` and serializes `TxCommand`s onto it — lands in the NEXT
//! task (S6 Task 3). Until then no `TxHandle` is ever registered, so `abort_session` iterates an
//! empty map and returns immediately; its awaitable + bounded shape is exactly what this task nails
//! down so the actor task can plug into it unchanged.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use crate::session::SessionId;

/// A command sent to a per-`tx_id` actor over its `mpsc` channel.
///
/// Only `Abort` exists in this task — it is all the seam needs (session-death / drain teardown).
/// S6-Task3 adds the rich set (`ExecInTx`/`Savepoint`/`Commit`/`Rollback`, each carrying a
/// `oneshot` reply sender) as further variants here; nothing else in this module changes.
#[derive(Debug)]
pub enum TxCommand {
    /// Tear the transaction down out-of-band: the actor rolls back the pinned conn (or marks it
    /// tainted), drops the `Checkout` so the conn returns to the pool, and deregisters itself.
    /// Sent by [`TxRegistry::abort_session`] on session death / drain.
    Abort,
    // S6-Task3: ExecInTx { .. }, Savepoint { .. }, Commit { .. }, Rollback { .. } land here, each
    // with its own `oneshot::Sender<...>` reply channel.
}

/// A registered transaction's control surface: who owns it, how to command its actor, and how to
/// await its teardown.
///
/// `Clone` so [`TxRegistry::lookup`] can hand a caller its own copy while the entry stays
/// registered: `cmd_tx` and `done` are both cheap-to-clone handles (an `mpsc::Sender` and a
/// `watch::Receiver`) and `owner` is `Copy`.
#[derive(Clone)]
pub struct TxHandle {
    /// The session that opened this transaction. A lookup by any other session is indistinguishable
    /// from an unknown `tx_id` (see [`TxRegistry::lookup`]).
    pub owner: SessionId,
    /// Command channel to the actor that owns the pinned `Checkout`.
    pub cmd_tx: mpsc::Sender<TxCommand>,
    /// Resolves when the actor has finished tearing down (rolled back + released the conn). The
    /// actor (S6-Task3) holds the paired `watch::Sender` and flips it to `true` after teardown —
    /// and dropping that sender resolves an awaiter too, so a panicked/aborted actor never wedges
    /// `abort_session`.
    pub done: watch::Receiver<bool>,
}

/// The reason a [`TxRegistry::lookup`] failed. A missing `tx_id` and an owner mismatch are
/// DELIBERATELY the same variant: cross-session and unknown are indistinguishable to the caller
/// (both map to a wire `Protocol` in the tx-dispatch task), so a client can never probe another
/// session's `tx_id` space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxLookupErr {
    NotFoundOrForbidden,
}

struct Inner {
    /// Monotonic, never-reused session ids, one drawn per accepted connection.
    next_session_id: AtomicU64,
    /// `tx_id` -> its actor's control surface.
    txs: Mutex<HashMap<u64, TxHandle>>,
    /// Bounds [`TxRegistry::abort_session`]'s teardown wait — wired to `config.drain_deadline`.
    abort_deadline: Duration,
}

/// A cloneable, `Arc`-backed handle to the process-global transaction registry. Cloning shares the
/// same counter + map (it is an `Arc` internally) — cloning does not create a second registry.
#[derive(Clone)]
pub struct TxRegistry {
    inner: Arc<Inner>,
}

impl TxRegistry {
    /// Build a registry whose [`TxRegistry::abort_session`] teardown wait is bounded by
    /// `abort_deadline` (wire it to `config.drain_deadline`).
    pub fn new(abort_deadline: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                next_session_id: AtomicU64::new(0),
                txs: Mutex::new(HashMap::new()),
                abort_deadline,
            }),
        }
    }

    /// Draw the next session id: monotonic, distinct per call, never reused. One per accepted
    /// connection (see `session::Session::run_with_handler`).
    pub fn next_session_id(&self) -> SessionId {
        self.inner.next_session_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Register a transaction's actor control surface under `tx_id`.
    ///
    /// S6-Task3: called by the BEGIN handler immediately after spawning the actor.
    pub fn register(&self, tx_id: u64, handle: TxHandle) {
        self.inner.txs.lock().unwrap().insert(tx_id, handle);
    }

    /// Remove a transaction from the registry.
    ///
    /// S6-Task3: called by the actor as it tears down (commit/rollback/deadline/abort).
    pub fn deregister(&self, tx_id: u64) {
        self.inner.txs.lock().unwrap().remove(&tx_id);
    }

    /// Look up a transaction by `tx_id` ON BEHALF OF `caller`. A missing id OR an owner other than
    /// `caller` both return [`TxLookupErr::NotFoundOrForbidden`] — cross-session and unknown are
    /// indistinguishable by design. On success returns a clone of the `TxHandle`; the entry stays
    /// registered.
    pub fn lookup(&self, tx_id: u64, caller: SessionId) -> Result<TxHandle, TxLookupErr> {
        let txs = self.inner.txs.lock().unwrap();
        match txs.get(&tx_id) {
            Some(handle) if handle.owner == caller => Ok(handle.clone()),
            _ => Err(TxLookupErr::NotFoundOrForbidden),
        }
    }

    /// Signal every transaction owned by `sid` to abort and await their teardown, BOUNDED by the
    /// registry's `abort_deadline`.
    ///
    /// Called on every session-end route, between the in-flight registry's `cancel_all()` and the
    /// supervisor drain (see `session::Session::run_with_handler`): `cancel_all()` only fires each
    /// request's `CancellationToken`, which a tx-scoped handler blocked on a `oneshot` recv does
    /// NOT observe — so aborting the actors here drops their reply senders, the in-flight handler's
    /// recv returns `Err`, it declares its one terminal, and the supervisor delivers the `END`
    /// inside the drain window.
    ///
    /// With no actor yet (this task), the map is always empty for `sid`, so this returns
    /// immediately. Its awaitable + bounded shape is the contract the actor task (S6-Task3) plugs
    /// into.
    pub async fn abort_session(&self, sid: SessionId) {
        // Collect the owned handles under the lock, then release it BEFORE any await (never hold a
        // std::sync::Mutex across an await point).
        let handles: Vec<TxHandle> = {
            let txs = self.inner.txs.lock().unwrap();
            txs.values().filter(|h| h.owner == sid).cloned().collect()
        };
        if handles.is_empty() {
            return;
        }

        let teardown = async {
            // Fire the abort signal at every owned tx first (a full send channel / gone actor is a
            // no-op — either way the actor is on its way down)...
            for handle in &handles {
                let _ = handle.cmd_tx.send(TxCommand::Abort).await;
            }
            // ...then await each actor's teardown-complete: the value flips to `true`, OR the
            // actor's `watch::Sender` drops (a panicked/finished task) — both resolve `wait_for`.
            for mut handle in handles {
                let _ = handle.done.wait_for(|torn_down| *torn_down).await;
            }
        };
        let _ = tokio::time::timeout(self.inner.abort_deadline, teardown).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn abort_session_on_empty_registry_returns_promptly() {
        let reg = TxRegistry::new(Duration::from_secs(5));
        let sid = reg.next_session_id();
        // No txs registered for `sid` -> must return well under the (5s) deadline, never hang. The
        // 100ms outer bound is the proof it is prompt; the registry's own timeout is the belt.
        tokio::time::timeout(Duration::from_millis(100), reg.abort_session(sid))
            .await
            .expect("abort_session on an empty registry must return promptly, not hang");
    }

    #[test]
    fn next_session_id_is_monotonic_and_distinct() {
        let reg = TxRegistry::new(Duration::from_secs(5));
        let ids: Vec<SessionId> = (0..8).map(|_| reg.next_session_id()).collect();

        for window in ids.windows(2) {
            assert!(
                window[1] > window[0],
                "session ids must be strictly monotonic, got {ids:?}"
            );
        }

        let mut deduped = ids.clone();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(
            deduped.len(),
            ids.len(),
            "session ids must be distinct, got {ids:?}"
        );
    }

    #[test]
    fn lookup_owner_mismatch_and_missing_both_not_found_or_forbidden() {
        let reg = TxRegistry::new(Duration::from_secs(5));
        let owner = reg.next_session_id();
        let other = reg.next_session_id();

        // A missing tx_id -> NotFoundOrForbidden.
        assert!(matches!(
            reg.lookup(999, owner),
            Err(TxLookupErr::NotFoundOrForbidden)
        ));

        // Register a tx owned by `owner`.
        let (cmd_tx, _cmd_rx) = mpsc::channel::<TxCommand>(1);
        let (_done_tx, done_rx) = watch::channel(false);
        reg.register(
            42,
            TxHandle {
                owner,
                cmd_tx,
                done: done_rx,
            },
        );

        // The owner finds it...
        assert!(reg.lookup(42, owner).is_ok());
        // ...but a DIFFERENT session gets the SAME NotFoundOrForbidden a missing id yields
        // (cross-session and unknown are indistinguishable to the caller by design).
        assert!(matches!(
            reg.lookup(42, other),
            Err(TxLookupErr::NotFoundOrForbidden)
        ));
    }

    // S6-Task3: the over-the-wire tests need the actor + daemon dispatch, so they live in the NEXT
    // task's suite, not here:
    //   * session-death-releases-a-conn: open a tx over the wire, drop the session, assert a fresh
    //     checkout gets a clean released conn (the actor rolled back on Abort);
    //   * cross-session reject over the wire: a tx_id from session A used by B -> Protocol, A
    //     undisturbed.
}
