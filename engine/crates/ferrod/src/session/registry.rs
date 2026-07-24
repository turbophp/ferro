//! In-flight registry: which request-bearing request ids (services SQL/TX/STREAM) currently have
//! a handler running for this session, plus the two pieces of per-request state that key off the
//! id rather than the handler task: an advisory `CancellationToken` (routed `CANCEL` frames cancel
//! it; the handler observes it and decides to `end_cancelled()`) and a `flow::Credit` window
//! (routed `WINDOW_UPDATE` frames replenish it). A plain `std::sync::Mutex`, not a tokio `Mutex` —
//! every access here is a fast, non-blocking read-modify-write of a small map held for a handful
//! of instructions, so a std mutex is both correct and cheaper than an async one; it also means
//! this type is safe to lock from a `Drop` impl or any other non-async context, should one ever
//! need to (charter/plan requirement: "Drop/sync contexts must lock it — never a tokio Mutex").
//!
//! The registry does not hold the handler's `JoinHandle` or any terminal state — those live with
//! the supervisor task and the `Responder` cell respectively (see `session::supervisor`,
//! `session::responder`). The ONLY removal path is `remove`, called by the supervisor after it has
//! sent the request's one terminal frame — never from a `Drop` impl, and never on any other code
//! path, so a request id is freed for reuse exactly once, right when its lifecycle is truly over.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio_util::sync::CancellationToken;

use super::flow::Credit;

/// Why `Registry::insert` rejected a request id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertErr {
    /// The id is already in-flight — a client reused it before its predecessor produced a
    /// terminal. This is a per-request protocol fault on the SECOND (reusing) frame; the
    /// original in-flight request is completely undisturbed.
    Reused,
    /// The registry already holds `max_inflight` entries.
    Full,
}

/// One in-flight request's registry-held state: its advisory cancellation token and its
/// flow-control credit window. Neither is touched by the registry itself beyond storage/lookup —
/// `cancel` and `replenish` below are the only mutators, both driven by routed core frames
/// (`CANCEL`, `WINDOW_UPDATE`) in `session::mod`'s reader loop.
struct InFlight {
    cancel: CancellationToken,
    credit: Credit,
}

/// The in-flight registry for one session. Holds only request-bearing (SQL/TX/STREAM) request
/// ids; core control/liveness frames (HELLO_ACK, PONG, WINDOW_UPDATE-ack, GOODBYE) never enter it
/// and are not subject to the one-`END` rule.
pub struct Registry {
    inner: Mutex<HashMap<u32, InFlight>>,
    max_inflight: usize,
}

impl Registry {
    pub fn new(max_inflight: usize) -> Self {
        Registry {
            inner: Mutex::new(HashMap::new()),
            max_inflight,
        }
    }

    /// Insert `id` — seeding a fresh `CancellationToken` and `credit` window — if it is neither
    /// already in-flight nor at capacity. On success, returns a clone of the new token for the
    /// caller to hand to the spawned handler (the registry keeps its own clone, so `cancel` below
    /// can reach it purely by `id` without the handler's cooperation). On `Err`, the caller must
    /// send a per-request diagnostic WITHOUT spawning anything and WITHOUT touching the registry
    /// further for this frame.
    pub fn insert(&self, id: u32, credit: Credit) -> Result<CancellationToken, InsertErr> {
        let mut map = self.inner.lock().unwrap();
        if map.contains_key(&id) {
            return Err(InsertErr::Reused);
        }
        if map.len() >= self.max_inflight {
            return Err(InsertErr::Full);
        }
        let cancel = CancellationToken::new();
        map.insert(
            id,
            InFlight {
                cancel: cancel.clone(),
                credit,
            },
        );
        Ok(cancel)
    }

    /// Free `id` for reuse. Called by the supervisor exactly once, immediately after it sends
    /// the request's single terminal frame.
    pub fn remove(&self, id: u32) {
        self.inner.lock().unwrap().remove(&id);
    }

    /// Advisory, idempotent CANCEL routing: if `id` is in-flight, cancel its token — cancelling
    /// an already-cancelled `CancellationToken` is itself a documented no-op, which is what makes
    /// a second CANCEL on the same id harmless. If `id` is unknown (never started, or already
    /// completed and removed), this is silently a no-op: SPEC §5.2, "If the request already
    /// completed, CANCEL is a no-op." Either way CANCEL never produces a reply frame of its own.
    pub fn cancel(&self, id: u32) {
        if let Some(inflight) = self.inner.lock().unwrap().get(&id) {
            inflight.cancel.cancel();
        }
    }

    /// Cancel EVERY currently in-flight request's token. Called once, at session shutdown (any
    /// exit path — EOF, GOODBYE-drain, a session-fatal classification, the writer exiting early),
    /// to nudge every cooperative handler still running toward finishing quickly before the
    /// session's own bounded per-request drain (see `session::mod`'s `drain_supervisors`) gives up
    /// and hard-aborts its supervisor task. A no-op on an empty registry; cancelling an
    /// already-cancelled token is itself a documented no-op, so this is safe to call more than
    /// once too.
    pub fn cancel_all(&self) {
        for inflight in self.inner.lock().unwrap().values() {
            inflight.cancel.cancel();
        }
    }

    /// Apply a routed `WINDOW_UPDATE {frames, bytes}` to `id`'s stored credit. An unknown `id` is
    /// silently a no-op (the target may never have existed, or may have already completed).
    pub fn replenish(&self, id: u32, frames: u32, bytes: u32) {
        if let Some(inflight) = self.inner.lock().unwrap().get_mut(&id) {
            inflight.credit.replenish(frames, bytes);
        }
    }

    /// Read back `id`'s current credit window. `None` if `id` is not currently in-flight. Used by
    /// tests to observe `WINDOW_UPDATE` routing without a stream producer to consume credit yet
    /// (that lands in S5); a plain, non-test-gated accessor since "what is this request's current
    /// window" is a reasonable thing for future introspection/metrics to want too.
    pub fn credit_snapshot(&self, id: u32) -> Option<Credit> {
        self.inner.lock().unwrap().get(&id).map(|f| f.credit)
    }

    /// The number of currently in-flight request ids.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Whether there are no currently in-flight request ids.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_credit() -> Credit {
        Credit::new(64, 4 * 1024 * 1024)
    }

    #[test]
    fn insert_rejects_reuse_and_enforces_capacity() {
        let registry = Registry::new(2);
        assert!(registry.insert(1, test_credit()).is_ok());
        assert_eq!(registry.insert(1, test_credit()), Err(InsertErr::Reused));
        assert!(registry.insert(2, test_credit()).is_ok());
        assert_eq!(registry.insert(3, test_credit()), Err(InsertErr::Full));
        assert_eq!(registry.len(), 2);

        registry.remove(1);
        assert_eq!(registry.len(), 1);
        assert!(registry.insert(3, test_credit()).is_ok());
    }

    #[test]
    fn cancel_is_idempotent_and_unknown_id_is_noop() {
        let registry = Registry::new(4);
        let cancel = registry.insert(1, test_credit()).unwrap();
        assert!(!cancel.is_cancelled());

        registry.cancel(1);
        assert!(cancel.is_cancelled());

        // Idempotent: cancelling again does not panic or otherwise misbehave.
        registry.cancel(1);
        assert!(cancel.is_cancelled());

        // Unknown id: silently a no-op.
        registry.cancel(999);
    }

    #[test]
    fn cancel_all_cancels_every_inflight_token_and_is_safe_when_empty() {
        let registry = Registry::new(4);
        // Safe on an empty registry: no panic, nothing to cancel.
        registry.cancel_all();

        let a = registry.insert(1, test_credit()).unwrap();
        let b = registry.insert(2, test_credit()).unwrap();
        assert!(!a.is_cancelled());
        assert!(!b.is_cancelled());

        registry.cancel_all();
        assert!(a.is_cancelled());
        assert!(b.is_cancelled());

        // Idempotent: calling again does not panic or otherwise misbehave.
        registry.cancel_all();
        assert!(a.is_cancelled());
        assert!(b.is_cancelled());
    }

    #[test]
    fn replenish_routes_to_the_targets_credit_and_ignores_unknown_ids() {
        let registry = Registry::new(4);
        registry.insert(10, Credit::new(2, 100)).unwrap();

        registry.replenish(10, 5, 900);
        let credit = registry.credit_snapshot(10).expect("id 10 is in-flight");
        assert_eq!(credit.frames(), 7);
        assert_eq!(credit.bytes(), 1000);

        // Unknown id: silently a no-op, no panic.
        registry.replenish(999, 1, 1);
        assert!(registry.credit_snapshot(999).is_none());
    }
}
