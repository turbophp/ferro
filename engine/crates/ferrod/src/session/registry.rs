//! In-flight registry: which request-bearing request ids (services SQL/TX/STREAM) currently have
//! a handler running for this session. A plain `std::sync::Mutex`, not a tokio `Mutex` — every
//! access here is a fast, non-blocking read-modify-write of a `HashSet<u32>` held for a handful
//! of instructions, so a std mutex is both correct and cheaper than an async one; it also means
//! this type is safe to lock from a `Drop` impl or any other non-async context, should one ever
//! need to (charter/plan requirement: "Drop/sync contexts must lock it — never a tokio Mutex").
//!
//! The registry only tracks membership (id -> in-flight or not); it does not hold the handler's
//! `JoinHandle` or any terminal state — those live with the supervisor task and the `Responder`
//! cell respectively (see `session::supervisor`, `session::responder`). The ONLY removal path is
//! `remove`, called by the supervisor after it has sent the request's one terminal frame — never
//! from a `Drop` impl, and never on any other code path, so a request id is freed for reuse
//! exactly once, right when its lifecycle is truly over.

use std::collections::HashSet;
use std::sync::Mutex;

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

/// The in-flight registry for one session. Holds only request-bearing (SQL/TX/STREAM) request
/// ids; core control/liveness frames (HELLO_ACK, PONG, WINDOW_UPDATE-ack, GOODBYE) never enter it
/// and are not subject to the one-`END` rule.
pub struct Registry {
    inner: Mutex<HashSet<u32>>,
    max_inflight: usize,
}

impl Registry {
    pub fn new(max_inflight: usize) -> Self {
        Registry {
            inner: Mutex::new(HashSet::new()),
            max_inflight,
        }
    }

    /// Insert `id` if it is neither already in-flight nor at capacity. On success, the caller
    /// (session's reader loop) proceeds to reserve a control-channel permit and spawn the
    /// handler; on `Err`, the caller must send a per-request diagnostic error WITHOUT spawning
    /// anything and WITHOUT touching the registry further for this frame.
    pub fn insert(&self, id: u32) -> Result<(), InsertErr> {
        let mut set = self.inner.lock().unwrap();
        if set.contains(&id) {
            return Err(InsertErr::Reused);
        }
        if set.len() >= self.max_inflight {
            return Err(InsertErr::Full);
        }
        set.insert(id);
        Ok(())
    }

    /// Free `id` for reuse. Called by the supervisor exactly once, immediately after it sends
    /// the request's single terminal frame.
    pub fn remove(&self, id: u32) {
        self.inner.lock().unwrap().remove(&id);
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

    #[test]
    fn insert_rejects_reuse_and_enforces_capacity() {
        let registry = Registry::new(2);
        assert!(registry.insert(1).is_ok());
        assert_eq!(registry.insert(1), Err(InsertErr::Reused));
        assert!(registry.insert(2).is_ok());
        assert_eq!(registry.insert(3), Err(InsertErr::Full));
        assert_eq!(registry.len(), 2);

        registry.remove(1);
        assert_eq!(registry.len(), 1);
        assert!(registry.insert(3).is_ok());
    }
}
