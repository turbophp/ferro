//! An injectable graceful-drain signal.
//!
//! `serve`'s accept loop and `main`'s SIGTERM watcher are both written against this handle rather
//! than a real OS signal directly, so `tests/shutdown.rs` can trigger a drain deterministically
//! (no real `kill -TERM`, no sleep-and-hope) while `main` wires the identical handle to a real
//! `SIGTERM`/`ctrl_c` watcher. Built on `tokio_util::sync::CancellationToken`: cheaply `Clone`
//! (every clone observes the same underlying state), and `cancelled()` is a *level* — it resolves
//! immediately on every poll once cancelled, not just the first time — which is exactly the
//! "drain has started" semantics this type wants (as opposed to a one-shot `oneshot::Receiver`,
//! which would only ever resolve for the first `.await`er).

use tokio_util::sync::CancellationToken;

/// A cheaply-clonable drain handle: `trigger()` starts the drain (idempotent), `wait()` resolves
/// once triggered, `is_draining()` polls the current state synchronously.
#[derive(Debug, Clone, Default)]
pub struct Drain(CancellationToken);

impl Drain {
    /// A fresh, not-yet-draining handle.
    pub fn new() -> Self {
        Drain(CancellationToken::new())
    }

    /// Start the drain. Idempotent — triggering an already-draining handle (via this clone or any
    /// other) changes nothing.
    pub fn trigger(&self) {
        self.0.cancel();
    }

    /// Resolves once `trigger()` has been called on this handle or any clone of it. A level, not
    /// an edge: every call resolves immediately once triggered, including calls made after the
    /// trigger already happened.
    pub async fn wait(&self) {
        self.0.cancelled().await;
    }

    /// Whether `trigger()` has been called on this handle or any clone of it.
    pub fn is_draining(&self) -> bool {
        self.0.is_cancelled()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_drain_is_not_draining() {
        let drain = Drain::new();
        assert!(!drain.is_draining());
    }

    #[tokio::test]
    async fn trigger_is_observed_by_every_clone() {
        let drain = Drain::new();
        let clone = drain.clone();
        assert!(!clone.is_draining());

        drain.trigger();

        assert!(clone.is_draining());
        // `wait()` must resolve immediately now -- a real-time timeout turns a regression (e.g.
        // treating this as a one-shot) into a fast, clear failure instead of a hang.
        tokio::time::timeout(std::time::Duration::from_secs(2), clone.wait())
            .await
            .expect("wait() must resolve once triggered");
    }

    #[test]
    fn trigger_is_idempotent() {
        let drain = Drain::new();
        drain.trigger();
        drain.trigger();
        assert!(drain.is_draining());
    }
}
