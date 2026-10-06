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

use std::sync::{Arc, OnceLock};

use tokio_util::sync::CancellationToken;

/// A cheaply-clonable drain handle: `trigger()` starts the drain (idempotent), `wait()` resolves
/// once triggered, `is_draining()` polls the current state synchronously.
///
/// **It records WHEN the drain began (M6-F4b, SPEC §23.6.1).** Ferro HTTP stops in-flight exchanges
/// at `FERRO_HTTP_DRAIN_MS` after the drain began, and `serve` waits `FERRO_HTTP_DRAIN_MS +
/// drain_deadline` from the same instant; one recorded instant, set before the token is cancelled,
/// is what makes every observer agree on it.
#[derive(Debug, Clone, Default)]
pub struct Drain {
    token: CancellationToken,
    at: Arc<OnceLock<tokio::time::Instant>>,
}

impl Drain {
    /// A fresh, not-yet-draining handle.
    pub fn new() -> Self {
        Drain::default()
    }

    /// Start the drain. Idempotent — triggering an already-draining handle (via this clone or any
    /// other) changes nothing, the recorded start included.
    pub fn trigger(&self) {
        self.at.get_or_init(tokio::time::Instant::now);
        self.token.cancel();
    }

    /// Resolves once `trigger()` has been called on this handle or any clone of it. A level, not
    /// an edge: every call resolves immediately once triggered, including calls made after the
    /// trigger already happened.
    pub async fn wait(&self) {
        self.token.cancelled().await;
    }

    /// Whether `trigger()` has been called on this handle or any clone of it.
    pub fn is_draining(&self) -> bool {
        self.token.is_cancelled()
    }

    /// When the drain began (`None` until it has).
    pub fn started_at(&self) -> Option<tokio::time::Instant> {
        self.at.get().copied()
    }

    /// The token cancelled when the drain begins, and the cell holding that instant — what a
    /// service needs to build its own view of this drain (Ferro HTTP's `DrainView`).
    pub fn parts(&self) -> (CancellationToken, Arc<OnceLock<tokio::time::Instant>>) {
        (self.token.clone(), Arc::clone(&self.at))
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

    /// The start instant is recorded once, by the first trigger, and every clone reads it.
    #[tokio::test]
    async fn the_start_is_recorded_once_and_shared() {
        let drain = Drain::new();
        let clone = drain.clone();
        assert_eq!(clone.started_at(), None);
        let before = tokio::time::Instant::now();
        drain.trigger();
        let at = clone.started_at().expect("recorded");
        assert!(at >= before);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        clone.trigger();
        assert_eq!(
            drain.started_at(),
            Some(at),
            "a second trigger does not move it"
        );
        let (token, cell) = clone.parts();
        assert!(token.is_cancelled());
        assert_eq!(cell.get().copied(), Some(at));
    }
}
