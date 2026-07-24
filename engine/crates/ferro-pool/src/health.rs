//! Background liveness reaper (S4 Task 3) + the jittered exponential backoff schedule.
//!
//! The reaper is spawned only when `PoolConfig::reap_interval` is `Some` (v2/M3): the
//! `start_paused`, reaper-*less* tests in `pool_semantics.rs` keep `reap_interval: None` so
//! nothing but the test itself can advance pool state when the clock is advanced. Reaper-specific
//! tests turn it on deliberately and stay separate from those deterministic tests.
//!
//! **Cancel-safety (v2):** the reaper task holds only a `Weak<PoolInner<B>>`, never a strong
//! `Arc`. Each tick `upgrade()`s it; once every strong `Arc` (i.e. every `Pool<B>` handle) is
//! dropped, `upgrade()` returns `None` and the task exits on its own. It never keeps the pool
//! alive — the opposite (a strong `Arc` held across ticks) would leak the pool forever.
//!
//! The reaper only *evicts* dead/expired idle connections; it does not proactively reconnect.
//! Reconnecting from the reaper would create a brand-new backend connection *outside* the
//! checkout path's semaphore permit — the mechanism that actually bounds `max_size` — which could
//! transiently push the pool's live connection count above `max_size` if a concurrent checkout is
//! also connecting fresh. Keeping the pool warm is left entirely to the next `checkout()` (Task
//! 2's connect-on-demand path), which is already permit-bounded. `backoff_delay` is still
//! implemented and unit-tested here as the schedule any future connect-retry logic should use
//! (per v2/M5 — backoff belongs to background reconnection, never to blocking a checkout).

use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::backend::PoolBackend;
use crate::pool::PoolInner;

const BACKOFF_BASE: Duration = Duration::from_millis(10);
const BACKOFF_CAP: Duration = Duration::from_secs(1);

/// Jittered exponential backoff delay for `attempt` (0-indexed): `min(cap, base * 2^attempt)`
/// scaled by a full-jitter factor drawn from `[0, 1)`. Base 10ms, doubling, capped at 1s (v2/M5).
///
/// This is a schedule *bound*, not a fixed value: callers (and tests) should assert the returned
/// delay never exceeds `min(1s, 10ms * 2^attempt)` and is never negative, not an exact number.
pub fn backoff_delay(attempt: u32) -> Duration {
    // Cap the shift so `1u32 << shift` cannot overflow. Any attempt this large already blows well
    // past BACKOFF_CAP once multiplied, so further doubling would not change the clamped result.
    let shift = attempt.min(16);
    let multiplier = 1u32 << shift;
    let exp = BACKOFF_BASE.saturating_mul(multiplier).min(BACKOFF_CAP);
    exp.mul_f64(fastrand::f64())
}

/// Spawns the background reaper for `inner`, ticking every `interval`. Called from `Pool::new`
/// only when `config.reap_interval` is `Some` — a `None` config never calls this, leaving the
/// pool exactly as reaper-less as Task 2 left it.
pub(crate) fn spawn_reaper<B: PoolBackend>(inner: &Arc<PoolInner<B>>, interval: Duration) {
    let weak: Weak<PoolInner<B>> = Arc::downgrade(inner);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let Some(inner) = weak.upgrade() else {
                // The pool was dropped: no strong `Arc` left to reap. Stop.
                return;
            };
            reap_once(&inner).await;
            // `inner` (the temporary strong Arc from this tick) drops here, before the next
            // `sleep` — the reaper never holds a strong reference across ticks.
        }
    });
}

/// One reaper pass. Pulls every idle connection out of the shared idle stack (a brief,
/// synchronous lock — never held across an `.await`), pings each one (short-circuited away if
/// it's already past `max_lifetime`), and puts back only the ones still healthy and young enough.
/// Anything dead or expired is simply dropped here; the next `checkout()` reconnects lazily.
async fn reap_once<B: PoolBackend>(inner: &Arc<PoolInner<B>>) {
    let candidates = {
        let mut idle = inner.idle.lock().unwrap();
        std::mem::take(&mut *idle)
    };

    let mut kept = Vec::with_capacity(candidates.len());
    for mut idle_conn in candidates {
        let too_old = idle_conn.created_at.elapsed() > inner.config.max_lifetime;
        let alive = !too_old && inner.backend.ping(&mut idle_conn.conn).await.is_ok();
        if alive {
            kept.push(idle_conn);
        }
        // else: evicted — `idle_conn` is dropped here, releasing its connection resources.
    }

    let mut idle = inner.idle.lock().unwrap();
    idle.extend(kept);
}
