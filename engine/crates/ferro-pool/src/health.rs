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
//! semaphore permit accounting — the mechanism that actually bounds `max_size` — which could
//! transiently push the pool's live connection count above `max_size` if a concurrent checkout is
//! also connecting fresh. Keeping the pool warm is left entirely to the next `checkout()` (Task
//! 2's connect-on-demand path), which is already permit-bounded. `backoff_delay` is still
//! implemented and unit-tested here as the schedule any future connect-retry logic should use
//! (per v2/M5 — backoff belongs to background reconnection, never to blocking a checkout).
//!
//! **Permit-per-pinged-conn (S4 CRITICAL fix):** pinging an idle connection also removes it from
//! `idle` for the duration of the round trip, so `reap_once` holds an owned semaphore permit for
//! that whole window too — otherwise a concurrent `checkout()` would see `idle` empty, no permit
//! held against the pinged connection, and `connect()` a fresh one past `max_size`. See
//! `reap_once`'s doc comment for the full account.

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

/// One reaper pass.
///
/// **S4 CRITICAL fix:** the old implementation pulled *every* idle connection out of the shared
/// idle stack in one `mem::take` (no semaphore permit involved), pinged them all with the lock
/// released, and reinserted the survivors. During that window `idle` was empty but nobody held a
/// permit for the pinged connections, so a concurrent `checkout()` — bounded only by its OWN
/// permit — could see `idle` empty and `connect()` a brand-new connection, and the reaper would
/// then reinsert the ones it had pulled out. Total live connections could exceed `max_size` and
/// the surplus never got cleaned up (an accumulating capacity leak, breaking G1 "M ≪ N").
///
/// The fix: process idle connections ONE AT A TIME, each gated behind an *owned* semaphore permit
/// for as long as it is out of the idle stack for pinging. Holding that permit is what makes a
/// pinged connection count against `max_size` exactly like a checked-out one — a concurrent
/// `checkout()` simply queues on the semaphore instead of falling through to `connect()`.
///
/// The `idle` mutex is still never held across an `.await`: each iteration pops ONE connection
/// under a brief, synchronous lock, pings it with the lock released, then re-locks only to push it
/// back (if healthy) — anything stale or dead is just dropped, releasing its resources, and the
/// next `checkout()` reconnects lazily.
///
/// Bounded to the number of idle connections observed at the START of this tick, so a healthy
/// connection that gets popped and immediately reinserted cannot make this loop spin forever. Note
/// that bound does not guarantee every *distinct* idle connection is examined exactly once this
/// tick (an adversarial reinsert order could see the same connection popped more than once while
/// another sits untouched) — but that doesn't matter for the invariant this fixes: the pool-size
/// guarantee only depends on a pinged connection being permit-gated while it's out of `idle`, not
/// on which connection that happens to be. A connection that isn't reached this tick simply waits
/// for the next one.
async fn reap_once<B: PoolBackend>(inner: &Arc<PoolInner<B>>) {
    let budget = { inner.idle.lock().unwrap().len() };

    for _ in 0..budget {
        // No permit available means every slot up to `max_size` is already spoken for by
        // concurrent checkouts (or other reaper iterations, though this loop only ever holds one
        // at a time) — stop this tick rather than block; the reaper must never hold up a
        // checkout.
        let permit = match Arc::clone(&inner.semaphore).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => break,
        };

        let popped = {
            let mut idle = inner.idle.lock().unwrap();
            idle.pop()
        };
        let Some(mut idle_conn) = popped else {
            drop(permit);
            break; // no idle conns left to reap this tick
        };

        let stale = idle_conn.created_at.elapsed() > inner.config.max_lifetime;
        // Short-circuited exactly like the previous implementation: a connection already past
        // `max_lifetime` is evicted without spending a round trip on it.
        let dead = !stale
            && (inner.backend.is_closed(&idle_conn.conn)
                || inner.backend.ping(&mut idle_conn.conn).await.is_err());

        if stale || dead {
            // Evicted: `idle_conn` drops here, releasing its connection resources. `permit`
            // releases right after, so the vacated capacity is available to the next checkout (or
            // the next reap iteration) only once this connection is truly gone.
        } else {
            let mut idle = inner.idle.lock().unwrap();
            idle.push(idle_conn);
        }
        drop(permit);
    }
}
