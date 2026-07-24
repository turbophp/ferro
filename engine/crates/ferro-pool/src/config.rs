//! Pool configuration (S4 Task 2).

use std::time::Duration;

/// Configuration for a [`crate::pool::Pool`].
///
/// `reap_interval: None` means **no reaper** (v2/M3): Task 2 always runs reaper-less, which is
/// what keeps `start_paused` tests deterministic — nothing else can advance state when the test
/// advances the clock. Task 3 wires up the background reaper when this is `Some(_)`.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of live connections the pool will ever hold (idle + checked out).
    pub max_size: usize,
    /// How long `checkout()` will wait for a permit/idle connection before returning
    /// `PoolError::Timeout`.
    pub checkout_timeout: Duration,
    /// A connection older than this (measured from `connect()`) is evicted and replaced instead
    /// of being reused, checked at checkout time (v2/B1 step 3).
    pub max_lifetime: Duration,
    /// Interval for the background liveness reaper. `None` disables the reaper entirely (Task 2
    /// default; Task 3 wires it up when `Some`).
    pub reap_interval: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 8,
            checkout_timeout: Duration::from_secs(5),
            max_lifetime: Duration::from_secs(30 * 60),
            reap_interval: None,
        }
    }
}
