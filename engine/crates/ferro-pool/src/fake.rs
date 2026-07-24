//! Deterministic in-memory `PoolBackend` for fast pool-semantics tests (Task 1). Later tasks
//! (checkout/release, max_lifetime, pin stub) drive this backend instead of a live Postgres so
//! the pool's mechanics are tested without a Docker dependency.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::time::Instant;

use crate::backend::PoolBackend;
use crate::error::PoolError;

/// The fake's connection handle. Fields are `pub` so tests can inspect/mutate state directly
/// (arm a ping failure, read `recorded`, flip `closed`, check `tx_open`) without a getter for
/// every field.
#[derive(Debug, Clone)]
pub struct FakeConn {
    pub id: u64,
    pub closed: bool,
    pub created_at: Instant,
    /// Every SQL string passed to `simple_query`/`reset`, in order — lets pin-stub tests
    /// (Task 4) assert the exact sequence (e.g. `["BEGIN", "COMMIT"]`).
    pub recorded: Vec<String>,
    pub tx_open: bool,
    fail_next_ping: bool,
}

impl FakeConn {
    /// Arms the *next* `ping()` call on this connection to fail with `ConnectionLost`; the flag
    /// is consumed (one-shot) so subsequent pings succeed again.
    pub fn arm_fail_next_ping(&mut self) {
        self.fail_next_ping = true;
    }
}

/// A `FakeBackend` connects instantly and never touches the network. `next_id` is atomic so
/// `connect()` only needs `&self` (matching the trait), matching how a real pool shares one
/// backend across many concurrent checkouts.
#[derive(Debug, Default)]
pub struct FakeBackend {
    next_id: AtomicU64,
    /// Scripted connect failures: each `connect()` call while this is > 0 fails with
    /// `ConnectionLost` and decrements the counter. Armed via `arm_fail_connect` (used by later
    /// tasks' backoff/reconnect tests; Task 1 only needs the mechanism to exist).
    fail_connect_remaining: AtomicU64,
}

impl FakeBackend {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(0),
            fail_connect_remaining: AtomicU64::new(0),
        }
    }

    /// Arms the next `n` `connect()` calls to fail with `PoolError::ConnectionLost`.
    pub fn arm_fail_connect(&self, n: u64) {
        self.fail_connect_remaining.store(n, Ordering::SeqCst);
    }
}

#[async_trait]
impl PoolBackend for FakeBackend {
    type Conn = FakeConn;

    async fn connect(&self) -> Result<Self::Conn, PoolError> {
        let should_fail = self
            .fail_connect_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n > 0 { Some(n - 1) } else { None }
            })
            .is_ok();
        if should_fail {
            return Err(PoolError::ConnectionLost);
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        Ok(FakeConn {
            id,
            closed: false,
            created_at: Instant::now(),
            recorded: Vec::new(),
            tx_open: false,
            fail_next_ping: false,
        })
    }

    async fn ping(&self, conn: &mut Self::Conn) -> Result<(), PoolError> {
        if conn.fail_next_ping {
            conn.fail_next_ping = false;
            return Err(PoolError::ConnectionLost);
        }
        Ok(())
    }

    fn is_closed(&self, conn: &Self::Conn) -> bool {
        conn.closed
    }

    async fn reset(&self, conn: &mut Self::Conn) -> Result<(), PoolError> {
        conn.tx_open = false;
        conn.recorded.push("RESET".to_string());
        Ok(())
    }

    async fn simple_query(&self, conn: &mut Self::Conn, sql: &str) -> Result<u64, PoolError> {
        conn.recorded.push(sql.to_string());
        Ok(0)
    }
}
