//! Deterministic in-memory `PoolBackend` for fast pool-semantics tests (Task 1). Later tasks
//! (checkout/release, max_lifetime, pin stub) drive this backend instead of a live Postgres so
//! the pool's mechanics are tested without a Docker dependency.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::backend::{PoolBackend, QueryResult};
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

/// The fake backend's no-op cancel handle (S6). There is no server statement to cancel, so this is
/// inert — its purpose is to exercise the `Checkout::cancel_handle` -> `B::CancelHandle` surface
/// (that it is callable on `&self` and returns a `Send + 'static` handle) without a live Postgres.
#[derive(Debug, Clone)]
pub struct FakeCancelHandle;

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
    /// When `Some`, every `ping()` call parks on this `Notify` until `release_pings()` clears it
    /// and wakes any waiters. Armed via `block_pings` -- lets a test deterministically freeze the
    /// reaper mid-ping (holding its owned semaphore permit) before issuing a concurrent burst of
    /// checkouts (S4 reaper-cap regression test).
    ping_gate: Mutex<Option<Arc<Notify>>>,
    /// Number of `ping()` calls currently parked on `ping_gate`. Tests poll this until it is `> 0`
    /// to prove the reaper has actually entered the blocked ping, rather than racing on timing.
    pings_waiting: AtomicU64,
    /// Canned result returned by `query()` (S5). Defaults to an empty `QueryResult`; a test arms
    /// it via `set_query_result` so the guarded `Checkout::query` path can be exercised (both the
    /// tx-control rejection AND a normal row-returning return) without a live Postgres.
    canned_query: Mutex<QueryResult>,
    /// When `Some`, every `simple_query()` call parks on this `Notify` until `release_simple_query`
    /// clears it (S6). Lets a test freeze the checkout-time recycle ROLLBACK to prove the
    /// bounded-recycle timeout EVICTS the poisoned conn rather than hanging a future checkout.
    simple_query_gate: Mutex<Option<Arc<Notify>>>,
}

impl FakeBackend {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(0),
            fail_connect_remaining: AtomicU64::new(0),
            ping_gate: Mutex::new(None),
            pings_waiting: AtomicU64::new(0),
            canned_query: Mutex::new(QueryResult::default()),
            simple_query_gate: Mutex::new(None),
        }
    }

    /// Arms the `QueryResult` that every subsequent `query()` returns (S5). Lets a test drive the
    /// guarded `Checkout::query` path — assert a bare `BEGIN` is rejected BEFORE the backend is
    /// reached, and that a normal statement returns exactly these canned rows.
    pub fn set_query_result(&self, result: QueryResult) {
        *self.canned_query.lock().unwrap() = result;
    }

    /// Arms the next `n` `connect()` calls to fail with `PoolError::ConnectionLost`.
    pub fn arm_fail_connect(&self, n: u64) {
        self.fail_connect_remaining.store(n, Ordering::SeqCst);
    }

    /// Arms every subsequent `ping()` call to block until `release_pings()` is called. Used by the
    /// reaper-cap regression test to freeze the reaper mid-ping -- and thus holding its owned
    /// semaphore permit -- while a concurrent burst of checkouts runs.
    pub fn block_pings(&self) {
        *self.ping_gate.lock().unwrap() = Some(Arc::new(Notify::new()));
    }

    /// Releases every `ping()` call currently parked by `block_pings()` and clears the gate so
    /// future `ping()` calls are no longer affected.
    pub fn release_pings(&self) {
        if let Some(notify) = self.ping_gate.lock().unwrap().take() {
            notify.notify_waiters();
        }
    }

    /// Number of `ping()` calls currently parked on the gate armed by `block_pings()`.
    pub fn pings_waiting(&self) -> u64 {
        self.pings_waiting.load(Ordering::SeqCst)
    }

    /// Arms every subsequent `simple_query()` call to block until `release_simple_query()` (S6).
    /// Used by the bounded-recycle test to freeze the checkout-time defensive ROLLBACK.
    pub fn block_simple_query(&self) {
        *self.simple_query_gate.lock().unwrap() = Some(Arc::new(Notify::new()));
    }

    /// Releases every `simple_query()` call currently parked by `block_simple_query()` and clears
    /// the gate so future calls are unaffected.
    pub fn release_simple_query(&self) {
        if let Some(notify) = self.simple_query_gate.lock().unwrap().take() {
            notify.notify_waiters();
        }
    }

    /// Total number of DISTINCT connections ever created by `connect()` (i.e. the next id that
    /// would be handed out). Used by the reaper-cap test to assert the pool never over-provisions
    /// past `max_size`.
    pub fn total_connected(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl PoolBackend for FakeBackend {
    type Conn = FakeConn;
    type CancelHandle = FakeCancelHandle;

    fn cancel_handle(&self, _conn: &Self::Conn) -> Self::CancelHandle {
        FakeCancelHandle
    }

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
        // Test-only gate (see `block_pings`/`release_pings`): if armed, park here until released.
        // The counter is bumped *before* the (only) await point below, in the same synchronous
        // span -- so once a test observes `pings_waiting() > 0` this call has already registered
        // as a waiter on `notify`, and a later `release_pings()` cannot race a lost wakeup.
        let gate = self.ping_gate.lock().unwrap().clone();
        if let Some(notify) = gate {
            self.pings_waiting.fetch_add(1, Ordering::SeqCst);
            notify.notified().await;
            self.pings_waiting.fetch_sub(1, Ordering::SeqCst);
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
        // Test-only gate (see `block_simple_query`/`release_simple_query`): if armed, park here so
        // the bounded-recycle test can freeze a checkout-time defensive ROLLBACK and prove the
        // recycle timeout evicts the conn instead of hanging.
        let gate = self.simple_query_gate.lock().unwrap().clone();
        if let Some(notify) = gate {
            notify.notified().await;
        }
        Ok(0)
    }

    async fn query(
        &self,
        conn: &mut Self::Conn,
        sql: &str,
        _params: &[ferro_proto::value::Value],
    ) -> Result<QueryResult, PoolError> {
        // Record the SQL that actually reached the backend: a guard-rejected statement never
        // reaches here, so a test can assert `recorded` to prove `Checkout::query`'s guard fired
        // (or didn't) before delegation.
        conn.recorded.push(sql.to_string());
        Ok(self.canned_query.lock().unwrap().clone())
    }
}
