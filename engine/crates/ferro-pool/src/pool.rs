//! The hand-rolled pool (S4 Task 2, decision D9 — no `deadpool`/`bb8`).
//!
//! Checkout is semaphore-bounded (`max_size`) and measures `queue_us` — the time spent waiting
//! for a permit *and* for a usable connection to end up in hand (v2/m2). Release (`Checkout`'s
//! `Drop`) is fully synchronous (v2/B1): it returns the connection to the idle stack and records
//! `tx_open`/`tainted` flags, but never runs the async ROLLBACK/reset itself. That async cleanup
//! happens at the START of the *next* `checkout()` — the "recycle-on-next-checkout" model.

use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use ferro_proto::value::Value;

use crate::backend::{PoolBackend, QueryResult};
use crate::config::PoolConfig;
use crate::error::PoolError;
use crate::pin::{self, PinCause, PinState, TxId};

/// A connection sitting idle in the pool, plus the bookkeeping needed to recycle it safely on
/// the next checkout.
///
/// `pub(crate)` (struct + the two fields the reaper touches) so `health.rs`'s reaper can inspect
/// age and ping the connection directly; `tx_open`/`tainted` stay module-private since the reaper
/// never needs them (it only evicts or keeps a candidate whole, never reconstructs one).
pub(crate) struct IdleConn<B: PoolBackend> {
    pub(crate) conn: B::Conn,
    pub(crate) created_at: Instant,
    /// Set when the connection served a transaction that was not explicitly committed/rolled
    /// back before release; the next checkout runs a defensive `ROLLBACK` before handing it out.
    tx_open: bool,
    /// Set when the connection needs a hygiene reset (e.g. session state) before reuse.
    tainted: bool,
}

/// `pub(crate)` (+ every field) so `health::spawn_reaper`/`reap_once` can read `backend`/`config`,
/// lock `idle`, and (S4 CRITICAL fix) acquire its own owned permit from `semaphore` while pinging
/// a connection it has pulled out of `idle` — the mechanism that makes a pinged conn count against
/// `max_size` exactly like a checked-out one.
pub(crate) struct PoolInner<B: PoolBackend> {
    pub(crate) backend: B,
    pub(crate) config: PoolConfig,
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) idle: Mutex<Vec<IdleConn<B>>>,
}

/// A cloneable handle to a pool. Cloning shares the same underlying connections/semaphore/idle
/// stack (it's an `Arc` internally) — cloning does not create a second pool.
pub struct Pool<B: PoolBackend> {
    inner: Arc<PoolInner<B>>,
}

impl<B: PoolBackend> Clone for Pool<B> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<B: PoolBackend> Pool<B> {
    /// Builds a pool over `backend` with `config`. Spawns the background liveness reaper (Task 3,
    /// `health::spawn_reaper`) iff `config.reap_interval` is `Some`; a `None` interval leaves the
    /// pool exactly as reaper-less as Task 2 left it (needed for deterministic `start_paused`
    /// tests — v2/M3).
    pub fn new(backend: B, config: PoolConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_size));
        let reap_interval = config.reap_interval;
        let inner = Arc::new(PoolInner {
            backend,
            config,
            semaphore,
            idle: Mutex::new(Vec::new()),
        });
        if let Some(interval) = reap_interval {
            crate::health::spawn_reaper(&inner, interval);
        }
        Self { inner }
    }

    /// Checks out a connection, waiting up to `config.checkout_timeout` for a free permit and a
    /// usable connection. `queue_us` on the returned `Checkout` covers the whole wait, including
    /// any async cleanup (defensive ROLLBACK/reset) performed on a recycled idle connection.
    pub async fn checkout(&self) -> Result<Checkout<B>, PoolError> {
        let start = Instant::now();

        let acquire = Arc::clone(&self.inner.semaphore).acquire_owned();
        let permit = match tokio::time::timeout(self.inner.config.checkout_timeout, acquire).await {
            Ok(Ok(permit)) => permit,
            // The semaphore is never explicitly closed in M0; treat it as a (non-retryable) pool
            // shutdown rather than panicking.
            Ok(Err(_)) => return Err(PoolError::Closed),
            Err(_) => return Err(PoolError::Timeout),
        };

        loop {
            let popped = {
                let mut idle = self.inner.idle.lock().unwrap();
                idle.pop()
            };

            let Some(mut idle_conn) = popped else {
                // No idle connection: connect a fresh one, up to max_size (the permit already
                // bounds this). A connect failure surfaces immediately (v2/M5) — no hidden retry
                // loop here; `permit` drops on this early return, releasing capacity so it is not
                // leaked.
                return match self.inner.backend.connect().await {
                    Ok(conn) => {
                        let queue_us = start.elapsed().as_micros() as u64;
                        Ok(Checkout::new(
                            conn,
                            Instant::now(),
                            permit,
                            Arc::clone(&self.inner),
                            queue_us,
                        ))
                    }
                    Err(_) => Err(PoolError::ConnectionLost),
                };
            };

            // v2/B1 async cleanup at checkout, before handing out a popped idle conn.
            let too_old = idle_conn.created_at.elapsed() > self.inner.config.max_lifetime;
            if too_old || self.inner.backend.is_closed(&idle_conn.conn) {
                // Evict: drop the dead/expired conn and try the next idle one (or connect fresh).
                continue;
            }

            // BOUNDED recycle (S6 BLOCKER-half): a poisoned `tx_open`/`tainted` conn whose
            // defensive ROLLBACK/reset HANGS must not block a future checkout unboundedly, so the
            // cleanup runs under a timeout. On timeout — OR a cleanup error — EVICT the conn (drop
            // it and try the next idle one / connect fresh), never wait on it forever. NOTE: the
            // `checkout_timeout` at the top of `checkout()` wraps ONLY the permit acquire, not this
            // pop/rollback/reset loop, so without this bound a single wedged conn could stall every
            // subsequent checkout. Reuses `checkout_timeout` as the per-conn cleanup bound.
            if idle_conn.tx_open || idle_conn.tainted {
                let cleanup = async {
                    if idle_conn.tx_open {
                        self.inner
                            .backend
                            .simple_query(&mut idle_conn.conn, "ROLLBACK")
                            .await?;
                        idle_conn.tx_open = false;
                    }
                    if idle_conn.tainted {
                        self.inner.backend.reset(&mut idle_conn.conn).await?;
                        idle_conn.tainted = false;
                    }
                    Ok::<(), PoolError>(())
                };
                match tokio::time::timeout(self.inner.config.checkout_timeout, cleanup).await {
                    Ok(Ok(())) => {}        // cleaned: hand it out below
                    Ok(Err(_)) => continue, // cleanup errored: evict + try again
                    Err(_) => continue,     // cleanup timed out: evict (drop) + try again
                }
            }

            let queue_us = start.elapsed().as_micros() as u64;
            return Ok(Checkout::new(
                idle_conn.conn,
                idle_conn.created_at,
                permit,
                Arc::clone(&self.inner),
                queue_us,
            ));
        }
    }

    /// Test-support hook: mutates the most-recently-idled connection in place, bypassing the
    /// Drop-time liveness filter. Used to simulate a connection dying *after* it was already
    /// returned to the pool (e.g. a backend driver discovering EOF asynchronously while the
    /// connection sat idle) so the checkout-time eviction path (step 3, v2/B1) can be exercised
    /// directly. Not intended for production callers.
    #[doc(hidden)]
    pub fn poison_idle_for_test(&self, f: impl FnOnce(&mut B::Conn)) {
        let mut idle = self.inner.idle.lock().unwrap();
        if let Some(idle_conn) = idle.last_mut() {
            f(&mut idle_conn.conn);
        }
    }

    /// Read-only access to the backend this pool was constructed with. Mainly for tests that need
    /// to observe backend-internal state (e.g. `FakeBackend::total_connected()`/`pings_waiting()`)
    /// that isn't otherwise visible through the pool's own public surface.
    pub fn backend(&self) -> &B {
        &self.inner.backend
    }
}

/// Stats observable on a successful checkout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckoutStats {
    /// Microseconds spent waiting for a permit + a usable connection.
    pub queue_us: u64,
}

/// RAII guard for a checked-out connection. Returns the connection to the pool's idle stack on
/// `Drop` — synchronously, with no `.await` (v2/B1): the async ROLLBACK/reset for a
/// `tx_open`/`tainted` connection runs at the *next* checkout instead.
pub struct Checkout<B: PoolBackend> {
    conn: Option<B::Conn>,
    created_at: Instant,
    // Never read directly: held purely so capacity releases back to the semaphore when this
    // struct (and thus this field) drops.
    #[allow(dead_code)]
    permit: Option<OwnedSemaphorePermit>,
    pool: Arc<PoolInner<B>>,
    queue_us: u64,
    tx_open: bool,
    tainted: bool,
    /// S4 Task 4 pin stub: set by `begin_tx`, cleared by `commit_tx`/`rollback_tx`. Always starts
    /// `Unpinned` — a fresh `Checkout` (even one that recycled an idle conn with a stale `tx_open`
    /// flag) never inherits pin state from a previous holder.
    pin: PinState,
    /// The most recent pin cause observed on this `Checkout` (for the pin-cause DoD assertion).
    /// Only ever `Some(PinCause::Tx)` in S4.
    last_pin_cause: Option<PinCause>,
}

impl<B: PoolBackend> Checkout<B> {
    fn new(
        conn: B::Conn,
        created_at: Instant,
        permit: OwnedSemaphorePermit,
        pool: Arc<PoolInner<B>>,
        queue_us: u64,
    ) -> Self {
        Self {
            conn: Some(conn),
            created_at,
            permit: Some(permit),
            pool,
            queue_us,
            tx_open: false,
            tainted: false,
            pin: PinState::Unpinned,
            last_pin_cause: None,
        }
    }

    /// Borrows the underlying connection.
    pub fn conn(&self) -> &B::Conn {
        self.conn.as_ref().expect("Checkout conn taken before Drop")
    }

    /// Mutably borrows the underlying connection.
    pub fn conn_mut(&mut self) -> &mut B::Conn {
        self.conn.as_mut().expect("Checkout conn taken before Drop")
    }

    /// Stats for this checkout (currently just `queue_us`).
    pub fn stats(&self) -> CheckoutStats {
        CheckoutStats {
            queue_us: self.queue_us,
        }
    }

    /// Marks (or clears) this connection as having an open transaction. Used by the pin task
    /// (Task 4) so release performs a defensive `ROLLBACK` on the *next* checkout rather than in
    /// `Drop`.
    pub fn set_tx_open(&mut self, open: bool) {
        self.tx_open = open;
    }

    /// Marks (or clears) this connection as needing a hygiene reset before reuse. Used by the pin
    /// task (Task 4).
    pub fn set_tainted(&mut self, tainted: bool) {
        self.tainted = tainted;
    }

    /// The pin hook: opens a transaction on the underlying connection with an ENGINE-COMPOSED
    /// `begin_sql` (e.g. `BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY`) and pins this `Checkout`
    /// to `tx_id` (S6). Drives the RAW, unguarded `PoolBackend::simple_query` (never
    /// `Checkout::query`/`exec` — those guarded entries reject bare tx-control, which would reject
    /// the pin hook's own BEGIN). `begin_sql` MUST be engine-composed, never client-raw SQL — the
    /// TX service composes it from the isolation/readonly request fields.
    ///
    /// Sets `pin`/`last_pin_cause(Tx)`/`tx_open` identically to the plain [`Checkout::begin_tx`]
    /// (which is just `begin_tx_with(id, "BEGIN")`).
    pub async fn begin_tx_with(&mut self, tx_id: TxId, begin_sql: &str) -> Result<(), PoolError> {
        let pool = Arc::clone(&self.pool);
        let conn = self.conn.as_mut().expect("Checkout conn taken before Drop");
        pool.backend.simple_query(conn, begin_sql).await?;
        self.pin = PinState::PinnedTx(tx_id);
        self.last_pin_cause = Some(PinCause::Tx);
        self.tx_open = true;
        Ok(())
    }

    /// Plain BEGIN pin hook — `begin_tx_with(tx_id, "BEGIN")`. For S4 this is called directly by
    /// tests; the TX service (S6) uses `begin_tx_with` with a composed BEGIN.
    pub async fn begin_tx(&mut self, tx_id: TxId) -> Result<(), PoolError> {
        self.begin_tx_with(tx_id, "BEGIN").await
    }

    /// ENGINE-ONLY transaction-control passthrough (S6): runs `sql` via the RAW, UNGUARDED
    /// `PoolBackend::simple_query` — with **NO `is_bare_tx_control` guard** — for engine-COMPOSED
    /// `SAVEPOINT sp_n` / `RELEASE sp_n` / `ROLLBACK TO sp_n` on a pinned connection.
    ///
    /// MUST NEVER receive client-raw SQL. The guard on the user-facing [`Checkout::query`] /
    /// [`Checkout::exec`] is what stops an `EXEC BEGIN`/`SAVEPOINT` from opening/managing an
    /// untracked transaction the next tenant on this pooled connection inherits (a cross-tenant
    /// leak — charter rule 6); this method deliberately bypasses that guard, so ONLY the engine's
    /// own composed savepoint statements (names the engine generates, never a client string) may
    /// flow through it.
    pub async fn tx_control(&mut self, sql: &str) -> Result<(), PoolError> {
        let pool = Arc::clone(&self.pool);
        let conn = self.conn.as_mut().expect("Checkout conn taken before Drop");
        pool.backend.simple_query(conn, sql).await.map(|_| ())
    }

    /// An out-of-band handle to cancel this connection's in-flight server statement (S6), WITHOUT
    /// borrowing the `Checkout` while a query future is live. Grab it BEFORE starting an
    /// interruptible statement (it borrows nothing the query future needs) and move it into a
    /// separate task/`select!` arm that fires the cancel on a deadline/abort. For Postgres it is a
    /// `tokio_postgres::CancelToken` that cancels via a SIDE connection; the pool stays
    /// backend-agnostic (returns `B::CancelHandle`).
    pub fn cancel_handle(&self) -> B::CancelHandle {
        let conn = self.conn.as_ref().expect("Checkout conn taken before Drop");
        self.pool.backend.cancel_handle(conn)
    }

    /// COMMITs the pinned transaction and unpins this `Checkout`. Raw `simple_query`, same
    /// rationale as `begin_tx`.
    pub async fn commit_tx(&mut self) -> Result<(), PoolError> {
        let pool = Arc::clone(&self.pool);
        let conn = self.conn.as_mut().expect("Checkout conn taken before Drop");
        pool.backend.simple_query(conn, "COMMIT").await?;
        self.pin = PinState::Unpinned;
        self.tx_open = false;
        Ok(())
    }

    /// ROLLBACKs the pinned transaction and unpins this `Checkout`. Raw `simple_query`, same
    /// rationale as `begin_tx`. Distinct from the defensive ROLLBACK the pool runs on the *next*
    /// checkout of a conn dropped mid-transaction (v2/B1) — this is the explicit, caller-driven
    /// rollback.
    pub async fn rollback_tx(&mut self) -> Result<(), PoolError> {
        let pool = Arc::clone(&self.pool);
        let conn = self.conn.as_mut().expect("Checkout conn taken before Drop");
        pool.backend.simple_query(conn, "ROLLBACK").await?;
        self.pin = PinState::Unpinned;
        self.tx_open = false;
        Ok(())
    }

    /// Current pin state (`Unpinned` or `PinnedTx(tx_id)`).
    pub fn pin_state(&self) -> PinState {
        self.pin
    }

    /// The most recent pin cause observed on this `Checkout` (the pin-cause DoD assertion). Only
    /// ever `Some(PinCause::Tx)` in S4.
    pub fn last_pin_cause(&self) -> Option<PinCause> {
        self.last_pin_cause
    }

    /// The guarded, user-facing statement entry (v2/M1). Rejects bare transaction-control
    /// statements (`BEGIN`, `START TRANSACTION`, `SAVEPOINT`, `COMMIT`, `END`, `ROLLBACK`,
    /// `ABORT`, `RELEASE`, `PREPARE TRANSACTION` — case-insensitive leading keyword, v2/M2) with
    /// `PoolError::Unsupported` so the pin stub cannot be bypassed; the real TX path is the TX
    /// service (S6). Anything else goes straight to the raw `PoolBackend::simple_query`.
    pub async fn exec(&mut self, sql: &str) -> Result<u64, PoolError> {
        if pin::is_bare_tx_control(sql) {
            return Err(PoolError::Unsupported(format!(
                "bare transaction-control statement not allowed via exec(): {sql:?} \
                 (use the TX service instead)"
            )));
        }
        let pool = Arc::clone(&self.pool);
        let conn = self.conn.as_mut().expect("Checkout conn taken before Drop");
        pool.backend.simple_query(conn, sql).await
    }

    /// The guarded, user-facing **row-returning** statement entry (S5, BLOCKER-2). Mirrors
    /// [`Checkout::exec`]'s guard structure EXACTLY: it runs `pin::is_bare_tx_control(sql)` FIRST
    /// and rejects a bare transaction-control statement (`BEGIN`/`COMMIT`/`ROLLBACK`/…, leading
    /// comment/whitespace tolerant) with `PoolError::Unsupported` — so an `EXEC BEGIN` can never
    /// reach the raw client and open an untracked transaction that the next tenant on this pooled
    /// connection would inherit (a cross-tenant leak; charter rule 6). Only a non-tx-control
    /// statement is delegated to `PoolBackend::query`.
    pub async fn query(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, PoolError> {
        if pin::is_bare_tx_control(sql) {
            return Err(PoolError::Unsupported(format!(
                "bare transaction-control statement not allowed via query(): {sql:?} \
                 (use the TX service instead)"
            )));
        }
        let pool = Arc::clone(&self.pool);
        let conn = self.conn.as_mut().expect("Checkout conn taken before Drop");
        pool.backend.query(conn, sql, params).await
    }
}

impl<B: PoolBackend> Drop for Checkout<B> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Only return live connections to the idle stack; a connection the backend already
            // considers closed is simply dropped (the permit still releases below).
            if !self.pool.backend.is_closed(&conn) {
                let mut idle = self.pool.idle.lock().unwrap();
                idle.push(IdleConn {
                    conn,
                    created_at: self.created_at,
                    tx_open: self.tx_open,
                    tainted: self.tainted,
                });
            }
        }
        // `self.permit` (an `Option<OwnedSemaphorePermit>`) drops here along with the rest of the
        // struct's fields, releasing capacity back to the semaphore. Fully synchronous: no
        // `.await` anywhere in this Drop impl.
    }
}
