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

use crate::backend::PoolBackend;
use crate::config::PoolConfig;
use crate::error::PoolError;

/// A connection sitting idle in the pool, plus the bookkeeping needed to recycle it safely on
/// the next checkout.
struct IdleConn<B: PoolBackend> {
    conn: B::Conn,
    created_at: Instant,
    /// Set when the connection served a transaction that was not explicitly committed/rolled
    /// back before release; the next checkout runs a defensive `ROLLBACK` before handing it out.
    tx_open: bool,
    /// Set when the connection needs a hygiene reset (e.g. session state) before reuse.
    tainted: bool,
}

struct PoolInner<B: PoolBackend> {
    backend: B,
    config: PoolConfig,
    semaphore: Arc<Semaphore>,
    idle: Mutex<Vec<IdleConn<B>>>,
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
    /// Builds a pool over `backend` with `config`. Does not spawn a reaper — Task 3 wires that up
    /// when `config.reap_interval` is `Some`; Task 2's `Pool` is always reaper-less.
    pub fn new(backend: B, config: PoolConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_size));
        Self {
            inner: Arc::new(PoolInner {
                backend,
                config,
                semaphore,
                idle: Mutex::new(Vec::new()),
            }),
        }
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

            if idle_conn.tx_open {
                match self
                    .inner
                    .backend
                    .simple_query(&mut idle_conn.conn, "ROLLBACK")
                    .await
                {
                    Ok(_) => idle_conn.tx_open = false,
                    Err(_) => continue, // cleanup failed: evict + try again
                }
            }

            if idle_conn.tainted {
                match self.inner.backend.reset(&mut idle_conn.conn).await {
                    Ok(_) => idle_conn.tainted = false,
                    Err(_) => continue, // cleanup failed: evict + try again
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
