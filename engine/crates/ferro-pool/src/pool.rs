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

use ferro_proto::messages::sql::ColMeta;
use ferro_proto::value::Value;

use crate::backend::{BackendRows, PoolBackend, QueryResult, ResetProfile, TxStatus};
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

/// What [`Checkout::guard_tx_control`] decided about one statement on one guarded entry (M1-S8a).
/// A refusal is an `Err` from the guard, so this enum only ever names the ALLOWED outcomes — and
/// every call site matches it exhaustively, so a future variant breaks the build rather than
/// silently falling into "run it normally".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxControlVerdict {
    /// Not transaction-control-shaped — run it on the entry's normal path.
    Proceed,
    /// A savepoint operation on a checkout that already has a transaction open. Allowed, but it
    /// MUST run on the raw TEXT leaf (`PoolBackend::simple_query`) — see [`Checkout::query`].
    SavepointPassthrough,
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
            //
            // M1-S3 (SPEC §7.2): a NON-tainted recycled conn now ALSO needs cleanup (the backend's
            // `clean_reset_profile()` targeted reset — the §7.4 assist-lexer blind-spot backstop),
            // so the guard widens to `tx_open || tainted || clean_reset_profile().is_some()`. A
            // backend that reports `None` (a future MySQL known-clean tracker) still SKIPS the
            // timeout wrapper entirely when there is genuinely nothing to do.
            if idle_conn.tx_open
                || idle_conn.tainted
                || self.inner.backend.clean_reset_profile().is_some()
            {
                let cleanup = async {
                    if idle_conn.tx_open {
                        self.inner
                            .backend
                            .simple_query(&mut idle_conn.conn, "ROLLBACK")
                            .await?;
                        idle_conn.tx_open = false;
                    }
                    // §7.2 conditional profile: a tainted conn (detected session mutation / error /
                    // aborted tx) gets the FULL reset; a non-tainted recycled conn gets the
                    // backend's clean profile (PG: Targeted — the §7.4 blind-spot backstop; MySQL
                    // later: None when the tracker says clean).
                    let profile = if idle_conn.tainted {
                        Some(ResetProfile::Full)
                    } else {
                        self.inner.backend.clean_reset_profile()
                    };
                    if let Some(p) = profile {
                        self.inner.backend.reset(&mut idle_conn.conn, p).await?;
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
    /// Pin identity: set by `begin_tx_with` (the real `TxId`), cleared by `commit_tx`/`rollback_tx`.
    /// Always starts `Unpinned` — a fresh `Checkout` (even one that recycled an idle conn with a
    /// stale `tx_open` flag) never inherits pin state from a previous holder. The M1-S1 RFQ
    /// authority (`apply_tx_status`) moves the reuse-safety bits (`tx_open`/`tainted`) but NEVER
    /// this identity field: it must not clobber a real `TxId`, nor fabricate one for an RFQ-only tx.
    pin: PinState,
    /// The most recent pin cause observed on this `Checkout` (for the pin-cause DoD assertion).
    /// `Some(PinCause::Tx)` from the RFQ tx-authority path (`apply_tx_status`, M1-S1); any of the
    /// other seven assist causes (`Listen`/`AdvisoryLock`/`Prepare`/`Temp`/`Set`/`PinFunction`/
    /// `Unknown`) from the classifier (`apply_classify`, M1-S2).
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
    ///
    /// CONTRACT: this is a raw-client side door that BYPASSES the M1-S1 RFQ pin authority
    /// entirely — executing a statement directly against the borrowed `B::Conn` (e.g. calling
    /// `tokio_postgres::Client::batch_execute`/`query` on `PgConn::client` instead of going
    /// through `Checkout::exec`/`query`/`begin_tx_with`/`tx_control`/`commit_tx`/`rollback_tx`)
    /// means NO `tx_status()` read runs afterward and NONE of the Err-arm fail-safe forcing
    /// (`tx_open`/`tainted`) applies. A statement run this way can silently leave the connection
    /// mid-transaction or aborted with this `Checkout` never finding out, so the next tenant that
    /// checks out this same pooled connection can inherit an open/aborted tx (a cross-tenant leak
    /// — charter rule 6). This accessor is for NON-STATEMENT inspection only (e.g. reading
    /// `pg_backend_pid()`-style state in tests) — any statement execution MUST go through one of
    /// the instrumented `Checkout` methods above, never through `conn_mut()` directly. See
    /// `ferrod/src/services/sql.rs`'s "NEVER conn_mut()/the raw client here" comment for the
    /// production-path enforcement of this rule.
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

    /// Whether this connection currently has an OPEN transaction, per the authoritative RFQ status
    /// (`apply_tx_status`) — the reuse-safety bit that makes the next checkout run a defensive
    /// `ROLLBACK`. Exposed so the RFQ-pin tests can assert the reuse-safety bits directly.
    ///
    /// M1-S8a: this is ALSO the condition [`Checkout::guard_tx_control`] reads to decide whether a
    /// savepoint statement may pass through. It stays a plain `pub fn` (never `#[cfg(test)]`)
    /// because `tests/query_guard.rs` is a separate crate that asserts through it.
    pub fn tx_open(&self) -> bool {
        self.tx_open
    }

    /// Whether this connection needs a hygiene reset before reuse (an aborted tx `E`, or ANY error
    /// on an instrumented statement). Set by `apply_tx_status(Failed)` and — as the Rule-A
    /// fail-safe — UNCONDITIONALLY on any `Err` arm (alongside `tx_open`), since the Err-arm RFQ
    /// atomic is untrustworthy and a batch can open a tx before erroring. Only ever cleared by the
    /// checkout-time recycle.
    pub fn tainted(&self) -> bool {
        self.tainted
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
        // RFQ authority (SPEC §7.1): read tx_status on BOTH arms; it is trustworthy only on the Ok
        // arm (post-drain), so we never `?` before the read and the guard below fails safe on Err.
        let r = pool.backend.simple_query(self.conn_mut(), begin_sql).await;
        // Defense-in-depth (kept from the M0 stub): on a successful BEGIN, record the real `TxId` +
        // cause + `tx_open` BY HAND *before* the RFQ read, so `apply_tx_status(InTx)` then CONFIRMS
        // `tx_open` without clobbering the real `TxId` (RFQ is additive authority here, not a
        // replacement of the manual pin).
        if r.is_ok() {
            self.pin = PinState::PinnedTx(tx_id);
            self.last_pin_cause = Some(PinCause::Tx);
            self.tx_open = true;
        }
        let st = pool.backend.tx_status(self.conn());
        self.apply_tx_status(st);
        // M1-S6: the SECOND assist signal, read at the same post-statement point. No-op for PG/Fake
        // (default `take_session_mutated -> false`); taints + labels `SessionTracker` for MySQL.
        self.apply_session_tracker();
        if r.is_err() {
            // Rule A fail-safe (uniform across all 6 instrumented methods): on Err the RFQ atomic
            // is stale-UNTRUSTWORTHY — postgres-protocol returns Err at `ErrorResponse` BEFORE the
            // trailing `ReadyForQuery` is decoded — AND a statement can OPEN a tx before erroring:
            // `exec` forwards a multi-statement batch to `batch_execute`, and the tx-control
            // guard only classifies the LEADING keyword, so `SELECT 1; BEGIN; SELECT 1/0` passes the guard,
            // opens a tx mid-batch from autocommit, then errors — leaving an OPEN, ABORTED tx while
            // the atomic still reads stale-`Idle`. So neither `apply_tx_status` nor a pre-captured
            // `tx_open` can be trusted here. Force BOTH bits UNCONDITIONALLY so the checkout-time
            // recycle runs `ROLLBACK` *then* `DISCARD ALL` (that order is required — DISCARD ALL
            // cannot run inside a tx block) and a possibly-poisoned conn is NEVER handed to the next
            // tenant (charter rule 6). Over-forcing on a genuinely-clean autocommit error (PG
            // auto-rolls-back to Idle) costs only one harmless extra ROLLBACK+reset at the next
            // checkout — the safe direction (charter rule 5: correctness over throughput).
            self.tx_open = true;
            self.tainted = true;
        }
        r.map(|_| ())
    }

    /// Plain BEGIN pin hook — `begin_tx_with(tx_id, "BEGIN")`. For S4 this is called directly by
    /// tests; the TX service (S6) uses `begin_tx_with` with a composed BEGIN.
    pub async fn begin_tx(&mut self, tx_id: TxId) -> Result<(), PoolError> {
        self.begin_tx_with(tx_id, "BEGIN").await
    }

    /// ENGINE-ONLY transaction-control passthrough (S6): runs `sql` via the RAW, UNGUARDED
    /// `PoolBackend::simple_query` — with **NO tx-control guard at all** — for engine-COMPOSED
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
        // RFQ authority on the Ok arm only (a savepoint op keeps the tx `T`, so tx_open stays set);
        // on Err the atomic may be stale, so the guard below taints any error while a tx is open.
        let r = pool
            .backend
            .simple_query(self.conn_mut(), sql)
            .await
            .map(|_| ());
        let st = pool.backend.tx_status(self.conn());
        self.apply_tx_status(st);
        // M1-S6: the SECOND assist signal, read at the same post-statement point. No-op for PG/Fake
        // (default `take_session_mutated -> false`); taints + labels `SessionTracker` for MySQL.
        self.apply_session_tracker();
        if r.is_err() {
            // Rule A fail-safe (uniform across all 6 instrumented methods): on Err the RFQ atomic
            // is stale-UNTRUSTWORTHY — postgres-protocol returns Err at `ErrorResponse` BEFORE the
            // trailing `ReadyForQuery` is decoded — AND a statement can OPEN a tx before erroring:
            // `exec` forwards a multi-statement batch to `batch_execute`, and the tx-control
            // guard only classifies the LEADING keyword, so `SELECT 1; BEGIN; SELECT 1/0` passes the guard,
            // opens a tx mid-batch from autocommit, then errors — leaving an OPEN, ABORTED tx while
            // the atomic still reads stale-`Idle`. So neither `apply_tx_status` nor a pre-captured
            // `tx_open` can be trusted here. Force BOTH bits UNCONDITIONALLY so the checkout-time
            // recycle runs `ROLLBACK` *then* `DISCARD ALL` (that order is required — DISCARD ALL
            // cannot run inside a tx block) and a possibly-poisoned conn is NEVER handed to the next
            // tenant (charter rule 6). Over-forcing on a genuinely-clean autocommit error (PG
            // auto-rolls-back to Idle) costs only one harmless extra ROLLBACK+reset at the next
            // checkout — the safe direction (charter rule 5: correctness over throughput).
            self.tx_open = true;
            self.tainted = true;
        }
        r
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
        // RFQ authority on the Ok arm only; on Err the atomic may be stale (guard below).
        let r = pool.backend.simple_query(self.conn_mut(), "COMMIT").await;
        // Defense-in-depth (kept): on a successful COMMIT, unpin + clear `tx_open` by hand; the RFQ
        // read below (`apply_tx_status(Idle)`) then CONFIRMS the conn is out of the tx.
        if r.is_ok() {
            self.pin = PinState::Unpinned;
            self.tx_open = false;
        }
        let st = pool.backend.tx_status(self.conn());
        self.apply_tx_status(st);
        // M1-S6: the SECOND assist signal, read at the same post-statement point. No-op for PG/Fake
        // (default `take_session_mutated -> false`); taints + labels `SessionTracker` for MySQL.
        self.apply_session_tracker();
        if r.is_err() {
            // Rule A fail-safe (uniform across all 6 instrumented methods): on Err the RFQ atomic
            // is stale-UNTRUSTWORTHY — postgres-protocol returns Err at `ErrorResponse` BEFORE the
            // trailing `ReadyForQuery` is decoded — AND a statement can OPEN a tx before erroring:
            // `exec` forwards a multi-statement batch to `batch_execute`, and the tx-control
            // guard only classifies the LEADING keyword, so `SELECT 1; BEGIN; SELECT 1/0` passes the guard,
            // opens a tx mid-batch from autocommit, then errors — leaving an OPEN, ABORTED tx while
            // the atomic still reads stale-`Idle`. So neither `apply_tx_status` nor a pre-captured
            // `tx_open` can be trusted here. Force BOTH bits UNCONDITIONALLY so the checkout-time
            // recycle runs `ROLLBACK` *then* `DISCARD ALL` (that order is required — DISCARD ALL
            // cannot run inside a tx block) and a possibly-poisoned conn is NEVER handed to the next
            // tenant (charter rule 6). Over-forcing on a genuinely-clean autocommit error (PG
            // auto-rolls-back to Idle) costs only one harmless extra ROLLBACK+reset at the next
            // checkout — the safe direction (charter rule 5: correctness over throughput).
            self.tx_open = true;
            self.tainted = true;
        }
        r.map(|_| ())
    }

    /// ROLLBACKs the pinned transaction and unpins this `Checkout`. Raw `simple_query`, same
    /// rationale as `begin_tx`. Distinct from the defensive ROLLBACK the pool runs on the *next*
    /// checkout of a conn dropped mid-transaction (v2/B1) — this is the explicit, caller-driven
    /// rollback.
    pub async fn rollback_tx(&mut self) -> Result<(), PoolError> {
        let pool = Arc::clone(&self.pool);
        // RFQ authority on the Ok arm only; on Err the atomic may be stale (guard below).
        let r = pool.backend.simple_query(self.conn_mut(), "ROLLBACK").await;
        // Defense-in-depth (kept): on a successful ROLLBACK, unpin + clear `tx_open` by hand; the RFQ
        // read below CONFIRMS the conn is `Idle` again. Any pre-existing `tainted` deliberately
        // survives (a clean `Idle` does not clear it) — the next checkout eats one DISCARD-ALL
        // reset; safe/conservative.
        if r.is_ok() {
            self.pin = PinState::Unpinned;
            self.tx_open = false;
        }
        let st = pool.backend.tx_status(self.conn());
        self.apply_tx_status(st);
        // M1-S6: the SECOND assist signal, read at the same post-statement point. No-op for PG/Fake
        // (default `take_session_mutated -> false`); taints + labels `SessionTracker` for MySQL.
        self.apply_session_tracker();
        if r.is_err() {
            // Rule A fail-safe (uniform across all 6 instrumented methods): on Err the RFQ atomic
            // is stale-UNTRUSTWORTHY — postgres-protocol returns Err at `ErrorResponse` BEFORE the
            // trailing `ReadyForQuery` is decoded — AND a statement can OPEN a tx before erroring:
            // `exec` forwards a multi-statement batch to `batch_execute`, and the tx-control
            // guard only classifies the LEADING keyword, so `SELECT 1; BEGIN; SELECT 1/0` passes the guard,
            // opens a tx mid-batch from autocommit, then errors — leaving an OPEN, ABORTED tx while
            // the atomic still reads stale-`Idle`. So neither `apply_tx_status` nor a pre-captured
            // `tx_open` can be trusted here. Force BOTH bits UNCONDITIONALLY so the checkout-time
            // recycle runs `ROLLBACK` *then* `DISCARD ALL` (that order is required — DISCARD ALL
            // cannot run inside a tx block) and a possibly-poisoned conn is NEVER handed to the next
            // tenant (charter rule 6). Over-forcing on a genuinely-clean autocommit error (PG
            // auto-rolls-back to Idle) costs only one harmless extra ROLLBACK+reset at the next
            // checkout — the safe direction (charter rule 5: correctness over throughput).
            self.tx_open = true;
            self.tainted = true;
        }
        r.map(|_| ())
    }

    /// Current pin state (`Unpinned` or `PinnedTx(tx_id)`).
    pub fn pin_state(&self) -> PinState {
        self.pin
    }

    /// The most recent pin cause observed on this `Checkout` (the pin-cause DoD assertion).
    /// `Some(PinCause::Tx)` comes from the RFQ tx-authority path (`apply_tx_status`, M1-S1);
    /// the assist lexer (`apply_classify`, M1-S2) can additionally set any of `Listen`,
    /// `AdvisoryLock`, `Prepare`, `Temp`, `Set`, `PinFunction`, or `Unknown`.
    pub fn last_pin_cause(&self) -> Option<PinCause> {
        self.last_pin_cause
    }

    /// The tx-control guard shared by [`Checkout::exec`], [`Checkout::query`] and
    /// [`Checkout::query_stream`] — ONE implementation, so a future entry point cannot be added
    /// with a subtly different rule (before M1-S8a the rule was copy-pasted three times).
    ///
    /// Exhaustive over [`pin::TxControlClass`] on purpose: a new class breaks the build here rather
    /// than silently falling into "allowed".
    ///
    /// **Why savepoints are different from boundary verbs.** A BOUNDARY verb changes whether a
    /// transaction is OPEN — the pin AUTHORITY — so one slipping through a guarded entry would
    /// leave a transaction the pool believes is not open on a connection that then returns to the
    /// pool for the next tenant (charter rule 6), with no `tx_id`, no actor, no deadline and no
    /// rollback-on-session-death. A SAVEPOINT verb does none of that: it cannot change transaction
    /// status (real Postgres's `ReadyForQuery` byte does not flip on any of them — the reason
    /// [`pin::leading_tx_verb`] has classified them "preserve" since M1-S1), and every savepoint is
    /// destroyed by the enclosing transaction's commit or rollback, which the tx actor owns on
    /// every exit path. So a savepoint may pass through *inside a transaction*, which is what makes
    /// Doctrine's nested-transaction emulation (`SAVEPOINT DOCTRINE_1` / `RELEASE SAVEPOINT …` /
    /// `ROLLBACK TO SAVEPOINT …`, all plain `exec()` SQL, never a driver API) work.
    ///
    /// **The three things a savepoint passthrough must ALSO satisfy**, all of them REFUSALS (each
    /// can only reject a statement that might have been fine — never admit one that is not):
    ///
    /// 1. **A transaction must already be open.** Not for symmetry: MySQL silently IGNORES a bare
    ///    `SAVEPOINT` under autocommit (no transaction is started and the savepoint has no effect),
    ///    so delegating would hand the caller a rollback point that does not exist. (`ROLLBACK TO`
    ///    / `RELEASE` under autocommit are already loud — measured `1305`/`42000` on MariaDB,
    ///    `25P01` on PG — the rule covers all three because a rule that is loud for one verb and
    ///    delegated for two is a rule nobody can reason about.)
    /// 2. **No bound parameters.** A savepoint passthrough runs on the raw TEXT leaf, which cannot
    ///    bind; accepting params would silently drop them (charter rule 6, no silent miscasts).
    /// 3. **A LONE statement** ([`pin::is_lone_statement`]). The text leaf executes EVERY statement
    ///    in the string on both engines, so without this, admitting a leading `SAVEPOINT` would
    ///    also admit the `COMMIT` riding behind it in `SAVEPOINT s; COMMIT` — a boundary verb
    ///    through the front door this guard exists to close.
    fn guard_tx_control(
        &self,
        sql: &str,
        params: &[Value],
        entry: &str,
    ) -> Result<TxControlVerdict, PoolError> {
        match pin::tx_control_class(sql) {
            None => Ok(TxControlVerdict::Proceed),
            Some(pin::TxControlClass::Savepoint) if self.tx_open => {
                if !params.is_empty() {
                    return Err(PoolError::Unsupported(format!(
                        "savepoint statement with bound parameters not allowed via {entry}(): \
                         {sql:?} (a savepoint passthrough runs on the text protocol, which cannot \
                         bind — inline the name instead)"
                    )));
                }
                if !pin::is_lone_statement(sql) {
                    return Err(PoolError::Unsupported(format!(
                        "compound savepoint statement not allowed via {entry}(): {sql:?} \
                         (a savepoint passthrough must be a single statement — the text protocol \
                         would run every statement in the string, including a transaction-boundary \
                         verb behind the savepoint)"
                    )));
                }
                Ok(TxControlVerdict::SavepointPassthrough)
            }
            Some(pin::TxControlClass::Savepoint) => Err(PoolError::Unsupported(format!(
                "savepoint statement outside a transaction not allowed via {entry}(): {sql:?} \
                 (open a transaction first; MySQL silently ignores a savepoint under autocommit)"
            ))),
            Some(pin::TxControlClass::Boundary) => Err(PoolError::Unsupported(format!(
                "bare transaction-control statement not allowed via {entry}(): {sql:?} \
                 (use the TX service instead)"
            ))),
        }
    }

    /// The guarded, user-facing statement entry (v2/M1). Rejects a transaction-BOUNDARY statement
    /// (`BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, a bare `ROLLBACK`, `ABORT`,
    /// `PREPARE TRANSACTION` — case-insensitive leading keyword, v2/M2) with
    /// `PoolError::Unsupported` so the pin stub cannot be bypassed; the real TX path is the TX
    /// service (S6). A SAVEPOINT statement is allowed iff a transaction is already open (M1-S8a,
    /// SPEC §22.2 (r)) — and needs no special routing here, since this entry's normal path IS the
    /// raw text leaf a savepoint passthrough requires. Anything else goes straight to
    /// `PoolBackend::simple_query`.
    pub async fn exec(&mut self, sql: &str) -> Result<u64, PoolError> {
        // `exec` binds no parameters, so the guard's no-params rule is trivially satisfied.
        self.guard_tx_control(sql, &[], "exec")?;
        let pool = Arc::clone(&self.pool);
        // RFQ authority (SPEC §7.1): tx_status is trustworthy only on the Ok arm (post-drain); on
        // Err the atomic may hold a STALE byte, so we read it but let the `is_err() && tx_open`
        // guard below fail safe by tainting any error that occurs while a tx is open.
        let r = pool.backend.simple_query(self.conn_mut(), sql).await;
        let st = pool.backend.tx_status(self.conn());
        self.apply_tx_status(st);
        // M1-S6: the SECOND assist signal, read at the same post-statement point. No-op for PG/Fake
        // (default `take_session_mutated -> false`); taints + labels `SessionTracker` for MySQL.
        self.apply_session_tracker();
        if r.is_err() {
            // Rule A fail-safe (uniform across all 6 instrumented methods): on Err the RFQ atomic
            // is stale-UNTRUSTWORTHY — postgres-protocol returns Err at `ErrorResponse` BEFORE the
            // trailing `ReadyForQuery` is decoded — AND a statement can OPEN a tx before erroring:
            // `exec` forwards a multi-statement batch to `batch_execute`, and the tx-control
            // guard only classifies the LEADING keyword, so `SELECT 1; BEGIN; SELECT 1/0` passes the guard,
            // opens a tx mid-batch from autocommit, then errors — leaving an OPEN, ABORTED tx while
            // the atomic still reads stale-`Idle`. So neither `apply_tx_status` nor a pre-captured
            // `tx_open` can be trusted here. Force BOTH bits UNCONDITIONALLY so the checkout-time
            // recycle runs `ROLLBACK` *then* `DISCARD ALL` (that order is required — DISCARD ALL
            // cannot run inside a tx block) and a possibly-poisoned conn is NEVER handed to the next
            // tenant (charter rule 6). Over-forcing on a genuinely-clean autocommit error (PG
            // auto-rolls-back to Idle) costs only one harmless extra ROLLBACK+reset at the next
            // checkout — the safe direction (charter rule 5: correctness over throughput).
            self.tx_open = true;
            self.tainted = true;
        }
        // Assist signal (SPEC §7.1): runs on BOTH the Ok and Err arms — a session-mutating
        // statement that errored is still labeled + tainted (idempotent alongside the Err-arm
        // force above). Never touches `tx_open`/`pin`; RFQ (above) stays the tx authority.
        self.apply_classify(sql);
        r
    }

    /// The guarded, user-facing **row-returning** statement entry (S5, BLOCKER-2). Mirrors
    /// [`Checkout::exec`]'s guard structure EXACTLY: it runs [`Checkout::guard_tx_control`] FIRST
    /// and rejects a transaction-BOUNDARY statement (`BEGIN`/`COMMIT`/a bare `ROLLBACK`/…, leading
    /// comment/whitespace tolerant) with `PoolError::Unsupported` — so an `EXEC BEGIN` can never
    /// reach the raw client and open an untracked transaction that the next tenant on this pooled
    /// connection would inherit (a cross-tenant leak; charter rule 6). A SAVEPOINT statement inside
    /// an open transaction is allowed (M1-S8a, SPEC §22.2 (r)); anything else non-tx-control is
    /// delegated to `PoolBackend::query`.
    ///
    /// **A savepoint passthrough is routed to `PoolBackend::simple_query`, not `query`** — the raw
    /// TEXT leaf, the SAME one [`Checkout::tx_control`] already uses for the engine's own composed
    /// `SAVEPOINT sp_N`. Two reasons, in order of force: (1) **MySQL 8 cannot run these on the
    /// prepared path at all** — measured on 8.4.11, `COM_STMT_PREPARE` of `SAVEPOINT` /
    /// `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT` fails with errno `1295` ("This command is not
    /// supported in the prepared statement protocol yet"), while MariaDB 11.8 and PG 17 accept
    /// them; relaxing the guard alone would therefore leave Doctrine's nested transactions broken
    /// on the single most important DBAL backend. (2) A savepoint statement takes no parameters and
    /// returns no rows, so the prepared/extended protocol buys nothing here. The guard's no-params
    /// and lone-statement rules are what make this routing safe.
    pub async fn query(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, PoolError> {
        let verdict = self.guard_tx_control(sql, params, "query")?;
        let pool = Arc::clone(&self.pool);
        // RFQ authority (SPEC §7.1): tx_status is trustworthy only on the Ok arm — `query::run`
        // fully drains the RowStream before returning, so on success the atomic holds this
        // statement's terminating RFQ; on Err (`ErrorResponse` before the trailing RFQ is consumed)
        // it may be STALE, so the guard below taints any error while a tx is open.
        let r = match verdict {
            TxControlVerdict::Proceed => pool.backend.query(self.conn_mut(), sql, params).await,
            // A savepoint yields no columns and no rows on either engine, so the empty
            // `QueryResult` is the faithful shape, not a stub. `affected` is whatever the backend's
            // text leaf reports for the statement (0 on both engines for a savepoint op).
            TxControlVerdict::SavepointPassthrough => pool
                .backend
                .simple_query(self.conn_mut(), sql)
                .await
                .map(|affected| QueryResult {
                    affected,
                    ..Default::default()
                }),
        };
        let st = pool.backend.tx_status(self.conn());
        self.apply_tx_status(st);
        // M1-S6: the SECOND assist signal, read at the same post-statement point. No-op for PG/Fake
        // (default `take_session_mutated -> false`); taints + labels `SessionTracker` for MySQL.
        self.apply_session_tracker();
        if r.is_err() {
            // Rule A fail-safe (uniform across all 6 instrumented methods): on Err the RFQ atomic
            // is stale-UNTRUSTWORTHY — postgres-protocol returns Err at `ErrorResponse` BEFORE the
            // trailing `ReadyForQuery` is decoded — AND a statement can OPEN a tx before erroring:
            // `exec` forwards a multi-statement batch to `batch_execute`, and the tx-control
            // guard only classifies the LEADING keyword, so `SELECT 1; BEGIN; SELECT 1/0` passes the guard,
            // opens a tx mid-batch from autocommit, then errors — leaving an OPEN, ABORTED tx while
            // the atomic still reads stale-`Idle`. So neither `apply_tx_status` nor a pre-captured
            // `tx_open` can be trusted here. Force BOTH bits UNCONDITIONALLY so the checkout-time
            // recycle runs `ROLLBACK` *then* `DISCARD ALL` (that order is required — DISCARD ALL
            // cannot run inside a tx block) and a possibly-poisoned conn is NEVER handed to the next
            // tenant (charter rule 6). Over-forcing on a genuinely-clean autocommit error (PG
            // auto-rolls-back to Idle) costs only one harmless extra ROLLBACK+reset at the next
            // checkout — the safe direction (charter rule 5: correctness over throughput).
            self.tx_open = true;
            self.tainted = true;
        }
        // Assist signal (SPEC §7.1): runs on BOTH the Ok and Err arms — a session-mutating
        // statement that errored is still labeled + tainted (idempotent alongside the Err-arm
        // force above). Never touches `tx_open`/`pin`; RFQ (above) stays the tx authority.
        self.apply_classify(sql);
        r
    }

    /// The guarded, user-facing **INCREMENTAL** row-returning entry (S5 Task 3, the constant-memory
    /// `fetch:stream` producer path). Mirrors [`Checkout::query`]'s guard EXACTLY: runs
    /// [`Checkout::guard_tx_control`] FIRST and rejects a transaction-BOUNDARY statement with
    /// `PoolError::Unsupported` so an `EXEC BEGIN` can never reach the raw client and open an
    /// untracked transaction the next tenant on this pooled connection would inherit (a cross-tenant
    /// leak; charter rule 6). Only a non-tx-control statement is delegated to
    /// `PoolBackend::query_stream`.
    ///
    /// **A savepoint is refused HERE even inside a transaction** — the one place the M1-S8a
    /// passthrough does not apply. A savepoint operation returns no result set, so there is nothing
    /// to stream; and the passthrough's routing (the raw text leaf, see [`Checkout::query`]) has no
    /// streaming counterpart to route to. Refusing is the honest answer, and it is a REFUSAL, so it
    /// cannot widen anything.
    ///
    /// Returns a [`RowStreamHandle`] that borrows `&mut self` — the borrow makes it impossible to
    /// return the `Checkout` to the pool (or run another statement on it) while the stream is live.
    /// Unlike [`Checkout::query`], the pin/taint bookkeeping does NOT run here: it is DEFERRED to the
    /// handle's mandatory [`RowStreamHandle::finish`] (which runs the exact post-drain sequence), and
    /// backstopped by the handle's `Drop` net if the stream is abandoned. This two-phase shape is
    /// what makes the streaming path safe under credit backpressure — the connection is pinned/
    /// tainted correctly on EVERY exit (normal end, mid-stream error, or abandonment) before it can
    /// return to the pool.
    pub async fn query_stream(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<RowStreamHandle<'_, B>, PoolError> {
        match self.guard_tx_control(sql, params, "query_stream")? {
            TxControlVerdict::Proceed => {}
            TxControlVerdict::SavepointPassthrough => {
                return Err(PoolError::Unsupported(format!(
                    "savepoint statement not allowed via query_stream(): {sql:?} \
                     (a savepoint operation returns no rows — run it through exec()/query())"
                )));
            }
        }
        // Clone the Arc so the `&self.pool` borrow does not collide with the `self.conn_mut()`
        // borrow the backend call needs (same pattern as `query`/`exec`).
        let pool = Arc::clone(&self.pool);
        match pool
            .backend
            .query_stream(self.conn_mut(), sql, params)
            .await
        {
            // `rows` is an OWNED `B::RowStream` (channel-backed for pg — it does NOT borrow the
            // conn), so the `self.conn_mut()` reborrow above is already released here and `self`
            // (the `&mut Checkout`) can move into the handle without a self-referential borrow.
            Ok((cols, rows)) => Ok(RowStreamHandle {
                checkout: self,
                rows,
                cols,
                sql: sql.to_owned(),
                errored: false,
                finished: false,
            }),
            // OPEN-TIME failure (prepare syntax error, an out-of-M0 column, a bind pre-validation
            // fault, or a bind-phase `ErrorResponse`): NO handle is created, so `finish()` will
            // never run — instrument the error arm HERE with the SAME post-statement sequence
            // `Checkout::query` runs (RFQ read via `apply_tx_status` + the Rule-A Err-arm
            // force-taint + the S2 `apply_classify`) so an open error is handled identically to the
            // buffered path. The conn is genuinely `Idle`/clean after these (extended-protocol
            // prepare/bind can't open a tx the way `exec`'s multi-statement batch can), so the
            // force-taint is conservative over-tainting — the safe direction (charter rule 5).
            Err(e) => {
                self.finalize_stream(true, sql);
                Err(e)
            }
        }
    }

    /// Applies the AUTHORITATIVE RFQ [`TxStatus`] (read after a statement's response is fully
    /// drained) to this `Checkout`'s pin state — the M1-S1 pin engine, replacing the M0 stub's
    /// engine-side-only bookkeeping (SPEC §7.1: protocol signals are the authority, the lexer is
    /// assist). Two separable concerns:
    ///
    /// * **Reuse-safety bits (`tx_open`/`tainted`) — set UNCONDITIONALLY from I/T/E.** These protect
    ///   the NEXT tenant, so RFQ is their sole authority: `tx_open` is assigned directly from the
    ///   status ([`pin::tx_status_bits`]), and `Failed`/`E` FORCEs `tainted = true` (an aborted tx
    ///   must be `ROLLBACK`'d before reuse). A clean `Idle`/`I` sets `tx_open = false` but LEAVES
    ///   `tainted` as-is — it does NOT clear a prior taint; the checkout-time recycle (the
    ///   `tx_open || tainted` branch in `checkout`) is what clears it.
    ///
    /// * **Identity bits (`pin`/`last_pin_cause`) — NEVER clobber a real `TxId`.** `last_pin_cause`
    ///   is set to [`PinCause::Tx`] whenever RFQ reports a tx (`InTx`/`Failed`) — in S1 a
    ///   transaction is the only pin cause. `self.pin` is deliberately LEFT UNTOUCHED here: if the
    ///   pool opened this tx via `begin_tx_with`, `pin` is already `PinnedTx(real_id)` and must not
    ///   be overwritten; for an RFQ-ONLY-detected `T`/`E` on a conn the pool did NOT open a tx on (a
    ///   leaked/guard-bypassed tx with no pool-assigned `TxId`), `pin` stays `Unpinned` — there is
    ///   NO `PinnedTx`-without-`TxId` variant to fabricate, and the reuse danger is fully carried by
    ///   `tx_open`/`tainted`, which force the checkout-time ROLLBACK/reset. (The `TxId` is only an
    ///   identity for the S6 actor, which always allocates it through `begin_tx_with`.)
    fn apply_tx_status(&mut self, st: TxStatus) {
        let (tx_open, force_taint) = pin::tx_status_bits(st);
        self.tx_open = tx_open;
        if force_taint {
            self.tainted = true;
        }
        if matches!(st, TxStatus::InTx | TxStatus::Failed) {
            self.last_pin_cause = Some(PinCause::Tx);
        }
        // NOTE: `self.pin` is intentionally NOT written here — never clobber a real `TxId`, never
        // fabricate a sentinel. See the doc comment above.
    }

    /// The M1-S6 SECOND assist signal (SPEC §7.1): the backend's post-statement PROTOCOL report of a
    /// session-state mutation — for MySQL/MariaDB, the OK-packet session trackers
    /// (`PoolBackend::take_session_mutated`). Runs immediately AFTER `apply_tx_status` on every
    /// instrumented method (the same post-statement point the RFQ/OK-packet is read), and is a pure
    /// NO-OP for any backend that inherits the default `take_session_mutated -> false` (Postgres, the
    /// `FakeBackend`): the call compiles and runs but never taints — PG's pin behavior is byte-identical.
    ///
    /// Like `apply_classify`, it is ASSIST-not-AUTHORITY: it only ADDS reuse-safety taint plus a cause
    /// label, and NEVER touches `self.pin`/`self.tx_open` (those belong to the RFQ tx authority). It
    /// mirrors `apply_classify`'s composition, with ONE deliberate difference: it does NOT overwrite an
    /// authoritative `Tx` label. `apply_tx_status` (which runs first) sets `last_pin_cause = Tx` when a
    /// tx is open; leaving that in place keeps the authority visible even when a session mutation
    /// rode along inside the same in-tx statement. (A later `apply_classify` may still overwrite the
    /// label with its own lexer cause — "most recently observed"; the tracker's own guard only
    /// protects the authority's `Tx`.)
    fn apply_session_tracker(&mut self) {
        // Clone the Arc so the `&self.pool` borrow does not collide with the `self.conn_mut()`
        // borrow `take_session_mutated` needs (same pattern as `exec`/`query`).
        let pool = Arc::clone(&self.pool);
        if pool.backend.take_session_mutated(self.conn_mut()) {
            self.tainted = true;
            // Never clobber the RFQ's authoritative `Tx` label (set by the preceding
            // `apply_tx_status`); otherwise record `SessionTracker` as the observed cause.
            if !matches!(self.last_pin_cause, Some(PinCause::Tx)) {
                self.last_pin_cause = Some(PinCause::SessionTracker);
            }
        }
    }

    /// The M1-S2 assist signal (SPEC §7.1): RFQ (`apply_tx_status`) is the tx AUTHORITY; the lexer
    /// only ADDS session-state taint + a cause label for protocol-invisible mutations. It NEVER
    /// clears a taint and NEVER touches `self.pin`/`self.tx_open` (those are the RFQ's/tx's).
    /// `classify()` is total (never panics) and multi-statement-aware, so `exec`'s batch path is
    /// covered.
    fn apply_classify(&mut self, sql: &str) {
        if let Some(trigger) = ferro_classify::classify(
            sql,
            self.pool.backend.dialect(),
            &self.pool.config.pin_functions,
            self.pool.config.pin_on_unknown,
        ) {
            self.tainted = true;
            self.last_pin_cause = Some(PinCause::from(trigger));
        }
    }

    /// The post-drain pin/taint sequence for a finished [`RowStreamHandle`] (S5 Task 3) — the EXACT
    /// sequence [`Checkout::query`] runs after its buffered `RowStream` is drained, factored here so
    /// the streaming path replicates it byte-for-byte (rather than the handle poking at the private
    /// `tx_open`/`tainted` fields inline). MUST be called only AFTER the underlying stream has been
    /// drained to `None`. `errored` is `true` iff any `next()` yielded an `Err`.
    ///
    /// **Which signal is load-bearing depends on `errored` — read this before touching either.**
    /// On the CLEAN arm (`errored == false`) the drain really did consume the terminating
    /// `ReadyForQuery`, so the `tx_status` read below is a valid POST-DRAIN read (the S1 rule) and is
    /// the authority. On the ERRORED arm it is NOT: a backend `RowStream` latches itself finished on
    /// the first `Err` (`PgRowStream::next` sets `done = true` and thereafter returns `None`
    /// immediately), so the drain loop in [`RowStreamHandle::finish`] exits on its FIRST poll having
    /// read *nothing* — no `CommandComplete`, no `ReadyForQuery`. `tx_status` is then a STALE
    /// PRE-drain read (observable: a clean 6000-row stream reports `affected = 6000`, the same
    /// stream poisoned mid-way by an `extract_value` refusal reports `affected = 0`, because the
    /// command tag was never read). What makes that harmless is the **Rule-A force below — not the
    /// drain**: it overwrites both bits unconditionally, so whatever `apply_tx_status` just wrote is
    /// irrelevant on that path. Verified live: 5 rounds on a `max_size=1` pool, same backend pid, no
    /// protocol desync and no leaked transaction. **The force is therefore NOT redundant with the
    /// drain**; deleting it on the reasoning that "the drain already guarantees a post-drain RFQ
    /// read" would silently reopen the cross-tenant-leak class (charter rule 6).
    fn finalize_stream(&mut self, errored: bool, sql: &str) {
        let pool = Arc::clone(&self.pool);
        // RFQ authority (SPEC §7.1): trustworthy only post-drain — which holds on the CLEAN arm
        // only. On an errored stream the drain read nothing, so this IS a stale pre-drain read and
        // whatever it writes is discarded by the Err-arm force below (see the fn doc).
        let st = pool.backend.tx_status(self.conn());
        self.apply_tx_status(st);
        // M1-S6: the SECOND assist signal, read at the same post-statement point. No-op for PG/Fake
        // (default `take_session_mutated -> false`); taints + labels `SessionTracker` for MySQL.
        self.apply_session_tracker();
        if errored {
            // Rule A fail-safe (uniform with the six instrumented methods): on ANY mid-stream error
            // the terminating `ReadyForQuery` has NOT been consumed — the stream latched itself
            // finished on the `Err`, so `finish`'s drain loop was a no-op — and a statement can open
            // a tx before erroring, so neither `apply_tx_status` (stale, PRE-drain) nor a
            // pre-captured `tx_open` can be trusted. THIS force, not the drain, is the mechanism
            // that keeps the errored path safe. Force BOTH bits UNCONDITIONALLY so the checkout-time
            // recycle runs ROLLBACK *then* DISCARD ALL and a possibly-poisoned conn is NEVER handed
            // to the next tenant
            // (charter rule 6). Over-forcing on a clean autocommit error costs one harmless extra
            // ROLLBACK+reset — the safe direction (charter rule 5).
            self.tx_open = true;
            self.tainted = true;
        }
        // Assist signal (SPEC §7.1): a session-mutating statement that streamed is still labeled +
        // tainted, on both arms (idempotent alongside the Err-arm force). Never touches
        // `tx_open`/`pin`; RFQ (above) stays the tx authority.
        self.apply_classify(sql);
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

/// The terminal result of a finished [`RowStreamHandle`] (S5 Task 3): the command-tag affected-row
/// count (valid because [`RowStreamHandle::finish`] fully drains the stream first — on a stream that
/// ERRORED mid-way the drain is a no-op and this is `0`, since the command tag never arrived) plus
/// the checkout [`CheckoutStats`], so the `fetch:stream` producer (Task 4) can frame the terminal
/// `END` without reaching back into the `Checkout` (whose borrow `finish` has already consumed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamEnd {
    /// Command-tag affected-row count (never a hardcoded 0 — the S4 defect), read post-drain on a
    /// clean stream; `0` on an errored one, where the tag was never read.
    pub affected: u64,
    /// The owning checkout's stats (currently `queue_us`), captured at `finish`.
    pub stats: CheckoutStats,
}

/// A pull-based, borrowed handle over an INCREMENTAL row stream (S5 Task 3) — the pool/backend half
/// of the `fetch:stream` reply path. Returned by [`Checkout::query_stream`]; borrows `&'a mut
/// Checkout` so the connection cannot be returned to the pool (nor reused for another statement)
/// while the stream is live — the borrow checker enforces the two-phase protocol.
///
/// Rows are pulled ONE AT A TIME via [`RowStreamHandle::next`] (constant memory, honoring the Task-4
/// credit window); the caller MUST end the stream with [`RowStreamHandle::finish`], which drains any
/// remainder, runs the post-drain pin/taint sequence, and returns the affected count. If the handle
/// is dropped WITHOUT `finish()` — abandonment, a panic mid-stream, or a cancel that drops it — the
/// [`Drop`] safety net force-taints the conn so the checkout-time recycle still cleans it. A
/// partially-drained connection is NEVER handed to the next tenant untainted (the cross-tenant-leak
/// hazard this two-phase API exists to close, charter rule 6).
///
/// Holding `&mut Checkout` AND owning `rows` is sound (no self-referential borrow): a backend
/// `RowStream` is OWNED, not a borrow of the conn (for pg it is channel-backed, driven by the
/// spawned connection task), and the `&mut Checkout` keeps the underlying `Client` alive so that
/// task keeps feeding the stream.
pub struct RowStreamHandle<'a, B: PoolBackend> {
    checkout: &'a mut Checkout<B>,
    rows: B::RowStream,
    cols: Vec<ColMeta>,
    sql: String,
    /// Set once any `next()` yields an `Err` — drives the Rule-A force-taint in `finish`.
    errored: bool,
    /// Set `true` by `finish()`; the `Drop` net force-taints iff this is still `false`.
    finished: bool,
}

impl<'a, B: PoolBackend> RowStreamHandle<'a, B> {
    /// The result columns (built from the prepared statement — correct even for a zero-row stream).
    pub fn cols(&self) -> &[ColMeta] {
        &self.cols
    }

    /// Pull the NEXT row (constant memory): `Some(Ok(row))` per row, `Some(Err(_))` on a mid-stream
    /// error, `None` once exhausted. Delegates to the backend's LAZY [`BackendRows::next`] (a row is
    /// not produced until this is polled) and records `errored` so [`RowStreamHandle::finish`] can
    /// apply the Rule-A force-taint.
    pub async fn next(&mut self) -> Option<Result<Vec<Value>, PoolError>> {
        match self.rows.next().await {
            Some(Err(e)) => {
                self.errored = true;
                Some(Err(e))
            }
            other => other,
        }
    }

    /// MANDATORY terminal (the normal exit path). Drains any rows the caller did not pull, then runs
    /// the EXACT pin/taint sequence [`Checkout::query`] runs (RFQ `apply_tx_status` + the Rule-A
    /// Err-arm force-taint + the S2 `apply_classify`, via `Checkout::finalize_stream`), and returns
    /// the command-tag `affected` count plus checkout `stats`. Consumes the handle, releasing the
    /// `&mut Checkout` borrow so the connection can be returned to the pool correctly pinned/tainted.
    ///
    /// **The drain only does its job on a stream that has not errored.** On a clean stream it does
    /// carry the connection to its terminating `ReadyForQuery`, making the RFQ read a valid
    /// POST-DRAIN read (the S1 rule) and `affected` a real command-tag count. Once any `next()` has
    /// yielded an `Err`, the backend stream has latched itself finished (`PgRowStream` sets
    /// `done = true` on the error and returns `None` from then on), so the loop below exits on its
    /// first poll having read NOTHING: no `CommandComplete` (`affected` falls back to 0) and no
    /// `ReadyForQuery` (the RFQ read is stale pre-drain state). Safety on that path rests entirely
    /// on `finalize_stream`'s **Rule-A force**, which sets `tx_open`/`tainted` unconditionally and
    /// discards the stale read — see its doc comment before assuming the two are redundant.
    pub async fn finish(mut self) -> Result<StreamEnd, PoolError> {
        // Drain the remainder (a no-op if the caller already pulled to `None`). Going through
        // `self.next()` records any late error into `self.errored`.
        while self.next().await.is_some() {}
        let affected = self.rows.rows_affected();
        let errored = self.errored;
        let stats = self.checkout.stats();
        self.checkout.finalize_stream(errored, &self.sql);
        self.finished = true;
        Ok(StreamEnd { affected, stats })
    }
}

impl<B: PoolBackend> Drop for RowStreamHandle<'_, B> {
    fn drop(&mut self) {
        if !self.finished {
            // The safety net (charter rule 6): an abandoned / panicked / cancelled stream left the
            // conn partially drained — mid-protocol, possibly mid-transaction. `Drop` is SYNCHRONOUS
            // and cannot `.await`, so it can neither drain the stream nor read `tx_status`; it does
            // the one thing it can — force `tainted` so the next checkout's async recycle runs the
            // full ROLLBACK + DISCARD ALL. `tx_open` is left as-is: it already reflects any tx the
            // pool opened via `begin_tx` (this streaming path never opens one itself), and `tainted`
            // alone trips the recycle guard (`tx_open || tainted || clean_reset_profile`).
            self.checkout.tainted = true;
        }
    }
}
