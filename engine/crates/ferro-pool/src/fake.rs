//! Deterministic in-memory `PoolBackend` for fast pool-semantics tests (Task 1). Later tasks
//! (checkout/release, max_lifetime, pin stub) drive this backend instead of a live Postgres so
//! the pool's mechanics are tested without a Docker dependency.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::time::Instant;

use ferro_proto::consts::{branch, errc};

use crate::backend::{Cancel, Dialect, PoolBackend, QueryResult, ResetProfile, TxStatus};
use crate::error::PoolError;
use crate::pin::{TxVerb, leading_tx_verb};

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
    /// One-shot: when set, the NEXT `simple_query()` on this conn returns `PoolError::Backend`
    /// (a statement-level error) WITHOUT flipping `tx_status`. Models the Task-4 Err-arm
    /// stale-`Idle` case — a statement that errors while the RFQ atomic still reads `Idle` (e.g. a
    /// multi-statement batch that opened a tx mid-batch, where the trailing `ReadyForQuery` has not
    /// been decoded yet) — so the pool's unconditional Err-arm fail-safe can be exercised
    /// deterministically without the fake having to model batch parsing.
    fail_next_simple_query: bool,
    /// Models the RFQ status this connection would report (Task 3). Lives PER-`FakeConn` (matching
    /// real per-`Client` semantics), defaults to `Idle` at checkout, and is updated by
    /// `simple_query`/`query` inferring from the leading SQL keyword (see `leading_tx_verb`) — NOT
    /// a shared `FakeBackend` field, which would let one connection's status leak into another's
    /// and (per the Task 3 verification blocker) let a stale `Idle` clobber a pin another conn just
    /// set.
    ///
    /// The inference is a faithful-to-real-Postgres model, not just "matches the bare tx-control
    /// guard's keyword list": a savepoint operation (`SAVEPOINT`/`RELEASE`/`ROLLBACK TO
    /// <savepoint>`) does NOT end the surrounding transaction on real Postgres, so `apply_leading_
    /// tx_verb` PRESERVES `tx_status` across those (and across any exotic tx-control form it
    /// doesn't specifically classify, e.g. `PREPARE TRANSACTION`) rather than dropping it to
    /// `Idle`. See `pin::leading_tx_verb`'s doc comment for the exact Open/Close/preserve mapping.
    tx_status: TxStatus,
}

impl FakeConn {
    /// Arms the *next* `ping()` call on this connection to fail with `ConnectionLost`; the flag
    /// is consumed (one-shot) so subsequent pings succeed again.
    pub fn arm_fail_next_ping(&mut self) {
        self.fail_next_ping = true;
    }

    /// Arms the *next* `simple_query()` call on this connection to fail with
    /// `PoolError::Backend` WITHOUT changing `tx_status` (one-shot). Used by the Task-4 Err-arm
    /// regression test to reproduce the stale-`Idle`-on-error condition the pool's unconditional
    /// fail-safe defends against.
    pub fn arm_fail_next_simple_query(&mut self) {
        self.fail_next_simple_query = true;
    }

    /// This connection's currently-modeled [`TxStatus`] (mirrors `PoolBackend::tx_status`).
    pub fn tx_status(&self) -> TxStatus {
        self.tx_status
    }

    /// Test hook: force this connection's modeled `TxStatus` directly. The only way to reach
    /// `Failed` (RFQ `E`) — no SQL keyword expresses "the last statement errored", so a test arms
    /// it explicitly to drive that case through `PoolBackend::tx_status`.
    pub fn set_tx_status(&mut self, status: TxStatus) {
        self.tx_status = status;
    }
}

/// Updates `conn.tx_status` by inferring from `sql`'s leading transaction-control keyword (shared
/// scan with `pin::is_bare_tx_control`, Task 3). See `pin::leading_tx_verb`'s doc comment for the
/// exact mapping; in short: `BEGIN`/`START TRANSACTION` models `InTx`; a BARE `COMMIT`/`END`/
/// `ABORT`, or a `ROLLBACK` NOT followed by `TO`, models `Idle`. Everything else — an ordinary
/// statement, `SAVEPOINT`/`RELEASE`/`ROLLBACK TO <savepoint>` (savepoint ops that do NOT end a
/// real Postgres transaction), or an exotic tx-control form this scan doesn't specifically
/// classify — PRESERVES `tx_status` unchanged, exactly like a real `ReadyForQuery` byte only
/// flipping on a statement that actually changes transaction state.
fn apply_leading_tx_verb(conn: &mut FakeConn, sql: &str) {
    if let Some(verb) = leading_tx_verb(sql) {
        conn.tx_status = match verb {
            TxVerb::Open => TxStatus::InTx,
            TxVerb::Close => TxStatus::Idle,
        };
    }
}

/// A gate a test arms to freeze `query()` mid-flight, plus the `cancelled` flag the matching
/// [`FakeCancelHandle`] flips to release it. Modelling a real Postgres statement cancel WITHOUT a
/// live server: while the gate is armed, `query()` parks on `notify`; a `FakeCancelHandle::cancel`
/// sets `cancelled` and wakes the waiter, which then returns a `57014`-shaped `Sql` error — exactly
/// the shape the pg `error_map` produces for a cancelled statement — so the tx actor's out-of-band
/// cancel path (deadline → cancel → drain the query future to its erroring completion → rollback)
/// is exercised deterministically.
#[derive(Debug)]
struct QueryGate {
    notify: Notify,
    cancelled: AtomicBool,
}

/// The fake backend's out-of-band cancel handle (S6). Carries a clone of the armed [`QueryGate`]
/// (if any) so [`Cancel::cancel`] can release a `query()` frozen on that gate — standing in for a
/// server-side statement cancel. When no gate is armed it is inert (its `gate` is `None`); either
/// way it exercises the `Checkout::cancel_handle` -> `B::CancelHandle` surface (grabbed on `&self`,
/// `Send + 'static`, fired fire-once by value) without a live Postgres.
#[derive(Debug, Clone)]
pub struct FakeCancelHandle {
    gate: Option<Arc<QueryGate>>,
}

#[async_trait]
impl Cancel for FakeCancelHandle {
    async fn cancel(self) {
        if let Some(gate) = self.gate {
            gate.cancelled.store(true, Ordering::SeqCst);
            gate.notify.notify_waiters();
        }
    }
}

/// A `FakeBackend` connects instantly and never touches the network. `next_id` is atomic so
/// `connect()` only needs `&self` (matching the trait), matching how a real pool shares one
/// backend across many concurrent checkouts.
///
/// NOTE (M1-S3 verification MINOR): this struct deliberately does NOT `#[derive(Default)]`. A
/// derived `Default` would zero every field structurally — including `clean_reset_profile` to
/// `None` — which would silently diverge from `new()`'s `Some(ResetProfile::Targeted)` (the
/// PG-like default policy every existing test relies on). `Default` is hand-implemented below to
/// delegate to `new()` so the two constructors can never drift apart again.
#[derive(Debug)]
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
    /// When `Some`, every `query()` call parks on this gate until a matching `FakeCancelHandle`
    /// fires (S6). Lets the tx-actor deadline test freeze a tx-scoped user statement so the actor's
    /// max/idle timer fires, then prove the out-of-band cancel unblocks the query into its erroring
    /// (`57014`) completion — the pin released, the tx tombstoned, nothing re-run.
    query_gate: Mutex<Option<Arc<QueryGate>>>,
    /// Number of `query()` calls currently parked on `query_gate`. A test polls this until `> 0` to
    /// prove the statement is actually in flight before relying on a timer to fire.
    queries_waiting: AtomicU64,
    /// The [`Dialect`] `dialect()` reports (M1-S2 Task 2). Defaults to `Dialect::Postgres` (via
    /// `Dialect: Default`, which is exactly what proves `#[derive(Default)]` on `FakeBackend`
    /// still compiles/is correct after this field lands) so every existing `FakeBackend::new()`/
    /// `FakeBackend::default()` call site keeps behaving as a Postgres backend without change. A
    /// test that needs a different dialect (e.g. to prove `Checkout` picks the right classify
    /// rule set) calls `set_dialect`.
    dialect: Mutex<Dialect>,
    /// The [`ResetProfile`] `clean_reset_profile()` reports for a NON-tainted recycled conn
    /// (M1-S3). Defaults to `Some(ResetProfile::Targeted)` — mirroring `PgBackend`'s policy — so
    /// pool tests exercise the targeted-hygiene path by default; a test that needs the MySQL-style
    /// skip case calls `set_clean_reset_profile(None)`.
    clean_reset_profile: Mutex<Option<ResetProfile>>,
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
            query_gate: Mutex::new(None),
            queries_waiting: AtomicU64::new(0),
            dialect: Mutex::new(Dialect::default()),
            clean_reset_profile: Mutex::new(Some(ResetProfile::Targeted)),
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

    /// Arms every subsequent `query()` call to park until a `FakeCancelHandle` for this backend is
    /// fired (S6). Used by the tx-actor deadline test to freeze a tx-scoped statement so a deadline
    /// timer fires while it is in flight, then prove the out-of-band cancel path drains it.
    pub fn block_query(&self) {
        *self.query_gate.lock().unwrap() = Some(Arc::new(QueryGate {
            notify: Notify::new(),
            cancelled: AtomicBool::new(false),
        }));
    }

    /// Number of `query()` calls currently parked on the gate armed by `block_query()`.
    pub fn queries_waiting(&self) -> u64 {
        self.queries_waiting.load(Ordering::SeqCst)
    }

    /// Test hook: force the [`Dialect`] this backend reports from `dialect()`. `&self` (not
    /// `&mut self`), matching every other test-armed knob on this backend (e.g.
    /// `set_query_result`), since tests typically hold the `FakeBackend` shared (e.g. behind the
    /// `Arc` a `Pool` wraps it in).
    pub fn set_dialect(&self, dialect: Dialect) {
        *self.dialect.lock().unwrap() = dialect;
    }

    /// Test hook: force the [`ResetProfile`] `clean_reset_profile()` reports for a NON-tainted
    /// recycled conn (M1-S3). Pass `None` to drive the MySQL-tracker-style "skip hygiene for a
    /// clean conn" case; pass `Some(profile)` to drive a specific profile (defaults to
    /// `Some(ResetProfile::Targeted)`, matching `PgBackend`).
    pub fn set_clean_reset_profile(&self, profile: Option<ResetProfile>) {
        *self.clean_reset_profile.lock().unwrap() = profile;
    }
}

impl Default for FakeBackend {
    /// Hand-implemented (NOT derived — see the struct's doc comment) so `FakeBackend::default()`
    /// can never silently diverge from `FakeBackend::new()`.
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PoolBackend for FakeBackend {
    type Conn = FakeConn;
    type CancelHandle = FakeCancelHandle;

    fn cancel_handle(&self, _conn: &Self::Conn) -> Self::CancelHandle {
        // Capture a clone of the armed query gate (if any) so a later `cancel()` can release a
        // `query()` frozen on it — the fake stand-in for a server-side statement cancel.
        FakeCancelHandle {
            gate: self.query_gate.lock().unwrap().clone(),
        }
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
            fail_next_simple_query: false,
            tx_status: TxStatus::Idle,
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

    fn dialect(&self) -> Dialect {
        *self.dialect.lock().unwrap()
    }

    fn tx_status(&self, conn: &Self::Conn) -> TxStatus {
        conn.tx_status
    }

    async fn reset(&self, conn: &mut Self::Conn, profile: ResetProfile) -> Result<(), PoolError> {
        conn.tx_open = false;
        conn.recorded.push(format!("RESET:{profile:?}"));
        Ok(())
    }

    fn clean_reset_profile(&self) -> Option<ResetProfile> {
        *self.clean_reset_profile.lock().unwrap()
    }

    async fn simple_query(&self, conn: &mut Self::Conn, sql: &str) -> Result<u64, PoolError> {
        conn.recorded.push(sql.to_string());
        apply_leading_tx_verb(conn, sql);
        // One-shot armed failure (see `arm_fail_next_simple_query`): return a statement-level error
        // WITHOUT flipping `tx_status`, modelling the Err-arm stale-`Idle` case. Consumed here.
        if conn.fail_next_simple_query {
            conn.fail_next_simple_query = false;
            return Err(PoolError::Backend(
                "fake: armed simple_query failure".to_string(),
            ));
        }
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
        apply_leading_tx_verb(conn, sql);

        // Test-only gate (see `block_query`): if armed, park until a `FakeCancelHandle` for this
        // backend fires. A fired cancel returns the `57014`-shaped `Sql` error the pg `error_map`
        // produces for a cancelled statement, so the tx actor drains the query future to an
        // erroring completion exactly as it would against a live Postgres.
        let gate = self.query_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            self.queries_waiting.fetch_add(1, Ordering::SeqCst);
            gate.notify.notified().await;
            self.queries_waiting.fetch_sub(1, Ordering::SeqCst);
            if gate.cancelled.load(Ordering::SeqCst) {
                return Err(PoolError::Sql {
                    code: errc::CANCELLED,
                    branch: branch::NON_RETRYABLE,
                    sqlstate: Some("57014".to_string()),
                    message: "canceling statement due to user request (fake)".to_string(),
                });
            }
        }
        Ok(self.canned_query.lock().unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M1-S3 TDD: `reset(conn, Full)` records `"RESET:Full"`.
    #[tokio::test]
    async fn reset_full_records_reset_full() {
        let backend = FakeBackend::new();
        let mut conn = backend.connect().await.expect("connect");
        backend
            .reset(&mut conn, ResetProfile::Full)
            .await
            .expect("reset");
        assert_eq!(conn.recorded, vec!["RESET:Full".to_string()]);
        assert!(!conn.tx_open, "reset clears tx_open regardless of profile");
    }

    /// M1-S3 TDD: `reset(conn, Targeted)` records `"RESET:Targeted"`.
    #[tokio::test]
    async fn reset_targeted_records_reset_targeted() {
        let backend = FakeBackend::new();
        let mut conn = backend.connect().await.expect("connect");
        backend
            .reset(&mut conn, ResetProfile::Targeted)
            .await
            .expect("reset");
        assert_eq!(conn.recorded, vec!["RESET:Targeted".to_string()]);
        assert!(!conn.tx_open, "reset clears tx_open regardless of profile");
    }

    /// M1-S3 TDD: `new()` defaults `clean_reset_profile()` to `Some(Targeted)` (mirrors `PgBackend`).
    #[test]
    fn new_defaults_clean_reset_profile_to_targeted() {
        let backend = FakeBackend::new();
        assert_eq!(backend.clean_reset_profile(), Some(ResetProfile::Targeted));
    }

    /// M1-S3 TDD (the Default/new divergence fix): `FakeBackend::default()` and `FakeBackend::new()`
    /// MUST agree on `clean_reset_profile()`. Before the fix (`#[derive(Default)]` structurally
    /// zeroing every field), `default()` would silently report `None` here while `new()` reported
    /// `Some(Targeted)` — this test is the proof the hand-impl'd `Default` closes that gap.
    #[test]
    fn default_and_new_agree_on_clean_reset_profile() {
        assert_eq!(
            FakeBackend::default().clean_reset_profile(),
            FakeBackend::new().clean_reset_profile()
        );
        assert_eq!(
            FakeBackend::default().clean_reset_profile(),
            Some(ResetProfile::Targeted)
        );
    }

    /// M1-S3 TDD: a `FakeBackend` with `clean_reset_profile` explicitly set to `None` (the
    /// MySQL-tracker-style skip case) reports `None`.
    #[test]
    fn set_clean_reset_profile_none_is_respected() {
        let backend = FakeBackend::new();
        backend.set_clean_reset_profile(None);
        assert_eq!(backend.clean_reset_profile(), None);
    }
}
