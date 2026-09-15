//! **C3-3e: the SQLite BEGIN lockstep — the C3-2 debt, discharged.**
//!
//! C3-2 shipped `compose_begin_sql`'s SQLite arm with a unit test asserting the two literals
//! (`BEGIN IMMEDIATE` / `BEGIN DEFERRED`), and C3-1's `p4b` separately proved that `BEGIN IMMEDIATE`
//! takes SQLite's writer lock. **Nothing connected the two.** Both tests would have stayed green if
//! the engine's arm were respelled to something that does not take the lock, because each asserts
//! its own literal: the unit test would be updated to match the new string, and `p4b` would keep
//! proving a fact about a string the engine no longer emits. That is precisely the gap the MySQL
//! lane closed with `begin_dialect_it.rs`, and it became writable here the moment a SQLite pool
//! existed — which is this slice.
//!
//! So this test never names a BEGIN string. It asks the POOL for its dialect, asks
//! `compose_begin_sql` what to emit for that dialect, runs the result through the same
//! `Checkout::begin_tx_with` the tx service uses, and then asks a RAW side connection to the same
//! database file whether the writer lock is held. A respelling changes what is emitted and the
//! observation changes with it.
//!
//! **The control is what makes it a proof rather than a tautology.** "B cannot take the writer lock"
//! is consistent with any number of accidents — B misconfigured, the file locked by something else,
//! the pool connection never opened at all. So the declared-readonly arm runs the identical sequence
//! and asserts B CAN take the lock. One arm blocks, the other does not, and the only difference
//! between them is the string the engine composed.
//!
//! SQLite needs no server, so this runs everywhere, CI included, with no gating env var.

use std::time::{Duration, Instant};

use ferro_backend_sqlite::SqliteBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::pin::TxId;
use ferro_pool::pool::Pool;
use ferrod::tx::actor::compose_begin_sql;

/// B's busy wait. Long enough that "parked" and "returned immediately" cannot be confused on a
/// loaded runner, short enough that the blocking arm costs well under a second.
const B_BUSY_TIMEOUT: Duration = Duration::from_millis(400);

/// Open a raw side connection to the same file — the SQLite equivalent of the raw `tokio-postgres` /
/// `mysql_async` connections the chaos suites use. Deliberately NOT a second pool: the point is to
/// observe the lock from outside anything the engine controls.
fn side_connection(path: &std::path::Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).expect("side connection opens");
    conn.busy_timeout(B_BUSY_TIMEOUT)
        .expect("side connection arms its busy timeout");
    conn
}

fn pool_on(path: &std::path::Path) -> Pool<SqliteBackend> {
    let dsn = format!("sqlite://{}", path.display());
    let config = PoolConfig {
        max_size: 4,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(60),
        reap_interval: None,
        pin_functions: Vec::new(),
        pin_on_unknown: true,
    };
    Pool::new(
        SqliteBackend::new(dsn).with_busy_timeout(config.checkout_timeout),
        config,
    )
}

/// Run the engine's composed BEGIN for `readonly` on a pooled connection, then report whether a raw
/// side connection can still take the writer lock while that transaction is open.
///
/// Returns `(writer_lock_available, elapsed)` — the elapsed time separates "refused after waiting
/// out the busy timeout", which is what contention looks like, from "refused instantly", which would
/// be a different failure wearing the same result.
async fn side_can_take_writer_lock(readonly: bool) -> (bool, Duration, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lockstep.db");

    let pool = pool_on(&path);
    let mut co = pool.checkout().await.expect("checkout");

    // The lockstep itself: the dialect comes from the pool's own backend and the string from the
    // engine's composer — the same two calls `services::sql::begin_on_pool` makes, in the same
    // order. Nothing here spells a BEGIN.
    let begin_sql = compose_begin_sql(pool.backend().dialect(), None, readonly).expect("composes");
    co.begin_tx_with(TxId(1), &begin_sql)
        .await
        .expect("the composed BEGIN runs");

    // OBSERVE IMMEDIATELY, BEFORE ANY STATEMENT RUNS — this is the whole discrimination and the
    // first version of this test got it wrong. A write issued inside a DEFERRED transaction
    // UPGRADES it to a writer and (absent contention at that instant) succeeds, so once a statement
    // has run both spellings hold the writer lock and the observation cannot tell them apart. The
    // mutation proved it: with the undeclared arm respelled to a bare `BEGIN`, the earlier version
    // of this test stayed GREEN.
    //
    // D13's claim is not "the transaction ends up holding the lock" — it is that the lock is taken
    // AT BEGIN, so no upgrade is ever needed and `SQLITE_BUSY_SNAPSHOT` is unreachable. That claim
    // is only visible in the window between BEGIN and the first statement, so that is where the
    // side connection looks.
    let b = side_connection(&path);
    let started = Instant::now();
    let taken = b.execute_batch("BEGIN IMMEDIATE");
    let elapsed = started.elapsed();
    let available = match taken {
        Ok(()) => {
            b.execute_batch("ROLLBACK").expect("B releases");
            true
        }
        Err(_) => false,
    };

    // The writer's own statement, AFTER the observation: it runs without ever upgrading, because it
    // never was a reader. This is `p4b`'s closing move and it costs nothing to keep here — it shows
    // the arm under test is a usable write transaction and not merely a lock nobody can use.
    if !readonly {
        co.exec("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY)")
            .await
            .expect("the writer writes without upgrading");
    }

    // Leave the pooled connection clean rather than letting the drop path do it, so a failure here
    // is this test's and not the recycle's.
    let _ = co.rollback_tx().await;
    (available, elapsed, begin_sql)
}

/// **The load-bearing arm.** An UNDECLARED request must hold the writer lock for the whole
/// transaction, so a competing writer waits out its busy timeout and is then refused.
///
/// MUTATION PROVEN: with `compose_begin_sql`'s undeclared SQLite arm returning `"BEGIN"` — SQLite's
/// DEFERRED mode, and the exact string C3-2 replaced — B takes the lock immediately and this fails
/// on `writer_lock_available`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engines_undeclared_begin_holds_the_writer_lock() {
    let (available, elapsed, begin_sql) = side_can_take_writer_lock(false).await;

    assert!(
        !available,
        "the engine composed {begin_sql:?} for an undeclared request and a competing writer still \
         took the lock — whatever that string is, it is not taking the writer lock at BEGIN (D13)"
    );

    // 90% of the armed timeout, the same floor and the same reason as the C3-1 spike's `p4b`:
    // SQLite's busy handler sleeps in increments and can return a hair early under scheduling
    // jitter, and the gap being separated (400 ms vs an immediate return) is enormous.
    let floor = B_BUSY_TIMEOUT.mul_f64(0.9);
    assert!(
        elapsed >= floor,
        "B was refused in {elapsed:?}, faster than the {B_BUSY_TIMEOUT:?} it armed — a refusal that \
         fast is not lock contention, so this assertion is not observing what it claims to"
    );
}

/// **The control.** A DECLARED-readonly request takes a deferred reader lock, so the same competing
/// writer proceeds at once. Without this arm the assertion above would be equally consistent with
/// "B can never take the lock in this fixture".
///
/// MUTATION PROVEN: with the readonly arm returning `"BEGIN IMMEDIATE"`, B blocks and this fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engines_declared_readonly_begin_leaves_the_writer_lock_free() {
    let (available, elapsed, begin_sql) = side_can_take_writer_lock(true).await;

    assert!(
        available,
        "the engine composed {begin_sql:?} for a declared-readonly request and a competing writer \
         was blocked — a readonly transaction that takes the writer lock serialises every reader \
         against every writer, which is the whole cost D13's declaration arm exists to avoid"
    );
    assert!(
        elapsed < B_BUSY_TIMEOUT,
        "B got the lock only after waiting {elapsed:?}, i.e. it contended for it rather than \
         finding it free"
    );
}

/// **C3-3a's wiring debt, proven by consequence.** The pool's `checkout_timeout` must reach the
/// backend as its `busy_timeout`, and the proof has to be behavioural: `SqliteBackend`'s standalone
/// `DEFAULT_BUSY_TIMEOUT` is 5 s and so is `ferrod`'s `DEFAULT_POOL_CHECKOUT_TIMEOUT`, so an
/// equality assertion at the daemon's own values would pass identically whether the wiring exists or
/// not. Said plainly rather than left for a later reader to discover: the registry-level test in
/// `pools.rs` pins the INTENT, and this one is what distinguishes wired from defaulted.
///
/// A configured value far from both defaults makes the difference impossible to miss — 250 ms
/// against 5 s is a twentyfold gap, not a timing judgement.
///
/// MUTATION PROVEN: dropping `.with_busy_timeout(...)` so the backend keeps its 5 s default leaves
/// the pooled connection parked for five seconds and fails the upper bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pools_checkout_timeout_becomes_the_connections_busy_timeout() {
    const CONFIGURED: Duration = Duration::from_millis(250);

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("busy.db");

    let config = PoolConfig {
        max_size: 4,
        checkout_timeout: CONFIGURED,
        max_lifetime: Duration::from_secs(60),
        reap_interval: None,
        pin_functions: Vec::new(),
        pin_on_unknown: true,
    };
    // Built exactly as `PoolRegistry::build_with` builds it — the value read off the config rather
    // than repeated as a literal, so this cannot drift from the registry arm it stands in for.
    let pool = Pool::new(
        SqliteBackend::new(format!("sqlite://{}", path.display()))
            .with_busy_timeout(config.checkout_timeout),
        config,
    );
    assert_eq!(
        pool.backend().busy_timeout(),
        CONFIGURED,
        "the backend carries the configured value"
    );

    // Warm the pool FIRST, then release. This is not ceremony — it is load-bearing, and the first
    // version of this test failed without it: `PRAGMA journal_mode=WAL` cannot switch a database's
    // journal mode while another connection holds a lock on it, so when the side connection created
    // the file (as a rollback-journal database) and took the lock before the pool ever dialled, the
    // backend's connect failed its WAL verification and the checkout came back `ConnectionLost`
    // instead of parking. Warming first makes the file WAL before anything contends, which is also
    // the ordinary case: a pool dials at startup, contention comes later.
    //
    // Releasing rather than holding it matters too — it leaves an IDLE connection for the checkout
    // below to reuse, so that checkout does not dial a new one while the lock is held.
    drop(pool.checkout().await.expect("warm the pool into WAL"));

    // The side connection takes the writer lock FIRST, so this time it is the POOLED connection that
    // has to wait — the reverse of the lockstep tests above, and the only arrangement in which the
    // pool's own busy timeout is the one being observed.
    let b = side_connection(&path);
    b.execute_batch("BEGIN IMMEDIATE")
        .expect("the side connection takes the lock first");

    let mut co = pool.checkout().await.expect("checkout");
    let begin_sql = compose_begin_sql(pool.backend().dialect(), None, false).expect("composes");

    let started = Instant::now();
    let refused = co.begin_tx_with(TxId(1), &begin_sql).await;
    let elapsed = started.elapsed();

    assert!(
        refused.is_err(),
        "the pooled connection must be refused while B holds the writer lock"
    );
    // The floor is the same 90% allowance the C3-1 spike uses (SQLite's busy handler sleeps in
    // increments); the ceiling is what actually discriminates, and it is nowhere near either default.
    assert!(
        elapsed >= CONFIGURED.mul_f64(0.9),
        "parked for {elapsed:?}, less than the {CONFIGURED:?} configured — it did not wait at all"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "parked for {elapsed:?}: that is the 5 s standalone default, not the {CONFIGURED:?} this \
         pool configured — the checkout_timeout never reached the backend"
    );

    b.execute_batch("ROLLBACK").expect("B releases");
}
